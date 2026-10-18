"""Transactional PostgreSQL persistence for versioned raw GitHub responses."""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json

from .loader import parse_github_time


def payload_hash(payload: dict) -> str:
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class StgStore:
    def __init__(self, connection):
        self.connection = connection

    @classmethod
    def from_env(cls):
        names = ("DWH_DB_HOST", "DWH_DB_PORT", "DWH_DB_NAME", "DWH_DB_USER", "DWH_DB_PASSWORD")
        missing = [name for name in names if not os.getenv(name)]
        if missing:
            raise ValueError("Missing database settings: " + ", ".join(missing))
        return cls(psycopg2.connect(
            host=os.environ["DWH_DB_HOST"],
            port=os.environ["DWH_DB_PORT"],
            dbname=os.environ["DWH_DB_NAME"],
            user=os.environ["DWH_DB_USER"],
            password=os.environ["DWH_DB_PASSWORD"],
            connect_timeout=10,
        ))

    def close(self):
        self.connection.close()

    def apply_schema(self):
        paths = sorted((Path(__file__).resolve().parents[2] / "sql" / "stg").glob("[0-9]*.sql"))
        with self.connection:
            with self.connection.cursor() as cursor:
                for path in paths:
                    cursor.execute(path.read_text(encoding="utf-8"))

    def start_batch(self, full_name: str, mode: str = "backfill"):
        if mode not in ("backfill", "sync", "audit"):
            raise ValueError("invalid load mode")
        batch_id = str(uuid.uuid4())
        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO stg.load_batches (batch_id, repository_full_name, load_mode, status) "
                    "VALUES (%s, %s, %s, 'running')",
                    (batch_id, full_name, mode),
                )
        return batch_id

    def record_issue_scan(self, batch_id, status: str, passes: int, issue_count: int):
        if status not in ("sample", "verified", "unverified") or passes < 0 or issue_count < 0:
            raise ValueError("invalid issue scan result")
        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE stg.load_batches SET issue_scan_status = %s, issue_scan_passes = %s, "
                    "issue_scan_issue_count = %s WHERE batch_id = %s AND status = 'running'",
                    (status, passes, issue_count, batch_id),
                )
                if cursor.rowcount != 1:
                    raise RuntimeError("batch not running or missing")

    def finish_batch(self, batch_id, status: str, error_message: str | None = None):
        if status not in ("succeeded", "failed"):
            raise ValueError("invalid batch status")
        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE stg.load_batches SET status = %s, finished_at = now(), "
                    "error_count = error_count + %s, error_message = %s "
                    "WHERE batch_id = %s AND status = 'running'",
                    (status, int(status == "failed"), error_message, batch_id),
                )
                if cursor.rowcount != 1:
                    raise RuntimeError("batch not running or missing")

    @staticmethod
    def _save_snapshot(cursor, table: str, id_column: str, entity_id: int, extra: dict, payload: dict, batch_id) -> bool:
        columns = [id_column, *extra, "raw_json", "payload_sha256", "first_seen_at", "last_seen_at", "first_batch_id", "last_batch_id"]
        observed_at = datetime.now(timezone.utc)
        digest = payload_hash(payload)
        values = [entity_id, *extra.values(), Json(payload), digest, observed_at, observed_at, batch_id, batch_id]
        query = sql.SQL("INSERT INTO stg.{} ({}) VALUES ({}) ON CONFLICT ({}, payload_sha256) DO NOTHING RETURNING snapshot_id").format(
            sql.Identifier(table),
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(", ").join(sql.Placeholder() for _ in columns),
            sql.Identifier(id_column),
        )
        cursor.execute(query, values)
        row = cursor.fetchone()
        inserted = row is not None
        if inserted:
            snapshot_id = row[0]
        else:
            cursor.execute(
                sql.SQL("UPDATE stg.{} SET last_seen_at = GREATEST(last_seen_at, %s), last_batch_id = %s "
                        "WHERE {} = %s AND payload_sha256 = %s RETURNING snapshot_id").format(
                            sql.Identifier(table), sql.Identifier(id_column)),
                (observed_at, batch_id, entity_id, digest),
            )
            row = cursor.fetchone()
            if row is None:
                raise RuntimeError("snapshot upsert did not affect one row")
            snapshot_id = row[0]
        observation_table = {
            "repositories_raw": "repository_observations",
            "issues_raw": "issue_observations",
            "issue_events_raw": "event_observations",
        }[table]
        cursor.execute(
            sql.SQL("INSERT INTO stg.{} (snapshot_id, batch_id, observed_at) VALUES (%s, %s, %s) "
                    "ON CONFLICT (batch_id, snapshot_id) DO NOTHING").format(sql.Identifier(observation_table)),
            (snapshot_id, batch_id, observed_at),
        )
        return inserted

    def has_completed_events(self, issue_id: int, updated_at: str | None, mode: str) -> bool:
        if mode not in ("backfill", "sync", "audit"):
            raise ValueError("invalid load mode")
        if mode == "audit":
            return False
        if mode == "sync" and not updated_at:
            return False
        with self.connection.cursor() as cursor:
            cursor.execute(
                "SELECT EXISTS (SELECT 1 FROM stg.issue_event_loads "
                "WHERE issue_id = %s AND status = 'succeeded' "
                "AND (%s = 'backfill' OR issue_updated_at = %s))",
                (issue_id, mode, parse_github_time(updated_at) if updated_at else None),
            )
            return cursor.fetchone()[0]

    def start_issue_events(self, batch_id, repository_id: int, issue: dict, mode: str = "backfill"):
        if mode not in ("backfill", "sync", "audit"):
            raise ValueError("invalid load mode")
        updated_at = issue.get("updated_at")
        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO stg.issue_event_loads "
                    "(batch_id, issue_id, repository_id, issue_number, issue_updated_at, mode, status) "
                    "VALUES (%s, %s, %s, %s, %s, %s, 'running')",
                    (batch_id, int(issue["id"]), repository_id, int(issue["number"]),
                     parse_github_time(updated_at) if updated_at else None, mode),
                )

    def finish_issue_events(self, batch_id, issue_id: int, status: str, error_message: str | None = None):
        if status not in ("succeeded", "failed"):
            raise ValueError("invalid issue event load status")
        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE stg.issue_event_loads SET status = %s, finished_at = now(), error_message = %s "
                    "WHERE batch_id = %s AND issue_id = %s AND status = 'running'",
                    (status, error_message, batch_id, issue_id),
                )
                if cursor.rowcount != 1:
                    raise RuntimeError("issue event load not running or missing")

    def save_repository(self, batch_id, repository: dict):
        with self.connection:
            with self.connection.cursor() as cursor:
                inserted = self._save_snapshot(
                    cursor, "repositories_raw", "repository_id", int(repository["id"]),
                    {"full_name": repository["full_name"]}, repository, batch_id,
                )
                cursor.execute(
                    "UPDATE stg.load_batches SET repositories_fetched = repositories_fetched + 1, "
                    "repository_versions_inserted = repository_versions_inserted + %s WHERE batch_id = %s",
                    (int(inserted), batch_id),
                )

    def save_issues(self, batch_id, repository_id: int, issues: list[dict], fetched_count: int):
        inserted = 0
        with self.connection:
            with self.connection.cursor() as cursor:
                for issue in issues:
                    inserted += int(self._save_snapshot(
                        cursor, "issues_raw", "issue_id", int(issue["id"]),
                        {"repository_id": repository_id, "issue_number": int(issue["number"])}, issue, batch_id,
                    ))
                cursor.execute(
                    "UPDATE stg.load_batches SET issues_fetched = issues_fetched + %s, "
                    "issues_selected = issues_selected + %s, issue_versions_inserted = issue_versions_inserted + %s "
                    "WHERE batch_id = %s",
                    (fetched_count, len(issues), inserted, batch_id),
                )

    def save_events(self, batch_id, repository_id: int, issue_id: int, events: list[dict]):
        inserted = 0
        with self.connection:
            with self.connection.cursor() as cursor:
                for event in events:
                    inserted += int(self._save_snapshot(
                        cursor, "issue_events_raw", "event_id", int(event["id"]),
                        {
                            "issue_id": issue_id,
                            "repository_id": repository_id,
                            "event_type": event["event"],
                            "github_created_at": parse_github_time(event["created_at"]),
                        }, event, batch_id,
                    ))
                cursor.execute(
                    "UPDATE stg.load_batches SET events_fetched = events_fetched + %s, "
                    "event_versions_inserted = event_versions_inserted + %s WHERE batch_id = %s",
                    (len(events), inserted, batch_id),
                )
                cursor.execute(
                    "UPDATE stg.issue_event_loads SET events_fetched = events_fetched + %s "
                    "WHERE batch_id = %s AND issue_id = %s AND status = 'running'",
                    (len(events), batch_id, issue_id),
                )
                if cursor.rowcount != 1:
                    raise RuntimeError("issue event load not running or missing")
