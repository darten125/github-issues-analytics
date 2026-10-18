"""Reproducible small DDS for CDM development, only in a separate *_test database.

The batches are synthetic and marked verified inside the test database only;
production publication still rejects sample batches. Control results of the six
reference metrics are documented in docs/CDM_HANDOFF.md.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

from src.stg.config import DEFAULT_CREATED_SINCE, REPOSITORIES

from .testdb import require_test_database

BASE = 9_100_000_000
REPOSITORY_IDS = (BASE + 1, BASE + 2, BASE + 3)
USERS = {name: {"id": BASE + 300_000 + number, "login": name, "type": "User"}
         for number, name in enumerate(("alice", "bob", "carol"), start=1)}
LABELS = {
    0: {"bug": BASE + 400_001, "help": BASE + 400_002, "feature": BASE + 400_003},
    1: {"bug": BASE + 400_011, "feature": BASE + 400_012},
    2: {"ui": BASE + 400_021},
}
# (repository, number, created_at, author, labels, assignees, [(event, created_at)])
ISSUES: tuple[tuple[Any, ...], ...] = (
    (0, 1, "2023-01-05T10:00:00Z", "alice", ("bug", "help"), ("alice", "bob"),
     [("labeled", "2023-01-05T11:00:00Z"), ("closed", "2023-01-15T10:00:00Z")]),
    (0, 2, "2023-02-01T00:00:00Z", None, ("bug",), (),
     [("closed", "2023-03-11T00:00:00Z")]),
    (0, 3, "2023-03-01T00:00:00Z", "bob", ("feature",), (),
     [("closed", "2023-03-03T00:00:00Z"), ("reopened", "2023-03-10T00:00:00Z"),
      ("closed", "2023-04-05T00:00:00Z")]),
    (0, 4, "2023-03-20T12:00:00Z", "alice", (), (), []),
    (1, 1, "2023-01-10T00:00:00Z", "carol", ("bug", "feature"), ("carol",),
     [("closed", "2023-04-10T00:00:00Z")]),
    (1, 2, "2023-03-31T23:00:00Z", "bob", (), (), []),
    (1, 3, "2023-04-02T00:00:00Z", "alice", ("bug",), (),
     [("closed", "2023-04-03T00:00:00Z")]),
    (2, 1, "2023-02-10T00:00:00Z", "carol", ("ui",), ("alice", "bob", "carol"),
     [("closed", "2023-02-20T00:00:00Z"), ("reopened", "2023-02-20T00:00:00Z")]),
    (2, 2, "2023-03-31T00:00:00Z", "bob", (), (),
     [("closed", "2023-04-01T00:00:00Z")]),
)
ISSUE_IDS = tuple(BASE + 100_000 + 10 * repository + number for repository, number, *_ in ISSUES)


def _issue_payload(index, issue_id):
    repository, number, created, author, labels, assignees, events = ISSUES[index]
    closures = [at for kind, at in events if kind == "closed"]
    state = "open" if not events or events[-1][0] != "closed" else "closed"
    return {
        "id": issue_id, "number": number, "title": f"fixture issue {repository}-{number}",
        "created_at": created, "updated_at": events[-1][1] if events else created,
        "state": state, "closed_at": closures[-1] if state == "closed" else None,
        "user": USERS[author] if author else None,
        "labels": [{"id": LABELS[repository][name], "name": name, "color": "ededed",
                    "description": f"fixture {name}"} for name in labels],
        "assignees": [USERS[name] for name in assignees],
        "html_url": f"https://github.com/{REPOSITORIES[repository]}/issues/{number}",
    }


def build_stg(store) -> list[str]:
    """Write one verified fixture batch per repository through the STG store API."""
    cohort = datetime.fromisoformat(DEFAULT_CREATED_SINCE.replace("Z", "+00:00")).astimezone(timezone.utc)
    batch_ids = []
    for repository, full_name in enumerate(REPOSITORIES):
        repository_id = REPOSITORY_IDS[repository]
        batch_id = store.start_batch(full_name, mode="backfill", created_since=cohort)
        batch_ids.append(batch_id)
        store.save_repository(batch_id, {"id": repository_id, "full_name": full_name,
                                         "html_url": f"https://github.com/{full_name}"})
        selected = [(index, ISSUE_IDS[index]) for index, issue in enumerate(ISSUES) if issue[0] == repository]
        payloads = [_issue_payload(index, issue_id) for index, issue_id in selected]
        store.save_issues(batch_id, repository_id, payloads, len(payloads))
        for (index, issue_id), payload in zip(selected, payloads):
            events = [{"id": BASE + 200_000 + 10 * (issue_id - BASE - 100_000) + offset, "event": kind,
                       "created_at": at, "actor": USERS["alice"],
                       **({"label": {"name": "bug", "color": "ededed"}} if kind == "labeled" else {})}
                      for offset, (kind, at) in enumerate(ISSUES[index][6], start=1)]
            store.start_issue_events(batch_id, repository_id, payload)
            store.save_events(batch_id, repository_id, issue_id, events)
            store.finish_issue_events(batch_id, issue_id, "succeeded")
        store.record_issue_scan(batch_id, "verified", 2, len(payloads))
        store.finish_batch(batch_id, "succeeded")
    return batch_ids


def main() -> int:
    import psycopg2

    from src.stg.store import StgStore

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        # The guard compares addresses before any connection is opened.
        settings = require_test_database(os.getenv("DDS_TEST_DSN"))
    except RuntimeError as exc:
        logging.error("Fixture refused: %s", exc)
        return 1
    connection = psycopg2.connect(os.environ["DDS_TEST_DSN"])
    try:
        store = StgStore(connection)
        store.apply_schema()
        batch_ids = build_stg(store)
    finally:
        connection.close()
    # Only this process is redirected to the guarded test database.
    for key, setting in (("HOST", "host"), ("PORT", "port"), ("NAME", "dbname"),
                         ("USER", "user"), ("PASSWORD", "password")):
        os.environ[f"DWH_DB_{key}"] = str(settings.get(setting) or ("5432" if key == "PORT" else ""))
    from .loader import load_dds

    result = load_dds(batch_ids)
    print(json.dumps({"status": result["status"], "source_batch_ids": batch_ids,
                      "issues": result["issues"], "events": result["events"],
                      "ready_for_cdm": result["ready_for_cdm"],
                      "coverage_complete": result["coverage_complete"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
