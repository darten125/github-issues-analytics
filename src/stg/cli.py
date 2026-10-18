"""CLI entry point; the same load_repository function can later be called by a DAG."""

from __future__ import annotations

import argparse
import logging
import os

import psycopg2

from .config import DEFAULT_CREATED_SINCE, REPOSITORIES
from .github_client import GitHubApiError, GitHubClient
from .loader import load_repository, parse_github_time
from .store import StgStore

LOGGER = logging.getLogger(__name__)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Load raw GitHub Issues data into STG")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("init-db", help="Apply the idempotent STG SQL schema")
    load = commands.add_parser("load", help="Load repository, issues and events")
    load.add_argument("--repository", action="append", choices=REPOSITORIES,
                      help="Repeat to select repositories; default is all three")
    load.add_argument("--created-since", default=os.getenv("GITHUB_CREATED_SINCE", DEFAULT_CREATED_SINCE),
                      help="Inclusive UTC creation cutoff, e.g. 2023-01-01T00:00:00Z")
    load.add_argument("--max-issues", type=int, help="Optional per-repository sample limit")
    load.add_argument("--mode", choices=("backfill", "sync", "audit"), default="backfill",
                      help="backfill resumes history, sync refreshes changed Issues, audit rereads all selected events")
    attest = commands.add_parser(
        "attest-cohort", help="Confirm the creation boundary of a legacy batch from its saved run configuration")
    attest.add_argument("--batch-id", required=True, help="UUID of the finished STG batch")
    attest.add_argument("--created-since", required=True,
                        help="Boundary from the saved run configuration, e.g. 2023-01-01T00:00:00Z")
    attest.add_argument("--evidence", required=True,
                        help="Reference to the saved configuration, e.g. DagRun ID and report path")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if args.command == "load":
        if args.max_issues is not None and args.max_issues < 1:
            LOGGER.error("--max-issues must be positive")
            return 1
        token = os.getenv("GITHUB_TOKEN", "").strip()
        if not token:
            LOGGER.error("GITHUB_TOKEN is missing; authorized STG load cannot run")
            return 1
        try:
            cutoff = parse_github_time(args.created_since)
        except ValueError:
            LOGGER.error("--created-since must be a timestamp with time zone")
            return 1
    if args.command == "attest-cohort":
        try:
            cutoff = parse_github_time(args.created_since)
        except ValueError:
            LOGGER.error("--created-since must be a timestamp with time zone")
            return 1

    store = None
    try:
        store = StgStore.from_env()
        store.apply_schema()
        if args.command == "init-db":
            LOGGER.info("STG schema applied")
            return 0
        if args.command == "attest-cohort":
            try:
                outcome = store.attest_created_since(args.batch_id, cutoff, args.evidence)
            except ValueError as exc:
                # Messages are fixed strings without payloads or credentials.
                LOGGER.error("Cohort attestation rejected: %s", exc)
                return 1
            LOGGER.info("Batch %s cohort boundary %s: %s", args.batch_id, args.created_since, outcome)
            return 0
        client = GitHubClient(token)
        for full_name in args.repository or REPOSITORIES:
            batch_id = load_repository(client, store, full_name, cutoff, args.max_issues, mode=args.mode)
            LOGGER.info("Repository %s finished in batch %s", full_name, batch_id)
        return 0
    except GitHubApiError as exc:
        LOGGER.error("GitHub API error: %s", exc)
    except psycopg2.Error as exc:
        LOGGER.error("PostgreSQL error (SQLSTATE %s)", exc.pgcode or "unknown")
    except (KeyError, TypeError, ValueError) as exc:
        LOGGER.error("Invalid data or configuration: %s", type(exc).__name__)
    except Exception as exc:
        LOGGER.error("STG load failed: %s", type(exc).__name__)
    finally:
        if store is not None:
            store.close()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
