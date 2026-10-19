"""PostgreSQL boundaries for CDM; this module never queries the STG schema."""

from __future__ import annotations

import os
from pathlib import Path

from . import CdmError

# DDS uses this session lock for publication. Hold it until CDM commits.
DDS_PUBLICATION_LOCK = 718230031


def connect():
    import psycopg2

    keys = ("HOST", "PORT", "NAME", "USER", "PASSWORD")
    missing = [f"DWH_DB_{key}" for key in keys if not os.getenv(f"DWH_DB_{key}")]
    if missing:
        raise CdmError("missing_database_settings", ", ".join(missing))
    return psycopg2.connect(
        host=os.environ["DWH_DB_HOST"], port=os.environ["DWH_DB_PORT"],
        dbname=os.environ["DWH_DB_NAME"], user=os.environ["DWH_DB_USER"],
        password=os.environ["DWH_DB_PASSWORD"], connect_timeout=10,
    )


def lock_dds(connection):
    connection.autocommit = True
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_advisory_lock(%s)", (DDS_PUBLICATION_LOCK,))


def apply_schema(connection):
    connection.autocommit = False
    folder = Path(__file__).resolve().parents[2] / "sql" / "cdm"
    with connection:
        with connection.cursor() as cursor:
            for path in sorted(folder.glob("[0-9]*.sql")):
                cursor.execute(path.read_text(encoding="utf-8"))


def ensure_reader(connection):
    """Create or rotate a CDM-only login when its local secret is configured."""
    from psycopg2 import sql

    password = os.getenv("CDM_READ_PASSWORD")
    if not password:
        return False
    with connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_roles WHERE rolname='cdm_reader'")
            if cursor.fetchone():
                cursor.execute(sql.SQL("ALTER ROLE {} LOGIN PASSWORD %s").format(
                    sql.Identifier("cdm_reader")), (password,))
            else:
                cursor.execute(sql.SQL("CREATE ROLE {} LOGIN PASSWORD %s").format(
                    sql.Identifier("cdm_reader")), (password,))
            cursor.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                sql.Identifier(os.environ["DWH_DB_NAME"]), sql.Identifier("cdm_reader")))
            cursor.execute("GRANT USAGE ON SCHEMA cdm TO cdm_reader")
            cursor.execute("GRANT SELECT ON cdm.repository_snapshot, cdm.issue_lifecycle, "
                           "cdm.issue_day, cdm.current_issue_label TO cdm_reader")
    return True


def fetch_dicts(connection, query, params=()):
    from psycopg2.extras import RealDictCursor

    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]
