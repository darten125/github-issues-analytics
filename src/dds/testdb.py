"""Guard for disposable test databases; runs before any connection is opened."""

from __future__ import annotations

import os
from collections.abc import Mapping

LOCAL_HOSTS = ("", "localhost", "127.0.0.1", "::1")


def _address(host, port, dbname):
    host = (host or "").strip().lower()
    return ("local" if host in LOCAL_HOSTS or host.startswith("/") else host,
            str(port or "5432").strip(), (dbname or "").strip())


def require_test_database(dsn: str | None, environ: Mapping[str, str] | None = None) -> dict:
    """Return parsed settings of an isolated *_test database or raise RuntimeError.

    The runtime DWH address is compared whenever its name is configured,
    whether or not a runtime password or user is present.
    """
    from psycopg2.extensions import parse_dsn

    environ = os.environ if environ is None else environ
    if not dsn:
        raise RuntimeError("DDS_TEST_DSN is required")
    try:
        settings = parse_dsn(dsn)
    except Exception:
        raise RuntimeError("DDS_TEST_DSN is not a valid PostgreSQL DSN") from None
    if not settings.get("dbname", "").endswith("_test"):
        raise RuntimeError("test database name must end with _test")
    runtime_name = environ.get("DWH_DB_NAME")
    if runtime_name:
        test_address = _address(settings.get("host"), settings.get("port"), settings["dbname"])
        live_address = _address(environ.get("DWH_DB_HOST"), environ.get("DWH_DB_PORT"), runtime_name)
        if test_address == live_address:
            raise RuntimeError("test database must differ from the runtime DWH database")
    return settings
