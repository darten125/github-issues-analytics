#!/usr/bin/env bash
set -euo pipefail

# Runs only when the analytics PostgreSQL volume is initialized for the first time.
psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  -v "metabase_db=$METABASE_DB_NAME" \
  -v "metabase_user=$METABASE_DB_USER" \
  -v "metabase_password=$METABASE_DB_PASSWORD" <<'SQL'
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS dds;
CREATE SCHEMA IF NOT EXISTS cdm;

SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'metabase_user', :'metabase_password') \gexec
SELECT format('CREATE DATABASE %I OWNER %I', :'metabase_db', :'metabase_user') \gexec
SQL
