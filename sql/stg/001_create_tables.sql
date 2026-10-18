-- Safe to re-run against an initialized analytics database. Do not drop or truncate data.
CREATE SCHEMA IF NOT EXISTS stg;

CREATE TABLE IF NOT EXISTS stg.load_batches (
    batch_id UUID PRIMARY KEY,
    repository_full_name TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
    repositories_fetched BIGINT NOT NULL DEFAULT 0,
    repository_versions_inserted BIGINT NOT NULL DEFAULT 0,
    issues_fetched BIGINT NOT NULL DEFAULT 0,
    issues_selected BIGINT NOT NULL DEFAULT 0,
    issue_versions_inserted BIGINT NOT NULL DEFAULT 0,
    events_fetched BIGINT NOT NULL DEFAULT 0,
    event_versions_inserted BIGINT NOT NULL DEFAULT 0,
    error_count BIGINT NOT NULL DEFAULT 0,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS stg.repositories_raw (
    snapshot_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    repository_id BIGINT NOT NULL,
    full_name TEXT NOT NULL,
    raw_json JSONB NOT NULL,
    payload_sha256 CHAR(64) NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL,
    first_batch_id UUID NOT NULL REFERENCES stg.load_batches(batch_id),
    last_batch_id UUID NOT NULL REFERENCES stg.load_batches(batch_id),
    UNIQUE (repository_id, payload_sha256)
);

CREATE TABLE IF NOT EXISTS stg.issues_raw (
    snapshot_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    issue_id BIGINT NOT NULL,
    repository_id BIGINT NOT NULL,
    issue_number INTEGER NOT NULL,
    raw_json JSONB NOT NULL,
    payload_sha256 CHAR(64) NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL,
    first_batch_id UUID NOT NULL REFERENCES stg.load_batches(batch_id),
    last_batch_id UUID NOT NULL REFERENCES stg.load_batches(batch_id),
    UNIQUE (issue_id, payload_sha256)
);

CREATE TABLE IF NOT EXISTS stg.issue_events_raw (
    snapshot_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id BIGINT NOT NULL,
    issue_id BIGINT NOT NULL,
    repository_id BIGINT NOT NULL,
    event_type TEXT NOT NULL,
    github_created_at TIMESTAMPTZ NOT NULL,
    raw_json JSONB NOT NULL,
    payload_sha256 CHAR(64) NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL,
    first_batch_id UUID NOT NULL REFERENCES stg.load_batches(batch_id),
    last_batch_id UUID NOT NULL REFERENCES stg.load_batches(batch_id),
    UNIQUE (event_id, payload_sha256)
);

CREATE INDEX IF NOT EXISTS repositories_raw_current_idx
    ON stg.repositories_raw (repository_id, last_seen_at DESC);
CREATE INDEX IF NOT EXISTS issues_raw_repository_idx
    ON stg.issues_raw (repository_id, issue_number, last_seen_at DESC);
CREATE INDEX IF NOT EXISTS issue_events_raw_issue_idx
    ON stg.issue_events_raw (issue_id, github_created_at, event_id);
