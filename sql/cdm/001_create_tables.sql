-- CDM rules v1. Every business table has a natural, documented grain.
CREATE SCHEMA IF NOT EXISTS cdm;

CREATE TABLE IF NOT EXISTS cdm.load_runs (
    run_id UUID PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL CHECK (status IN ('succeeded', 'failed', 'unchanged')),
    source_fingerprint TEXT,
    error_code TEXT,
    result JSONB
);

-- Exactly one row identifies the currently published CDM generation.
CREATE TABLE IF NOT EXISTS cdm.current_state (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    run_id UUID NOT NULL REFERENCES cdm.load_runs(run_id),
    source_fingerprint TEXT NOT NULL,
    common_last_day DATE NOT NULL,
    published_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- Grain: one repository in the active publication.
CREATE TABLE IF NOT EXISTS cdm.repository_snapshot (
    repository_id BIGINT PRIMARY KEY,
    full_name TEXT NOT NULL UNIQUE,
    source_batch_id UUID NOT NULL,
    dds_run_id UUID NOT NULL,
    source_batch_finished_at TIMESTAMPTZ NOT NULL,
    reporting_horizon TIMESTAMPTZ NOT NULL,
    last_complete_report_day DATE NOT NULL,
    common_last_day DATE NOT NULL
);

-- Grain: one GitHub Issue in the active publication.
CREATE TABLE IF NOT EXISTS cdm.issue_lifecycle (
    issue_id BIGINT PRIMARY KEY,
    repository_id BIGINT NOT NULL REFERENCES cdm.repository_snapshot(repository_id),
    issue_number INTEGER NOT NULL,
    author_id BIGINT,
    created_at TIMESTAMPTZ NOT NULL,
    first_closed_at TIMESTAMPTZ,
    first_closed_day DATE,
    first_reopened_at TIMESTAMPTZ,
    first_close_days NUMERIC,
    CHECK (first_closed_at IS NULL OR first_closed_at >= created_at),
    CHECK ((first_closed_at IS NULL) = (first_closed_day IS NULL)),
    CHECK (first_reopened_at IS NULL OR first_closed_at IS NOT NULL),
    CHECK (first_close_days IS NULL OR first_close_days >= 0)
);
CREATE INDEX IF NOT EXISTS cdm_lifecycle_repo_created_idx
    ON cdm.issue_lifecycle (repository_id, created_at);
CREATE INDEX IF NOT EXISTS cdm_lifecycle_first_closed_idx
    ON cdm.issue_lifecycle (first_closed_at);

-- Grain: one Issue on one complete UTC report day, from its creation day
-- through the common last complete day. Status is just before next midnight.
CREATE TABLE IF NOT EXISTS cdm.issue_day (
    issue_id BIGINT NOT NULL REFERENCES cdm.issue_lifecycle(issue_id),
    report_day DATE NOT NULL,
    repository_id BIGINT NOT NULL REFERENCES cdm.repository_snapshot(repository_id),
    status_eod TEXT NOT NULL CHECK (status_eod IN ('open', 'closed')),
    is_new BOOLEAN NOT NULL,
    ever_closed BOOLEAN NOT NULL,
    ever_reopened BOOLEAN NOT NULL,
    first_close_days NUMERIC,
    PRIMARY KEY (issue_id, report_day),
    CHECK (NOT ever_reopened OR ever_closed),
    CHECK ((first_close_days IS NOT NULL) = ever_closed)
);
CREATE INDEX IF NOT EXISTS cdm_issue_day_report_repo_idx
    ON cdm.issue_day (report_day, repository_id);

-- Grain: one Issue / current label ID pair. Labels are not historical.
CREATE TABLE IF NOT EXISTS cdm.current_issue_label (
    issue_id BIGINT NOT NULL REFERENCES cdm.issue_lifecycle(issue_id),
    repository_id BIGINT NOT NULL REFERENCES cdm.repository_snapshot(repository_id),
    label_id BIGINT NOT NULL,
    label_name TEXT NOT NULL,
    label_color TEXT,
    PRIMARY KEY (issue_id, label_id)
);
CREATE INDEX IF NOT EXISTS cdm_current_label_repo_name_idx
    ON cdm.current_issue_label (repository_id, label_name);
