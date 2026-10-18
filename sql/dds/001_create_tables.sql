CREATE SCHEMA IF NOT EXISTS dds;

CREATE TABLE IF NOT EXISTS dds.repositories (
    repository_id BIGINT PRIMARY KEY CHECK (repository_id > 0),
    full_name TEXT NOT NULL UNIQUE,
    html_url TEXT,
    source_batch_id UUID NOT NULL REFERENCES stg.load_batches(batch_id),
    source_snapshot_id BIGINT NOT NULL REFERENCES stg.repositories_raw(snapshot_id),
    source_observation_id BIGINT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    current_run_id UUID NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.users (
    user_id BIGINT PRIMARY KEY CHECK (user_id > 0),
    login TEXT,
    user_type TEXT,
    observed_at TIMESTAMPTZ NOT NULL,
    source_priority INTEGER NOT NULL,
    source_observation_id BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.issues (
    issue_id BIGINT PRIMARY KEY CHECK (issue_id > 0),
    repository_id BIGINT NOT NULL REFERENCES dds.repositories(repository_id),
    issue_number INTEGER NOT NULL CHECK (issue_number > 0),
    author_id BIGINT REFERENCES dds.users(user_id),
    title TEXT NOT NULL,
    html_url TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    source_state TEXT NOT NULL CHECK (source_state IN ('open', 'closed')),
    state_reason TEXT,
    source_closed_at TIMESTAMPTZ,
    source_batch_id UUID NOT NULL REFERENCES stg.load_batches(batch_id),
    source_snapshot_id BIGINT NOT NULL REFERENCES stg.issues_raw(snapshot_id),
    source_observation_id BIGINT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    history_batch_id UUID NOT NULL REFERENCES stg.load_batches(batch_id),
    UNIQUE (repository_id, issue_number),
    UNIQUE (issue_id, repository_id),
    CHECK (updated_at >= created_at),
    CHECK (source_closed_at IS NULL OR source_closed_at >= created_at),
    CHECK (source_state <> 'open' OR source_closed_at IS NULL)
);

CREATE TABLE IF NOT EXISTS dds.labels (
    label_id BIGINT PRIMARY KEY CHECK (label_id > 0),
    repository_id BIGINT NOT NULL REFERENCES dds.repositories(repository_id),
    name TEXT NOT NULL,
    color TEXT,
    description TEXT,
    observed_at TIMESTAMPTZ NOT NULL,
    source_priority INTEGER NOT NULL,
    source_observation_id BIGINT NOT NULL,
    UNIQUE (label_id, repository_id)
);

CREATE TABLE IF NOT EXISTS dds.issue_current_labels (
    issue_id BIGINT NOT NULL,
    repository_id BIGINT NOT NULL,
    label_id BIGINT NOT NULL,
    PRIMARY KEY (issue_id, label_id),
    FOREIGN KEY (issue_id, repository_id) REFERENCES dds.issues(issue_id, repository_id),
    FOREIGN KEY (label_id, repository_id) REFERENCES dds.labels(label_id, repository_id)
);

CREATE TABLE IF NOT EXISTS dds.issue_current_assignees (
    issue_id BIGINT NOT NULL REFERENCES dds.issues(issue_id),
    user_id BIGINT NOT NULL REFERENCES dds.users(user_id),
    PRIMARY KEY (issue_id, user_id)
);

CREATE TABLE IF NOT EXISTS dds.issue_events (
    event_id BIGINT PRIMARY KEY CHECK (event_id > 0),
    issue_id BIGINT NOT NULL,
    repository_id BIGINT NOT NULL,
    event_type TEXT NOT NULL,
    github_created_at TIMESTAMPTZ NOT NULL,
    actor_id BIGINT REFERENCES dds.users(user_id),
    assignee_id BIGINT REFERENCES dds.users(user_id),
    assigner_id BIGINT REFERENCES dds.users(user_id),
    target_label_id BIGINT,
    label_name TEXT,
    label_color TEXT,
    status_before TEXT CHECK (status_before IN ('open', 'closed')),
    status_after TEXT CHECK (status_after IN ('open', 'closed')),
    is_status_transition BOOLEAN NOT NULL,
    source_batch_id UUID NOT NULL REFERENCES stg.load_batches(batch_id),
    source_snapshot_id BIGINT NOT NULL REFERENCES stg.issue_events_raw(snapshot_id),
    source_observation_id BIGINT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    FOREIGN KEY (issue_id, repository_id) REFERENCES dds.issues(issue_id, repository_id),
    FOREIGN KEY (target_label_id, repository_id) REFERENCES dds.labels(label_id, repository_id),
    CHECK ((status_before IS NULL AND status_after IS NULL AND NOT is_status_transition)
        OR (status_before IS NOT NULL AND status_after IS NOT NULL
            AND is_status_transition = (status_before <> status_after)))
);
CREATE INDEX IF NOT EXISTS issue_events_history_idx
    ON dds.issue_events (issue_id, github_created_at, event_id);

CREATE TABLE IF NOT EXISTS dds.issue_status_versions (
    version_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    issue_id BIGINT NOT NULL REFERENCES dds.issues(issue_id),
    status TEXT NOT NULL CHECK (status IN ('open', 'closed')),
    valid_from TIMESTAMPTZ NOT NULL,
    valid_to TIMESTAMPTZ,
    is_current BOOLEAN NOT NULL,
    source_event_id BIGINT REFERENCES dds.issue_events(event_id),
    UNIQUE (issue_id, valid_from),
    CHECK (valid_to IS NULL OR valid_to > valid_from),
    CHECK (is_current = (valid_to IS NULL))
);
CREATE UNIQUE INDEX IF NOT EXISTS issue_status_one_current_idx
    ON dds.issue_status_versions (issue_id) WHERE is_current;
