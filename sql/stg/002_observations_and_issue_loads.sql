-- Additive, re-runnable migration. Existing raw snapshots and batches are preserved.
CREATE TABLE IF NOT EXISTS stg.repository_observations (
    observation_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    snapshot_id BIGINT NOT NULL REFERENCES stg.repositories_raw(snapshot_id),
    batch_id UUID NOT NULL REFERENCES stg.load_batches(batch_id),
    observed_at TIMESTAMPTZ NOT NULL,
    UNIQUE (batch_id, snapshot_id)
);

CREATE TABLE IF NOT EXISTS stg.issue_observations (
    observation_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    snapshot_id BIGINT NOT NULL REFERENCES stg.issues_raw(snapshot_id),
    batch_id UUID NOT NULL REFERENCES stg.load_batches(batch_id),
    observed_at TIMESTAMPTZ NOT NULL,
    UNIQUE (batch_id, snapshot_id)
);

CREATE TABLE IF NOT EXISTS stg.event_observations (
    observation_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    snapshot_id BIGINT NOT NULL REFERENCES stg.issue_events_raw(snapshot_id),
    batch_id UUID NOT NULL REFERENCES stg.load_batches(batch_id),
    observed_at TIMESTAMPTZ NOT NULL,
    UNIQUE (batch_id, snapshot_id)
);

CREATE INDEX IF NOT EXISTS repository_observations_snapshot_idx
    ON stg.repository_observations (snapshot_id, observed_at DESC, observation_id DESC);
CREATE INDEX IF NOT EXISTS issue_observations_snapshot_idx
    ON stg.issue_observations (snapshot_id, observed_at DESC, observation_id DESC);
CREATE INDEX IF NOT EXISTS event_observations_snapshot_idx
    ON stg.event_observations (snapshot_id, observed_at DESC, observation_id DESC);

CREATE TABLE IF NOT EXISTS stg.issue_event_loads (
    batch_id UUID NOT NULL REFERENCES stg.load_batches(batch_id),
    issue_id BIGINT NOT NULL,
    repository_id BIGINT NOT NULL,
    issue_number INTEGER NOT NULL,
    issue_updated_at TIMESTAMPTZ,
    mode TEXT NOT NULL CHECK (mode IN ('backfill', 'sync')),
    status TEXT NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
    events_fetched BIGINT NOT NULL DEFAULT 0,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    error_message TEXT,
    PRIMARY KEY (batch_id, issue_id)
);

CREATE INDEX IF NOT EXISTS issue_event_loads_success_idx
    ON stg.issue_event_loads (issue_id, issue_updated_at, finished_at DESC)
    WHERE status = 'succeeded';
CREATE INDEX IF NOT EXISTS issue_event_loads_incomplete_idx
    ON stg.issue_event_loads (repository_id, issue_id, started_at DESC)
    WHERE status <> 'succeeded';

-- First and last observations held by the legacy schema are known. Intermediate
-- observations cannot be reconstructed. No legacy event load is marked complete.
INSERT INTO stg.repository_observations (snapshot_id, batch_id, observed_at)
SELECT snapshot_id, first_batch_id, first_seen_at FROM stg.repositories_raw
ON CONFLICT (batch_id, snapshot_id) DO NOTHING;
INSERT INTO stg.repository_observations (snapshot_id, batch_id, observed_at)
SELECT snapshot_id, last_batch_id, last_seen_at FROM stg.repositories_raw
ON CONFLICT (batch_id, snapshot_id) DO NOTHING;

INSERT INTO stg.issue_observations (snapshot_id, batch_id, observed_at)
SELECT snapshot_id, first_batch_id, first_seen_at FROM stg.issues_raw
ON CONFLICT (batch_id, snapshot_id) DO NOTHING;
INSERT INTO stg.issue_observations (snapshot_id, batch_id, observed_at)
SELECT snapshot_id, last_batch_id, last_seen_at FROM stg.issues_raw
ON CONFLICT (batch_id, snapshot_id) DO NOTHING;

INSERT INTO stg.event_observations (snapshot_id, batch_id, observed_at)
SELECT snapshot_id, first_batch_id, first_seen_at FROM stg.issue_events_raw
ON CONFLICT (batch_id, snapshot_id) DO NOTHING;
INSERT INTO stg.event_observations (snapshot_id, batch_id, observed_at)
SELECT snapshot_id, last_batch_id, last_seen_at FROM stg.issue_events_raw
ON CONFLICT (batch_id, snapshot_id) DO NOTHING;
