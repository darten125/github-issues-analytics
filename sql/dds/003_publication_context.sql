-- Additive DDS rules v2 migration. Existing rows and keys are preserved; context
-- unknown for older publications stays NULL until a v2 publication confirms it.
ALTER TABLE dds.repositories
    ADD COLUMN IF NOT EXISTS source_batch_started_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS source_batch_finished_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS cohort_created_since TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS cohort_source TEXT,
    ADD COLUMN IF NOT EXISTS cohort_evidence TEXT,
    ADD COLUMN IF NOT EXISTS verified_issue_count BIGINT;

ALTER TABLE dds.issues
    ADD COLUMN IF NOT EXISTS history_started_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS history_finished_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS history_mode TEXT,
    ADD COLUMN IF NOT EXISTS history_issue_updated_at TIMESTAMPTZ;

ALTER TABLE dds.users
    ADD COLUMN IF NOT EXISTS source_kind TEXT,
    ADD COLUMN IF NOT EXISTS source_snapshot_id BIGINT,
    ADD COLUMN IF NOT EXISTS accepted_run_id UUID REFERENCES dds.load_runs(run_id);

ALTER TABLE dds.labels
    ADD COLUMN IF NOT EXISTS source_kind TEXT,
    ADD COLUMN IF NOT EXISTS source_snapshot_id BIGINT,
    ADD COLUMN IF NOT EXISTS accepted_run_id UUID REFERENCES dds.load_runs(run_id);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conrelid = 'dds.repositories'::regclass
                     AND conname = 'repositories_publication_context_check') THEN
        ALTER TABLE dds.repositories ADD CONSTRAINT repositories_publication_context_check CHECK (
            (cohort_source IS NULL OR cohort_source IN ('recorded', 'attested'))
            AND (verified_issue_count IS NULL OR verified_issue_count >= 0)
            AND (source_batch_finished_at IS NULL OR source_batch_started_at <= source_batch_finished_at));
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conrelid = 'dds.issues'::regclass
                     AND conname = 'issues_history_context_check') THEN
        ALTER TABLE dds.issues ADD CONSTRAINT issues_history_context_check CHECK (
            (history_mode IS NULL OR history_mode IN ('backfill', 'sync', 'audit'))
            AND (history_finished_at IS NULL OR history_started_at <= history_finished_at));
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conrelid = 'dds.users'::regclass
                     AND conname = 'users_provenance_check') THEN
        ALTER TABLE dds.users ADD CONSTRAINT users_provenance_check CHECK (
            (source_kind IS NULL OR (source_kind, source_priority) IN (('issue', 1), ('event', 0)))
            AND (accepted_run_id IS NULL OR (source_kind IS NOT NULL AND source_snapshot_id IS NOT NULL)));
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conrelid = 'dds.labels'::regclass
                     AND conname = 'labels_provenance_check') THEN
        ALTER TABLE dds.labels ADD CONSTRAINT labels_provenance_check CHECK (
            (source_kind IS NULL OR (source_kind, source_priority) IN (('issue', 1), ('event', 0)))
            AND (accepted_run_id IS NULL OR (source_kind IS NOT NULL AND source_snapshot_id IS NOT NULL)));
    END IF;
END $$;

-- Rules v1 wrote priority 1 only for Issue snapshots and 0 only for events, and the
-- observation ID is the primary key of the matching STG table. Only these exact
-- matches are restored; the accepting run of a v1 value is unknown and stays NULL.
UPDATE dds.users u SET source_kind = 'issue', source_snapshot_id = o.snapshot_id
FROM stg.issue_observations o
WHERE u.source_kind IS NULL AND u.source_priority = 1
  AND o.observation_id = u.source_observation_id AND o.observed_at = u.observed_at;
UPDATE dds.users u SET source_kind = 'event', source_snapshot_id = o.snapshot_id
FROM stg.event_observations o
WHERE u.source_kind IS NULL AND u.source_priority = 0
  AND o.observation_id = u.source_observation_id AND o.observed_at = u.observed_at;
UPDATE dds.labels l SET source_kind = 'issue', source_snapshot_id = o.snapshot_id
FROM stg.issue_observations o
WHERE l.source_kind IS NULL AND l.source_priority = 1
  AND o.observation_id = l.source_observation_id AND o.observed_at = l.observed_at;
UPDATE dds.labels l SET source_kind = 'event', source_snapshot_id = o.snapshot_id
FROM stg.event_observations o
WHERE l.source_kind IS NULL AND l.source_priority = 0
  AND o.observation_id = l.source_observation_id AND o.observed_at = l.observed_at;

CREATE INDEX IF NOT EXISTS issues_repository_idx ON dds.issues (repository_id);

-- Reads only DDS. reporting_horizon is the earliest Issue observation of the
-- publication (batch end for an empty confirmed population); it bounds complete
-- report days and is not a guarantee that the GitHub API history is exhaustive.
-- Keep the rules version literal equal to src.dds.RULES_VERSION.
CREATE OR REPLACE VIEW dds.publication_manifest AS
WITH issue_context AS (
    SELECT repository_id,
           count(*) AS published_issue_count,
           min(observed_at) AS min_issue_observed_at,
           max(observed_at) AS max_issue_observed_at,
           min(history_finished_at) AS history_checked_from,
           max(history_finished_at) AS history_checked_to,
           count(*) FILTER (WHERE history_finished_at IS NULL) AS issues_without_history_context
    FROM dds.issues
    GROUP BY repository_id
), horizon AS (
    SELECT r.repository_id,
           coalesce(i.min_issue_observed_at, r.source_batch_finished_at) AS reporting_horizon
    FROM dds.repositories r
    LEFT JOIN issue_context i USING (repository_id)
)
SELECT r.repository_id,
       r.full_name,
       r.current_run_id,
       l.status AS run_status,
       l.rules_version,
       l.started_at AS run_started_at,
       l.finished_at AS run_finished_at,
       r.source_batch_id,
       r.source_batch_started_at,
       r.source_batch_finished_at,
       r.cohort_created_since,
       r.cohort_source,
       r.cohort_evidence,
       r.verified_issue_count,
       coalesce(i.published_issue_count, 0) AS published_issue_count,
       i.min_issue_observed_at,
       i.max_issue_observed_at,
       i.history_checked_from,
       i.history_checked_to,
       h.reporting_horizon,
       (h.reporting_horizon AT TIME ZONE 'UTC')::date - 1 AS last_complete_report_day,
       (l.status = 'succeeded'
        AND l.rules_version = '2'
        AND r.source_batch_started_at IS NOT NULL
        AND r.source_batch_finished_at IS NOT NULL
        AND r.cohort_created_since = TIMESTAMPTZ '2023-01-01 00:00:00+00'
        AND r.cohort_source IN ('recorded', 'attested')
        AND coalesce(i.published_issue_count, 0) = r.verified_issue_count
        AND coalesce(i.issues_without_history_context, 0) = 0) AS ready_for_cdm
FROM dds.repositories r
JOIN dds.load_runs l ON l.run_id = r.current_run_id
JOIN horizon h USING (repository_id)
LEFT JOIN issue_context i USING (repository_id);

-- Validates a historical cut-off: it may not exceed the minimum reporting horizon
-- of the selected repositories (NULL = all published). valid_to IS NULL after
-- the horizon is the last known status, not a forecast.
CREATE OR REPLACE FUNCTION dds.report_cutoff(p_at TIMESTAMPTZ, p_repository_ids BIGINT[] DEFAULT NULL)
RETURNS TIMESTAMPTZ
LANGUAGE plpgsql STABLE AS $$
DECLARE
    selected_count INTEGER;
    expected_count INTEGER;
    horizon TIMESTAMPTZ;
BEGIN
    IF p_at IS NULL THEN
        RAISE EXCEPTION 'report cut-off is required' USING ERRCODE = '22023';
    END IF;
    SELECT count(*), min(reporting_horizon) INTO selected_count, horizon
    FROM dds.publication_manifest
    WHERE p_repository_ids IS NULL OR repository_id = ANY (p_repository_ids);
    expected_count := CASE WHEN p_repository_ids IS NULL THEN selected_count
                           ELSE (SELECT count(DISTINCT value) FROM unnest(p_repository_ids) AS value) END;
    IF selected_count = 0 OR selected_count <> expected_count THEN
        RAISE EXCEPTION 'report repositories are not published' USING ERRCODE = '22023';
    END IF;
    IF p_at > horizon THEN
        RAISE EXCEPTION 'report cut-off % is after reporting horizon %', p_at, horizon
            USING ERRCODE = '22023';
    END IF;
    RETURN p_at;
END
$$;

-- End of a complete UTC day: the next midnight, allowed only up to the horizon.
CREATE OR REPLACE FUNCTION dds.report_day_end(p_day DATE, p_repository_ids BIGINT[] DEFAULT NULL)
RETURNS TIMESTAMPTZ
LANGUAGE sql STABLE AS $$
    SELECT dds.report_cutoff((p_day + 1)::timestamp AT TIME ZONE 'UTC', p_repository_ids)
$$;
