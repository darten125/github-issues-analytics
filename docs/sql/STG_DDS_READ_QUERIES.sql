-- Read-only STG -> DDS examples. Run in psql with -v issue_id=2159200356
-- and -v batch_id= (latest run per repository), or pass the exact Airflow
-- batch_id to check that run. Do not count snapshots or observations as
-- distinct GitHub objects.

-- 1. Unique repositories; keep the most recently observed JSON version.
SELECT DISTINCT ON (r.repository_id)
       r.repository_id, r.full_name, r.raw_json, o.observed_at
FROM stg.repository_observations o
JOIN stg.repositories_raw r ON r.snapshot_id = o.snapshot_id
ORDER BY r.repository_id, o.observed_at DESC, o.observation_id DESC;

-- 2. Current observed snapshot for each unique Issue ID.
SELECT DISTINCT ON (i.issue_id)
       i.issue_id, i.repository_id, i.issue_number, i.raw_json,
       o.observed_at, i.snapshot_id
FROM stg.issue_observations o
JOIN stg.issues_raw i ON i.snapshot_id = o.snapshot_id
ORDER BY i.issue_id, o.observed_at DESC, o.observation_id DESC,
         i.snapshot_id DESC;

-- 3. One latest observed JSON version per global GitHub event ID.
SELECT DISTINCT ON (e.event_id)
       e.event_id, e.issue_id, e.repository_id, e.event_type,
       e.github_created_at, e.raw_json, o.observed_at, e.snapshot_id
FROM stg.event_observations o
JOIN stg.issue_events_raw e ON e.snapshot_id = o.snapshot_id
ORDER BY e.event_id, o.observed_at DESC, o.observation_id DESC,
         e.snapshot_id DESC;

-- 4. Status-relevant events for one Issue. Conversion is surfaced but
-- interpreting it as a closure requires an explicit DDS business rule.
WITH latest_events AS (
    SELECT DISTINCT ON (e.event_id)
           e.event_id, e.issue_id, e.repository_id, e.event_type,
           e.github_created_at, e.raw_json
    FROM stg.event_observations o
    JOIN stg.issue_events_raw e ON e.snapshot_id = o.snapshot_id
    WHERE e.issue_id = :issue_id
    ORDER BY e.event_id, o.observed_at DESC, o.observation_id DESC,
             e.snapshot_id DESC
)
SELECT * FROM latest_events
WHERE event_type IN ('closed', 'reopened', 'converted_to_discussion')
ORDER BY github_created_at, event_id;

-- 5. Successful /events pagination for the Issue, freshness against the
-- latest Issue updated_at, and the state of its most recent attempt. An
-- earlier successful sample is valid when a later verified backfill skipped
-- that Issue's unchanged history. A later failed attempt is shown separately.
WITH latest_issue AS (
    SELECT DISTINCT ON (i.issue_id)
           i.issue_id, i.repository_id,
           (i.raw_json->>'updated_at')::timestamptz AS current_updated_at
    FROM stg.issue_observations o
    JOIN stg.issues_raw i ON i.snapshot_id = o.snapshot_id
    WHERE i.issue_id = :issue_id
    ORDER BY i.issue_id, o.observed_at DESC, o.observation_id DESC,
             i.snapshot_id DESC
), latest_success AS (
    SELECT DISTINCT ON (issue_id)
           issue_id, batch_id, issue_updated_at, finished_at, events_fetched
    FROM stg.issue_event_loads
    WHERE issue_id = :issue_id AND status = 'succeeded'
    ORDER BY issue_id, finished_at DESC NULLS LAST, batch_id DESC
), latest_attempt AS (
    SELECT DISTINCT ON (issue_id)
           issue_id, batch_id, status, started_at
    FROM stg.issue_event_loads
    WHERE issue_id = :issue_id
    ORDER BY issue_id, started_at DESC, batch_id DESC
)
SELECT i.issue_id, i.repository_id,
       (s.issue_id IS NOT NULL) AS history_loaded,
       i.current_updated_at, s.issue_updated_at AS loaded_updated_at,
       (i.current_updated_at IS NOT NULL AND
        i.current_updated_at = s.issue_updated_at) AS timestamp_matches,
       a.status AS latest_attempt_status,
       (s.issue_id IS NOT NULL AND i.current_updated_at IS NOT NULL AND
        i.current_updated_at = s.issue_updated_at AND
        a.status = 'succeeded') AS ready_by_recorded_status,
       s.batch_id AS successful_history_batch_id, s.finished_at,
       s.events_fetched
FROM latest_issue i
LEFT JOIN latest_success s USING (issue_id)
LEFT JOIN latest_attempt a USING (issue_id);

-- 6. Readiness of the requested batch, or of the latest run per repository
-- when batch_id is empty. Rank all runs before checking readiness: a newer
-- failed, running, sample, or unverified run cannot fall back to an older one.
WITH requested AS (
    SELECT NULLIF(:'batch_id', '')::uuid AS batch_id
), ranked AS (
    SELECT b.*,
           row_number() OVER (
               PARTITION BY b.repository_full_name
               ORDER BY b.started_at DESC, b.batch_id DESC
           ) AS recency
    FROM stg.load_batches b
), selected AS (
    SELECT b.*
    FROM ranked b
    CROSS JOIN requested r
    WHERE (r.batch_id IS NOT NULL AND b.batch_id = r.batch_id)
       OR (r.batch_id IS NULL AND b.recency = 1)
)
SELECT b.repository_full_name, b.batch_id, b.started_at, b.finished_at,
       b.status, b.issue_scan_status, b.issue_scan_passes,
       b.issue_scan_issue_count,
       (b.status = 'succeeded'
        AND b.issue_scan_status = 'verified'
        AND b.issue_scan_passes >= 2
        AND NOT EXISTS (
            SELECT 1 FROM stg.issue_event_loads l
            WHERE l.batch_id = b.batch_id AND l.status <> 'succeeded'
        )) AS ready
FROM selected b
ORDER BY b.repository_full_name;

-- 7. All distinct events for one Issue in deterministic GitHub-time order.
-- This is a sequence, not an SCD2 interval table.
WITH latest_events AS (
    SELECT DISTINCT ON (e.event_id)
           e.event_id, e.issue_id, e.event_type, e.github_created_at
    FROM stg.event_observations o
    JOIN stg.issue_events_raw e ON e.snapshot_id = o.snapshot_id
    WHERE e.issue_id = :issue_id
    ORDER BY e.event_id, o.observed_at DESC, o.observation_id DESC,
             e.snapshot_id DESC
)
SELECT row_number() OVER (ORDER BY github_created_at, event_id) AS event_order,
       issue_id, event_id, event_type, github_created_at
FROM latest_events
ORDER BY github_created_at, event_id;

-- 8. Read-only check for a closed Issue without a 'closed' event but with
-- 'converted_to_discussion'. This returned Issue 2159200356, number 1232.
WITH latest_issues AS (
    SELECT DISTINCT ON (i.issue_id)
           i.issue_id, i.issue_number, i.repository_id,
           i.raw_json->>'state' AS state,
           i.raw_json->>'closed_at' AS closed_at
    FROM stg.issue_observations o
    JOIN stg.issues_raw i ON i.snapshot_id = o.snapshot_id
    ORDER BY i.issue_id, o.observed_at DESC, o.observation_id DESC,
             i.snapshot_id DESC
), latest_events AS (
    SELECT DISTINCT ON (e.event_id)
           e.event_id, e.issue_id, e.repository_id, e.event_type
    FROM stg.event_observations o
    JOIN stg.issue_events_raw e ON e.snapshot_id = o.snapshot_id
    ORDER BY e.event_id, o.observed_at DESC, o.observation_id DESC,
             e.snapshot_id DESC
)
SELECT i.issue_id, i.issue_number, i.state, i.closed_at,
       count(e.event_id) FILTER (WHERE e.event_type = 'closed') AS closed_events,
       count(e.event_id) FILTER (WHERE e.event_type = 'converted_to_discussion')
           AS conversion_events
FROM latest_issues i
LEFT JOIN latest_events e ON e.issue_id = i.issue_id
                         AND e.repository_id = i.repository_id
WHERE EXISTS (SELECT 1 FROM stg.repositories_raw r
              WHERE r.repository_id = i.repository_id
                AND r.full_name = 'android/nowinandroid')
  AND i.state = 'closed'
GROUP BY i.issue_id, i.issue_number, i.state, i.closed_at
HAVING count(e.event_id) FILTER (WHERE e.event_type = 'closed') = 0
   AND count(e.event_id) FILTER (WHERE e.event_type = 'converted_to_discussion') > 0
ORDER BY i.issue_id;
