-- Read-only DDS contracts. Change only example timestamp/date literals.
-- Run all queries in a single REPEATABLE READ, READ ONLY transaction when a
-- consistent view across queries is required. All calendar boundaries use UTC.
-- Historical cut-offs pass dds.report_cutoff/dds.report_day_end: a moment after
-- the reporting horizon of dds.publication_manifest raises an error. An open last
-- interval (valid_to IS NULL) is the last known status, not a forecast.

-- 1. State at an instant: intervals are [valid_from, valid_to).
WITH parameters AS (
    SELECT dds.report_cutoff(TIMESTAMPTZ '2024-03-11T22:47:34Z') AS at
)
SELECT i.issue_id, i.repository_id, i.issue_number, v.status,
       v.valid_from, v.valid_to
FROM parameters p
JOIN dds.issues i ON i.created_at <= p.at
JOIN dds.issue_status_versions v ON v.issue_id = i.issue_id
    AND v.valid_from <= p.at
    AND (v.valid_to IS NULL OR p.at < v.valid_to)
ORDER BY i.repository_id, i.issue_number;

-- 2. End of UTC day: state just before the next midnight.
-- A transition exactly at that midnight belongs to the following day.
WITH parameters AS (
    SELECT dds.report_day_end(DATE '2024-03-11') AS until
)
SELECT i.issue_id, i.repository_id, i.issue_number, v.status
FROM parameters p
JOIN dds.issues i ON i.created_at < p.until
JOIN dds.issue_status_versions v ON v.issue_id = i.issue_id
    AND v.valid_from < p.until
    AND (v.valid_to IS NULL OR v.valid_to >= p.until)
ORDER BY i.repository_id, i.issue_number;

-- 3. First actual closure of each Issue, including conversion to Discussion.
-- Events preserve closure/reopening at one instant even when intervals coalesce.
SELECT i.issue_id, i.repository_id, i.issue_number,
       min(e.github_created_at) AS first_closed_at
FROM dds.issues i
LEFT JOIN dds.issue_events e ON e.issue_id = i.issue_id
    AND e.is_status_transition AND e.status_after = 'closed'
GROUP BY i.issue_id, i.repository_id, i.issue_number
ORDER BY i.repository_id, i.issue_number;

-- 4. Ever reopened through an instant; repeated ineffective reopened is excluded.
WITH parameters AS (
    SELECT dds.report_cutoff(TIMESTAMPTZ '2024-03-12T00:00:00Z') AS at
)
SELECT i.issue_id, i.repository_id, i.issue_number,
       EXISTS (
           SELECT 1 FROM dds.issue_events e
           WHERE e.issue_id = i.issue_id AND e.is_status_transition
             AND e.status_after = 'open' AND e.github_created_at <= p.at
       ) AS was_reopened
FROM parameters p
JOIN dds.issues i ON i.created_at <= p.at
ORDER BY i.repository_id, i.issue_number;

-- 5. Current labels: one row per Issue/label; unlabeled Issues have NULL label.
SELECT i.issue_id, i.repository_id, i.issue_number,
       l.label_id, l.name, l.color
FROM dds.issues i
LEFT JOIN dds.issue_current_labels il ON il.issue_id = i.issue_id
LEFT JOIN dds.labels l ON l.label_id = il.label_id
ORDER BY i.repository_id, i.issue_number, l.label_id;

-- 6. Current assignees: separate grain avoids multiplying labels by assignees.
SELECT i.issue_id, i.repository_id, i.issue_number, u.user_id, u.login
FROM dds.issues i
LEFT JOIN dds.issue_current_assignees ia ON ia.issue_id = i.issue_id
LEFT JOIN dds.users u ON u.user_id = ia.user_id
ORDER BY i.repository_id, i.issue_number, u.user_id;

-- 7. Publication manifest, read from DDS only: run, source batch, cohort,
-- history check freshness, reporting horizon and readiness for the final CDM.
SELECT repository_id, full_name, current_run_id, run_status, rules_version,
       source_batch_id, source_batch_finished_at, cohort_created_since, cohort_source,
       verified_issue_count, published_issue_count, history_checked_from, history_checked_to,
       reporting_horizon, last_complete_report_day, ready_for_cdm
FROM dds.publication_manifest
ORDER BY full_name;

-- 8. Diagnostics; no raw GitHub payloads or connection credentials.
SELECT run_id, severity, code, issue_id, repository_id, details, recorded_at
FROM dds.quality_issues
ORDER BY recorded_at DESC, quality_id DESC;
