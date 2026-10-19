-- Read-only acceptance queries. Run in one REPEATABLE READ READ ONLY transaction
-- after src.cdm.cli validate. No STG tables are read.

-- The active CDM source must be the ready DDS publication of each repository.
SELECT m.full_name, m.ready_for_cdm,
       m.source_batch_id = c.source_batch_id AS same_batch,
       m.current_run_id = c.dds_run_id AS same_dds_run,
       m.last_complete_report_day, c.common_last_day
FROM dds.publication_manifest m
JOIN cdm.repository_snapshot c USING (repository_id)
ORDER BY m.full_name;

-- Unique Issue counts must match per repository.
SELECT r.full_name,
       (SELECT count(*) FROM dds.issues i WHERE i.repository_id=r.repository_id) AS dds_issues,
       (SELECT count(*) FROM cdm.issue_lifecycle i WHERE i.repository_id=r.repository_id) AS cdm_issues
FROM cdm.repository_snapshot r ORDER BY r.full_name;

-- Historical open/closed counts at the common last complete day.
WITH cutoff AS (
    SELECT common_last_day AS day,
           (common_last_day + 1)::timestamp AT TIME ZONE 'UTC' AS at
    FROM cdm.current_state
), dds_counts AS (
    SELECT i.repository_id,
           count(*) FILTER (WHERE v.status='open') AS open_issues,
           count(*) FILTER (WHERE v.status='closed') AS closed_issues
    FROM cutoff p
    JOIN dds.issues i ON i.created_at < p.at
    JOIN dds.issue_status_versions v ON v.issue_id=i.issue_id
         AND v.valid_from < p.at AND (v.valid_to IS NULL OR v.valid_to >= p.at)
    GROUP BY i.repository_id
), cdm_counts AS (
    SELECT d.repository_id,
           count(*) FILTER (WHERE d.status_eod='open') AS open_issues,
           count(*) FILTER (WHERE d.status_eod='closed') AS closed_issues
    FROM cdm.issue_day d, cutoff p
    WHERE d.report_day=p.day
    GROUP BY d.repository_id
)
SELECT r.full_name, coalesce(dds_counts.open_issues,0) AS dds_open,
       coalesce(cdm_counts.open_issues,0) AS cdm_open,
       coalesce(dds_counts.closed_issues,0) AS dds_closed,
       coalesce(cdm_counts.closed_issues,0) AS cdm_closed
FROM cdm.repository_snapshot r
LEFT JOIN dds_counts USING (repository_id)
LEFT JOIN cdm_counts USING (repository_id)
ORDER BY r.full_name;

-- Transition counts before the same cut-off, including same-second cycles.
WITH cutoff AS (
    SELECT (common_last_day + 1)::timestamp AT TIME ZONE 'UTC' AS at
    FROM cdm.current_state
), dds_lifecycle AS (
    SELECT e.repository_id,
           count(DISTINCT e.issue_id) FILTER (WHERE e.status_after='closed') AS ever_closed,
           count(DISTINCT e.issue_id) FILTER (WHERE e.status_after='open') AS ever_reopened
    FROM dds.issue_events e, cutoff p
    WHERE e.is_status_transition AND e.github_created_at < p.at
    GROUP BY e.repository_id
), cdm_lifecycle AS (
    SELECT i.repository_id,
           count(*) FILTER (WHERE i.first_closed_at < p.at) AS ever_closed,
           count(*) FILTER (WHERE i.first_reopened_at < p.at) AS ever_reopened
    FROM cdm.issue_lifecycle i, cutoff p
    GROUP BY i.repository_id
)
SELECT r.full_name, coalesce(d.ever_closed,0) AS dds_ever_closed,
       coalesce(c.ever_closed,0) AS cdm_ever_closed,
       coalesce(d.ever_reopened,0) AS dds_ever_reopened,
       coalesce(c.ever_reopened,0) AS cdm_ever_reopened
FROM cdm.repository_snapshot r
LEFT JOIN dds_lifecycle d USING (repository_id)
LEFT JOIN cdm_lifecycle c USING (repository_id)
ORDER BY r.full_name;

-- Current label links must match exactly, independent of report-day filters.
SELECT r.full_name,
       (SELECT count(*) FROM dds.issue_current_labels l
        WHERE l.repository_id=r.repository_id) AS dds_label_links,
       (SELECT count(*) FROM cdm.current_issue_label l
        WHERE l.repository_id=r.repository_id) AS cdm_label_links
FROM cdm.repository_snapshot r ORDER BY r.full_name;

-- A small review list: reopened, converted, and midnight-adjacent Issues.
SELECT r.full_name, d.issue_number, d.issue_id,
       i.first_closed_at, i.first_reopened_at,
       e.event_type, e.github_created_at,
       h.status_eod AS status_on_common_last_day
FROM cdm.issue_lifecycle i
JOIN cdm.repository_snapshot r USING (repository_id)
JOIN dds.issues d USING (issue_id)
LEFT JOIN LATERAL (
    SELECT event_type, github_created_at FROM dds.issue_events
    WHERE issue_id=i.issue_id AND is_status_transition
    ORDER BY github_created_at DESC, event_id DESC LIMIT 1
) e ON TRUE
LEFT JOIN cdm.issue_day h ON h.issue_id=i.issue_id AND h.report_day=r.common_last_day
WHERE i.first_reopened_at IS NOT NULL OR e.event_type='converted_to_discussion'
ORDER BY (e.event_type='converted_to_discussion') DESC, r.full_name, d.issue_number LIMIT 12;
