-- Reference definitions of the six handoff metrics, read-only over DDS.
-- Not a CDM implementation: the grain and marts belong to participant 3.
-- Run in one REPEATABLE READ, READ ONLY transaction. Change only the date
-- literals in params. The report day must be complete: dds.report_day_end
-- raises an error after the reporting horizon. Only publications that are
-- ready_for_cdm are read. Group totals come from summed numerators and
-- denominators; a zero denominator yields NULL, not zero.

-- 1. New issues: unique Issues by UTC creation day, from month_start to report_day.
WITH params AS (
    SELECT DATE '2023-03-01' AS month_start, dds.report_day_end(DATE '2023-03-31') AS cutoff
)
SELECT m.full_name, (i.created_at AT TIME ZONE 'UTC')::date AS created_day,
       count(DISTINCT i.issue_id) AS new_issues
FROM params p
JOIN dds.issues i ON i.created_at >= p.month_start::timestamp AT TIME ZONE 'UTC'
    AND i.created_at < p.cutoff
JOIN dds.publication_manifest m ON m.repository_id = i.repository_id AND m.ready_for_cdm
GROUP BY m.full_name, created_day
ORDER BY m.full_name, created_day;

-- 2-3. Closed share and open Issues at the end of report_day (SCD2 state just
-- before the next UTC midnight). Share = closed / all cohort Issues created by then.
WITH params AS (
    SELECT dds.report_day_end(DATE '2023-03-31') AS cutoff
), state AS (
    SELECT m.full_name, i.issue_id, v.status
    FROM params p
    JOIN dds.issues i ON i.created_at < p.cutoff
    JOIN dds.publication_manifest m ON m.repository_id = i.repository_id AND m.ready_for_cdm
    JOIN dds.issue_status_versions v ON v.issue_id = i.issue_id
        AND v.valid_from < p.cutoff AND (v.valid_to IS NULL OR v.valid_to >= p.cutoff)
)
SELECT coalesce(full_name, 'ALL') AS repository,
       count(DISTINCT issue_id) AS issues_created,
       count(DISTINCT issue_id) FILTER (WHERE status = 'closed') AS closed_issues,
       count(DISTINCT issue_id) FILTER (WHERE status = 'open') AS open_issues,
       round(count(DISTINCT issue_id) FILTER (WHERE status = 'closed')::numeric
             / nullif(count(DISTINCT issue_id), 0), 4) AS closed_share
FROM state
GROUP BY ROLLUP (full_name)
ORDER BY full_name NULLS LAST;

-- 4. Time to first closure: median of individual durations among Issues first
-- closed before the cut-off (cumulative), and separately among those first closed
-- within the month. Medians of groups are never averaged.
WITH params AS (
    SELECT DATE '2023-03-01' AS month_start, dds.report_day_end(DATE '2023-03-31') AS cutoff
), first_close AS (
    SELECT i.issue_id, i.repository_id,
           extract(epoch FROM min(e.github_created_at) - i.created_at) / 86400 AS days,
           min(e.github_created_at) >= p.month_start::timestamp AT TIME ZONE 'UTC' AS in_month
    FROM params p
    JOIN dds.issues i ON i.created_at < p.cutoff
    JOIN dds.issue_events e ON e.issue_id = i.issue_id AND e.is_status_transition
        AND e.status_after = 'closed' AND e.github_created_at < p.cutoff
    GROUP BY i.issue_id, i.repository_id, i.created_at, p.month_start
)
SELECT coalesce(m.full_name, 'ALL') AS repository,
       count(f.issue_id) AS first_closed_issues,
       round((percentile_cont(0.5) WITHIN GROUP (ORDER BY f.days))::numeric, 4) AS median_days_cumulative,
       count(f.issue_id) FILTER (WHERE f.in_month) AS first_closed_in_month,
       round((percentile_cont(0.5) WITHIN GROUP (ORDER BY f.days) FILTER (WHERE f.in_month))::numeric, 4)
           AS median_days_in_month
FROM dds.publication_manifest m
LEFT JOIN first_close f ON f.repository_id = m.repository_id
WHERE m.ready_for_cdm
GROUP BY ROLLUP (m.full_name)
ORDER BY m.full_name NULLS LAST;

-- 5. Reopening: Issues ever reopened / Issues ever closed, both before the same cut-off.
WITH params AS (
    SELECT dds.report_day_end(DATE '2023-03-31') AS cutoff
), lifecycle AS (
    SELECT m.full_name, i.issue_id,
           bool_or(e.status_after = 'closed') AS ever_closed,
           bool_or(e.status_after = 'open') AS ever_reopened
    FROM params p
    JOIN dds.issues i ON i.created_at < p.cutoff
    JOIN dds.publication_manifest m ON m.repository_id = i.repository_id AND m.ready_for_cdm
    LEFT JOIN dds.issue_events e ON e.issue_id = i.issue_id AND e.is_status_transition
        AND e.github_created_at < p.cutoff
    GROUP BY m.full_name, i.issue_id
)
SELECT coalesce(full_name, 'ALL') AS repository,
       count(*) FILTER (WHERE ever_closed) AS ever_closed,
       count(*) FILTER (WHERE ever_reopened) AS ever_reopened,
       round(count(*) FILTER (WHERE ever_reopened)::numeric
             / nullif(count(*) FILTER (WHERE ever_closed), 0), 4) AS reopen_rate
FROM lifecycle
GROUP BY ROLLUP (full_name)
ORDER BY full_name NULLS LAST;

-- 6. Current labels: unique Issues inside each label of the current snapshot.
-- Not a historical state; one Issue with several labels counts once per label.
SELECT m.full_name, l.name AS label, count(DISTINCT cl.issue_id) AS issues
FROM dds.issue_current_labels cl
JOIN dds.labels l ON l.label_id = cl.label_id
JOIN dds.publication_manifest m ON m.repository_id = cl.repository_id AND m.ready_for_cdm
GROUP BY m.full_name, l.name
ORDER BY m.full_name, l.name;
