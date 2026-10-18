CREATE TABLE IF NOT EXISTS dds.load_runs (
    run_id UUID PRIMARY KEY,
    rules_version TEXT NOT NULL,
    source_batch_ids JSONB NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
    started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    finished_at TIMESTAMPTZ,
    result JSONB,
    error_code TEXT
);

CREATE TABLE IF NOT EXISTS dds.quality_issues (
    quality_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES dds.load_runs(run_id),
    severity TEXT NOT NULL CHECK (severity IN ('warning', 'error')),
    code TEXT NOT NULL,
    issue_id BIGINT,
    repository_id BIGINT,
    details JSONB NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS quality_issues_run_idx ON dds.quality_issues (run_id);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conrelid = 'dds.repositories'::regclass
                     AND conname = 'repositories_current_run_fk') THEN
        ALTER TABLE dds.repositories ADD CONSTRAINT repositories_current_run_fk
            FOREIGN KEY (current_run_id) REFERENCES dds.load_runs(run_id);
    END IF;
END $$;
