-- Additive STG v3 migration. Existing rows retain their snapshots and statuses.
ALTER TABLE stg.issue_event_loads
    DROP CONSTRAINT IF EXISTS issue_event_loads_mode_check;
ALTER TABLE stg.issue_event_loads
    ADD CONSTRAINT issue_event_loads_mode_check
    CHECK (mode IN ('backfill', 'sync', 'audit'));

ALTER TABLE stg.load_batches
    ADD COLUMN IF NOT EXISTS load_mode TEXT NOT NULL DEFAULT 'legacy';
ALTER TABLE stg.load_batches
    ADD COLUMN IF NOT EXISTS issue_scan_status TEXT NOT NULL DEFAULT 'legacy';
ALTER TABLE stg.load_batches
    ADD COLUMN IF NOT EXISTS issue_scan_passes INTEGER NOT NULL DEFAULT 0;
ALTER TABLE stg.load_batches
    ADD COLUMN IF NOT EXISTS issue_scan_issue_count BIGINT NOT NULL DEFAULT 0;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'stg.load_batches'::regclass
          AND conname = 'load_batches_load_mode_check'
    ) THEN
        ALTER TABLE stg.load_batches ADD CONSTRAINT load_batches_load_mode_check
            CHECK (load_mode IN ('legacy', 'backfill', 'sync', 'audit'));
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'stg.load_batches'::regclass
          AND conname = 'load_batches_issue_scan_status_check'
    ) THEN
        ALTER TABLE stg.load_batches ADD CONSTRAINT load_batches_issue_scan_status_check
            CHECK (issue_scan_status IN ('legacy', 'sample', 'verified', 'unverified'));
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'stg.load_batches'::regclass
          AND conname = 'load_batches_issue_scan_passes_check'
    ) THEN
        ALTER TABLE stg.load_batches ADD CONSTRAINT load_batches_issue_scan_passes_check
            CHECK (issue_scan_passes >= 0 AND issue_scan_issue_count >= 0);
    END IF;
END $$;
