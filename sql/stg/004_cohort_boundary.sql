-- Additive STG v4 migration: the actual Issue creation boundary of each batch.
-- Existing batches stay 'unknown'; they are never relabelled automatically.
-- 'recorded' is written by the loader at batch start, 'attested' only by the
-- explicit attest-cohort command with a reference to the saved run configuration.
ALTER TABLE stg.load_batches
    ADD COLUMN IF NOT EXISTS created_since TIMESTAMPTZ;
ALTER TABLE stg.load_batches
    ADD COLUMN IF NOT EXISTS created_since_source TEXT NOT NULL DEFAULT 'unknown';
ALTER TABLE stg.load_batches
    ADD COLUMN IF NOT EXISTS created_since_evidence TEXT;
ALTER TABLE stg.load_batches
    ADD COLUMN IF NOT EXISTS created_since_attested_at TIMESTAMPTZ;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'stg.load_batches'::regclass
          AND conname = 'load_batches_created_since_check'
    ) THEN
        ALTER TABLE stg.load_batches ADD CONSTRAINT load_batches_created_since_check CHECK (
            (created_since_source = 'unknown' AND created_since IS NULL
             AND created_since_evidence IS NULL AND created_since_attested_at IS NULL)
            OR (created_since_source = 'recorded' AND created_since IS NOT NULL
                AND created_since_evidence IS NULL AND created_since_attested_at IS NULL)
            OR (created_since_source = 'attested' AND created_since IS NOT NULL
                AND btrim(coalesce(created_since_evidence, '')) <> ''
                AND created_since_attested_at IS NOT NULL)
        );
    END IF;
END $$;
