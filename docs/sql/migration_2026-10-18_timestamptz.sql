-- R2 (2026-10-18): TIMESTAMP (KST wall clock, no zone) -> TIMESTAMPTZ
-- Existing values were written by the app as KST LocalDateTime, so interpret them as Asia/Seoul.
-- Verified before migration: labeled_at (timestamptz) vs transactions.created_at differ by 0~0.2 s
-- for all labeled rows when created_at is read as KST.
-- Apply with the API and simulator stopped. ALTER TYPE rewrites the table under an ACCESS EXCLUSIVE lock.
BEGIN;

ALTER TABLE transactions
    ALTER COLUMN "timestamp" TYPE timestamptz USING "timestamp" AT TIME ZONE 'Asia/Seoul',
    ALTER COLUMN created_at  TYPE timestamptz USING created_at  AT TIME ZONE 'Asia/Seoul';

ALTER TABLE fraud_results
    ALTER COLUMN created_at  TYPE timestamptz USING created_at  AT TIME ZONE 'Asia/Seoul';

ALTER TABLE model_metadata
    ALTER COLUMN created_at  TYPE timestamptz USING created_at  AT TIME ZONE 'Asia/Seoul',
    ALTER COLUMN deployed_at TYPE timestamptz USING deployed_at AT TIME ZONE 'Asia/Seoul';

COMMIT;
