-- 2026-10-19 Apply the missing FRAUD_RESULTS(TRANSACTION_ID) index to the running DB.
-- Background: migration_2026-10-09_shadow_results.sql and schema.sql define
--   IX_FRAUD_RESULTS_TRANSACTION, and the 10/9 work log records it as applied,
--   but on 2026-10-19 pg_indexes showed only pk_fraud_results on fraud_results.
--   ROLE column, CHECK, UNIQUE and all foreign keys were present. Cause not identified.
-- Purpose: champion/challenger join by transaction id (ml/report_shadow.py).
--   PostgreSQL does not create indexes for foreign keys automatically.
-- Rollback: DROP INDEX IF EXISTS ix_fraud_results_transaction;
CREATE INDEX IF NOT EXISTS ix_fraud_results_transaction ON fraud_results (transaction_id);
