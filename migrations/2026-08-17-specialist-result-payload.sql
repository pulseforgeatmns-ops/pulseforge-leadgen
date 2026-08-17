-- SPEC-101 — persist specialist result payload so investigation provenance
-- and consumed context survive process restart. Additive.
-- Rollback: migrations/2026-08-17-specialist-result-payload.rollback.sql

ALTER TABLE specialist_results
  ADD COLUMN IF NOT EXISTS payload JSONB NOT NULL DEFAULT '{}'::jsonb;
