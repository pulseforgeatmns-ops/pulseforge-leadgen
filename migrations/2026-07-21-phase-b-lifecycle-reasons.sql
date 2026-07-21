-- Phase B — structured lifecycle reasons (additive only).
--
-- Adds a structured reason code to the canonical lifecycle event stream so
-- non-terminal outcomes stop being collapsed into permanent Dead semantics:
--   nurture              — alive; re-surfaces on a long-dated callback
--   data_remediation     — alive; contact data needs repair (e.g. new phone)
--   terminal_suppression — do-not-call; dead AND globally suppressed
--
-- No legacy field is touched. The canonical stages are unchanged.
-- Runtime reconcile: utils/lifecycleSchema.js applies the same DDL idempotently.

BEGIN;

ALTER TABLE prospect_lifecycle_events
  ADD COLUMN IF NOT EXISTS lifecycle_reason TEXT
  CHECK (lifecycle_reason IS NULL OR lifecycle_reason IN ('nurture', 'data_remediation', 'terminal_suppression'));

COMMIT;
