-- SPEC-259 — Opportunity-level paid attribution provenance snapshot.
ALTER TABLE opportunities
  ADD COLUMN IF NOT EXISTS attribution_metadata JSONB;

COMMENT ON COLUMN opportunities.attribution_metadata IS
  'Read-only snapshot of first-party paid attribution provenance at opportunity admission (SPEC-259).';
