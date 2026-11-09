-- Operator-applied, additive; no startup DDL. Default-off observer only.
BEGIN;
CREATE TABLE IF NOT EXISTS max_interpretation_shadow (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL,
  observation JSONB NOT NULL CHECK (observation->>'shadowOnly' = 'true')
);
CREATE INDEX IF NOT EXISTS max_interpretation_shadow_tenant_recent
  ON max_interpretation_shadow (tenant_id, observed_at DESC);
COMMIT;
-- Retention: governed maintenance deletes observations older than 90 days.
