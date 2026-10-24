-- Apply separately through the approved migration workflow. No runtime DDL.
CREATE TABLE IF NOT EXISTS scout_query_checkpoints (
  tenant_id text NOT NULL,
  search_fingerprint text NOT NULL,
  query_key text NOT NULL,
  plan_version text NOT NULL,
  state text NOT NULL CHECK (state IN ('fetched', 'handled')),
  candidates jsonb NOT NULL CHECK (jsonb_typeof(candidates) = 'array'),
  fetched_at timestamptz NOT NULL DEFAULT now(),
  handled_at timestamptz,
  PRIMARY KEY (tenant_id, search_fingerprint, query_key),
  CHECK ((state = 'handled') = (handled_at IS NOT NULL))
);
