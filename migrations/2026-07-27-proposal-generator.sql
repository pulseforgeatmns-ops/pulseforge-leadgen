-- SPEC-027B Proposal Generator — proposal version store
-- Durable versions attached to opportunity / mission for learning loop.

CREATE TABLE IF NOT EXISTS proposal_versions (
  id                BIGSERIAL PRIMARY KEY,
  opportunity_id    TEXT,
  mission_id        TEXT,
  client_id         INTEGER,
  tenant_id         TEXT NOT NULL DEFAULT '',
  version           INTEGER NOT NULL DEFAULT 1,
  status            TEXT NOT NULL DEFAULT 'review',
  discovery_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
  discovery_profile_id TEXT,
  pricing_package_id TEXT NOT NULL DEFAULT 'setup_monthly',
  document          JSONB NOT NULL DEFAULT '{}'::jsonb,
  html              TEXT,
  accepted_changes  JSONB NOT NULL DEFAULT '[]'::jsonb,
  client_outcome    TEXT,
  win_loss          TEXT,
  feedback          TEXT,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT proposal_versions_status_check
    CHECK (status IN ('draft', 'review', 'approved', 'sent', 'won', 'lost')),
  CONSTRAINT proposal_versions_win_loss_check
    CHECK (win_loss IS NULL OR win_loss IN ('win', 'loss'))
);

CREATE INDEX IF NOT EXISTS idx_proposal_versions_opportunity
  ON proposal_versions (opportunity_id);

CREATE INDEX IF NOT EXISTS idx_proposal_versions_mission
  ON proposal_versions (mission_id);

CREATE INDEX IF NOT EXISTS idx_proposal_versions_client
  ON proposal_versions (client_id);

CREATE INDEX IF NOT EXISTS idx_proposal_versions_tenant_created
  ON proposal_versions (tenant_id, created_at DESC);
