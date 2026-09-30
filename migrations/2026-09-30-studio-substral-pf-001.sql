-- SPEC-SUBSTRAL-PF-001 — Studio Substral assessment opportunities (tenant-bound)

CREATE TABLE IF NOT EXISTS studio_substral_assessment_opportunities (
  id BIGSERIAL PRIMARY KEY,
  client_id INTEGER NOT NULL REFERENCES clients(id),
  agent_action_id UUID REFERENCES agent_actions(id),
  company_id UUID REFERENCES companies(id),
  prospect_id UUID REFERENCES prospects(id),
  mission_id TEXT,
  domain TEXT NOT NULL,
  request_key TEXT,
  stage TEXT NOT NULL DEFAULT 'REQUESTED',
  outcome TEXT,
  decision_context TEXT,
  evidence_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
  six_layer_findings JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence_class TEXT,
  observed_constraint TEXT,
  recommended_next_action TEXT,
  source TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS studio_substral_assessment_opportunities_client_idx
  ON studio_substral_assessment_opportunities (client_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS studio_substral_assessment_opportunities_domain_idx
  ON studio_substral_assessment_opportunities (client_id, domain);

CREATE UNIQUE INDEX IF NOT EXISTS studio_substral_assessment_request_key_uidx
  ON studio_substral_assessment_opportunities (client_id, request_key)
  WHERE request_key IS NOT NULL;
