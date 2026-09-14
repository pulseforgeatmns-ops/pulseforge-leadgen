-- SPEC-251 — Max OBSERVE reaction policy (append-only reactions + latest candidate state).

CREATE TABLE IF NOT EXISTS acquisition_mission_observe_reactions (
  id TEXT PRIMARY KEY,
  observation_id TEXT NOT NULL UNIQUE,
  mission_id TEXT NOT NULL REFERENCES acquisition_missions(id) ON DELETE CASCADE,
  tenant_id TEXT NOT NULL,
  prospect_id TEXT,
  evidence_type TEXT NOT NULL,
  evidence_strength TEXT NOT NULL,
  interpretation_type TEXT,
  prior_disposition TEXT,
  updated_disposition TEXT NOT NULL,
  mission_evidence_tier TEXT,
  recommended_next_action TEXT NOT NULL,
  recommended_timing JSONB NOT NULL DEFAULT '{}'::jsonb,
  rationale TEXT NOT NULL DEFAULT '',
  human_approval_required BOOLEAN NOT NULL DEFAULT FALSE,
  external_action_permitted BOOLEAN NOT NULL DEFAULT FALSE,
  cadence_source TEXT NOT NULL DEFAULT 'unresolved',
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS acquisition_mission_observe_reactions_mission_idx
  ON acquisition_mission_observe_reactions (mission_id, at ASC);

CREATE INDEX IF NOT EXISTS acquisition_mission_observe_reactions_prospect_idx
  ON acquisition_mission_observe_reactions (mission_id, prospect_id, at ASC);

CREATE TABLE IF NOT EXISTS acquisition_mission_candidate_observe_state (
  mission_id TEXT NOT NULL REFERENCES acquisition_missions(id) ON DELETE CASCADE,
  prospect_id TEXT NOT NULL,
  tenant_id TEXT NOT NULL,
  disposition TEXT NOT NULL,
  evidence_strength TEXT,
  last_observation_id TEXT,
  last_evidence_type TEXT,
  last_reaction_id TEXT,
  recommended_next_action TEXT,
  recommended_timing JSONB NOT NULL DEFAULT '{}'::jsonb,
  sequence_step_sent INTEGER,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (mission_id, prospect_id)
);

CREATE INDEX IF NOT EXISTS acquisition_mission_candidate_observe_state_tenant_idx
  ON acquisition_mission_candidate_observe_state (tenant_id, mission_id);
