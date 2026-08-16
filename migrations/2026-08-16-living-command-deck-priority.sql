-- SPEC-097 Living Command Deck — domain priority persistence for explainable transitions

CREATE TABLE IF NOT EXISTS command_deck_domain_priority (
  tenant_id TEXT NOT NULL,
  domain_id TEXT NOT NULL,
  priority_state TEXT NOT NULL DEFAULT 'normal',
  previous_state TEXT,
  transition_reason TEXT,
  evidence_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
  changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  reviewed_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, domain_id),
  CONSTRAINT command_deck_domain_priority_state_check
    CHECK (priority_state IN ('monitored', 'normal', 'elevated', 'urgent')),
  CONSTRAINT command_deck_domain_priority_previous_check
    CHECK (previous_state IS NULL OR previous_state IN ('monitored', 'normal', 'elevated', 'urgent'))
);

CREATE INDEX IF NOT EXISTS idx_command_deck_domain_priority_tenant
  ON command_deck_domain_priority (tenant_id);
