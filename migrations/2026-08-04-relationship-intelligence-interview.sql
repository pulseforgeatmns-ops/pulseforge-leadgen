-- SPEC-064 Relationship Intelligence Interview (v1)
-- Durable interaction + insight storage for Max-owned debriefs.
-- Additive only. Rollback: migrations/2026-08-04-relationship-intelligence-interview.rollback.sql
-- Soft TEXT entity refs only — does NOT FK or write CRM companies/prospects/opportunities.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS relationship_interactions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id INTEGER,
  company_id TEXT,
  contact_id TEXT,
  opportunity_id TEXT,
  user_id TEXT,
  interaction_type TEXT NOT NULL
    CHECK (interaction_type IN (
      'cold_call',
      'discovery_call',
      'walkthrough',
      'estimate',
      'meeting',
      'demo',
      'proposal_review',
      'follow_up',
      'other'
    )),
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  source TEXT NOT NULL DEFAULT 'max_interview',
  status TEXT NOT NULL DEFAULT 'draft'
    CHECK (status IN ('draft', 'reviewed', 'committed')),
  raw_summary TEXT,
  structured_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
  confidence NUMERIC(4, 3),
  interview_state JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS relationship_interactions_client_status_occurred_idx
  ON relationship_interactions (client_id, status, occurred_at DESC);

CREATE INDEX IF NOT EXISTS relationship_interactions_company_idx
  ON relationship_interactions (company_id);

CREATE INDEX IF NOT EXISTS relationship_interactions_status_idx
  ON relationship_interactions (status);

CREATE TABLE IF NOT EXISTS relationship_interaction_insights (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  interaction_id UUID NOT NULL
    REFERENCES relationship_interactions(id) ON DELETE CASCADE,
  kind TEXT NOT NULL
    CHECK (kind IN (
      'pain',
      'goal',
      'objection',
      'timeline',
      'budget',
      'decision_maker',
      'stakeholder',
      'competitor',
      'next_step',
      'commitment',
      'risk',
      'buying_signal',
      'open_question',
      'preference',
      'context'
    )),
  label TEXT NOT NULL DEFAULT '',
  value TEXT NOT NULL DEFAULT '',
  confidence NUMERIC(4, 3),
  source_quote TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS relationship_interaction_insights_interaction_kind_idx
  ON relationship_interaction_insights (interaction_id, kind);

-- Idempotent named CHECK repair (also covered by
-- migrations/2026-08-05-relationship-intelligence-constraints.sql for DBs that
-- already ran CREATE TABLE IF NOT EXISTS without usable constraints).
DO $ri_constraints$
BEGIN
  IF to_regclass('public.relationship_interactions') IS NULL THEN
    RETURN;
  END IF;

  ALTER TABLE relationship_interactions
    DROP CONSTRAINT IF EXISTS relationship_interactions_interaction_type_check;
  ALTER TABLE relationship_interactions
    ADD CONSTRAINT relationship_interactions_interaction_type_check
    CHECK (interaction_type IN (
      'cold_call',
      'discovery_call',
      'walkthrough',
      'estimate',
      'meeting',
      'demo',
      'proposal_review',
      'follow_up',
      'other'
    ));

  ALTER TABLE relationship_interactions
    DROP CONSTRAINT IF EXISTS relationship_interactions_status_check;
  ALTER TABLE relationship_interactions
    ADD CONSTRAINT relationship_interactions_status_check
    CHECK (status IN ('draft', 'reviewed', 'committed'));

  IF to_regclass('public.relationship_interaction_insights') IS NULL THEN
    RETURN;
  END IF;

  ALTER TABLE relationship_interaction_insights
    DROP CONSTRAINT IF EXISTS relationship_interaction_insights_kind_check;
  ALTER TABLE relationship_interaction_insights
    ADD CONSTRAINT relationship_interaction_insights_kind_check
    CHECK (kind IN (
      'pain',
      'goal',
      'objection',
      'timeline',
      'budget',
      'decision_maker',
      'stakeholder',
      'competitor',
      'next_step',
      'commitment',
      'risk',
      'buying_signal',
      'open_question',
      'preference',
      'context'
    ));
END
$ri_constraints$;

