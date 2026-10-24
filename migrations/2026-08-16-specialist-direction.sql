-- SPEC-096 Max Specialist Direction & Operator Rationale (v1 thin slice)
-- Durable operator corrections on specialist recommendations (Paige v1).
-- Additive. Rollback: migrations/2026-08-16-specialist-direction.rollback.sql

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS content_recommendations (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id TEXT NOT NULL,
  client_id INTEGER NOT NULL,
  specialist TEXT NOT NULL DEFAULT 'paige',
  kind TEXT NOT NULL DEFAULT 'paige_campaign_content_recommendation',
  parent_recommendation_id UUID REFERENCES content_recommendations (id) ON DELETE SET NULL,
  campaign_id TEXT,
  objective_id UUID,
  objective TEXT,
  channel TEXT,
  recommended_direction TEXT NOT NULL,
  reason TEXT NOT NULL DEFAULT '',
  confidence NUMERIC(5, 4),
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  status TEXT NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending', 'accepted', 'refined', 'rejected', 'superseded', 'failed')),
  supporting_learning_ids UUID[] NOT NULL DEFAULT '{}'::uuid[],
  supporting_publication_ids UUID[] NOT NULL DEFAULT '{}'::uuid[],
  provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS content_recommendations_tenant_status_idx
  ON content_recommendations (tenant_id, status, created_at DESC);

CREATE INDEX IF NOT EXISTS content_recommendations_tenant_campaign_idx
  ON content_recommendations (tenant_id, campaign_id, created_at DESC);

CREATE TABLE IF NOT EXISTS specialist_directions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id TEXT NOT NULL,
  client_id INTEGER NOT NULL,
  specialist TEXT NOT NULL DEFAULT 'paige',
  source_recommendation_id UUID NOT NULL REFERENCES content_recommendations (id) ON DELETE CASCADE,
  source_objective_id UUID,
  source_campaign_id TEXT,
  operator_message TEXT NOT NULL,
  disposition TEXT NOT NULL
    CHECK (disposition IN ('accept', 'refine', 'reject')),
  accepted_elements TEXT[] NOT NULL DEFAULT '{}'::text[],
  changed_elements TEXT[] NOT NULL DEFAULT '{}'::text[],
  updated_direction TEXT,
  rationale TEXT NOT NULL DEFAULT '',
  scope TEXT NOT NULL DEFAULT 'recommendation_only'
    CHECK (scope IN (
      'recommendation_only',
      'experiment_campaign',
      'durable_preference',
      'business_constraint'
    )),
  confidence NUMERIC(5, 4),
  refinement_state TEXT NOT NULL DEFAULT 'interpreted'
    CHECK (refinement_state IN (
      'interpreted',
      'clarification_needed',
      'delegated',
      'completed',
      'failed'
    )),
  resulting_recommendation_id UUID REFERENCES content_recommendations (id) ON DELETE SET NULL,
  operator_learning_id UUID,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS specialist_directions_tenant_scope_idx
  ON specialist_directions (tenant_id, scope, created_at DESC);

CREATE INDEX IF NOT EXISTS specialist_directions_source_rec_idx
  ON specialist_directions (source_recommendation_id, created_at DESC);

CREATE INDEX IF NOT EXISTS specialist_directions_campaign_idx
  ON specialist_directions (tenant_id, source_campaign_id, created_at DESC)
  WHERE source_campaign_id IS NOT NULL;
