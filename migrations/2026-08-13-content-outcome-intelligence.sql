-- SPEC-092 Content Outcome Intelligence (v1 thin slice)
-- Product brief used "SPEC-085"; repository SPEC-085 is Executive Business Brief.
-- Durable publication → performance → business outcome → qualitative signal records.
-- Extends Outcome Intelligence (SPEC-013 / SPEC-036) for Paige content.
-- Additive only. Rollback: migrations/2026-08-13-content-outcome-intelligence.rollback.sql
-- Soft TEXT refs to pending_comments / knowledge entities — no hard CRM FKs.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS content_publications (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id INTEGER NOT NULL,
  tenant_id TEXT NOT NULL,
  content_artifact_id TEXT NOT NULL,
  channel TEXT NOT NULL DEFAULT 'linkedin'
    CHECK (channel IN ('linkedin', 'facebook', 'gbp', 'instagram', 'blog', 'other')),
  external_post_id TEXT,
  external_url TEXT,
  published_at TIMESTAMPTZ NOT NULL,
  objective TEXT,
  topic TEXT,
  thesis TEXT,
  format TEXT,
  intended_audience TEXT[] NOT NULL DEFAULT '{}'::text[],
  campaign_id TEXT,
  title TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS content_publications_client_published_idx
  ON content_publications (client_id, published_at DESC);

CREATE INDEX IF NOT EXISTS content_publications_tenant_published_idx
  ON content_publications (tenant_id, published_at DESC);

CREATE INDEX IF NOT EXISTS content_publications_artifact_idx
  ON content_publications (client_id, content_artifact_id);

CREATE INDEX IF NOT EXISTS content_publications_channel_objective_idx
  ON content_publications (client_id, channel, objective);

CREATE TABLE IF NOT EXISTS content_performance_snapshots (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id INTEGER NOT NULL,
  tenant_id TEXT NOT NULL,
  publication_id UUID NOT NULL
    REFERENCES content_publications(id) ON DELETE CASCADE,
  observed_at TIMESTAMPTZ NOT NULL,
  impressions INTEGER CHECK (impressions IS NULL OR impressions >= 0),
  members_reached INTEGER CHECK (members_reached IS NULL OR members_reached >= 0),
  reactions INTEGER CHECK (reactions IS NULL OR reactions >= 0),
  comments INTEGER CHECK (comments IS NULL OR comments >= 0),
  reposts INTEGER CHECK (reposts IS NULL OR reposts >= 0),
  saves INTEGER CHECK (saves IS NULL OR saves >= 0),
  profile_views_attributed INTEGER
    CHECK (profile_views_attributed IS NULL OR profile_views_attributed >= 0),
  followers_gained INTEGER CHECK (followers_gained IS NULL OR followers_gained >= 0),
  connection_requests INTEGER
    CHECK (connection_requests IS NULL OR connection_requests >= 0),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS content_performance_snapshots_pub_observed_idx
  ON content_performance_snapshots (publication_id, observed_at ASC);

CREATE INDEX IF NOT EXISTS content_performance_snapshots_client_idx
  ON content_performance_snapshots (client_id, observed_at DESC);

CREATE TABLE IF NOT EXISTS content_business_outcomes (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id INTEGER NOT NULL,
  tenant_id TEXT NOT NULL,
  publication_id UUID NOT NULL
    REFERENCES content_publications(id) ON DELETE CASCADE,
  outcome_type TEXT NOT NULL
    CHECK (outcome_type IN (
      'qualified_dm',
      'prospect_conversation',
      'partner_conversation',
      'builder_connection',
      'demo_interest',
      'meeting_booked',
      'pilot_interest',
      'customer_opportunity',
      'other'
    )),
  occurred_at TIMESTAMPTZ NOT NULL,
  company_id TEXT,
  person_id TEXT,
  interaction_id TEXT,
  evidence_id TEXT,
  description TEXT,
  confidence NUMERIC(4, 3)
    CHECK (confidence IS NULL OR (confidence >= 0 AND confidence <= 1)),
  attribution TEXT NOT NULL DEFAULT 'unknown'
    CHECK (attribution IN ('direct', 'likely', 'possible', 'unknown')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS content_business_outcomes_pub_occurred_idx
  ON content_business_outcomes (publication_id, occurred_at ASC);

CREATE INDEX IF NOT EXISTS content_business_outcomes_client_type_idx
  ON content_business_outcomes (client_id, outcome_type, occurred_at DESC);

CREATE TABLE IF NOT EXISTS content_qualitative_signals (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id INTEGER NOT NULL,
  tenant_id TEXT NOT NULL,
  publication_id UUID NOT NULL
    REFERENCES content_publications(id) ON DELETE CASCADE,
  observed_at TIMESTAMPTZ NOT NULL,
  signal_type TEXT NOT NULL
    CHECK (signal_type IN (
      'message_resonance',
      'audience_signal',
      'objection',
      'question',
      'language_adoption',
      'partnership_signal',
      'buyer_signal',
      'technical_interest',
      'unexpected_response',
      'other'
    )),
  description TEXT NOT NULL,
  audience_type TEXT,
  sentiment TEXT,
  strength TEXT,
  evidence_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS content_qualitative_signals_pub_observed_idx
  ON content_qualitative_signals (publication_id, observed_at ASC);

CREATE INDEX IF NOT EXISTS content_qualitative_signals_client_type_idx
  ON content_qualitative_signals (client_id, signal_type, observed_at DESC);
