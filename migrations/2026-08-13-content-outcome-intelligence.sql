-- SPEC-092 Content Outcome Intelligence (planning draft: SPEC-085)
-- Extends Outcome Intelligence (SPEC-013) with durable content publication outcomes.
-- Additive only. Rollback: migrations/2026-08-13-content-outcome-intelligence.rollback.sql
--
-- Records: ContentArtifact → ContentPublication → Performance / Business / Qualitative
-- Does NOT mutate Paige strategy. No LinkedIn API dependency. No vanity scores.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Source content object (Paige pending_comment or manual LinkedIn backfill).
CREATE TABLE IF NOT EXISTS content_artifacts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id TEXT NOT NULL,
  client_id INTEGER NOT NULL,
  pending_comment_id TEXT,
  title TEXT,
  body TEXT,
  channel TEXT,
  format TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS content_artifacts_tenant_created_idx
  ON content_artifacts (tenant_id, created_at DESC);

CREATE INDEX IF NOT EXISTS content_artifacts_client_pending_idx
  ON content_artifacts (client_id, pending_comment_id)
  WHERE pending_comment_id IS NOT NULL;

-- One published instance of an artifact on a channel.
CREATE TABLE IF NOT EXISTS content_publications (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id TEXT NOT NULL,
  client_id INTEGER NOT NULL,
  content_artifact_id UUID NOT NULL
    REFERENCES content_artifacts(id) ON DELETE RESTRICT,
  channel TEXT NOT NULL DEFAULT 'linkedin',
  external_post_id TEXT,
  external_url TEXT,
  published_at TIMESTAMPTZ NOT NULL,
  objective TEXT,
  topic TEXT,
  thesis TEXT,
  format TEXT,
  intended_audience TEXT[] NOT NULL DEFAULT '{}',
  campaign_id TEXT,
  linkedin_post_stats_id BIGINT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS content_publications_tenant_published_idx
  ON content_publications (tenant_id, published_at DESC);

CREATE INDEX IF NOT EXISTS content_publications_tenant_objective_idx
  ON content_publications (tenant_id, objective, published_at DESC);

CREATE INDEX IF NOT EXISTS content_publications_tenant_topic_idx
  ON content_publications (tenant_id, topic, published_at DESC);

CREATE INDEX IF NOT EXISTS content_publications_artifact_idx
  ON content_publications (content_artifact_id);

-- Immutable performance observations over time (velocity / longevity).
CREATE TABLE IF NOT EXISTS content_performance_snapshots (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id TEXT NOT NULL,
  client_id INTEGER NOT NULL,
  publication_id UUID NOT NULL
    REFERENCES content_publications(id) ON DELETE CASCADE,
  observed_at TIMESTAMPTZ NOT NULL,
  impressions INTEGER CHECK (impressions IS NULL OR impressions >= 0),
  members_reached INTEGER CHECK (members_reached IS NULL OR members_reached >= 0),
  reactions INTEGER CHECK (reactions IS NULL OR reactions >= 0),
  comments INTEGER CHECK (comments IS NULL OR comments >= 0),
  reposts INTEGER CHECK (reposts IS NULL OR reposts >= 0),
  saves INTEGER CHECK (saves IS NULL OR saves >= 0),
  profile_views_attributed INTEGER CHECK (profile_views_attributed IS NULL OR profile_views_attributed >= 0),
  followers_gained INTEGER CHECK (followers_gained IS NULL OR followers_gained >= 0),
  connection_requests INTEGER CHECK (connection_requests IS NULL OR connection_requests >= 0),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS content_performance_snapshots_pub_observed_idx
  ON content_performance_snapshots (publication_id, observed_at ASC);

CREATE INDEX IF NOT EXISTS content_performance_snapshots_tenant_idx
  ON content_performance_snapshots (tenant_id, observed_at DESC);

-- Downstream business outcomes (distinct from vanity metrics).
CREATE TABLE IF NOT EXISTS content_business_outcomes (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id TEXT NOT NULL,
  client_id INTEGER NOT NULL,
  publication_id UUID NOT NULL
    REFERENCES content_publications(id) ON DELETE CASCADE,
  outcome_type TEXT NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL,
  company_id TEXT,
  person_id TEXT,
  interaction_id TEXT,
  evidence_id TEXT,
  description TEXT,
  confidence NUMERIC(4, 3),
  attribution TEXT NOT NULL DEFAULT 'unknown'
    CHECK (attribution IN ('direct', 'likely', 'possible', 'unknown')),
  -- Soft link to canonical Outcome Intelligence / knowledge Outcome node.
  canonical_outcome_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS content_business_outcomes_pub_idx
  ON content_business_outcomes (publication_id, occurred_at DESC);

CREATE INDEX IF NOT EXISTS content_business_outcomes_tenant_type_idx
  ON content_business_outcomes (tenant_id, outcome_type, occurred_at DESC);

-- Qualitative observations (not automatic conclusions).
CREATE TABLE IF NOT EXISTS content_qualitative_signals (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id TEXT NOT NULL,
  client_id INTEGER NOT NULL,
  publication_id UUID NOT NULL
    REFERENCES content_publications(id) ON DELETE CASCADE,
  observed_at TIMESTAMPTZ NOT NULL,
  signal_type TEXT NOT NULL,
  description TEXT NOT NULL,
  audience_type TEXT,
  sentiment TEXT,
  strength TEXT,
  evidence_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS content_qualitative_signals_pub_idx
  ON content_qualitative_signals (publication_id, observed_at DESC);

CREATE INDEX IF NOT EXISTS content_qualitative_signals_tenant_idx
  ON content_qualitative_signals (tenant_id, observed_at DESC);
