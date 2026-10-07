-- SPEC-PAIGE-ANCHOR-SOCIAL-002 — Anchor adaptive organic social planning state
-- Rollback: migrations/2026-10-07-paige-anchor-organic-social-002.rollback.sql

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS paige_media_assets (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id INTEGER NOT NULL,
  drive_file_id TEXT NOT NULL,
  drive_folder_id TEXT,
  filename TEXT NOT NULL,
  mime_type TEXT,
  media_kind TEXT NOT NULL DEFAULT 'other',
  byte_size BIGINT,
  created_time TIMESTAMPTZ,
  modified_time TIMESTAMPTZ,
  thumbnail_link TEXT,
  web_view_link TEXT,
  job_group_key TEXT,
  visual_hints JSONB NOT NULL DEFAULT '{}'::jsonb,
  usage_count INTEGER NOT NULL DEFAULT 0,
  last_used_at TIMESTAMPTZ,
  discovered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  UNIQUE (client_id, drive_file_id)
);

CREATE INDEX IF NOT EXISTS paige_media_assets_client_discovered_idx
  ON paige_media_assets (client_id, discovered_at DESC);

CREATE TABLE IF NOT EXISTS paige_content_backlog (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id INTEGER NOT NULL,
  story_concept TEXT NOT NULL,
  content_category TEXT NOT NULL,
  target_platform TEXT NOT NULL,
  proposed_format TEXT NOT NULL,
  asset_ids UUID[] NOT NULL DEFAULT '{}'::uuid[],
  body_draft TEXT,
  platform_variants JSONB NOT NULL DEFAULT '{}'::jsonb,
  proposed_publish_at TIMESTAMPTZ,
  scheduling_rationale TEXT,
  planning_rationale TEXT,
  approval_state TEXT NOT NULL DEFAULT 'draft',
  artifact_id UUID,
  exploration BOOLEAN NOT NULL DEFAULT false,
  meta JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS paige_content_backlog_client_state_idx
  ON paige_content_backlog (client_id, approval_state, proposed_publish_at);

CREATE TABLE IF NOT EXISTS paige_social_platform_history (
  client_id INTEGER NOT NULL,
  platform TEXT NOT NULL,
  content_category TEXT NOT NULL DEFAULT '',
  format TEXT NOT NULL DEFAULT '',
  dow SMALLINT NOT NULL,
  hour_local SMALLINT NOT NULL,
  sample_count INTEGER NOT NULL DEFAULT 0,
  impressions_sum BIGINT NOT NULL DEFAULT 0,
  engagement_sum BIGINT NOT NULL DEFAULT 0,
  engagement_rate_avg NUMERIC,
  last_observed_at TIMESTAMPTZ,
  PRIMARY KEY (client_id, platform, content_category, format, dow, hour_local)
);
