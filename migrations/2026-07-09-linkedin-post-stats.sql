BEGIN;

CREATE TABLE IF NOT EXISTS linkedin_post_stats (
  id BIGSERIAL PRIMARY KEY,
  client_id INTEGER NOT NULL DEFAULT 1 REFERENCES clients(id),
  posted_at TIMESTAMPTZ NOT NULL,
  post_url TEXT,
  buffer_post_id TEXT,
  format TEXT NOT NULL CHECK (format IN ('text', 'carousel', 'video', 'image', 'poll')),
  hook_type TEXT NOT NULL CHECK (hook_type IN ('dialogue', 'claim', 'numbers', 'story', 'question', 'other')),
  content_snippet TEXT,
  impressions INTEGER CHECK (impressions IS NULL OR impressions >= 0),
  members_reached INTEGER CHECK (members_reached IS NULL OR members_reached >= 0),
  engagement_rate NUMERIC(5,2) CHECK (engagement_rate IS NULL OR engagement_rate >= 0),
  first_hour_active BOOLEAN NOT NULL DEFAULT FALSE,
  warmed_before_publish BOOLEAN NOT NULL DEFAULT FALSE,
  stats_captured_at TIMESTAMPTZ,
  notes TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_linkedin_post_stats_client_posted
  ON linkedin_post_stats (client_id, posted_at DESC);

CREATE INDEX IF NOT EXISTS idx_linkedin_post_stats_missing_stats
  ON linkedin_post_stats (client_id, posted_at ASC)
  WHERE stats_captured_at IS NULL;

COMMIT;
