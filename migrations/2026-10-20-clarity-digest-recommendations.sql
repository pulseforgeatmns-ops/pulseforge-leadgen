-- PF-CLARITY-001. Explicit operator-applied migration; never startup DDL.
BEGIN;
CREATE TABLE IF NOT EXISTS clarity_integrations (
  client_id INTEGER PRIMARY KEY REFERENCES clients(id),
  mailbox_email TEXT NOT NULL,
  digest_sender TEXT NOT NULL,
  project_id TEXT NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT FALSE,
  consent_status TEXT NOT NULL CHECK (consent_status IN ('authorized','revoked')),
  oauth_scopes JSONB NOT NULL,
  oauth_client_id TEXT NOT NULL,
  ingestion_cursor JSONB,
  refresh_token_encrypted JSONB NOT NULL,
  token_key_id TEXT NOT NULL,
  consented_at TIMESTAMPTZ NOT NULL,
  next_run_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_success_at TIMESTAMPTZ,
  consecutive_failures INTEGER NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS clarity_snapshots (
  id UUID PRIMARY KEY,
  client_id INTEGER NOT NULL REFERENCES clients(id),
  window_start DATE NOT NULL,
  window_end DATE NOT NULL,
  metrics JSONB NOT NULL,
  deltas_pct JSONB NOT NULL,
  provenance JSONB NOT NULL,
  synthetic BOOLEAN NOT NULL DEFAULT FALSE,
  cross_evidence JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (client_id,id),
  UNIQUE (client_id,window_start,window_end),
  CHECK (window_end >= window_start AND window_end - window_start <= 7)
);
CREATE TABLE IF NOT EXISTS clarity_digest_deliveries (
  client_id INTEGER NOT NULL,
  gmail_message_id TEXT NOT NULL,
  snapshot_id UUID NOT NULL,
  window_start DATE NOT NULL,
  window_end DATE NOT NULL,
  provenance JSONB NOT NULL,
  received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (client_id,gmail_message_id,window_start,window_end),
  FOREIGN KEY (client_id,snapshot_id) REFERENCES clarity_snapshots(client_id,id)
);
CREATE TABLE IF NOT EXISTS clarity_recommendations (
  id UUID PRIMARY KEY,
  client_id INTEGER NOT NULL,
  snapshot_id UUID NOT NULL,
  recommendation_key TEXT NOT NULL,
  rank INTEGER NOT NULL CHECK (rank BETWEEN 1 AND 3),
  digest TEXT NOT NULL,
  content JSONB NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','approved','rejected')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  decided_at TIMESTAMPTZ,
  UNIQUE (client_id,id),
  UNIQUE (client_id,snapshot_id,recommendation_key),
  UNIQUE (client_id,snapshot_id,rank),
  FOREIGN KEY (client_id,snapshot_id) REFERENCES clarity_snapshots(client_id,id)
);
CREATE TABLE IF NOT EXISTS clarity_review_decisions (
  id UUID PRIMARY KEY,
  client_id INTEGER NOT NULL,
  recommendation_id UUID NOT NULL,
  operator_id INTEGER NOT NULL REFERENCES users(id),
  decision TEXT NOT NULL CHECK (decision IN ('approve','reject')),
  expected_digest TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  execution_request_id TEXT NOT NULL,
  website_changes_authorized BOOLEAN NOT NULL DEFAULT FALSE CHECK (website_changes_authorized = FALSE),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (client_id,idempotency_key),
  UNIQUE (client_id,recommendation_id),
  FOREIGN KEY (client_id,recommendation_id) REFERENCES clarity_recommendations(client_id,id)
);
CREATE TABLE IF NOT EXISTS clarity_ingestion_runs (
  id UUID PRIMARY KEY,
  client_id INTEGER NOT NULL REFERENCES clients(id),
  status TEXT NOT NULL,
  counts JSONB NOT NULL DEFAULT '{}'::jsonb,
  error_code TEXT,
  rejections JSONB NOT NULL DEFAULT '[]'::jsonb,
  next_run_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS clarity_snapshots_history ON clarity_snapshots(client_id,window_end DESC);
CREATE INDEX IF NOT EXISTS clarity_integrations_due ON clarity_integrations(next_run_at) WHERE enabled;
COMMIT;
