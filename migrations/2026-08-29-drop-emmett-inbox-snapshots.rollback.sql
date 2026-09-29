-- Rollback AUDIT-083 drop of emmett_inbox_snapshots (historical table; unused at runtime).

CREATE TABLE IF NOT EXISTS emmett_inbox_snapshots (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  client_id INTEGER,
  local_date DATE NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS emmett_inbox_snapshots_tenant_date_idx
  ON emmett_inbox_snapshots (tenant_id, local_date DESC);
