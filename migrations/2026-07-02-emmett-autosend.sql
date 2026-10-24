ALTER TABLE clients
  ADD COLUMN IF NOT EXISTS warmup_start_date DATE,
  ADD COLUMN IF NOT EXISTS autosend_enabled BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE prospects
  ADD COLUMN IF NOT EXISTS next_touch_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS email_sequence_completed_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS emmett_warmup_config (
  business_day_start INTEGER PRIMARY KEY CHECK (business_day_start > 0),
  daily_cap INTEGER NOT NULL CHECK (daily_cap > 0),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO emmett_warmup_config (business_day_start, daily_cap)
VALUES (1, 5), (6, 10), (11, 20), (16, 35), (21, 50)
ON CONFLICT (business_day_start) DO NOTHING;

CREATE TABLE IF NOT EXISTS emmett_schema_migrations (
  name TEXT PRIMARY KEY,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO emmett_schema_migrations (name)
VALUES ('2026-07-02-enable-anchor-autosend')
ON CONFLICT (name) DO NOTHING;
