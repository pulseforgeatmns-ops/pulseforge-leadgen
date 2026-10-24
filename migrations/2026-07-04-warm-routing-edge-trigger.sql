-- Explicit warm-routing edge-state migration.
-- Run deliberately after deploying with WARM_ROUTING_ENABLED=false.
-- This file is intentionally not imported by server.js or any ensure* helper.

BEGIN;

CREATE TABLE warm_signal_state (
  client_id INTEGER NOT NULL,
  prospect_id UUID NOT NULL REFERENCES prospects(id),
  signal_type TEXT NOT NULL CHECK (signal_type IN (
    'ICP_ELIGIBILITY', 'ICP_SCORE', 'ENGAGEMENT_CLUSTER', 'REPLY'
  )),
  is_active BOOLEAN NOT NULL DEFAULT FALSE,
  last_observed_value JSONB NOT NULL DEFAULT '{}'::jsonb,
  last_source_event_key TEXT,
  last_fired_value JSONB,
  last_fired_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (client_id, prospect_id, signal_type)
);

CREATE INDEX warm_signal_state_active_idx
  ON warm_signal_state (client_id, signal_type, is_active)
  WHERE is_active = TRUE;

ALTER TABLE warm_trigger_fires
  ADD COLUMN event_key TEXT;

CREATE UNIQUE INDEX warm_trigger_fires_event_key_uidx
  ON warm_trigger_fires (client_id, event_key)
  WHERE event_key IS NOT NULL;

DO $$
DECLARE
  constraint_name TEXT;
BEGIN
  FOR constraint_name IN
    SELECT con.conname
    FROM pg_constraint con
    JOIN pg_class rel ON rel.oid = con.conrelid
    WHERE rel.relname = 'warm_trigger_fires'
      AND con.contype = 'c'
      AND pg_get_constraintdef(con.oid) ILIKE '%trigger_reason%'
  LOOP
    EXECUTE format('ALTER TABLE warm_trigger_fires DROP CONSTRAINT %I', constraint_name);
  END LOOP;
END $$;

ALTER TABLE warm_trigger_fires
  ADD CONSTRAINT warm_trigger_fires_trigger_reason_check
  CHECK (trigger_reason IN (
    'ICP_JUMP_15', 'ICP_CROSS_90', 'REPLY_RECEIVED',
    'ENGAGEMENT_CLUSTER', 'ICP_CROSS_80_RECENT'
  ));

CREATE TABLE warm_signal_events (
  id BIGSERIAL PRIMARY KEY,
  client_id INTEGER NOT NULL,
  prospect_id UUID NOT NULL REFERENCES prospects(id),
  signal_type TEXT NOT NULL CHECK (signal_type IN (
    'ICP_ELIGIBILITY', 'ICP_SCORE', 'ENGAGEMENT_CLUSTER', 'REPLY', 'LEGACY_FIRE'
  )),
  event_key TEXT NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'consumed', 'failed')),
  routed_fire_id BIGINT REFERENCES warm_trigger_fires(id),
  consumed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (client_id, event_key)
);

CREATE INDEX warm_signal_events_pending_idx
  ON warm_signal_events (client_id, status, observed_at)
  WHERE status <> 'consumed';

CREATE TABLE warm_routing_control (
  client_id INTEGER PRIMARY KEY,
  seed_version TEXT NOT NULL,
  seeded_at TIMESTAMPTZ,
  projected_first_run_fires INTEGER,
  seed_details JSONB NOT NULL DEFAULT '{}'::jsonb
);

COMMIT;
