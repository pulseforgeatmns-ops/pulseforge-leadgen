BEGIN;

-- This migration enables data capture and reviewed manual-send logging only.
-- It does not enable providers, agents, bookings, or revenue writes.
DO $$
BEGIN
  IF to_regclass('public.clients') IS NULL
    OR to_regclass('public.prospects') IS NULL
    OR to_regclass('public.call_dispositions') IS NULL
    OR to_regclass('public.campaigns') IS NULL THEN
    RAISE EXCEPTION 'Anchor Phone Setter v1 prerequisites are missing';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM clients WHERE id = 10 AND active = TRUE) THEN
    RAISE EXCEPTION 'Anchor Cleaning client_id=10 is not active';
  END IF;
END $$;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Preserve the exact tenant targeting state once, so approved rollback can
-- restore it without guessing or touching any other client.
CREATE TABLE IF NOT EXISTS anchor_phone_setter_v1_targeting_backup (
  client_id INTEGER PRIMARY KEY REFERENCES clients(id) ON DELETE RESTRICT,
  target_verticals JSONB NOT NULL,
  vertical_tiers JSONB NOT NULL,
  captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO anchor_phone_setter_v1_targeting_backup (client_id, target_verticals, vertical_tiers)
SELECT id, target_verticals, vertical_tiers FROM clients WHERE id = 10
ON CONFLICT (client_id) DO NOTHING;

ALTER TABLE call_dispositions
  ADD COLUMN IF NOT EXISTS details JSONB NOT NULL DEFAULT '{}'::jsonb;

CREATE INDEX IF NOT EXISTS call_dispositions_anchor_details_idx
  ON call_dispositions (client_id, created_at DESC)
  WHERE details <> '{}'::jsonb;

CREATE TABLE IF NOT EXISTS setter_follow_up_drafts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id INTEGER NOT NULL REFERENCES clients(id) ON DELETE RESTRICT,
  prospect_id UUID NOT NULL REFERENCES prospects(id) ON DELETE RESTRICT,
  channel TEXT NOT NULL CHECK (channel IN ('email','sms')),
  body TEXT NOT NULL CHECK (char_length(body) <= 5000),
  status TEXT NOT NULL DEFAULT 'draft' CHECK (status IN ('draft','reviewed','dismissed','manual_sent')),
  reviewer_id INTEGER,
  reviewed_at TIMESTAMPTZ,
  dismissed_by INTEGER,
  dismissed_at TIMESTAMPTZ,
  manual_sent_by INTEGER,
  manual_sent_at TIMESTAMPTZ,
  manual_send_reference TEXT,
  created_by INTEGER NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (client_id, id)
);
CREATE INDEX IF NOT EXISTS setter_follow_up_drafts_tenant_prospect_idx
  ON setter_follow_up_drafts (client_id, prospect_id, created_at DESC);

-- The foundation schema intentionally leaves campaign shape generic. These
-- additive fields make the paused Anchor campaign durable without assuming a
-- pre-existing campaign application.
ALTER TABLE campaigns
  ADD COLUMN IF NOT EXISTS campaign_key TEXT,
  ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'paused',
  ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb;
CREATE UNIQUE INDEX IF NOT EXISTS campaigns_client_campaign_key_uidx
  ON campaigns (client_id, campaign_key)
  WHERE campaign_key IS NOT NULL;

INSERT INTO campaigns (client_id, campaign_key, status, metadata)
SELECT 10, 'anchor_phone_setter_immediate_cash_v1', 'paused',
  jsonb_build_object('mode','manual_phone','external_sends_enabled',false,'revenue_writes_enabled',false)
WHERE NOT EXISTS (
  SELECT 1 FROM campaigns
  WHERE client_id = 10 AND campaign_key = 'anchor_phone_setter_immediate_cash_v1'
);

UPDATE clients
SET target_verticals = $json$
[
  {"vertical":"cleaning_company_overflow","tier":"A","autonomous_sourcing":true,"seed_terms":["commercial cleaning company {city} {state}","janitorial service {city} {state}"]},
  {"vertical":"str_manager","tier":"A","autonomous_sourcing":true,"seed_terms":["short term rental management {city} {state}","Airbnb property management {city} {state}"]},
  {"vertical":"property_manager","tier":"A","autonomous_sourcing":true,"seed_terms":["property management company {city} {state}","commercial property management {city} {state}"]},
  {"vertical":"realtor","tier":"A","autonomous_sourcing":true,"seed_terms":["real estate agency {city} {state}","realtor office {city} {state}"]},
  {"vertical":"restoration_remodeling_partner","tier":"A","autonomous_sourcing":true,"seed_terms":["water damage restoration {city} {state}","remodeling contractor {city} {state}"]},
  {"vertical":"commercial_office","tier":"A","autonomous_sourcing":true,"seed_terms":["commercial office {city} {state}","office park {city} {state}"]}
]
$json$::jsonb,
vertical_tiers = $json$
{"cleaning_company_overflow":"A","str_manager":"A","property_manager":"A","realtor":"A","restoration_remodeling_partner":"A","commercial_office":"A"}
$json$::jsonb
WHERE id = 10;

COMMIT;
