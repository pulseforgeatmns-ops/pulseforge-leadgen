-- SPEC-248 Tenant mailbox / sending identity infrastructure.
-- Additive canonical persistence for tenant-owned outreach mailboxes.

CREATE TABLE IF NOT EXISTS tenant_mailbox_integrations (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  provider_type TEXT NOT NULL,
  mailbox_address TEXT NOT NULL,
  display_name TEXT,
  smtp_host TEXT,
  smtp_port INTEGER,
  smtp_tls_mode TEXT,
  imap_host TEXT,
  imap_port INTEGER,
  imap_tls_mode TEXT,
  smtp_secret_ref TEXT,
  imap_secret_ref TEXT,
  shared_secret_ref TEXT,
  status TEXT NOT NULL DEFAULT 'unverified',
  verification_state JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  disabled_at TIMESTAMPTZ,
  revoked_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS tenant_mailbox_integrations_mailbox_idx
  ON tenant_mailbox_integrations (tenant_id, lower(mailbox_address));

CREATE TABLE IF NOT EXISTS tenant_sending_identities (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  mailbox_integration_id TEXT NOT NULL REFERENCES tenant_mailbox_integrations(id),
  sender_email TEXT NOT NULL,
  sender_display_name TEXT,
  reply_to_address TEXT,
  status TEXT NOT NULL DEFAULT 'unverified',
  verification_state JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  disabled_at TIMESTAMPTZ,
  revoked_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS tenant_sending_identities_sender_idx
  ON tenant_sending_identities (tenant_id, lower(sender_email), mailbox_integration_id);

CREATE TABLE IF NOT EXISTS tenant_outreach_threads (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  mission_id TEXT,
  prospect_id TEXT,
  contact_ref TEXT,
  participants JSONB NOT NULL DEFAULT '[]'::jsonb,
  current_status TEXT NOT NULL DEFAULT 'open',
  latest_inbound_message_id TEXT,
  latest_outbound_message_id TEXT,
  last_activity_at TIMESTAMPTZ,
  reply_state TEXT NOT NULL DEFAULT 'none',
  sequence_state TEXT NOT NULL DEFAULT 'not_yet_sent',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS tenant_outreach_threads_binding_idx
  ON tenant_outreach_threads (tenant_id, mission_id, prospect_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS tenant_outreach_messages (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  mission_id TEXT,
  prospect_id TEXT,
  contact_ref TEXT,
  sending_identity_id TEXT,
  thread_id TEXT NOT NULL REFERENCES tenant_outreach_threads(id),
  direction TEXT NOT NULL,
  subject TEXT,
  body TEXT,
  content_ref TEXT,
  sender JSONB NOT NULL DEFAULT '{}'::jsonb,
  recipients JSONB NOT NULL DEFAULT '[]'::jsonb,
  sent_at TIMESTAMPTZ,
  received_at TIMESTAMPTZ,
  provider_message_id TEXT,
  rfc_message_id TEXT,
  in_reply_to TEXT,
  references_header TEXT,
  status TEXT NOT NULL,
  outreach_asset_id TEXT,
  sequence_step_ref TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  failure_code TEXT,
  failure_message TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS tenant_outreach_messages_provider_idx
  ON tenant_outreach_messages (tenant_id, provider_message_id)
  WHERE provider_message_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS tenant_outreach_messages_rfc_idx
  ON tenant_outreach_messages (tenant_id, rfc_message_id)
  WHERE rfc_message_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS tenant_outreach_messages_thread_idx
  ON tenant_outreach_messages (tenant_id, thread_id, created_at DESC);

CREATE TABLE IF NOT EXISTS tenant_outreach_events (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  mission_id TEXT,
  prospect_id TEXT,
  thread_id TEXT,
  message_id TEXT,
  event_type TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS tenant_outreach_events_thread_idx
  ON tenant_outreach_events (tenant_id, thread_id, created_at DESC);

CREATE TABLE IF NOT EXISTS tenant_outreach_suppressions (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  contact_ref TEXT,
  email TEXT NOT NULL,
  reason TEXT NOT NULL,
  source TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  revoked_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS tenant_outreach_suppressions_active_email_idx
  ON tenant_outreach_suppressions (tenant_id, email)
  WHERE revoked_at IS NULL;

CREATE TABLE IF NOT EXISTS tenant_mailbox_poll_state (
  integration_id TEXT PRIMARY KEY REFERENCES tenant_mailbox_integrations(id),
  tenant_id TEXT NOT NULL,
  last_uid_validity TEXT,
  last_uid BIGINT NOT NULL DEFAULT 0,
  last_seen_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
