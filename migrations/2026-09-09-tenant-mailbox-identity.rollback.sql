-- Rollback SPEC-248 tenant mailbox infrastructure.

DROP TABLE IF EXISTS tenant_mailbox_poll_state;
DROP TABLE IF EXISTS tenant_outreach_suppressions;
DROP TABLE IF EXISTS tenant_outreach_events;
DROP TABLE IF EXISTS tenant_outreach_messages;
DROP TABLE IF EXISTS tenant_outreach_threads;
DROP TABLE IF EXISTS tenant_sending_identities;
DROP TABLE IF EXISTS tenant_mailbox_integrations;
