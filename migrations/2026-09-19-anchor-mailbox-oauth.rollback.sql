-- Rollback OAuth columns only. Does not restore password secret refs.

ALTER TABLE tenant_mailbox_integrations
  DROP COLUMN IF EXISTS oauth_refresh_secret_ref;

ALTER TABLE tenant_mailbox_integrations
  DROP COLUMN IF EXISTS imap_auth_mode;
