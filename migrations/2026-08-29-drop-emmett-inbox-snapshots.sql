-- AUDIT-083 — remove unused emmett_inbox_snapshots persistence scaffolding.
-- Inbox snapshots are runtime objects from buildInboxSnapshot(), not durable rows.

DROP INDEX IF EXISTS emmett_inbox_snapshots_tenant_date_idx;
DROP TABLE IF EXISTS emmett_inbox_snapshots;
