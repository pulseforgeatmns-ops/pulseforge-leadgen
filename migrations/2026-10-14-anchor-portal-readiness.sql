-- Additive portal upgrade only. Requires existing anchor_portal_* tables.
-- No accounts, credentials, demo fixtures, historical rewrites or permission changes.
-- Review/approve production application separately; do not run against live data
-- as part of the disposable demo or tests.
BEGIN;
ALTER TABLE anchor_portal_visits ADD COLUMN IF NOT EXISTS completed_by INTEGER REFERENCES users(id);
ALTER TABLE anchor_portal_visits ADD COLUMN IF NOT EXISTS request_id TEXT;
ALTER TABLE anchor_portal_issues ADD COLUMN IF NOT EXISTS issue_type TEXT;
ALTER TABLE anchor_portal_issues ADD COLUMN IF NOT EXISTS request_id TEXT;
ALTER TABLE anchor_portal_evidence ADD COLUMN IF NOT EXISTS purpose TEXT NOT NULL DEFAULT 'service';
CREATE UNIQUE INDEX IF NOT EXISTS idx_anchor_portal_report_request ON anchor_portal_visits (completed_by, request_id) WHERE request_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_anchor_portal_issue_request ON anchor_portal_issues (reported_by_user_id, request_id) WHERE request_id IS NOT NULL;
COMMIT;
