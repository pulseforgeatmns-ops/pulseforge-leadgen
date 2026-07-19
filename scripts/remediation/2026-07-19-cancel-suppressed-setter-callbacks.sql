-- REMEDIATION (NOT AUTHORIZED FOR AUTOMATIC EXECUTION)
-- Phase 3H post-migration: cancel pending setter_callbacks for DNC/synthetic prospects.
--
-- Root cause: 2026-07-19-setter-pilot-quality-control.sql backfilled
-- setter_callbacks from prospects.callback_at without filtering do_not_contact.
-- The prospects_suppression_cleanup trigger fires only on prospect UPDATE OF
-- do_not_contact/is_synthetic, not on setter_callbacks INSERT.
--
-- Safety: Anchor setter_pipeline_v2 remains false; app schema-presence cache
-- still pre-migration until restart. This remediation is still recommended
-- before any restart/pilot enablement.
--
-- Preserve history: cancel only; do not DELETE rows.
-- Rollback implication: re-setting status='pending' would reintroduce the defect;
-- prefer leaving cancelled and re-creating intentionally if needed.

BEGIN;

-- Preview (run separately in a read-only session before applying):
-- SELECT count(*) FROM setter_callbacks sc
-- JOIN prospects p ON p.id = sc.prospect_id AND p.client_id = sc.client_id
-- WHERE sc.status = 'pending' AND (p.do_not_contact OR p.is_synthetic);

UPDATE setter_callbacks sc
SET status = 'cancelled',
    cancelled_at = COALESCE(sc.cancelled_at, NOW()),
    updated_at = NOW()
FROM prospects p
WHERE p.id = sc.prospect_id
  AND p.client_id = sc.client_id
  AND sc.status = 'pending'
  AND (p.do_not_contact = true OR p.is_synthetic = true);

-- Optional drafts cleanup if the relation exists:
-- UPDATE setter_follow_up_drafts d
-- SET status = 'dismissed', dismissed_at = COALESCE(d.dismissed_at, NOW()), updated_at = NOW()
-- FROM prospects p
-- WHERE p.id = d.prospect_id AND p.client_id = d.client_id
--   AND d.status IN ('draft','reviewed')
--   AND (p.do_not_contact OR p.is_synthetic);

COMMIT;
