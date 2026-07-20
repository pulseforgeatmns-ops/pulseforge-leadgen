-- REMEDIATION — Phase 3H Gate 2 suppressed-callback cancel
-- Canonical path: scripts/remediation/2026-07-19-cancel-suppressed-setter-callbacks.sql
--
-- Scope: UPDATE setter_callbacks ONLY.
-- Targets: status='pending' AND related prospect do_not_contact OR is_synthetic.
-- Sets status='cancelled'; preserves history; never DELETE.
-- Idempotent for already-cancelled rows (they no longer match status='pending').
-- Does not modify prospects, clients, call_dispositions, drafts, activity_log,
-- agent_log, touchpoints, or feature flags. No Anchor enablement. No outbound.
--
-- Fail-closed: aborts the transaction unless exactly 1 row is updated.

BEGIN;

DO $$
DECLARE
  n integer;
BEGIN
  UPDATE setter_callbacks sc
  SET status = 'cancelled',
      cancelled_at = COALESCE(sc.cancelled_at, NOW()),
      updated_at = NOW()
  FROM prospects p
  WHERE p.id = sc.prospect_id
    AND p.client_id = sc.client_id
    AND sc.status = 'pending'
    AND (p.do_not_contact = true OR p.is_synthetic = true);

  GET DIAGNOSTICS n = ROW_COUNT;
  IF n <> 1 THEN
    RAISE EXCEPTION
      'suppressed-callback remediation expected exactly 1 affected row, got % — aborting',
      n;
  END IF;
END $$;

COMMIT;
