BEGIN;

-- A structured phone history is an operational record. Never discard it via
-- the normal rollback path; use the approved archival rollback procedure.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM call_dispositions
    WHERE client_id = 10 AND details <> '{}'::jsonb
  ) THEN
    RAISE EXCEPTION 'Rollback blocked: Anchor structured call history exists';
  END IF;
END $$;

UPDATE clients c
SET target_verticals = b.target_verticals,
    vertical_tiers = b.vertical_tiers
FROM anchor_phone_setter_v1_targeting_backup b
WHERE c.id = b.client_id AND c.id = 10;

DELETE FROM campaigns
WHERE client_id = 10 AND campaign_key = 'anchor_phone_setter_immediate_cash_v1';

DROP TABLE IF EXISTS setter_follow_up_drafts;
DROP INDEX IF EXISTS call_dispositions_anchor_details_idx;
ALTER TABLE call_dispositions DROP COLUMN IF EXISTS details;
DROP TABLE IF EXISTS anchor_phone_setter_v1_targeting_backup;

COMMIT;
