-- R8 deployment impact (READ ONLY, all tenants, aggregates only — no names or contacts).
-- Run before deploying the global review-hold setter gate. Per tenant:
--   held_rows                  prospects with ao_outreach_review_required = true
--   held_visible_now           held rows already setter-visible (they disappear from the setter
--                              queue at their next recompute / stage change / handoff after deploy)
--   held_visible_manual        of those, visible through a manual or engagement override
--   held_pre_fix_exposed       held rows the current (pre-R8) gate would expose on a handoff recompute
--   held_assigned_setter       held rows with an assigned setter (assignment keeps working; visibility does not)
-- Tenant 10 already applies the hold today, so its numbers should not change behaviour.
BEGIN TRANSACTION READ ONLY;
SET LOCAL statement_timeout = '30s';
SELECT client_id,
       count(*) FILTER (WHERE ao_outreach_review_required IS TRUE) AS held_rows,
       count(*) FILTER (WHERE ao_outreach_review_required IS TRUE AND setter_visible IS TRUE) AS held_visible_now,
       count(*) FILTER (WHERE ao_outreach_review_required IS TRUE AND setter_visible IS TRUE
                          AND coalesce(to_jsonb(p)->>'setter_visibility_reason', '') IN ('manual', 'engagement')) AS held_visible_manual,
       count(*) FILTER (WHERE ao_outreach_review_required IS TRUE
                          AND coalesce(status, '') NOT IN ('dead', 'disqualified', 'bounced', 'do_not_email')
                          AND coalesce(do_not_contact, false) = false
                          AND coalesce(icp_score, 0) >= 70
                          AND nullif(btrim(coalesce(service_area_match, '')), '') IS NOT NULL) AS held_pre_fix_exposed,
       count(*) FILTER (WHERE ao_outreach_review_required IS TRUE AND to_jsonb(p)->>'assigned_setter_id' IS NOT NULL) AS held_assigned_setter,
       count(*) FILTER (WHERE ao_outreach_review_required IS TRUE AND coalesce(setter_status, 'new') <> 'new') AS held_in_setter_pipeline
  FROM prospects p
 GROUP BY client_id
HAVING count(*) FILTER (WHERE ao_outreach_review_required IS TRUE) > 0
 ORDER BY client_id;
-- Where held rows come from (source x tenant).
SELECT client_id, source, count(*) AS held_rows
  FROM prospects WHERE ao_outreach_review_required IS TRUE GROUP BY 1, 2 ORDER BY 1, 2;
ROLLBACK;
