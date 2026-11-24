-- Levi (tenant 21) prospecting-only pilot: READ ONLY pre-invite checks.
-- Aggregates only; no emails, names, phones or secrets are selected.
-- psql "$AUDIT_DATABASE_URL" -X -v ON_ERROR_STOP=0 -f docs/audits/levi-pilot-preinvite.sql
BEGIN TRANSACTION READ ONLY;
SET LOCAL statement_timeout = '15s';

SELECT current_database() AS database;                       -- expect pulseforge_production

-- P1 Tenant row. Expect active = true, enabled_agents limited to max (no emmett/sam/cal/paige/faye/link/riley),
--    legacy_pin_set = false (the /client/:id PIN portal has no rate limit; a set PIN is a weak parallel path).
SELECT id, active, enabled_agents, (nullif(btrim(coalesce(pin, '')), '') IS NOT NULL) AS legacy_pin_set
  FROM clients WHERE id = 21;

-- P2 Existing accounts bound to tenant 21 (roles only). Expect no setter/closer/sales/ao for tenant 21,
--    and no existing client account if Levi's is to be created fresh.
SELECT role, active, password_change_required, count(*) FROM users WHERE client_id = 21 GROUP BY 1, 2, 3;
-- Accounts that could act across tenants (admin/manager with no tenant binding) — informational.
SELECT role, count(*) FROM users WHERE role IN ('admin', 'manager') AND active GROUP BY 1;

-- P3 Outbound capability for tenant 21. Expect 0 rows in each.
SELECT status, count(*) FROM tenant_sending_identities WHERE tenant_id = '21' GROUP BY 1;
SELECT count(*) AS scheduled_sends FROM tenant_outreach_scheduled_sends WHERE tenant_id = '21';
SELECT status, count(*) FROM acquisition_outbound_items WHERE tenant_id = '21' GROUP BY 1;
SELECT status, count(*) FROM emmett_send_plans WHERE tenant_id = '21' OR client_id = 21 GROUP BY 1;
SELECT event_type, count(*) FROM email_events WHERE client_id = 21 GROUP BY 1;

-- P4 Prospect safety. Expect total 29, held 29 (every row review-held), setter_visible 0, contacted 0, dnc informational.
SELECT count(*) AS total,
       count(*) FILTER (WHERE ao_outreach_review_required IS TRUE) AS held,
       count(*) FILTER (WHERE setter_visible IS TRUE) AS setter_visible,
       count(*) FILTER (WHERE last_contacted_at IS NOT NULL) AS contacted,
       count(*) FILTER (WHERE do_not_contact IS TRUE) AS dnc,
       count(*) FILTER (WHERE do_not_contact IS FALSE) AS visible_in_prospect_list
  FROM prospects WHERE client_id = 21;
SELECT source, count(*) FILTER (WHERE ao_outreach_review_required IS NOT TRUE) AS not_held, count(*) AS total
  FROM prospects WHERE client_id = 21 GROUP BY 1;

-- P5 Weekly cycle (must not be replayed): exactly one claim for levi-week-2026-11-23.
SELECT payload->>'cycleId' AS cycle_id, count(*) FROM agent_log
 WHERE client_id = 21 AND agent_name = 'scout_levi' AND action = 'research_cycle_claim' GROUP BY 1;

ROLLBACK;
