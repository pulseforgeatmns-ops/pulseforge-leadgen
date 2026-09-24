'use strict';

const pool = require('../db');
const { createMission } = require('../services/acquisitionMission');
const { resetEngine } = require('../services/acquisitionMission');

const MAYNARD_WEB_SLUG = 'maynard-web';
const MAYNARD_WEB_MISSION_OBJECTIVE =
  'Acquire one profitable website redesign client with contract value of at least $2,500 while minimizing operator acquisition time.';

const WEB_MISSION_CONSTRAINTS = Object.freeze({
  contract_floor: 2500,
  operator_hourly_rate: 50,
  operator_capacity_hours: 40,
  capacity_window_days: 30,
  geography: 'United States',
  outreach_enabled: false,
  cohort: 'WEB-COHORT-001',
});

async function findMaynardWebClient(db = pool) {
  const res = await db.query(
    `SELECT * FROM clients WHERE slug = $1 LIMIT 1`,
    [MAYNARD_WEB_SLUG]
  );
  return res.rows[0] || null;
}

async function ensureMaynardWebTenant(db = pool) {
  let client = await findMaynardWebClient(db);
  if (!client) {
    const inserted = await db.query(
      `INSERT INTO clients (
        name, slug, business_name, vertical, email, primary_contact,
        country, timezone, industry, service_area, verticals, target_clients,
        scoring_profile, enabled_agents, active, notes
      ) VALUES (
        'Maynard Web',
        $1,
        'Maynard Web',
        'web_design',
        'jacob@gopulseforge.com',
        'Jacob Maynard',
        'United States',
        'America/New_York',
        'Website design and development',
        ARRAY['United States'],
        ARRAY['professional_services','legal','accounting','home_services','dental','fitness','restaurant','salon','hvac','roofing','landscaping','med_spa'],
        'Established SMBs in the United States where website credibility and customer acquisition plausibly depend on web presence',
        'web_design',
        ARRAY['scout','max'],
        true,
        'SPEC-WEB-001 — neutral working identity; branding replaceable without architecture changes. NO outbound until explicitly authorized.'
      )
      ON CONFLICT (slug) DO UPDATE SET
        scoring_profile = EXCLUDED.scoring_profile,
        enabled_agents = EXCLUDED.enabled_agents,
        notes = EXCLUDED.notes
      RETURNING *`,
      [MAYNARD_WEB_SLUG]
    );
    client = inserted.rows[0];
  }

  return client;
}

async function ensureMaynardWebMission(db = pool, { reset = false } = {}) {
  const client = await ensureMaynardWebTenant(db);
  if (reset) resetEngine();

  let existing = { rows: [] };
  try {
    existing = await db.query(
      `SELECT payload->>'id' AS mission_id, payload
         FROM acquisition_missions
        WHERE client_id = $1
          AND payload->>'objective' = $2
        ORDER BY created_at DESC
        LIMIT 1`,
      [client.id, MAYNARD_WEB_MISSION_OBJECTIVE]
    );
  } catch {
    existing = { rows: [] };
  }

  if (existing.rows[0]?.mission_id) {
    return {
      client,
      missionId: existing.rows[0].mission_id,
      mission: existing.rows[0].payload,
      created: false,
    };
  }

  const mission = await createMission({
    tenantId: String(client.id),
    clientId: client.id,
    objective: MAYNARD_WEB_MISSION_OBJECTIVE,
    targetSegment: 'Established SMBs — United States',
    priority: 'high',
    createdBy: 'max',
    constraints: WEB_MISSION_CONSTRAINTS,
  }, { pool: db, persist: true });

  return {
    client,
    missionId: mission.id,
    mission,
    created: true,
  };
}

module.exports = {
  MAYNARD_WEB_SLUG,
  MAYNARD_WEB_MISSION_OBJECTIVE,
  WEB_MISSION_CONSTRAINTS,
  findMaynardWebClient,
  ensureMaynardWebTenant,
  ensureMaynardWebMission,
};
