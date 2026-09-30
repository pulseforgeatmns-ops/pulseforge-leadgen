'use strict';

/**
 * SPEC-SUBSTRAL-PF-001 — Studio Substral first-class PulseForge tenant bootstrap.
 * Isolated from Anchor Cleaning, Babrun/Fedir, and Maynard Web.
 */

const pool = require('../db');
const { createMission, resetEngine } = require('../services/acquisitionMission');

const STUDIO_SUBSTRAL_SLUG = 'studio-substral';
const STUDIO_SUBSTRAL_DOMAIN = 'studiosubstral.com';

const SUBSTRAL_MISSION_OBJECTIVE =
  'Acquire one paid Studio Substral website assessment from an established business with a live website and a real decision about what to fix, rebuild, or leave alone.';

const SUBSTRAL_MISSION_CONSTRAINTS = Object.freeze({
  mission_kind: 'paid_website_assessment',
  success_criterion: 'one_paid_assessment',
  primary_cta: 'website_assessment',
  outreach_enabled: false,
  emmett_authorized: false,
  geography: 'United States',
  sales_doctrine: 'diagnosis_before_design',
});

const SUBSTRAL_BRAND_VOICE = [
  'Diagnosis before design.',
  'Use contractions where natural, plain language, and short sentences.',
  'Open with evidence specific to the prospect’s site — never generic agency language.',
  'Sell getting the decision right; do not manufacture urgency or fear about the website.',
  'Do not assume a redesign is required.',
  'Primary CTA: paid website assessment — not redesign consultation, free strategy call, or website makeover.',
].join(' ');

const SUBSTRAL_NEVER_SAY = [
  'free strategy call',
  'website makeover',
  'redesign consultation',
  'your site is broken',
  'you are losing customers',
  'urgent website fix',
].join('; ');

async function findStudioSubstralClient(db = pool) {
  if (!db) return null;
  const res = await db.query(`SELECT * FROM clients WHERE slug = $1 LIMIT 1`, [STUDIO_SUBSTRAL_SLUG]);
  return res.rows[0] || null;
}

async function ensureStudioSubstralTenant(db = pool) {
  let client = await findStudioSubstralClient(db);
  if (!client) {
    const inserted = await db.query(
      `INSERT INTO clients (
        name, slug, business_name, vertical, email, primary_contact,
        country, timezone, industry, website, service_area, verticals, target_clients,
        scoring_profile, enabled_agents, active, notes,
        brand_voice, never_say, lead_with, sender_name, sender_email, sending_domain
      ) VALUES (
        'Studio Substral',
        $1,
        'Studio Substral',
        'web_design',
        'hello@studiosubstral.com',
        'Jacob Maynard',
        'United States',
        'America/New_York',
        'Website diagnosis, targeted remediation, redesign and build',
        'https://studiosubstral.com',
        ARRAY['United States'],
        ARRAY['professional_services','legal','accounting','home_services','dental','fitness','restaurant','salon','hvac','roofing','landscaping','med_spa'],
        'Established businesses with a live website, meaningful commercial value on that site, and a real decision about what to fix, rebuild, or leave alone.',
        'studio_substral',
        ARRAY['scout','max','paige'],
        true,
        'SPEC-SUBSTRAL-PF-001 — first-class tenant. NO Emmett/outbound until governed mailbox readiness passes.',
        $2,
        $3,
        'Evidence-first website assessment',
        'Studio Substral',
        'hello@studiosubstral.com',
        'studiosubstral.com'
      )
      ON CONFLICT (slug) DO UPDATE SET
        scoring_profile = EXCLUDED.scoring_profile,
        enabled_agents = EXCLUDED.enabled_agents,
        website = EXCLUDED.website,
        brand_voice = EXCLUDED.brand_voice,
        never_say = EXCLUDED.never_say,
        lead_with = EXCLUDED.lead_with,
        sender_email = EXCLUDED.sender_email,
        sending_domain = EXCLUDED.sending_domain,
        notes = EXCLUDED.notes
      RETURNING *`,
      [STUDIO_SUBSTRAL_SLUG, SUBSTRAL_BRAND_VOICE, SUBSTRAL_NEVER_SAY]
    );
    client = inserted.rows[0];
  }
  return client;
}

const SUBSTRAL_MISSION_PROFILE = Object.freeze({
  ...SUBSTRAL_MISSION_CONSTRAINTS,
  doctrine: Object.freeze([
    'diagnosis_before_design',
    'redesign_not_assumed',
    'assessment_satisfies_initial_mission',
  ]),
  valid_downstream_conclusions: Object.freeze([
    'TARGETED_FIX',
    'REDESIGN_BUILD',
    'NO_WORK_REQUIRED',
    'INSUFFICIENT_EVIDENCE',
  ]),
});

async function applySubstralMissionProfile(db, clientId, missionId) {
  if (!missionId) return;
  await db.query(
    `UPDATE acquisition_missions
        SET payload = jsonb_set(
          jsonb_set(payload, '{studio_substral_profile}', $1::jsonb, true),
          '{targetSegment}',
          to_jsonb($2::text),
          true
        )
      WHERE client_id = $3
        AND payload->>'id' = $4`,
    [
      JSON.stringify(SUBSTRAL_MISSION_PROFILE),
      'Established businesses with live websites — United States',
      clientId,
      missionId,
    ]
  );
}

async function ensureStudioSubstralMission(db = pool, { reset = false } = {}) {
  const client = await ensureStudioSubstralTenant(db);
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
      [client.id, SUBSTRAL_MISSION_OBJECTIVE]
    );
  } catch {
    existing = { rows: [] };
  }

  if (existing.rows[0]?.mission_id) {
    await applySubstralMissionProfile(db, client.id, existing.rows[0].mission_id);
    const refreshed = await db.query(
      `SELECT payload FROM acquisition_missions
        WHERE client_id = $1 AND payload->>'id' = $2 LIMIT 1`,
      [client.id, existing.rows[0].mission_id]
    );
    return {
      client,
      missionId: existing.rows[0].mission_id,
      mission: refreshed.rows[0]?.payload || existing.rows[0].payload,
      created: false,
    };
  }

  const mission = await createMission({
    tenantId: String(client.id),
    clientId: client.id,
    objective: SUBSTRAL_MISSION_OBJECTIVE,
    targetSegment: 'Established businesses with live websites — United States',
    priority: 'high',
    createdBy: 'max',
    constraints: SUBSTRAL_MISSION_CONSTRAINTS,
  }, { pool: db, persist: true });

  await applySubstralMissionProfile(db, client.id, mission.id);
  const refreshed = await db.query(
    `SELECT payload FROM acquisition_missions
      WHERE client_id = $1 AND payload->>'id' = $2 LIMIT 1`,
    [client.id, mission.id]
  );

  return {
    client,
    missionId: mission.id,
    mission: refreshed.rows[0]?.payload || mission,
    created: true,
  };
}

async function resolveStudioSubstralClientId(db = pool) {
  const configured = process.env.STUDIO_SUBSTRAL_CLIENT_ID;
  if (configured) {
    const raw = Number(configured);
    if (!Number.isSafeInteger(raw) || raw < 1) {
      throw new Error('Invalid Studio Substral review tenant');
    }
    return raw;
  }
  const client = await ensureStudioSubstralTenant(db);
  return client.id;
}

function isStudioSubstralScoringProfile(scoringProfile) {
  return scoringProfile === 'studio_substral';
}

function usesWebsiteOpportunityIntelligence(scoringProfile) {
  return scoringProfile === 'web_design' || scoringProfile === 'studio_substral';
}

module.exports = {
  STUDIO_SUBSTRAL_SLUG,
  STUDIO_SUBSTRAL_DOMAIN,
  SUBSTRAL_MISSION_OBJECTIVE,
  SUBSTRAL_MISSION_CONSTRAINTS,
  SUBSTRAL_MISSION_PROFILE,
  SUBSTRAL_BRAND_VOICE,
  findStudioSubstralClient,
  ensureStudioSubstralTenant,
  ensureStudioSubstralMission,
  resolveStudioSubstralClientId,
  isStudioSubstralScoringProfile,
  usesWebsiteOpportunityIntelligence,
};
