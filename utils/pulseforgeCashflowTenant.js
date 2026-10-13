'use strict';

const pool = require('../db');
const { createMission } = require('../services/acquisitionMission');
const { ensureClientTenantWorkspaceBinding } = require('../services/tenantWorkspace');
const {
  createStructuredMission,
  freezeStructuredMission,
  MISSION_TYPES,
  SUCCESS_METRIC_TYPES,
} = require('../packages/acquisition-mission/StructuredMission');
const {
  PULSEFORGE_CLIENT_ID,
  PULSEFORGE_TENANT_ID,
  PULSEFORGE_CASHFLOW_OFFER,
  PULSEFORGE_CASHFLOW_OFFER_ID,
} = require('./pulseforgeCashflowOffer');

const PULSEFORGE_CASHFLOW_OBJECTIVE =
  'Collect the first $500 to $1,000 in cash by October 18, 2026, by selling one or two fixed-scope Pulseforge website repair sprints at $500 each, paid upfront.';

const PULSEFORGE_TARGET_SEGMENT = 'Owner-led United States businesses with a live website and an observable contact, booking, mobile, or lead-path defect.';

const PULSEFORGE_ELIGIBLE_SUBSEGMENTS = Object.freeze([
  'home_services',
  'professional_services',
  'dental',
  'fitness',
  'restaurant',
  'salon',
  'hvac',
  'roofing',
  'landscaping',
  'med_spa',
]);

function pulseforgeStructuredMission(approvedAt = new Date().toISOString()) {
  const draft = createStructuredMission({
    missionType: MISSION_TYPES.ACQUISITION,
    objective: PULSEFORGE_CASHFLOW_OBJECTIVE,
    successMetric: { type: SUCCESS_METRIC_TYPES.REVENUE, target: 500 },
    market: {
      segment: 'small_business_owner',
      industry: 'local_services',
      buyer: 'owner_or_marketing_lead',
      label: PULSEFORGE_TARGET_SEGMENT,
      eligibleSubsegments: PULSEFORGE_ELIGIBLE_SUBSEGMENTS,
    },
    geography: { region: 'United States', scope: 'nationwide', cities: [] },
    constraints: [
      'observable_website_defect_required',
      'targeted_remediation_only',
      'verified_company_bound_contact_only',
      'one_first_touch_only',
      'no_redesign_pitch',
      'cash_collection_target_2026_10_18',
    ],
    priority: 1,
    evidence: { minimumConfidence: 0.7, minimumBuyingSignals: 1, thresholdLabel: 'medium' },
    sourceText: PULSEFORGE_CASHFLOW_OBJECTIVE,
  });
  return freezeStructuredMission({
    ...draft,
    offer: PULSEFORGE_CASHFLOW_OFFER,
  }, { approvedBy: 'operator', approvedAt });
}

async function ensurePulseforgeCashflowTenant(db = pool) {
  const current = await db.query('SELECT * FROM clients WHERE id=$1 LIMIT 1', [PULSEFORGE_CLIENT_ID]);
  if (!current.rows[0]) {
    throw Object.assign(new Error('Pulseforge client 1 is missing.'), { code: 'pulseforge_tenant_missing' });
  }
  const updated = await db.query(
    `UPDATE clients SET
       name='Pulseforge', business_name='Pulseforge', primary_contact='Jacob Maynard',
       email='jacob@pulseforge.com', country='United States', timezone='America/New_York',
       industry='Fixed-scope website repair', website='https://pulseforge.com',
       target_clients=$2, scoring_profile='web_design',
       enabled_agents=ARRAY['scout','max','paige','emmett'], active=true,
       autosend_enabled=false,
       notes=$3, brand_voice=$4, never_say=$5, lead_with=$6,
       sender_name='Jacob Maynard', sender_email='jacob@pulseforge.com', sending_domain='pulseforge.com'
     WHERE id=$1 RETURNING *`,
    [
      PULSEFORGE_CLIENT_ID,
      PULSEFORGE_TARGET_SEGMENT,
      'Pulseforge tenant 1 cash-flow motion. Governed outbound only; website evidence, verified contacts, Paige doctrine, and Emmett capacity remain mandatory.',
      'Plain, direct, evidence-first, useful, and specific. Speak as Jacob. State the fixed price and scope without hype.',
      'free audit; guaranteed results; your site is broken; losing customers; full redesign; vague agency pitch',
      '$500 fixed-scope website repair sprint, paid upfront, targeting agreed fixes within 48 hours',
    ]
  );
  await ensureClientTenantWorkspaceBinding({
    pool: db,
    clientId: PULSEFORGE_CLIENT_ID,
    client: updated.rows[0],
    origin: 'bootstrap',
  });
  return updated.rows[0];
}

async function ensurePulseforgeCashflowMission(db = pool) {
  const client = await ensurePulseforgeCashflowTenant(db);
  const existing = await db.query(
    `SELECT id,payload FROM acquisition_missions
      WHERE tenant_id=$1 AND objective=$2
      ORDER BY created_at DESC LIMIT 1`,
    [PULSEFORGE_TENANT_ID, PULSEFORGE_CASHFLOW_OBJECTIVE]
  ).catch(() => ({ rows: [] }));
  if (existing.rows[0]) {
    const structuredMission = pulseforgeStructuredMission(
      existing.rows[0].payload?.structuredMission?.approvedAt || new Date().toISOString()
    );
    const refreshed = await db.query(
      `UPDATE acquisition_missions SET
         stage='ready', status='Ready', target_segment=$3,
         payload=payload || jsonb_build_object(
           'stage', 'ready',
           'status', 'Ready',
           'structuredMission', $4::jsonb,
           'structuredMissionApproved', true,
           'cashflowOffer', $5::jsonb
         ),
         updated_at=NOW()
       WHERE tenant_id=$1 AND id=$2 RETURNING *`,
      [
        PULSEFORGE_TENANT_ID,
        existing.rows[0].id,
        PULSEFORGE_TARGET_SEGMENT,
        JSON.stringify(structuredMission),
        JSON.stringify(PULSEFORGE_CASHFLOW_OFFER),
      ]
    );
    return { client, missionId: refreshed.rows[0].id, mission: refreshed.rows[0].payload, created: false };
  }

  const structuredMission = pulseforgeStructuredMission();
  const mission = await createMission({
    tenantId: PULSEFORGE_TENANT_ID,
    clientId: PULSEFORGE_CLIENT_ID,
    objective: PULSEFORGE_CASHFLOW_OBJECTIVE,
    targetSegment: PULSEFORGE_TARGET_SEGMENT,
    title: 'Pulseforge $500 Website Repair Sprint',
    priority: 'high',
    owner: 'Jacob Maynard',
    createdBy: 'max',
    stage: 'ready',
    structuredMission,
    structuredMissionApproved: true,
    skipMissionPlanning: true,
    constraints: structuredMission.constraints,
  }, { pool: db, persist: true });
  const persisted = await db.query(
    `UPDATE acquisition_missions
       SET payload=jsonb_set(payload, '{cashflowOffer}', $3::jsonb, true), updated_at=NOW()
     WHERE tenant_id=$1 AND id=$2 RETURNING *`,
    [PULSEFORGE_TENANT_ID, mission.id, JSON.stringify(PULSEFORGE_CASHFLOW_OFFER)]
  );
  return { client, missionId: mission.id, mission: persisted.rows[0]?.payload || mission, created: true };
}

module.exports = {
  PULSEFORGE_CASHFLOW_OBJECTIVE,
  PULSEFORGE_TARGET_SEGMENT,
  PULSEFORGE_ELIGIBLE_SUBSEGMENTS,
  PULSEFORGE_CASHFLOW_OFFER_ID,
  pulseforgeStructuredMission,
  ensurePulseforgeCashflowTenant,
  ensurePulseforgeCashflowMission,
};
