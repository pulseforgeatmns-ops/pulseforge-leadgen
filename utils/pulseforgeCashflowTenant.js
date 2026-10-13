'use strict';

const pool = require('../db');
const { createMission } = require('../services/acquisitionMission');
const { createStructuredMission, MISSION_TYPES, SUCCESS_METRIC_TYPES } = require('../packages/acquisition-mission/StructuredMission');
const {
  PULSEFORGE_CLIENT_ID, PULSEFORGE_TENANT_ID, PULSEFORGE_CASHFLOW_OFFER,
  PULSEFORGE_CASHFLOW_OFFER_ID, PULSEFORGE_TARGET_INDUSTRIES, PULSEFORGE_TARGET_CITIES,
  FIRST_TOUCH_EXCLUSIONS_VERSION,
} = require('./pulseforgeCashflowOffer');

const PULSEFORGE_CASHFLOW_OBJECTIVE = 'Offer the $297 Pulseforge Missed Revenue Review to plumbing, heating and HVAC businesses in southern New Hampshire.';
const PULSEFORGE_TARGET_SEGMENT = 'Owner-led southern New Hampshire plumbing, heating and HVAC businesses.';
const PULSEFORGE_ELIGIBLE_SUBSEGMENTS = PULSEFORGE_TARGET_INDUSTRIES;

// Deliberately a draft. Offer confirmation is not approval of a persisted
// mission, send policy, recipient list, or automatic external communications.
function pulseforgeStructuredMission() {
  return {
    ...createStructuredMission({
      missionType: MISSION_TYPES.ACQUISITION,
      objective: PULSEFORGE_CASHFLOW_OBJECTIVE,
      successMetric: { type: SUCCESS_METRIC_TYPES.CUSTOMERS, target: 1 },
      market: { segment: 'small_business_owner', industry: 'home_services', buyer: 'owner',
        label: PULSEFORGE_TARGET_SEGMENT, eligibleSubsegments: PULSEFORGE_ELIGIBLE_SUBSEGMENTS },
      geography: { region: 'Southern New Hampshire', scope: 'regional', cities: PULSEFORGE_TARGET_CITIES },
      constraints: [
        'verified_company_bound_contact_only', 'one_first_touch_only',
        'exclude_prior_manual_contacts', 'implementation_quoted_separately',
        'private_workflow_access_requires_permission', 'no_revenue_promises', 'no_invented_findings',
      ],
      priority: 1,
      evidence: { minimumConfidence: 0.7, minimumBuyingSignals: 1, thresholdLabel: 'medium' },
      sourceText: PULSEFORGE_CASHFLOW_OBJECTIVE,
    }),
    offer: PULSEFORGE_CASHFLOW_OFFER,
    firstTouchExclusionsVersion: FIRST_TOUCH_EXCLUSIONS_VERSION,
  };
}

async function ensurePulseforgeCashflowTenant(db = pool) {
  const current = await db.query('SELECT * FROM clients WHERE id=$1 LIMIT 1', [PULSEFORGE_CLIENT_ID]);
  if (!current.rows[0]) throw Object.assign(new Error('Pulseforge client 1 is missing.'), { code: 'pulseforge_tenant_missing' });
  // Never overwrite live tenant strategy/settings as a side effect of creating
  // a review draft. Any historical tenant lead_with/notes correction is separate.
  return current.rows[0];
}
async function ensurePulseforgeCashflowMission(db = pool, { create = createMission } = {}) {
  const client = await ensurePulseforgeCashflowTenant(db);
  const existing = await db.query(
    `SELECT id,payload FROM acquisition_missions
      WHERE tenant_id=$1 AND (payload->'structuredMission'->'offer'->>'id'=$2
        OR payload->'missionPlanDraft'->'offer'->>'id'=$2)
      ORDER BY created_at DESC LIMIT 1`,
    [PULSEFORGE_TENANT_ID, PULSEFORGE_CASHFLOW_OFFER_ID]
  );
  if (existing.rows[0]) {
    // Preserve reviewed sources, historical copy, state, timestamps and hashes.
    return { client, missionId: existing.rows[0].id, mission: existing.rows[0].payload, created: false };
  }
  const structuredMission = pulseforgeStructuredMission();
  const mission = await create({
    tenantId: PULSEFORGE_TENANT_ID, clientId: PULSEFORGE_CLIENT_ID,
    objective: PULSEFORGE_CASHFLOW_OBJECTIVE, targetSegment: PULSEFORGE_TARGET_SEGMENT,
    title: 'Pulseforge $297 Missed Revenue Review', priority: 'high', owner: 'Jacob Maynard',
    createdBy: 'operator', missionPlanDraft: structuredMission, structuredMissionApproved: false,
    constraints: structuredMission.constraints,
  }, { pool: db, persist: true });
  return { client, missionId: mission.id, mission, created: true, reviewRequired: true };
}
module.exports = {
  PULSEFORGE_CASHFLOW_OBJECTIVE, PULSEFORGE_TARGET_SEGMENT, PULSEFORGE_ELIGIBLE_SUBSEGMENTS,
  PULSEFORGE_CASHFLOW_OFFER_ID, pulseforgeStructuredMission,
  ensurePulseforgeCashflowTenant, ensurePulseforgeCashflowMission,
};
