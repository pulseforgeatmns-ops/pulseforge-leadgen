'use strict';

/**
 * Proposes an immutable successor to Anchor's current acquisition mission with
 * the current facility-buyer ICP, and activates it only on explicit operator
 * confirmation after the prepared envelope has drained.
 *
 * The 2026-10-03 mission still approves realtor, short-term-rental and
 * residential-leaning property-manager subsegments over six cities. Scout's
 * query plan, admission and inventory scope all derive from it, so Scout keeps
 * producing that inventory until the mission itself is revised.
 *
 * This changes approved targeting only. It grants no research, enrichment,
 * preparation or send authority and does not touch the recovery controls.
 * Existing realtor/STR prospects are kept; they stop counting as clean inventory
 * for this program because they are outside the revised scope.
 *
 * Review: node scripts/activateAnchorFacilityBuyerMission.js
 * Apply:  node scripts/activateAnchorFacilityBuyerMission.js --apply --confirm=anchor-facility-buyer-icp-2026-10-28
 */

require('dotenv').config();
const pool = require('../db');
const { hash } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const { plan, apply } = require('./activateAnchorInventoryCapacityMission');

const TENANT_ID = '10';
const CONFIRMATION = 'anchor-facility-buyer-icp-2026-10-28';
const VERSION = '2026-10-28';

// Priority order is meaningful: discovery buys at most one new page per cycle.
// managed_facilities covers schools/campuses, commercial property managers and
// offices, manufacturers/warehouses and dealerships; restaurants stay FOH-only.
const ELIGIBLE_SUBSEGMENTS = Object.freeze(['managed_facilities', 'restaurant_foh']);
const SERVICE_CITIES = Object.freeze([
  'Manchester', 'Bedford', 'Londonderry', 'Hooksett', 'Auburn', 'Goffstown', 'Nashua', 'Merrimack',
]);
const RETIRED_SUBSEGMENTS = Object.freeze(['realtor', 'str_manager', 'property_manager', 'commercial_office']);

function facilityBuyerSuccessorMission(row) {
  const now = new Date().toISOString();
  const payload = structuredClone(row.payload || {});
  const structured = payload.structuredMission || {};
  const market = structured.market || {};
  const geography = structured.geography || {};
  const constraints = [...new Set([
    ...(Array.isArray(payload.constraints) ? payload.constraints : []),
    'Priority: schools/campuses/large facilities; commercial property managers with facility-service purchasing responsibility; manufacturers, warehouses and distribution centers; multi-location operators and dealerships.',
    'Residential real estate agents and brokerage offices are not targeted.',
    'Short-term-rental hosts and small offices without meaningful recurring contract potential are not targeted.',
    'Restaurant acquisition is front-of-house only.',
    'Service geography: Manchester, Bedford, Londonderry, Hooksett, Auburn, Goffstown, Nashua and Merrimack, NH.',
  ])];
  const id = `mission_anchor_facility_buyers_${hash([row.id, ELIGIBLE_SUBSEGMENTS, SERVICE_CITIES, VERSION]).slice(0, 20)}`;
  return {
    id,
    tenantId: TENANT_ID,
    clientId: row.client_id,
    stage: row.stage,
    status: row.status,
    objective: `${row.objective} Revised ${VERSION}: prioritize institutional, commercial-property, industrial and multi-site facility buyers with recurring cleaning contracts across the full Anchor service geography.`,
    targetSegment: row.target_segment,
    campaign: row.campaign,
    title: `${row.title || 'Anchor acquisition'} — facility buyers`,
    priority: row.priority,
    confidence: row.confidence,
    owner: row.owner,
    createdBy: 'operator_authorized_facility_buyer_icp',
    orchestrationMissionId: row.orchestration_mission_id,
    structuredMission: {
      ...structured,
      market: { ...market, eligibleSubsegments: [...ELIGIBLE_SUBSEGMENTS], retiredSubsegments: [...RETIRED_SUBSEGMENTS] },
      geography: { ...geography, cities: [...SERVICE_CITIES] },
      immutable: true,
    },
    constraints,
    predecessorMissionId: row.id,
    authorizationEvidence: {
      kind: 'operator_instruction',
      receivedAt: now,
      scope: 'Anchor P0 facility-buyer ICP: managed facilities and restaurant FOH across eight NH service cities; realtor/STR retired',
    },
    createdAt: now,
    updatedAt: now,
  };
}

if (require.main === module) {
  const doApply = process.argv.includes('--apply');
  const confirm = process.argv.find(arg => arg.startsWith('--confirm='))?.split('=')[1];
  if (doApply && confirm !== CONFIRMATION) throw new Error(`Refusing apply without --confirm=${CONFIRMATION}`);
  (doApply ? apply(pool, facilityBuyerSuccessorMission) : plan(pool, facilityBuyerSuccessorMission).then(row => ({
    apply: false,
    currentMissionId: row.source.id,
    currentEligibleSubsegments: row.source.payload?.structuredMission?.market?.eligibleSubsegments || null,
    successorMissionId: row.mission.id,
    eligibleSubsegments: row.mission.structuredMission.market.eligibleSubsegments,
    cities: row.mission.structuredMission.geography.cities,
    pending: row.pending,
    policyHash: row.policyHash,
    scopeHash: row.scopeHash,
  })))
    .then(result => console.log(JSON.stringify(result, null, 2)))
    .catch(err => { console.error(err.message || err); process.exitCode = 1; })
    .finally(() => pool.end().catch(() => {}));
}

module.exports = { CONFIRMATION, ELIGIBLE_SUBSEGMENTS, SERVICE_CITIES, facilityBuyerSuccessorMission };
