'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { isNationwideMissionScope, isProspectServiceAreaConfirmed } = require('../utils/missionGeography');
const { qualifyKnowledgeContact, normalizedKnowledgeContent, discoverGovernedInventory } = require('../services/acquisitionMissionInventory');
const { loadCleanInventory } = require('../services/maxOutboundControlLoop');
const { governedContactReason, contactEvidence } = require('../utils/governedContactEligibility');

const BABRUN_SCOPE = {
  segment: 'small_business_owners',
  region: 'United States',
  scope: 'nationwide',
  cities: [],
};

const mission = {
  tenantId: '13',
  structuredMission: {
    market: { segment: 'small_business_owners' },
    geography: { region: 'United States', scope: 'nationwide', cities: [] },
  },
};

function cohortJeremyRow(overrides = {}) {
  return {
    id: 'prospect-jeremy',
    client_id: 13,
    company_id: 'co-jeremy',
    company_name: "Barco's Painting of Colorado",
    email: 'jeremy@barcospainting.com',
    email_verified: true,
    email_status: 'valid',
    vertical: 'unknown',
    service_area_match: null,
    do_not_contact: false,
    acquisition_metadata: {
      contactResolution: {
        finalState: 'VERIFIED_FOUNDER_EMAIL',
        bestEmail: 'jeremy@barcospainting.com',
      },
      provenance: { operationalizedAt: '2026-09-28T21:08:25.371Z' },
    },
    knowledge_id: 'ak_babrun_cohort002_c002',
    knowledge_content: {
      company: "Barco's Painting of Colorado",
      contact: 'Jeremy Barton',
      role: 'Owner',
      location: 'Castle Rock, Colorado',
      website: 'https://barcospainting.com/',
      cohort: 'babrun_cohort_002',
      icpEvaluation: { fit: true, reasons: [{ kind: 'OBSERVED', text: 'Owner-operated painting company' }] },
      contactResolution: { finalState: 'VERIFIED_FOUNDER_EMAIL', bestEmail: 'jeremy@barcospainting.com' },
    },
    knowledge_provenance: {
      scoutRun: 'babrun_cohort_002',
      sourceUrls: ['https://barcospainting.com/', 'https://barcospainting.com/about/'],
      sourceVerification: { fetchedAt: '2026-09-28T21:08:06.711Z' },
    },
    approved_asset: null,
    ...overrides,
  };
}

test('nationwide Babrun scope confirms service area without service_area_match string', () => {
  assert.equal(isNationwideMissionScope(BABRUN_SCOPE), true);
  assert.equal(
    isProspectServiceAreaConfirmed({ service_area_match: null, company_location: null }, BABRUN_SCOPE),
    true
  );
});

test('cohort AK normalizes operating evidence and qualifies founder inventory without rediscovery', () => {
  const row = cohortJeremyRow();
  const normalized = normalizedKnowledgeContent(row);
  assert.ok(normalized.operatingEvidence?.ownerName);
  assert.equal(qualifyKnowledgeContact(row, mission), null);
  assert.equal(governedContactReason(row, { tenantId: '13' }), null);
  assert.equal(contactEvidence(row).classification, 'VERIFIED_FOUNDER_EMAIL');
});

test('loadCleanInventory surfaces verified founder cohort inventory for tenant 13', async () => {
  const jeremy = cohortJeremyRow();
  const ventura = cohortJeremyRow({
    id: 'prospect-ventura',
    email: 'luis@venturalawncare.com',
    company_name: 'Ventura Landscape',
    knowledge_id: 'ak_babrun_prospect_p013',
    acquisition_metadata: {
      contactResolution: {
        finalState: 'VERIFIED_FOUNDER_EMAIL',
        bestEmail: 'luis@venturalawncare.com',
      },
    },
    knowledge_content: {
      company: 'Ventura Landscape',
      contact: 'Luis Ventura',
      cohort: 'babrun_first_ten',
      icpEvaluation: { fit: true, reasons: [{ kind: 'OBSERVED', text: 'Founder-led landscaping business' }] },
      location: 'Houston, Texas',
    },
  });
  const roleInbox = cohortJeremyRow({
    id: 'prospect-role',
    email: 'hello@thelemoncleaning.com',
    acquisition_metadata: {
      contactResolution: { finalState: 'VERIFIED_ROLE_EMAIL', bestEmail: 'hello@thelemoncleaning.com' },
    },
  });
  const reviewRequired = cohortJeremyRow({
    id: 'prospect-review',
    email: null,
    acquisition_metadata: { contactResolution: { finalState: 'REVIEW_REQUIRED', bestEmail: null } },
  });

  const pool = {
    query: async (sql) => {
      if (/assigned_ao_id IS NOT NULL|prior_touch/.test(sql)) return { rows: [] };
      if (/SELECT p\.\*, p\.id AS prospect_id/.test(sql)) {
        return { rows: [jeremy, ventura, roleInbox, reviewRequired] };
      }
      if (/FROM prospects p\s+JOIN companies c ON c\.id=p\.company_id AND c\.client_id=p\.client_id\s+WHERE p\.client_id=\$1\s+AND p\.email IS NOT NULL/.test(sql)) {
        return { rows: [jeremy, ventura, roleInbox, reviewRequired] };
      }
      return { rows: [] };
    },
  };
  const store = {
    clientId: 13,
    candidateOwnership: async (candidate) => (
      candidate.prospectId === 'prospect-ventura' ? 'already_attempted' : null
    ),
    suppression: async () => null,
  };
  const source = {
    payload: {
      structuredMission: mission.structuredMission,
    },
  };
  const inventory = await loadCleanInventory(pool, store, source, 13, { tenantId: '13' });
  assert.equal(inventory.clean.length, 1);
  assert.equal(inventory.clean[0].email, 'jeremy@barcospainting.com');
  assert.equal(inventory.excluded.find(row => row.prospectId === 'prospect-ventura')?.reason, 'already_attempted');
  assert.ok(inventory.excluded.some(row => row.prospectId === 'prospect-role'));
  assert.ok(!inventory.clean.some(row => row.prospectId === 'prospect-review'));
});

test('discoverGovernedInventory reuses tenant 13 clean inventory before net-new Scout', async () => {
  const jeremy = cohortJeremyRow();
  const pool = {
    query: async (sql) => {
      if (/assigned_ao_id IS NOT NULL|prior_touch/.test(sql)) return { rows: [] };
      if (/SELECT p\.\*, p\.id AS prospect_id/.test(sql)) return { rows: [jeremy] };
      if (/FROM prospects p\s+JOIN companies c ON c\.id=p\.company_id AND c\.client_id=p\.client_id\s+WHERE p\.client_id=\$1\s+AND p\.email IS NOT NULL/.test(sql)) {
        return { rows: [jeremy] };
      }
      if (/SELECT p.id,p.company_id,p.vertical/.test(sql)) {
        return {
          rows: [{
            id: jeremy.id,
            company_id: jeremy.company_id,
            vertical: 'home_services',
            service_area_match: 'Castle Rock, Colorado',
            name: jeremy.company_name,
            website: 'https://barcospainting.com',
            domain: 'barcospainting.com',
            location: 'Castle Rock, Colorado',
            enrichment_provenance: { email: { source: 'bouncer', source_url: 'https://barcospainting.com/', resolved_at: '2026-09-28T21:08:06.711Z' } },
          }],
        };
      }
      return { rows: [] };
    },
  };
  const missionPayload = {
    id: 'mission_validation_test',
    tenantId: '13',
    orchestrationMissionId: 'mission_validation_test',
    structuredMission: mission.structuredMission,
  };
  const { hash, missionScope } = require('../packages/acquisition-mission/DailyOutboundPolicy');
  const governedProgram = {
    source_mission_id: 'mission_validation_test',
    scope_hash: hash(missionScope(missionPayload)),
    policy: { tenantId: '13' },
  };
  const result = await discoverGovernedInventory(missionPayload, { pool, governedProgram });
  assert.ok(result);
  assert.equal(result.payload.source, 'governed_clean_inventory');
  assert.equal(result.payload.qualifiedCount, 1);
});
