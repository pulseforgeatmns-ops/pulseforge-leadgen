'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { buildDiscoveryPlan } = require('../packages/scout/coverage/DiscoveryCoverageEngine');
const { buildQueriesForEvidence } = require('../packages/capabilities/discovery/providers/PlacesProvider');
const { evaluateReplenishmentAdmission } = require('../utils/replenishmentVertical');
const { babrunPromotionContactResolution } = require('../scripts/promoteUnenriched');
const { fillActiveAoAccounts } = require('../utils/aoAccountFill');
const { successorMission } = require('../scripts/activateAnchorInventoryCapacityMission');

function searchDefinition(generation) {
  return {
    tenantId: '10',
    businessNeed: 'commercial_cleaning',
    segments: ['property_manager'],
    geography: { label: 'Manchester, NH', cities: ['Manchester'], state: 'NH' },
    discoveryGeneration: generation,
  };
}

test('zero-yield search generations use distinct approved concepts and provider queries', () => {
  const adapter = { id: 'places', sourceType: 'public_business_data', available: () => true };
  const first = buildDiscoveryPlan(searchDefinition(0), { adapters: [adapter] });
  const next = buildDiscoveryPlan(searchDefinition(1), { adapters: [adapter] });
  assert.notDeepEqual(next.concepts, first.concepts);
  assert.deepEqual(next.concepts, ['HOA Management', 'Condominium Management', 'Apartment Management']);

  const queries = buildQueriesForEvidence({
    segment: 'property_management',
    evidenceType: 'identity',
    cities: ['Manchester'],
    state: 'NH',
    discoveryConcept: next.concepts[0],
  });
  assert.deepEqual(queries, ['HOA Management Manchester NH']);
});

test('Manchester restaurant FOH is admitted while BOH-only businesses stay separate', () => {
  const context = {
    missionSegments: ['restaurant_foh'],
    missionCities: ['manchester'],
    allowedCities: ['manchester'],
  };
  const foh = evaluateReplenishmentAdmission({
    name: 'Elm Street Bistro', domain: 'elm-bistro.example', location: 'Manchester, NH',
    description: 'Independent restaurant and bistro with dining room',
  }, context);
  assert.equal(foh.admitted, true);
  assert.equal(foh.vertical, 'restaurant');

  const boh = evaluateReplenishmentAdmission({
    name: 'Queen City Commissary', domain: 'qc-kitchen.example', location: 'Manchester, NH',
    description: 'Commercial kitchen and food production facility',
  }, context);
  assert.deepEqual(boh, { admitted: false, reason: 'restaurant_boh_only' });
});

test('Babrun promotion writes founder classification only for verified attributable first-party email', () => {
  const founder = babrunPromotionContactResolution({
    enriched: {
      email: 'maya@sampleco.example', contact: 'Maya Chen', title: 'Founder',
      source: ['website_email'], sourceUrl: 'https://sampleco.example/about',
    },
    verification: {
      emailVerified: true, emailStatus: 'valid', emailVerificationMethod: 'bouncer',
    },
    officialDomain: 'sampleco.example',
  });
  assert.equal(founder.contactResolution.finalState, 'VERIFIED_FOUNDER_EMAIL');

  const unattributed = babrunPromotionContactResolution({
    enriched: {
      email: 'maya@sampleco.example', contact: 'Maya Chen', title: 'Founder',
      source: ['third_party'], sourceUrl: 'https://directory.example/maya',
    },
    verification: { emailVerified: true, emailStatus: 'valid' },
    officialDomain: 'sampleco.example',
  });
  assert.equal(unattributed.contactResolution.finalState, 'REVIEW_REQUIRED');

  const attributedProvider = babrunPromotionContactResolution({
    enriched: {
      email: 'maya@sampleco.example', contact: 'Maya Chen', title: 'Owner',
      source: ['prospeo'], sourceUrl: null,
    },
    verification: { emailVerified: true, emailStatus: 'valid' },
    officialDomain: 'sampleco.example',
  });
  assert.equal(attributedProvider.contactResolution.finalState, 'VERIFIED_FOUNDER_EMAIL');
});

test('AO refill triggers below 12, fills toward target, and assigns each company once', async () => {
  let candidateSql = '';
  const db = {
    query: async (sql, params = []) => {
      const compact = String(sql).replace(/\s+/g, ' ');
      if (/FROM users u/.test(compact)) return { rows: [{ id: 24, name: 'Tony', active: true }] };
      if (/COUNT\(\*\)::int AS n/.test(compact)) return { rows: [{ n: 11 }] };
      if (/FROM prospects p LEFT JOIN companies c/.test(compact)) {
        candidateSql = compact;
        const base = {
          client_id: 10, do_not_contact: false, vertical: 'property_manager', icp_score: 90,
          phone: '603-555-0100', service_area_match: 'Manchester', company_location: 'Manchester, NH',
        };
        return { rows: [
          { ...base, id: '00000000-0000-0000-0000-000000000001', company_id: 'co-a', company_name: 'A PM' },
          { ...base, id: '00000000-0000-0000-0000-000000000002', company_id: 'co-a', company_name: 'A PM alt' },
          { ...base, id: '00000000-0000-0000-0000-000000000003', company_id: 'co-b', company_name: 'B PM' },
        ] };
      }
      return { rows: [], rowCount: 0 };
    },
  };
  const result = await fillActiveAoAccounts({
    clientId: 10, db, dryRun: true, minPerAo: 12, targetPerAo: 13,
  });
  assert.equal(result.assigned.length, 2);
  assert.equal(new Set(result.assigned.map(row => row.company)).size, 2);
  assert.match(candidateSql, /acquisition_outbound_items governed/);
  assert.match(candidateSql, /email_verified/);
});

test('AO refill mutations serialize inside a tenant-scoped transaction', async () => {
  const calls = [];
  let released = false;
  const client = {
    query: async (sql) => {
      const compact = String(sql).replace(/\s+/g, ' ').trim();
      calls.push(compact);
      if (/FROM users u/.test(compact)) return { rows: [{ id: 24, name: 'Tony', active: true }] };
      if (/COUNT\(\*\)::int AS n/.test(compact)) return { rows: [{ n: 20 }] };
      return { rows: [], rowCount: 0 };
    },
    release: () => { released = true; },
  };
  const db = {
    query: async () => ({ rows: [], rowCount: 0 }),
    connect: async () => client,
  };

  const result = await fillActiveAoAccounts({ clientId: 10, db, dryRun: false });
  assert.equal(result.assigned.length, 0);
  assert.equal(calls[0], 'BEGIN');
  assert.match(calls[1], /pg_advisory_xact_lock\(hashtext\(\$1\)\)/);
  assert.equal(calls.at(-1), 'COMMIT');
  assert.equal(released, true);
});

test('Anchor successor mission adds only bounded restaurant FOH scope and preserves immutable predecessor', () => {
  const source = {
    id: 'source', client_id: 10, stage: 'ready', status: 'active', objective: 'Find property managers',
    target_segment: 'commercial_cleaning', campaign: null, title: 'Anchor', priority: 'high',
    confidence: 0.9, owner: 'max', payload: {
      tenantId: '10', structuredMission: { immutable: true, market: { eligibleSubsegments: ['property_manager'] } },
      constraints: ['Restaurants excluded'],
    },
  };
  const before = JSON.stringify(source);
  const next = successorMission(source);
  assert.deepEqual(next.structuredMission.market.eligibleSubsegments, ['property_manager', 'restaurant_foh']);
  assert.ok(next.constraints.some(value => /Back-of-house/.test(value)));
  assert.equal(next.structuredMission.immutable, true);
  assert.equal(JSON.stringify(source), before);
});
