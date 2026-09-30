'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const {
  runMaxOutboundControlLoop,
  sourceScope,
  defaultScoutRamp,
  resolveReplenishmentTenantContext,
  resolveScoutRampAllowedCities,
  _test: {
    scoutInput,
    persistDiscoveredCompanies,
    runEnrichmentBatches,
  },
} = require('../services/maxOutboundControlLoop');
const { buildAcquisitionSearchDefinition } = require('../services/scoutAcquisitionIntelligence');

const GREATER_MANCHESTER_CITIES = [
  'Manchester',
  'Hooksett',
  'Bedford',
  'Auburn',
  'Goffstown',
  'Londonderry',
];
const NH_CITY_SET = new Set(GREATER_MANCHESTER_CITIES.map(city => city.toLowerCase()));

const ANCHOR_SOURCE = {
  id: 'mission_anchor_str',
  tenantId: '10',
  objective: 'Acquire one recurring commercial cleaning client from a short-term rental operator in Greater Manchester.',
  payload: {
    structuredMission: {
      market: {
        segment: 'short_term_rental',
        industry: 'hospitality',
        capability: 'commercial_cleaning',
        businessType: 'commercial_cleaning',
      },
      geography: {
        region: 'Greater Manchester',
        cities: GREATER_MANCHESTER_CITIES.slice(),
      },
    },
    commercialCapability: 'commercial_cleaning',
    businessType: 'commercial_cleaning',
  },
};

const BABRUN_SOURCE = {
  id: 'mission_babrun_smb',
  tenantId: '13',
  objective: 'Book discovery calls with founder-led small business owners in the United States.',
  payload: {
    structuredMission: {
      market: {
        segment: 'small_business_owner',
        industry: 'founder_led_smb',
        capability: 'business_transformation',
        businessType: 'founder_led_smb',
      },
      geography: {
        region: 'United States',
        scope: 'nationwide',
        cities: [],
      },
    },
    commercialCapability: 'business_transformation',
    businessType: 'founder_led_smb',
  },
};

const PLAN = {
  deficit: 4,
  planningDailyCapacity: 2,
  safeDailyCapacity: 2,
  dispatchCapacityNow: 2,
  recommendedSafeDailyCapacity: 2,
  targetDays: 3,
  targetInventory: 6,
  cleanInventory: 2,
};

function infrastructure() {
  return {
    cap: 2,
    snapshot: { sentToday: 0 },
    assessed: { governor: { outcome: 'proceed' }, health: { score: 80 } },
    operating: {
      recommendedSafeDailyCapacity: 2,
      authorizationLimitedCapacity: 2,
      scheduleLimitedCapacity: 2,
      dispatchCapacityNow: 2,
      planningDailyCapacity: 2,
      dispatchableDailyCapacity: 2,
      effectiveDailyCapacity: 2,
      governor: 'proceed',
    },
  };
}

function programFor(tenantId, sourceId) {
  return {
    id: `outbound_${tenantId}`,
    tenant_id: String(tenantId),
    mode: 'active',
    policy_hash: 'policy_hash',
    source_mission_id: sourceId,
    policy: { dailyCap: 2 },
  };
}

function storeFor(tenantId, writes) {
  return {
    tenantId: String(tenantId),
    clientId: Number(tenantId),
    candidateOwnership: async () => null,
    event: async () => {},
    persistKnowledge: async (row) => {
      writes.ak.push(row);
      return row;
    },
  };
}

function candidateFor(tenantId) {
  if (String(tenantId) === '10') {
    return {
      name: 'Granite Vacation Stays',
      description: 'Airbnb and vacation rental management',
      location: 'Bedford, NH',
      website: 'https://granite-stays.example',
      domain: 'granite-stays.example',
    };
  }
  return {
    name: 'Founder Ledger Offices',
    description: 'Commercial office building management for owner-operated firms',
    location: 'Austin, TX',
    website: 'https://founder-ledger.example',
    domain: 'founder-ledger.example',
    vertical: 'commercial_office',
  };
}

function createTracePool(writes) {
  return {
    query: async (sql, params = []) => {
      if (/INSERT INTO scout_unenriched/i.test(sql)) {
        const row = {
          id: `unenriched-${writes.unenriched.length + 1}`,
          client_id: params[7],
          company: params[0],
          website_url: params[1],
          domain: params[2],
          vertical: params[3],
          location: params[4],
        };
        writes.unenriched.push(row);
        return { rowCount: 1, rows: [{ id: row.id }] };
      }
      if (/INSERT INTO prospects/i.test(sql)) {
        const row = {
          id: `prospect-${writes.prospects.length + 1}`,
          client_id: params.find(value => Number.isInteger(value) && (value === 10 || value === 13))
            || params[4],
        };
        writes.prospects.push(row);
        return { rowCount: 1, rows: [{ id: row.id }] };
      }
      if (/INSERT INTO acquisition_knowledge_objects/i.test(sql)) {
        writes.ak.push({
          tenant_id: params[2],
          client_id: params[3],
        });
        return { rowCount: 1, rows: [{ id: `ak-${writes.ak.length}` }] };
      }
      if (/FROM companies/i.test(sql)) return { rows: [], rowCount: 0 };
      return { rows: [], rowCount: 0 };
    },
  };
}

function createRampHooks(tenantId, writes, traces) {
  const enrichmentCalls = [];
  traces.enrichment.set(String(tenantId), enrichmentCalls);
  return {
    enrichment: {
      run: async (params) => {
        enrichmentCalls.push({
          client_id: params.client_id,
          tenantId: params.tenantId,
        });
        const owned = writes.unenriched.filter(row => Number(row.client_id) === Number(params.client_id));
        if (!owned.length) {
          return {
            client_id: params.client_id,
            considered: 0,
            promoted: 0,
            recovered: 0,
            unresolved: 0,
            emailResolved: 0,
            emailVerified: 0,
          };
        }
        writes.prospects.push({
          id: `prospect-${tenantId}-${writes.prospects.length + 1}`,
          client_id: params.client_id,
        });
        writes.ak.push({
          tenant_id: String(params.tenantId || params.client_id),
          client_id: Number(params.client_id),
        });
        return {
          client_id: params.client_id,
          considered: owned.length,
          promoted: 1,
          recovered: 0,
          unresolved: 0,
          emailResolved: 1,
          emailVerified: 1,
        };
      },
    },
    runDiscovery: async (input, opts) => {
      traces.scoutInput.set(String(tenantId), input);
      traces.search.set(String(tenantId), buildAcquisitionSearchDefinition(input));
      const persist = await opts.persistCompanies({
        tenantId: input.tenantId,
        companies: [candidateFor(tenantId)],
        searchDefinition: traces.search.get(String(tenantId)),
      });
      traces.persist.set(String(tenantId), persist);
      return { kind: 'investigate', delegated: true };
    },
  };
}

async function runForcedReplenishment(tenantId, source, traces) {
  const writes = { unenriched: [], prospects: [], ak: [] };
  traces.writes.set(String(tenantId), writes);
  const hooks = createRampHooks(tenantId, writes, traces);
  const result = await runMaxOutboundControlLoop({
    pool: createTracePool(writes),
    program: programFor(tenantId, source.id),
    source,
    store: storeFor(tenantId, writes),
    infrastructure: infrastructure(),
    inventory: { clean: [], excluded: [], scope: {}, exclusionCounts: {} },
    inventoryAfter: {
      clean: [{ prospectId: `${tenantId}-1` }, { prospectId: `${tenantId}-2` }],
      excluded: [],
      scope: {},
      exclusionCounts: {},
    },
    timestamps: {},
    funnel: {},
    skipVerificationRetry: true,
    scoutRamp: async (args) => defaultScoutRamp({
      ...args,
      skipVerificationRetry: true,
      enrichment: hooks.enrichment,
      runDiscovery: hooks.runDiscovery,
    }),
  });
  traces.results.set(String(tenantId), result);
  traces.allowedCities.set(String(tenantId), resolveScoutRampAllowedCities(sourceScope(source)));
  return result;
}

test('forced replenishment keeps tenant 10 Greater Manchester and tenant 13 nationwide isolated', async () => {
  const traces = {
    scoutInput: new Map(),
    search: new Map(),
    persist: new Map(),
    enrichment: new Map(),
    allowedCities: new Map(),
    writes: new Map(),
    results: new Map(),
  };

  const first = await runForcedReplenishment('10', ANCHOR_SOURCE, traces);
  const second = await runForcedReplenishment('13', BABRUN_SOURCE, traces);

  assert.equal(first.scout != null, true);
  assert.equal(second.scout != null, true);
  assert.ok(first.scout.discoveredQueued >= 1);
  assert.ok(second.scout.discoveredQueued >= 1);

  const anchorInput = traces.scoutInput.get('10');
  const babrunInput = traces.scoutInput.get('13');
  assert.equal(anchorInput.tenantId, '10');
  assert.equal(anchorInput.authorizedTenantId, '10');
  assert.equal(anchorInput.businessContext.serviceGeography, 'Greater Manchester');
  assert.equal(anchorInput.targetContext.geography, 'Greater Manchester');
  assert.equal(babrunInput.tenantId, '13');
  assert.equal(babrunInput.authorizedTenantId, '13');
  assert.equal(babrunInput.businessContext.serviceGeography, 'United States');
  assert.equal(babrunInput.targetContext.geography, 'United States');
  assert.equal(babrunInput.targetContext.geographyScope, 'nationwide');

  const anchorSearch = traces.search.get('10');
  const babrunSearch = traces.search.get('13');
  assert.equal(anchorSearch.geography.label, 'Greater Manchester');
  assert.ok(GREATER_MANCHESTER_CITIES.every(city => anchorSearch.geography.cities.includes(city)));
  assert.equal(babrunSearch.geography.label, 'United States');
  assert.equal(babrunSearch.geography.scope, 'nationwide');
  assert.deepEqual(babrunSearch.geography.cities, []);

  const anchorCities = traces.allowedCities.get('10').map(city => city.toLowerCase());
  const babrunCities = traces.allowedCities.get('13');
  assert.ok(GREATER_MANCHESTER_CITIES.every(city => anchorCities.includes(city.toLowerCase())));
  assert.deepEqual(babrunCities, []);
  assert.equal(babrunCities.some(city => NH_CITY_SET.has(String(city).toLowerCase())), false);

  const anchorEnrichment = traces.enrichment.get('10');
  const babrunEnrichment = traces.enrichment.get('13');
  assert.ok(anchorEnrichment.length >= 1);
  assert.ok(babrunEnrichment.length >= 1);
  assert.ok(anchorEnrichment.every(call => call.client_id === 10));
  assert.ok(babrunEnrichment.every(call => call.client_id === 13));
  assert.equal(anchorEnrichment.some(call => call.client_id === 13), false);
  assert.equal(babrunEnrichment.some(call => call.client_id === 10), false);

  assert.equal(traces.persist.get('10').tenantId, '10');
  assert.equal(traces.persist.get('10').clientId, 10);
  assert.equal(traces.persist.get('13').tenantId, '13');
  assert.equal(traces.persist.get('13').clientId, 13);

  const anchorWrites = traces.writes.get('10');
  const babrunWrites = traces.writes.get('13');
  assert.ok(anchorWrites.unenriched.length >= 1);
  assert.ok(babrunWrites.unenriched.length >= 1);
  assert.ok(anchorWrites.unenriched.every(row => Number(row.client_id) === 10));
  assert.ok(babrunWrites.unenriched.every(row => Number(row.client_id) === 13));
  assert.ok(anchorWrites.prospects.every(row => Number(row.client_id) === 10));
  assert.ok(babrunWrites.prospects.every(row => Number(row.client_id) === 13));
  assert.ok(anchorWrites.ak.every(row => String(row.tenant_id) === '10'));
  assert.ok(babrunWrites.ak.every(row => String(row.tenant_id) === '13'));
  assert.equal(anchorWrites.unenriched.some(row => Number(row.client_id) === 13), false);
  assert.equal(babrunWrites.unenriched.some(row => Number(row.client_id) === 10), false);
  assert.equal(babrunWrites.ak.some(row => String(row.tenant_id) === '10'), false);

  assert.notEqual(babrunInput.businessContext.serviceGeography, 'Greater Manchester');
  assert.equal(
    resolveScoutRampAllowedCities(sourceScope(BABRUN_SOURCE)).some(city => NH_CITY_SET.has(city.toLowerCase())),
    false
  );
});

test('empty nationwide cities are not converted into Anchor NH defaults', () => {
  const cities = resolveScoutRampAllowedCities({
    region: 'United States',
    scope: 'nationwide',
    cities: [],
  });
  assert.deepEqual(cities, []);
  assert.equal(cities.some(city => NH_CITY_SET.has(String(city).toLowerCase())), false);
});

test('Greater Manchester mission still resolves the Anchor city set', () => {
  const cities = resolveScoutRampAllowedCities(sourceScope(ANCHOR_SOURCE));
  assert.deepEqual(
    cities.map(city => city.toLowerCase()).sort(),
    GREATER_MANCHESTER_CITIES.map(city => city.toLowerCase()).sort()
  );
});

test('missing tenant context fails closed instead of defaulting to 10', async () => {
  await assert.rejects(
    () => runEnrichmentBatches({ run: async () => ({ considered: 0 }) }, {}, 1),
    { code: 'governed_outbound_tenant_required' }
  );
  assert.throws(
    () => resolveReplenishmentTenantContext({}),
    { code: 'governed_outbound_tenant_required' }
  );
  assert.throws(
    () => scoutInput(
      { source_mission_id: 'mission_source' },
      {
        payload: {
          structuredMission: {
            market: { segment: 'short_term_rental' },
            geography: { region: 'Greater Manchester' },
          },
        },
      },
      PLAN
    ),
    { code: 'governed_outbound_tenant_required' }
  );
  await assert.rejects(
    () => persistDiscoveredCompanies({}, { candidateOwnership: async () => null }, {
      companies: [candidateFor('10')],
      scoutContext: { scope: { segment: 'short_term_rental' } },
    }),
    { code: 'governed_outbound_tenant_required' }
  );
  await assert.rejects(
    () => defaultScoutRamp({
      pool: { query: async () => ({ rows: [] }) },
      store: { candidateOwnership: async () => null },
      program: { id: 'outbound_missing', mode: 'active' },
      source: {
        payload: {
          structuredMission: {
            market: { segment: 'short_term_rental' },
            geography: { region: 'Greater Manchester' },
          },
        },
      },
      plan: PLAN,
      skipVerificationRetry: true,
    }),
    { code: 'governed_outbound_tenant_required' }
  );
});

test('replenishment path source no longer hardcodes tenant 10 or Anchor service_area', () => {
  const src = fs.readFileSync(path.join(__dirname, '../services/maxOutboundControlLoop.js'), 'utf8');
  assert.doesNotMatch(src, /client_id:\s*10/);
  assert.doesNotMatch(src, /service_area:\s*\[\s*'Manchester'/);
  assert.doesNotMatch(src, /tenantId === '13'\s*\?\s*'United States'/);
  assert.doesNotMatch(src, /tenantId === '13'\s*\?\s*'business_transformation'/);
});
