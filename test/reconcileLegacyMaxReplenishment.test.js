'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const {
  parseArgs,
  isNonCanonicalVertical,
  parseDiscoveryFromNotes,
  rowToCandidate,
  planLegacyReconciliation,
  applyLegacyReconciliation,
  runLegacyMaxReplenishmentReconciliation,
} = require('../scripts/reconcileLegacyMaxReplenishment');
const { resolveLegacyReconciliationOutcome } = require('../utils/replenishmentVertical');
const { _test: { persistDiscoveredCompanies } } = require('../services/maxOutboundControlLoop');

const ADMISSION_CONTEXT = {
  missionSegment: 'short_term_rental',
  service_area: ['Manchester', 'Bedford', 'Goffstown', 'Hooksett', 'Londonderry', 'Auburn'],
  clientConfig: {
    service_area: ['Manchester', 'Bedford', 'Goffstown', 'Hooksett', 'Londonderry', 'Auburn'],
  },
};

function legacyRow(overrides = {}) {
  return {
    id: overrides.id || 'row-id',
    company: overrides.company || 'Example Co',
    website_url: overrides.website_url || 'https://example.com',
    domain: overrides.domain || 'example.com',
    vertical: overrides.vertical || 'short_term_rental_management_manchester_nh',
    location: overrides.location || 'Manchester, NH',
    source: 'max_buffer_replenishment',
    notes: overrides.notes ?? null,
    enrichment_attempts: 0,
    ...overrides,
  };
}

test('parseArgs defaults to dry-run and requires confirm-production for apply', () => {
  assert.deepEqual(parseArgs([]), { dryRun: true, apply: false, confirmProduction: false });
  assert.throws(() => parseArgs(['--apply']), /confirm-production/);
  assert.deepEqual(
    parseArgs(['--confirm-production', '--apply']),
    { dryRun: false, apply: true, confirmProduction: true }
  );
});

test('isNonCanonicalVertical flags legacy query slugs and accepts canonical values', () => {
  assert.equal(isNonCanonicalVertical('short_term_rental_management_manchester_nh'), true);
  assert.equal(isNonCanonicalVertical('str_manager'), false);
  assert.equal(isNonCanonicalVertical('property_manager'), false);
});

test('planLegacyReconciliation canonicalizes valid legacy rows and removes contradictory rows', () => {
  const rows = [
    legacyRow({
      id: 'row-valid',
      company: 'Vacation Rental Management Co',
      description: 'Full-service Airbnb and vacation rental management',
      vertical: 'short_term_rental_management_manchester_nh',
    }),
    legacyRow({
      id: 'row-bad',
      company: 'NorthPoint Equipment Rentals',
      businessType: 'equipment rental',
      domain: 'northpoint.example',
      website_url: 'https://northpoint.example',
      vertical: 'short_term_rental_management_hooksett_nh',
      location: 'Hooksett, NH',
    }),
  ];

  const plan = planLegacyReconciliation(rows, ADMISSION_CONTEXT, { dryRun: true, runId: 'test-run', at: '2026-09-25T00:00:00.000Z' });
  assert.equal(plan.scanned, 2);
  assert.equal(plan.summary.canonicalize, 1);
  assert.equal(plan.summary.hold, 0);
  assert.equal(plan.summary.remove, 1);
  assert.equal(plan.canonicalize[0].canonicalVertical, 'str_manager');
  assert.equal(plan.remove[0].reason, 'contradictory_business_type');
  assert.match(plan.canonicalize[0].notes, /legacy_reconciliation/);
});

test('Sentry legacy unclassifiable row => HOLD', () => {
  const plan = planLegacyReconciliation([
    legacyRow({
      id: 'sentry',
      company: 'Sentry Management',
      domain: 'sentrymanagement.com',
      website_url: 'https://sentrymanagement.com',
      vertical: 'property_management_manchester_nh',
    }),
  ], ADMISSION_CONTEXT, { dryRun: true });

  assert.equal(plan.summary.hold, 1);
  assert.equal(plan.summary.remove, 0);
  assert.equal(plan.hold[0].reason, 'unclassifiable_vertical');
  assert.equal(plan.hold[0].reconciliation.action, 'hold');
});

test('PMI W legacy unclassifiable row => HOLD', () => {
  const plan = planLegacyReconciliation([
    legacyRow({
      id: 'pmi',
      company: 'PMI W Properties',
      domain: 'pmiwproperties.com',
      website_url: 'https://pmiwproperties.com',
      vertical: 'airbnb_property_management_bedford_nh',
      location: 'Bedford, NH',
    }),
  ], ADMISSION_CONTEXT, { dryRun: true });

  assert.equal(plan.summary.hold, 1);
  assert.equal(plan.summary.remove, 0);
  assert.equal(plan.hold[0].reason, 'unclassifiable_vertical');
});

test('equipment rental with explicit contradictory evidence => REMOVE', () => {
  const plan = planLegacyReconciliation([
    legacyRow({
      id: 'equip',
      company: 'NorthPoint Equipment Rentals',
      businessType: 'equipment rental',
      domain: 'northpoint.example',
      website_url: 'https://northpoint.example',
      vertical: 'short_term_rental_management_hooksett_nh',
      location: 'Hooksett, NH',
    }),
  ], ADMISSION_CONTEXT, { dryRun: true });

  assert.equal(plan.summary.remove, 1);
  assert.equal(plan.remove[0].reason, 'contradictory_business_type');
});

test('outside geography => REMOVE', () => {
  const plan = planLegacyReconciliation([
    legacyRow({
      id: 'far',
      company: 'Boston Property Management LLC',
      description: 'Residential property management services',
      domain: 'bostonpm.example',
      website_url: 'https://bostonpm.example',
      vertical: 'property_management_boston_ma',
      location: 'Boston, MA',
    }),
  ], ADMISSION_CONTEXT, { dryRun: true });

  assert.equal(plan.summary.remove, 1);
  assert.equal(plan.remove[0].reason, 'outside_geography');
});

test('applyLegacyReconciliation preserves held rows and only writes canonicalize/remove', async () => {
  const store = new Map([
    ['row-valid', {
      id: 'row-valid',
      client_id: 10,
      company: 'Vacation Rental Management Co',
      website_url: 'https://example.com',
      domain: 'example.com',
      vertical: 'short_term_rental_management_manchester_nh',
      location: 'Manchester, NH',
      source: 'max_buffer_replenishment',
      notes: 'Full-service Airbnb and vacation rental management',
      enrichment_attempts: 0,
    }],
    ['row-held', {
      id: 'row-held',
      client_id: 10,
      company: 'Sentry Management',
      website_url: 'https://sentrymanagement.com',
      domain: 'sentrymanagement.com',
      vertical: 'property_management_manchester_nh',
      location: 'Manchester, NH',
      source: 'max_buffer_replenishment',
      notes: null,
      enrichment_attempts: 0,
    }],
  ]);

  const db = {
    query: async (sql, params) => {
      if (/UPDATE scout_unenriched/i.test(sql)) {
        const id = params[2];
        const row = store.get(id);
        if (!row || row.vertical !== params[5]) return { rowCount: 0, rows: [] };
        row.vertical = params[0];
        row.notes = params[1];
        return { rowCount: 1, rows: [{ id }] };
      }
      if (/DELETE FROM scout_unenriched/i.test(sql)) {
        const id = params[0];
        const row = store.get(id);
        if (!row || row.vertical !== params[3]) return { rowCount: 0, rows: [] };
        store.delete(id);
        return { rowCount: 1, rows: [{ id }] };
      }
      return { rowCount: 0, rows: [] };
    },
  };

  const rows = [...store.values()];
  const plan = planLegacyReconciliation(rows, ADMISSION_CONTEXT, { dryRun: false, runId: 'apply-run', at: '2026-09-25T00:00:00.000Z' });
  assert.equal(plan.summary.hold, 1);
  const first = await applyLegacyReconciliation(db, plan);
  assert.equal(first.appliedCounts.canonicalized, 1);
  assert.equal(first.appliedCounts.removed, 0);
  assert.ok(store.has('row-held'));
  assert.equal(store.get('row-held').vertical, 'property_management_manchester_nh');
});

test('applyLegacyReconciliation is idempotent for already-canonical rows', async () => {
  const store = new Map([
    ['row-valid', {
      id: 'row-valid',
      client_id: 10,
      company: 'Vacation Rental Management Co',
      website_url: 'https://example.com',
      domain: 'example.com',
      vertical: 'short_term_rental_management_manchester_nh',
      location: 'Manchester, NH',
      source: 'max_buffer_replenishment',
      notes: 'Airbnb and vacation rental management',
      enrichment_attempts: 0,
    }],
  ]);

  const db = {
    query: async (sql, params) => {
      if (/UPDATE scout_unenriched/i.test(sql)) {
        const id = params[2];
        const row = store.get(id);
        if (!row || row.vertical !== params[5]) return { rowCount: 0, rows: [] };
        row.vertical = params[0];
        row.notes = params[1];
        return { rowCount: 1, rows: [{ id }] };
      }
      if (/DELETE FROM scout_unenriched/i.test(sql)) {
        const id = params[0];
        const row = store.get(id);
        if (!row || row.vertical !== params[3]) return { rowCount: 0, rows: [] };
        store.delete(id);
        return { rowCount: 1, rows: [{ id }] };
      }
      return { rowCount: 0, rows: [] };
    },
  };

  const rows = [...store.values()];
  const plan = planLegacyReconciliation(rows, ADMISSION_CONTEXT, { dryRun: false, runId: 'apply-run', at: '2026-09-25T00:00:00.000Z' });
  const first = await applyLegacyReconciliation(db, plan);
  assert.equal(first.appliedCounts.canonicalized, 1);

  const remaining = [...store.values()].filter(row => isNonCanonicalVertical(row.vertical));
  assert.equal(remaining.length, 0);

  const replay = await applyLegacyReconciliation(db, plan);
  assert.equal(replay.appliedCounts.canonicalized, 0);
});

test('malformed legacy held row does not block fresh canonical rediscovery', async () => {
  const insertedRows = [
    {
      id: 'legacy-held',
      client_id: 10,
      company: 'Sentry Management',
      website_url: 'https://sentrymanagement.com',
      domain: 'sentrymanagement.com',
      vertical: 'property_management_manchester_nh',
      location: 'Manchester, NH',
      source: 'max_buffer_replenishment',
      enrichment_attempts: 0,
    },
  ];

  const pool = {
    query: async (sql, params) => {
      if (/INSERT INTO scout_unenriched/i.test(sql)) {
        const domain = params[2];
        const company = params[0];
        const canonicalVerticals = params[6] || [];
        const blocked = insertedRows.some(row => {
          const sameIdentity = row.domain.toLowerCase() === domain.toLowerCase()
            || row.company.trim().toLowerCase() === company.trim().toLowerCase();
          if (!sameIdentity) return false;
          const legacyMalformed = row.source === 'max_buffer_replenishment'
            && Number(row.enrichment_attempts || 0) === 0
            && !canonicalVerticals.includes(String(row.vertical || '').trim().toLowerCase());
          return !legacyMalformed;
        });
        if (blocked) return { rowCount: 0, rows: [] };
        const newRow = {
          id: `row-${insertedRows.length + 1}`,
          client_id: 10,
          company: params[0],
          website_url: params[1],
          domain: params[2],
          vertical: params[3],
          location: params[4],
          source: 'max_buffer_replenishment',
          enrichment_attempts: 0,
          notes: params[5],
        };
        insertedRows.push(newRow);
        return { rowCount: 1, rows: [{ id: newRow.id }] };
      }
      return { rowCount: 0, rows: [] };
    },
  };
  const store = { candidateOwnership: async () => null };

  const result = await persistDiscoveredCompanies(pool, store, {
    companies: [{
      name: 'Sentry Management',
      description: 'Residential property management services',
      location: 'Manchester, NH',
      website: 'https://sentrymanagement.com',
      domain: 'sentrymanagement.com',
    }],
    scoutContext: {
      scope: { segment: 'short_term_rental' },
      serviceAreas: ADMISSION_CONTEXT.service_area,
    },
  });

  assert.equal(result.inserted, 1);
  assert.equal(insertedRows.at(-1).vertical, 'property_manager');
});

test('canonical existing row still blocks duplicate admission', async () => {
  const insertedRows = [
    {
      id: 'canonical-existing',
      client_id: 10,
      company: 'Granite State Property Management',
      website_url: 'https://granitepm.com',
      domain: 'granitepm.com',
      vertical: 'property_manager',
      location: 'Bedford, NH',
      source: 'max_buffer_replenishment',
      enrichment_attempts: 0,
    },
  ];

  const pool = {
    query: async (sql, params) => {
      if (/INSERT INTO scout_unenriched/i.test(sql)) {
        const domain = params[2];
        const company = params[0];
        const canonicalVerticals = params[6] || [];
        const blocked = insertedRows.some(row => {
          const sameIdentity = row.domain.toLowerCase() === domain.toLowerCase()
            || row.company.trim().toLowerCase() === company.trim().toLowerCase();
          if (!sameIdentity) return false;
          const legacyMalformed = row.source === 'max_buffer_replenishment'
            && Number(row.enrichment_attempts || 0) === 0
            && !canonicalVerticals.includes(String(row.vertical || '').trim().toLowerCase());
          return !legacyMalformed;
        });
        if (blocked) return { rowCount: 0, rows: [] };
        return { rowCount: 1, rows: [{ id: 'new' }] };
      }
      return { rowCount: 0, rows: [] };
    },
  };
  const store = { candidateOwnership: async () => null };

  const result = await persistDiscoveredCompanies(pool, store, {
    companies: [{
      name: 'Granite State Property Management',
      description: 'Residential property management services',
      location: 'Bedford, NH',
      website: 'https://granitepm.com',
      domain: 'granitepm.com',
    }],
    scoutContext: {
      scope: { segment: 'short_term_rental' },
      serviceAreas: ADMISSION_CONTEXT.service_area,
    },
  });

  assert.equal(result.inserted, 0);
});

test('rowToCandidate preserves discovery metadata from notes and legacy slug context', () => {
  const candidate = rowToCandidate({
    company: 'Example Co',
    website_url: 'https://example.com',
    domain: 'example.com',
    vertical: 'airbnb_property_management_manchester_nh',
    location: 'Manchester, NH',
    source: 'max_buffer_replenishment',
    notes: 'Base note | discovery: {"discoveryConcept":"Airbnb property management Manchester NH","discoveryCity":"Manchester NH","discoverySource":"google_maps"}',
  });
  assert.equal(candidate.discoveryConcept, 'Airbnb property management Manchester NH');
  assert.equal(candidate.discoveryCity, 'Manchester NH');
});

test('runLegacyMaxReplenishmentReconciliation dry-run performs no writes', async () => {
  let writes = 0;
  const rows = [legacyRow({
    id: 'row-valid',
    company: 'Manchester Property Management LLC',
    description: 'Full-service property management for residential rentals',
    domain: 'mpm.example',
    website_url: 'https://mpm.example',
    vertical: 'property_management_manchester_nh',
  })];
  const db = {
    query: async (sql) => {
      if (/UPDATE scout_unenriched|DELETE FROM scout_unenriched/i.test(sql)) writes += 1;
      if (/CREATE TABLE IF NOT EXISTS scout_unenriched/i.test(sql)) return { rows: [] };
      return { rows: [] };
    },
  };

  const report = await runLegacyMaxReplenishmentReconciliation({
    db,
    rows,
    admissionContext: ADMISSION_CONTEXT,
    args: { dryRun: true, apply: false, confirmProduction: false },
    skipEnsureTable: true,
  });

  assert.equal(writes, 0);
  assert.equal(report.dryRun, true);
  assert.equal(report.summary.canonicalize, 1);
});

test('parseDiscoveryFromNotes extracts embedded discovery JSON', () => {
  const notes = 'Discovered by Max | discovery: {"discoveryConcept":"short term rental management Manchester NH","discoveryCity":"Manchester NH"} | legacy_reconciliation: {"action":"canonicalized"}';
  assert.deepEqual(parseDiscoveryFromNotes(notes), {
    discoveryConcept: 'short term rental management Manchester NH',
    discoveryCity: 'Manchester NH',
  });
});

test('resolveLegacyReconciliationOutcome maps insufficient_business_fit to hold', () => {
  assert.deepEqual(
    resolveLegacyReconciliationOutcome({ admitted: false, reason: 'insufficient_business_fit' }),
    { outcome: 'hold', reason: 'insufficient_business_fit', detail: null }
  );
});
