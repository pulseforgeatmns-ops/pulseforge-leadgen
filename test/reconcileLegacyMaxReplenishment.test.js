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

const ADMISSION_CONTEXT = {
  missionSegment: 'short_term_rental',
  service_area: ['Manchester', 'Bedford', 'Goffstown', 'Hooksett', 'Londonderry', 'Auburn'],
  clientConfig: {
    service_area: ['Manchester', 'Bedford', 'Goffstown', 'Hooksett', 'Londonderry', 'Auburn'],
  },
};

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
    {
      id: 'row-valid',
      company: 'Vacation Rental Management Co',
      website_url: 'https://example.com',
      domain: 'example.com',
      vertical: 'short_term_rental_management_manchester_nh',
      location: 'Manchester, NH',
      source: 'max_buffer_replenishment',
      notes: 'Discovered by Max-directed Scout inventory replenishment; no contact performed.',
      enrichment_attempts: 0,
    },
    {
      id: 'row-bad',
      company: 'NorthPoint Equipment Rentals',
      businessType: 'equipment rental',
      website_url: 'https://northpoint.example',
      domain: 'northpoint.example',
      vertical: 'short_term_rental_management_hooksett_nh',
      location: 'Hooksett, NH',
      source: 'max_buffer_replenishment',
      notes: null,
      enrichment_attempts: 0,
    },
  ];

  const plan = planLegacyReconciliation(rows, ADMISSION_CONTEXT, { dryRun: true, runId: 'test-run', at: '2026-09-25T00:00:00.000Z' });
  assert.equal(plan.scanned, 2);
  assert.equal(plan.summary.canonicalize, 1);
  assert.equal(plan.summary.remove, 1);
  assert.equal(plan.canonicalize[0].canonicalVertical, 'str_manager');
  assert.equal(plan.remove[0].reason, 'contradictory_business_type');
  assert.match(plan.canonicalize[0].notes, /legacy_reconciliation/);
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
  const first = await applyLegacyReconciliation(db, plan);
  assert.equal(first.appliedCounts.canonicalized, 1);

  const remaining = [...store.values()].filter(row => isNonCanonicalVertical(row.vertical));
  assert.equal(remaining.length, 0);

  const replay = await applyLegacyReconciliation(db, plan);
  assert.equal(replay.appliedCounts.canonicalized, 0);
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
  const rows = [{
    id: 'row-valid',
    company: 'Manchester Property Management LLC',
    website_url: 'https://mpm.example',
    domain: 'mpm.example',
    vertical: 'property_management_manchester_nh',
    location: 'Manchester, NH',
    source: 'max_buffer_replenishment',
    notes: null,
    enrichment_attempts: 0,
  }];
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
