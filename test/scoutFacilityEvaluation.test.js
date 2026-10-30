'use strict';
const test = require('node:test'), assert = require('node:assert/strict'), fs = require('node:fs');
const { evaluate, markdown, exportCohort } = require('../scripts/evaluateScoutFacilityResearch.cjs');
const bytes = fs.readFileSync(require.resolve('./fixtures/scout-p0-004/synthetic-evaluation.json'), 'utf8');

test('offline evaluation is deterministic, non-mutating and honest about incomplete evidence', () => {
  const input = JSON.parse(bytes), before = structuredClone(input);
  const a = evaluate(input, bytes), b = evaluate(input, bytes);
  assert.deepEqual(input, before); assert.deepEqual(a, b);
  assert.deepEqual(a.cohort, { expected: 7, evaluated: 7, missing: 0, completeRawCohort: false });
  assert.equal(a.summary.newlyRecognizedAwaiting.length, 3);
  assert.equal(a.summary.awaitingWithSuitableCategoryAndGeography.length, 2);
  assert.equal(a.summary.demonstrablyUnsuitableRetained.length, 1);
  assert.deepEqual(a.summary.geographicCorrections, ['Synthetic Foreign School']);
  assert.equal(a.queryCoverage.proposedCategories, 8); assert.equal(a.queryCoverage.proposedTowns, 8);
  assert.equal(a.summary.databaseWrites, 0); assert.equal(a.summary.googleCalls, 0);
  assert.match(markdown(a), /7\/7/); assert.match(markdown(a), /PENDING/);
  assert.equal(a.rows[1].proposed.admitted, false, 'missing identity/program evidence is not invented');
  for (const row of a.rows) assert.equal(row.proposed.facilityDecision.procurement.independentPurchasingAuthority, false);
});

test('export is exact-grant, Tenant 10 and transactionally read-only', async () => {
  const statements = [];
  const db = { release() {}, query: async (sql, values) => {
    statements.push(sql); assert.match(sql, /^(BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY|SET LOCAL|SELECT|COMMIT|ROLLBACK)/);
    if (/FROM scout_research_only_grants/.test(sql)) {
      assert.deepEqual(values, ['a'.repeat(64)]); assert.match(sql, /tenant_id='10'/);
      return { rows: [{ grant_id: 'a'.repeat(64), search_fingerprint: 'b'.repeat(64), research_mission_id: 'm' }] };
    }
    if (/FROM scout_research_missions/.test(sql)) return { rows: [{ payload: { structuredMission: {
      market: { eligibleSubsegments: ['managed_facilities', 'restaurant_foh'] }, geography: { cities: ['Manchester'] } } } }] };
    if (/AS tickets/.test(sql)) return { rows: [{ tickets: 1, observations: 1, raw_bytes: '500' }] };
    if (/FROM scout_candidate_work/.test(sql)) return { rows: [{ candidate_key: 'c', state: 'blocked', reason: 'business_evidence_not_authorized',
      observations: [{ raw: { name: 'Synthetic Primary School', placeTypes: ['primary_school'], address: '101 Fixture Way, Manchester, NH 03101, USA', website: 'school.example' } }] }] };
    return { rows: [] };
  } };
  await assert.rejects(exportCohort({ connect: () => assert.fail('invalid grant cannot connect') }, 'invalid'));
  const exported = await exportCohort({ connect: async () => db }, 'a'.repeat(64));
  assert.equal(exported.records.length, 1); assert.equal(exported.authoritativeRawExport, true);
  assert.equal(evaluate(exported).cohort.completeRawCohort, false);
  assert.equal(statements[0], 'BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY'); assert.equal(statements.at(-1), 'COMMIT');
});
