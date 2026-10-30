'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const { exportCohort, exportForAdmin, LIMITS } = require('../services/scoutResearchRetrospective');
const GRANT = 'a'.repeat(64), FINGERPRINT = 'b'.repeat(64);
function fixture(overrides = {}) {
  const statements = [], observations = [{ raw: { name: 'Synthetic School', website: 'https://www.school.example/path',
    placeTypes: ['primary_school'], address: 'Manchester, NH 03101, USA' }, queryKey: 'q', ordinal: 0, createdAt: '2026-10-29T16:00:00Z' },
  { raw: { name: 'Synthetic School', website: 'school.example', placeTypes: ['school'], address: 'Manchester, England, UK' }, queryKey: 'q2', ordinal: 0 }];
  const defaults = {
    grant: { grant_id: GRANT, search_fingerprint: FINGERPRINT, research_mission_id: 'synthetic-mission', baseline_revision: 'c'.repeat(40) },
    mission: { payload: { structuredMission: { market: { eligibleSubsegments: ['managed_facilities', 'restaurant_foh'] }, geography: { cities: ['Manchester'] } } } },
    counts: { tickets: 1, observations: 2, raw_bytes: '500' },
    work: [{ candidate_key: 'c', plan_version: 'plan', ticket_version: 'ticket', state: 'handled', reason: 'queued', observations }],
    retained: [{ company: 'Synthetic School', domain: 'school.example', vertical: 'school', cycle_number: 3 }],
  };
  const data = { ...defaults, ...overrides };
  let released = false;
  const pool = { connect: async () => ({ release() { released = true; }, query: async (sql, values) => {
    statements.push(sql); assert.match(sql, /^(BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY|SET LOCAL|SELECT|COMMIT|ROLLBACK)/);
    if (/FROM scout_research_only_grants/.test(sql)) { assert.deepEqual(values, [GRANT]); assert.match(sql, /tenant_id='10'/); return { rows: data.grant ? [data.grant] : [] }; }
    if (/FROM scout_research_missions/.test(sql)) { assert.match(sql, /tenant_id='10'/); return { rows: data.mission ? [data.mission] : [] }; }
    if (/AS tickets/.test(sql)) return { rows: [data.counts] };
    if (/FROM scout_candidate_work w/.test(sql)) { assert.deepEqual(values, [FINGERPRINT]); return { rows: data.work }; }
    if (/FROM scout_research_candidates/.test(sql)) { assert.deepEqual(values, [GRANT]); return { rows: data.retained }; }
    return { rows: [] };
  } }) };
  return { pool, data, statements, released: () => released };
}

test('exact-grant export preserves contradictory raw evidence and identifies its search-plan scope', async () => {
  const f = fixture(), before = structuredClone(f.data);
  const result = await exportForAdmin(f.pool, { grantId: GRANT });
  assert.deepEqual(f.data, before); assert.equal(result.tenantId, '10'); assert.equal(result.grantId, GRANT);
  assert.equal(result.records[0].previous.status, 'retained');
  assert.deepEqual(result.records[0].candidate.classificationObservations.map(x => x.address),
    ['Manchester, NH 03101, USA', 'Manchester, England, UK']);
  assert.equal(result.records[0].source.memberships[1].queryKey, 'q2');
  assert.equal(result.retained.length, 1); assert.equal(result.exportScope.fingerprint, FINGERPRINT);
  assert.equal(result.baselineRevision, 'c'.repeat(40)); assert.equal(result.authoritativeRawExport, true);
  assert.equal(f.statements.at(-1), 'COMMIT'); assert.equal(f.released(), true);
});

test('no implicit grant, caller-specified tenant, partial cohort or missing mission is accepted', async () => {
  const never = { connect() { assert.fail('invalid input reached the database'); } };
  for (const input of [null, {}, { grantId: GRANT, tenantId: '13' }, { grantId: 'invalid' }])
    await assert.rejects(async () => exportForAdmin(never, input), /research_only_retrospective_grant_required/);
  for (const overrides of [{ grant: null }, { mission: null }, { work: [] }, { counts: { tickets: LIMITS.tickets + 1, observations: 2, raw_bytes: '1' } },
    { counts: { tickets: 1, observations: LIMITS.observations + 1, raw_bytes: '1' } },
    { counts: { tickets: 1, observations: 2, raw_bytes: String(LIMITS.bytes + 1) } }]) {
    const f = fixture(overrides); await assert.rejects(exportCohort(f.pool, GRANT));
    assert.equal(f.statements.at(-1), 'ROLLBACK'); assert.equal(f.released(), true);
  }
});

test('every prior ticket outcome remains distinguishable and missing memberships remain missing', async () => {
  const reasons = ['queued', 'business_evidence_not_authorized', 'evidence_budget_deferred', 'ownership_policy_observed',
    'geography_unresolved', 'identity_incomplete', 'policy_excluded'];
  const work = reasons.map((reason, i) => ({ candidate_key: String(i), state: reason === 'queued' ? 'handled' : 'blocked', reason,
    observations: i === 5 ? null : [{ raw: { name: `Synthetic ${i}` } }] }));
  const f = fixture({ work, retained: [], counts: { tickets: 7, observations: 6, raw_bytes: '500' } });
  const result = await exportCohort(f.pool, GRANT);
  assert.deepEqual(result.records.map(row => row.previous.reason), reasons);
  assert.equal(result.records[0].previous.status, 'accepted_not_retained');
  assert.equal(result.records[1].previous.status, 'awaiting_website_evidence');
  assert.equal(result.authoritativeRawExport, false);
  assert.deepEqual(result.records[5].missingEvidence, ['raw_membership_evidence_missing']);
  assert.equal(result.records[5].candidate.name, undefined);
});

test('the byte ceiling rejects the whole download without committing a partial snapshot', async () => {
  const work = [{ candidate_key: 'large', state: 'pending', observations: [{ raw: { description: 'x'.repeat(LIMITS.bytes) } }] }];
  const f = fixture({ work, counts: { tickets: 1, observations: 1, raw_bytes: '500' } }); await assert.rejects(exportCohort(f.pool, GRANT), /limit_exceeded/);
  assert.equal(f.statements.at(-1), 'ROLLBACK'); assert.equal(f.statements.includes('COMMIT'), false);
});

test('shared domain and name cannot invent which physical ticket was retained', async () => {
  const f = fixture();
  f.data.work.push({ ...f.data.work[0], candidate_key: 'distinct-physical-facility' });
  f.data.counts = { tickets: 2, observations: 4, raw_bytes: '1000' };
  const result = await exportCohort(f.pool, GRANT);
  assert.equal(result.records.length, 2); assert.equal(result.retained.length, 1);
  assert.ok(result.records.every(row => row.previous.status === 'accepted_retention_unresolved'));
  assert.ok(result.records.every(row => row.previous.retainedMatchBasis === null));
});
