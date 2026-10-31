'use strict';
// Scout P0 recovery (A + D) on disposable PostgreSQL: research-candidate
// accounting in the grant receipt, and research-only tickets that need website
// evidence are parked instead of being re-evaluated every cycle. Synthetic
// Places only; no website, provider or production database is reached.
const deny = require('./helpers/denyExternalProviders.cjs');
const test = require('node:test'), assert = require('node:assert/strict');
const { startResearchOnlyHarness, placesFetch } = require('./helpers/scoutResearchOnlyFixture');

const quiet = { log() {}, warn() {}, error() {} };
const iso = ms => new Date(Math.floor((Date.now() + ms) / 1000) * 1000).toISOString();

test('research candidate accounting and parked website-evidence tickets (Tenant 10)', { timeout: 600000 }, async t => {
  const h = await startResearchOnlyHarness(); t.after(() => h.close());
  const { pool } = h;
  const grants = require('../services/scoutResearchOnlyGrant');
  const missions = require('../services/scoutResearchMission');
  const { runMaxOutboundControlLoop } = require('../services/maxOutboundControlLoop');
  const { createCandidateTicketStore } = require('../services/scoutCandidateTickets');
  require('../utils/placesQueryCache').clearPlacesQueryCache();
  const places = placesFetch({ unclassifiable: true }); globalThis.fetch = places.fetchImpl;
  await pool.query(`INSERT INTO api_spend_policies(provider,daily_request_limit,monthly_request_limit,daily_usd_limit,monthly_usd_limit,paused,updated_by)
    VALUES('google_places',NULL,NULL,50,500,false,'operator')`);

  const count = async (sql, values = []) => (await pool.query(sql, values)).rows[0].n;
  const crm = async () => ({ queue: await count('SELECT count(*)::int n FROM scout_unenriched'),
    companies: await count('SELECT count(*)::int n FROM companies'), prospects: await count('SELECT count(*)::int n FROM prospects') });
  const tickets = async () => (await pool.query(`SELECT candidate_key,state,reason,attempts::int AS attempts
    FROM scout_candidate_work WHERE tenant_id='10' ORDER BY candidate_key`)).rows;
  const cycle = id => runMaxOutboundControlLoop({ pool, tenantId: '10', cycleId: id, logger: quiet,
    runPreparationRefill: async () => assert.fail('preparation must never run') });

  const missionPreview = await missions.previewResearchMission(pool, { approvalRef: 'synthetic-accounting-mission' }, h.actor);
  const research = await missions.installResearchMission(pool, { approvalRef: 'synthetic-accounting-mission',
    mission: missionPreview.mission, missionHash: missionPreview.missionHash }, h.actor);
  const grantPreview = await grants.previewResearchOnlyGrant(pool, { approvalRef: 'synthetic-accounting-grant', expiresAt: iso(86400000),
    statement: 'Synthetic operator approval for research-only Scout discovery.', researchMissionId: research.missionId }, h.actor);
  assert.deepEqual(grantPreview.blockers, []);
  const grant = await grants.installResearchOnlyGrant(pool, { manifest: grantPreview.manifest, manifestHash: grantPreview.manifestHash }, h.actor);
  const crmBefore = await crm();

  await t.test('D: a business that needs website evidence is parked, not deferred for replay', async () => {
    const first = await cycle('accounting-1');
    assert.equal(first.researchOnly.status, 'completed', JSON.stringify(first.researchOnly));
    const rows = await tickets();
    const parked = rows.filter(row => row.reason === 'business_evidence_not_authorized');
    assert.ok(parked.length > 0, 'the synthetic unclassifiable business reached admission');
    assert.ok(parked.every(row => row.state === 'blocked' && row.attempts === 1));
    assert.equal(rows.filter(row => row.reason === 'evidence_budget_deferred').length, 0, 'no zero-delay replay reason');
    // Eligibility is unchanged: classifiable facilities are still accepted and retained.
    assert.ok(first.researchOnly.acceptedCandidates > 0);
    assert.ok(first.researchOnly.retainedCandidates > 0);
    assert.equal(first.researchOnly.evaluatedCandidates, rows.length);

    await require('./helpers/releaseResearchCycle').releaseNextResearchCycle(pool, h.actor, grant.grantId, 'synthetic-accounting-release-2');
    const second = await cycle('accounting-2');
    assert.equal(second.researchOnly.status, 'completed', JSON.stringify(second.researchOnly));
    const after = await tickets();
    for (const row of parked) {
      const now = after.find(candidate => candidate.candidate_key === row.candidate_key);
      assert.equal(now.attempts, 1, 'a parked ticket is not claimed again by the same research authority');
      assert.equal(now.reason, 'business_evidence_not_authorized');
    }
    assert.equal(second.researchOnly.evaluatedCandidates, after.length - rows.length, 'cycle 2 evaluates only its new businesses');
  });

  await t.test('D: parked tickets are preserved and due again for a pass with different authority', async () => {
    const fingerprint = grant.fingerprint;
    const store = createCandidateTicketStore({ pool, tenantId: '10', fingerprint });
    const parked = (await tickets()).filter(row => row.reason === 'business_evidence_not_authorized').length;
    const snapshot = await store.snapshot({ dependencyKey: 'f'.repeat(64) });
    assert.equal(snapshot.byReason.business_evidence_not_authorized, parked);
    assert.ok(snapshot.due >= parked, 'a different dependency key (e.g. future website authority) sees them due');
  });

  await t.test('D: a ticket left with the earlier zero-delay reason is parked on its next research claim', async () => {
    const target = (await tickets()).find(row => row.reason === 'business_evidence_not_authorized');
    // Production holds tickets written before this change in exactly this shape.
    await pool.query(`UPDATE scout_candidate_work SET state='deferred',reason='evidence_budget_deferred',next_attempt_at=now()
      WHERE tenant_id='10' AND candidate_key=$1`, [target.candidate_key]);
    const legacy = await grants.inspectResearchOnlyGrant(pool, grant.grantId);
    assert.equal(legacy.candidates.ledger.byReason.evidence_budget_deferred, 1);
    assert.ok(legacy.candidates.ledger.awaitingBusinessEvidence.rows.some(row => row.reason === 'evidence_budget_deferred'));
    await require('./helpers/releaseResearchCycle').releaseNextResearchCycle(pool, h.actor, grant.grantId, 'synthetic-accounting-release-3');
    assert.equal((await cycle('accounting-3')).researchOnly.status, 'completed');
    const now = (await tickets()).find(row => row.candidate_key === target.candidate_key);
    assert.equal(now.state, 'blocked'); assert.equal(now.reason, 'business_evidence_not_authorized'); assert.equal(now.attempts, 2);
  });

  await t.test('A: the grant receipt reports retained, accepted, awaiting and spend from durable records, read-only', async () => {
    const readonly = { async connect() { const db = await pool.connect(); return { release: db.release.bind(db),
      query(sql, values) { assert.match(typeof sql === 'string' ? sql : sql.text, /^\s*(SELECT|BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY|SET LOCAL|COMMIT|ROLLBACK)/); return db.query(sql, values); } }; } };
    const receipt = await grants.inspectResearchOnlyGrant(readonly, grant.grantId);
    const { candidates } = receipt;
    const retained = (await pool.query('SELECT domain,cycle_number FROM scout_research_candidates WHERE grant_id=$1 ORDER BY id', [grant.grantId])).rows;
    assert.equal(candidates.retained.total, retained.length);
    assert.deepEqual(candidates.retained.rows.map(row => row.domain), retained.map(row => row.domain));
    const rows = await tickets();
    assert.equal(candidates.ledger.total, rows.length);
    assert.equal(candidates.ledger.scope, 'search_fingerprint');
    const accepted = rows.filter(row => row.state === 'handled' && row.reason === 'queued').length;
    assert.equal(candidates.ledger.acceptedTickets, accepted);
    assert.equal(candidates.ledger.acceptedNotRetained, accepted - retained.length, 'accepted and retained are reported separately');
    const parked = rows.filter(row => row.reason === 'business_evidence_not_authorized').length;
    assert.equal(candidates.ledger.awaitingBusinessEvidence.total, parked);
    for (const row of candidates.ledger.awaitingBusinessEvidence.rows) {
      assert.match(row.name, /Granite Works/);
      assert.deepEqual(row.placeTypes, ['point_of_interest', 'establishment']);
      assert.ok(row.location && row.website);
    }
    const cycles = (await pool.query('SELECT result FROM scout_research_only_cycles WHERE grant_id=$1 ORDER BY cycle_number', [grant.grantId])).rows;
    assert.deepEqual(candidates.perCycle.map(row => row.retainedCandidates), cycles.map(row => row.result.retainedCandidates));
    assert.deepEqual(candidates.perCycle.map(row => row.acceptedCandidates), cycles.map(row => row.result.acceptedCandidates));
    assert.ok(candidates.perCycle.every(row => Number.isSafeInteger(row.acceptedCandidates) && Number.isSafeInteger(row.evaluatedCandidates)));
    assert.equal(receipt.spendBasis, 'reserved_upper_bound');
    assert.equal(receipt.reservedUsdMicros, await count('SELECT coalesce(sum(reserved_usd_micros),0)::int n FROM scout_research_google_spend WHERE grant_id=$1', [grant.grantId]));
  });

  await t.test('boundaries: nothing reached enrichment, CRM, websites or real providers', async () => {
    assert.deepEqual(await crm(), crmBefore);
    assert.ok(places.calls.length > 0);
    assert.equal(deny.attempts(), 0, 'no real external transport (including first-party websites)');
    assert.equal(await count(`SELECT count(*)::int n FROM scout_research_candidates r JOIN scout_unenriched u
      ON u.client_id=10 AND lower(u.domain)=lower(r.domain)`), 0);
  });
});
