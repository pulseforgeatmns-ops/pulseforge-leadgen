'use strict';
const deny = require('./helpers/denyExternalProviders.cjs');
const test = require('node:test'), assert = require('node:assert/strict');
const { startResearchOnlyHarness, placesFetch } = require('./helpers/scoutResearchOnlyFixture');
const FLAG = 'SCOUT_ANCHOR_FACILITY_RECOVERY_ENABLED';

test('eight native research cycles diversify without widening authority, paid pages or downstream admission', { timeout: 600000 }, async t => {
  const h = await startResearchOnlyHarness(); t.after(() => h.close());
  const originalFlag = process.env[FLAG], originalFetch = globalThis.fetch;
  t.after(() => { if (originalFlag === undefined) delete process.env[FLAG]; else process.env[FLAG] = originalFlag; globalThis.fetch = originalFetch; });
  const { pool } = h;
  const grants = require('../services/scoutResearchOnlyGrant'), missions = require('../services/scoutResearchMission');
  const { runMaxOutboundControlLoop } = require('../services/maxOutboundControlLoop');
  const dispatch = require('../services/scoutCandidateTicketDispatch');
  const source = (await pool.query("SELECT * FROM acquisition_missions WHERE tenant_id='10'")).rows[0];
  const program = (await pool.query("SELECT * FROM acquisition_outbound_programs WHERE tenant_id='10' ORDER BY authorized_at DESC LIMIT 1")).rows[0];
  const dependency = dispatch.candidateTicketDependencyKey({ tenantId: '10', program, source });
  process.env[FLAG] = 'true';
  assert.equal(dispatch.candidateTicketDependencyKey({ tenantId: '10', program, source }), dependency,
    'turning on the new implementation does not rearm the 34 parked tickets');
  await pool.query("INSERT INTO api_spend_policies(provider,daily_usd_limit,paused,updated_by) VALUES('google_places',50,false,'operator')");
  const protectedTables = ['anchor_email_recovery_control', 'anchor_email_quarantines', 'anchor_scoped_resume_grants',
    'acquisition_outbound_items', 'acquisition_outbound_envelopes', 'acquisition_missions', 'api_spend_policies', 'scout_unenriched', 'companies', 'prospects'];
  const snapshot = async () => Object.fromEntries(await Promise.all(protectedTables.map(async table => [table,
    (await pool.query(`SELECT to_jsonb(x) AS row FROM ${table} x ORDER BY to_jsonb(x)::text`)).rows])));
  const before = await snapshot();
  const preview = await missions.previewResearchMission(pool, { approvalRef: 'synthetic-p004-mission' }, h.actor);
  assert.deepEqual(preview.blockers, []);
  const mission = await missions.installResearchMission(pool, { approvalRef: 'synthetic-p004-mission', mission: preview.mission, missionHash: preview.missionHash }, h.actor);
  const grantPreview = await grants.previewResearchOnlyGrant(pool, { approvalRef: 'synthetic-p004-grant', researchMissionId: mission.missionId,
    expiresAt: new Date(Date.now() + 86400000).toISOString(), statement: 'Synthetic bounded research approval.' }, h.actor);
  assert.deepEqual(grantPreview.blockers, []);
  const grant = await grants.installResearchOnlyGrant(pool, { manifest: grantPreview.manifest, manifestHash: grantPreview.manifestHash }, h.actor);
  const google = placesFetch({ historicalMarker: true }); globalThis.fetch = google.fetchImpl;
  require('../utils/placesQueryCache').clearPlacesQueryCache();
  const selected = [];
  for (let n = 1; n <= 8; n++) {
    if (n > 1) await require('./helpers/releaseResearchCycle').releaseNextResearchCycle(pool, h.actor, grant.grantId, `synthetic-p004-release-${n}`);
    const searchesBefore = google.calls.filter(x => x.path.endsWith('/textsearch/json')).length;
    const result = await runMaxOutboundControlLoop({ pool, tenantId: '10', cycleId: `synthetic-p004-${n}`,
      logger: { log() {}, warn() {} }, runPreparationRefill: async () => assert.fail('preparation forbidden') });
    assert.equal(result.researchOnly.status, 'completed', JSON.stringify(result.researchOnly));
    const receipt = result.researchOnly.researchCoverage;
    assert.ok(receipt, 'classification/coverage must reach the native production research branch');
    const chosen = receipt.decisions.filter(x => x.status === 'selected');
    assert.equal(chosen.length, 1); selected.push(chosen[0]);
    assert.equal(google.calls.filter(x => x.path.endsWith('/textsearch/json')).length - searchesBefore, 1);
    assert.equal(result.researchOnly.queuedForEnrichment, 0); assert.equal(result.sent, 0);
    assert.equal(result.scout.admission.admittedToEnrichment, 0);
    assert.equal(result.scout.admission.retainedForResearch, result.researchOnly.retainedCandidates);
    assert.ok(result.researchOnly.retainedCandidates > 0, 'successful facility retention must still work');
    assert.equal(result.researchOnly.providerReceipt.reportsAvailable, true);
    const providers = result.researchOnly.providerReceipt;
    assert.ok(providers.providers.some(row => row.executed === null
      && row.errors.some(error => error.cause === 'facility_research_identity_only')),
    `the identity-only gate must remain visible without HTTP: ${JSON.stringify(providers)}`);
    assert.equal(providers.reportsTruncated, true);
    assert.equal(providers.discovery.providerIssuesComplete, false);
    assert.notEqual(providers.discovery.blocker, 'none', 'a truncated pass with refused work must not be labeled healthy');
    assert.equal((await pool.query("SELECT count(*)::int AS n FROM scout_research_candidates WHERE company LIKE '%Historical Marker%'")).rows[0].n, 0);
    if (n === 1) {
      const marker = (await pool.query("SELECT raw_candidate FROM scout_candidate_memberships WHERE raw_candidate->>'companyName'='Synthetic Factory Historical Marker' LIMIT 1")).rows[0];
      assert.ok(marker?.raw_candidate.website, 'the historical listing has a complete website identity');
      const attempts = google.calls.length;
      const held = await runMaxOutboundControlLoop({ pool, tenantId: '10', cycleId: 'synthetic-p004-unreleased', logger: { log() {}, warn() {} } });
      assert.equal(held.researchOnly.status, 'awaiting_cycle_release');
      assert.equal(google.calls.length, attempts);
    }
  }
  assert.equal(new Set(selected.map(x => x.category)).size, 8, JSON.stringify(selected));
  assert.equal(new Set(selected.map(x => x.city)).size, 8, JSON.stringify(selected));
  const receipt = await grants.inspectResearchOnlyGrant(pool, grant.grantId);
  assert.equal(receipt.cyclesConsumed, 8); assert.equal(receipt.cyclesRemaining, 0);
  assert.ok(receipt.cycles.every(row => row.result.researchCoverage?.maxNewPages === 1));
  assert.ok(receipt.cycles.every(row => row.result.providerReceipt?.reportsAvailable === true));
  assert.ok(receipt.reservedUsdMicros <= 5000000);
  const facts = (await pool.query("SELECT candidate FROM scout_tenant_place_observations WHERE tenant_id='10' AND state='observed'")).rows;
  assert.ok(facts.length > 0); assert.ok(facts.some(row => row.candidate.addressComponents?.length));
  assert.ok(facts.every(row => row.candidate.businessStatus === 'OPERATIONAL'));
  assert.deepEqual(await snapshot(), before, 'all recovery, suppression/ownership, source missions and downstream records are intact');
  const attempts = google.calls.length;
  const exhausted = await runMaxOutboundControlLoop({ pool, tenantId: '10', cycleId: 'synthetic-p004-exhausted', logger: { log() {}, warn() {} } });
  assert.equal(exhausted.researchOnly.status, 'cycles_exhausted'); assert.equal(google.calls.length, attempts);
  assert.equal(receipt.executionStatus, 'cycles_exhausted');
  assert.equal(deny.attempts(), 0);
});
