'use strict';
// SPEC-SCOUT-P0-003: governed research-only activation on disposable PostgreSQL.
// Real recovery/quarantine schema, a still-pending prepared envelope item, real
// native Scout discovery (PlacesProvider -> utils/placesApi), synthetic HTTP.
const deny = require('./helpers/denyExternalProviders.cjs');
const test = require('node:test'), assert = require('node:assert/strict');
const { startResearchOnlyHarness, placesFetch, ENV, ADMIN_ID, OWNED_DOMAIN } = require('./helpers/scoutResearchOnlyFixture');

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const quiet = { log() {}, warn() {}, error() {} };
const iso = ms => new Date(Math.floor((Date.now() + ms) / 1000) * 1000).toISOString();

// Each part gets its own disposable database. Tenant-wide durable search memory
// is permanent by design, so a fresh database is the only fresh search plan.
test('governed research-only activation (Tenant 10)', { timeout: 600000 }, t => run(t, 'governance'));
test('research mission isolation, ownership, non-admission and reconciliation', { timeout: 600000 }, t => run(t, 'research_mission'));

async function run(t, part) {
  const h = await startResearchOnlyHarness(); t.after(() => h.close());
  const { pool } = h;
  const grants = require('../services/scoutResearchOnlyGrant');
  const missions = require('../services/scoutResearchMission');
  const { runMaxOutboundControlLoop, _test: { loadSource } } = require('../services/maxOutboundControlLoop');
  const placesApi = require('../utils/placesApi');
  const { withPlacesContext } = require('../utils/placesCostAttribution');
  const pause = require('../utils/anchorResearchProviderPause');
  const recovery = require('../services/anchorEmailRecovery');
  const { hash } = require('../services/scoutBoundedAcquisitionBudget');
  // A fresh database is a fresh plan; drop the process-local Places cache too.
  require('../utils/placesQueryCache').clearPlacesQueryCache();
  const places = placesFetch(); globalThis.fetch = places.fetchImpl;
  let approvals = 0;
  let current = { programId: 'synthetic-anchor-research-program', missionId: 'synthetic-anchor-research-mission' };
  let research = null;
  // The separately approved research mission (facility-buyer ICP) for the
  // current program. Never written to acquisition_missions.
  async function stageResearch(approvalRef = `synthetic-research-mission-${++approvals}`) {
    const preview = await missions.previewResearchMission(pool, { approvalRef }, h.actor);
    assert.deepEqual(preview.blockers, []);
    const installed = await missions.installResearchMission(pool, { approvalRef, mission: preview.mission, missionHash: preview.missionHash }, h.actor);
    research = { ...installed, preview };
    return research;
  }
  const retained = () => count('SELECT count(*)::int n FROM scout_research_candidates');
  const crm = async () => ({ queue: await count('SELECT count(*)::int n FROM scout_unenriched'),
    companies: await count('SELECT count(*)::int n FROM companies'), prospects: await count('SELECT count(*)::int n FROM prospects') });

  const count = async (sql, values = []) => (await pool.query(sql, values)).rows[0].n;
  async function history() {
    const out = {};
    for (const table of ['anchor_email_recovery_control', 'anchor_email_quarantines', 'anchor_email_recovery_reservations',
      'anchor_scoped_resume_grants', 'acquisition_outbound_items', 'acquisition_outbound_envelopes',
      'acquisition_mission_outbound_executions', 'acquisition_mission_contributions', 'acquisition_missions', 'api_spend_policies'])
      out[table] = (await pool.query(`SELECT to_jsonb(x) AS row FROM ${table} x ORDER BY to_jsonb(x)::text`)).rows;
    out.programs = (await pool.query(`SELECT to_jsonb(x)-'last_tick_at'-'last_error' AS row FROM acquisition_outbound_programs x ORDER BY id`)).rows;
    return out;
  }
  async function install({ expiresIn = 86400000, actor = h.actor, mutate, blockers = [] } = {}) {
    const preview = await grants.previewResearchOnlyGrant(pool, { approvalRef: `synthetic-research-only-${++approvals}`,
      expiresAt: iso(expiresIn), statement: 'Synthetic operator approval for research-only Scout discovery.',
      researchMissionId: research.missionId }, actor);
    assert.deepEqual(preview.blockers, blockers); assert.equal(preview.previewOnly, true);
    const manifest = structuredClone(preview.manifest); if (mutate) mutate(manifest);
    const result = await grants.installResearchOnlyGrant(pool, { manifest, manifestHash: mutate ? hash(manifest) : preview.manifestHash }, actor);
    assert.equal(result.installed, true);
    return { ...result, manifest };
  }
  async function revokeOpen() {
    for (const row of (await pool.query('SELECT grant_id FROM scout_research_only_grants WHERE revoked_at IS NULL')).rows)
      await grants.revokeResearchOnlyGrant(pool, { grantId: row.grant_id, approvalReference: 'synthetic-close' }, { kind: 'operator_cli' });
  }
  const cycle = (id, extra = {}) => runMaxOutboundControlLoop({ pool, tenantId: '10', cycleId: id, logger: quiet,
    runPreparationRefill: async () => assert.fail('preparation must never run'), ...extra });
  async function claimDirect(cycleId, extra = {}) {
    const program = (await pool.query('SELECT * FROM acquisition_outbound_programs WHERE id=$1', [current.programId])).rows[0];
    return grants.claimResearchCycle(pool, { program, source: await loadSource(pool, program, '10'), cycleId, ...extra });
  }
  const openCycles = () => count(`SELECT count(*)::int n FROM scout_research_only_cycles c
    JOIN scout_research_only_grants g USING(grant_id) WHERE g.revoked_at IS NULL`);
  async function scenario(name, fn) {
    await t.test(name, async () => {
      const before = await history();
      try { await fn(); } finally { await revokeOpen(); }
      assert.deepEqual(await history(), before, 'quarantine, envelope, items, missions, programs and shared policy never change');
      for (const [key, value] of Object.entries(ENV)) assert.equal(process.env[key], value, `${key} unchanged`);
      assert.equal(deny.attempts(), 0, 'no real external transport');
    });
  }

  // Shared provider authority (used by Babrun and every other tenant) is a
  // policy row research-only never touches; it is in every history snapshot.
  await pool.query(`INSERT INTO api_spend_policies(provider,daily_request_limit,monthly_request_limit,daily_usd_limit,monthly_usd_limit,paused,updated_by)
    VALUES('google_places',NULL,NULL,50,500,false,'operator')`);
  assert.equal(await count("SELECT count(*)::int n FROM acquisition_outbound_items WHERE tenant_id='10' AND status='pending'"), 1,
    'fixture mirrors production: prepared envelope not drained');
  assert.equal(await count("SELECT count(*)::int n FROM anchor_email_quarantines WHERE tenant_id='10'"), 1);

  if (part === 'governance') {
  await scenario('without a grant the existing recovery halt is unchanged and no provider is called', async () => {
    const calls = places.calls.length;
    assert.equal((await cycle('no-grant')).halted, 'anchor_recovery_separate_preparation_approval_required');
    assert.equal(places.calls.length, calls);
    assert.equal(await count('SELECT count(*)::int n FROM scout_research_only_cycles'), 0);
  });

  await scenario('the research mission is separately approved, immutable, facility-buyer only and outside the outbound mission', async () => {
    const outbound = (await pool.query('SELECT * FROM acquisition_outbound_programs WHERE id=$1', [current.programId])).rows[0];
    const readonly = { async connect() { const db = await pool.connect(); return { release: db.release.bind(db),
      query(sql, values) { assert.match(typeof sql === 'string' ? sql : sql.text, /^\s*(SELECT|BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY|SET LOCAL|COMMIT|ROLLBACK)/); return db.query(sql, values); } }; } };
    const preview = await missions.previewResearchMission(readonly, { approvalRef: 'synthetic-research-mission-preview' }, h.actor);
    assert.deepEqual(preview.eligibleSubsegments, ['managed_facilities', 'restaurant_foh']);
    assert.deepEqual(preview.cities, ['Manchester', 'Bedford', 'Londonderry', 'Hooksett', 'Auburn', 'Goffstown', 'Nashua', 'Merrimack']);
    assert.equal(preview.mission.researchOnly, true); assert.equal(preview.mission.outboundEligible, false);
    assert.equal(preview.predecessorMissionId, outbound.source_mission_id);
    // Install needs the admin's live session and the exact previewed document.
    const input = { approvalRef: 'synthetic-research-mission-1', mission: preview.mission, missionHash: preview.missionHash };
    await assert.rejects(missions.installResearchMission(pool, { ...input, missionHash: '0'.repeat(64) }, h.actor), { code: 'research_mission_exact_confirmation_required' });
    const widened = structuredClone(preview.mission); widened.structuredMission.market.eligibleSubsegments.push('realtor');
    await assert.rejects(missions.installResearchMission(pool, { ...input, mission: widened, missionHash: hash(widened) }, h.actor), { code: 'research_mission_changed' });
    await assert.rejects(missions.installResearchMission(pool, input, { ...h.actor, sessionId: 'other-session' }));
    await assert.rejects(missions.installResearchMission(pool, input, { ...h.actor, role: 'manager' }), { code: 'research_mission_operator_invalid' });
    assert.equal(await count('SELECT count(*)::int n FROM scout_research_missions'), 0);
    const staged = await stageResearch('synthetic-research-mission-1');
    assert.equal(staged.installed, true);
    assert.equal((await missions.installResearchMission(pool, input, h.actor)).idempotent, true);
    // Never an acquisition mission; the outbound program still points at the old mission.
    assert.equal(await count('SELECT count(*)::int n FROM acquisition_missions WHERE id=$1', [staged.missionId]), 0);
    assert.equal((await pool.query('SELECT source_mission_id FROM acquisition_outbound_programs WHERE id=$1', [current.programId])).rows[0].source_mission_id, outbound.source_mission_id);
    await assert.rejects(pool.query("UPDATE scout_research_missions SET payload=payload || '{\"outboundEligible\":true}'::jsonb"), /permanent/);
    await assert.rejects(pool.query('DELETE FROM scout_research_missions'), /permanent/);
    // The successor activation guard is untouched: still refuses while the envelope is pending.
    const { apply } = require('../scripts/activateAnchorInventoryCapacityMission');
    const { facilityBuyerSuccessorMission } = require('../scripts/activateAnchorFacilityBuyerMission');
    await assert.rejects(apply(pool, facilityBuyerSuccessorMission), /prepared_envelope_not_drained:1/);
  });

  await scenario('preview is read-only and never installs', async () => {
    const readonly = { async connect() { const db = await pool.connect(); return { release: db.release.bind(db),
      query(sql, values) { assert.match(typeof sql === 'string' ? sql : sql.text, /^\s*(SELECT|BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY|SET LOCAL|COMMIT|ROLLBACK)/); return db.query(sql, values); } }; } };
    const preview = await grants.previewResearchOnlyGrant(readonly, { approvalRef: 'synthetic-preview-only', expiresAt: iso(3600000),
      statement: 'Preview only.', researchMissionId: research.missionId }, h.actor);
    assert.equal(preview.installable, true);
    assert.deepEqual(preview.manifest.permissions, { discovery: true, enrichment: false, verification: false, preparation: false, outbound: false });
    assert.equal(preview.manifest.maxCycles, 8); assert.equal(preview.manifest.dailyGoogleUsdMicros, 5000000);
    assert.equal(await count('SELECT count(*)::int n FROM scout_research_only_grants'), 0);
  });

  await scenario('research-only discovery runs with the quarantined email unresolved; preparation and sending stay blocked', async () => {
    const grant = await install();
    const before = places.calls.length, crmBefore = await crm();
    const result = await cycle('research-1');
    assert.equal(result.researchOnly.status, 'completed', JSON.stringify(result.researchOnly));
    assert.equal(result.researchOnly.cycleNumber, 1); assert.equal(result.researchOnly.cyclesRemaining, 7);
    assert.ok(result.researchOnly.retainedCandidates > 0, 'research candidates retained');
    assert.equal(result.researchOnly.researchMissionId, research.missionId);
    assert.equal(result.preparedAdded, 0); assert.equal(result.sent, 0);
    const calls = places.calls.slice(before);
    assert.equal(calls.filter(c => c.path.endsWith('/textsearch/json')).length, 1, 'one new page per cycle');
    const spend = (await pool.query('SELECT state,api_usage_id FROM scout_research_google_spend WHERE grant_id=$1', [grant.grantId])).rows;
    assert.equal(spend.length, calls.length, 'every HTTP attempt has its own durable Tenant 10 debit');
    assert.ok(spend.every(row => row.state === 'ok' && row.api_usage_id), 'each linked to one shared meter event');
    assert.equal(await count("SELECT count(*)::int n FROM api_usage_events WHERE provider='google_places'"), calls.length);
    assert.deepEqual(await crm(), crmBefore, 'nothing reaches the enrichment queue, companies or prospects');
    assert.equal(await retained(), result.researchOnly.retainedCandidates);
    assert.equal(await count('SELECT count(*)::int n FROM scout_research_candidates WHERE research_mission_id<>$1', [research.missionId]), 0);
    // The recovery guard still refuses preparation admission and outbound.
    assert.equal((await recovery.loadRecoveryState(pool)).mode, 'maintenance');
    const db = await pool.connect();
    try {
      await db.query('BEGIN');
      const item = (await db.query("SELECT * FROM acquisition_outbound_items WHERE status='pending'")).rows[0];
      const program = (await db.query('SELECT p.* FROM acquisition_outbound_programs p JOIN acquisition_outbound_envelopes e ON e.program_id=p.id WHERE e.id=$1', [item.envelope_id])).rows[0];
      await assert.rejects(recovery.assertRecoveryAdmission(db, { program, item, stage: 'preparation' }), { code: 'anchor_recovery_maintenance' });
      await assert.rejects(recovery.assertRecoveryLeaf(db, { channel: 'email' }), { code: 'anchor_recovery_maintenance' });
    } finally { await db.query('ROLLBACK'); db.release(); }
    // A retried/restarted scheduler run with the same id never executes twice.
    const replayCalls = places.calls.length;
    assert.equal((await cycle('research-1')).halted, 'research_only_cycle_already_claimed');
    assert.equal(places.calls.length, replayCalls);
    const receipt = await grants.inspectResearchOnlyGrant(pool, grant.grantId);
    assert.equal(receipt.cyclesConsumed, 1); assert.equal(receipt.reservedUsdMicros, result.researchOnly.googleReservedUsdMicros);
    assert.ok(!JSON.stringify(receipt).includes('execution_token'));
  });

  await scenario('durable memory: later cycles never repurchase a searched query or a detailed place', async () => {
    const grant = await install();
    const before = places.calls.length;
    for (let i = 1; i <= 3; i++) assert.equal((await cycle(`memory-${i}`)).researchOnly?.status, 'completed');
    const searches = places.calls.filter(c => c.path.endsWith('/textsearch/json')).map(c => c.query);
    const details = places.calls.filter(c => c.path.endsWith('/details/json')).map(c => c.placeId);
    assert.equal(new Set(searches).size, searches.length, 'no text search query is bought twice');
    assert.equal(new Set(details).size, details.length, 'no place detail is bought twice');
    assert.equal(await count('SELECT count(*)::int n FROM scout_research_google_spend WHERE grant_id=$1', [grant.grantId]), places.calls.length - before);
  });

  for (const [name, key, value, code] of [
    ['research-provider pause lowered', 'ANCHOR_RESEARCH_PROVIDERS_PAUSED', 'false', 'research_only_closed_environment_required'],
    ['sending enabled', 'ANCHOR_GOVERNED_OUTBOUND_ENABLED', 'true', 'research_only_closed_environment_required'],
    ['durable discovery memory off', 'SCOUT_ANCHOR_DISCOVERY_MEMORY_ENABLED', 'false', 'research_only_discovery_memory_required'],
    ['different deployed revision', 'RAILWAY_GIT_COMMIT_SHA', 'c'.repeat(40), 'research_only_revision_mismatch'],
    ['recovery kill switch lowered', 'ANCHOR_EMAIL_RECOVERY_MAINTENANCE', 'false', null],
    ['scoped resume activated', 'ANCHOR_EMAIL_SCOPED_RESUME_ID', 'synthetic-other', null],
  ]) await scenario(`${name} rejects the grant without a cycle or provider call`, async () => {
    await install();
    const saved = process.env[key], before = places.calls.length;
    process.env[key] = value;
    try {
      const result = await cycle(`env-${key}`);
      if (code) assert.equal(result.halted, code); else assert.ok(result.halted, 'the existing recovery halt applies');
      assert.notEqual(result.researchOnly?.status, 'completed');
    } finally { saved === undefined ? delete process.env[key] : process.env[key] = saved; }
    assert.equal(places.calls.length, before);
    assert.equal(await openCycles(), 0);
  });

  await scenario('flag off: deploying the code changes nothing', async () => {
    await install();
    process.env.SCOUT_RESEARCH_ONLY_GRANTS_ENABLED = 'false';
    try { assert.equal((await cycle('flag-off')).halted, 'anchor_recovery_separate_preparation_approval_required'); }
    finally { process.env.SCOUT_RESEARCH_ONLY_GRANTS_ENABLED = 'true'; }
    assert.equal(await openCycles(), 0);
  });

  await scenario('install refuses widened, tampered or foreign approvals', async () => {
    for (const mutate of [m => { m.permissions.preparation = true; }, m => { m.permissions.outbound = true; }, m => { m.permissions.enrichment = true; },
      m => { m.maxCycles = 9; }, m => { m.dailyGoogleUsdMicros = 5000001; }, m => { m.providers = ['google_places', 'prospeo']; },
      m => { m.tenantId = '13'; }, m => { m.expiresAt = iso(8 * 86400000); }, m => { m.programHash = '0'.repeat(64); },
      m => { m.quarantineDecisionHash = '0'.repeat(64); }, m => { m.operator.id = '72'; }])
      await assert.rejects(install({ mutate }));
    const preview = await grants.previewResearchOnlyGrant(pool, { approvalRef: 'synthetic-confirm', expiresAt: iso(3600000), statement: 'x',
      researchMissionId: research.missionId }, h.actor);
    const input = { manifest: preview.manifest, manifestHash: preview.manifestHash };
    await assert.rejects(grants.installResearchOnlyGrant(pool, { ...input, manifestHash: '0'.repeat(64) }, h.actor),
      { code: 'research_only_exact_confirmation_required' });
    await assert.rejects(grants.installResearchOnlyGrant(pool, input, { ...h.actor, sessionId: 'other-session' }), { code: 'research_only_operator_invalid' });
    await assert.rejects(grants.installResearchOnlyGrant(pool, input, { ...h.actor, role: 'manager' }), { code: 'research_only_operator_invalid' });
    await pool.query(`UPDATE "session" SET expire='2000-01-01' WHERE sid=$1`, [h.actor.sessionId]);
    try { await assert.rejects(grants.installResearchOnlyGrant(pool, input, h.actor)); }
    finally { await pool.query(`UPDATE "session" SET expire='2099-01-01' WHERE sid=$1`, [h.actor.sessionId]); }
    assert.equal(await count('SELECT count(*)::int n FROM scout_research_only_grants WHERE revoked_at IS NULL'), 0);
  });

  await scenario('a second open grant is refused; grant and ledger rows cannot be edited or deleted', async () => {
    const grant = await install();
    await assert.rejects(install({ blockers: ['research_only_other_grant_open'] }), { code: 'research_only_other_grant_open' });
    await assert.rejects(pool.query('UPDATE scout_research_only_grants SET daily_google_usd_micros=4999999 WHERE grant_id=$1', [grant.grantId]), /permanent/);
    await assert.rejects(pool.query('DELETE FROM scout_research_only_grants'), /permanent/);
    await assert.rejects(pool.query('DELETE FROM scout_research_google_spend'), /permanent/);
    await assert.rejects(pool.query('UPDATE scout_research_google_spend SET reserved_usd_micros=1'), /permanent/);
  });

  await scenario('mismatched program, mission, control or operator state rejects the next cycle', async () => {
    await install();
    await pool.query('UPDATE users SET active=false WHERE id=$1', [ADMIN_ID]);
    try { assert.equal((await cycle('deactivated-admin')).halted, 'research_only_operator_invalid'); }
    finally { await pool.query('UPDATE users SET active=true WHERE id=$1', [ADMIN_ID]); }
    const policy = (await pool.query('SELECT policy FROM acquisition_outbound_programs WHERE id=$1', [current.programId])).rows[0].policy;
    await pool.query('UPDATE acquisition_outbound_programs SET policy=$2 WHERE id=$1', [current.programId, { ...policy, dailyCap: 6 }]);
    try { assert.equal((await cycle('program-changed')).halted, 'research_only_program_changed'); }
    finally { await pool.query('UPDATE acquisition_outbound_programs SET policy=$2 WHERE id=$1', [current.programId, policy]); }
    const mission = (await pool.query('SELECT payload FROM acquisition_missions WHERE id=$1', [current.missionId])).rows[0].payload;
    const widened = structuredClone(mission); widened.structuredMission.geography.cities.push('Nashua');
    await pool.query('UPDATE acquisition_missions SET payload=$2 WHERE id=$1', [current.missionId, widened]);
    try { assert.equal((await cycle('mission-changed')).halted, 'research_only_mission_changed'); }
    finally { await pool.query('UPDATE acquisition_missions SET payload=$2 WHERE id=$1', [current.missionId, mission]); }
    assert.equal(await openCycles(), 0);
    assert.equal((await cycle('restored')).researchOnly?.status, 'completed', 'restoring the exact pinned state is accepted');
  });

  await scenario('an expired grant cannot run a cycle', async () => {
    await install({ expiresIn: 2500 });
    await sleep(3000);
    assert.equal((await cycle('expired')).halted, 'research_only_grant_window_inactive');
    assert.equal((await grants.inspectResearchOnlyGrant(pool)).status, 'expired');
    assert.equal(await openCycles(), 0);
  });

  // Outside any history snapshot: a newer outbound program, with its own
  // separately approved research mission (same facility-buyer targeting).
  current = await h.approveProgram();
  await stageResearch();
  await scenario('durable memory is tenant-wide: a newly approved research mission never rebuys a searched query', async () => {
    await install();
    const before = places.calls.length;
    const result = await cycle('same-geography-new-mission');
    assert.equal(result.researchOnly.status, 'completed');
    assert.equal(places.calls.slice(before).filter(c => c.path.endsWith('/textsearch/json')
      && places.calls.slice(0, before).some(o => o.query === c.query)).length, 0, 'no previously bought query is bought again');
  });

  await scenario('eight cycles is a hard cap, including concurrent claims and restarts', async () => {
    const grant = await install();
    // Concurrent workers: exactly one claim wins, the rest are refused.
    const racing = await Promise.allSettled(Array.from({ length: 6 }, (_, i) => claimDirect(`race-${i}`)));
    assert.equal(racing.filter(r => r.status === 'fulfilled').length, 1);
    for (const r of racing.filter(r => r.status === 'rejected'))
      assert.ok(r.reason.code === 'research_only_cycle_in_progress' || /lock/i.test(r.reason.message), r.reason.message);
    await grants.finishResearchCycle(pool, racing.find(r => r.status === 'fulfilled').value, { completed: false, result: { reason: 'synthetic_crash' } });
    // Restart: a claimed cycle whose worker died is abandoned after its lease,
    // stays consumed, and its token can no longer reserve Google spend.
    const orphan = await claimDirect('orphan', { leaseMs: 400 });
    await assert.rejects(claimDirect('too-soon'), { code: 'research_only_cycle_in_progress' });
    await sleep(700);
    await assert.rejects(grants.runInResearchScope(orphan, () => grants.googleMeter().reserve({ endpoint: 'text_search', tenantId: '10' }), pool),
      { code: grants.SPEND_CODE });
    const results = [];
    for (let i = 0; i < 8; i++) results.push(await cycle(`cap-${i}`));
    assert.equal(results.filter(r => r.researchOnly?.status === 'completed').length, 6, '2 consumed + 6 run = 8');
    // Raw queue admissions count as research-only yield: no zero-yield backoff
    // or premature search-generation rotation while queries still admit.
    for (const r of results.slice(0, 6)) assert.notEqual(r.researchOnly.discovery, 'backoff', JSON.stringify(r.researchOnly));
    // Retaining candidates keeps the zero-yield backoff from rotating the plan.
    assert.ok(results.slice(0, 6).filter(r => r.researchOnly.retainedCandidates > 0).length >= 3);
    assert.equal(results[6].halted, 'research_only_cycles_exhausted'); assert.equal(results[7].halted, 'research_only_cycles_exhausted');
    const states = (await pool.query('SELECT state FROM scout_research_only_cycles WHERE grant_id=$1 ORDER BY cycle_number', [grant.grantId])).rows.map(r => r.state);
    assert.deepEqual(states, ['stopped', 'abandoned', 'completed', 'completed', 'completed', 'completed', 'completed', 'completed']);
    const afterCap = places.calls.length; await cycle('cap-again'); assert.equal(places.calls.length, afterCap);
    await assert.rejects(pool.query("UPDATE scout_research_only_cycles SET state='claimed',finished_at=NULL WHERE grant_id=$1", [grant.grantId]), /permanent/);
    await assert.rejects(pool.query('DELETE FROM scout_research_only_cycles'), /permanent/);
    assert.equal((await grants.inspectResearchOnlyGrant(pool, grant.grantId)).status, 'exhausted');
  });

  await scenario('revocation immediately prevents new cycles and new Google attempts', async () => {
    const grant = await install();
    const claim = await claimDirect('revoke-live');
    await grants.runInResearchScope(claim, async () => {
      const debit = await grants.googleMeter().reserve({ endpoint: 'text_search', tenantId: '10' });
      await debit.settle('ok', 200);
      await grants.revokeResearchOnlyGrant(pool, { grantId: grant.grantId, approvalReference: 'synthetic-revoke' }, { id: ADMIN_ID, role: 'admin' });
      await assert.rejects(grants.googleMeter().reserve({ endpoint: 'place_details', tenantId: '10' }), { code: grants.SPEND_CODE, reason: 'grant_revoked' });
    }, pool);
    await grants.finishResearchCycle(pool, claim, { completed: false, result: { reason: 'revoked' } });
    const before = places.calls.length;
    assert.equal((await cycle('after-revoke')).halted, 'anchor_recovery_separate_preparation_approval_required');
    assert.equal(places.calls.length, before);
    assert.equal((await grants.revokeResearchOnlyGrant(pool, { grantId: grant.grantId, approvalReference: 'again' }, { kind: 'operator_cli' })).idempotent, true);
  });

  await scenario('only metered Places opens: direct transports, other providers and other tenants stay blocked', async () => {
    await install();
    const before = places.calls.length;
    let observed = null;
    // Real ramps run inside the tenant research scope (as defaultScoutRamp does).
    const result = await cycle('transports', { scoutRamp: () => pause.withResearchProviderScope('10', async () => {
      observed = {};
      // A direct (unmetered) transport asserts the provider before its own HTTP.
      for (const provider of ['google_places', 'prospeo', 'hunter', 'serpapi', 'apollo', 'phone_enrichment']) {
        try { pause.assertResearchProviderAllowed(provider, '10'); observed[provider] = 'allowed'; } catch (e) { observed[provider] = e.code; }
      }
      const ok = await withPlacesContext({ tenantId: '10', feature: 'Discovery', caller: 'ResearchOnlyTest' }, () =>
        placesApi.legacyTextSearch({ query: 'Synthetic metered probe Concord NH', apiKey: 'k', fetchImpl: places.fetchImpl, useCache: false }));
      assert.equal(ok.ok, true, 'the metered client is the only Places path that succeeds');
      await assert.rejects(withPlacesContext({ tenantId: '13', feature: 'Discovery', caller: 'ResearchOnlyTest' }, () =>
        placesApi.legacyTextSearch({ query: 'Synthetic foreign probe', apiKey: 'k', fetchImpl: places.fetchImpl, useCache: false })),
      { code: grants.SPEND_CODE, reason: 'tenant_mismatch' });
      return { discoveryAttempted: false };
    }) });
    assert.equal(result.researchOnly.status, 'completed', result.researchOnly.reason);
    assert.deepEqual(observed, Object.fromEntries(['google_places', 'prospeo', 'hunter', 'serpapi', 'apollo', 'phone_enrichment']
      .map(p => [p, 'anchor_research_provider_paused'])));
    assert.equal(places.calls.length - before, 1);
    // Outside a cycle: Tenant 10 stays paused; another tenant keeps its own path.
    assert.throws(() => pause.assertResearchProviderAllowed('research', '10'), { code: 'anchor_research_provider_paused' });
    assert.equal(grants.googleMeter(), null);
    const ledger = await count('SELECT count(*)::int n FROM scout_research_google_spend');
    const other = await pause.withResearchProviderScope('13', () => withPlacesContext({ tenantId: '13', feature: 'Discovery', caller: 'BabrunTest' }, () =>
      placesApi.legacyTextSearch({ query: 'Synthetic Babrun probe', apiKey: 'k', fetchImpl: places.fetchImpl, useCache: false })));
    assert.equal(other.ok, true);
    assert.equal(await count('SELECT count(*)::int n FROM scout_research_google_spend'), ledger, 'Babrun is never metered as Tenant 10');
    assert.equal(await count('SELECT count(*)::int n FROM api_usage_events WHERE tenant_id=13'), 1, 'Babrun keeps the shared meter');
  });

  await scenario('an exhausted approved search plan is held without consuming a cycle', async () => {
    await install();
    const { fingerprint } = await grants.inspectResearchOnlyGrant(pool);
    await pool.query(`INSERT INTO acquisition_outbound_events(id,tenant_id,program_id,event_type,payload)
      VALUES('synthetic-exhausted','10',$1,'max_outbound_control',$2)`, [current.programId,
      { scoutSearchFingerprint: fingerprint, scoutDiscoveryAttempted: true, scoutSearchGenerationExhausted: true, scoutSearchGeneration: 3 }]);
    try {
      const before = places.calls.length;
      assert.equal((await cycle('exhausted-plan')).halted, 'scout_approved_search_plan_exhausted');
      assert.equal(places.calls.length, before); assert.equal(await openCycles(), 0);
    } finally { await pool.query("DELETE FROM acquisition_outbound_events WHERE id='synthetic-exhausted'"); }
  });

  }
  if (part === 'research_mission') {
  await stageResearch();
  await scenario('discovery uses only the research mission: facility-buyer queries, research fingerprint, no outbound-scope tickets', async () => {
    await install();
    const before = places.calls.length;
    for (let i = 0; i < 2; i++) assert.equal((await cycle(`scope-${i}`)).researchOnly?.status, 'completed');
    const queries = places.calls.slice(before).filter(c => c.path.endsWith('/textsearch/json')).map(c => c.query);
    assert.ok(queries.length >= 2);
    const cities = ['Manchester', 'Bedford', 'Londonderry', 'Hooksett', 'Auburn', 'Goffstown', 'Nashua', 'Merrimack'];
    for (const query of queries) {
      assert.doesNotMatch(query, /realtor|real estate|short.term|vacation rental|airbnb|brokerage/i, query);
      assert.ok(cities.some(city => query.includes(` ${city} NH`)), query);
    }
    assert.ok(places.calls.length > before);
    const program = (await pool.query('SELECT * FROM acquisition_outbound_programs WHERE id=$1', [current.programId])).rows[0];
    const { scoutSearchFingerprint } = require('../services/maxOutboundControlLoop');
    const outboundFingerprint = scoutSearchFingerprint(program, await loadSource(pool, program, '10'));
    assert.equal(await count('SELECT count(*)::int n FROM scout_candidate_memberships WHERE search_fingerprint=$1', [outboundFingerprint]), 0,
      'nothing is ever filed under the outbound mission');
    const grant = (await grants.inspectResearchOnlyGrant(pool));
    assert.notEqual(grant.fingerprint, outboundFingerprint);
    assert.equal(grant.researchMissionId, research.missionId);
  });

  await scenario('candidate ownership: a business Tenant 10 already owns is never retained', async () => {
    await install();
    const result = await cycle('ownership');
    assert.equal(result.researchOnly.status, 'completed');
    assert.ok(places.calls.some(c => c.placeId?.startsWith('owned-')) || places.calls.some(c => c.query), 'owned business was offered by Places');
    assert.equal(await count('SELECT count(*)::int n FROM scout_research_candidates WHERE lower(domain)=$1', [OWNED_DOMAIN]), 0);
    const rejected = result.scout?.admission?.rejected || {};
    assert.ok(Object.entries(rejected).some(([key, value]) => /owned|collision|suppressed/.test(key) && value > 0), JSON.stringify(rejected));
    // Retention is unique per Tenant 10 business: no duplicate rows across cycles or missions.
    assert.equal(await count('SELECT count(*)::int n FROM (SELECT lower(domain) FROM scout_research_candidates GROUP BY 1 HAVING count(*)>1) d'), 0);
  });

  await scenario('downstream non-admission: retained candidates never reach enrichment, inventory, preparation or sending', async () => {
    const before = await crm();
    await install();
    assert.equal((await cycle('non-admission')).researchOnly?.status, 'completed');
    assert.ok(await retained() > 0);
    assert.deepEqual(await crm(), before);
    // The enrichment agent's own queue view and clean inventory see nothing new.
    const { loadCleanInventory } = require('../services/maxOutboundControlLoop');
    const { GovernedOutboundStore } = require('../services/governedOutboundStore');
    const program = (await pool.query('SELECT * FROM acquisition_outbound_programs WHERE id=$1', [current.programId])).rows[0];
    const inventory = await loadCleanInventory(pool, new GovernedOutboundStore(pool, '10'), await loadSource(pool, program, '10'), 10, program.policy);
    const domains = new Set((await pool.query('SELECT lower(domain) d FROM scout_research_candidates')).rows.map(r => r.d));
    assert.ok(domains.size > 0);
    assert.ok(inventory.clean.every(row => !domains.has(String(row.domain || '').toLowerCase())));
    // Reconciliation is refused while recovery maintenance is active.
    const plan = await missions.previewReconciliation(pool, { researchMissionId: research.missionId });
    assert.ok(plan.blockers.includes('research_reconcile_recovery_maintenance_active'));
    assert.ok(plan.blockers.includes('research_reconcile_target_scope_mismatch'), 'the obsolete outbound mission is not the facility-buyer scope');
    await assert.rejects(missions.applyReconciliation(pool, { researchMissionId: research.missionId, approvalRef: 'synthetic-reconcile', confirm: plan.planHash }),
      { code: 'research_reconcile_recovery_maintenance_active' });
    assert.deepEqual(await crm(), before);
  });

  // Reconciliation only after (simulated) recovery has ended and the successor
  // facility-buyer mission is activated through its own guarded transaction.
  await t.test('reconciliation: explicit, one-time, deduplicated, ownership-checked, never outbound', async () => {
    await revokeOpen();
    const { facilityBuyerSuccessorMission } = require('../scripts/activateAnchorFacilityBuyerMission');
    const program = (await pool.query('SELECT * FROM acquisition_outbound_programs WHERE id=$1', [current.programId])).rows[0];
    const predecessor = (await pool.query('SELECT * FROM acquisition_missions WHERE id=$1', [program.source_mission_id])).rows[0];
    const successor = facilityBuyerSuccessorMission(predecessor);
    const saved = { maintenance: process.env.ANCHOR_EMAIL_RECOVERY_MAINTENANCE };
    const reconcile = { researchMissionId: research.missionId };
    try {
      // Successor active (as its activation script would do once the envelope drains).
      await pool.query(`INSERT INTO acquisition_missions(id,tenant_id,objective,target_segment,payload,stage,status,client_id)
        VALUES($1,'10',$2,'commercial_cleaning_buyers',$3,'discover','active',10)`, [successor.id, successor.objective, successor]);
      await pool.query('UPDATE acquisition_outbound_programs SET source_mission_id=$2 WHERE id=$1', [program.id, successor.id]);
      assert.ok((await missions.previewReconciliation(pool, reconcile)).blockers.includes('research_reconcile_recovery_maintenance_active'));
      // Recovery ended: no recovery control table, kill switch lowered.
      await pool.query('ALTER TABLE anchor_email_recovery_control RENAME TO anchor_email_recovery_control_parked');
      process.env.ANCHOR_EMAIL_RECOVERY_MAINTENANCE = 'false';
      const plan = await missions.previewReconciliation(pool, reconcile);
      assert.deepEqual(plan.blockers, []); assert.ok(plan.candidates > 1);
      const candidates = (await pool.query('SELECT id,domain FROM scout_research_candidates WHERE research_mission_id=$1 ORDER BY id', [research.missionId])).rows;
      // One candidate is now owned by an AO; one is already in the queue.
      await pool.query(`INSERT INTO companies(id,client_id,name,domain,website) VALUES('late-owned',10,'Late Owned',$1,$2)`, [candidates[0].domain, `https://${candidates[0].domain}`]);
      await pool.query(`INSERT INTO prospects(id,client_id,company_id,email,do_not_contact,is_synthetic,ao_outreach_review_required,assigned_ao_id,last_contacted_at)
        VALUES('late-owned-p',10,'late-owned',$1,false,false,false,7,now())`, [`a@${candidates[0].domain}`]);
      await pool.query(`INSERT INTO scout_unenriched(client_id,company,website_url,domain,vertical,source,enrichment_attempts)
        VALUES(10,'Already Queued',$1,$2,'managed_facilities','max_buffer_replenishment',0)`, [`https://${candidates[1].domain}`, candidates[1].domain]);
      const before = await crm();
      await assert.rejects(missions.applyReconciliation(pool, { ...reconcile, approvalRef: 'synthetic-reconcile', confirm: '0'.repeat(64) }),
        { code: 'research_reconcile_plan_changed' });
      const applied = await missions.applyReconciliation(pool, { ...reconcile, approvalRef: 'synthetic-reconcile', confirm: plan.planHash });
      assert.equal(applied.ownershipHeld, 1); assert.equal(applied.alreadyQueued, 1);
      assert.equal(applied.queued, candidates.length - 2); assert.equal(applied.prospectsCreated, 0); assert.equal(applied.sent, 0);
      const after = await crm();
      assert.equal(after.queue, before.queue + applied.queued); assert.equal(after.prospects, before.prospects); assert.equal(after.companies, before.companies);
      // One-time and idempotent: a second pass queues nothing and duplicates nothing.
      const again = await missions.previewReconciliation(pool, reconcile);
      assert.equal(again.candidates, 1, 'only the ownership-held candidate remains for later review');
      const second = await missions.applyReconciliation(pool, { ...reconcile, approvalRef: 'synthetic-reconcile-2', confirm: again.planHash });
      assert.equal(second.queued, 0); assert.equal((await crm()).queue, after.queue);
      assert.equal(await count('SELECT count(*)::int n FROM (SELECT lower(domain) FROM scout_unenriched GROUP BY 1 HAVING count(*)>1) d'), 0);
      await assert.rejects(pool.query("UPDATE scout_research_candidates SET reconciled_at=NULL"), /permanent/);
      // Reconciled rows are ordinary queue work for the authorized mission, never prepared or sent here.
      assert.equal(await count("SELECT count(*)::int n FROM acquisition_outbound_items WHERE status='pending'"), 1);
    } finally {
      process.env.ANCHOR_EMAIL_RECOVERY_MAINTENANCE = saved.maintenance;
      await pool.query('ALTER TABLE IF EXISTS anchor_email_recovery_control_parked RENAME TO anchor_email_recovery_control');
      await pool.query('UPDATE acquisition_outbound_programs SET source_mission_id=$2 WHERE id=$1', [program.id, program.source_mission_id]);
      await pool.query('DELETE FROM acquisition_missions WHERE id=$1', [successor.id]);
      await pool.query("DELETE FROM prospects WHERE id='late-owned-p'"); await pool.query("DELETE FROM companies WHERE id='late-owned'");
    }
  });

  }
  if (part === 'governance') {
  await scenario('unavailable enforcement fails closed', async () => {
    await install();
    const claim = await claimDirect('meter-down');
    const broken = { connect: async () => { throw new Error('db down'); }, query: async () => { throw new Error('db down'); } };
    await assert.rejects(grants.runInResearchScope(claim, () => grants.googleMeter().reserve({ endpoint: 'text_search', tenantId: '10' }), broken),
      { code: grants.SPEND_CODE, reason: 'reservation_unavailable' });
    await grants.finishResearchCycle(pool, claim, { completed: false, result: {} });
  });

  await scenario('Google spending cannot exceed $5 per UTC day for Tenant 10, even under concurrency', async () => {
    const grant = await install();
    const spentToday = async () => Number((await pool.query(`SELECT COALESCE(sum(reserved_usd_micros),0)::bigint n FROM scout_research_google_spend
      WHERE tenant_id='10' AND usage_day=(clock_timestamp() AT TIME ZONE 'UTC')::date`)).rows[0].n);
    const claim = await claimDirect('budget');
    const start = await spentToday();
    const outcomes = await grants.runInResearchScope(claim, () => Promise.allSettled(Array.from({ length: 160 }, () =>
      grants.googleMeter().reserve({ endpoint: 'text_search', tenantId: '10' }))), pool);
    const granted = outcomes.filter(o => o.status === 'fulfilled');
    assert.equal(granted.length, Math.floor((5000000 - start) / 40000));
    for (const o of outcomes.filter(o => o.status === 'rejected')) assert.equal(o.reason.code, grants.SPEND_CODE, o.reason.message);
    assert.ok(await spentToday() <= 5000000);
    await grants.runInResearchScope(claim, async () => {
      // Fill any remainder with the cheapest paid call, then nothing fits.
      while (await grants.googleMeter().reserve({ endpoint: 'place_details', tenantId: '10' }).then(d => granted.push({ value: d }), () => false) !== false);
      await assert.rejects(grants.googleMeter().reserve({ endpoint: 'place_details', tenantId: '10' }), { reason: 'daily_budget_exhausted' });
      await assert.rejects(grants.googleMeter().reserve({ endpoint: 'unknown_endpoint', tenantId: '10' }), { reason: 'cost_unknown' });
    }, pool);
    for (const o of granted) await o.value.settle('uncertain');
    await grants.finishResearchCycle(pool, claim, { completed: false, result: { reason: 'budget_test' } });
    // Settlement never refunds, and a fresh grant cannot reset the day.
    const spent = await spentToday();
    assert.ok(spent <= 5000000 && spent > 5000000 - 25000, String(spent));
    await grants.revokeResearchOnlyGrant(pool, { grantId: grant.grantId, approvalReference: 'budget-close' }, { kind: 'operator_cli' });
    await install();
    const before = places.calls.length;
    assert.equal((await cycle('budget-next')).halted, 'research_only_daily_budget_exhausted', 'no cycle consumed when nothing is fundable');
    assert.equal(places.calls.length, before);
    assert.equal(await openCycles(), 0);
  });
  }
}
