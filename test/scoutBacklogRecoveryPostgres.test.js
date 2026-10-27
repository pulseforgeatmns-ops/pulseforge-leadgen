'use strict';
const deny = require('./helpers/denyExternalProviders.cjs');
const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), path = require('node:path');
const { startResearchHarness, ENV, REVISION } = require('./helpers/scoutBoundedResearchFixture');
const backlog = require('../services/scoutBacklogRecovery');
const { createCandidateTicketStore } = require('../services/scoutCandidateTickets');
const { loadRecoveryState } = require('../services/anchorEmailRecovery');
const researchAuthority = require('../services/scoutBoundedResearchAuthority');
const { createBacklogRecovery, draftManifest } = backlog._test;
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

test('authorized Scout backlog recovery re-processes only approved persisted tickets with every closed control armed', { timeout:240000 }, async t => {
  const h = await startResearchHarness(); t.after(() => h.close());
  const { pool } = h;
  await pool.query(fs.readFileSync(path.join(__dirname, '../migrations/2026-10-27-scout-backlog-recovery.sql'), 'utf8'));
  const envBefore = Object.fromEntries(Object.keys(ENV).map(k => [k, process.env[k]]));
  const n = async (sql, values = []) => (await pool.query(sql, values)).rows[0].n;
  let seq = 0;

  async function fixture({ windowMs = 30 * 60000, maxCandidates = 20 } = {}) {
    // Synthetic fixtures reuse business names; isolate the raw queue per scenario.
    await pool.query("DELETE FROM scout_unenriched; DELETE FROM prospects WHERE id LIKE 'owned-%'; DELETE FROM companies WHERE id LIKE 'owned-%'");
    const f = await h.newManifest();
    const fingerprint = f.manifest.fingerprint;
    const recovery = createBacklogRecovery({ approvedFingerprint:fingerprint });
    const draft = await draftManifest(pool, { approvalRef:`backlog-recovery-${++seq}`, programId:f.manifest.programId,
      missionId:f.manifest.missionId, quarantineDecisionHash:h.legacy.quarantine.decision_hash, expectedRevision:REVISION,
      notBefore:new Date(Date.now() - 1000).toISOString(), expiresAt:new Date(Date.now() + windowMs).toISOString(), maxCandidates },
    { approvedFingerprint:fingerprint });
    const work = () => pool.query(`SELECT candidate_key,state,reason,attempts::int AS attempts,claim_token
      FROM scout_candidate_work WHERE search_fingerprint=$1 ORDER BY candidate_key`, [fingerprint]).then(r => r.rows);
    return { f, fingerprint, recovery, draft, manifest:draft.manifest, hash:draft.manifestHash, work,
      install:() => recovery.install(pool, draft.manifest, draft.manifestHash),
      execute:(opts = {}) => recovery.execute({ pool:opts.pool || pool, manifest:opts.manifest || draft.manifest,
        confirmationHash:opts.hash || draft.manifestHash }) };
  }
  async function closeOpen() {
    for (const row of (await pool.query('SELECT manifest,manifest_hash FROM scout_backlog_recovery_grants WHERE revoked_at IS NULL')).rows) {
      await createBacklogRecovery({ approvedFingerprint:row.manifest.fingerprint }).revoke(pool, row.manifest, row.manifest_hash);
    }
  }
  const discoverySnapshot = async () => ({
    checkpoints:(await pool.query('SELECT to_jsonb(x) AS row FROM scout_query_checkpoints x ORDER BY to_jsonb(x)::text')).rows,
    pages:await n('SELECT count(*)::int n FROM scout_query_pages'),
    memberships:await n('SELECT count(*)::int n FROM scout_candidate_memberships'),
    work:await n('SELECT count(*)::int n FROM scout_candidate_work'),
    apiUsage:await n('SELECT count(*)::int n FROM api_usage_events'),
    placesRequests:await n('SELECT count(*)::int n FROM places_api_requests'),
    boundedRuns:await n('SELECT count(*)::int n FROM scout_bounded_acquisition_runs'),
    researchGrants:await n('SELECT count(*)::int n FROM scout_bounded_research_grants'),
  });
  async function scenario(name, fn) {
    await t.test(name, async () => {
      const history = await h.historySnapshot();
      try { await fn(); } finally { await closeOpen(); }
      assert.deepEqual(await h.historySnapshot(), history, 'no recovery/quarantine/preparation/mission history mutation');
      assert.deepEqual(Object.fromEntries(Object.keys(ENV).map(k => [k, process.env[k]])), envBefore, 'environment unchanged');
      const state = await loadRecoveryState(pool);
      assert.deepEqual(state, { mode:'maintenance', reason:'recovery_kill_switch' }, 'kill switch stays armed');
      assert.equal(deny.attempts(), 0, 'no external transport');
      assert.equal(h.hydrationGuard.attempts(), 0, 'no acquisition runtime/hydration entry');
    });
  }
  async function noConsumption(x) {
    assert.equal(await n('SELECT count(*)::int n FROM scout_backlog_recovery_runs WHERE approval_ref=$1', [x.manifest.approvalRef]), 0);
    assert.ok((await x.work()).every(row => row.attempts === 0 && row.claim_token === null), 'no claim or attempt');
  }

  await scenario('draft/preview are read-only; execution without an installed grant consumes nothing', async () => {
    const x = await fixture();
    assert.equal(x.draft.dueUnattempted, 20); assert.equal(x.manifest.candidates.length, 20); assert.equal(x.manifest.maxCandidates, 20);
    const readonly = { async connect() { const db = await pool.connect(); return { release:db.release.bind(db),
      async query(sql, values) { assert.match(typeof sql === 'string' ? sql : sql.text, /^(SELECT|BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY|SET LOCAL|COMMIT|ROLLBACK)/); return db.query(sql, values); } }; } };
    const preview = await x.recovery.preview(readonly, x.manifest);
    assert.equal(preview.status, 'not_installed'); assert.equal(preview.eligibleCandidates, 20); assert.equal(preview.manifestHash, x.hash);
    assert.equal((await x.recovery.inspect(readonly, x.manifest)).outcome, 'not_started');
    await assert.rejects(x.execute(), /backlog_recovery_grant_required/); await noConsumption(x);
  });

  await scenario('installed one-shot recovery queues approved backlog with no discovery, provider or downstream action', async () => {
    const x = await fixture(); const before = await discoverySnapshot();
    assert.equal((await x.install()).installed, true);
    const result = await x.execute();
    assert.equal(result.status, 'completed', JSON.stringify(result));
    assert.equal(result.totals.claimed, 20); assert.equal(result.totals.completed, 20);
    assert.equal(result.totals.queued, 20); assert.equal(result.totals.queuedVerifiedDownstream, 20); assert.equal(result.admitted, 20);
    assert.equal(result.providerCalls, 0); assert.equal(result.cleanVerifiedContacts, 0);
    assert.deepEqual(await discoverySnapshot(), before, 'no checkpoint, page, ticket creation, Places or API usage');
    const work = await x.work();
    assert.ok(work.every(row => row.state === 'handled' && row.reason === 'queued' && row.attempts === 1 && row.claim_token === null));
    // Actual downstream records: one raw enrichment-queue row per queued business, nothing beyond the queue.
    const queue = (await pool.query("SELECT company,domain,source,enrichment_attempts FROM scout_unenriched WHERE client_id=10 AND domain LIKE $1",
      [`old-${x.f.manifest.approvalRef.split('-').pop()}-%`])).rows;
    assert.equal(queue.length, 20); assert.ok(queue.every(row => row.source === 'max_buffer_replenishment' && row.enrichment_attempts === 0));
    const receipt = await x.recovery.inspect(pool, x.manifest);
    assert.equal(receipt.outcome, 'completed'); assert.equal(receipt.totals.queuedVerifiedDownstream, 20); assert.deepEqual(receipt.skipped, []);
    assert.ok(receipt.candidates.every(c => c.downstream.verified && c.downstream.matchedBy === 'domain' && c.downstream.queueRowIds.length === 1));
  });

  await scenario('duplicate and concurrent execution are replay-safe: exactly one consumption, no second claim', async () => {
    const x = await fixture(); await x.install();
    const settled = await Promise.allSettled([x.execute(), x.execute()]);
    assert.equal(settled.filter(s => s.status === 'fulfilled').length, 1);
    assert.match(settled.find(s => s.status === 'rejected').reason.code, /backlog_recovery_(in_progress|already_consumed)/);
    await assert.rejects(x.execute(), /backlog_recovery_already_consumed/);
    assert.ok((await x.work()).every(row => row.attempts === 1), 'no candidate attempted twice');
    assert.equal(await n('SELECT count(*)::int n FROM scout_backlog_recovery_runs WHERE approval_ref=$1', [x.manifest.approvalRef]), 1);
    assert.equal(await n("SELECT count(*)::int n FROM scout_backlog_recovery_items WHERE approval_ref=$1 AND disposition='claimed'", [x.manifest.approvalRef]), 20);
    await assert.rejects(pool.query('DELETE FROM scout_backlog_recovery_runs'), /permanent/);
    await assert.rejects(pool.query("UPDATE scout_backlog_recovery_runs SET state='claimed',finished_at=NULL WHERE approval_ref=$1", [x.manifest.approvalRef]), /permanent/);
    await assert.rejects(pool.query('UPDATE scout_backlog_recovery_items SET skip_reason=NULL'), /permanent/);
    // A revoked consumed approval can never be reinstalled.
    await x.recovery.revoke(pool, x.manifest, x.hash);
    await assert.rejects(x.install(), /backlog_recovery_grant_revoked/);
  });

  await scenario('expired approval: cannot install, and an installed grant that lapses cannot execute', async () => {
    const expired = await fixture({ windowMs:1500 });
    await sleep(1600);
    await assert.rejects(expired.install(), /backlog_recovery_window_inactive/); await noConsumption(expired);
    await closeOpen();
    const lapsing = await fixture({ windowMs:2500 });
    await lapsing.install(); await sleep(2600);
    await assert.rejects(lapsing.execute(), /backlog_recovery_window_inactive/); await noConsumption(lapsing);
    assert.equal((await lapsing.recovery.inspect(pool, lapsing.manifest)).status, 'expired');
  });

  await scenario('wrong tenant, wrong fingerprint and widened scope are rejected before any database work', async () => {
    const x = await fixture();
    const mutate = fn => { const m = structuredClone(x.manifest); fn(m); return m; };
    await assert.rejects(x.recovery.preview(pool, mutate(m => { m.tenantId = '11'; })), /backlog_recovery_tenant_not_authorized/);
    await assert.rejects(x.execute({ manifest:mutate(m => { m.tenantId = 11; }) }), /backlog_recovery_tenant_not_authorized/);
    await assert.rejects(x.recovery.preview(pool, mutate(m => { m.fingerprint = 'e'.repeat(64); })), /backlog_recovery_fingerprint_not_authorized/);
    // The production-bound API accepts only the verified production fingerprint.
    await assert.rejects(backlog.previewBacklogRecovery(pool, x.manifest), /backlog_recovery_fingerprint_not_authorized/);
    await assert.rejects(backlog.executeBacklogRecovery({ pool, manifest:x.manifest, confirmationHash:x.hash }), /backlog_recovery_fingerprint_not_authorized/);
    await assert.rejects(draftManifest(pool, { approvalRef:'x', programId:x.manifest.programId,
      missionId:x.manifest.missionId, quarantineDecisionHash:h.legacy.quarantine.decision_hash, expectedRevision:REVISION,
      notBefore:x.manifest.window.notBefore, expiresAt:x.manifest.window.expiresAt, maxCandidates:20 }), /backlog_recovery_fingerprint_not_authorized/);
    for (const [label, fn, code] of [
      ['21 candidates', m => { m.maxCandidates = 21; }, /candidate_limit/],
      ['count mismatch', m => { m.maxCandidates = 19; }, /candidate_limit/],
      ['duplicate candidate', m => { m.candidates[1] = m.candidates[0]; }, /candidates_invalid/],
      ['discovery right', m => { m.authority.discovery = true; }, /authority_invalid/],
      ['places right', m => { m.authority.places = true; }, /authority_invalid/],
      ['email right', m => { m.authority.email = true; }, /authority_invalid/],
      ['unknown field', m => { m.extra = true; }, /manifest_invalid/],
      ['window over 60 minutes', m => { m.window.expiresAt = new Date(Date.parse(m.window.notBefore) + 3600001).toISOString(); }, /window_invalid/],
      ['other plan version', m => { m.planVersion = 'other'; }, /manifest_invalid/],
    ]) await assert.rejects(x.recovery.preview(pool, mutate(fn)), code, label);
    // A pinned program/source that changed, or a different revision, fails closed.
    await assert.rejects(x.recovery.preview(pool, mutate(m => { m.programHash = '0'.repeat(64); })), /backlog_recovery_source_changed/);
    await assert.rejects(x.recovery.preview(pool, mutate(m => { m.authority.expectedRevision = 'b'.repeat(40); })), /closed_environment/);
    await assert.rejects(x.recovery.install(pool, x.manifest, '0'.repeat(64)), /exact_confirmation/);
    await x.install();
    await assert.rejects(x.execute({ hash:'0'.repeat(64) }), /exact_confirmation/);
    await noConsumption(x);
  });

  await scenario('lowering any closed control or opening another authority blocks execution', async () => {
    const x = await fixture(); await x.install();
    for (const [key, value] of [['ANCHOR_EMAIL_RECOVERY_MAINTENANCE', 'false'], ['ANCHOR_RESEARCH_PROVIDERS_PAUSED', 'false'],
      ['ANCHOR_GOVERNED_OUTBOUND_ENABLED', 'true'], ['ANCHOR_EMAIL_SCOPED_RESUME_ID', 'synthetic'], ['ANCHOR_EMAIL_RECOVERY_MANIFEST_ID', 'synthetic']]) {
      const old = process.env[key]; process.env[key] = value;
      try { await assert.rejects(x.execute(), /closed_environment/); } finally { old === undefined ? delete process.env[key] : process.env[key] = old; }
    }
    const research = await h.newManifest();
    await researchAuthority.installResearchAuthority(pool, research.manifest, (await researchAuthority.previewResearchAuthority(pool, research.manifest)).manifestHash);
    try { await assert.rejects(x.execute(), /backlog_recovery_other_authority_open/); }
    finally { await researchAuthority.revokeResearchAuthority(pool, research.manifest, (await researchAuthority.inspectResearchAuthority(pool, research.manifest)).manifestHash); }
    await x.recovery.revoke(pool, x.manifest, x.hash);
    await assert.rejects(x.execute(), /backlog_recovery_grant_revoked/);
    await noConsumption(x);
  });

  await scenario('stale claims: work claimed elsewhere or changed since approval is skipped and left untouched', async () => {
    const x = await fixture();
    const [held, changed, ...rest] = x.manifest.candidates.map(c => c.candidateKey);
    const store = createCandidateTicketStore({ pool, tenantId:'10', fingerprint:x.fingerprint });
    const other = await store.claimDue({ dependencyKey:'other-worker', limit:1, candidateKeys:[held] });
    assert.equal(other.length, 1);
    await pool.query('UPDATE scout_candidate_work SET membership_revision=membership_revision+1 WHERE search_fingerprint=$1 AND candidate_key=$2', [x.fingerprint, changed]);
    await x.install();
    const result = await x.execute();
    assert.equal(result.status, 'completed'); assert.equal(result.totals.claimed, 18); assert.equal(result.totals.queuedVerifiedDownstream, 18);
    assert.deepEqual(result.skipped.map(s => s.reason).sort(), ['claimed_elsewhere', 'membership_revision_changed']);
    const work = new Map((await x.work()).map(row => [row.candidate_key, row]));
    assert.equal(work.get(held).claim_token, other[0].claimToken, 'other worker claim is not stolen'); assert.equal(work.get(held).attempts, 1);
    assert.equal(work.get(changed).attempts, 0); assert.equal(work.get(changed).state, 'pending');
    assert.ok(rest.every(key => work.get(key).reason === 'queued'));
  });

  await scenario('a lease that expires mid-run is not finished by this run; the run stops and cannot be replayed', async () => {
    const x = await fixture(); await x.install();
    const target = x.manifest.candidates[5].candidateKey; let expired = false;
    const intercepting = { options:pool.options, query:(...args) => pool.query(...args), async connect() {
      const db = await pool.connect();
      return { release:db.release.bind(db), on:db.on.bind(db), removeListener:db.removeListener.bind(db),
        async query(sql, values) {
          if (!expired && typeof sql === 'string' && sql.includes('AS lease_active') && Array.isArray(values?.[4]) && values[4].includes(target)) {
            expired = true;
            await pool.query("UPDATE scout_candidate_work SET lease_until=clock_timestamp()-interval '1 second' WHERE search_fingerprint=$1 AND candidate_key=$2", [x.fingerprint, target]);
          }
          return db.query(sql, values);
        } };
    } };
    const result = await x.execute({ pool:intercepting });
    assert.equal(expired, true);
    assert.equal(result.status, 'stopped'); assert.equal(result.stopReason, 'backlog_recovery_claims_not_completed');
    assert.equal(result.totals.unfinished, 1); assert.equal(result.totals.completed, 19); assert.equal(result.totals.queuedVerifiedDownstream, 19);
    const row = (await x.work()).find(r => r.candidate_key === target);
    assert.notEqual(row.reason, 'queued'); assert.equal(row.state, 'pending', 'stale ticket remains durable work for a later authorized pass');
    await assert.rejects(x.execute(), /backlog_recovery_already_consumed/);
    assert.equal((await x.recovery.inspect(pool, x.manifest)).outcome, 'stopped');
  });

  await scenario('ownership collision defers the owned business instead of queueing it', async () => {
    const x = await fixture(); await x.install();
    const raw = x.f.raw[3];
    const company = (await pool.query("INSERT INTO companies(id,client_id,name,domain,website) VALUES($1,10,$2,$3,$4) RETURNING id",
      [`owned-${x.manifest.approvalRef}`, raw.companyName, raw.website, `https://${raw.website}`])).rows[0];
    await pool.query("INSERT INTO prospects(id,company_id,client_id,email,last_contacted_at) VALUES($1,$2,10,$3,now()-interval '2 days')",
      [`owned-prospect-${x.manifest.approvalRef}`, company.id, `owner@${raw.website}`]);
    const result = await x.execute();
    assert.equal(result.totals.completed, 20); assert.equal(result.totals.queued, 19); assert.equal(result.totals.queuedVerifiedDownstream, 19);
    assert.equal(result.admitted, 19);
    const owned = result.candidates.filter(c => c.reason !== 'queued');
    assert.equal(owned.length, 1); assert.equal(owned[0].state, 'deferred'); assert.match(owned[0].reason, /ownership|contact_policy|already_usable/);
    assert.equal(await n('SELECT count(*)::int n FROM scout_unenriched WHERE domain=$1', [raw.website]), 0, 'owned business never queued');
  });

  await scenario('partial failure: committed outcomes are kept, the failing claim is not finished, the approval is not reusable', async () => {
    const x = await fixture(); await x.install();
    const poison = x.f.raw[7].website;
    await pool.query(`CREATE OR REPLACE FUNCTION synthetic_queue_failure() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN IF NEW.domain='${poison}' THEN RAISE EXCEPTION 'synthetic queue failure'; END IF; RETURN NEW; END $$;
      CREATE TRIGGER synthetic_queue_failure BEFORE INSERT ON scout_unenriched FOR EACH ROW EXECUTE FUNCTION synthetic_queue_failure();`);
    try {
      await assert.rejects(x.execute(), error => { assert.equal(error.backlogRecoveryResult.totals.claimed, 20); return true; });
    } finally { await pool.query('DROP TRIGGER synthetic_queue_failure ON scout_unenriched; DROP FUNCTION synthetic_queue_failure();'); }
    const receipt = await x.recovery.inspect(pool, x.manifest);
    assert.equal(receipt.outcome, 'stopped'); assert.equal(receipt.run.stopReason, 'backlog_recovery_failed');
    assert.deepEqual(Object.keys(receipt.totals.byReason), ['queued']);
    assert.equal(receipt.totals.queued, await n('SELECT count(*)::int n FROM scout_unenriched'), 'one queue row per committed queue outcome');
    assert.equal(receipt.totals.completed + receipt.totals.unfinished, 20); assert.ok(receipt.totals.unfinished >= 1);
    assert.equal(receipt.totals.queuedNotVerifiedDownstream, 0, 'every committed queue outcome has its downstream row');
    assert.equal(await n('SELECT count(*)::int n FROM scout_unenriched WHERE domain=$1', [poison]), 0);
    await assert.rejects(x.execute(), /backlog_recovery_already_consumed/);
  });

  await scenario('an approval whose candidates were all processed since approval consumes nothing', async () => {
    const x = await fixture(); await x.install(); await x.execute();
    await x.recovery.revoke(pool, x.manifest, x.hash);
    const again = { ...structuredClone(x.manifest), approvalRef:`${x.manifest.approvalRef}-again` };
    const hash = x.recovery.validateManifest(again).manifestHash;
    await x.recovery.install(pool, again, hash);
    await assert.rejects(x.recovery.execute({ pool, manifest:again, confirmationHash:hash }), /backlog_recovery_no_eligible_candidates/);
    assert.equal(await n('SELECT count(*)::int n FROM scout_backlog_recovery_runs WHERE approval_ref=$1', [again.approvalRef]), 0);
    assert.ok((await x.work()).every(row => row.attempts === 1));
  });
});
