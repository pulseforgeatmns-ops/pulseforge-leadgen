'use strict';

// Real PostgreSQL only. No DATABASE_URL, provider, scheduler, route or external migration.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { execFileSync } = require('node:child_process');
const { randomUUID } = require('node:crypto');
const { createTrackedPostgresPool, closePostgresFixture } = require('./helpers/closePostgresPool');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { createCandidateTicketStore, candidateKey, TICKET_VERSION, CRASH_RETRY_MS, LEASE_MS } = require('../services/scoutCandidateTickets');
const { PLAN_VERSION } = require('../services/scoutQueryCheckpoints');

function available(name) {
  try { execFileSync('sh', ['-c', `command -v ${name}`], { stdio: 'pipe' }); return true; }
  catch {
    try { return fs.existsSync(path.join(execFileSync('pg_config', ['--bindir'], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] }).trim(), name)); }
    catch { return false; }
  }
}
const availablePg = available('initdb') && available('pg_ctl');
const required = process.env.SCOUT_CANDIDATE_REQUIRE_POSTGRES === 'true';

test('real PostgreSQL: atomic raw transfer, exact leases, independent outcomes and restart fencing', {
  timeout: 60000,
  skip: !availablePg && !required ? 'Local PostgreSQL unavailable; release gate must set SCOUT_CANDIDATE_REQUIRE_POSTGRES=true' : false,
}, async t => {
  assert.ok(availablePg, 'Release validation requires local PostgreSQL binaries');
  const instance = await startDisposablePostgres('scout-candidate-');
  const db = createTrackedPostgresPool({ connectionString: instance.connectionString, max: 6 });
  t.after(() => closePostgresFixture(db, instance));
  const migration = name => fs.readFileSync(path.join(__dirname, '../migrations', name), 'utf8');
  await db.query(migration('2026-10-17-scout-query-checkpoints.sql'));
  const absent = createCandidateTicketStore({ pool: db, tenantId: 'absent', fingerprint: 'f' });
  await assert.rejects(absent.assertReady(), error => error.ticketCode === 'schema_not_ready');
  await db.query(migration('2026-10-19-scout-candidate-tickets.sql'));
  await db.query(migration('2026-10-19-scout-candidate-tickets.sql')); // additive/idempotent
  await absent.assertReady(); // failure is not permanently cached
  await db.query('CREATE TABLE fixture_business_writes (id text PRIMARY KEY, tenant_id text, value text)');

  function fixture(options = {}) {
    const tenantId = options.tenantId || randomUUID();
    const fingerprint = options.fingerprint || randomUUID();
    const planVersion = options.planVersion || PLAN_VERSION;
    const ticketVersion = options.ticketVersion || TICKET_VERSION;
    const config = { pool: db, tenantId, fingerprint, planVersion, ticketVersion };
    const store = createCandidateTicketStore(config);
    return { ...config, store,
      async stage(queryKey, raw, version = planVersion) {
        await db.query(`INSERT INTO scout_query_checkpoints(tenant_id,search_fingerprint,query_key,plan_version,state,candidates)
          VALUES($1,$2,$3,$4,'fetched',$5::jsonb)`, [tenantId, fingerprint, queryKey, version, JSON.stringify(raw)]);
      },
      async work() { return (await db.query(`SELECT * FROM scout_candidate_work WHERE tenant_id=$1 AND search_fingerprint=$2
        AND plan_version=$3 AND ticket_version=$4 ORDER BY candidate_key`, [tenantId, fingerprint, planVersion, ticketVersion])).rows; },
      async due() { await db.query(`UPDATE scout_candidate_work SET next_attempt_at=clock_timestamp()-interval '1 second'
        WHERE tenant_id=$1 AND search_fingerprint=$2`, [tenantId, fingerprint]); },
      async expire() { await db.query(`UPDATE scout_candidate_work SET lease_until=clock_timestamp()-interval '1 second'
        WHERE tenant_id=$1 AND search_fingerprint=$2 AND claim_token IS NOT NULL`, [tenantId, fingerprint]); },
    };
  }
  const outcome = (state, reason, extra = {}) => ({ state, reason, dependencyKey: 'dep-v1', ...extra });
  const clean = raw => { const { _scoutTicketRefs, ...rest } = raw; return rest; };
  async function createClaim(raw = [{ placeId: randomUUID(), name: 'Fixture' }]) {
    const f = fixture(); await f.stage('q', raw); await f.store.transferQuery('q');
    return { ...f, claims: await f.store.claimDue({ dependencyKey: 'dep-v1' }) };
  }

  await t.test('raw payload, contradictory observations and duplicates survive atomic exact transfer', async () => {
    const f = fixture();
    const raw = [{ placeId: 'p', name: 'A', website: 'a.example', evidence: { industry: 'cleaning' } },
      { placeId: 'p', name: 'B', website: 'b.example', evidence: { industry: 'restaurant' } },
      { placeId: 'p', name: 'A', website: 'a.example', evidence: { industry: 'cleaning' } }];
    await f.stage('q1', raw);
    const [a, b] = await Promise.all([f.store.transferQuery('q1'), f.store.transferQuery('q1')]);
    assert.deepEqual(a, b); assert.deepEqual(a.candidates.map(clean), raw);
    assert.equal(new Set(a.candidates.flatMap(hit => hit._scoutTicketRefs.map(ref => ref.memberKey))).size, 3);
    assert.equal((await f.work()).length, 1); assert.equal((await f.work())[0].membership_revision, '3');
    const checkpoint = (await db.query('SELECT state,candidates FROM scout_query_checkpoints WHERE tenant_id=$1', [f.tenantId])).rows[0];
    assert.equal(checkpoint.state, 'handled'); assert.deepEqual(checkpoint.candidates, raw);
    const [claim] = await f.store.claimDue({ dependencyKey: 'dep-v1' });
    assert.deepEqual(claim.members.map(clean), raw); assert.equal(claim.refs.length, 3);
    await assert.rejects(f.store.finish(claim.refs.slice(0, 1), outcome('handled', 'queued')), /Complete/);
    await assert.rejects(f.store.renew([{ ...claim.refs[0], queryKey: 'not-observed' }, ...claim.refs.slice(1)]), /membership was not found/);
    await assert.rejects(f.store.renew([{ ...claim.refs[0], tenantId: 'other' }]), /scope/);
    await assert.rejects(f.store.finish(a.candidates[0]._scoutTicketRefs, outcome('handled', 'queued')), /claim mismatch/);
  });


  await t.test('JSONB transfer preserves exact large numbers and distinguishes raw fallback identities', async () => {
    const f = fixture();
    const rawText = '[{"value":9007199254740992,"nested":{"decimal":0.12345678901234567890123456789}},{"value":9007199254740993,"nested":{"decimal":0.12345678901234567890123456789}}]';
    await db.query(`INSERT INTO scout_query_checkpoints(tenant_id,search_fingerprint,query_key,plan_version,state,candidates)
      VALUES($1,$2,'precise',$3,'fetched',$4::jsonb)`, [f.tenantId, f.fingerprint, PLAN_VERSION, rawText]);
    await f.store.transferQuery('precise');
    assert.equal((await f.work()).length, 2);
    const equality = await db.query(`SELECT bool_and(m.raw_candidate=c.candidates->m.ordinal) AS exact
      FROM scout_candidate_memberships m JOIN scout_query_checkpoints c
      ON c.tenant_id=m.tenant_id AND c.search_fingerprint=m.search_fingerprint AND c.query_key=m.query_key
      WHERE m.tenant_id=$1`, [f.tenantId]);
    assert.equal(equality.rows[0].exact, true);
    await f.store.transferQuery('precise');
    assert.deepEqual((await f.work()).map(row => row.membership_revision), ['1', '1']);
  });

  await t.test('new query memberships are exact; opposite-order transfers converge without deadlock', async () => {
    const f = fixture(); const a = { placeId: 'a', address: 'North' }; const b = { placeId: 'b', address: 'South' };
    await f.stage('q1', [a, b]); await f.stage('q2', [b, a]);
    await Promise.all([f.store.transferQuery('q1'), f.store.transferQuery('q2')]);
    assert.deepEqual((await f.work()).map(row => row.membership_revision), ['2', '2']);
    const claims = await f.store.claimDue({ dependencyKey: 'dep-v1' });
    for (const claim of claims) assert.deepEqual(new Set(claim.refs.map(ref => ref.queryKey)), new Set(['q1', 'q2']));
  });

  await t.test('independent workers use SKIP LOCKED; all connections are released before provider work', async () => {
    const f = fixture(); await f.stage('q', Array.from({ length: 6 }, (_, i) => ({ placeId: `p${i}` })));
    await f.store.transferQuery('q');
    const [left, right] = await Promise.all([f.store.claimDue({ dependencyKey: 'dep-v1', limit: 3 }),
      f.store.claimDue({ dependencyKey: 'dep-v1', limit: 3 })]);
    assert.equal(left.length, 3); assert.equal(right.length, 3);
    assert.equal(new Set([...left, ...right].map(claim => claim.candidateKey)).size, 6);
    assert.equal(new Set([...left, ...right].map(claim => claim.claimToken)).size, 6);
    assert.equal(db.waitingCount, 0); assert.equal(db.idleCount, db.totalCount);
    assert.equal((await f.store.snapshot({ dependencyKey: 'dep-v1' })).claimed, 6);
    assert.deepEqual(await f.store.claimDue({ dependencyKey: 'dep-v2' }), []);
    const row = (await f.work())[0];
    assert.ok(row.lease_until - row.updated_at <= LEASE_MS + 100);
    assert.ok(row.next_attempt_at - row.updated_at >= CRASH_RETRY_MS - 100);
  });

  await t.test('callback and terminal receipt commit atomically; replay does not repeat business writes', async () => {
    const f = await createClaim(); const [claim] = f.claims; let calls = 0;
    const write = async client => { calls += 1; await client.query('INSERT INTO fixture_business_writes VALUES($1,$2,$3)', [f.tenantId, f.tenantId, 'queued']); return 'saved'; };
    assert.deepEqual(await f.store.finish(claim.refs, outcome('handled', 'queued'), write), { applied: true, result: 'saved' });
    assert.deepEqual(await f.store.finish(claim.refs, outcome('handled', 'queued'), write), { applied: true, replayed: true, result: null });
    assert.equal(calls, 1);
    await assert.rejects(f.store.finish(claim.refs, outcome('handled', 'recovered'), write), /Conflicting/);
    assert.deepEqual(await f.store.claimDue({ dependencyKey: 'changed' }), []);
    assert.equal((await f.work())[0].state, 'handled');
  });

  await t.test('failed business write rolls back outcome and all same-client writes', async () => {
    const f = await createClaim(); const [claim] = f.claims;
    await assert.rejects(f.store.finish(claim.refs, outcome('handled', 'queued'), async client => {
      await client.query('INSERT INTO fixture_business_writes VALUES($1,$2,$3)', [f.tenantId, f.tenantId, 'partial']);
      throw Error('injected business failure');
    }), /injected/);
    assert.equal((await db.query('SELECT * FROM fixture_business_writes WHERE id=$1', [f.tenantId])).rows.length, 0);
    assert.equal((await f.work())[0].claim_token, claim.claimToken);
    assert.equal((await f.work())[0].state, 'pending');
    await f.store.finish(claim.refs, outcome('handled', 'queued'));
  });

  await t.test('ambiguous completion COMMIT is safe to retry without another callback', async () => {
    const f = await createClaim(); const [claim] = f.claims; let inject = true; let calls = 0;
    const wrapped = { query: (...args) => db.query(...args), connect: async () => {
      const client = await db.connect();
      return { query: async (...args) => { const result = await client.query(...args);
        if (inject && args[0] === 'COMMIT') { inject = false; throw Error('lost commit response'); } return result; },
      release: broken => client.release(broken) };
    } };
    const store = createCandidateTicketStore({ ...f, pool: wrapped });
    await assert.rejects(store.finish(claim.refs, outcome('handled', 'queued'), async client => {
      calls += 1; await client.query('INSERT INTO fixture_business_writes VALUES($1,$2,$3)', [f.tenantId, f.tenantId, 'committed']);
    }), error => error.ticketCode === 'commit_uncertain');
    const replay = await f.store.finish(claim.refs, outcome('handled', 'queued'), async () => { calls += 1; });
    assert.equal(replay.replayed, true); assert.equal(calls, 1);
    assert.equal((await db.query('SELECT * FROM fixture_business_writes WHERE id=$1', [f.tenantId])).rows.length, 1);
  });

  await t.test('membership arrival during active claim fences stale completion and preserves new work', async () => {
    const f = await createClaim([{ placeId: 'same', website: 'old.example' }]); const [claim] = f.claims;
    await f.stage('new-query', [{ placeId: 'same', website: 'contradictory.example' }]);
    await f.store.transferQuery('new-query');
    await assert.rejects(f.store.renew(claim.refs), error => error.ticketCode === 'membership_changed');
    let calls = 0;
    const result = await f.store.finish(claim.refs, outcome('handled', 'queued'), async () => { calls += 1; });
    assert.deepEqual(result, { applied: false, reason: 'membership_changed', result: null }); assert.equal(calls, 0);
    assert.equal((await f.work())[0].state, 'pending'); assert.equal((await f.work())[0].membership_revision, '2');
    assert.deepEqual(await f.store.claimDue({ dependencyKey: 'dep-v1' }), []); // retain crash reservation
    await f.due();
    const [fresh] = await f.store.claimDue({ dependencyKey: 'dep-v1' }); assert.equal(fresh.members.length, 2);
    await f.store.finish(fresh.refs, outcome('handled', 'already_usable'));
    await f.stage('third-query', [{ placeId: 'same', website: 'third.example' }]); await f.store.transferQuery('third-query');
    assert.equal((await f.work())[0].state, 'pending');
    assert.equal((await f.store.claimDue({ dependencyKey: 'dep-v1' }))[0].members.length, 3);
  });

  await t.test('expired leases are fenced and crashes wait one hour; replacement tokens cannot be borrowed', async () => {
    const f = await createClaim(); const [old] = f.claims; await f.expire();
    await assert.rejects(f.store.renew(old.refs), error => error.ticketCode === 'stale_claim');
    await assert.rejects(f.store.finish(old.refs, outcome('handled', 'queued')), error => error.ticketCode === 'stale_claim');
    assert.deepEqual(await f.store.claimDue({ dependencyKey: 'dep-v1' }), []);
    await f.due(); const [fresh] = await f.store.claimDue({ dependencyKey: 'dep-v1' });
    assert.notEqual(fresh.claimToken, old.claimToken);
    await assert.rejects(f.store.renew(old.refs), error => error.ticketCode === 'stale_claim');
    assert.deepEqual(await f.store.renew(fresh.refs), fresh.refs);
    assert.equal((await f.work())[0].attempts, '2');
  });

  await t.test('blocked dependencies do not hot-loop; uncertain deferrals cool down; clean budget deferral releases', async () => {
    const f = await createClaim(); let [claim] = f.claims;
    await f.store.finish(claim.refs, outcome('blocked', 'recovery_tenant_unsupported'));
    for (let i = 0; i < 5; i += 1) assert.deepEqual(await f.store.claimDue({ dependencyKey: 'dep-v1' }), []);
    const snap = await f.store.snapshot({ dependencyKey: 'dep-v1' });
    assert.equal(snap.byReason.recovery_tenant_unsupported, 1); assert.equal(snap.due, 0); assert.equal(snap.status, 'pending_candidate_work');
    [claim] = await f.store.claimDue({ dependencyKey: 'dep-v2' }); assert.ok(claim);
    await f.store.finish(claim.refs, outcome('deferred', 'recovery_failed', { retryAfterMs: 0, dependencyKey: 'dep-v2' }));
    assert.deepEqual(await f.store.claimDue({ dependencyKey: 'dep-v2' }), []);
    await f.due(); [claim] = await f.store.claimDue({ dependencyKey: 'dep-v2' });
    await f.store.finish(claim.refs, outcome('deferred', 'evidence_budget_deferred', { retryAfterMs: 0, dependencyKey: 'dep-v2' }));
    assert.equal((await f.store.claimDue({ dependencyKey: 'dep-v2' })).length, 1);
  });

  await t.test('unclassifiable evidence cannot be terminal; evidence-backed policy exclusion is explicit', async () => {
    const f = await createClaim(); const [claim] = f.claims;
    for (const reason of ['fetch_failed', 'unclassifiable', 'provider_unavailable']) {
      await assert.rejects(f.store.finish(claim.refs, outcome('handled', reason)), /Unsupported/);
    }
    await assert.rejects(f.store.finish(claim.refs, outcome('handled', 'policy_excluded')), /supporting evidence/);
    await f.store.finish(claim.refs, outcome('handled', 'policy_excluded', { evidence: { classification: 'outside_service_area', source: 'raw-address' } }));
    assert.equal((await f.work())[0].outcome_evidence.classification, 'outside_service_area');
  });

  await t.test('tenant, mission, plan and ticket versions isolate claims and references', async () => {
    const f = await createClaim(); const [claim] = f.claims;
    for (const changes of [{ tenantId: 'other' }, { fingerprint: 'other' }, { planVersion: 'other' }, { ticketVersion: 'other' }]) {
      const other = createCandidateTicketStore({ ...f, ...changes });
      assert.deepEqual(await other.claimDue({ dependencyKey: 'dep-v1' }), []);
      await assert.rejects(other.renew(claim.refs), /scope/);
      if (!changes.ticketVersion) await assert.rejects(other.transferQuery('q'), /checkpoint/);
    }
    const otherVersion = createCandidateTicketStore({ ...f, ticketVersion: 'next' });
    const transferred = await otherVersion.transferQuery('q'); assert.equal(transferred.candidates.length, 1);
    assert.equal((await otherVersion.claimDue({ dependencyKey: 'dep-v1' })).length, 1);
    assert.equal((await f.work())[0].claim_token, claim.claimToken);
  });


  await t.test('real backend crash rolls back partial business writes and leaves the claimed reservation intact', async () => {
    const f = await createClaim(); const [claim] = f.claims;
    await assert.rejects(f.store.finish(claim.refs, outcome('handled', 'queued'), async client => {
      await client.query('INSERT INTO fixture_business_writes VALUES($1,$2,$3)', [f.tenantId, f.tenantId, 'partial']);
      const pid = (await client.query('SELECT pg_backend_pid() AS pid')).rows[0].pid;
      await db.query('SELECT pg_terminate_backend($1)', [pid]);
      await client.query('SELECT 1');
    }));
    assert.equal((await db.query('SELECT * FROM fixture_business_writes WHERE id=$1', [f.tenantId])).rows.length, 0);
    const row = (await f.work())[0];
    assert.equal(row.state, 'pending'); assert.equal(row.claim_token, claim.claimToken);
    assert.deepEqual(await f.store.claimDue({ dependencyKey: 'dep-v1' }), []);
  });

  await t.test('ambiguous transfer commit retries converge on exact memberships without revision inflation', async () => {
    const f = fixture(); await f.stage('q', [{ placeId: 'one' }, { placeId: 'two' }]); let inject = true;
    const wrapped = { query: (...args) => db.query(...args), connect: async () => {
      const client = await db.connect(); return { query: async (...args) => {
        const result = await client.query(...args);
        if (inject && args[0] === 'COMMIT') { inject = false; throw Error('lost transfer response'); }
        return result;
      }, release: broken => client.release(broken) };
    } };
    await assert.rejects(createCandidateTicketStore({ ...f, pool: wrapped }).transferQuery('q'), error => error.ticketCode === 'commit_uncertain');
    const result = await f.store.transferQuery('q'); assert.equal(result.candidates.length, 2);
    assert.deepEqual((await f.work()).map(row => row.membership_revision), ['1', '1']);
  });

  await t.test('transfer failure is all-or-nothing and retry preserves one membership', async () => {
    const f = fixture(); await f.stage('q', [{ placeId: 'p' }]); let failOnce = true;
    const wrapped = { query: (...args) => db.query(...args), connect: async () => {
      const client = await db.connect(); return { query: (...args) => {
        if (failOnce && /UPDATE scout_query_checkpoints/.test(args[0])) { failOnce = false; throw Error('transfer crash'); }
        return client.query(...args);
      }, release: broken => client.release(broken) };
    } };
    await assert.rejects(createCandidateTicketStore({ ...f, pool: wrapped }).transferQuery('q'), /transfer crash/);
    assert.equal((await f.work()).length, 0);
    assert.equal((await db.query('SELECT state FROM scout_query_checkpoints WHERE tenant_id=$1', [f.tenantId])).rows[0].state, 'fetched');
    await f.store.transferQuery('q'); await f.store.transferQuery('q');
    assert.equal((await f.work())[0].membership_revision, '1');
  });
  await t.test('raw tenant aliases fail closed before transfer and preserve the staged payload', async () => {
    for (const field of ['tenantId','tenant_id','clientId','client_id']) {
      const f=fixture(); await f.stage('q',[{placeId:'p',[field]:'other'}]);
      await assert.rejects(f.store.transferQuery('q'), /tenant binding/);
      assert.equal((await f.work()).length,0);
      assert.equal((await db.query('SELECT state FROM scout_query_checkpoints WHERE tenant_id=$1',[f.tenantId])).rows[0].state,'fetched');
    }
  });
  await t.test('claim inputs are snapshotted before asynchronous database work', async () => {
    const f=await createClaim(); const refs=structuredClone(f.claims[0].refs);
    const finishing=f.store.finish(refs,outcome('handled','queued'));
    refs[0].queryKey='mutated'; refs[0].memberKey='0'.repeat(64); refs.length=0;
    assert.equal((await finishing).applied,true);
  });
  await t.test('material identity repair wakes blocked work; repeated query labels never wake unchanged work', async () => {
    const f=fixture(); await f.stage('q1',[{placeId:'p',website:'a.example'}]); await f.store.transferQuery('q1');
    const [claim]=await f.store.claimDue({dependencyKey:'dep-v1'});
    await f.store.finish(claim.refs,outcome('blocked','identity_incomplete'));
    await f.stage('q2',[{placeId:'p',website:'a.example',discoveryQuery:'Different search',industry:'Different search'}]);
    await f.store.transferQuery('q2'); assert.deepEqual(await f.store.claimDue({dependencyKey:'dep-v1'}),[]);
    await f.stage('q3',[{placeId:'p',name:'Now observed',website:'a.example',address:'Manchester NH'}]);
    await f.store.transferQuery('q3'); const [repaired]=await f.store.claimDue({dependencyKey:'dep-v1'});
    assert.equal(repaired.refs.length,3); await f.store.finish(repaired.refs,outcome('handled','queued'));
    await f.stage('q4',[{placeId:'p',name:'Now observed',website:'a.example',address:'Manchester NH',discoveryQuery:'Again'}]);
    await f.store.transferQuery('q4'); assert.deepEqual(await f.store.claimDue({dependencyKey:'dep-v1'}),[]);
  });

  await t.test('late legacy acknowledgment cannot clear an already-transferred immutable query payload', async () => {
    const f=fixture();const raw=[{placeId:'p',name:'Preserve original'}];await f.stage('q',raw);await f.store.transferQuery('q');
    await db.query("UPDATE scout_query_checkpoints SET candidates='[]'::jsonb,state='handled',handled_at=now() WHERE tenant_id=$1",[f.tenantId]);
    assert.deepEqual((await db.query('SELECT candidates FROM scout_query_checkpoints WHERE tenant_id=$1',[f.tenantId])).rows[0].candidates,raw);
    assert.equal((await f.store.claimDue({dependencyKey:'dep-v1'}))[0].members[0].name,'Preserve original');
  });

  await t.test('overlapping old-binary acknowledgment waits for transfer and still retains raw payload', async () => {
    const f=fixture();const raw=[{placeId:'p',name:'Concurrent immutable'}];await f.stage('q',raw);
    let release;const gate=new Promise(resolve=>{release=resolve;});let reached;
    const ready=new Promise(resolve=>{reached=resolve;});
    const wrapped={query:(...args)=>db.query(...args),connect:async()=>{
      const client=await db.connect();return{query:async(...args)=>{
        if(/UPDATE scout_query_checkpoints/.test(args[0])){reached();await gate;}
        return client.query(...args);
      },release:broken=>client.release(broken)};
    }};
    const transfer=createCandidateTicketStore({...f,pool:wrapped}).transferQuery('q');await ready;
    const oldAck=db.query("UPDATE scout_query_checkpoints SET candidates='[]'::jsonb,state='handled',handled_at=now() WHERE tenant_id=$1",[f.tenantId]);
    await new Promise(resolve=>setTimeout(resolve,20));release();await Promise.all([transfer,oldAck]);
    assert.deepEqual((await db.query('SELECT candidates FROM scout_query_checkpoints WHERE tenant_id=$1',[f.tenantId])).rows[0].candidates,raw);
  });

});
