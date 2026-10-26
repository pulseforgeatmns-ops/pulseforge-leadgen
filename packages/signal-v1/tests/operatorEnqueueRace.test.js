'use strict';

const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const {createTrackedPostgresPool,closePostgresFixture}=require('../../../test/helpers/closePostgresPool');
const {startDisposablePostgres}=require('../../../test/helpers/disposablePostgres');
const {ensureSignalSchema}=require('../storage/ensureSignalSchema');
const {OperatorAlertOutbox}=require('../operator/alertOutbox');
const {OperatorStore}=require('../../../services/signalOperator/store');
const {createOperatorWorker}=require('../../../services/signalOperator/worker');
const {mailPayload}=require('../../../services/signalOperator/relay');

// Pipeline-shaped fixtures run only against a new disposable database and fake
// transports. No production credentials, Telegram reads or outbound mail.
const token='2fRDA5f353VXLs2PeLJNqqHqTMhrjJunAXmWWpLkpump';
const t=n=>new Date(Date.UTC(2026,9,7,0,0,n));
function event(messageId) {
  return {sourceId:'telegram-front-runners',externalMessageId:`telegram:123:${messageId}`,
    extractedCa:token,occurredAt:t(1).toISOString(),ingestedAt:t(2).toISOString(),
    provenance:{dataClass:'EMPIRICAL',telegramChannelId:'123'}};
}
function researchInput(messageId) {
  return {evidence:{...event(messageId),id:`local-research-evidence-${messageId}`},
    researchState:'FIRST_CALLER',independentConvergence:'pending',
    timestamps:{pulseForgeReceivedAt:t(3),callPersistedAt:t(4),alertCreatedAt:t(5)}};
}
async function database(run) {
  const instance=await startDisposablePostgres('sig-outbox-race-',{socketPrefix:'sigr-'});
  const pool=createTrackedPostgresPool({connectionString:instance.connectionString});
  try {
    await ensureSignalSchema(pool);
    for(const name of ['2026-10-07-signal-v1-operator-outbox.sql','2026-10-07-signal-v1-operator-relay.sql']) {
      await pool.query(fs.readFileSync(path.join(__dirname,'../../../migrations',name),'utf8'));
    }
    await run(pool);
  } finally {await closePostgresFixture(pool,instance);}
}
async function rowFor(pool,messageId) {
  const rows=(await pool.query('SELECT * FROM signal_operator_alert_outbox WHERE external_message_id=$1',
    [`telegram:123:${messageId}`])).rows;
  assert.equal(rows.length,1,'one durable row per source/message/CA');
  return rows[0];
}

test('worker enrichment preserves alert evidence and freezes first-attempt text across retry',async()=>{
  const {buildOperatorAlert}=require('../operator/alertOutbox');
  let now=t(10);
  let payload=buildOperatorAlert({...researchInput(301),approvedChannelId:'123'});
  const original=structuredClone(payload),sent=[];
  const store={
    claim:async()=>({id:payload.id,payload,attempts:sent.length+1}),
    setPayload:async(_id,value)=>{payload=structuredClone(value);},
    failed:async()=>{},accepted:async()=>{},
  };
  const options={store,feedUrl:'http://feed.railway.internal:3099/operator-events',
    token:'OPERATIONAL-TEST-ONLY-AUTH-FIXTURE-000',channelId:'123',
    gate:()=>({ok:true,startAt:t(0).toISOString()}),now:()=>now,
    marketProvider:{getLiveTokenSnapshot:()=>new Promise(()=>{})},marketTimeoutMs:5,
    relay:{send:async alert=>{sent.push(mailPayload(alert));if(sent.length===1)throw new Error('relay_transport_unknown');return {receipt:'local-unit-receipt'};}}};
  await createOperatorWorker(options).sendOnce();
  now=t(20);
  await createOperatorWorker(options).sendOnce();
  assert.equal(sent.length,2);
  assert.deepEqual(sent[1],sent[0]);
  for(const key of ['evidenceId','researchState','independentConvergence','knowledgeAt','occurredAt','ingestedAt','createdAt']) {
    assert.equal(payload[key],original[key],key);
  }
  assert.equal(payload.latency.callPersistedAt,original.latency.callPersistedAt);
  assert.equal(payload.transportSentAt,t(10).toISOString());
});

test('shared outbox promotes research-first rows and deduplicates either source order and concurrent readers',async()=>{
  await database(async pool=>{
    const research=new OperatorAlertOutbox(pool,{channelId:'123'});
    const operational=new OperatorStore(pool,{channelId:'123',now:()=>t(10)});

    await research.enqueue(researchInput(101));
    const first=await rowFor(pool,101);
    assert.equal(first.operator_event_id,null);
    assert.equal(await operational.claim(),null,'research alone cannot authorize delivery');
    await operational.enqueue(event(101));
    let promoted=await rowFor(pool,101);
    assert.equal(promoted.operator_event_id,promoted.id);
    assert.equal(promoted.evidence_id,first.evidence_id);
    assert.deepEqual(promoted.payload,first.payload,'promotion preserves original empirical clocks and research context');
    const claimed=await operational.claim({since:t(0)});
    assert.equal(claimed.id,first.id,'research-first row is now sendable');
    await operational.enqueue(event(101));
    await research.enqueue(researchInput(101));
    promoted=await rowFor(pool,101);
    assert.equal(promoted.attempts,1);
    assert.equal(+promoted.lease_until,+claimed.lease_until);
    assert.equal(promoted.delivery_state,'SENDING');
    await operational.accepted(claimed.id,'local-accepted-101',claimed.attempts);
    await operational.recordReceipt(claimed.id,'RECEIVED','local-received-101');
    await Promise.all([research.enqueue(researchInput(101)),operational.enqueue(event(101))]);
    promoted=await rowFor(pool,101);
    assert.equal(promoted.delivery_state,'ACCEPTED');
    assert.equal(promoted.transport_receipt,'local-accepted-101');
    assert.ok(promoted.received_at);
    assert.equal(await operational.claim(),null,'duplicates never resurrect accepted alerts');

    await operational.enqueue(event(102));
    const operatorFirst=await rowFor(pool,102);
    await research.enqueue(researchInput(102));
    await operational.enqueue(event(102));
    assert.deepEqual((await rowFor(pool,102)).payload,operatorFirst.payload,
      'the winning durable payload remains stable in the reverse order too');
    const reverseClaim=await operational.claim();
    assert.equal(reverseClaim.id,operatorFirst.id);
    await operational.accepted(reverseClaim.id,'local-accepted-102',reverseClaim.attempts);

    for(let messageId=103;messageId<113;messageId++) {
      await Promise.all(Array.from({length:8},(_,index)=>index%2
        ? new OperatorStore(pool,{channelId:'123',now:()=>t(10)}).enqueue(event(messageId))
        : research.enqueue(researchInput(messageId))));
      const row=await rowFor(pool,messageId);
      assert.equal(row.operator_event_id,row.id);
      assert.equal(row.attempts,0);
      const [left,right]=await Promise.all([operational.claim(),
        new OperatorStore(pool,{channelId:'123',now:()=>t(10)}).claim()]);
      const claims=[left,right].filter(Boolean);
      assert.equal(claims.length,1,'concurrent senders obtain only one lease');
      assert.equal(claims[0].id,row.id);
      await operational.accepted(row.id,'local-concurrent-receipt',claims[0].attempts);
    }
    assert.equal((await pool.query('SELECT count(*) FROM signal_operator_events')).rows[0].count,'12');
    assert.equal((await pool.query('SELECT count(*) FROM signal_operator_alert_outbox')).rows[0].count,'12');
    for(const table of ['signal_raw_caller_evidence','signal_events','signal_research_observations','signal_research_cohorts']) {
      assert.equal((await pool.query(`SELECT count(*) FROM ${table}`)).rows[0].count,'0',table);
    }
  });
});

test('research-first delivery preserves knowledge and evidence; retry after restart has identical mail payload',async()=>{
  await database(async pool=>{
    let now=t(10);
    const research=new OperatorAlertOutbox(pool,{channelId:'123'});
    const store=new OperatorStore(pool,{channelId:'123',now:()=>now});
    await research.enqueue(researchInput(201));
    await store.enqueue(event(201));
    const original=(await rowFor(pool,201)).payload;
    const sent=[];
    const options={store,feedUrl:'http://feed.railway.internal:3099/operator-events',
      token:'OPERATIONAL-TEST-ONLY-AUTH-FIXTURE-000',channelId:'123',
      gate:()=>({ok:true,startAt:t(0).toISOString()}),now:()=>now,
      fetchImpl:async()=>{throw new Error('no feed reads in this test');},
      marketProvider:{getLiveTokenSnapshot:()=>new Promise(()=>{})},marketTimeoutMs:5,
      relay:{send:async alert=>{
        sent.push(mailPayload(alert));
        assert.equal(alert.evidenceId,original.evidenceId);
        assert.equal(alert.researchState,'FIRST_CALLER');
        assert.equal(alert.independentConvergence,'pending');
        assert.equal(alert.knowledgeAt,t(2).toISOString());
        assert.equal(alert.occurredAt,original.occurredAt);
        assert.equal(alert.ingestedAt,original.ingestedAt);
        assert.equal(alert.createdAt,original.createdAt);
        assert.equal(alert.latency.callPersistedAt,t(4).toISOString());
        assert.equal(alert.latency.pulseForgeReceivedAt,t(3).toISOString());
        if(sent.length===1)throw new Error('relay_transport_unknown');
        return {receipt:'local-retry-receipt'};
      }}};
    await createOperatorWorker(options).sendOnce();
    const pending=await rowFor(pool,201);
    assert.equal(pending.delivery_state,'PENDING');
    assert.equal(pending.payload.transportSentAt,t(10).toISOString());
    now=t(20);
    await Promise.all([store.enqueue(event(201)),research.enqueue(researchInput(201))]);
    assert.equal(+(await rowFor(pool,201)).next_attempt_at,+pending.next_attempt_at,
      'duplicate source reads must not reset retry timing');
    const restarted=new OperatorStore(pool,{channelId:'123',now:()=>now});
    await createOperatorWorker({...options,store:restarted,
      marketProvider:{getLiveTokenSnapshot:()=>{throw new Error('retry must use frozen quote');}}}).sendOnce();
    assert.equal(sent.length,2);
    assert.deepEqual(sent[1],sent[0],'all idempotent mail fields, including latency, are frozen');
    assert.equal((await rowFor(pool,201)).delivery_state,'ACCEPTED');
  });
});
