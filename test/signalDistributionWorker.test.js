'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const {createDistributionWorker}=require('../services/signalDistribution/worker');
const {validateConfiguration,startDistribution}=require('../services/signalDistribution');
test('disabled startup never touches database, provider or scheduler',async()=>{
  let calls=0;
  assert.equal(await startDistribution({env:{},pool:{query(){calls++;throw Error();}}}),null);
  assert.equal(calls,0);
});
test('activation requires explicitly scoped approved shared rate budget; safe one-token default',()=>{
  const env={SIGNAL_DISTRIBUTION_WINDOW_START:'2026-10-15T00:00:00Z',SIGNAL_DISTRIBUTION_WINDOW_END:'2026-10-16T00:00:00Z',SIGNAL_DISTRIBUTION_ENABLED:'1',SIGNAL_OPERATOR_ENABLED:'1',SIGNAL_DISTRIBUTION_TENANT_ID:'1',SIGNAL_DISTRIBUTION_SHARED_BUDGET_APPROVED:'1'};
  assert.equal(validateConfiguration(env).maxActive,1);
  for(const key of ['SIGNAL_OPERATOR_ENABLED','SIGNAL_DISTRIBUTION_TENANT_ID','SIGNAL_DISTRIBUTION_SHARED_BUDGET_APPROVED'])assert.throws(()=>validateConfiguration({...env,[key]:undefined}));
});
test('poll only observes pinned token with no ENTRY or position gate',async()=>{
  const row={id:'m',token_address:'CA',state:{pairId:'POOL'}};
  let got,observed;
  const worker=createDistributionWorker({store:{claim:async()=>row,observe:async(r,s)=>{observed=[r,s];return {ok:true};}},gate:()=>({ok:true}),createProvider:()=>({getSnapshot:async input=>{got=input;return {tokenAddress:'CA'};}})});
  assert.deepEqual(await worker.pollOnce(),{ok:true});assert.deepEqual(got,{tokenAddress:'CA',poolAddress:'POOL'});assert.equal(observed[0],row);
});
test('overlapping polls coalesce and provider failure visibly records unavailable',async()=>{
  let resolve,claims=0,failed=0;
  const worker=createDistributionWorker({store:{claim:async()=>{claims++;return {state:{}};},failed:async()=>failed++},gate:()=>({ok:true}),createProvider:()=>({getSnapshot:()=>new Promise((_,reject)=>{resolve=reject;})})});
  const first=worker.pollOnce();await new Promise(r=>setImmediate(r));
  assert.deepEqual(await worker.pollOnce(),{skipped:true});resolve(Error('budget'));await first;assert.equal(claims,1);assert.equal(failed,1);
});
test('closed operation gate never claims',async()=>{
  const worker=createDistributionWorker({store:{claim:()=>{throw Error('unexpected');}},createProvider:()=>{throw Error('unexpected');}});
  assert.deepEqual(await worker.pollOnce(),{skipped:true});
});

test('distribution registration failure cannot block original CA enqueue or later events',async()=>{
  const {createOperatorWorker}=require('../services/signalOperator/worker');
  let enqueued=0;
  const worker=createOperatorWorker({store:{enqueue:async()=>{enqueued++;}},feedUrl:'http://caller.railway.internal/operator-events',token:'t'.repeat(32),channelId:'123',gate:()=>({ok:true,startAt:'2026-10-14T00:00:00Z'}),onEvent:async()=>{throw Error('schema');},fetchImpl:async()=>({ok:true,json:async()=>({events:[{occurredAt:'2026-10-14T12:00:00Z'},{occurredAt:'2026-10-14T12:01:00Z'}]})})});
  assert.deepEqual(await worker.ingestOnce(),{enqueued:2,distributionDeferred:2});assert.equal(enqueued,2);
});

test('bounded activation configuration requires explicit timezone window and capped SMS count',()=>{
 const {activationConfig}=require('../services/signalDistribution/activation');
 const env={SIGNAL_DISTRIBUTION_WINDOW_START:'2026-10-15T00:00:00Z',SIGNAL_DISTRIBUTION_WINDOW_END:'2026-10-16T00:00:00Z'};
 assert.equal(activationConfig(env,'1').smsLimit,5);
 for(const bad of [{SIGNAL_DISTRIBUTION_WINDOW_END:'2026-10-17T00:00:00Z'},{SIGNAL_DISTRIBUTION_SMS_MAX_MESSAGES:'21'},{SIGNAL_DISTRIBUTION_SMS_MAX_MESSAGES:'-1'},{SIGNAL_DISTRIBUTION_WINDOW_START:undefined},{SIGNAL_DISTRIBUTION_WINDOW_START:'2026-10-15T00:00:00'}])assert.throws(()=>activationConfig({...env,...bad},'1'));
 assert.equal(activationConfig({...env,SIGNAL_DISTRIBUTION_SMS_MAX_MESSAGES:'0'},'1').smsLimit,0);
});
