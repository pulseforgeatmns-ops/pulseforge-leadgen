'use strict';
const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
const {createOperatorWorker}=require('../../../services/signalOperator/worker');
const event={occurredAt:'2026-10-14T12:00:00Z',externalMessageId:'synthetic-event'};
const gate=()=>({ok:true,startAt:'2026-10-14T00:00:00Z'});
const feed={feedUrl:'http://caller.railway.internal/operator-events',token:'synthetic-auth-'.repeat(3),channelId:'123',gate,
  fetchImpl:async()=>({ok:true,json:async()=>({events:[event]})})};

test('eligible operator event is registered once after enqueue without deferral',async()=>{
  const calls=[];
  const worker=createOperatorWorker({...feed,store:{enqueue:async e=>calls.push(['enqueue',e])},
    onEvent:async e=>{calls.push(['register',e]);return {ok:true};}});
  assert.deepEqual(await worker.ingestOnce(),{enqueued:1});
  assert.deepEqual(calls,[['enqueue',event],['register',event]]);
});
test('omitted optional distribution callback does not report false deferral',async()=>{
  const worker=createOperatorWorker({...feed,store:{enqueue:async()=>{}}});
  assert.deepEqual(await worker.ingestOnce(),{enqueued:1});
});
test('real callback deferral remains visible without dropping the operator event',async()=>{
  let registered=0,enqueued=0;
  const worker=createOperatorWorker({...feed,store:{enqueue:async()=>enqueued++},
    onEvent:async()=>{registered++;return {skipped:true};}});
  assert.deepEqual(await worker.ingestOnce(),{enqueued:1,distributionDeferred:1});
  assert.equal(registered,1);assert.equal(enqueued,1);
});
test('closed gate and pre-start events cannot reach distribution registration',async()=>{
  let registered=0,enqueued=0;
  const options={...feed,store:{enqueue:async()=>enqueued++},onEvent:async()=>registered++};
  assert.deepEqual(await createOperatorWorker({...options,gate:()=>({ok:false})}).ingestOnce(),{skipped:true});
  assert.deepEqual(await createOperatorWorker({...options,gate:()=>({ok:true,startAt:'2026-10-15T00:00:00Z'})}).ingestOnce(),{enqueued:0});
  assert.equal(registered,0);assert.equal(enqueued,0);
});

function bootFixture({enabled=true,distributionEnabled=true}={}) {
  const calls=[],module={exports:{}};
  const source=fs.readFileSync(path.join(__dirname,'../../../services/signalOperator/index.js'),'utf8');
  const distribution=distributionEnabled?{register:async e=>{calls.push(['register',e]);return {ok:true};},stop:()=>calls.push(['stopDistribution'])}:null;
  const mocks={
    'node:fs':{readFileSync:()=>''},'node:path':path,
    './operationGate':{operationFromEnv:()=>gate},
    './store':{OperatorStore:class {async enqueue(e){calls.push(['enqueue',e]);}}},
    './worker':{createOperatorWorker:options=>createOperatorWorker({...options,fetchImpl:feed.fetchImpl}),
      startOperatorTimers:()=>{calls.push(['startOperator']);return ()=>calls.push(['stopOperator']);}},
    './relay':{createBrevoRelay:options=>{assert.equal(options.enabled,false);assert.equal(options.consented,false);return {}; }},
    '../../packages/signal-v1/providers/GeckoTerminalMarketDataProvider':{GeckoTerminalMarketDataProvider:class {}},
    '../../db':{query:async()=>{}},
    '../../packages/signal-v1/storage/ensureSignalSchema':{ensureSignalSchema:async()=>{}},
    '../signalDistribution':{startDistribution:async()=>distribution},
  };
  vm.runInNewContext(source,{module,exports:module.exports,__dirname:path.join(__dirname,'../../../services/signalOperator'),
    process:{env:{SIGNAL_OPERATOR_ENABLED:enabled?'1':'0',SIGNAL_REQUIRED_CALLER_CHANNEL_ID:'123',SIGNAL_OPERATOR_FEED_TOKEN:feed.token,SIGNAL_OPERATOR_FEED_URL:feed.feedUrl}},
    console:{error:()=>assert.fail('unexpected boot failure')},
    require:name=>{assert.ok(Object.hasOwn(mocks,name),`unexpected dependency ${name}`);return mocks[name];}});
  return {start:module.exports.startSignalOperator,calls};
}
test('operator bootstrap connects distribution registration and stops both runtimes',async()=>{
  const fixture=bootFixture(),runtime=await fixture.start();
  assert.ok(runtime);
  assert.deepEqual(await runtime.worker.ingestOnce(),{enqueued:1});
  runtime.stop();
  assert.deepEqual(fixture.calls,[['startOperator'],['enqueue',event],['register',event],['stopOperator'],['stopDistribution']]);
});
test('disabled distribution keeps operator ingestion and shutdown available',async()=>{
  const fixture=bootFixture({distributionEnabled:false}),runtime=await fixture.start();
  assert.deepEqual(await runtime.worker.ingestOnce(),{enqueued:1});runtime.stop();
  assert.deepEqual(fixture.calls,[['startOperator'],['enqueue',event],['stopOperator']]);
});
test('default-off operator never starts either runtime',async()=>{
  const fixture=bootFixture({enabled:false});
  assert.equal(await fixture.start(),null);assert.deepEqual(fixture.calls,[]);
});
