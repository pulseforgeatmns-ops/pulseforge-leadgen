'use strict';
const test=require('node:test');const assert=require('node:assert/strict');
const {runStage,classify,dispatchStages,whenStagesSettled}=require('../services/governedOutboundStageScheduler');
function fixture(){
 const calls=[];return {calls,leaseMs:60000,acquire:async(t,s)=>({status:'claimed',tenantId:t,stage:s,runId:'r',release:async()=>calls.push('release')}),start:async()=>calls.push('start'),renew:async()=>{},finish:async(c,s,r)=>calls.push([s,r])};
}
test('stage executes only after durable start and terminal receipt precedes release',async()=>{const s=fixture();const r=await runStage({store:s,tenantId:'10',stage:'tick',execute:async()=>{s.calls.push('execute');return {sent:0,completed:true,envelopeId:'env'};}});assert.equal(r.status,'completed');assert.deepEqual(s.calls,['start','execute',['completed',null],'release']);});
test('arbitrary handler failure quarantines instead of replaying provider effects',async()=>{const s=fixture();let calls=0;const r=await runStage({store:s,tenantId:'10',stage:'tick',logger:{},execute:async()=>{calls++;throw Error('after provider');}});assert.equal(calls,1);assert.equal(r.status,'uncertain');assert.deepEqual(s.calls[1],['uncertain','handler_outcome_unknown']);});
test('pre-execution failure never invokes handler and records bounded retry',async()=>{const s=fixture();s.start=async()=>{throw Error('start not confirmed')};let calls=0;const r=await runStage({store:s,tenantId:'10',stage:'control',logger:{},execute:async()=>{calls++}});assert.equal(calls,0);assert.equal(r.status,'retry');});
test('lease loss never turns a handler receipt into completed',async()=>{const s=fixture();let heartbeat;s.renew=async()=>{throw Error('lost')};const r=await runStage({store:s,tenantId:'13',stage:'poll',setTimer:fn=>{heartbeat=fn;return 1},clearTimer:()=>{},execute:async()=>{heartbeat();await new Promise(setImmediate);return {results:[]}}});assert.equal(r.status,'uncertain');assert.equal(s.calls.filter(Array.isArray).length,0);});
test('unavailable and policy-held receipts are not completed',()=>{assert.equal(classify(null).status,'uncertain');assert.equal(classify({halted:'no_program'}).status,'held');assert.equal(classify({results:[{success:false}]},'poll','10').status,'uncertain');assert.equal(classify({status:'failed'},'tick','10').status,'uncertain');});
test('a hanging tenant/stage does not stop other independent stages',async()=>{const s=fixture();let resolve;const gate=new Promise(r=>resolve=r);let second=false;dispatchStages({pool:{},store:s,tenantIds:['10','13'],stages:['control'],execute:async({tenantId})=>{if(tenantId==='10')await gate;else second=true;return {ok:true}}});await new Promise(setImmediate);assert.equal(second,true);resolve();await whenStagesSettled();});
test('invalid mixed tenant request starts no operations',()=>{const s=fixture();assert.throws(()=>dispatchStages({pool:{},store:s,tenantIds:['10','999'],stages:['poll']}));assert.equal(s.calls.length,0);});
test('unacknowledged start and terminal write is uncertain, not confirmed retry',async()=>{const s=fixture();s.start=async()=>{throw Error('commit acknowledgement lost')};s.finish=async()=>{throw Error('state unknown')};let called=false;const r=await runStage({store:s,tenantId:'10',stage:'tick',logger:{},execute:async()=>{called=true}});assert.equal(called,false);assert.equal(r.status,'uncertain');});
test('successful handler with lost terminal write is not completed',async()=>{const s=fixture();s.finish=async()=>{throw Error('db lost')};const r=await runStage({store:s,tenantId:'10',stage:'poll',logger:{},execute:async()=>({results:[]})});assert.equal(r.status,'uncertain');});

const goodPoll = tenantId => ({results:[{tenantId,integrationId:'fixture',success:true,skipped:false,fetched:0,inserted:0,duplicates:0,unmatched:0,replies:0}]});
test('only genuine same-tenant poll receipts refresh completion, including valid empty mailbox',()=>{
 assert.equal(classify(goodPoll('10'),'poll','10').status,'completed');
 for(const change of [{skipped:true,reason:'overlap'},{success:false,skipped:true,reason:'credential_unavailable'},{success:false,skipped:true,reason:'not_pollable'},{tenantId:'13'},{fetched:undefined}]) {
  const result=goodPoll('10'); Object.assign(result.results[0],change);assert.notEqual(classify(result,'poll','10').status,'completed');
 }
 for(const result of [{},{error:'x'},{results:[]},{status:'started'}]) assert.notEqual(classify(result,'poll','10').status,'completed');
});
test('tick and control require their own terminal evidence, not dispatch acknowledgement',()=>{
 assert.equal(classify({sent:1,envelopeId:'e',itemId:'i'},'tick','10').status,'completed');
 assert.equal(classify({mode:'shadow',sent:0,envelopeId:'e'},'tick','10').status,'held');
 assert.notEqual(classify({sent:0},'tick','10').status,'completed');
 assert.notEqual(classify({bounded:true,tenants:{10:{status:'started'}}},'control','10').status,'completed');
 assert.equal(classify({cycleId:'c',programId:'p',plan:{},cycleStartedAt:'2026-10-21T00:00:00Z',cycleCompletedAt:'2026-10-21T00:00:01Z'},'control','10').status,'completed');
});
test('bounded admission preserves pool headroom and deduplicates pending stage requests',async()=>{const s=fixture();const pool={options:{max:4}};let release;let running=0,peak=0;const gate=new Promise(r=>release=r);const execute=async()=>{running++;peak=Math.max(peak,running);await gate;running--;return {halted:'fixture_hold'}};dispatchStages({pool,store:s,tenantIds:['10','13'],stages:['control'],execute});const again=dispatchStages({pool,store:s,tenantIds:['10','13'],stages:['control'],execute});assert.ok(again.requests.every(r=>r.status==='already_requested'));await new Promise(setImmediate);assert.equal(peak,1);release();await whenStagesSettled();assert.equal(peak,1);});
test('undersized production pool fails before claiming work',()=>{const s=fixture();assert.throws(()=>dispatchStages({pool:{options:{max:3}},store:s,tenantIds:['10'],stages:['poll']}),{code:'scheduler_pool_capacity'});assert.deepEqual(s.calls,[]);});
test('runtime deadline quarantines but never releases a still running side effect for replay',async()=>{const s=fixture();let heartbeat,resolve;let now=0;const gate=new Promise(r=>resolve=r);const pending=runStage({store:s,tenantId:'10',stage:'control',logger:{},maxRuntimeMs:1000,monotonicNow:()=>now,setTimer:fn=>{heartbeat=fn;return 1},clearTimer:()=>{},execute:()=>gate});await new Promise(setImmediate);now=1001;heartbeat();await new Promise(setImmediate);assert.ok(s.calls.some(c=>Array.isArray(c)&&c[0]==='uncertain'&&c[1]==='stage_runtime_exceeded'));assert.ok(!s.calls.includes('release'));resolve({halted:'late'});assert.equal((await pending).status,'uncertain');});

test('arbitrary failed receipts are quarantined rather than implicitly scheduled again',()=>{
 for(const result of [{error:'after_effect'},{success:false},{status:'failed'},{halted:'provider_acceptance_unknown'}]) assert.equal(classify(result,'tick','10').status,'uncertain');
});
test('characterization: two unresolved handlers exhaust bounded capacity until supervised recovery',async()=>{
 const s=fixture(),pool={options:{max:10}};let release;const gate=new Promise(r=>release=r);let healthy=false;
 dispatchStages({pool,store:s,tenantIds:['10','13','17'],stages:['control'],execute:async({tenantId})=>{if(tenantId!=='17') await gate;else healthy=true;return {halted:'no_program'}}});
 await new Promise(setImmediate);assert.equal(healthy,false); // UNRESOLVED liveness, not an acceptance pass.
 release();await whenStagesSettled();assert.equal(healthy,true);
});
