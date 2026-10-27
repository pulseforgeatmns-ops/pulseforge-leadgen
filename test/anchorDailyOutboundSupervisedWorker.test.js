'use strict';
const test=require('node:test');const assert=require('node:assert/strict');const path=require('node:path');const {randomUUID}=require('node:crypto');
const {superviseStage}=require('../services/governedOutboundStageSupervisor');
const workerPath=path.join(__dirname,'fixtures/durable-stage/supervisedWorker.js');
function workerTerminated(pid,readStat=require('node:fs').readFileSync){
 try{return readStat(`/proc/${pid}/stat`,'utf8').split(') ')[1].startsWith('Z');}
 catch(error){if(error.code==='ENOENT'||error.code==='ESRCH')return true;throw error;}
}
test('orphan inspection recognizes both Linux process-disappearance errors without hiding other failures',()=>{
 for(const code of ['ENOENT','ESRCH'])assert.equal(workerTerminated(123,()=>{throw Object.assign(new Error('gone'),{code});}),true);
 assert.equal(workerTerminated(123,()=> '123 (worker) Z 1'),true);
 for(const state of ['R','S','D'])assert.equal(workerTerminated(123,()=>`123 (worker) ${state} 1`),false);
 assert.throws(()=>workerTerminated(123,()=>{throw Object.assign(new Error('denied'),{code:'EACCES'});}),{code:'EACCES'});
});
function args(tenantId,onUncertain=async()=>{}){return{tenantId,stage:'poll',runId:randomUUID(),fence:1,workerPath,deadlineMs:200,graceMs:20,persistenceMs:50,onUncertain};}
test('actual supervisor contains two hangs, confirms exit, then admits healthy tenant',{timeout:5000},async()=>{
 const q=[];const results=await Promise.allSettled([superviseStage(args('10',async()=>q.push('10'))),superviseStage(args('13',async()=>q.push('13')))]);
 assert.ok(results.every(x=>x.status==='rejected'&&x.reason.code==='worker_outcome_uncertain'));assert.deepEqual(q.sort(),['10','13']);
 const result=await superviseStage(args('17'));assert.equal(result.outcome.status,'completed');
});
test('hung quarantine persistence is reported unverified after child exit without starving healthy tenant',{timeout:5000},async()=>{
 await assert.rejects(superviseStage(args('10',()=>new Promise(()=>{}))),{code:'worker_quarantine_unverified'});
 assert.equal((await superviseStage(args('17'))).outcome.status,'completed');
});
test('actual default runStage wiring supervises handler and records canonical receipt',{timeout:5000},async()=>{
 const {runStage}=require('../services/governedOutboundStageScheduler');const writes=[];
 const claim={status:'claimed',tenantId:'17',stage:'poll',runId:randomUUID(),fence:1,release:async()=>{}};
 const store={leaseMs:3000,acquire:async()=>claim,start:async()=>writes.push('start'),renew:async()=>{},quarantine:async()=>writes.push('uncertain'),finish:async(c,status)=>writes.push(status)};
 const result=await runStage({store,pool:{},tenantId:'17',stage:'poll',maxRuntimeMs:1000,supervisorOptions:{workerPath,graceMs:20},logger:{}});
 assert.equal(result.status,'completed');assert.deepEqual(writes,['start','completed']);
});
test('Linux launcher kills orphan worker when its parent dies',{timeout:5000},async()=>{
 const {fork}=require('node:child_process');const parent=fork(path.join(__dirname,'fixtures/durable-stage/orphanParent.js'),[],{stdio:['ignore','ignore','ignore','ipc']});
 let pid;try{pid=await new Promise((resolve,reject)=>{parent.once('message',m=>resolve(m.pid));parent.once('error',reject);});parent.kill('SIGKILL');await new Promise(resolve=>parent.once('exit',resolve));
 let terminated=false;for(let i=0;i<50;i++){if(workerTerminated(pid)){terminated=true;break;}await new Promise(resolve=>setTimeout(resolve,20));}assert.equal(terminated,true);
 }finally{parent.kill('SIGKILL');if(pid){try{process.kill(pid,'SIGKILL');}catch(_){}}}
});
test('actual supervisor rejects nonzero exit after receipt, stale fence, duplicate receipt',{timeout:5000},async()=>{
 for(const tenantId of ['1','10','13'])await assert.rejects(superviseStage({...args(tenantId),workerPath:path.join(__dirname,'fixtures/durable-stage/receiptWorker.js')}),{code:'worker_outcome_uncertain'});
});
test('malformed IPC flood cannot extend first forced-termination deadline',{timeout:5000},async()=>{
 const start=Date.now();await assert.rejects(superviseStage({...args('17'),deadlineMs:2000,workerPath:path.join(__dirname,'fixtures/durable-stage/receiptWorker.js')}),{code:'worker_outcome_uncertain'});assert.ok(Date.now()-start<1500);
});
test('parent rejects missing stage proof and unexpected evidence fields',()=>{
 const {validateReceipt}=require('../services/governedOutboundStageSupervisor');
 assert.equal(validateReceipt({outcome:{status:'completed'},evidenceRefs:{}},'poll'),null);
 assert.equal(validateReceipt({outcome:{status:'held'},evidenceRefs:{secret:'not-permitted'}},'poll'),null);
 assert.equal(validateReceipt({outcome:{status:'completed'},evidenceRefs:{integrationIds:['ok']},proof:{verifiedPoll:true,integrationCount:1}},'poll').outcome.status,'completed');
});
test('shutdown stops admissions and waits for confirmed child termination',{timeout:5000},async()=>{
 const {shutdownStageWorkers}=require('../services/governedOutboundStageSupervisor');
 const pending=superviseStage({...args('10'),deadlineMs:3000});const observed=pending.catch(error=>error);
 await new Promise(resolve=>setTimeout(resolve,50));await shutdownStageWorkers();assert.equal((await observed).code,'worker_outcome_uncertain');
 await assert.rejects(superviseStage(args('17')),{code:'worker_supervisor_stopping',noWorkerStarted:true});
});
