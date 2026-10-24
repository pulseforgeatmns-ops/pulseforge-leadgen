'use strict';
const test=require('node:test');const assert=require('node:assert/strict');const path=require('node:path');
const {createSupervisor}=require('../docs/reliability/prototypes/isolatedStageSupervisor');
test('LOCAL prototype: two hung isolated workers terminate before healthy tenant gains capacity',{timeout:5000},async()=>{
 const quarantines=[];const supervisor=createSupervisor({workerPath:path.join(__dirname,'fixtures/durable-stage/isolatedWorker.js'),capacity:2,deadlineMs:300,graceMs:30,onUncertain:async input=>quarantines.push(input.tenantId)});
 const [a,b,c]=await Promise.all([supervisor.run({tenantId:'10',behavior:'hang'}),supervisor.run({tenantId:'13',behavior:'hang'}),supervisor.run({tenantId:'17',behavior:'complete'})]);
 assert.equal(a.status,'uncertain');assert.equal(b.status,'uncertain');assert.equal(a.workerExited,true);assert.equal(b.workerExited,true);assert.deepEqual(quarantines.sort(),['10','13']);assert.equal(c.status,'worker_result');assert.equal(c.receipt.tenantId,'17');
});
test('LOCAL prototype: failed quarantine storage does not pretend durable success or block worker termination',{timeout:5000},async()=>{
 const supervisor=createSupervisor({workerPath:path.join(__dirname,'fixtures/durable-stage/isolatedWorker.js'),capacity:1,deadlineMs:300,graceMs:30,onUncertain:async()=>{throw Error('fixture database unavailable')}});
 const [a,b]=await Promise.all([supervisor.run({tenantId:'10',behavior:'hang'}),supervisor.run({tenantId:'13',behavior:'complete'})]);assert.equal(a.status,'uncertain');assert.equal(a.workerExited,true);assert.equal(b.status,'worker_result');
});

test('LOCAL prototype: IPC loss, process crash and wrong-run receipts remain uncertain',{timeout:5000},async()=>{
 const supervisor=createSupervisor({workerPath:path.join(__dirname,'fixtures/durable-stage/isolatedWorker.js'),capacity:1,deadlineMs:300,graceMs:30});
 for(const behavior of ['disconnect','crash','mismatch']) {const result=await supervisor.run({tenantId:'10',behavior});assert.equal(result.status,'uncertain');assert.equal(result.workerExited,true);}
});
