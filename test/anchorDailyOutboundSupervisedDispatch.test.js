'use strict';
const test=require('node:test');const assert=require('node:assert/strict');const {randomUUID}=require('node:crypto');const path=require('node:path');
const {dispatchStages,whenStagesSettled,startDurableClock}=require('../services/governedOutboundStageScheduler');
const workerPath=path.join(__dirname,'fixtures/durable-stage/supervisedWorker.js');
function fixture(){const seen=[],states=new Map();return{seen,leaseMs:3000,async acquire(tenantId,stage){seen.push(`claim:${tenantId}:${stage}`);if(states.get(`${tenantId}:${stage}`)==='uncertain')return{status:'uncertain'};return{status:'claimed',tenantId,stage,runId:randomUUID(),fence:1,release:async()=>{}};},async start(c){seen.push(`start:${c.tenantId}:${c.stage}`);},async renew(){},async quarantine(c){states.set(`${c.tenantId}:${c.stage}`,'uncertain');seen.push(`uncertain:${c.tenantId}:${c.stage}`);},async finish(c,status){seen.push(`${status}:${c.tenantId}:${c.stage}`);}};}
const options=store=>({store,pool:{options:{max:10}},maxRuntimeMs:1000,supervisorOptions:{workerPath,graceMs:20,persistenceMs:50},logger:{}});
test('actual dispatch queues healthy tenant behind two hangs, then progresses without replay',{timeout:5000},async()=>{
 const store=fixture(),opts=options(store);dispatchStages({...opts,tenantIds:['10','13','17'],stages:['poll']});await new Promise(r=>setTimeout(r,10));assert.equal(store.seen.filter(x=>x.startsWith('claim:')).length,2);
 const results=await whenStagesSettled();assert.equal(results.find(x=>x.tenantId==='17').status,'completed');assert.equal(store.seen.filter(x=>x==='start:10:poll').length,1);
 dispatchStages({...opts,tenantIds:['10'],stages:['poll']});const retry=await whenStagesSettled();assert.equal(retry[0].status,'uncertain');assert.equal(store.seen.filter(x=>x==='start:10:poll').length,1);
});
test('same tenant stages are never admitted concurrently',{timeout:5000},async()=>{
 const store=fixture();dispatchStages({...options(store),tenantIds:['17'],stages:['poll','control']});await whenStagesSettled();assert.ok(store.seen.indexOf('completed:17:poll')<store.seen.indexOf('claim:17:control'));
});
test('stopAndDrain drops queued work before claim and terminates active children',{timeout:5000},async()=>{
 const store=fixture(),opts=options(store);const clock=startDurableClock({...opts,autostart:false});dispatchStages({...opts,tenantIds:['10','13','17'],stages:['poll']});await new Promise(r=>setTimeout(r,20));await clock.stopAndDrain();assert.equal(store.seen.includes('claim:17:poll'),false);assert.equal(dispatchStages({...opts,tenantIds:['17'],stages:['poll']}).reason,'scheduler_shutdown');
});
