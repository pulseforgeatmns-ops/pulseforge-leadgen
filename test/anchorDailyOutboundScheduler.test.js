'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {startAnchorGovernedScheduler}=require('../services/anchorGovernedScheduler');
test('governed scheduler is opt-in; polls before dispatch, avoids overlap and continues after errors',async()=>{
 assert.equal(startAnchorGovernedScheduler({enabled:false}),null);
 let release;const calls=[];let callback;let count=0;
 const cron={poll:async()=>{calls.push('poll');if(count++===0)await new Promise(r=>release=r);},run:async()=>{calls.push('tick');return {sent:0};}};
 const scheduler=startAnchorGovernedScheduler({enabled:true,cron,logger:{log(){},error(){}},setInterval(fn,ms){callback=fn;assert.equal(ms,60000);return{};},clearInterval(){}});
 await callback();assert.deepEqual(calls,['poll']);release();await new Promise(r=>setImmediate(r));assert.deepEqual(calls,['poll','tick']);
 for(let i=0;i<5;i++)await callback();assert.equal(calls.filter(c=>c==='tick').length,2);
 cron.poll=async()=>{throw Error('poll failed');};await callback();assert.equal(calls.filter(c=>c==='tick').length,2);
 scheduler.stop();const before=calls.length;await callback();assert.equal(calls.length,before);
});
