'use strict';
const test=require('node:test');const assert=require('node:assert/strict');const {boundedStorePool}=require('../services/governedOutboundStageStore');
test('late database acquisition is destroyed without issuing SQL',async()=>{
 let resolve,queries=0,destroyed=0;
 const storePool=boundedStorePool({connect:()=>new Promise(r=>{resolve=r;})},{connectMs:10});
 await assert.rejects(storePool.connect(),/connect_timeout/);resolve({release:destroy=>{if(destroy)destroyed++;},query:()=>queries++});await new Promise(r=>setImmediate(r));assert.equal(destroyed,1);assert.equal(queries,0);
});
test('parent storage deadline destroys client and disallows late follow-on SQL',async()=>{
 let destroyed=0;const storePool=boundedStorePool({connect:async()=>({release:destroy=>{if(destroy)destroyed++;},query:async()=>({rows:[]})})},{operationMs:10});
 const client=await storePool.connect();await client.query('BEGIN');await new Promise(r=>setTimeout(r,20));assert.equal(destroyed,1);await assert.rejects(client.query('COMMIT'),/database_deadline/);client.release();assert.equal(destroyed,1);
});
