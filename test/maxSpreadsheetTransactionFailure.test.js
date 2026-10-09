'use strict';
const {test}=require('node:test');
const assert=require('node:assert/strict');
const {PostgresSpreadsheetProposalStore}=require('../packages/max/stateIngestion/spreadsheetProposalStore');
function failingConnection({onCommit=false,rollbackFails=false}={}) {
 const original=new Error(onCommit?'Lost COMMIT acknowledgment':'Original INSERT failed');
 let released;
 const connection={
  query:async query=>{
   const sql=query.text;
   if(sql==='COMMIT' && onCommit) throw original;
   if(sql==='ROLLBACK' && rollbackFails) throw new Error('Lost rollback connection');
   if(sql.startsWith('INSERT INTO max_spreadsheet_proposals')) {
    if(!onCommit) throw original;
    return {rows:[{id:'proposal',plan:{operations:[]}}]};
   }
   return {rows:[]};
  },
  release:discard=>{released=discard;},
 };
 const db={connect:async()=>connection};
 return {db,original,get released(){return released;}};
}
for(const situation of [{onCommit:true,rollbackFails:true},{onCommit:true,rollbackFails:false},{onCommit:false,rollbackFails:true},{onCommit:false,rollbackFails:false}]) {
 test(`transaction failure preserves original error, discards uncertain session ${JSON.stringify(situation)}`,async()=>{
  const fake=failingConnection(situation);
  const store=new PostgresSpreadsheetProposalStore(fake.db,{clientId:1,aoId:10,approverUserId:11});
  let observed;
  try {await store.createProposal({actorId:11,conversationId:'test',sourceHash:'a'.repeat(64),plan:{operations:[]},baseline:{users:[],prospects:[]}});} catch(error) {observed=error;}
  assert.equal(observed,fake.original);
  assert.equal(fake.released,situation.onCommit || situation.rollbackFails);
  assert.equal(observed.commitOutcomeUnknown===true,situation.onCommit);
  assert.equal(observed.rollbackFailed===true,situation.rollbackFails);
 });
}
for(const rollbackFails of [false,true]) {
 test(`commit acknowledgment failure preserves retry identity and discards connection (rollback failure ${rollbackFails})`,async()=>{
  const original=new Error('Connection lost after COMMIT');let released,requestDigest;
  const connection={query:async query=>{
   const sql=query.text,args=query.values;
   if(sql==='COMMIT') throw original;
   if(sql==='ROLLBACK' && rollbackFails) throw new Error('rollback connection lost');
   if(sql.startsWith('SELECT plan FROM')) return {rows:[{plan:{operations:[]}}]};
   if(sql.startsWith('SELECT id,client_id,role,active')) return {rows:[{id:11,client_id:1,role:'admin',active:true},{id:10,client_id:1,role:'ao',active:true}]};
   if(sql.startsWith('SELECT * FROM max_spreadsheet_proposals')) return {rows:[{status:'committed',digest:'reviewed',source_hash:'a'.repeat(64),receipt:{selectedOperationIds:['op']}}]};
   if(sql.startsWith('INSERT INTO max_spreadsheet_commit_requests')) requestDigest=args[2];
   if(sql.startsWith('SELECT request_digest')) return {rows:[{request_digest:requestDigest}]};
   return {rows:[]};
  },release:discard=>{released=discard;}};
  const store=new PostgresSpreadsheetProposalStore({connect:async()=>connection},{clientId:1,aoId:10,approverUserId:11});
  let observed;
  try {await store.commitProposal({proposalId:'proposal',actorId:11,conversationId:'test',sourceHash:'a'.repeat(64),selectedOperationIds:['op'],approvedBy:11,expectedDigest:'reviewed',idempotencyKey:'stable-key'});} catch(error) {observed=error;}
  assert.equal(observed,original);assert.equal(observed.commitOutcomeUnknown,true);assert.equal(released,true);assert.equal(observed.rollbackFailed===true,rollbackFails);
 });
}
test('no-response query and rollback have application deadlines and discard the session',async()=>{
 let discarded=false;const configs=[];
 const connection={query:config=>{configs.push(config);return new Promise(()=>{});},release:value=>{discarded=value;}};
 const store=new PostgresSpreadsheetProposalStore({connect:async()=>connection},{clientId:1,aoId:10,queryTimeoutMs:20,rollbackTimeoutMs:15,connectTimeoutMs:20});
 const start=Date.now();let error;
 try {await store.createProposal({actorId:11,conversationId:'test',sourceHash:'a'.repeat(64),plan:{operations:[]},baseline:{users:[],prospects:[]}});} catch(caught) {error=caught;}
 assert.equal(error.code,'SPREADSHEET_DB_TIMEOUT');assert.equal(error.rollbackFailed,true);assert.equal(discarded,true);assert.ok(Date.now()-start<1000);
 assert.equal(configs[0].query_timeout,20);assert.equal(configs[1].query_timeout,15);assert.equal(configs[1].text,'ROLLBACK');
});
test('no-response COMMIT remains unknown and retryable with connection discarded',async()=>{
 let discarded=false;const configs=[];
 const connection={query:config=>{
  configs.push(config);
  if(config.text==='COMMIT') return new Promise(()=>{});
  if(config.text.startsWith('INSERT INTO max_spreadsheet_proposals')) return Promise.resolve({rows:[{id:'proposal',plan:{operations:[]}}]});
  return Promise.resolve({rows:[]});
 },release:value=>{discarded=value;}};
 const store=new PostgresSpreadsheetProposalStore({connect:async()=>connection},{clientId:1,aoId:10,queryTimeoutMs:20,rollbackTimeoutMs:15,connectTimeoutMs:20});
 let error;try {await store.createProposal({actorId:11,conversationId:'test',sourceHash:'a'.repeat(64),plan:{operations:[]},baseline:{users:[],prospects:[]}});} catch(caught) {error=caught;}
 assert.equal(error.code,'SPREADSHEET_DB_TIMEOUT');assert.equal(error.commitOutcomeUnknown,true);assert.equal(discarded,true);
 assert.ok(configs.some(c=>c.text.includes("idle_in_transaction_session_timeout='20s'")));
});
test('late connection checkout is bounded and the late session is discarded',async()=>{
 let provide,discarded=false;
 const pool={connect:()=>new Promise(resolve=>{provide=resolve;})};
 const store=new PostgresSpreadsheetProposalStore(pool,{clientId:1,aoId:10,queryTimeoutMs:20,rollbackTimeoutMs:15,connectTimeoutMs:20});
 await assert.rejects(store.createProposal({actorId:11,conversationId:'test',sourceHash:'a'.repeat(64),plan:{operations:[]},baseline:{users:[],prospects:[]}}),/SPREADSHEET_DB_TIMEOUT/);
 provide({release:value=>{discarded=value;}});await new Promise(resolve=>setImmediate(resolve));assert.equal(discarded,true);
});
