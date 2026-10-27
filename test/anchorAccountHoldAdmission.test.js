'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const hold = require('../utils/anchorOutreachReview');
const { computeSetterVisible } = require('../utils/setterVisibility');

const target = { id:'10000000-0000-0000-0000-000000000001', client_id:10,
  company_id:'20000000-0000-0000-0000-000000000001', ao_outreach_review_required:false,
  do_not_contact:false,is_synthetic:false,ao_call_suppressed:false };
const sibling = { ...target,id:'10000000-0000-0000-0000-000000000002' };
function providerGuard() {
  const mod={exports:{}};
  vm.runInNewContext(fs.readFileSync(path.join(__dirname,'../utils/callEligibility.js'),'utf8'),{
    module:mod,exports:mod.exports,require(id){
      if(id==='./anchorOutreachReview')return hold;
      if(id==='../services/anchorEmailRecovery')return {
        loadRecoveryState:async()=>({mode:'legacy'}),assertRecoveryLeaf:async()=>({legacy:true})};
      throw new Error(`Unexpected provider dependency: ${id}`);
    }});
  return mod.exports;
}
function poolWithPeer(peer) {
  const trace=[];
  const client={async query(q,values){
    const sql=typeof q==='string'?q:q.text;values=typeof q==='string'?values:q.values;
    trace.push({sql,values});
    if(sql.startsWith('SELECT do_not_contact'))return {rows:[target]};
    if(sql.startsWith('SELECT id, client_id, company_id')&&sql.includes('WHERE id=$1'))return {rows:[target]};
    if(sql.includes('FROM companies'))return {rows:[{id:target.company_id,client_id:10}]};
    if(sql.includes('FROM prospects p WHERE company_id=$1'))return {rows:[target,peer]};
    if(/^(BEGIN|COMMIT|ROLLBACK|SET LOCAL|SELECT set_config|SELECT pg_advisory)/.test(sql))return {rows:[]};
    throw new Error(`Unexpected admission query: ${sql}`);
  },release(discard){trace.push({release:discard});}};
  return {trace,connect:async()=>client};
}

for(const flag of [true,null,undefined])test(`a sibling becoming ${String(flag)} before the final locked read blocks every provider`,async()=>{
  for(const name of ['withEmailAuthorization','withSmsAuthorization','withCallAuthorization']){
    const pool=poolWithPeer({...sibling,ao_outreach_review_required:flag});let sends=0;
    await assert.rejects(providerGuard()[name](pool,[{prospectId:target.id,clientId:10}],async()=>{sends++;}),{code:'outreach_review_required'});
    assert.equal(sends,0,name);
    assert.ok(pool.trace.some(x=>x.sql==='BEGIN'));
    assert.ok(pool.trace.some(x=>/FROM companies.*FOR UPDATE/.test(x.sql)));
    assert.ok(pool.trace.some(x=>/company_id=\$1.*ORDER BY id FOR SHARE/.test(x.sql)));
    assert.ok(pool.trace.some(x=>x.sql==='ROLLBACK'));
  }
});

test('current reviewed siblings permit one handoff while locks remain held through it',async()=>{
  for(const name of ['withEmailAuthorization','withSmsAuthorization','withCallAuthorization']){
    const pool=poolWithPeer(sibling);let sends=0;
    await providerGuard()[name](pool,[{prospectId:target.id,clientId:10}],async()=>{
      sends++;pool.trace.push({provider:true});
      assert.ok(!pool.trace.some(x=>x.sql==='COMMIT'));
    });
    assert.equal(sends,1,name);
    assert.ok(pool.trace.findIndex(x=>x.sql==='COMMIT')>pool.trace.findIndex(x=>x.provider));
  }
});

test('setter manual and engagement overrides cannot surface held or unknown Anchor accounts',()=>{
  for(const reason of ['scout','handoff','recompute','manual','engagement','stage_change']){
    const row={...target,icp_score:99,service_area_match:'Manchester, NH',setter_visible:true,status:'warm'};
    assert.equal(computeSetterVisible(row,{reason}),true);
    for(const flag of [true,null,undefined])assert.equal(computeSetterVisible({...row,ao_outreach_review_required:flag},{reason}),false);
    assert.equal(computeSetterVisible({...row,client_id:13,ao_outreach_review_required:null},{reason}),true,'non-Anchor policy is unchanged');
  }
});

test('missing parent identity or a failed sibling read prevents warm handoff',async()=>{
  for(const failure of ['company','siblings']){
    const pool=poolWithPeer(sibling);const client=await pool.connect();const query=client.query.bind(client);
    client.query=async(q,v)=>{
      const sql=typeof q==='string'?q:q.text;
      if(failure==='company'&&sql.includes('FROM companies'))return {rows:[]};
      if(failure==='siblings'&&sql.includes('company_id=$1'))throw new Error('synthetic sibling read failed');
      return query(q,v);
    };
    let actions=0;
    await assert.rejects(hold.withAnchorOutreachReview(pool,target.id,10,async()=>{actions++;}));
    assert.equal(actions,0);
    assert.ok(pool.trace.some(x=>x.sql==='ROLLBACK'));
  }
});

test('a spreadsheet task cannot activate a held or unknown Anchor account',async()=>{
  const {PostgresSpreadsheetProposalStore}=require('../packages/max/stateIngestion/spreadsheetProposalStore');
  for(const flag of [true,null,undefined]){
    let writes=0;const row={...target,ao_outreach_review_required:flag,assigned_ao_id:24};
    const db={async query(sql){
      if(sql.startsWith('SELECT * FROM prospects')||sql.startsWith('SELECT id, client_id'))return {rows:[row]};
      writes++;throw new Error('Held spreadsheet task reached a write');
    }};
    const store=new PostgresSpreadsheetProposalStore(db,{clientId:10,aoId:24});
    await assert.rejects(store.applyOperation(db,{type:'ADD_TASK',target:{accountId:target.id},after:{description:'Call the facilities manager',kind:'call'}}),{code:'outreach_review_required'});
    assert.equal(writes,0);
  }
});

test('scheduling a callback cannot bypass missing or unknown review state',async()=>{
  for(const flag of [true,null,undefined]){
    let writes=0;const row={...target,ao_outreach_review_required:flag};
    const client={async query(sql){
      if(/SELECT \* FROM prospects/.test(sql)||sql.startsWith('SELECT id, client_id'))return {rows:[row]};
      if(/^(BEGIN|COMMIT|ROLLBACK)/.test(sql))return {rows:[]};
      writes++;throw new Error('Held callback reached a write');
    },release(){}};
    const pool={connect:async()=>client};const mod={exports:{}};
    vm.runInNewContext(fs.readFileSync(path.join(__dirname,'../services/lifecycleService.js'),'utf8'),{
      module:mod,exports:mod.exports,require(id){
        if(id==='../db')return pool;
        if(id==='../utils/anchorOutreachReview')return hold;
        if(id==='../utils/callEligibility')return require('../utils/callEligibility');
        if(id==='../utils/lifecycleSchema')return {ensureLifecycleSchema:async()=>{}};
        throw new Error(`Unexpected callback dependency: ${id}`);
      }});
    await assert.rejects(mod.exports.scheduleProspectCallback({pool,clientId:10,prospectId:target.id,callbackAt:new Date('2026-11-01T14:00:00Z'),source:'offline-review-test'}));
    assert.equal(writes,0);
  }
});
