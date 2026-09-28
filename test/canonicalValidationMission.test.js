'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { ensureValidationMission } = require('../scripts/lib/canonicalValidationMission');
const { normalizeArgs } = require('../scripts/babrunDailyOutbound');
const { parse } = require('../scripts/anchorDailyOutbound');
const input = {tenantId:'13',createdBy:'validator',resolvedObjective:{ready:true,objective:'Approved objective.',geography:{region:'United States',cities:[],scope:'nationwide'},market:'small_business',segmentLabel:'Small Business Owners'}};
function fixture(initial=[]) {
 const missions=[...initial], cancelled=[];
 return {missions,cancelled,deps:{listMissions:async()=>missions,
   inspectMission:async id=>({mission:missions.find(m=>m.id===id),contributions:[]}),
   cancelMission:async id=>{cancelled.push(id);missions.find(m=>m.id===id).planCancelled=true;},
   createMission:async m=>{missions.push(m);return m;}}};
}
test('validation reruns reuse one durable mission',async()=>{
 const f=fixture();const a=await ensureValidationMission(input,f.deps);const b=await ensureValidationMission(input,f.deps);
 assert.equal(a.id,b.id);assert.equal(f.missions.length,1);
});
test('only incomplete validation drafts are retired through canonical cancellation',async()=>{
 const f=fixture([{id:'stale',createdBy:'validator',resolvedObjective:{objective:input.resolvedObjective.objective,geography:{region:null}}},{id:'other',createdBy:'operator'}]);
 await ensureValidationMission(input,f.deps);assert.deepEqual(f.cancelled,['stale']);assert.equal(f.missions.filter(m=>m.createdBy==='validator'&&!m.planCancelled).length,1);
});
test('committed scope cannot be silently replaced',async()=>{
 const f=fixture([{id:'locked',createdBy:'validator',structuredMission:{immutable:true}}]);
 await assert.rejects(()=>ensureValidationMission(input,f.deps),{code:'validation_scope_changed'});assert.equal(f.cancelled.length,0);
});
test('ambiguous evidence creates no mission',async()=>{
 const f=fixture();await assert.rejects(()=>ensureValidationMission({...input,resolvedObjective:{ready:false}},f.deps),{code:'canonical_plan_ambiguous'});assert.equal(f.missions.length,0);
});
test('Babrun documented CLI and default status preserve command and tenant',()=>{
 assert.deepEqual(parse(normalizeArgs(['tick','--tenant-id=13','--confirm','bounded-babrun-execution'])),{command:'tick',options:{'tenant-id':'13',confirm:'bounded-babrun-execution'}});
 assert.deepEqual(parse(normalizeArgs([])),{command:'status',options:{'tenant-id':'13'}});
 assert.throws(()=>normalizeArgs(['tick','--tenant-id=10']));
 assert.throws(()=>normalizeArgs(['tick','--tenant-id','13','--tenant-id','10']));
});
