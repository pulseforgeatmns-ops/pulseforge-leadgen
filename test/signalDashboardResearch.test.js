'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {readEmpiricalEvents}=require('../services/signalDashboard/researchBridge');
const {CA,at,later}=require('./helpers/signalDashboardFixtures');
test('empirical bridge keeps original knowledge time, imports strict engine results and never reads performance',async()=>{
 const queries=[];const pool={query:async(sql)=>{queries.push(sql);if(sql.includes('FROM signal_raw_caller_evidence'))return {rows:[{id:'raw',source_id:'telegram-front-runners',external_message_id:'msg',occurred_at:at,ingested_at:later(1000),created_at:later(2000),raw_text:`5x Add to positions ${CA}`,provenance:{dataClass:'EMPIRICAL'}}]};
 return {rows:[{id:'strict',token_address:CA,observation_type:'INDEPENDENT_CONVERGENCE',occurred_at:later(3000),created_at:later(4000),definition_version:'prospective',metadata:{strictProvenIndependent:true,independentClusterCount:2,clusterIds:['a','b']}},{id:'not-strict',token_address:CA,observation_type:'INDEPENDENT_CONVERGENCE',occurred_at:at,created_at:at,metadata:{independentClusterCount:2,clusterIds:['a','b']}}]};}};
 const events=await readEmpiricalEvents(pool,[CA],{now:new Date(later(10000))});assert.equal(events.find(x=>x.category==='PROMOTION_EVENT').knownAt,later(2000));assert.equal(events.filter(x=>x.category==='INDEPENDENT_CONVERGENCE').length,1);
 assert.equal(events.find(x=>x.category==='INDEPENDENT_CONVERGENCE').provenance.performanceBlinded,true);assert.doesNotMatch(queries.join(' '),/outcomes|performance|return_15m|precision/);assert.match(queries[1],/jsonb_array_length/);
});
