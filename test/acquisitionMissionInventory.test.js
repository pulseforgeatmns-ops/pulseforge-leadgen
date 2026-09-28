'use strict';
const test=require('node:test');const assert=require('node:assert/strict');
const {qualifyKnowledgeContact,discoverKnowledgeInventory}=require('../services/acquisitionMissionInventory');
const {mapScoutIntelligenceToDiscoveryPayload}=require('../packages/max/workspace/ScoutDiscoveryExecutor');
const {buildPerProspectVariants}=require('../packages/max/workspace/PaigeVariantsExecutor');
const mission={tenantId:'13',structuredMission:{market:{segment:'small_business_owners'},geography:{region:'United States',cities:[]}}};
function row(){return {id:'p',client_id:13,company_id:'c',company_name:'Business',email:'owner@business.example',email_verified:true,email_status:'valid',acquisition_metadata:{contactResolution:{finalState:'VERIFIED_FOUNDER_EMAIL',bestEmail:'owner@business.example'}},knowledge_id:'ak',knowledge_content:{company:'Business',contact:'Owner Name',icpFit:'Excellent',operatingEvidence:{ownerName:'Owner Name',ownerRole:'Founder',operatingBusiness:true,country:'United States',city:'Houston',sourceUrl:'https://example.com/about',observedAt:'2026-09-28T12:00:00Z',observation:'Founder runs an operating business with crews'}},approved_asset:{id:'asset',version:1,content:{subject:'Question',statement:'Hi Owner, would you be open to a short conversation?'}}};}
test('AK qualification requires attributable operating, geographic, approved asset and bound founder contact evidence',()=>{
 assert.equal(qualifyKnowledgeContact(row(),mission),null);
 for(const mutate of [r=>delete r.knowledge_content.operatingEvidence,r=>r.knowledge_content.operatingEvidence.country='Canada',r=>r.knowledge_content.contact='Other',r=>r.approved_asset=null,r=>r.knowledge_content.icpFit='Unknown',r=>r.acquisition_metadata.contactResolution.finalState='REVIEW_REQUIRED']){const r=row();mutate(r);assert.ok(qualifyKnowledgeContact(r,mission));}
 assert.equal(qualifyKnowledgeContact(row(),{...mission,structuredMission:{...mission.structuredMission,geography:{region:'United States',cities:['Atlanta']}}}),'knowledge_city_mismatch');
});
test('existing AK passes canonical Scout evidence handoff without inventing readiness or buying signals',async()=>{
 const pool={query:async sql=>({rows:sql.includes('SELECT p.*, p.id AS prospect_id')?[row()]:[]})};
 const discovery=await discoverKnowledgeInventory(mission,{pool});
 const payload=mapScoutIntelligenceToDiscoveryPayload(discovery,{missionObjective:'Acquire a qualified founder'});
 assert.equal(payload.qualifiedCount,1);assert.equal(payload.rankedProspects.length,1);assert.equal(payload.rankedProspects[0].id,'c');assert.equal(payload.rankedProspects[0].readinessState,require('../packages/max/scoutAcquisition/Types').READINESS_STATES.UNKNOWN);
 assert.equal(require('../packages/acquisition-mission/DecisionReadiness').evaluatePrioritizationReadiness(payload).sufficient,true);assert.ok(payload.evidence.length>=2);assert.equal(payload.buyingSignals.length,0);
});
test('Paige consumes only the exact prospect-bound approved asset',()=>{
 const input={mission,clientId:13,max:{priorities:[{companyId:'c',name:'Business'}]},approvedCopies:{c:row().approved_asset}};
 const copy=buildPerProspectVariants(input)[0];assert.equal(copy.subject,'Question');assert.equal(copy.body,row().approved_asset.content.statement);assert.equal(copy.attributableIntelligence.scoutPersonalization.acquisitionKnowledgeAssetId,'asset');
 assert.throws(()=>buildPerProspectVariants({...input,approvedCopies:{other:row().approved_asset}}),{code:'approved_copy_missing'});
});
test('the canonical Paige SEC entry loads approved tenant assets from runtime dependencies',async()=>{
 const pool={query:async()=>({rows:[row()]})};
 const result=await require('../packages/max/workspace/PaigeVariantsExecutor').runPaigeVariants({mission,missionPlan:mission.structuredMission,workspaceContext:{max:{priorities:[{companyId:'c',name:'Business'}]},scout:{}}},{pool});
 assert.equal(result.status,require('../packages/acquisition-mission').EXECUTION_STATUSES.SUCCESS);assert.equal(result.contributions.variants[0].subject,'Question');assert.equal(result.contributions.variants[0].body,row().approved_asset.content.statement);
});
