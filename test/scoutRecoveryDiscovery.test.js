'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { toDiscoveredCompany } = require('../packages/max/scoutAcquisition/DiscoveryAdapters');
const { executeCoveragePlan } = require('../packages/scout/coverage/DiscoveryCoverageEngine');
const { evaluateReplenishmentAdmission } = require('../utils/replenishmentVertical');
const { findOrCreateCompanyForClient } = require('../scripts/promoteUnenriched');
test('Places business evidence survives mapping; query geography is never evidence', () => {
 const search={tenantId:'10',segments:['short_term_rental'],geography:{label:'Manchester, NH'}};
 const row=toDiscoveredCompany({name:'Granite Property Services',website:'granite.example',address:'Manchester, NH',placeTypes:['real_estate_agency']},search,'google_places');
 assert.deepEqual(row.placeTypes,['real_estate_agency']);
 assert.equal(evaluateReplenishmentAdmission(row,{missionSegment:'short_term_rental',allowedCities:['Manchester']}).admitted,true);
 const unknown=toDiscoveredCompany({name:'Granite',website:'granite.example'},search,'google_places');
 assert.equal(unknown.location,null); assert.equal(unknown.industry,null);
});
test('coverage requests property management strategies and reuses identical Places calls per cycle',async()=>{
 const requests=[];
 const adapter={id:'public_business_places',sourceType:'public_business_data',discover:async d=>{requests.push(d.evidenceRequest);return {candidates:[]};}};
 const plan={workloads:['STR','Vacation Rental','Property Manager'].map(concept=>({city:'Manchester NH',concept,source:'public_business_data'})),cities:['Manchester NH'],concepts:['STR','Vacation Rental','Property Manager'],sources:['public_business_data']};
 const def={tenantId:'10',segments:['short_term_rental'],geography:{cities:['Manchester'],state:'NH',label:'Manchester NH'}};
 await executeCoveragePlan(plan,def,[adapter]);
 assert.deepEqual(requests.map(r=>r.segment),['short_term_rental','property_management']);
 await executeCoveragePlan(plan,def,[adapter]); assert.equal(requests.length,4);
});
test('company promotion resolves same-domain alias and fails closed on ambiguous identity',async()=>{
 let inserted=false;
 const db={query:async(sql,params)=>{
  if(/SELECT id, domain/.test(sql)){assert.equal(params[2],'granite.example');return {rows:[{id:'company-1',domain:'granite.example'}]};}
  if(/INSERT INTO companies/.test(sql))inserted=true;
  return {rows:[]};
 }};
 assert.equal(await findOrCreateCompanyForClient({name:'Granite Alias',domain:'granite.example',clientId:10},db),'company-1');
 assert.equal(inserted,false);
 db.query=async sql=>({rows:/SELECT id, domain/.test(sql)?[{id:'1'},{id:'2'}]:[]});
 await assert.rejects(findOrCreateCompanyForClient({name:'Granite',domain:'granite.example',clientId:10},db),/ambiguous_canonical_company/);
});
test('official website supplies business fit with source evidence; foreign redirects cannot',async()=>{
 const { acquireBusinessEvidence }=require('../services/scoutWebsiteBusinessEvidence');
 const candidate={name:'Blue Door Living',website:'https://bluedoor.example',location:'Manchester, NH'};
 const context={missionSegment:'short_term_rental',allowedCities:['Manchester']};
 const html='<html><body><h1>Blue Door Living</h1>We provide residential property management in Manchester.</body></html>';
 const result=await acquireBusinessEvidence(candidate,context,{fetchPage:async url=>({ok:true,text:html,url})});
 assert.equal(result.admission.vertical,'property_manager');
 assert.match(result.evidence.quote,/property management/);
 assert.equal(result.evidence.source_url,'https://bluedoor.example/');
 assert.equal(await acquireBusinessEvidence(candidate,context,{fetchPage:async url=>({ok:true,url,text:'<html><body><h1>Insurance for property management</h1>We provide insurance to property management companies.</body></html>'})}),null);
 assert.equal(await acquireBusinessEvidence(candidate,context,{fetchPage:async()=>({ok:true,text:html,url:'https://unrelated.example/'})}),null);
});
test('website crawler falls back to www without fetching successful paths twice',async()=>{
 const {crawlWebsite}=require('../utils/websiteEnrichmentCrawl');
 const result=await crawlWebsite('example.com',async url=>({ok:url.includes('www.'),text:'<html><body>Contact team@example.com</body></html>',url}),{maxSuccessfulPages:1,maxRequests:4});
 assert.equal(result.pages[0].url,'https://www.example.com/');
 assert.equal(result.fetchCounts.size,2);
});
test('Hunter resolves observed role addresses when personal contacts are unavailable',async()=>{
 const axios=require('axios'); const original=axios.get; const key=process.env.HUNTER_API_KEY;
 process.env.HUNTER_API_KEY='test-key';
 axios.get=async(url,opts)=>{assert.equal(opts.params.type,undefined);return {data:{data:{emails:[{value:'info@business.example',type:'generic',sources:[{uri:'https://business.example/contact'}]}]}}};};
 try {const contact=await require('../leadgen').enrichWithHunter('business.example');
 assert.equal(contact.email,'info@business.example');assert.equal(contact.sourceUrl,'https://business.example/contact');
 } finally {axios.get=original;if(key===undefined)delete process.env.HUNTER_API_KEY;else process.env.HUNTER_API_KEY=key;}
});
test('explicit approved subsegments admit real estate offices without widening legacy STR scope',()=>{
 const { sourceScope, _test: { scoutInput } }=require('../services/maxOutboundControlLoop');
 const candidate={name:'Granite Realty',website:'https://granite.example',location:'Manchester, NH',placeTypes:['real_estate_agency']};
 const legacy={missionSegment:'short_term_rental',allowedCities:['Manchester']};
 assert.equal(evaluateReplenishmentAdmission(candidate,legacy).reason,'segment_mismatch');
 const approved={...legacy,missionSegments:['property_manager','realtor','commercial_office']};
 assert.equal(evaluateReplenishmentAdmission(candidate,approved).vertical,'realtor');
 assert.equal(evaluateReplenishmentAdmission({...candidate,location:'Concord, NH'},approved).reason,'outside_geography');
 assert.equal(evaluateReplenishmentAdmission({...candidate,placeTypes:[],businessType:'restaurant'},approved).admitted,false);
 const mission={tenantId:'10',structuredMission:{market:{segment:'commercial_cleaning_buyers',eligibleSubsegments:['property_manager','realtor','commercial_office']},geography:{region:'Greater Manchester',cities:['Manchester']}}};
 assert.deepEqual(sourceScope({mission}).eligibleSubsegments,approved.missionSegments);
 assert.equal(require('../services/maxOutboundControlLoop').missionCandidateReason({vertical:'realtor',service_area_match:'Manchester',company_location:'Manchester, NH'},sourceScope({mission})),null);
 const input=scoutInput({tenant_id:'10'},{mission},{deficit:5},{tenantId:'10'});
 assert.deepEqual(input.targetContext.segments,approved.missionSegments);
});
