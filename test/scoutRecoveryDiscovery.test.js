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
 assert.equal(await acquireBusinessEvidence(candidate,context,{fetchPage:async()=>({ok:true,text:html,url:'https://unrelated.example/'})}),null);
});
test('website crawler falls back to www without fetching successful paths twice',async()=>{
 const {crawlWebsite}=require('../utils/websiteEnrichmentCrawl');
 const result=await crawlWebsite('example.com',async url=>({ok:url.includes('www.'),text:'<html><body>Contact team@example.com</body></html>',url}),{maxSuccessfulPages:1,maxRequests:4});
 assert.equal(result.pages[0].url,'https://www.example.com/');
 assert.equal(result.fetchCounts.size,2);
});
