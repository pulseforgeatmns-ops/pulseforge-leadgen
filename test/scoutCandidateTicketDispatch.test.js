'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const dbPath=require.resolve('../db');
require.cache[dbPath]={id:dbPath,filename:dbPath,loaded:true,exports:{query:async()=>{throw Error('Shared database forbidden');}}};
const { claimedCompanies, createCandidateTicketDispatcher, candidateTicketDiagnostic, queryProgressionDiagnostic }=require('../services/scoutCandidateTicketDispatch');
const { mergeTicketRefs }=require('../utils/scoutTicketMembership');
function ref(memberKey,extra={}) { return {tenantId:'13',fingerprint:'f',planVersion:'p',ticketVersion:'t',queryKey:'q',payloadHash:'h',
  memberKey,candidateKey:'c',claimToken:'lease',membershipRevision:'2',...extra}; }
function claim(records) { return {members:records.map((raw,index)=>({...raw,_scoutTicketRefs:[ref(String(index))]}))}; }
test('one durable candidate remains one complete obligation despite mutable raw IDs',()=>{
  const result=claimedCompanies([claim([{place_id:'p',id:'a',name:'Alpha',website:'https://a.example',location:'Manchester NH'},
    {place_id:'p',id:'b',name:'Alias',website:'https://a.example',location:'Manchester NH'}])],'13');
  assert.equal(result.length,1); assert.equal(result[0]._scoutTicketRefs.length,2);
  assert.equal(result[0].classificationObservations.length,2);
});
test('contrary business observations remain paired with their own query provenance',()=>{
  const result=claimedCompanies([claim([{placeId:'p',name:'A',website:'https://a.example',description:'Cleaning operator',discoveryQuery:'Cleaning'},
    {placeId:'p',name:'A',website:'https://a.example',description:'Software for cleaning companies',discoveryQuery:'Software'}])],'13');
  assert.ok(result[0].classificationObservations.some(row=>row.description==='Software for cleaning companies'&&row.discoveryQuery==='Software'));
});
test('unclaimed observed records cannot borrow ticket authority and conflicting claims fail closed',()=>{
  const work=claim([{name:'A',website:'https://a.example'}]);
  const result=claimedCompanies([work],'13',[{name:'Foreign',website:'https://foreign.example',_scoutTicketRefs:[ref('different')]}]);
  assert.equal(result[0].name,'A');
  assert.throws(()=>mergeTicketRefs([ref('a')],[ref('a',{claimToken:'other'})]),/Conflicting/);
});
test('dispatch is allowlisted and performs no duplicate fallback attempt after an ambiguous failure',async()=>{
  assert.throws(()=>createCandidateTicketDispatcher({tenantId:'17'}),/not enabled/);
  let claims=0, persisting=0;
  const dispatcher=createCandidateTicketDispatcher({tenantId:'13',dependencyKey:'d',ticketStore:{claimDue:async()=>{claims++;return[];}},
    persist:async()=>{persisting++;throw Error('uncertain');}});
  await assert.rejects(dispatcher.drain(),/uncertain/); await dispatcher.drain();
  assert.equal(claims,1);assert.equal(persisting,1);
});
test('durable diagnostics expose only finite known counts and never raw provider data or tokens',()=>{
  const secret='secret-provider-token';
  const output={candidate:candidateTicketDiagnostic({status:'pending_candidate_work',total:1,byReason:{identity_incomplete:1,[secret]:3},raw:secret}),
    query:queryProgressionDiagnostic({status:'pending_query_work',continuation:{statuses:{finite_query_exhausted:2,[secret]:1},token:secret},raw:secret})};
  assert.ok(!JSON.stringify(output).includes(secret));
  assert.equal(output.query.continuation.statuses.finite_query_exhausted,2);
});
