'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { governedContactReason, founderFirst } = require('../utils/governedContactEligibility');
const { hash, missionScope, policy } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const { validateGovernedSchedule } = require('../services/governedTenantSchedule');
const { createGovernedTenantMailboxSend } = require('../utils/governedOutboundTransport');
const { createProviderBoundaryTracker, markLeafProviderSend } = require('../services/governedOutboundProviderBoundary');
const { classifyPending } = require('../services/governedOutboundReplies');
const now = new Date('2026-09-28T15:00:00Z');
function contact(classification='VERIFIED_FOUNDER_EMAIL') {
  return { id:'p',prospect_id:'p',company_id:'c',client_id:13,email:'owner@business.example',email_verified:true,email_status:'valid',
    acquisition_metadata:{contactResolution:{finalState:classification,bestEmail:'owner@business.example'}} };
}
test('classification is bound to the verified address and reviewed role policy',()=>{
  assert.equal(governedContactReason(contact()),null);
  for(const cls of ['REVIEW_REQUIRED','UNRESOLVED',null]) assert.ok(governedContactReason(contact(cls)));
  assert.equal(governedContactReason(contact('VERIFIED_ROLE_EMAIL')),'contact_classification_not_authorized');
  assert.equal(governedContactReason(contact('VERIFIED_ROLE_EMAIL'),{allowedContactClassifications:['VERIFIED_ROLE_EMAIL']}),null);
  assert.equal(governedContactReason({...contact(),email:'other@business.example'}),'contact_classification_email_changed');
  assert.ok(founderFirst(contact(),contact('VERIFIED_ROLE_EMAIL'))<0);
  assert.equal(governedContactReason({...contact(),do_not_contact:true}),'do_not_contact');
});
function fixture(){
  const mission={id:'m',tenantId:'13',objective:'Approved goal',targetSegment:'owners',structuredMission:{immutable:true}};
  const p=policy({tenantId:'13',sourceMissionId:'m',senderEmail:'hello@business.example',inboxIntegrationId:'mb',sendingIdentityId:'identity',startsAt:now.toISOString(),expiresAt:'2026-09-30T15:00:00Z',dailyCap:1,totalCap:1,spacingMinutes:240,endHour:16},now);
  const program={id:'program',tenant_id:'13',source_mission_id:'m',mode:'active',policy:p,policy_hash:hash(p),scope_hash:hash(missionScope(mission)),pool:{}};
  const message={candidateId:'p',subject:'A question',body:'How are you handling the next stage of growth?'};
  const snapshot={candidateId:'p',prospectId:'p',companyId:'c',email:contact().email,message,sender:{senderEmail:p.senderEmail}};
  const item={id:'item',candidate_id:'p',prospect_id:'p',company_id:'c',email:contact().email,snapshot,status:'attempted',attempted_at:now};
  const envelope={id:'envelope',program_id:program.id,status:'authorized',approval_id:'approval',revision:'revision',mission_id:'m',manifest:[snapshot]};envelope.manifest_hash=hash(envelope.manifest);
  const binding={programId:program.id,policyHash:program.policy_hash,envelopeId:envelope.id,itemId:item.id,manifestHash:envelope.manifest_hash,approvalId:envelope.approval_id,revision:envelope.revision,mailboxIntegrationId:'mb',outreachAssetId:'asset'};
  const schedule={id:'schedule',status:'EXECUTING',tenantId:'13',prospectId:'p',missionId:'m',sendingIdentityId:'identity',recipientEmail:item.email,outreachAssetId:'asset',outreachAssetVersion:'1',authorizationSnapshot:{subject:message.subject,body:message.body,recipientEmail:item.email,governed:binding}};
  const store={program:async()=>program,envelope:async()=>envelope,items:async()=>[item],counts:async()=>({today:1,total:1,uncertain:1}),suppression:async()=>null};
  const adapters={loadMission:async()=>({mission}),prepared:async()=>({revision:'revision',candidates:[{candidateId:'p',message,item:{email:item.email,sendable:true,paige:{candidateId:'p'}}}]}),contact:async()=>contact(),liveGate:async()=>{}};
  const outreachAsset={version:1,content:{subject:message.subject,body:message.body,prospectId:'p',preparedArtifactRevision:'revision'}};
  return {program,envelope,item,schedule,store,adapters,outreachAsset};
}
test('durable execution rechecks kill switch, grant, scope, payload, suppression and contact classification',async()=>{
 const prior=process.env.BABRUN_GOVERNED_OUTBOUND_ENABLED;process.env.BABRUN_GOVERNED_OUTBOUND_ENABLED='true';
 try {
  for (const [change,code] of [
   [f=>f.program.mode='paused','governed_grant_changed'],
   [f=>f.schedule.recipientEmail='different@business.example','governed_schedule_payload_changed'],
   [f=>f.item.status='uncertain','governed_item_not_authorized'],
   [f=>f.store.suppression=async()=> 'reply_received','reply_received'],
   [f=>f.adapters.contact=async()=>contact('REVIEW_REQUIRED'),'contact_classification_not_sendable'],
   [f=>f.program.policy.dailyCap=2,'governed_grant_changed'],
   [f=>f.envelope.manifest=[],'governed_item_not_authorized'],
   [f=>f.outreachAsset.version=2,'governed_outreach_asset_changed'],
  ]) { const f=fixture();change(f);await assert.rejects(validateGovernedSchedule(f.schedule,{pool:{},now,governedStore:f.store,governedAdapters:f.adapters,outreachAsset:f.outreachAsset}),{code}); }
  const f=fixture();let liveGateOpts;f.adapters.liveGate=async(...args)=>{liveGateOpts=args[4];};assert.equal((await validateGovernedSchedule(f.schedule,{pool:{},now,governedStore:f.store,governedAdapters:f.adapters,outreachAsset:f.outreachAsset})).item.id,'item');assert.deepEqual(liveGateOpts,{reservedScheduleId:'schedule'});
  process.env.BABRUN_GOVERNED_OUTBOUND_ENABLED='false';await assert.rejects(validateGovernedSchedule(f.schedule,{pool:{},now}),{code:'environment_kill_switch'});
 }finally{if(prior===undefined)delete process.env.BABRUN_GOVERNED_OUTBOUND_ENABLED;else process.env.BABRUN_GOVERNED_OUTBOUND_ENABLED=prior;}
});
test('governed transport only authorizes and executes the bound durable schedule',async()=>{
 const f=fixture();let calls=0,received;
 const send=createGovernedTenantMailboxSend(f.program,f,{bridge:{createGovernedOutreachAsset:async()=>({id:'asset',version:'1'})},scheduler:{authorizeAndExecuteScheduledSend:async input=>{calls++;received=input;return {result:'sent',schedule:{id:'schedule'},message:{id:'message',providerMessageId:'provider',rfcMessageId:'<rfc>',threadId:'thread'}};}}});
 const boundary=createProviderBoundaryTracker();markLeafProviderSend(send,boundary);assert.equal(boundary.crossed,false);
 await assert.rejects(send({toEmail:'someone@else.example',subject:'A question',body:f.item.snapshot.message.body}),{code:'provider_payload_changed'});assert.equal(calls,0);
 const r=await send({toEmail:f.item.email,...f.item.snapshot.message,idempotencyKey:'execution'});
 assert.equal(calls,1);assert.equal(received.prospectId,'p');assert.equal(received.missionId,'m');assert.equal(received.governed.programId,'program');assert.equal(received.governed.mailboxIntegrationId,'mb');assert.equal(r.canonicalMessageId,'message');
});
test('reply classification scopes both tenant queries and never creates an AO handoff for mailbox tenant',async()=>{
 const queries=[];
 const pool={connect:async()=>({release(){},async query(sql,args=[]){queries.push({sql,args});
  if(sql.includes('pg_try_advisory_lock'))return {rows:[{locked:true}]};
  if(sql.includes('SELECT * FROM acquisition_outbound_replies'))return {rows:[{id:`reply-${args[0]}`,email:'owner@business.example',prospect_id:'p',payload:{body:'remove me'},attempts:0}]};
  if(sql.includes('SELECT * FROM acquisition_outbound_lifecycle'))return {rows:[{state:'dnc',next_action:'stop'}]};
  return {rows:[]};}})};
 const result=await classifyPending(pool,{tenantIds:['10','13'],classify:async()=>({classification:'unsubscribe'})});
 assert.equal(result.classified,2);
 assert.deepEqual(queries.filter(q=>q.sql.includes('SELECT * FROM acquisition_outbound_replies')).map(q=>q.args[0]),['10','13']);
 assert.deepEqual(queries.filter(q=>q.sql.includes('UPDATE prospects SET do_not_contact')).map(q=>q.args[1]),[10,13]);
 assert.ok(!queries.some(q=>q.sql.includes('INSERT INTO ao_leads')));
});
test('tenant mailbox suppression lookup cannot read another governed tenant',async()=>{
 const {PostgresTenantMailboxStore}=require('../services/tenantMailbox');const calls=[];
 const store=new PostgresTenantMailboxStore({query:async(sql,args)=>{calls.push({sql,args});return {rows:sql.includes('to_regclass')?[{installed:true}]:sql.includes('acquisition_outbound_lifecycle')?[{reason:'reply_received'}]:[]};}});
 store.ensureSchema=async()=>{};await store.findSuppression('13','owner@business.example');
 const governed=calls.find(q=>q.sql.includes('acquisition_outbound_lifecycle'));
 assert.ok(governed);assert.equal(governed.args[1],'13');assert.ok(!governed.sql.includes("tenant_id='10'"));
});
test('interested mailbox replies go to the tenant operator without Anchor AO writes',async()=>{
 const queries=[];const pool={connect:async()=>({release(){},async query(sql,args=[]){queries.push({sql,args});
 if(sql.includes('pg_try_advisory_lock'))return {rows:[{locked:true}]};
 if(sql.includes('SELECT * FROM acquisition_outbound_replies'))return {rows:[{id:'reply',email:'owner@business.example',prospect_id:'p',payload:{body:'interested'},attempts:0}]};
 if(sql.includes('SELECT * FROM acquisition_outbound_lifecycle'))return {rows:[{state:'interested',next_action:'operator_handoff'}]};return {rows:[]};}})};
 const r=await classifyPending(pool,{tenantId:'13',classify:async()=>({classification:'interested'})});assert.equal(r.classified,1);
 const action=queries.find(q=>q.sql.includes('INSERT INTO agent_actions'));assert.equal(action.args[3],13);assert.equal(action.args[2].nextAction,'operator_handoff');assert.ok(!queries.some(q=>q.sql.includes('ao_leads')));
});
