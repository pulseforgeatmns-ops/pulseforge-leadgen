'use strict';
const {test}=require('node:test');
const assert=require('node:assert/strict');
const {project,validate,ConversationContextStore}=require('../ConversationContextStore');
const {ConversationContinuity}=require('../ConversationContinuity');
const session={id:'s1',context:{tenantId:'t1',missionId:'m1',executionApproved:true},operationalFocus:{tenantId:'t1',missionIds:['m1'],at:new Date().toISOString()},messages:[{text:'private customer text'}],presentedExecutionDecision:{approved:true}};

test('projection preserves referents while excluding authority, transcript and customer data',()=>{
  const value=project(session,'t1');
  assert.deepEqual(value.candidates,['m1']);
  assert.equal(value.requiresEvidenceRefresh,true);
  assert.doesNotMatch(JSON.stringify(value),/approved|execution|customer|private/i);
  assert.throws(()=>project(session,'t2'),/tenant/);
  assert.throws(()=>validate({...value,approval:true}),/Invalid/);
  assert.throws(()=>validate({...value,candidates:['x'.repeat(200)]}),/Invalid/);
});
test('reads are tenant-scoped and expired context cannot be recovered',async()=>{
  const calls=[];
  const store=new ConversationContextStore({db:{query:async(sql,args)=>{calls.push({sql,args});return {rows:[]};}},now:()=>100});
  assert.equal(await store.load('t2','s1'),null);
  assert.deepEqual(calls[0].args.slice(0,2),['t2','s1']);
  assert.match(calls[0].sql,/expires_at > \$3/);
});
test('writes use compare-and-swap, TTL, bounded JSON, and refuse lost updates',async()=>{
  const calls=[];
  const store=new ConversationContextStore({db:{query:async(sql,args)=>{calls.push({sql,args});return {rows:[]};}},now:()=>100});
  const r=await store.save('t1','s1',project(session,'t1'),'rev1');
  assert.equal(r.conflict,true);
  assert.equal(calls.length,1);
  assert.match(calls[0].sql,/revision=\$6 AND expires_at > \$7/);
  assert.equal(calls[0].args[4].getTime(),7200100);
});
test('restart recovery returns continuity hints and failure recovers without authority',async()=>{
  let persisted;
  const store={load:async()=>persisted||null,save:async(t,s,context)=>{persisted={revision:'r1',context};return {saved:true};}};
  const one=new ConversationContinuity({env:{MAX_CONVERSATION_CONTEXT:'true'},store});
  one.remember(session);await one.drain();
  const two=new ConversationContinuity({env:{MAX_CONVERSATION_CONTEXT:'true'},store});
  assert.deepEqual((await two.resume('t1','s1')).context.candidates,['m1']);
  const broken=new ConversationContinuity({env:{MAX_CONVERSATION_CONTEXT:'true'},audit:async()=>{throw Error('telemetry unavailable');},store:{load:async()=>{throw Error('database down');}}});
  assert.equal(await broken.resume('t1','s1'),null);
  broken.remember(session);await broken.drain();
  const off=new ConversationContinuity({env:{},store});
  assert.equal(await off.resume('t1','s1'),null);
});

test('in-memory sessions have idle expiry and a bounded capacity',()=>{
  const {SessionStore}=require('../../workspace/SessionStore');
  let now=0;
  const store=new SessionStore({now:()=>now,ttlMs:100,maxSessions:2});
  const a=store.create({tenantId:'t1'});now=1;
  const b=store.create({tenantId:'t1'});now=2;
  const c=store.create({tenantId:'t1'});
  assert.equal(store.get(a.id),null);
  assert.ok(store.get(b.id));assert.ok(store.get(c.id));
  now=103;assert.equal(store.get(b.id),null);assert.equal(store.size,0);
});

test('same-session observer writes preserve completion order without overlapping',async()=>{
  let active=0,peak=0;
  const written=[];
  const store={load:async()=>null,save:async(t,s,context)=>{
    active++;peak=Math.max(peak,active);
    await new Promise(resolve=>setImmediate(resolve));
    written.push(context.candidates[0]);active--;return {saved:true};
  }};
  const observer=new ConversationContinuity({env:{MAX_CONVERSATION_CONTEXT:'true'},store});
  observer.remember({id:'s1',context:{tenantId:'t1',missionId:'m1'}});
  observer.remember({id:'s1',context:{tenantId:'t1',missionId:'m2'}});
  await observer.drain();
  assert.equal(peak,1);assert.deepEqual(written,['m1','m2']);
  assert.equal(observer.queues.size,0);
});
