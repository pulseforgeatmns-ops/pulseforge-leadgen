'use strict';
const test=require('node:test');const assert=require('node:assert/strict');
const {createSmsAdapter,createEmailAdapter,createDistributionDispatcher}=require('../services/signalDistribution/dispatch');
const {newCriticalEvidence,DeliveryStore}=require('../services/signalDistribution/deliveryStore');
const event={id:'e1',tenantId:'1',namespace:'operational',monitorId:'m1',kind:'RISK_TRANSITION',state:'HIGH',reasons:['DRAWDOWN'],observedAt:'2026-10-14T00:00:00Z'};
const yes=()=>true;
test('SMS disabled default; TEST never reaches injected provider even with consent',async()=>{
 let calls=0;const client={messages:{create:async()=>{calls++;return{sid:'fake-test-sid'};}}};
 const opts={client,from:'+15555550101',destination:'+15555550102'};
 await assert.rejects(createSmsAdapter(opts).send(event));
 await assert.rejects(createSmsAdapter({...opts,enabled:yes,consented:yes}).send({...event,namespace:'test'}));
 assert.equal(calls,0);
});
test('isolated SMS primitive reports acceptance only',async()=>{
 let payload;const sender=createSmsAdapter({enabled:yes,consented:yes,from:'+15555550101',destination:'+15555550102',client:{messages:{create:async p=>{payload=p;return{sid:'fake-test-sid'};}}}});
 assert.deepEqual(await sender.send(event),{accepted:true,receipt:'fake-test-sid'});assert.match(payload.body,/Manual only; no trade/);assert.equal(payload.to,'+15555550102');
});
test('email adapter remains opt-in and TEST blocked',async()=>{
 let calls=0;const sendMail=async()=>{calls++;return{accepted:true,receipt:'fake-mail'};};
 await assert.rejects(createEmailAdapter({sendMail}).send(event,'id'));
 await assert.rejects(createEmailAdapter({sendMail,enabled:yes,consented:yes}).send({...event,namespace:'test'},'id'));assert.equal(calls,0);
});
test('dispatch defaults off and never claims test namespace',async()=>{
 let claims=0;const store={claim:async()=>{claims++;}};
 await createDistributionDispatcher({store}).sendOnce({tenantId:'1',channel:'sms'});
 await createDistributionDispatcher({store,enabled:yes,consented:yes,adapters:[{id:'sms'}]}).sendOnce({tenantId:'1',namespace:'test',channel:'sms'});assert.equal(claims,0);
});
test('transport ambiguity becomes UNKNOWN; no retry or email fallback',async()=>{
 const changes=[];let sends=0;const store={claim:async()=>({id:'d',tenant_id:'1',namespace:'operational',channel:'sms',payload:event}),unknown:async id=>changes.push(id)};
 const d=createDistributionDispatcher({store,enabled:yes,consented:yes,adapters:[{id:'sms',send:async()=>{sends++;throw new Error('secret provider failure');}}]});
 assert.deepEqual(await d.sendOnce({tenantId:'1',channel:'sms'}),{accepted:false,unknown:true});assert.deepEqual(changes,['d']);assert.equal(sends,1);
});
test('channel acceptance is independent and not receipt/display proof',async()=>{
 const receipts=[];const store={claim:async({channel})=>({id:channel,tenant_id:'1',namespace:'operational',channel,payload:event}),accepted:async(...a)=>receipts.push(a)};
 const d=createDistributionDispatcher({store,enabled:yes,consented:yes,adapters:['sms','email'].map(id=>({id,send:async()=>({accepted:true,receipt:id+'-receipt'})}))});
 for(const channel of ['sms','email'])assert.deepEqual(await d.sendOnce({tenantId:'1',channel}),{accepted:true,received:false,displayed:false});assert.equal(receipts.length,2);
});
test('CRITICAL bypass requires escalation or a novel reason, not changed timestamp',()=>{
 const c={...event,state:'CRITICAL',coverage:{observationFreshness:'FRESH',pair:'PINNED',sourceFreshness:'UNKNOWN'},reasons:['SEVERE_PRICE_DRAWDOWN_WITH_SELL_PRESSURE']};assert.equal(newCriticalEvidence(c,event),true);
 assert.equal(newCriticalEvidence({...c,observedAt:'later'},c),false);
 assert.equal(newCriticalEvidence({...c,reasons:['SEVERE_PRICE_DRAWDOWN_WITH_SELL_PRESSURE','SEVERE_PRICE_DRAWDOWN_WITH_RESERVE_DECLINE']},c),true);
 assert.equal(newCriticalEvidence({...c,reasons:[]},event),false);
});
test('read delivery model only SELECTs scoped sanitized columns',async()=>{
 const calls=[];const s=new DeliveryStore({query:async(q,p)=>{calls.push([q,p]);return{rows:[]};}});
 await s.getDeliveries({tenantId:'1',namespace:'test',monitorId:'m'});assert.match(calls[0][0],/^SELECT/);assert.deepEqual(calls[0][1],['1','test','m']);assert.doesNotMatch(calls[0][0],/provider_receipt|payload/);
});

test('SMS stays GSM7 single-septet ASCII within 300 chars, missing milestone never invents 2x',()=>{
 const {formatDistributionSms}=require('../services/signalDistribution/dispatch');
 const text=formatDistributionSms({...event,kind:'MILESTONE_2X',tokenAddress:'A'.repeat(44),reasons:['🔥危険{}[]^~€'.repeat(100)],metrics:{referenceMultiple:123.456,drawdownFromObservedPeak:0.3}});
 assert.ok(text.length<=300);assert.match(text,/UNKNOWNx/);assert.doesNotMatch(text,/2x/);assert.match(text,/SOURCE TIME UNKNOWN; Manual only; no trade$/);
 assert.match(text,/^[a-zA-Z0-9 .,;:_+%()\/=!?@#&*-]+$/);
});
