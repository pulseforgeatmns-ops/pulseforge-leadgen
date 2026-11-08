'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const {createTransports}=require('../services/signalOpportunities/notifications');
const env={SIGNAL_OPPORTUNITY_ALERTS_ENABLED:'1',SIGNAL_TELEGRAM_BOT_TOKEN:'test-token',SIGNAL_TELEGRAM_CHAT_ID:'5747865899'};
test('Telegram transport is opt-in and requires private configuration',()=>{
 const t=createTransports(env,{fetchFn:async()=>{throw Error('should not send')}}).find(x=>x.channel==='telegram');assert.equal(t.ready,true);
 assert.equal(createTransports({...env,SIGNAL_TELEGRAM_CHAT_ID:''}).find(x=>x.channel==='telegram').ready,false);
 assert.equal(createTransports({...env,SIGNAL_OPPORTUNITY_ALERTS_ENABLED:'0'}).find(x=>x.channel==='telegram').ready,false);
});
test('Telegram sends operational test without token or trade claims',async()=>{
 let url,body;
 const t=createTransports(env,{fetchFn:async(u,opts)=>{url=u;body=JSON.parse(opts.body);return {ok:true,json:async()=>({ok:true,result:{message_id:42}})};}}).find(x=>x.channel==='telegram');
 assert.equal(await t.send({kind:'OPERATIONAL_TRANSPORT_TEST',testId:'tg-test-1'}),'42');
 assert.match(url,/api.telegram.org\/bottest-token\/sendMessage/);
 assert.equal(body.chat_id,'5747865899');assert.match(body.text,/DELIVERY TEST/);assert.match(body.text,/No token, source call/);
});
test('Telegram rejects provider failures and invalid receipts',async()=>{
 const t=createTransports(env,{fetchFn:async()=>({ok:false})}).find(x=>x.channel==='telegram');await assert.rejects(t.send({kind:'OPERATIONAL_TRANSPORT_TEST',testId:'x'}),/telegram_provider_rejected/);
});
