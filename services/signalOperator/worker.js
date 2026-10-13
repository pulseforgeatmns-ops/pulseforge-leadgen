'use strict';
const {captureMarketSnapshot}=require('../../packages/signal-v1/prospective/marketCapture');
const {buildOperatorAlert}=require('../../packages/signal-v1/operator/alertOutbox');
function bounded(task,ms) {
  let timer;
  return Promise.race([Promise.resolve().then(task),new Promise(resolve=>{timer=setTimeout(()=>resolve(null),ms);})])
    .finally(()=>clearTimeout(timer));
}
function iso(now) { return now().toISOString(); }
async function sendSmsStage({store,row,alert,smsRelay,stage,method,now}) {
  const delivery={...(alert.delivery || {})};
  const prior=delivery[stage];
  if(prior?.state==='DISPATCHING') {
    delivery[stage]={...prior,state:'UNKNOWN',error:'dispatch_result_not_persisted'};
    alert={...alert,delivery};
    await store.setPayload(row.id,alert,row.attempts);
    return alert;
  }
  if(prior || !smsRelay?.[method])return alert;
  delivery[stage]={state:'DISPATCHING',dispatchedAt:iso(now)};
  alert={...alert,delivery};
  await store.setPayload(row.id,alert,row.attempts);
  try {
    const result=await smsRelay[method](alert);
    delivery[stage]=result?.skipped
      ? {state:'SKIPPED',reason:result.reason || 'not_meaningful'}
      : {state:'ACCEPTED',dispatchedAt:delivery[stage].dispatchedAt,
        acceptedAt:result.acceptedAt || iso(now),receipt:result.receipt,status:result.status || null};
  } catch {
    delivery[stage]={state:'FAILED',dispatchedAt:delivery[stage].dispatchedAt,error:'sms_dispatch_failed'};
  }
  alert={...alert,delivery};
  await store.setPayload(row.id,alert,row.attempts);
  return alert;
}
function createOperatorWorker({store,feedUrl,token,channelId,marketProvider,relay,smsRelay,gate,sendEnabled=()=>true,fetchImpl=globalThis.fetch,now=()=>new Date(),marketTimeoutMs=2000}) {
  const url=new URL(feedUrl);
  if(url.protocol!=='http:' || !url.hostname.endsWith('.railway.internal') || url.pathname!=='/operator-events'
    || url.username || url.password || url.search || url.hash || typeof token!=='string' || token.length<32) throw new Error('invalid_private_operator_feed');
  let ingesting=false,sending=false;
  return {
    async ingestOnce() {
      if(ingesting || !gate().ok) return {skipped:true};
      ingesting=true;
      try {
        const response=await fetchImpl(url.href,{headers:{authorization:`Bearer ${token}`},redirect:'error',signal:AbortSignal.timeout(2000)});
        if(!response.ok) throw new Error('operator_feed_unavailable');
        const body=await response.json();
        if(!Array.isArray(body.events) || body.events.length>2000) throw new Error('invalid_operator_feed_response');
        const readiness=gate();if(!readiness.ok)return {skipped:true};
        let enqueued=0;
        for(const event of body.events) {
          if(Date.parse(event.occurredAt)<Date.parse(readiness.startAt))continue;
          if(!gate().ok)break;
          await store.enqueue(event);enqueued++;
        }
        return {enqueued};
      }finally{ingesting=false;}
    },
    async sendOnce() {
      if(sending || !sendEnabled() || !gate().ok)return {skipped:true};
      sending=true;
      let row;
      try {
        row=await store.claim({since:gate().startAt});if(!row)return {empty:true};
        // SMS #1 is intentionally attempted before market enrichment. The
        // DISPATCHING marker is durable and suppresses a duplicate after a
        // crash where Twilio may have accepted the message.
        let alert=await sendSmsStage({store,row,alert:row.payload,smsRelay,stage:'smsInitial',method:'sendInitial',now});
        // Persist the exact mail body before the first attempt. Retries never
        // change market fields while reusing an idempotency key.
        if(!alert.marketAttempted) {
          let capture=null;
          try { capture=await bounded(()=>captureMarketSnapshot(marketProvider,alert.tokenAddress,now(),{now}),marketTimeoutMs); }catch{}
          alert={...buildOperatorAlert({evidence:{id:alert.evidenceId,sourceId:alert.sourceId,
            externalMessageId:alert.externalMessageId,extractedCa:alert.tokenAddress,occurredAt:alert.occurredAt,
            ingestedAt:alert.ingestedAt,provenance:{dataClass:'EMPIRICAL',telegramChannelId:alert.channelId}},
            approvedChannelId:channelId,snapshot:capture?.ok?capture.snapshot:null}),
            delivery:alert.delivery,marketAttempted:true};
          await store.setPayload(row.id,alert,row.attempts);
        }
        // At most one enrichment update, and only after an accepted initial
        // SMS. Null/partial context is skipped by the SMS relay.
        if(alert.delivery?.smsInitial?.state==='ACCEPTED') {
          alert=await sendSmsStage({store,row,alert,smsRelay,stage:'smsUpdate',method:'sendUpdate',now});
        }
        if(!gate().ok)throw new Error('pilot_not_ready');
        const result=await relay.send(alert);
        await store.accepted(row.id,result.receipt,row.attempts);
        return {accepted:true,received:false,displayed:false,
          smsInitial:alert.delivery?.smsInitial || null,smsUpdate:alert.delivery?.smsUpdate || null};
      }catch(err){
        const allowed=['relay_retryable','relay_rejected_or_duplicate','relay_transport_unknown','relay_disabled_or_not_ready','pilot_not_ready'];
        if(row)await store.failed(row,allowed.includes(err.message)?err.message:'operator_attempt_failed');
        return {accepted:false};
      }finally{sending=false;}
    },
  };
}
function startOperatorTimers(worker,{setInterval:interval=setInterval,clearInterval:clear=clearInterval}={}) {
  const safe=fn=>()=>Promise.resolve().then(fn).catch(()=>{});
  const timers=[interval(safe(()=>worker.ingestOnce()),1000),interval(safe(()=>worker.sendOnce()),1000)];
  timers.forEach(t=>t.unref?.());return ()=>timers.forEach(clear);
}
module.exports={createOperatorWorker,startOperatorTimers,bounded,sendSmsStage};
