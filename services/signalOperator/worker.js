'use strict';
const {captureMarketSnapshot}=require('../../packages/signal-v1/prospective/marketCapture');
const {buildOperatorAlert}=require('../../packages/signal-v1/operator/alertOutbox');
function bounded(task,ms) {
  let timer;
  return Promise.race([Promise.resolve().then(task),new Promise(resolve=>{timer=setTimeout(()=>resolve(null),ms);})])
    .finally(()=>clearTimeout(timer));
}
function createOperatorWorker({store,feedUrl,token,channelId,marketProvider,relay,gate,sendEnabled=()=>true,fetchImpl=globalThis.fetch,now=()=>new Date(),marketTimeoutMs=2000}) {
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
        // Persist the exact mail body before the first attempt. Retries never
        // change market fields while reusing an idempotency key.
        let alert=row.payload;
        if(!alert.marketAttempted) {
          let capture=null;
          try { capture=await bounded(()=>captureMarketSnapshot(marketProvider,alert.tokenAddress,now(),{now}),marketTimeoutMs); }catch{}
          alert={...buildOperatorAlert({evidence:{id:alert.evidenceId,sourceId:alert.sourceId,
            externalMessageId:alert.externalMessageId,extractedCa:alert.tokenAddress,occurredAt:alert.occurredAt,
            ingestedAt:alert.ingestedAt,provenance:{dataClass:'EMPIRICAL',telegramChannelId:alert.channelId}},
            approvedChannelId:channelId,snapshot:capture?.ok?capture.snapshot:null}),marketAttempted:true};
          await store.setPayload(row.id,alert,row.attempts);
        }
        if(!gate().ok)throw new Error('pilot_not_ready');
        const result=await relay.send(alert);
        await store.accepted(row.id,result.receipt,row.attempts);
        return {accepted:true,received:false,displayed:false};
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
module.exports={createOperatorWorker,startOperatorTimers,bounded};
