'use strict';
const { formatOperatorAlertText } = require('../../packages/signal-v1/operator/alertText');
const SENDER='jacob@gopulseforge.com';
const RECIPIENT='pulseforgeatmns@gmail.com';
function operationalTransportTest(testId) {
  if(typeof testId!=='string' || !/^[a-zA-Z0-9-]{1,80}$/.test(testId))throw new Error('invalid_operational_test_id');
  return {kind:'OPERATIONAL_TRANSPORT_TEST',id:require('node:crypto').createHash('sha256').update(`operational-test:${testId}`).digest('hex')};
}
function mailPayload(alert) {
  const id=alert.id;
  const key=`${id.slice(0,8)}-${id.slice(8,12)}-4${id.slice(13,16)}-a${id.slice(17,20)}-${id.slice(20,32)}`;
  const v=x=>x==null?'UNAVAILABLE':String(x);
  if(alert.kind==='OPERATIONAL_TRANSPORT_TEST')return {sender:{email:SENDER},to:[{email:RECIPIENT}],
    subject:`[Signal Operational Test] ${id}`,headers:{idempotencyKey:key,'X-Signal-Alert-Id':id},
    textContent:`SIGNAL_OPERATIONAL_TRANSPORT_TEST_V1 ${id}\nOperational delivery test only. No CA, Telegram observation, research evidence, market signal or trade. Exclude from all research and performance measurements.`};
  const textContent = formatOperatorAlertText(alert);
  return {sender:{email:SENDER},to:[{email:RECIPIENT}],
    subject:`[Signal] FRONT RUNNERS — ${alert.tokenAddress?.slice(0, 8) || id}`,
    headers:{idempotencyKey:key,'X-Signal-Alert-Id':id},
    textContent: [`SIGNAL_OPERATOR_ALERT_V1 ${id}`, textContent].join('\n\n')};
}
function createBrevoRelay({enabled=false,consented=false,operationalTestApproved=false,apiKey,fetchImpl=globalThis.fetch,gate=()=>({ok:false})}={}) {
  return {
    id: 'brevo-gmail',
    enabled: () => enabled && consented && Boolean(apiKey) && gate().ok,
    async send(alert) {
      if (!enabled || !consented || !apiKey || !gate().ok) throw new Error('relay_disabled_or_not_ready');
      if(alert.kind==='OPERATIONAL_TRANSPORT_TEST' && !operationalTestApproved)throw new Error('operational_test_not_approved');
      let response;
      try {
        response=await fetchImpl('https://api.brevo.com/v3/smtp/email',{method:'POST',redirect:'error',
          signal:AbortSignal.timeout(5000),headers:{'api-key':apiKey,'Content-Type':'application/json','Accept':'application/json'},
          body:JSON.stringify(mailPayload(alert))});
      } catch { throw new Error('relay_transport_unknown'); }
      if (!response.ok) throw new Error(response.status===429 || response.status>=500 ? 'relay_retryable':'relay_rejected_or_duplicate');
      let body;try{body=await response.json();}catch{throw new Error('relay_transport_unknown');}
      if (typeof body.messageId!=='string' || body.messageId.length>512) throw new Error('relay_transport_unknown');
      return {receipt:body.messageId,accepted:true,received:false,displayed:false};
    },
  };
}
module.exports={mailPayload,createBrevoRelay,operationalTransportTest,SENDER,RECIPIENT};
