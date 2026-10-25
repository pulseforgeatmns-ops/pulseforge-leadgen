'use strict';
const clean=x=>String(x??'UNKNOWN').replace(/[\r\n\u0000-\u001f]/g,' ').slice(0,100);
const dollars=x=>typeof x==='number'&&Number.isFinite(x)?'$'+Math.round(x).toLocaleString('en-US'):'UNKNOWN';
function alertText(p){
 const positive=p.evidence.increases.map(e=>e.text),negative=p.evidence.decreases.map(e=>e.text);
 return [`SIGNAL — ${p.opportunityState} ${p.opportunityState==='AVOID'?'RISK':'OPPORTUNITY'}`,
  clean(p.symbol||p.tokenAddress.slice(0,8)),`CA: ${p.tokenAddress}`,
  `${p.independentClusters.length} verified independent clusters; ${p.sources.length} observed sources`,
  `MC: ${dollars(p.marketCap)} | Liquidity: ${dollars(p.liquidity)}`,
  `Score: ${p.opportunityScore}/100 (${p.definitionVersion}; unvalidated)`,
  `WHY SCORE INCREASED: ${positive.join(' ')||'No positive evidence.'}`,
  `WHY SCORE DECREASED: ${negative.join(' ')||'No negative evidence recorded; this does not establish safety.'}`,
  `Major amplifier: ${p.amplifiers.some(a=>a.status==='OBSERVED')?'OBSERVED':p.amplifiers.some(a=>a.status==='COVERAGE_UNKNOWN')?'COVERAGE INCOMPLETE; arrival not predicted':'NOT YET OBSERVED; arrival not predicted'}`,
  `Risk: ${p.distributionRisk}`,`WHAT IS UNKNOWN: ${p.unknowns.join(' ')}`,`Known at: ${p.knowledgeAt}`,'Manual decision.'].join('\n');
}
function createTransports(env=process.env,{fetchFn=globalThis.fetch}={}){
 const on=env.SIGNAL_OPPORTUNITY_ALERTS_ENABLED==='1',phone=/^\+[1-9]\d{7,14}$/;
 const from=env.TWILIO_FROM||env.TWILIO_PHONE_NUMBER;
 return [
  {channel:'sms',ready:on&&Boolean(env.TWILIO_ACCOUNT_SID&&env.TWILIO_AUTH_TOKEN&&phone.test(from||'')&&phone.test(env.OPS_ALERT_PHONE||'')),async send(p){
   // Reuse the existing Pulseforge Twilio account/sender/destination. This
   // isolated adapter avoids importing Sam and outbound CRM dependencies.
   const r=await fetchFn(`https://api.twilio.com/2010-04-01/Accounts/${env.TWILIO_ACCOUNT_SID}/Messages.json`,{method:'POST',redirect:'error',signal:AbortSignal.timeout(5000),headers:{authorization:'Basic '+Buffer.from(env.TWILIO_ACCOUNT_SID+':'+env.TWILIO_AUTH_TOKEN).toString('base64'),'Content-Type':'application/x-www-form-urlencoded'},body:new URLSearchParams({From:from,To:env.OPS_ALERT_PHONE,Body:alertText(p)})});
   if(!r.ok)throw Error('sms_provider_rejected');const j=await r.json();if(!/^SM[a-f0-9]{32}$/i.test(j.sid||''))throw Error('sms_receipt_unknown');return j.sid;
  }},
  {channel:'email',ready:on&&Boolean(env.BREVO_API_KEY&&env.SIGNAL_OPPORTUNITY_EMAIL_TO&&env.SIGNAL_OPPORTUNITY_EMAIL_FROM),async send(p){
   const r=await fetchFn('https://api.brevo.com/v3/smtp/email',{method:'POST',redirect:'error',signal:AbortSignal.timeout(5000),headers:{'api-key':env.BREVO_API_KEY,'Content-Type':'application/json'},body:JSON.stringify({sender:{email:env.SIGNAL_OPPORTUNITY_EMAIL_FROM},to:[{email:env.SIGNAL_OPPORTUNITY_EMAIL_TO}],subject:`[Signal ${p.opportunityState}] ${clean(p.symbol||p.tokenAddress.slice(0,8))}`,textContent:alertText(p),headers:{'X-Signal-Opportunity-Id':p.id}})});
   if(!r.ok)throw Error('email_provider_rejected');const j=await r.json();if(typeof j.messageId!=='string'||!j.messageId)throw Error('email_receipt_unknown');return j.messageId;
  }}
 ];
}
async function deliver(store,transports){
 for(const t of transports){if(!t.ready)continue;const row=await store.claim(t.channel);if(!row)continue;
  try{const receipt=await t.send(row.payload);await store.finish(row,'ACCEPTED',receipt);}catch{await store.finish(row,'UNKNOWN');}
 }
}
module.exports={alertText,createTransports,deliver};
