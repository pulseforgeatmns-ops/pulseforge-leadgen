'use strict';
const {createHash,timingSafeEqual}=require('node:crypto');
const {extractSolanaContractAddresses}=require('../../packages/signal-v1/prospective/caExtraction');
function authorized(header, token) {
  if (typeof token !== 'string' || token.length < 32 || typeof header !== 'string') return false;
  const digest = value=>createHash('sha256').update(value).digest();
  return timingSafeEqual(digest(header),digest(`Bearer ${token}`));
}
function minimalEvents(calls, channelId, {startAt,now=new Date()}={}) {
  if (!/^\d+$/.test(String(channelId))) return [];
  const start=Date.parse(startAt);
  if (!Number.isFinite(start)) return [];
  return calls.flatMap(call=>{
    if (call.sourceId !== 'telegram-front-runners' || String(call.provenance?.telegramChannelId)!==String(channelId)
      || call.provenance?.dataClass!=='EMPIRICAL' || call.provenance?.synthetic || call.provenance?.testOnly
      || !Number.isFinite(Date.parse(call.occurredAt)) || !Number.isFinite(Date.parse(call.ingestedAt))
      || Date.parse(call.occurredAt)<start || Date.parse(call.occurredAt)>+now || Date.parse(call.ingestedAt)>+now
      || !new RegExp(`^telegram:${channelId}:\\d+$`).test(call.externalMessageId)) return [];
    return extractSolanaContractAddresses(call.text || '').map(ca=>({
      sourceId:call.sourceId,externalMessageId:call.externalMessageId,extractedCa:ca,
      occurredAt:call.occurredAt,ingestedAt:call.ingestedAt,
      provenance:{dataClass:'EMPIRICAL',telegramChannelId:String(channelId)},
    }));
  });
}
module.exports={authorized,minimalEvents};
