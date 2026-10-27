'use strict';
const {id}=require('./wallet');
const {knownBy}=require('./quality');
const {extractSolanaContractAddresses}=require('../../packages/signal-v1/prospective/caExtraction');
const CATEGORIES=['CALLER','FIRST_CALLER','INDEPENDENT_CONVERGENCE','FRONT_RUNNERS_CALL','PROMOTION_EVENT','DEX_BOOST','AMPLIFIER_EVENT','BUY','SELL','DE_RISK_MILESTONE','DISTRIBUTION_WATCH','DISTRIBUTION_HIGH','DISTRIBUTION_CRITICAL','LIQUIDITY_WARNING'];
const PROMOTION=new Set(['PROMOTION_EVENT','DEX_BOOST','AMPLIFIER_EVENT']);
function feedEvents(call, {now=new Date(), registry={}}={}) {
  if(call.provenance?.dataClass!=='EMPIRICAL'||call.provenance.synthetic||call.provenance.testOnly||!call.externalMessageId
    ||!Number.isFinite(Date.parse(call.occurredAt))||!Number.isFinite(Date.parse(call.ingestedAt))
    ||Date.parse(call.occurredAt)>+now||Date.parse(call.ingestedAt)>+now)return[];
  const cas=extractSolanaContractAddresses(call.text||call.rawText||'');
  // Only text with an explicit CA is associated. Replies/catalyst mentions without a CA stay unknown.
  return cas.flatMap(tokenAddress=>{
    const text=String(call.text||call.rawText||'').slice(0,12000), reg=registry[call.sourceId]||{};
    const clusterId=reg.clusterId||null;
    const base={tokenAddress,source:call.sourceId,sourceId:call.sourceId,clusterId,
      independence:call.forwardedFrom||call.forwarding?'CORRELATED':reg.relationship||'UNKNOWN',
      externalMessageId:call.externalMessageId,occurredAt:call.occurredAt,knownAt:new Date(Math.max(Date.parse(call.ingestedAt),+now)).toISOString(),
      evidence:text,provenance:{...call.provenance,feedIngestedAt:call.ingestedAt,importedAt:now.toISOString(),url:call.url||null}};
    const kinds=['CALLER',...(call.sourceId==='telegram-front-runners'?['FRONT_RUNNERS_CALL']:[])];
    if(/boost/i.test(text))kinds.push('DEX_BOOST');
    else if(/add to|positions|\b\d+(\.\d+)?x\b|catalyst|pump|buy|moon|launch|listing/i.test(text))kinds.push('PROMOTION_EVENT');
    // One source message is one promotion, even if multiple descriptive labels apply.
    return kinds.map(category=>({...base,id:id([call.sourceId,call.externalMessageId,tokenAddress,category]),category}));
  });
}
function promotionIntensity(events,asOf) {
  const end=Date.parse(asOf),dedupe=new Map();
  for(const e of events)if(PROMOTION.has(e.category)&&knownBy(e,asOf))dedupe.set(`${e.sourceId}:${e.externalMessageId}`,e);
  const all=[...dedupe.values()], recent=all.filter(e=>Date.parse(e.occurredAt)>end-300000),prior=all.filter(e=>Date.parse(e.occurredAt)>end-600000&&Date.parse(e.occurredAt)<=end-300000);
  const sources=new Set(recent.map(e=>e.sourceId)),clusters={};
  for(const e of recent)clusters[e.clusterId||'UNKNOWN']=(clusters[e.clusterId||'UNKNOWN']||0)+1;
  return {state:recent.length>=12?'EXTREME':recent.length>=6?'HIGH':recent.length>=2?'NORMAL':'LOW',
    promotionEventCount:recent.length,promotionVelocityPerMinute:recent.length/5,priorWindowCount:prior.length,
    rising:recent.length>prior.length,uniquePromotionalSources:sources.size,sameClusterActivity:clusters,
    independentConfirmations:null,quality:'PARTIAL',researchOnly:true,interpretation:'Descriptive activity; high promotion is not a bullish confirmation.'};
}
module.exports={CATEGORIES,PROMOTION,feedEvents,promotionIntensity};
