'use strict';
const {evaluate}=require('../signalDistribution/risk');
const {knownBy}=require('./quality');
const {promotionIntensity}=require('./events');
function peaks(observations,entryAt,callAt,asOf) {
  const scan=start=>{
    if(!start)return {priceUsd:null,at:null,drawdown:null,chronology:[]};
    let peak=null,at=null,current=null;const chronology=[];
    for(const o of observations.filter(x=>knownBy(x,asOf)&&Date.parse(x.observedAt)>=Date.parse(start)).sort((a,b)=>Date.parse(a.observedAt)-Date.parse(b.observedAt))) {
      if(typeof o.priceUsd!=='number'||!Number.isFinite(o.priceUsd)||o.priceUsd<=0)continue;
      current=o.priceUsd;
      if(peak===null||o.priceUsd>peak){peak=o.priceUsd;at=o.observedAt;chronology.push({priceUsd:peak,observedAt:at,knownAt:o.knownAt,provider:o.provider});}
    }
    return {priceUsd:peak,at,drawdown:peak&&current!==null?(current-peak)/peak:null,chronology};
  };
  return {postEntry:scan(entryAt),postCall:scan(callAt),quality:'PARTIAL',basis:'Only observations actually collected after the boundary; not historical ATH.'};
}
function distribution(observations,events,asOf,retainedState=null) {
  const eligible=observations.filter(o=>knownBy(o,asOf)).sort((a,b)=>Date.parse(a.observedAt)-Date.parse(b.observedAt));
  let state=retainedState||{}, transitions=[];
  for(const snapshot of eligible){
    const result=evaluate(state,snapshot,{now:new Date(snapshot.knownAt)});
    if(result.ignored)continue;
    if(state.riskState!==result.state.riskState)transitions.push({category:`DISTRIBUTION_${result.state.riskState}`,occurredAt:snapshot.observedAt,knownAt:snapshot.knownAt,source:'distribution-v1.0',evidence:result.state.reasons,provenance:snapshot.provenance});
    state=result.state;
  }
  const promotion=promotionIntensity(events,asOf),latest=eligible.at(-1),prior=eligible.filter(o=>Date.parse(o.observedAt)<=Date.parse(asOf)-300000).at(-1);
  const momentum=latest&&prior&&latest.pairId===prior.pairId&&prior.priceUsd>0?(latest.priceUsd-prior.priceUsd)/prior.priceUsd:null;
  const sellShare=state.metrics?.sellShare??state.metrics?.sellCountShare??null;
  const divergent=promotion.rising&&(momentum!==null&&momentum<0||sellShare!==null&&sellShare>=.6||state.metrics?.liquidityValueDeclineFromBaseline>=.1);
  const unknown=['Historical large-holder activity unavailable','Developer wallet identity unavailable'];
  if(!latest)return {state:'UNKNOWN',quality:'UNKNOWN',for:[],against:[],unknown:[...unknown,'No market observations'],promotion,divergence:{state:'UNKNOWN',researchOnly:true},transitions};
  if(!latest.providerTimestamp)unknown.push('Provider source freshness unavailable');
  if(latest.sellVolume5mUsd==null||latest.buyVolume5mUsd==null)unknown.push('Buy/sell USD volume unavailable; transaction counts are a proxy');
  if(momentum===null)unknown.push('Five-minute momentum baseline unavailable');
  const stale=Date.parse(asOf)-Date.parse(latest.observedAt)>20000;
  const evidence=[...(state.reasons||[]),...(divergent?['PROMOTION_MARKET_DIVERGENCE']:[]),...(stale?['MARKET_OBSERVATION_DELAYED']:[])];
  const against=[];
  if(state.metrics?.drawdownFromObservedPeak!=null&&state.metrics.drawdownFromObservedPeak<.1)against.push('Observed drawdown below 10%');
  if(sellShare!==null&&sellShare<.5)against.push('Observed sell activity below 50%');
  if(state.metrics?.liquidityValueDeclineFromBaseline!=null&&state.metrics.liquidityValueDeclineFromBaseline<=0)against.push('Pool reserve value has not declined from observed baseline');
  let risk=state.riskState||'UNKNOWN';if((divergent||stale)&&risk==='NORMAL')risk='WATCH';
  return {state:risk,quality:stale?'DELAYED':'PARTIAL',for:evidence,against,unknown,
    disclaimer:'No alert does not mean safe. Reserve value changes do not prove LP withdrawals.',
    observedAt:latest.observedAt,knownAt:latest.knownAt,version:state.version,metrics:state.metrics,promotion,
    divergence:{state:divergent?'OBSERVED':momentum===null?'UNKNOWN':'NOT_OBSERVED',researchOnly:true,momentum,sellActivityShare:sellShare,automaticExit:false},transitions};
}
module.exports={peaks,distribution};
