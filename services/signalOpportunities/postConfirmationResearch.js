'use strict';
// Offline discovery-only evaluator. No database, network, scoring or holdout access.
const HORIZONS=[10,30,60];
function ms(v){const t=Date.parse(v);return Number.isFinite(t)?t:NaN;}
function evaluatePostConfirmation({record,confirmation,observations,delayMs=60000,maxEntryLagMs=120000,feeBps=0,slippageBps=0,liquidityFloorUsd=0}){
 if(!record||record.partition!=='HISTORICAL_DISCOVERY'||record.dataClass!=='HISTORICAL_BACKFILL')throw Error('discovery_historical_only');
 if(!confirmation||!Array.isArray(observations))throw Error('invalid_inputs');
 const confirmed=ms(confirmation.observedAt),known=ms(confirmation.knownAt);
 // Historical import time is never evidence of when an amplifier became observable.
 if(!Number.isFinite(confirmed)||!Number.isFinite(known)||known<confirmed) return {status:'UNOBSERVABLE_CONFIRMATION'};
 if(!Number.isFinite(delayMs)||delayMs<0||!Number.isFinite(maxEntryLagMs)||maxEntryLagMs<0)throw Error('invalid_delay');
 if(![feeBps,slippageBps].every(v=>Number.isFinite(v)&&v>=0&&v<10000)||!Number.isFinite(liquidityFloorUsd)||liquidityFloorUsd<0)throw Error('invalid_costs');
 const decisionAt=Math.max(confirmed,known),earliest=decisionAt+delayMs;
 const market=observations.filter(o=>o.tokenAddress===record.tokenAddress&&Number(o.priceUsd)>0&&Number.isFinite(ms(o.observedAt))&&Number.isFinite(ms(o.knownAt))&&ms(o.knownAt)>=ms(o.observedAt)&&Number(o.liquidityUsd)>=liquidityFloorUsd).sort((a,b)=>ms(a.observedAt)-ms(b.observedAt));
 const entry=market.find(o=>ms(o.observedAt)>=earliest&&ms(o.knownAt)<=ms(o.observedAt)+maxEntryLagMs&&ms(o.knownAt)<=earliest+maxEntryLagMs);
 if(!entry)return {status:'NO_EXECUTABLE_ENTRY'};
 const entryAt=ms(entry.knownAt),entryPrice=Number(entry.priceUsd)*(1+(feeBps+slippageBps)/10000);
 const outcomes={};
 for(const h of HORIZONS){
  const cutoff=entryAt+h*60000;
  const window=market.filter(o=>ms(o.knownAt)>entryAt&&ms(o.knownAt)<=cutoff&&ms(o.observedAt)<=ms(o.knownAt)&&ms(o.knownAt)<=ms(o.observedAt)+maxEntryLagMs);
  const end=window.slice().sort((a,b)=>ms(a.knownAt)-ms(b.knownAt)).at(-1);
  if(!end){outcomes[h+'m']={status:'INSUFFICIENT_MARKET_DATA'};continue;}
  const net=p=>100*(p*(1-(feeBps+slippageBps)/10000)/entryPrice-1);
  const returns=window.map(o=>net(Number(o.priceUsd)));
  outcomes[h+'m']={status:'OBSERVED_PARTIAL',sampleCount:window.length,netLastPct:net(Number(end.priceUsd)),netMfePct:Math.max(...returns),netMaePct:Math.min(...returns),reached2x:returns.some(v=>v>=100),lastObservedAt:end.observedAt,lastKnownAt:end.knownAt};
 }
 return {status:'EVALUATED',entryAt:entry.knownAt,entryMarketObservedAt:entry.observedAt,confirmationKnownAt:confirmation.knownAt,entryPriceUsd:entryPrice,outcomes,quality:'OBSERVATION_BASED_NOT_EXECUTION_PROOF'};
}
module.exports={HORIZONS,evaluatePostConfirmation};
