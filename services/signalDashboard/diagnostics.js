'use strict';
// Private service logs contain collection health, never wallet amounts,
// transaction signatures, account identities or provider credentials.
function operationalSummary(position,observations=[],candle=null){
 if(!position)return {state:'UNKNOWN'};
 const known=name=>position.market?.[name]?.value!=null;
 const times=observations.map(o=>Date.parse(o.observedAt)).filter(Number.isFinite).sort((a,b)=>b-a);
 return {asOf:position.asOf,marketObservedAt:position.lastUpdated,marketIntervalsMs:times.slice(0,6).flatMap((t,i)=>times[i+1]!=null?[t-times[i+1]]:[]),
  paperZoneResearch:{mode:position.researchZones?.mode||'UNKNOWN',quality:position.researchZones?.quality||'UNKNOWN',recordedInWindow:position.researchZones?.counts?.recordedInWindow||0,lastCheckedAt:position.researchZones?.lastCheckedAt||null},
  historicalCandlesAvailable:Boolean(candle),lastCandleInterval:candle?.interval||null,lastCandleCollectedAt:candle?.knownAt||null,
  priceAvailable:known('priceUsd'),marketCapAvailable:known('marketCapUsd'),liquidityAvailable:known('liquidityUsd'),
  historyExhausted:position.coverage.historyExhausted,headCaughtUp:position.coverage.headCaughtUp,
  balanceObservedAt:position.coverage.walletObservedAt,reconciliation:position.coverage.reconciliation.state,
  tradeMarkerCount:position.events.filter(e=>['BUY','SELL'].includes(e.category)).length,
  quoteAccountingAvailable:position.quoteAccounting?.totalPnl?.value!=null,
  distributionState:position.risk.state,
  collectors:position.coverage.statuses.map(s=>({component:s.component.split(':')[0],state:s.state,reason:s.reason}))};
}
module.exports={operationalSummary};
