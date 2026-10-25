'use strict';
// Private service logs contain collection health, never wallet amounts,
// transaction signatures, account identities or provider credentials.
function operationalSummary(position){
 if(!position)return {state:'UNKNOWN'};
 const known=name=>position.market?.[name]?.value!=null;
 return {asOf:position.asOf,marketObservedAt:position.lastUpdated,
  priceAvailable:known('priceUsd'),marketCapAvailable:known('marketCapUsd'),liquidityAvailable:known('liquidityUsd'),
  historyExhausted:position.coverage.historyExhausted,headCaughtUp:position.coverage.headCaughtUp,
  balanceObservedAt:position.coverage.walletObservedAt,reconciliation:position.coverage.reconciliation.state,
  tradeMarkerCount:position.events.filter(e=>['BUY','SELL'].includes(e.category)).length,
  quoteAccountingAvailable:position.quoteAccounting?.totalPnl?.value!=null,
  distributionState:position.risk.state,
  collectors:position.coverage.statuses.map(s=>({component:s.component.split(':')[0],state:s.state,reason:s.reason}))};
}
module.exports={operationalSummary};
