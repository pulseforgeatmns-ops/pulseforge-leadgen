'use strict';
const crypto=require('node:crypto');
const VERSION='signal-paper-v1.0.0';
const CONFIG=Object.freeze({startingCashUsd:10000,maxPositionPct:.10,feeBps:50,slippageBps:100});
const hash=x=>crypto.createHash('sha256').update(JSON.stringify(x)).digest('hex');
function applyCost(price,side){const bps=(CONFIG.feeBps+CONFIG.slippageBps)/10000;return price*(side==='BUY'?1+bps:1-bps);}
function desiredAction({opportunityState,position}){
 const held=Number(position?.tokens)>0;
 if(!held&&['OPPORTUNITY','HOT'].includes(opportunityState))return 'ENTER';
 if(held&&['AVOID','OBSERVED'].includes(opportunityState))return 'EXIT';
 return 'PASS';
}
function decide({decision,observation,account,position,asOf}){
 if(!decision||!observation||!Number(observation.priceUsd)>0)return {action:'PASS',reason:'CURRENT_MARKET_OBSERVATION_REQUIRED'};
 const action=desiredAction({opportunityState:decision.opportunityState,position});
 const evidence={opportunityId:decision.id,opportunityState:decision.opportunityState,opportunityKnowledgeAt:decision.knowledgeAt,marketObservedAt:observation.observedAt,marketKnownAt:observation.knownAt,priceUsd:Number(observation.priceUsd),liquidityUsd:Number(observation.liquidityUsd)||null};
 if(action==='PASS')return {action,reason:'NO_PAPER_PORTFOLIO_TRANSITION',evidence};
 if(action==='ENTER'){const budget=Math.min(Number(account.cashUsd),CONFIG.startingCashUsd*CONFIG.maxPositionPct);if(budget<=0)return {action:'PASS',reason:'NO_PAPER_CASH',evidence};const fillPrice=applyCost(evidence.priceUsd,'BUY'),tokens=budget/fillPrice;return {action,side:'BUY',notionalUsd:budget,tokens,fillPrice,evidence};}
 const tokens=Number(position.tokens),fillPrice=applyCost(evidence.priceUsd,'SELL');return {action,side:'SELL',notionalUsd:tokens*fillPrice,tokens,fillPrice,evidence};
}
class PaperAccount{
 constructor({store,now=()=>new Date()}){this.store=store;this.now=now;}
 async snapshot(){const events=await this.store.list('paper','*',{ascending:true,limit:10000}),account={startingCashUsd:CONFIG.startingCashUsd,cashUsd:CONFIG.startingCashUsd,realizedPnlUsd:0,positions:{},trades:0};
  for(const e of events){if(e.side==='BUY'){account.cashUsd-=e.notionalUsd;account.positions[e.tokenAddress]={tokens:e.tokens,costUsd:e.notionalUsd,entryFillPrice:e.fillPrice,enteredAt:e.knownAt};account.trades++;}
   if(e.side==='SELL'&&account.positions[e.tokenAddress]){const p=account.positions[e.tokenAddress];account.cashUsd+=e.notionalUsd;account.realizedPnlUsd+=e.notionalUsd-p.costUsd;delete account.positions[e.tokenAddress];account.trades++;}}
  return account;
 }
 async evaluate(decision,observation){const account=await this.snapshot(),position=account.positions[decision.tokenAddress],asOf=this.now().toISOString(),d=decide({decision,observation,account,position,asOf});
  if(!d.side)return {created:false,decision:d,account};
  const id=hash([VERSION,decision.id,d.side,d.evidence.marketKnownAt]);const existing=(await this.store.list('paper','*',{ascending:false,limit:10000})).find(x=>x.id===id);if(existing)return {created:false,decision:d,account};
  const event={id,version:VERSION,dataClass:'PAPER_EXECUTION',tokenAddress:decision.tokenAddress,action:d.action,side:d.side,notionalUsd:d.notionalUsd,tokens:d.tokens,fillPrice:d.fillPrice,feeBps:CONFIG.feeBps,slippageBps:CONFIG.slippageBps,evidence:d.evidence,occurredAt:asOf,knownAt:asOf,provenance:{mode:'PROSPECTIVE_PAPER',decisionId:decision.id,marketObservationKnownAt:d.evidence.marketKnownAt}};
  await this.store.put('paper','*',id,event);return {created:true,event,account:await this.snapshot()};
 }
}
module.exports={VERSION,CONFIG,PaperAccount,decide,desiredAction,applyCost};
