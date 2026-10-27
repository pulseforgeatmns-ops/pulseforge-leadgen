'use strict';
const VERSION='recommendation-v1.0.0';
const finite=x=>Number.isFinite(Number(x));
function buildRecommendation(position){
 const risk=position?.risk||{}, price=Number(position?.current?.priceUsd), held=Number(position?.accounting?.remainingTokenQuantity?.value||0)>0 || Number(position?.accounting?.remainingCostBasis?.value||0)>0;
 const reasons=(risk.for||[]).filter(x=>!/UNKNOWN|UNAVAILABLE/.test(x)), unknown=[...(risk.unknown||[]),...(risk.for||[]).filter(x=>/UNKNOWN|UNAVAILABLE/.test(x))];
 const evidenceQuality=unknown.length===0?'STRONG':reasons.length>=2?'PARTIAL':'LIMITED';
 const trigger=finite(price)&&price>0?price*.9:null;
 let action='NO_ACTION_RECOMMENDATION',headline='No action recommendation yet',instruction='Signal does not have enough evidence to recommend changing the position right now.';
 let conditional=trigger?{priceUsd:trigger,percentFromNow:-10,action:'REASSESS',instruction:'If price falls another 10% from here, Signal recommends reassessing immediately against selling pressure and pool-value evidence.'}:null;
 if(held&&risk.state==='CRITICAL'&&reasons.length>=2){action='EXIT';headline='Signal recommends exiting';instruction='The current risk evidence is severe enough that Signal recommends exiting the remaining observed position.';conditional=null;}
 else if(held&&risk.state==='HIGH'&&reasons.length>=2){action='DE_RISK';headline='Signal recommends reducing exposure';instruction='Signal recommends reducing the remaining position now rather than waiting for a full invalidation.';if(conditional)conditional={...conditional,action:'EXIT',instruction:'If price falls another 10% from here and elevated distribution evidence persists, Signal recommends exiting the remaining position.'};}
 else if(held&&risk.state==='WATCH'&&reasons.length){action='HOLD_WITH_EXIT_TRIGGER';headline='Signal recommends holding with a defined exit trigger';instruction='Signal sees warning signs, but the current evidence does not justify an immediate exit.';if(conditional)conditional={...conditional,action:'EXIT_IF_CONFIRMED',instruction:'If price falls another 10% from here and selling pressure or pool deterioration confirms the move, Signal recommends exiting the remaining position.'};}
 else if(held&&risk.state==='NORMAL'){action='HOLD';headline='Signal recommends holding';instruction='No elevated distribution warning is currently established in the evidence Signal can observe.';}
 return {version:VERSION,action,headline,instruction,conditional,evidenceQuality,riskState:risk.state||'UNKNOWN',reasons,unknown,knownAt:position?.asOf||position?.current?.knownAt||null,mode:'RESEARCH_RECOMMENDATION'};
}
module.exports={VERSION,buildRecommendation};
