'use strict';
// Operating model is an explicit deployment choice. Continuous operation never
// reads pilot files, requires Railway management credentials, or adds an expiry.
function operationFromEnv(env=process.env,{now=()=>new Date()}={}) {
  if(env.SIGNAL_OPERATION_MODE==='pilot')return require('./pilotGate').pilotFromEnv(env);
  return () => {
    if(env.SIGNAL_OPERATION_MODE!=='continuous')return {ok:false,reason:'operation_mode_not_selected'};
    const start=Date.parse(env.SIGNAL_OPERATION_START_AT);
    const monthlyBudget=Number(env.SIGNAL_MONTHLY_BUDGET_USD);
    if(!Number.isFinite(start)||start>Number(now()))return {ok:false,reason:'operation_start_invalid'};
    if(!Number.isFinite(monthlyBudget)||monthlyBudget<=0)return {ok:false,reason:'monthly_budget_not_selected'};
    return {ok:true,mode:'continuous',startAt:new Date(start).toISOString(),monthlyBudgetUsd:monthlyBudget,
      budgetEnforcement:'monitoring_only'};
  };
}
module.exports={operationFromEnv};
