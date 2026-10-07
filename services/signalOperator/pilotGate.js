'use strict';
const fs = require('node:fs');
function checkPilot({ startAt, expiresAt, budgetUsd = 2, readiness }, now = new Date()) {
  const start = Date.parse(startAt), end = Date.parse(expiresAt), time = +now;
  if (!Number.isFinite(start) || !Number.isFinite(end) || end <= start || end-start > 48*3600000)
    return {ok:false,reason:'pilot_window_invalid'};
  if (time < start || time >= end) return {ok:false,reason:'pilot_inactive_or_expired'};
  if (!Number.isFinite(budgetUsd) || budgetUsd <= 0 || budgetUsd > 2) return {ok:false,reason:'pilot_budget_invalid'};
  const age = time-Date.parse(readiness?.observedAt);
  if (!Number.isFinite(age) || age < 0 || age > 15*60000) return {ok:false,reason:'usage_readiness_stale'};
  if(readiness?.projectId!=='5f6c50eb-6a61-4649-885e-dc3f3b80a2e5'
    || readiness?.environmentId!=='7046b237-d3a6-4884-8a1e-bd75d809fa3b'
    || Date.parse(readiness?.pilotStartAt)!==start)return {ok:false,reason:'usage_readiness_scope_mismatch'};
  if (readiness?.resourceLimitsVerified !== true || readiness?.feedCpu > .25 || readiness?.feedCpu <= 0
    || readiness?.feedMemoryBytes > 268435456 || readiness?.feedMemoryBytes <= 0
    || readiness?.feedVolumeGB !== 1 || readiness?.feedReplicas !== 1
    || readiness?.privateOnly !== true || readiness?.stopMechanismVerified !== true
    || !Number.isFinite(readiness?.feedCpu) || !Number.isFinite(readiness?.feedMemoryBytes))
    return {ok:false,reason:'resource_readiness_unverified'};
  if (!Number.isFinite(readiness?.incrementalSpendUsd) || readiness.incrementalSpendUsd < 0
    || readiness.incrementalSpendUsd >= budgetUsd) return {ok:false,reason:'pilot_budget_exhausted_or_unknown'};
  return {ok:true,startAt:new Date(start).toISOString(),expiresAt:new Date(end).toISOString()};
}
function pilotFromEnv(env=process.env) {
  return () => {
    let readiness;
    try { readiness=JSON.parse(fs.readFileSync(env.SIGNAL_PILOT_READINESS_PATH,'utf8')); } catch { /* fail closed */ }
    return checkPilot({startAt:env.SIGNAL_PILOT_START_AT,expiresAt:env.SIGNAL_PILOT_EXPIRES_AT,
      budgetUsd:Number(env.SIGNAL_PILOT_BUDGET_USD || 2),readiness});
  };
}
module.exports={checkPilot,pilotFromEnv};
