'use strict';
const { performance } = require('node:perf_hooks');
const { PostgresStageStore, STAGES, identity } = require('./governedOutboundStageStore');
const active = new Set();
const admissions = new WeakMap();
const admissionStates=new Set();
let admissionClosed=false;
// Reserve business-pool headroom. At most two handlers per process/pool, each
// budgeted for existing nested lock/transaction usage; small pools fail closed.
function admissionFor(pool) {
  let state=admissions.get(pool);
  if(!state){
    const capacity=Number(pool.options?.max ?? 10);
    if(!Number.isInteger(capacity)||capacity<4) throw Object.assign(Error('scheduler_pool_capacity'),{code:'scheduler_pool_capacity'});
    state={limit:Math.min(2,Math.floor((capacity-1)/3)),running:0,queue:[],keys:new Set(),tenants:new Set()};admissions.set(pool,state);admissionStates.add(state);
  }
  return state;
}
function enqueue(state,key,task) {
  if(state.keys.has(key)) return null;
  state.keys.add(key);
  const promise=new Promise(resolve=>state.queue.push({key,task,resolve}));
  const pump=()=>{
    while(state.running<state.limit && state.queue.length){
      const index=state.queue.findIndex(job=>!state.tenants.has(job.key.split(':')[0]));
      if(index<0)break;
      const [job]=state.queue.splice(index,1);state.running++;state.tenants.add(job.key.split(':')[0]);
      Promise.resolve().then(job.task).catch(error=>({status:'failed',reason:error.code||'admission_failed'})).then(job.resolve).finally(()=>{state.running--;state.keys.delete(job.key);state.tenants.delete(job.key.split(':')[0]);pump();});
    }
  };
  pump();return promise;
}
function durableSchedulingEnabled() { return process.env.GOVERNED_OUTBOUND_DURABLE_SCHEDULING_ENABLED === 'true'; }
// First scoped rollout is control-only and tenant 10 only. An invalid value must
// never fall back to legacy execution or silently broaden to configured tenants.
function scopedDurableControlTenantIds(tenantIds, env = process.env) {
  const raw = env.GOVERNED_OUTBOUND_DURABLE_CONTROL_TENANT_IDS;
  if (raw == null || raw === '') return [];
  if (raw !== '10') throw Object.assign(Error('invalid_durable_control_tenant_scope'), { code: 'invalid_durable_control_tenant_scope' });
  return tenantIds.map(String).includes('10') ? ['10'] : [];
}
function classify(result, stage, tenantId) {
  const unknown = { status:'uncertain',reason:'missing_terminal_evidence' };
  if (!result || typeof result !== 'object') return unknown;
  const safeHolds = new Set(['no_program','no_enabled_program','source_mission_missing','preparation_only_grant',
    'environment_kill_switch','cap_reached','spacing','shadow_mode','anchor_mailbox_missing',
    'overlap','poll_not_verified_fresh','anchor_recovery_separate_preparation_approval_required']);
  if(result.halted) return {status:safeHolds.has(result.halted)?'held':'uncertain',reason:String(result.halted).slice(0,100)};
  if (result.error || result.success === false || ['failed','uncertain'].includes(result.status)) return {status:'uncertain',reason:'handler_failure_without_no_effect_proof'};
  if (result.skipped || ['blocked','held','skipped','requested','started','queued'].includes(result.status)) {
    return {status:safeHolds.has(result.reason)?'held':'uncertain',reason:String(result.reason||result.status||'handler_not_completed').slice(0,100)};
  }
  if(stage==='poll') {
    if(!Array.isArray(result.results)||!result.results.length) return {status:'held',reason:'no_poll_evidence'};
    const genuine=result.results.every(row=>row && row.success===true && row.skipped===false && !row.halted && !row.error
      && String(row.tenantId)===String(tenantId) && typeof row.integrationId==='string' && row.integrationId.length>0
      && ['fetched','inserted','duplicates','unmatched','replies'].every(key=>Number.isInteger(row[key])&&row[key]>=0));
    if(genuine) return {status:'completed',reason:null};
    const allKnownSkips=result.results.every(row=>row?.skipped===true && ['overlap','not_pollable','credential_unavailable'].includes(row.reason));
    return {status:allKnownSkips?'held':'uncertain',reason:'poll_not_verified'};
  }
  if(stage==='tick') {
    if(result.mode==='shadow') return {status:'held',reason:'shadow_mode'};
    if(typeof result.envelopeId==='string' && ((result.sent===1 && typeof result.itemId==='string') || (result.sent===0 && result.completed===true))) return {status:'completed',reason:null};
    return unknown;
  }
  if(stage==='control') {
    if(typeof result.cycleId==='string' && typeof result.programId==='string' && result.plan && Number.isFinite(Date.parse(result.cycleStartedAt)) && Number.isFinite(Date.parse(result.cycleCompletedAt)) && Date.parse(result.cycleCompletedAt)>=Date.parse(result.cycleStartedAt)) return {status:'completed',reason:null};
    return unknown;
  }
  return unknown;
}
function evidenceReferences(result, stage) {
  const refs={};
  for(const key of ['cycleId','programId','envelopeId','itemId']) {
    if(typeof result?.[key]==='string' && result[key].length<=200) refs[key]=result[key];
  }
  if(stage==='poll' && Array.isArray(result?.results)) refs.integrationIds=[...new Set(result.results.map(r=>r?.integrationId).filter(id=>typeof id==='string'&&id.length<=200))].slice(0,50);
  return refs;
}
async function runStage({store,pool,tenantId,stage,execute=null,logger=console,setTimer=setTimeout,clearTimer=clearTimeout,maxRuntimeMs=null,supervisorOptions={},monotonicNow=()=>performance.now()}) {
  identity(tenantId,stage);
  maxRuntimeMs ??= {poll:120000,tick:300000,control:1200000}[stage];
  if(!Number.isFinite(maxRuntimeMs)||maxRuntimeMs<1000||maxRuntimeMs>3600000) throw Error('invalid_stage_runtime');
  const claim=await store.acquire(tenantId,stage);
  if(claim.status!=='claimed') return claim;
  let timer,renewing=Promise.resolve(), stopped=false, leaseLost=false, started=false;
  const startedMonotonic=monotonicNow();
  const controller=new AbortController();
  const {bounded}=require('./governedOutboundStageSupervisor');
  const onError=()=>{leaseLost=true;};
  claim.db?.on?.('error',onError);
  const pulse=()=>{
    timer=setTimer(()=>{
      renewing=(async()=>{
        if(monotonicNow()-startedMonotonic>=maxRuntimeMs) {
          leaseLost=true;
          controller.abort();
          await bounded(store.quarantine ? store.quarantine(claim,'stage_runtime_exceeded') : store.finish(claim,'uncertain','stage_runtime_exceeded')).catch(()=>{});
          logger.error?.('[durable-outbound-stage-stalled]',{tenantId:claim.tenantId,stage,runId:claim.runId});
          return; // never release/replay a still-running handler
        }
        await bounded(store.renew(claim));
      })().catch(()=>{leaseLost=true;controller.abort();}).then(()=>{if(!stopped&&!leaseLost)pulse();});
    },Math.floor(store.leaseMs/3)); timer?.unref?.();
  };
  const stopHeartbeat=async()=>{stopped=true;clearTimer(timer);await renewing;};
  try {
    // If start COMMIT acknowledgement is uncertain, never invoke the handler.
    // Expired claims with persisted start are quarantined, not guessed safe.
    await bounded(store.start(claim)); started=true; pulse();
    const result=execute ? await execute({tenantId:claim.tenantId,stage,pool,runId:claim.runId,fence:claim.fence,logger}) : await require('./governedOutboundStageSupervisor').superviseStage({...supervisorOptions,tenantId:claim.tenantId,stage,runId:claim.runId,fence:claim.fence,signal:controller.signal,deadlineMs:maxRuntimeMs,onUncertain:reason=>store.quarantine(claim,reason)});
    await stopHeartbeat();
    if(leaseLost) return {status:'uncertain',reason:'lease_or_connection_lost',runId:claim.runId};
    const outcome=execute?classify(result,stage,claim.tenantId):result.outcome;
    await bounded(store.finish(claim,outcome.status,outcome.reason,execute?evidenceReferences(result,stage):result.evidenceRefs));
    return {...outcome,runId:claim.runId,tenantId:claim.tenantId,stage,result};
  } catch(error) {
    await stopHeartbeat();
    // An arbitrary thrown error cannot prove no provider effect. Recovery never
    // retries an in-flight handler, even if its receipt failed to persist.
    let status=error.noWorkerStarted?'held':started?'uncertain':'retry';
    if(!leaseLost) {
      try { await bounded(started && !error.noWorkerStarted && store.quarantine ? store.quarantine(claim,'handler_outcome_unknown') : store.finish(claim,status,started?'handler_outcome_unknown':'pre_execution_failure')); }
      catch (_) { status='uncertain'; } // terminal write itself is not verified
    } else status='uncertain';
    logger.error?.('[durable-outbound-stage]',{tenantId:claim.tenantId,stage,runId:claim.runId,code:error.code||'stage_failure'});
    return {status,reason:status==='uncertain'?'handler_or_persistence_outcome_unknown':'pre_execution_failure',runId:claim.runId};
  } finally {
    await stopHeartbeat();
    try {await claim.release();} catch(_) { /* durable claim remains fenced */ }
    claim.db?.removeListener?.('error',onError);
  }
}
function dispatchStages(options={}) {
  if(admissionClosed)return {durable:true,status:'held',reason:'scheduler_shutdown',requests:[]};
  const pool=options.pool||require('../db');
  const store=options.store||new PostgresStageStore(pool);
  const tenants=options.tenantIds||require('./governedOutboundTenant').parseGovernedOutboundTenantIds();
  const stages=options.stages||Object.keys(STAGES);
  // Validate the full request before starting any work.
  for(const tenant of tenants) for(const stage of stages) identity(tenant,stage);
  const admission=admissionFor(pool);
  const requests=[];
  for(const tenantId of [...new Set(tenants.map(String))]) for(const stage of [...new Set(stages)]) {
    const pending=enqueue(admission,`${tenantId}:${stage}`,()=>runStage({...options,pool,store,tenantId,stage}).catch(error=>{
      options.logger?.error?.('[durable-outbound-stage-admission]',{tenantId,stage,code:error.code||'admission_failed'});
      return {status:'failed',reason:error.code||'admission_failed',tenantId,stage};
    }));
    if(!pending) {requests.push({tenantId,stage,status:'already_requested'});continue;}
    active.add(pending); pending.finally(()=>active.delete(pending));
    requests.push({tenantId,stage,status:'requested'});
  }
  // Honest asynchronous acknowledgement, NOT an admission/completion receipt.
  return {durable:true,status:'requested',requests};
}
async function whenStagesSettled(){return Promise.all([...active]);}
function startDurableClock(options={}) {
  const schedule=options.setInterval||setInterval,cancel=options.clearInterval||clearInterval;
  let stopped=false;
  const cycle=()=>{if(stopped)return;if(!durableSchedulingEnabled()) return {status:'held',reason:'durable_mode_disabled'};return dispatchStages({...options,stages:options.maxControlEnabled?['poll','tick','control']:['poll','tick']});};
  const timer=schedule(cycle,60000);timer.unref?.();
  if(options.autostart!==false)cycle();
  return {cycle,stop(){stopped=true;cancel(timer);},async stopAndDrain(){stopped=true;cancel(timer);admissionClosed=true;
    for(const state of admissionStates){for(const job of state.queue.splice(0)){state.keys.delete(job.key);job.resolve({status:'held',reason:'shutdown_before_claim'});}}
    await require('./governedOutboundStageSupervisor').shutdownStageWorkers();return whenStagesSettled();}};
}
module.exports={durableSchedulingEnabled,scopedDurableControlTenantIds,runStage,dispatchStages,startDurableClock,whenStagesSettled,classify,evidenceReferences};
