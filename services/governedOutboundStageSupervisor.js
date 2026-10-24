'use strict';
const {spawn}=require('node:child_process');
const {randomUUID}=require('node:crypto');
const path=require('node:path');
const live=new Map();
let closing=false;
function failure(reason){return Object.assign(Error(reason),{code:reason});}
function bounded(promise,ms=5000){let timer;return Promise.race([promise,new Promise((_,reject)=>{timer=setTimeout(()=>reject(failure('persistence_timeout')),ms);})]).finally(()=>clearTimeout(timer));}
function validateReceipt(message,stage) {
 const outcome=message.outcome;
 if(!outcome||!['completed','held','uncertain'].includes(outcome.status)||
   (outcome.reason!=null&&(typeof outcome.reason!=='string'||!/^[a-z0-9_:-]{1,100}$/.test(outcome.reason))))return null;
 const refs={};const input=message.evidenceRefs;
 if(!input||typeof input!=='object'||Array.isArray(input))return null;
 for(const key of Object.keys(input)){
  if(['cycleId','programId','envelopeId','itemId'].includes(key)){
   if(typeof input[key]!=='string'||!/^[a-zA-Z0-9_-]{1,200}$/.test(input[key]))return null;refs[key]=input[key];
  }else if(key==='integrationIds'){
   if(!Array.isArray(input[key])||input[key].length>50||!input[key].every(x=>typeof x==='string'&&/^[a-zA-Z0-9_-]{1,200}$/.test(x)))return null;refs[key]=[...new Set(input[key])];
  }else return null;
 }
 if(outcome.status==='completed'){
  const proof=message.proof;
  if(stage==='poll'&&!(proof?.verifiedPoll===true&&refs.integrationIds?.length>0&&proof.integrationCount===refs.integrationIds.length))return null;
  if(stage==='tick'&&!(refs.envelopeId&&((proof?.sent===1&&refs.itemId)||(proof?.sent===0&&proof.completed===true))))return null;
  if(stage==='control'&&!(refs.cycleId&&refs.programId&&Number.isFinite(Date.parse(proof?.startedAt))&&Number.isFinite(Date.parse(proof?.completedAt))&&Date.parse(proof.completedAt)>=Date.parse(proof.startedAt)))return null;
 }
 return {outcome:{status:outcome.status,reason:outcome.reason||null},evidenceRefs:refs};
}
async function superviseStage({tenantId,stage,runId,fence,onUncertain,signal,deadlineMs,
 workerPath=path.join(__dirname,'governedOutboundStageWorker.js'),persistenceMs=5000,graceMs=1000}) {
 if(closing)throw Object.assign(failure('worker_supervisor_stopping'),{noWorkerStarted:true});
 if(live.size>=2||[...live.values()].some(x=>x.tenantId===tenantId))throw Object.assign(failure('worker_capacity'),{noWorkerStarted:true});
 if(process.platform!=='linux')throw Object.assign(failure('parent_death_containment_unavailable'),{noWorkerStarted:true});
 const binding={tenantId,stage,runId,fence:String(fence),instanceId:randomUUID()};
 const key=binding.instanceId;
 let child,exitSeen=false,receipt=null,uncertain=false,quarantine=null,deadline,killTimer,heartbeat,ready=false;
 let resolveExit;
 const exited=new Promise(resolve=>{resolveExit=resolve;});
 const quarantineOnce=reason=>{
  if(!quarantine)quarantine=bounded(Promise.resolve().then(()=>onUncertain(reason)),persistenceMs).then(()=>true,()=>false);
  return quarantine;
 };
 const stop=reason=>{
  uncertain=true;quarantineOnce(reason);
  if(child&&!exitSeen&&!killTimer){child.kill('SIGTERM');killTimer=setTimeout(()=>{if(!exitSeen)child.kill('SIGKILL');},graceMs);}
 };
 live.set(key,{tenantId,stop,exited});
 try {
  // Inherit existing runtime config, never serialize credentials through IPC.
  // Worker-mode marker bounds the single existing db.js singleton pool.
  child=spawn('python3',[path.join(__dirname,'../scripts/reliability/stage-worker-launcher.py'),String(process.pid),process.execPath,workerPath],{stdio:['ignore','ignore','ignore','ipc'],env:{...process.env,PF_DURABLE_STAGE_WORKER:'1'}});
  child.on('error',()=>{stop('worker_launch_or_ipc_failure');if(!child.pid){exitSeen=true;resolveExit();}});
  child.once('exit',(code)=>{exitSeen=true;if(code!==0||!receipt)stop('worker_exit_without_receipt');resolveExit();});
  child.on('disconnect',()=>{if(!exitSeen&&!receipt)stop('worker_ipc_lost');});
  child.on('message',message=>{
   if(!message||JSON.stringify(message).length>12000){stop('invalid_worker_receipt');return;}
   if(message.type==='ready'&&!ready){ready=true;child.send({type:'run',...binding},error=>{if(error)stop('worker_send_failed');});return;}
   if(message.type!=='terminal'||receipt||!Object.entries(binding).every(([k,v])=>message[k]===v)||!['completed','held','uncertain'].includes(message.outcome?.status)){stop('invalid_worker_receipt');return;}
   const validated=validateReceipt(message,stage);if(!validated){stop('invalid_stage_evidence');return;}
   if(!uncertain)receipt=validated;
  });
  const onAbort=()=>stop('worker_claim_lost');
  signal?.addEventListener('abort',onAbort,{once:true});if(signal?.aborted)onAbort();
  deadline=setTimeout(()=>stop('worker_deadline_exceeded'),deadlineMs);
  heartbeat=setInterval(()=>{if(child.connected)child.send({type:'parent_heartbeat',instanceId:binding.instanceId},error=>{if(error)stop('worker_heartbeat_failed');});},1000);
  await exited; // Never free process capacity until confirmed exit or no spawn.
  clearTimeout(deadline);clearTimeout(killTimer);clearInterval(heartbeat);signal?.removeEventListener('abort',onAbort);
  if(uncertain||!receipt){const persisted=await quarantineOnce('worker_outcome_uncertain');throw failure(persisted?'worker_outcome_uncertain':'worker_quarantine_unverified');}
  return receipt;
 } finally {
  // A live child cannot escape through an exceptional path.
  if(!exitSeen&&child?.pid){stop('supervisor_exception');await exited;await quarantineOnce('supervisor_exception');}
  clearTimeout(deadline);clearTimeout(killTimer);clearInterval(heartbeat);live.delete(key);
 }
}
async function shutdownStageWorkers(){closing=true;for(const worker of live.values())worker.stop('supervisor_shutdown');await Promise.all([...live.values()].map(x=>x.exited));}
module.exports={superviseStage,shutdownStageWorkers,bounded,validateReceipt};
