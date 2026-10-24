'use strict';
// Dedicated entry point: never imports HTTP server, cron registration or timers.
const {identity}=require('./governedOutboundStageStore');
const {classify,evidenceReferences}=require('./governedOutboundStageScheduler');
let binding=null,lastHeartbeat=Date.now();
const watchdog=setInterval(()=>{if(Date.now()-lastHeartbeat>5000)process.exit(74);},1000);
process.on('disconnect',()=>process.exit(74));
process.on('message',async message=>{
 if(message?.type==='parent_heartbeat'){if(!binding||message.instanceId===binding.instanceId)lastHeartbeat=Date.now();return;}
 if(binding||message?.type!=='run')return process.exit(75);
 try {
  identity(message.tenantId,message.stage);
  if(!/^[0-9a-f-]{36}$/.test(message.runId)||!message.instanceId||!/^\d+$/.test(message.fence))throw Error('invalid_binding');
  binding=message;
  const pool=require('../db');
  let result;
  if(message.stage==='poll')result=await require('../anchorDailyOutboundCron').poll({pool,tenantIds:[message.tenantId],mailboxOnly:message.tenantId==='10'});
  else if(message.stage==='tick')result=await require('../anchorDailyOutboundCron').run({pool,tenantIds:[message.tenantId]});
  else result=await require('./governedOutboundControlDispatch').runBoundedMaxOutboundControl({pool,tenantId:message.tenantId,execute:true,cycleId:message.runId,durableInternal:true});
  const outcome=classify(result,message.stage,message.tenantId),refs=evidenceReferences(result,message.stage);
  const proof=message.stage==='poll'?{verifiedPoll:outcome.status==='completed',integrationCount:refs.integrationIds?.length||0}:message.stage==='tick'?{sent:result.sent,completed:result.completed===true}:{startedAt:result.cycleStartedAt,completedAt:result.cycleCompletedAt};
  await pool.end();clearInterval(watchdog);
  process.send({...binding,type:'terminal',outcome,evidenceRefs:refs,proof},()=>process.exit(0));
 } catch(_){process.exit(76);}
});
process.send({type:'ready'});
