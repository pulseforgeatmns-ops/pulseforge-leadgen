'use strict';
// LOCAL DESIGN PROTOTYPE ONLY. Not imported by any runtime entry point.
// No credentials, provider calls, or database access. Production integration
// requires review of worker pool sizing, durable quarantine and IPC protocol.
const { fork } = require('node:child_process');
const { randomUUID } = require('node:crypto');
function createSupervisor({ workerPath, capacity=2, deadlineMs=100, graceMs=25, onUncertain=async()=>{} }) {
  if(!Number.isInteger(capacity)||capacity<1||capacity>2)throw Error('invalid_capacity');
  let running=0; const queue=[];
  const pump=()=>{
    while(running<capacity && queue.length){
      const job=queue.shift();running++;
      const child=fork(workerPath,[],{stdio:['ignore','ignore','ignore','ipc'],env:{PATH:process.env.PATH}});
      let receipt=null,uncertain=false,exited=false,killTimer;
      const markUncertain=()=>{
        if(uncertain)return; uncertain=true;
        // Quarantine persistence cannot delay termination of this isolated
        // process. If DB acknowledgement fails, caller must report unverified
        // persistence and retain claim until normal expiry quarantine.
        Promise.resolve().then(()=>onUncertain(job.input)).catch(()=>{});
        child.kill('SIGTERM');
        killTimer=setTimeout(()=>{if(!exited)child.kill('SIGKILL');},graceMs);
      };
      const timer=setTimeout(markUncertain,deadlineMs);
      child.on('message',message=>{
        if(message?.type!=='terminal'||message.runId!==job.input.runId||message.tenantId!==job.input.tenantId){markUncertain();return;}
        receipt=message.receipt;
      });
      child.on('disconnect',()=>{if(!receipt&&!exited)markUncertain();});
      child.on('error',()=>markUncertain());
      child.once('exit',(code,signal)=>{
        exited=true;clearTimeout(timer);clearTimeout(killTimer);
        if((code!==0||!receipt)&&!uncertain){uncertain=true;Promise.resolve().then(()=>onUncertain(job.input)).catch(()=>{});}
        // Even a terminal IPC receipt is not a durable business receipt. This
        // prototype reports local execution only; production must persist it.
        const result=uncertain||code!==0||!receipt
          ? {status:'uncertain',workerExited:true,signal:signal||null}
          : {status:'worker_result',workerExited:true,receipt};
        running--;job.resolve(result);pump();
      });
      child.send(job.input,error=>{if(error)markUncertain();});
    }
  };
  return { run(input){
    if(!['1','10','13','17'].includes(input.tenantId))return Promise.reject(Error('invalid_tenant'));
    return new Promise(resolve=>{queue.push({input:{...input,runId:randomUUID()},resolve});pump();});
  } };
}
module.exports={createSupervisor};
