'use strict';
const fs=require('node:fs');
function readRuntimeLimits(read=file=>fs.readFileSync(file,'utf8')) {
  let cpu=null,memoryBytes=null;
  try{const [quota,period]=read('/sys/fs/cgroup/cpu.max').trim().split(/\s+/);if(quota!=='max' && +period>0)cpu=+quota/+period;}catch{}
  if(cpu==null)try{const quota=+read('/sys/fs/cgroup/cpu/cpu.cfs_quota_us'),period=+read('/sys/fs/cgroup/cpu/cpu.cfs_period_us');if(quota>0&&period>0)cpu=quota/period;}catch{}
  try{memoryBytes=Number(read('/sys/fs/cgroup/memory.max').trim());}catch{}
  if(!Number.isFinite(memoryBytes))try{memoryBytes=Number(read('/sys/fs/cgroup/memory/memory.limit_in_bytes').trim());}catch{}
  return {cpu:Number.isFinite(cpu)&&cpu>0?cpu:null,memoryBytes:Number.isFinite(memoryBytes)&&memoryBytes>0?memoryBytes:null};
}
function installPilotDeadline({expiresAt,onExpire,now=()=>new Date(),setTimer=setTimeout,clearTimer=clearTimeout}) {
  const expiry=Date.parse(expiresAt);
  if(!Number.isFinite(expiry))return ()=>{};
  const timer=setTimer(onExpire,Math.max(0,expiry-Number(now())));timer?.unref?.();return ()=>clearTimer(timer);
}
module.exports={readRuntimeLimits,installPilotDeadline};
