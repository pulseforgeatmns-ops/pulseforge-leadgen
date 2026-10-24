'use strict';
// Hermetic fixture. Never imports production application modules.
process.on('message',message=>{
 if(message.behavior==='crash'){process.exit(2);return;}
 if(message.behavior==='disconnect'){process.disconnect();setInterval(()=>{},1000);return;}
 if(message.behavior==='hang') {process.on('SIGTERM',()=>{});setInterval(()=>{},1000);return;}
 process.send({type:'terminal',runId:message.behavior==='mismatch'?'wrong':message.runId,tenantId:message.tenantId,receipt:{tenantId:message.tenantId,observed:true}},()=>process.exit(0));
});
