'use strict';
process.on('disconnect',()=>process.exit(74));
process.on('message',message=>{
 if(message.type!=='run')return;
 if(message.tenantId==='10'||message.tenantId==='13'){process.on('SIGTERM',()=>{});setInterval(()=>{},1000);return;}
 process.send({...message,type:'terminal',outcome:{status:'completed',reason:null},evidenceRefs:{integrationIds:['fixture-integration']},proof:{verifiedPoll:true,integrationCount:1}},()=>process.exit(0));
});
process.send({type:'ready'});
