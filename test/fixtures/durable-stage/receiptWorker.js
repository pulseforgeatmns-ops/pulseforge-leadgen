'use strict';
process.on('disconnect',()=>process.exit(74));
process.on('message',m=>{
 if(m.type!=='run')return;
 if(m.tenantId==='17'){process.on('SIGTERM',()=>{});setInterval(()=>process.send({type:'invalid'}),1);return;}
 const receipt={...m,type:'terminal',outcome:{status:'completed',reason:null},evidenceRefs:{integrationIds:['fixture-integration']},proof:{verifiedPoll:true,integrationCount:1}};
 if(m.tenantId==='1'){process.send(receipt,()=>process.exit(2));return;}
 if(m.tenantId==='10'){process.send({...receipt,fence:'999'},()=>process.exit(0));return;}
 if(m.tenantId==='13'){process.send(receipt);process.send(receipt,()=>process.exit(0));return;}
 process.send(receipt,()=>process.exit(0));
});
process.send({type:'ready'});
