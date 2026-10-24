'use strict';
const {spawn}=require('node:child_process');const path=require('node:path');
const child=spawn('python3',[path.join(__dirname,'../../../scripts/reliability/stage-worker-launcher.py'),String(process.pid),process.execPath,path.join(__dirname,'supervisedWorker.js')],{stdio:['ignore','ignore','ignore','ipc']});
child.on('message',message=>{if(message.type==='ready'){child.send({type:'run',tenantId:'10'});process.send({pid:child.pid});}});
