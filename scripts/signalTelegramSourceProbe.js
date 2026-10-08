#!/usr/bin/env node
'use strict';
// Run only after action-time authorization, in the existing credential-owning
// service. One read-only probe, no joining, sending, saving or persistent polling.
async function probe() {
  const {TelegramClient}=require('telegram');
  const {StringSession}=require('telegram/sessions');
  const {Logger,LogLevel}=require('telegram/extensions/Logger');
  const {loadTelegramCredentials}=require('../services/telegramCallerFeed/credentials');
  const credentials=loadTelegramCredentials();
  if(!credentials.ok) return {source:'frontrunz',accessible:false,reason:'credentials_missing'};
  const client=new TelegramClient(new StringSession(credentials.sessionString),credentials.apiId,credentials.apiHash,
    {connectionRetries:1,baseLogger:new Logger(LogLevel.NONE)});
  try {
    await client.connect();
    if(!await client.checkAuthorization())return {source:'frontrunz',accessible:false,reason:'session_not_authorized'};
    const entity=await client.getEntity('frontrunz');
    const rows=await client.getMessages(entity,{limit:1});
    return {source:'frontrunz',accessible:true,channelId:String(entity.id),username:entity.username,
      title:entity.title,messageReadSucceeded:true,latestMessageId:rows[0]?.id||null,
      latestMessageAt:rows[0]?.date?new Date(rows[0].date*1000).toISOString():null};
  } catch(err) {
    const reason=['USERNAME_NOT_OCCUPIED','USERNAME_INVALID','CHANNEL_PRIVATE','AUTH_KEY_UNREGISTERED','SESSION_REVOKED','SESSION_EXPIRED','FLOOD_WAIT']
      .find(code=>String(err.errorMessage||err.message).includes(code))||'access_probe_failed';
    return {source:'frontrunz',accessible:false,reason};
  } finally { await client.disconnect(); }
}
if(require.main===module){
  const deadline=setTimeout(()=>{console.log(JSON.stringify({source:'frontrunz',accessible:false,reason:'probe_timeout'}));process.exit(1);},30000);
  probe().then(result=>{clearTimeout(deadline);console.log(JSON.stringify(result));process.exit(result.accessible?0:1);})
    .catch(()=>{clearTimeout(deadline);console.log(JSON.stringify({source:'frontrunz',accessible:false,reason:'probe_initialization_failed'}));process.exit(1);});
}
module.exports={probe};
