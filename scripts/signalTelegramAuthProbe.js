#!/usr/bin/env node
'use strict';
const readline=require('node:readline');
const {TelegramClient}=require('telegram');
const {StringSession}=require('telegram/sessions');
const {Api}=require('telegram/tl');
const {Logger,LogLevel}=require('telegram/extensions/Logger');
const apiId=Number(process.env.TELEGRAM_API_ID),apiHash=process.env.TELEGRAM_API_HASH;
function ask(q){const rl=readline.createInterface({input:process.stdin,output:process.stdout});return new Promise(r=>rl.question(q,a=>{rl.close();r(a.trim())}))}
function name(x){return x?.className||x?.constructor?.name||'UNKNOWN'}
(async()=>{if(!apiId||!apiHash)throw Error('missing Telegram API credentials');const client=new TelegramClient(new StringSession(''),apiId,apiHash,{connectionRetries:3,baseLogger:new Logger(LogLevel.NONE)});try{await client.connect();const phone=await ask('Phone number (international format): ');const sent=await client.invoke(new Api.auth.SendCode({phoneNumber:phone,apiId,apiHash,settings:new Api.CodeSettings({})}));console.log('Telegram response:');console.log('  delivery:',name(sent.type));console.log('  next delivery:',name(sent.nextType));console.log('  retry after seconds:',sent.timeout??'not supplied');console.log('  code length:',sent.type?.length??'not supplied');console.log('No code or phone number has been logged.');}catch(e){console.error('Telegram RPC:',e.errorMessage||e.message);process.exitCode=1;}finally{await client.disconnect().catch(()=>{})}})();
