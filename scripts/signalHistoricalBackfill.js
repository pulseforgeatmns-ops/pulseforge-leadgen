'use strict';
const fs=require('node:fs');const path=require('node:path');
const {loadTelegramCredentials}=require('../services/telegramCallerFeed/credentials');
const {loadConfiguredSources}=require('../services/telegramCallerFeed/sources');
const {createGramJsClient,resolvePublicChannel}=require('../services/telegramCallerFeed/telegramAdapter');
const {backfillSource}=require('../services/telegramCallerFeed/historicalBackfill');
const {canonicalize,assertNoProspectiveLeak}=require('../services/signalOpportunities/historicalDataset');
async function main(){const creds=loadTelegramCredentials();if(!creds.ok)throw Error(creds.reason);const client=await createGramJsClient(creds);const root=process.env.SIGNAL_HISTORICAL_BACKFILL_PATH||'/data/historical';fs.mkdirSync(root,{recursive:true});
 const max=Math.max(100,Math.min(100000,Number(process.env.SIGNAL_HISTORICAL_MAX_MESSAGES_PER_SOURCE)||5000));const summary=[];
 try{for(const source of loadConfiguredSources()){let entity;try{entity=await resolvePublicChannel(client,source.username);}catch(e){summary.push({sourceId:source.sourceId,status:'UNAVAILABLE',reason:String(e.message||e)});continue;}
  const file=path.join(root,source.sourceId+'.jsonl'),seen=new Set();if(fs.existsSync(file))for(const line of fs.readFileSync(file,'utf8').split('\n')){if(!line)continue;try{const x=JSON.parse(line);seen.add(String(x.messageId));}catch{}}
  const result=await backfillSource(client,entity,source,{maxMessages:max,onRecord:async r=>{if(seen.has(String(r.messageId)))return;const row=canonicalize(r);assertNoProspectiveLeak(row);fs.appendFileSync(file,JSON.stringify(row)+'\n');seen.add(String(r.messageId));}});summary.push({...result,status:'OK',retainedRecords:seen.size});console.log('[signal-historical-backfill]',JSON.stringify(summary.at(-1)));}
 }finally{try{await client.disconnect();}catch{}}
 fs.writeFileSync(path.join(root,'summary.json'),JSON.stringify({generatedAt:new Date().toISOString(),maxMessagesPerSource:max,sources:summary},null,2));console.log('[signal-historical-backfill-summary]',JSON.stringify(summary));}
main().catch(e=>{console.error('[signal-historical-backfill-error]',e);process.exitCode=1;});
