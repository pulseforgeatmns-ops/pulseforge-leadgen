'use strict';
const {mapGramMessage}=require('./telegramAdapter');
const {toFeedCall}=require('./normalize');
const {extractSolanaContractAddresses}=require('../../packages/signal-v1/prospective/caExtraction');
const VERSION='signal-historical-backfill-v1.0.0';
async function fetchHistoricalPage(client,entity,{offsetId=0,limit=100}={}){
 const batch=await client.getMessages(entity,{offsetId,limit:Math.min(100,Math.max(1,limit))});
 return (batch||[]).map(m=>mapGramMessage(m,entity)).filter(Boolean).sort((a,b)=>b.messageId-a.messageId);
}
function historicalRecord(msg,source){
 const addresses=extractSolanaContractAddresses(msg.text||'');if(!addresses.length)return null;
 const call=toFeedCall({...msg,sourceId:source.sourceId,clusterId:source.clusterId||null,sourceRole:source.sourceRole||'UNKNOWN',clusterRelationshipStatus:source.clusterRelationshipStatus||'UNKNOWN',universeVersion:source.universeVersion||null,ingestedAt:new Date()});
 if(!call)return null;
 return {dataClass:'HISTORICAL_BACKFILL',backfillVersion:VERSION,sourceId:source.sourceId,channelId:String(msg.channelId),messageId:msg.messageId,occurredAt:msg.occurredAt.toISOString(),historicalKnowledgeAt:null,knowledgeAtStatus:'UNOBSERVABLE_HISTORICALLY',addresses,text:msg.text,url:msg.url||null,forwarding:msg.forwarding||null,editAt:msg.editDate?msg.editDate.toISOString():null};
}
async function backfillSource(client,entity,source,{maxMessages=1000,startOffsetId=0,onRecord=async()=>{}}={}){
 let offsetId=startOffsetId||0,scanned=0,caBearing=0,oldestMessageId=null;
 while(scanned<maxMessages){const page=await fetchHistoricalPage(client,entity,{offsetId,limit:Math.min(100,maxMessages-scanned)});if(!page.length)break;
  for(const msg of page){scanned++;oldestMessageId=msg.messageId;const record=historicalRecord(msg,source);if(record){caBearing++;await onRecord(record);}}
  const next=Math.min(...page.map(x=>x.messageId));if(!Number.isFinite(next)||next<=1||next===offsetId)break;offsetId=next;
 }
 return {sourceId:source.sourceId,dataClass:'HISTORICAL_BACKFILL',scanned,caBearing,oldestMessageId,nextOffsetId:offsetId,complete:scanned<maxMessages};
}
module.exports={VERSION,fetchHistoricalPage,historicalRecord,backfillSource};
