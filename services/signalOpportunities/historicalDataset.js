'use strict';
const crypto=require('node:crypto');
const VERSION='signal-historical-dataset-v1.0.0';
function hashUnit(value,salt){const h=crypto.createHash('sha256').update(salt+'|'+value).digest();return h.readUInt32BE(0)/0xffffffff;}
function assignPartition(record,{holdoutFraction=.25,salt='signal-historical-holdout-v1'}={}){
 if(record.dataClass!=='HISTORICAL_BACKFILL')throw Error('historical_backfill_required');
 const key=(record.addresses||[]).slice().sort().join(':')||[record.channelId,record.messageId].join(':');
 return hashUnit(key,salt)<holdoutFraction?'HISTORICAL_HOLDOUT':'HISTORICAL_DISCOVERY';
}
function canonicalize(record,opts={}){return {...record,datasetVersion:VERSION,partition:assignPartition(record,opts),eligibleForLiveScoring:false,knowledgeAt:null,knowledgeAtStatus:'UNOBSERVABLE_HISTORICALLY'};}
function assertNoProspectiveLeak(record){if(record.dataClass==='HISTORICAL_BACKFILL'&&(record.eligibleForLiveScoring||record.knowledgeAt))throw Error('historical_prospective_leak');return true;}
module.exports={VERSION,assignPartition,canonicalize,assertNoProspectiveLeak};
