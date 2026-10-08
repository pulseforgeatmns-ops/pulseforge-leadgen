#!/usr/bin/env node
'use strict';
// Offline generator: accepts sanitized probe metadata, never credentials.
const fs = require('node:fs');
function buildActivationBundle(probe,operatingModel=null) {
  if (probe?.source !== 'frontrunz' || probe.username !== 'frontrunz'
    || probe.accessible !== true || probe.messageReadSucceeded !== true
    || !/^\d+$/.test(String(probe.channelId))) {
    throw new Error('A successful frontrunz identity/read probe with actual numeric channel ID is required');
  }
  if(operatingModel && (operatingModel.mode!=='continuous' || !Number.isFinite(operatingModel.monthlyBudgetUsd)
    || operatingModel.monthlyBudgetUsd<=0 || !Number.isFinite(Date.parse(operatingModel.startAt)))) {
    throw new Error('Continuous activation requires the user-selected positive monthly budget and actual start timestamp');
  }
  const operationVariables=operatingModel ? {SIGNAL_OPERATION_MODE:'continuous',
    SIGNAL_OPERATION_START_AT:new Date(operatingModel.startAt).toISOString(),SIGNAL_MONTHLY_BUDGET_USD:String(operatingModel.monthlyBudgetUsd)}
    : {SIGNAL_OPERATION_MODE:'unselected'};
  const sources=[{sourceId:'telegram-front-runners',displayName:'Front Runners',username:'frontrunz',
    expectedChannelId:String(probe.channelId),platform:'telegram',sourceRole:'CALLER',
    clusterRelationshipStatus:'UNKNOWN',collector:'telegram-caller-feed'}];
  return {
    apply:false,approvalRequired:true,
    operatingModel,activationBlocked:!operatingModel,
    projectId:'5f6c50eb-6a61-4649-885e-dc3f3b80a2e5',environmentId:'7046b237-d3a6-4884-8a1e-bd75d809fa3b',
    phase1:{service:'telegramCallerFeed',variables:{...operationVariables,TELEGRAM_CALLER_SOURCES_JSON:JSON.stringify(sources),SIGNAL_REQUIRED_CALLER_CHANNEL_ID:String(probe.channelId)}},
    requiredReadiness:{connected:true,sourceId:'telegram-front-runners',channelId:String(probe.channelId),available:true,active:true},
    phase2:{service:'pulseforge-leadgen',variables:{
      ...operationVariables,
      SIGNAL_CALLER_FEED_URL:'http://${{telegramCallerFeed.RAILWAY_PRIVATE_DOMAIN}}:3099/feed',
      SIGNAL_REQUIRED_CALLER_SOURCE_ID:'telegram-front-runners',SIGNAL_REQUIRED_CALLER_CHANNEL_ID:String(probe.channelId),
      SIGNAL_SHADOW_MODE:'1',SIGNAL_SHADOW_POLL_MS:'60000',SIGNAL_CAPTURE_POLL_MS:'1000',
    }},
    delivery:{enabled:false,destination:'ChatGPT',reason:'Local relay implemented; operating model approval, user-provisioned feed auth and Gmail event activation pending',
      proposedFeedVariables:{SIGNAL_OPERATOR_FEED_ENABLED:'1',SIGNAL_OPERATOR_FEED_TOKEN:'${{pulseforge-leadgen.SIGNAL_OPERATOR_FEED_TOKEN}}'},
      proposedPulseForgeVariables:{SIGNAL_OPERATOR_ENABLED:'1',SIGNAL_OPERATOR_FEED_URL:'http://${{telegramCallerFeed.RAILWAY_PRIVATE_DOMAIN}}:3099/operator-events',
        SIGNAL_OPERATOR_RELAY_ENABLED:'1',SIGNAL_OPERATOR_RELAY_CONSENT:'1'},
      prerequisiteVariables:['SIGNAL_OPERATION_MODE','SIGNAL_OPERATION_START_AT','SIGNAL_MONTHLY_BUDGET_USD','SIGNAL_OPERATOR_FEED_TOKEN'],
    },
  };
}
if(require.main===module){
  try { const file=process.argv[2];if(!file)throw new Error('Provide sanitized source-probe JSON path');
    const model=process.argv[3]?JSON.parse(fs.readFileSync(process.argv[3],'utf8')):null;
    process.stdout.write(JSON.stringify(buildActivationBundle(JSON.parse(fs.readFileSync(file,'utf8')),model),null,2)+'\n');
  }catch(err){console.error(err.message);process.exitCode=1;}
}
module.exports={buildActivationBundle};
