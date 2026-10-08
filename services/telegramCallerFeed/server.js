'use strict';

require('dotenv').config();

const express = require('express');
const { createTelegramCallerFeedEngine } = require('./engine');
const { buildFeedPayload } = require('./normalize');
const { loadTelegramCredentials } = require('./credentials');
const {authorized,minimalEvents}=require('./operatorEvents');
const {operationFromEnv}=require('../signalOperator/operationGate');
const {checkPilot}=require('../signalOperator/pilotGate');
const {readRuntimeLimits,installPilotDeadline}=require('../signalOperator/runtimeLimits');
const fs=require('node:fs');

const PORT = Number(process.env.TELEGRAM_CALLER_FEED_PORT || process.env.PORT || 3099);

function feedAuthConfigured(env = process.env) {
  const token = env.SIGNAL_OPERATOR_FEED_TOKEN;
  return typeof token === 'string' && token.length >= 32;
}

function createApp(engine = createTelegramCallerFeedEngine(), options={}) {
  const app = express();
  app.disable('x-powered-by');
  const env=options.env || process.env;
  const gate=options.gate || operationFromEnv(env);
  app.get('/runtime-limits',(req,res)=>{
    if(!authorized(req.headers.authorization,env.SIGNAL_OPERATOR_FEED_TOKEN))return res.sendStatus(401);
    return res.json({...readRuntimeLimits(),configuredSourceCount:require('./sources').loadConfiguredSources().length});
  });
  app.post('/pilot-readiness',express.json({limit:'4kb'}),(req,res)=>{
    if(env.SIGNAL_OPERATION_MODE!=='pilot')return res.sendStatus(404);
    if(!authorized(req.headers.authorization,env.SIGNAL_OPERATOR_FEED_TOKEN))return res.sendStatus(401);
    const body=req.body;
    if(!env.SIGNAL_PILOT_READINESS_PATH || !Number.isFinite(Date.parse(env.SIGNAL_PILOT_START_AT))
      || body?.pilotStartAt!==new Date(env.SIGNAL_PILOT_START_AT).toISOString())return res.sendStatus(409);
    let prior;try{prior=JSON.parse(fs.readFileSync(env.SIGNAL_PILOT_READINESS_PATH,'utf8'));}catch{}
    if(prior?.disabled && prior.pilotStartAt===body.pilotStartAt && !body.disabled)return res.sendStatus(409);
    const fields=['projectId','environmentId','pilotStartAt','observedAt','resourceLimitsVerified','feedCpu','feedMemoryBytes','feedVolumeGB','feedReplicas','privateOnly','stopMechanismVerified','incrementalSpendUsd'];
    const ready=body.disabled===true?{disabled:true,pilotStartAt:body.pilotStartAt}:Object.fromEntries(fields.map(k=>[k,body[k]]));
    if(!ready.disabled && !checkPilot({startAt:env.SIGNAL_PILOT_START_AT,expiresAt:env.SIGNAL_PILOT_EXPIRES_AT,readiness:ready}).ok)return res.sendStatus(422);
    try{const file=env.SIGNAL_PILOT_READINESS_PATH;fs.mkdirSync(require('node:path').dirname(file),{recursive:true});fs.writeFileSync(file+'.tmp',JSON.stringify(ready),{mode:0o600});fs.renameSync(file+'.tmp',file);}
    catch{return res.sendStatus(503);}
    return res.json({stored:true});
  });
  app.get('/operator-events',(req,res)=>{
    if(env.SIGNAL_OPERATOR_FEED_ENABLED!=='1')return res.sendStatus(404);
    if(!authorized(req.headers.authorization,env.SIGNAL_OPERATOR_FEED_TOKEN))return res.sendStatus(401);
    const readiness=gate();if(!readiness.ok)return res.status(503).json({error:readiness.reason});
    const health=engine.getHealth();
    const source=health.sources?.find(s=>s.sourceId==='telegram-front-runners'
      && String(s.channelId)===String(env.SIGNAL_REQUIRED_CALLER_CHANNEL_ID) && s.active && s.available);
    const age=Date.now()-Date.parse(health.lastSuccessfulPoll);
    if(!source || !Number.isFinite(age) || age<0 || age>45000)return res.status(503).json({error:'source_not_ready'});
    return res.json({events:minimalEvents(engine.getRecentCalls(),env.SIGNAL_REQUIRED_CALLER_CHANNEL_ID,{startAt:readiness.startAt})});
  });

  app.get('/health', async (req, res) => {
    const creds = engine.credentialsStatus();
    let poll = { connected: false, errors: [] };
    if (creds.ok) {
      poll = await engine.pollOnce();
    }
    const health = {
      ...engine.getHealth({
        connected: creds.ok && poll.connected,
        errors: poll.errors,
      }),
      feedAuthConfigured: feedAuthConfigured(env),
    };
    const status = creds.ok ? 200 : 503;
    res.status(status).json(health);
  });

  app.get('/feed', async (req, res) => {
    if (!feedAuthConfigured(env)) return res.sendStatus(503);
    if (!authorized(req.headers.authorization, env.SIGNAL_OPERATOR_FEED_TOKEN)) return res.sendStatus(401);
    const creds = loadTelegramCredentials();
    if (!creds.ok) {
      return res.status(503).json({
        error: 'credentials_missing',
        message: creds.reason,
        calls: [],
      });
    }
    const poll = await engine.pollOnce();
    const calls = engine.getRecentCalls();
    const health = engine.getHealth({ connected: poll.connected, errors: poll.errors });
    res.json(buildFeedPayload(calls, health));
  });

  app.get('/sources', (req, res) => {
    const health = engine.getHealth();
    res.json({ sources: health.sources || [] });
  });

  return app;
}

async function main() {
  const creds = loadTelegramCredentials();
  if (!creds.ok) {
    console.error(`[telegram-caller-feed] fail closed: ${creds.reason}`);
    process.exit(1);
  }
  if (!feedAuthConfigured()) {
    console.error('[telegram-caller-feed] fail closed: feed_auth_not_configured');
    process.exit(1);
  }
  const engine = createTelegramCallerFeedEngine();
  engine.startPolling();
  const app = createApp(engine);
  const server=app.listen(PORT, () => {
    console.log(`[telegram-caller-feed] listening on ${PORT}`);
  });
  if(process.env.SIGNAL_OPERATION_MODE==='pilot' && process.env.SIGNAL_PILOT_REQUIRED==='1')installPilotDeadline({expiresAt:process.env.SIGNAL_PILOT_EXPIRES_AT,onExpire:()=>{
    engine.stopPolling();server.close();process.exit(0);
  }});
}

if (require.main === module) {
  main().catch(err => {
    console.error('[telegram-caller-feed] fatal:', err.message);
    process.exit(1);
  });
}

module.exports = {
  createApp,
  createTelegramCallerFeedEngine,
  feedAuthConfigured,
};
