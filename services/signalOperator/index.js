'use strict';
const fs=require('node:fs');
const path=require('node:path');
const {operationFromEnv}=require('./operationGate');
const {OperatorStore}=require('./store');
const {createOperatorWorker,startOperatorTimers}=require('./worker');
const {createBrevoRelay}=require('./relay');
const {GeckoTerminalMarketDataProvider}=require('../../packages/signal-v1/providers/GeckoTerminalMarketDataProvider');
let bootPromise;
async function boot() {
  if(process.env.SIGNAL_OPERATOR_ENABLED!=='1')return null;
  const gate=operationFromEnv();
  // Continuous and optional pilot gates are selected explicitly; no default expiry.
  const token=process.env.SIGNAL_OPERATOR_FEED_TOKEN;
  const channelId=process.env.SIGNAL_REQUIRED_CALLER_CHANNEL_ID;
  if(!/^\d+$/.test(channelId||'') || !token || token.length<32)throw new Error('operator_identity_or_auth_not_ready');
  const pool=require('../../db');
  // Explicit operator enablement is required before these opt-in migrations run.
  await require('../../packages/signal-v1/storage/ensureSignalSchema').ensureSignalSchema(pool);
  for(const name of ['2026-10-07-signal-v1-operator-outbox.sql','2026-10-07-signal-v1-operator-relay.sql'])
    await pool.query(fs.readFileSync(path.join(__dirname,'../../migrations',name),'utf8'));
  let initialSms=null;
  try {initialSms=await require('./initialSms').startInitialSms({pool,channelId,gate,deferStart:true});}
  catch {console.error('[signal-initial-sms] activation blocked; email continues, SMS not ready');}
  let distribution=null;
  try {distribution=await require('../signalDistribution').startDistribution({pool,channelId,gate});}
  catch {console.error('[signal-distribution] activation blocked; existing operator continues without distribution monitor');}
  const sendEnabled=()=>process.env.SIGNAL_OPERATOR_RELAY_ENABLED==='1' && process.env.SIGNAL_OPERATOR_RELAY_CONSENT==='1';
  let worker;
  try {worker=createOperatorWorker({store:new OperatorStore(pool,{channelId}),channelId,gate,sendEnabled,token,
    feedUrl:process.env.SIGNAL_OPERATOR_FEED_URL,
    marketProvider:new GeckoTerminalMarketDataProvider(),
    onInitialEvent:initialSms ? event=>initialSms.enqueue(event) : undefined,
    onEvent:distribution ? event=>distribution.register(event) : undefined,
    relay:createBrevoRelay({enabled:sendEnabled(),consented:sendEnabled(),apiKey:process.env.BREVO_API_KEY,gate})});
  } catch(error) {initialSms?.stop();distribution?.stop();throw error;}
  initialSms?.start();
  const stopOperator=startOperatorTimers(worker);
  return {worker,stop:()=>{stopOperator();initialSms?.stop();distribution?.stop();}};
}
function startSignalOperator() {
  if(!bootPromise)bootPromise=boot().catch(()=>{console.error('[signal-operator] startup blocked; verify approved configuration/readiness');return null;});
  return bootPromise;
}
module.exports={startSignalOperator};
