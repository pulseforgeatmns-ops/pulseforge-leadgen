'use strict';
const fs=require('node:fs');
const path=require('node:path');
const {DistributionStore}=require('./store');
const {DeliveryStore}=require('./deliveryStore');
const {createDistributionWorker}=require('./worker');
const {GeckoTerminalDistributionProvider}=require('./marketProvider');
const {configureSharedRequestBudget}=require('./requestBudget');
const {createDistributionDispatcher,createSmsAdapter,createEmailAdapter}=require('./dispatch');
let runtime=null;
function validateConfiguration(env) {
  if(env.SIGNAL_DISTRIBUTION_ENABLED!=='1')return {enabled:false};
  if(env.SIGNAL_OPERATOR_ENABLED!=='1'||!/^\d+$/.test(env.SIGNAL_DISTRIBUTION_TENANT_ID||'')
    ||env.SIGNAL_DISTRIBUTION_SHARED_BUDGET_APPROVED!=='1')throw new Error('distribution_activation_not_approved_or_scoped');
  const maxActive=Number(env.SIGNAL_DISTRIBUTION_MAX_ACTIVE||1);
  const milestoneMultiple=Number(env.SIGNAL_DISTRIBUTION_MILESTONE_MULTIPLE||2);
  if(!Number.isInteger(maxActive)||maxActive!==1||!Number.isFinite(milestoneMultiple)||milestoneMultiple<1)throw new Error('invalid_distribution_limits');
  return {enabled:true,maxActive,milestoneMultiple};
}
async function startDistribution({pool,channelId,gate,env=process.env,fetchImpl=globalThis.fetch,setInterval:interval=setInterval,clearInterval:clear=clearInterval}={}) {
  const config=validateConfiguration(env);if(!config.enabled)return null;
  if(runtime)return runtime;
  if(env.SIGNAL_DISTRIBUTION_APPLY_MIGRATIONS==='1') {
    for(const file of ['2026-10-14-signal-distribution.sql','2026-10-14-signal-distribution-delivery.sql'])
      await pool.query(fs.readFileSync(path.join(__dirname,'../../migrations',file),'utf8'));
  }
  // No automatic fixture/research evaluation. A missing approved schema blocks startup.
  await pool.query('SELECT id FROM signal_distribution_monitors LIMIT 0');
  await pool.query('SELECT id FROM signal_distribution_deliveries LIMIT 0');
  await pool.query('SELECT id FROM signal_distribution_transitions LIMIT 0');
  await pool.query('SELECT id FROM signal_distribution_observations LIMIT 0');
  await pool.query('SELECT id FROM signal_distribution_admissions LIMIT 0');
  await pool.query('SELECT id FROM signal_distribution_request_attempts LIMIT 0');
  const quota=await pool.query('SELECT id FROM signal_distribution_request_budget WHERE id=1');
  if(quota.rowCount!==1)throw new Error('distribution_budget_not_initialized');
  const tenantId=env.SIGNAL_DISTRIBUTION_TENANT_ID;
  const active=await pool.query("SELECT count(*)::int AS count FROM signal_distribution_monitors WHERE tenant_id=$1 AND namespace='operational' AND expires_at>clock_timestamp()",[tenantId]);
  if(active.rows[0].count>1)throw new Error('distribution_active_capacity_exceeds_tonight_limit');
  const store=new DistributionStore(pool,{tenantId,channelId,maxActive:config.maxActive,milestoneMultiple:config.milestoneMultiple});
  const deliveryStore=new DeliveryStore(pool);
  const enabled=channel=>env['SIGNAL_DISTRIBUTION_'+channel.toUpperCase()+'_ENABLED']==='1'&&gate().ok;
  const consented=channel=>env['SIGNAL_DISTRIBUTION_'+channel.toUpperCase()+'_CONSENT']==='1';
  const adapters=[];
  if(enabled('sms')&&consented('sms')) {
    if(!env.TWILIO_ACCOUNT_SID||!env.TWILIO_AUTH_TOKEN||!env.TWILIO_PHONE_NUMBER||!env.OPS_ALERT_PHONE)throw new Error('distribution_sms_not_configured');
    const client=require('twilio')(env.TWILIO_ACCOUNT_SID,env.TWILIO_AUTH_TOKEN,{timeout:5000});
    adapters.push(createSmsAdapter({client,from:env.TWILIO_PHONE_NUMBER,destination:env.OPS_ALERT_PHONE,enabled:()=>enabled('sms'),consented:()=>consented('sms')}));
  }
  if(enabled('email')&&consented('email')) {
    if(!env.BREVO_API_KEY)throw new Error('distribution_email_not_configured');
    // Existing approved Signal operational mailbox pair, not outreach recipients.
    const {SENDER,RECIPIENT}=require('../signalOperator/relay');
    adapters.push(createEmailAdapter({enabled:()=>enabled('email'),consented:()=>consented('email'),sendMail:async message=>{
      const response=await fetchImpl('https://api.brevo.com/v3/smtp/email',{method:'POST',redirect:'error',signal:AbortSignal.timeout(5000),
        headers:{'api-key':env.BREVO_API_KEY,'Content-Type':'application/json'},body:JSON.stringify({sender:{email:SENDER},to:[{email:RECIPIENT}],subject:message.subject,textContent:message.textContent,headers:{'X-Signal-Distribution-Id':message.id}})});
      if(!response.ok)throw new Error('distribution_email_unknown');
      const body=await response.json();return {accepted:true,receipt:body.messageId};
    }}));
  }
  configureSharedRequestBudget({consume:lane=>store.consume(lane),backoff:until=>store.backoff(until)});
  const worker=createDistributionWorker({store,gate,createProvider:()=>new GeckoTerminalDistributionProvider()});
  const dispatcher=createDistributionDispatcher({store:deliveryStore,adapters,enabled,consented});
  let sending=false;
  const tick=async()=>{
    if(sending||!gate().ok)return;sending=true;
    try {
      await deliveryStore.syncTransitions({tenantId,namespace:'operational'});
      for(const channel of ['sms','email'])await dispatcher.sendOnce({tenantId,namespace:'operational',channel});
    }finally{sending=false;}
  };
  const safe=fn=>()=>Promise.resolve().then(fn).catch(()=>{console.error('[signal-distribution] background attempt blocked');});
  const timers=[interval(safe(()=>worker.pollOnce()),5000),interval(safe(tick),5000)];
  timers.forEach(t=>t.unref?.());
  runtime={store,deliveryStore,worker,dispatcher,register:event=>store.register(event),stop:()=>timers.forEach(clear)};
  return runtime;
}
function getStore(){if(!runtime)throw new Error('distribution_not_running');return runtime.store;}
function getDeliveryStore(){if(!runtime)throw new Error('distribution_not_running');return runtime.deliveryStore;}
module.exports={startDistribution,getStore,getDeliveryStore,validateConfiguration};
