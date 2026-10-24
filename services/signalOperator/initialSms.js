'use strict';
const {createHash,randomUUID}=require('node:crypto');
const {performance}=require('node:perf_hooks');
const {buildOperatorAlert}=require('../../packages/signal-v1/operator/alertOutbox');
const {extractSolanaContractAddresses}=require('../../packages/signal-v1/prospective/caExtraction');
const hash=value=>createHash('sha256').update(JSON.stringify(value)).digest('hex');
const PHONE=/^\+[1-9]\d{7,14}$/;
function configuration(env) {
  if(env.SIGNAL_OPERATOR_SMS_ENABLED!=='1')return null;
  const tenantId=env.SIGNAL_OPERATOR_TENANT_ID;
  const startsAt=env.SIGNAL_OPERATOR_SMS_WINDOW_START,endsAt=env.SIGNAL_OPERATOR_SMS_WINDOW_END;
  const valid=s=>typeof s==='string'&&/^\d{4}-\d\d-\d\dT.*(?:Z|[+-]\d\d:\d\d)$/.test(s)&&Number.isFinite(Date.parse(s));
  const limit=Number(env.SIGNAL_OPERATOR_SMS_MAX_MESSAGES||5);
  if(!/^\d+$/.test(tenantId||'')||!valid(startsAt)||!valid(endsAt)||Date.parse(endsAt)<=Date.parse(startsAt)
    ||Date.parse(endsAt)-Date.parse(startsAt)>86400000||!Number.isInteger(limit)||limit<0||limit>20
    ||!PHONE.test(env.TWILIO_PHONE_NUMBER||'')||!PHONE.test(env.OPS_ALERT_PHONE||''))throw new Error('initial_sms_configuration_invalid');
  return {tenantId,namespace:'operational',startsAt:new Date(startsAt).toISOString(),endsAt:new Date(endsAt).toISOString(),limit};
}
class InitialSmsStore {
  constructor(pool,{tenantId,namespace='operational',startsAt,endsAt,limit,channelId}) {
    if(!/^\d+$/.test(String(tenantId))||!['operational','test'].includes(namespace)||!/^\d+$/.test(String(channelId))
      ||!Number.isFinite(Date.parse(startsAt))||!Number.isFinite(Date.parse(endsAt))||Date.parse(endsAt)<=Date.parse(startsAt)
      ||Date.parse(endsAt)-Date.parse(startsAt)>86400000||!Number.isInteger(limit)||limit<0||limit>20)throw new Error('initial_sms_scope_invalid');
    Object.assign(this,{pool,tenantId:String(tenantId),namespace,startsAt,endsAt,limit,channelId:String(channelId)});
    this.windowId=hash([this.tenantId,namespace,new Date(startsAt).toISOString(),new Date(endsAt).toISOString()]);
  }
  async initialize() {
    // Match the established distribution outbox's immutable, non-overlapping windows.
    // No schema creation at startup; missing draft schema blocks this lane only.
    const c=await this.pool.connect();
    try {
      await c.query('BEGIN');
      await c.query('SELECT pg_advisory_xact_lock(hashtext($1))',['initial-sms-window:'+this.tenantId+':'+this.namespace]);
      const old=(await c.query('SELECT * FROM signal_initial_sms_windows WHERE id=$1',[this.windowId])).rows[0];
      if(old) {
        if(old.tenant_id!==this.tenantId||old.namespace!==this.namespace||+old.starts_at!==Date.parse(this.startsAt)||+old.ends_at!==Date.parse(this.endsAt)||old.sms_limit!==this.limit)throw new Error('initial_sms_window_immutable');
      } else {
        const now=(await c.query('SELECT clock_timestamp() AS now')).rows[0].now;
        if(Date.parse(this.startsAt)<+now)throw new Error('initial_sms_new_window_must_start_in_future');
        const overlap=await c.query('SELECT id FROM signal_initial_sms_windows WHERE tenant_id=$1 AND namespace=$2 AND ends_at>$3 AND starts_at<$4',[this.tenantId,this.namespace,this.startsAt,this.endsAt]);
        if(overlap.rowCount)throw new Error('initial_sms_window_overlap');
        await c.query('INSERT INTO signal_initial_sms_windows(id,tenant_id,namespace,starts_at,ends_at,sms_limit) VALUES($1,$2,$3,$4,$5,$6)',[this.windowId,this.tenantId,this.namespace,this.startsAt,this.endsAt,this.limit]);
      }
      await c.query('COMMIT');
    }catch(e){await c.query('ROLLBACK');throw e;}finally{c.release();}
  }
  async enqueue(event) {
    if(!new RegExp(`^telegram:${this.channelId}:\\d+$`).test(event.externalMessageId||'')
      ||!extractSolanaContractAddresses(event.extractedCa||'').includes(event.extractedCa))throw new Error('initial_sms_invalid_event');
    const alert=buildOperatorAlert({evidence:event,approvedChannelId:this.channelId});
    const id=hash([this.tenantId,this.namespace,alert.id,'sms']);
    // DB clock, not event text or client clock, establishes active window and freshness.
    const r=await this.pool.query(`INSERT INTO signal_initial_sms_deliveries(id,tenant_id,namespace,window_id,event_id,payload,status)
      SELECT $1,$2,$3,w.id,$5,$6::jsonb,$7 FROM signal_initial_sms_windows w
      WHERE w.id=$4 AND w.tenant_id=$2 AND w.namespace=$3 AND clock_timestamp()>=w.starts_at AND clock_timestamp()<w.ends_at
      AND $8::timestamptz>=w.starts_at AND $8::timestamptz<=clock_timestamp() AND $9::timestamptz<=clock_timestamp()
      AND $10::timestamptz>clock_timestamp()-interval '5 minutes'
      ON CONFLICT(tenant_id,namespace,event_id,channel) DO NOTHING RETURNING id`,
    [id,this.tenantId,this.namespace,this.windowId,alert.id,JSON.stringify(alert),this.namespace==='test'?'TEST_ONLY':'PENDING',alert.occurredAt,alert.ingestedAt,alert.knowledgeAt]);
    return {id,created:r.rowCount===1};
  }
  async claim() {
    if(this.namespace!=='operational')return null;
    await this.pool.query(`UPDATE signal_initial_sms_deliveries SET status='UNKNOWN',lease_until=NULL
      WHERE tenant_id=$1 AND namespace=$2 AND status='SENDING' AND lease_until<=clock_timestamp()`,[this.tenantId,this.namespace]);
    await this.pool.query(`UPDATE signal_initial_sms_deliveries d SET status='EXPIRED' FROM signal_initial_sms_windows w
      WHERE d.window_id=w.id AND d.tenant_id=$1 AND d.namespace=$2 AND d.status='PENDING'
      AND (w.ends_at<=clock_timestamp() OR (d.payload->>'knowledgeAt')::timestamptz<=clock_timestamp()-interval '5 minutes')`,[this.tenantId,this.namespace]);
    const r=await this.pool.query(`WITH candidate AS(SELECT d.id FROM signal_initial_sms_deliveries d JOIN signal_initial_sms_windows w ON w.id=d.window_id
      WHERE d.tenant_id=$1 AND d.namespace=$2 AND d.window_id=$3 AND d.status='PENDING'
      AND clock_timestamp()>=w.starts_at AND clock_timestamp()<w.ends_at
      AND (d.payload->>'knowledgeAt')::timestamptz>clock_timestamp()-interval '5 minutes'
      ORDER BY d.created_at,d.id FOR UPDATE OF d SKIP LOCKED LIMIT 1)
      UPDATE signal_initial_sms_deliveries d SET status='SENDING',claimed_at=clock_timestamp(),lease_until=clock_timestamp()+interval '30 seconds',lease_token=$4
      FROM candidate c WHERE d.id=c.id RETURNING d.*`,[this.tenantId,this.namespace,this.windowId,randomUUID()]);
    return r.rows[0]||null;
  }
  async authorize(row) {
    const c=await this.pool.connect();
    try {
      await c.query('BEGIN');
      const d=(await c.query(`SELECT * FROM signal_initial_sms_deliveries WHERE id=$1 AND tenant_id=$2 AND namespace=$3 AND window_id=$4 FOR UPDATE`,[row.id,this.tenantId,this.namespace,this.windowId])).rows[0];
      if(!d||d.status!=='SENDING'||d.lease_token!==row.lease_token||d.authorized_at){await c.query('ROLLBACK');return null;}
      const w=(await c.query(`SELECT *,clock_timestamp() AS now FROM signal_initial_sms_windows WHERE id=$1 FOR UPDATE`,[this.windowId])).rows[0];
      const remainingMs=Math.min(+w.ends_at,+d.lease_until,Date.parse(d.payload.knowledgeAt)+300000)-+w.now;
      if(+w.now<+w.starts_at||remainingMs<=0||w.sms_reserved>=w.sms_limit){
        await c.query("UPDATE signal_initial_sms_deliveries SET status='SUPPRESSED',lease_until=NULL WHERE id=$1",[d.id]);
        await c.query('COMMIT');return null;
      }
      await c.query('UPDATE signal_initial_sms_windows SET sms_reserved=sms_reserved+1 WHERE id=$1',[w.id]);
      await c.query('UPDATE signal_initial_sms_deliveries SET authorized_at=clock_timestamp() WHERE id=$1',[d.id]);
      await c.query('COMMIT');return {remainingMs};
    }catch(e){await c.query('ROLLBACK');throw e;}finally{c.release();}
  }
  async finish(row,status,receipt=null) {
    if(!['ACCEPTED','UNKNOWN'].includes(status)||status==='ACCEPTED'&&(typeof receipt!=='string'||!receipt||receipt.length>512))throw new Error('initial_sms_invalid_receipt');
    const r=await this.pool.query(`UPDATE signal_initial_sms_deliveries SET status=$5,provider_receipt=$6,
      accepted_at=CASE WHEN $5='ACCEPTED' THEN clock_timestamp() ELSE NULL END,lease_until=NULL
      WHERE id=$1 AND tenant_id=$2 AND namespace=$3 AND lease_token=$4 AND status='SENDING'`,[row.id,this.tenantId,this.namespace,row.lease_token,status,receipt]);
    if(!r.rowCount)throw new Error('initial_sms_claim_lost');
  }
}
function createInitialSmsTransport({client,from,destination}) {
  if(!PHONE.test(from||'')||!PHONE.test(destination||'')||typeof client?.messages?.create!=='function')throw new Error('initial_sms_transport_invalid');
  return async(alert,deadline)=>{
    if(!Number.isFinite(deadline)||performance.now()>=deadline)throw new Error('initial_sms_window_closed');
    // Fixed deployment-approved recipient; payload cannot choose a phone or action.
    const body=`Signal CA: ${alert.tokenAddress}\nKnown: ${alert.knowledgeAt}\nEntry risk UNKNOWN. Enrichment pending. Unverified caller. Manual only; no trade.`;
    const result=await client.messages.create({from,to:destination,body});
    if(typeof result?.sid!=='string'||!result.sid||result.sid.length>512)throw new Error('initial_sms_receipt_unknown');
    return result.sid;
  };
}
function createInitialSmsWorker({store,send,enabled,gate}) {
  let busy=false;
  return {async sendOnce(){
    if(busy||!enabled()||!gate().ok)return {skipped:true};busy=true;let row;
    try {
      row=await store.claim();if(!row)return {empty:true};
      if(row.namespace!=='operational'||row.channel!=='sms'||String(row.tenant_id)!==store.tenantId)throw new Error('initial_sms_scope_mismatch');
      const started=performance.now(),authorization=await store.authorize(row);
      if(!authorization)return {suppressed:true};
      if(!enabled()||!gate().ok)throw new Error('initial_sms_gate_closed');
      const receipt=await send(row.payload,started+authorization.remainingMs);
      await store.finish(row,'ACCEPTED',receipt);
      return {accepted:true,received:false,displayed:false};
    }catch(_){if(row)await store.finish(row,'UNKNOWN');return {accepted:false,unknown:true};}
    finally{busy=false;}
  }};
}
async function startInitialSms({pool,channelId,gate,env=process.env,client,deferStart=false,setInterval:interval=setInterval,clearInterval:clear=clearInterval}) {
  const config=configuration(env);if(!config)return null;
  let stopped=false;
  const enabled=()=>!stopped&&env.SIGNAL_OPERATOR_SMS_ENABLED==='1'&&env.SIGNAL_OPERATOR_SMS_CONSENT==='1';
  if(!enabled())return null;
  const store=new InitialSmsStore(pool,{...config,channelId});await store.initialize();
  if(!client){if(!env.TWILIO_ACCOUNT_SID||!env.TWILIO_AUTH_TOKEN)throw new Error('initial_sms_transport_unconfigured');client=require('twilio')(env.TWILIO_ACCOUNT_SID,env.TWILIO_AUTH_TOKEN,{timeout:5000});}
  const send=createInitialSmsTransport({client,from:env.TWILIO_PHONE_NUMBER,destination:env.OPS_ALERT_PHONE});
  const worker=createInitialSmsWorker({store,send,enabled,gate});
  let timer=null;
  const start=()=>{if(stopped||timer!==null)return;timer=interval(()=>worker.sendOnce().catch(()=>console.error('[signal-initial-sms] attempt failed; verify durable state')),1000);timer.unref?.();};
  if(!deferStart)start();
  return {store,worker,start,enqueue:event=>enabled()&&gate().ok?store.enqueue(event):Promise.resolve({skipped:true}),stop:()=>{stopped=true;if(timer!==null)clear(timer);}};
}
module.exports={configuration,InitialSmsStore,createInitialSmsTransport,createInitialSmsWorker,startInitialSms};
