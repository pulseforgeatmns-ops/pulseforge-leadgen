'use strict';
const {createHash,randomUUID}=require('node:crypto');
const {extractSolanaContractAddresses}=require('../../packages/signal-v1/prospective/caExtraction');
const {evaluate}=require('./risk');
const hash=value=>createHash('sha256').update(JSON.stringify(value)).digest('hex');
function scope(tenantId,namespace) {
  if(typeof tenantId!=='string'||!/^\d+$/.test(tenantId)||!['operational','test'].includes(namespace))throw new Error('invalid_distribution_scope');
}
class DistributionStore {
  constructor(pool,{tenantId,channelId,namespace='operational',now=()=>new Date(),maxActive=1,ttlMs=86400000,milestoneMultiple=2,activation=null}={}) {
    scope(tenantId,namespace);
    if(!/^\d+$/.test(String(channelId)))throw new Error('invalid_distribution_channel');
    this.pool=pool;this.tenantId=tenantId;this.channelId=String(channelId);this.namespace=namespace;this.now=now;
    this.activation=activation;
    this.maxActive=Math.min(10,Math.max(1,maxActive));this.ttlMs=Math.min(86400000,Math.max(60000,ttlMs));this.milestoneMultiple=milestoneMultiple;
  }
  async transaction(fn) {
    const c=await this.pool.connect();
    try{await c.query('BEGIN');const result=await fn(c);await c.query('COMMIT');return result;}
    catch(err){await c.query('ROLLBACK');throw err;}finally{c.release();}
  }
  async initializeActivation() {
    if(!this.activation)throw new Error('distribution_window_required');
    const a=this.activation;
    return this.transaction(async c=>{
      await c.query('SELECT pg_advisory_xact_lock(hashtext($1))',['distribution-window:'+this.tenantId+':'+this.namespace]);
      const old=(await c.query('SELECT * FROM signal_distribution_activations WHERE id=$1',[a.id])).rows[0];
      if(old) {
        if(old.tenant_id!==this.tenantId||old.namespace!==this.namespace||+old.starts_at!==Date.parse(a.startsAt)||+old.ends_at!==Date.parse(a.endsAt)||old.sms_limit!==a.smsLimit)throw new Error('distribution_window_configuration_changed');
        return old;
      }
      const now=(await c.query('SELECT clock_timestamp() AS now')).rows[0].now;
      if(Date.parse(a.startsAt)<+now)throw new Error('distribution_new_window_must_start_in_future');
      const overlap=await c.query('SELECT id FROM signal_distribution_activations WHERE tenant_id=$1 AND namespace=$2 AND ends_at>$3 AND starts_at<$4',[this.tenantId,this.namespace,a.startsAt,a.endsAt]);
      if(overlap.rowCount)throw new Error('distribution_window_overlap');
      return (await c.query('INSERT INTO signal_distribution_activations(id,tenant_id,namespace,starts_at,ends_at,sms_limit) VALUES($1,$2,$3,$4,$5,$6) RETURNING *',[a.id,this.tenantId,this.namespace,a.startsAt,a.endsAt,a.smsLimit])).rows[0];
    });
  }
  async activeWindow(c=this.pool) {
    if(!this.activation)return null;
    return (await c.query('SELECT *,clock_timestamp() AS db_now FROM signal_distribution_activations WHERE id=$1 AND tenant_id=$2 AND namespace=$3 AND starts_at<=clock_timestamp() AND ends_at>clock_timestamp()',[this.activation.id,this.tenantId,this.namespace])).rows[0]||null;
  }
  async remainingMs() {const a=await this.activeWindow();return a?Math.max(0,+a.ends_at-+a.db_now):0;}
  async shouldApplyBudget() {return !!(await this.activeWindow());}
  async register(event) {
    const window=await this.activeWindow();
    if(!window)return {skipped:true,reason:'activation_window_closed'};
    const now=window.db_now;
    if(event?.sourceId!=='telegram-front-runners'||event.provenance?.dataClass!=='EMPIRICAL'||event.provenance?.synthetic||event.provenance?.testOnly
      ||String(event.provenance?.telegramChannelId)!==this.channelId||!new RegExp('^telegram:'+this.channelId+':\\d+$').test(event.externalMessageId)
      ||!extractSolanaContractAddresses(event.extractedCa||'').includes(event.extractedCa)
      ||!Number.isFinite(Date.parse(event.occurredAt))||!Number.isFinite(Date.parse(event.ingestedAt))
      ||Date.parse(event.ingestedAt)<Date.parse(event.occurredAt)||Date.parse(event.occurredAt)>+now||Date.parse(event.ingestedAt)>+now)throw new Error('invalid_distribution_event');
    if(Date.parse(event.occurredAt)<+window.starts_at)return {skipped:true,reason:'pre_activation_event'};
    const expires=new Date(Math.min(Date.parse(event.occurredAt)+this.ttlMs,+window.ends_at));
    if(+expires<=+now)return {skipped:true,reason:'expired_event'};
    return this.transaction(async c=>{
      await c.query('SELECT pg_advisory_xact_lock(hashtext($1))',['distribution-registration:'+this.tenantId+':'+this.namespace]);
      const currentWindow=await this.activeWindow(c);
      if(!currentWindow)return {skipped:true,reason:'activation_window_closed'};
      const now=currentWindow.db_now;
      const existing=await c.query('SELECT id FROM signal_distribution_monitors WHERE tenant_id=$1 AND namespace=$2 AND token_address=$3 AND expires_at>$4 ORDER BY created_at LIMIT 1',[this.tenantId,this.namespace,event.extractedCa,now]);
      if(existing.rowCount)return {id:existing.rows[0].id,existing:true};
      const count=await c.query('SELECT count(*)::int AS count FROM signal_distribution_monitors WHERE tenant_id=$1 AND namespace=$2 AND expires_at>$3',[this.tenantId,this.namespace,now]);
      if(count.rows[0].count>=this.maxActive) {
        await c.query('INSERT INTO signal_distribution_admissions(id,tenant_id,namespace,token_address,external_message_id,reason,created_at) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING',[hash([this.tenantId,this.namespace,event.externalMessageId,event.extractedCa]),this.tenantId,this.namespace,event.extractedCa,event.externalMessageId,'ACTIVE_CAPACITY_REACHED',now]);
        return {skipped:true,reason:'active_capacity_reached'};
      }
      const id=hash([this.tenantId,this.namespace,event.externalMessageId,event.extractedCa]);
      const initial={riskState:'WATCH',reasons:['BASELINE_PENDING'],coverage:{walletDistribution:'UNKNOWN',developerIdentity:'UNKNOWN'},event};
      const inserted=await c.query('INSERT INTO signal_distribution_monitors(id,tenant_id,namespace,token_address,event_id,state,created_at,expires_at,next_poll_at,activation_id) VALUES($1,$2,$3,$4,$5,$6::jsonb,$7,$8,$7,$9) ON CONFLICT DO NOTHING RETURNING id',[id,this.tenantId,this.namespace,event.extractedCa,event.externalMessageId,JSON.stringify(initial),now,expires,this.activation.id]);
      return {id,created:inserted.rowCount===1};
    });
  }
  async claim() {
    const window=await this.activeWindow();if(!window)return null;
    const now=window.db_now,lease=randomUUID();
    const result=await this.pool.query("WITH candidate AS (SELECT id FROM signal_distribution_monitors WHERE tenant_id=$1 AND namespace=$2 AND activation_id=$5 AND expires_at>clock_timestamp() AND next_poll_at<=$3 AND (lease_until IS NULL OR lease_until<=$3) ORDER BY next_poll_at,created_at,id FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE signal_distribution_monitors m SET lease_until=$3::timestamptz+interval '45 seconds',lease_token=$4 FROM candidate WHERE m.id=candidate.id RETURNING m.*",[this.tenantId,this.namespace,now,lease,this.activation.id]);
    return result.rows[0]||null;
  }
  async observe(row,snapshot) {
    if(snapshot.tokenAddress!==row.token_address)throw new Error('distribution_token_mismatch');
    return this.transaction(async c=>{
      const found=await c.query('SELECT * FROM signal_distribution_monitors WHERE id=$1 AND tenant_id=$2 AND namespace=$3 FOR UPDATE',[row.id,this.tenantId,this.namespace]);
      const window=await this.activeWindow(c);if(!window)throw new Error('distribution_window_closed');
      const current=found.rows[0],now=window.db_now;
      if(!current||current.lease_token!==row.lease_token||+new Date(current.lease_until)<=+now||+new Date(current.expires_at)<=+now)throw new Error('distribution_lease_lost');
      const evaluated=evaluate(current.state,snapshot,{now,milestoneMultiple:this.milestoneMultiple});
      const state=evaluated.ignored?current.state:evaluated.state;
      const transitions=[];
      if(!evaluated.ignored) {
        await c.query('INSERT INTO signal_distribution_observations(id,monitor_id,tenant_id,namespace,observed_at,payload,created_at) VALUES($1,$2,$3,$4,$5,$6::jsonb,$7) ON CONFLICT DO NOTHING',[hash([this.tenantId,this.namespace,row.id,snapshot.observedAt]),row.id,this.tenantId,this.namespace,snapshot.observedAt,JSON.stringify(snapshot),now]);
        const changed=state.riskState!==current.state.riskState||JSON.stringify(state.reasons)!==JSON.stringify(current.state.reasons);
        const kinds=[...(changed?['RISK_TRANSITION']:[]),...(evaluated.milestoneNew?['MILESTONE_2X']:[])];
        for(const kind of kinds) {
          const id=hash([this.tenantId,this.namespace,row.id,snapshot.observedAt,kind,state.riskState,state.reasons]);
          const payload={id,monitorId:row.id,tenantId:this.tenantId,namespace:this.namespace,tokenAddress:row.token_address,kind,previousState:current.state.riskState,state:state.riskState,reasons:state.reasons,observedAt:snapshot.observedAt,metrics:state.metrics,coverage:state.coverage,evidence:{provider:snapshot.provider||null,pairId:snapshot.pairId,providerTimestamp:snapshot.providerTimestamp||null,timestampBasis:'response_received'},version:state.version,milestoneMultiple:this.milestoneMultiple};
          await c.query('INSERT INTO signal_distribution_transitions(id,monitor_id,tenant_id,namespace,payload,created_at) VALUES($1,$2,$3,$4,$5::jsonb,$6) ON CONFLICT DO NOTHING',[id,row.id,this.tenantId,this.namespace,JSON.stringify(payload),now]);transitions.push(payload);
        }
      }
      await c.query("UPDATE signal_distribution_monitors SET state=$2::jsonb,next_poll_at=$3::timestamptz+interval '60 seconds',lease_until=NULL,lease_token=NULL WHERE id=$1",[row.id,JSON.stringify(state),now]);
      return {ignored:evaluated.ignored,transitions,state};
    });
  }
  async failed(row,reason='MARKET_UNAVAILABLE') {
    return this.transaction(async c=>{
      const result=await c.query('SELECT * FROM signal_distribution_monitors WHERE id=$1 AND tenant_id=$2 AND namespace=$3 FOR UPDATE',[row.id,this.tenantId,this.namespace]);
      const window=await this.activeWindow(c);if(!window)return {ignored:true};
      const current=result.rows[0],now=window.db_now;
      if(!current||current.lease_token!==row.lease_token||+new Date(current.lease_until)<=+now||+new Date(current.expires_at)<=+now)return {ignored:true};
      const previous=current.state;
      const state={...previous,lastPollError:reason,lastPollErrorAt:now.toISOString(),
        riskState:previous.riskState==='NORMAL'?'WATCH':previous.riskState,
        reasons:[...new Set([...(previous.reasons||[]),'MARKET_OBSERVATION_UNAVAILABLE'])].sort(),
        coverage:{...previous.coverage,observationFreshness:'STALE'}};
      if(JSON.stringify(state.reasons)!==JSON.stringify(previous.reasons)||state.riskState!==previous.riskState) {
        const id=hash([this.tenantId,this.namespace,row.id,'failure',previous.lastObservedAt||current.created_at]);
        const payload={id,monitorId:row.id,tenantId:this.tenantId,namespace:this.namespace,tokenAddress:row.token_address,kind:'RISK_TRANSITION',previousState:previous.riskState,state:state.riskState,reasons:state.reasons,observedAt:previous.lastObservedAt||null,recordedAt:now.toISOString(),metrics:previous.metrics||{},coverage:state.coverage,version:previous.version||'distribution-v1.0'};
        await c.query('INSERT INTO signal_distribution_transitions(id,monitor_id,tenant_id,namespace,payload,created_at) VALUES($1,$2,$3,$4,$5::jsonb,$6) ON CONFLICT DO NOTHING',[id,row.id,this.tenantId,this.namespace,JSON.stringify(payload),now]);
      }
      await c.query("UPDATE signal_distribution_monitors SET state=$2::jsonb,next_poll_at=$3::timestamptz+interval '60 seconds',lease_until=NULL,lease_token=NULL WHERE id=$1",[row.id,JSON.stringify(state),now]);
      return {state};
    });
  }
  async setManualReference({id,priceUsd,recordedAt,provenance}) {
    if(typeof priceUsd!=='number'||!Number.isFinite(priceUsd)||priceUsd<=0||provenance!=='EXPLICIT_MANUAL_OPERATOR'||typeof recordedAt!=='string'||!Number.isFinite(Date.parse(recordedAt))||Date.parse(recordedAt)<0)throw new Error('invalid_manual_reference');
    // Set once only. It is a price reference, never a position or execution record.
    return this.transaction(async c=>{
      const found=await c.query('SELECT * FROM signal_distribution_monitors WHERE id=$1 AND tenant_id=$2 AND namespace=$3 FOR UPDATE',[id,this.tenantId,this.namespace]);
      const window=await this.activeWindow(c),row=found.rows[0];
      if(!window||!row||row.activation_id!==this.activation.id||+row.expires_at<=+window.db_now||row.state.reference)throw new Error('reference_missing_monitor_or_already_set');
      if(Date.parse(recordedAt)>+window.db_now)throw new Error('invalid_manual_reference');
      await c.query('UPDATE signal_distribution_monitors SET state=state||$2::jsonb WHERE id=$1',[id,JSON.stringify({reference:{priceUsd,recordedAt,provenance,savedAt:window.db_now.toISOString()},referencePeakPriceUsd:null,milestoneReached:false})]);
    });
  }

  async listAdmissions({tenantId,namespace='operational'}) {scope(tenantId,namespace);return (await this.pool.query('SELECT token_address,external_message_id,reason,created_at FROM signal_distribution_admissions WHERE tenant_id=$1 AND namespace=$2 ORDER BY created_at DESC LIMIT 100',[tenantId,namespace])).rows;}
  async listMonitors({tenantId,namespace='operational'}) {scope(tenantId,namespace);return (await this.pool.query('SELECT id,tenant_id,namespace,token_address,state,created_at,expires_at,next_poll_at,activation_id,CASE WHEN activation_id IS NULL OR expires_at<=clock_timestamp() THEN \'EXPIRED\' ELSE \'ACTIVE\' END AS monitoring_status FROM signal_distribution_monitors WHERE tenant_id=$1 AND namespace=$2 ORDER BY created_at DESC LIMIT 100',[tenantId,namespace])).rows;}
  async getMonitor({tenantId,namespace='operational',id}) {scope(tenantId,namespace);return (await this.pool.query('SELECT id,tenant_id,namespace,token_address,state,created_at,expires_at,next_poll_at,activation_id,CASE WHEN activation_id IS NULL OR expires_at<=clock_timestamp() THEN \'EXPIRED\' ELSE \'ACTIVE\' END AS monitoring_status FROM signal_distribution_monitors WHERE tenant_id=$1 AND namespace=$2 AND id=$3',[tenantId,namespace,id])).rows[0]||null;}
  async getTransitions({tenantId,namespace='operational',monitorId}) {scope(tenantId,namespace);return (await this.pool.query('SELECT * FROM signal_distribution_transitions WHERE tenant_id=$1 AND namespace=$2 AND monitor_id=$3 ORDER BY created_at DESC,id LIMIT 100',[tenantId,namespace,monitorId])).rows;}
  async getObservations({tenantId,namespace='operational',monitorId}) {scope(tenantId,namespace);return (await this.pool.query('SELECT observed_at,payload FROM signal_distribution_observations WHERE tenant_id=$1 AND namespace=$2 AND monitor_id=$3 ORDER BY observed_at DESC LIMIT 100',[tenantId,namespace,monitorId])).rows;}
  async consume(lane) {
    if(!['existing','distribution'].includes(lane))throw new Error('invalid_budget_lane');
    return this.transaction(async c=>{
      const gate=await c.query('SELECT blocked_until FROM signal_distribution_request_budget WHERE id=1 FOR UPDATE');
      const now=(await c.query('SELECT clock_timestamp() AS now')).rows[0].now;
      if(!await this.activeWindow(c))return lane==='existing';
      if(!gate.rowCount)throw new Error('distribution_budget_not_initialized');
      if(gate.rows[0].blocked_until&&+new Date(gate.rows[0].blocked_until)>+now)return false;
      const counts=(await c.query("SELECT count(*) FILTER(WHERE attempted_at>$1::timestamptz-interval '60 seconds')::int AS minute,count(*) FILTER(WHERE lane='distribution' AND attempted_at>$1::timestamptz-interval '60 seconds')::int AS distribution_minute,count(*) FILTER(WHERE lane='distribution')::int AS distribution_hour FROM signal_distribution_request_attempts WHERE attempted_at>$1::timestamptz-interval '1 hour'",[now])).rows[0];
      if(counts.minute>=6||(lane==='distribution'&&(counts.distribution_minute>=2||counts.distribution_hour>=75)))return false;
      await c.query('INSERT INTO signal_distribution_request_attempts(lane,attempted_at) VALUES($1,$2)',[lane,now]);
      await c.query("DELETE FROM signal_distribution_request_attempts WHERE attempted_at<$1::timestamptz-interval '2 hours'",[now]);return true;
    });
  }
  async backoff(untilMs) {
    if(!Number.isFinite(untilMs))throw new Error('invalid_budget_backoff');
    await this.pool.query("UPDATE signal_distribution_request_budget SET blocked_until=GREATEST(blocked_until,clock_timestamp()+interval '60 seconds',$1) WHERE id=1",[new Date(untilMs)]);
  }
}
module.exports={DistributionStore,scope};
