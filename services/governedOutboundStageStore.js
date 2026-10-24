'use strict';
const { randomUUID } = require('node:crypto');
const { assertGovernedOutboundTenantId } = require('./governedOutboundTenant');
const STAGES = Object.freeze({ poll: 60000, tick: 300000, control: 900000 });
function identity(tenantId, stage) {
  const tenant = assertGovernedOutboundTenantId(tenantId);
  if (!Object.hasOwn(STAGES, stage)) throw Object.assign(Error('unsupported_stage'), { code: 'unsupported_stage' });
  return { tenant, stage, key: Number(tenant) * 10 + Object.keys(STAGES).indexOf(stage) };
}
function lost() { return Object.assign(Error('scheduler_claim_lost'), { code: 'scheduler_claim_lost' }); }
// Bound parent storage resource ownership too. A timed-out connect is destroyed
// if it arrives late; an operation deadline destroys its borrowed connection.
// This is not cancellation/release of a business handler or provider request.
function boundedStorePool(pool,{connectMs=3000,operationMs=4000,queryMs=3000}={}) {
  const connect=()=>new Promise((resolve,reject)=>{
    let expired=false;
    const wait=setTimeout(()=>{expired=true;reject(Error('scheduler_database_connect_timeout'));},connectMs);
    Promise.resolve().then(()=>pool.connect()).then(client=>{
      clearTimeout(wait);if(expired){client.release(true);return;}
      let released=false;
      const release=destroy=>{if(!released){released=true;clearTimeout(lifetime);client.release(destroy);}};
      const lifetime=setTimeout(()=>release(true),operationMs);
      resolve({query:(text,values)=>{if(released)return Promise.reject(Error('scheduler_database_deadline'));return client.query({text,values,query_timeout:queryMs});},release});
    },error=>{clearTimeout(wait);reject(error);});
  });
  return {connect,async query(text,values){const client=await connect();let failed=false;try{return await client.query(text,values);}catch(error){failed=true;throw error;}finally{client.release(failed);}}};
}
// Uses PostgreSQL time, not the worker's wall clock. Row/fence ownership is
// durable; no scheduler connection is held while a handler runs. Expired started
// work is quarantined, never reclaimed, so a stale external operation cannot overlap a retry.
class PostgresStageStore {
  constructor(pool, { leaseMs = 120000, maxAttempts = 3 } = {}) {
    if (!pool?.connect || leaseMs < 3000 || maxAttempts < 1 || maxAttempts > 10) throw Error('invalid_stage_store');
    this.pool = boundedStorePool(pool); this.leaseMs = leaseMs; this.maxAttempts = maxAttempts;
  }
  async health(tenantId) {
    const tenant=assertGovernedOutboundTenantId(tenantId);
    const rows=(await this.pool.query(`SELECT tenant_id,stage,due_at,status,run_id,fence,
      lease_until,execution_started_at,attempts,last_completed_at,last_outcome,reason,updated_at,
      clock_timestamp() AS observed_at FROM governed_outbound_stage_schedule WHERE tenant_id=$1 ORDER BY stage`,[tenant])).rows;
    // No claim tokens, credentials, or inferred business completion. An absent
    // row means never observed, not a successful empty stage.
    return Object.keys(STAGES).map(stage=>rows.find(row=>row.stage===stage)||{tenant_id:tenant,stage,status:'not_observed'});
  }
  async acquire(tenantId, stage) {
    const id = identity(tenantId, stage);
    const db = await this.pool.connect(); let begun = false; let released=false;
    const release = async (destroy=false) => { if(!released) { released=true; db.release(destroy); } };
    try {
      await db.query('BEGIN'); begun = true;
      // No init/DDL here. Missing migration fails closed.
      await db.query('INSERT INTO governed_outbound_stage_schedule(tenant_id,stage) VALUES($1,$2) ON CONFLICT DO NOTHING', [id.tenant, stage]);
      let row = (await db.query('SELECT *, clock_timestamp() AS db_now FROM governed_outbound_stage_schedule WHERE tenant_id=$1 AND stage=$2 FOR UPDATE', [id.tenant, stage])).rows[0];
      const now = new Date(row.db_now).getTime();
      const finishSkip = async (status, reason) => { await db.query('COMMIT'); begun = false; await release(); return { status, reason, tenantId: id.tenant, stage }; };
      if (row.status === 'running') {
        if (new Date(row.lease_until).getTime() > now) return finishSkip('skipped', 'lease_active');
        if (row.execution_started_at) {
          await db.query("UPDATE governed_outbound_stage_runs SET status='uncertain',reason='expired_in_flight',finished_at=clock_timestamp() WHERE run_id=$1 AND tenant_id=$2 AND stage=$3", [row.run_id,id.tenant,stage]);
          await db.query("UPDATE governed_outbound_stage_schedule SET status='uncertain',reason='expired_in_flight',claim_token=NULL,lease_until=NULL,updated_at=clock_timestamp() WHERE tenant_id=$1 AND stage=$2", [id.tenant,stage]);
          return finishSkip('uncertain', 'expired_in_flight');
        }
        // Claim committed but execution never started: safe recovery, bounded.
      } else if (['uncertain','held'].includes(row.status)) return finishSkip(row.status, row.reason);
      if (new Date(row.due_at).getTime() > now) return finishSkip('skipped', 'not_due');
      if (row.attempts >= this.maxAttempts) {
        await db.query("UPDATE governed_outbound_stage_schedule SET status='held',reason='retry_limit',claim_token=NULL,lease_until=NULL WHERE tenant_id=$1 AND stage=$2", [id.tenant,stage]);
        if(row.run_id) await db.query("UPDATE governed_outbound_stage_runs SET status='held',reason='retry_limit',finished_at=clock_timestamp() WHERE run_id=$1 AND tenant_id=$2 AND stage=$3", [row.run_id,id.tenant,stage]);
        return finishSkip('held','retry_limit');
      }
      if (stage === 'tick') {
        const poll = (await db.query("SELECT last_completed_at,last_outcome,status FROM governed_outbound_stage_schedule WHERE tenant_id=$1 AND stage='poll'", [id.tenant])).rows[0];
        if (!poll || poll.status === 'running' || poll.status === 'uncertain' || poll.last_outcome !== 'completed' || now-new Date(poll.last_completed_at).getTime() > 120000) return finishSkip('blocked','poll_not_verified_fresh');
      }
      const token = randomUUID(), runId = row.run_id || randomUUID();
      row = (await db.query("UPDATE governed_outbound_stage_schedule SET status='running',run_id=$3,claim_token=$4,fence=fence+1,attempts=attempts+1,lease_until=clock_timestamp()+$5*interval '1 millisecond',execution_started_at=NULL,reason=NULL,updated_at=clock_timestamp() WHERE tenant_id=$1 AND stage=$2 RETURNING *", [id.tenant,stage,runId,token,this.leaseMs])).rows[0];
      await db.query("INSERT INTO governed_outbound_stage_runs(run_id,tenant_id,stage,due_at,attempt,fence,status) VALUES($1,$2,$3,$4,$5,$6,'claimed') ON CONFLICT(run_id) DO UPDATE SET attempt=EXCLUDED.attempt,fence=EXCLUDED.fence,status='claimed',reason=NULL,finished_at=NULL", [runId,id.tenant,stage,row.due_at,row.attempts,row.fence]);
      await db.query('COMMIT'); begun = false;
      await release();
      return { status:'claimed', tenantId:id.tenant, stage, runId, token, fence:row.fence, attempt:row.attempts, release:async()=>{} };
    } catch(error) { if(begun) await db.query('ROLLBACK').catch(()=>{}); await release(true).catch(()=>{}); throw error; }
  }
  async start(claim) {
    const {tenantId,stage,token,fence,runId}=claim;
    const db=await this.pool.connect();
    let failed=false;
    try {
      await db.query('BEGIN');
      const result=await db.query("UPDATE governed_outbound_stage_schedule SET execution_started_at=clock_timestamp() WHERE tenant_id=$1 AND stage=$2 AND claim_token=$3 AND fence=$4 AND status='running' AND lease_until>clock_timestamp() AND execution_started_at IS NULL RETURNING execution_started_at",[tenantId,stage,token,fence]);
      if(!result.rows.length) throw lost();
      await db.query("UPDATE governed_outbound_stage_runs SET status='running',started_at=$2 WHERE run_id=$1 AND tenant_id=$3 AND stage=$4 AND fence=$5",[runId,result.rows[0].execution_started_at,tenantId,stage,fence]);
      await db.query('COMMIT');
    } catch(error) {failed=true;await db.query('ROLLBACK').catch(()=>{});throw error;} finally {db.release(failed);}
  }
  async renew(claim) {
    const r=await this.pool.query("UPDATE governed_outbound_stage_schedule SET lease_until=clock_timestamp()+$5*interval '1 millisecond',updated_at=clock_timestamp() WHERE tenant_id=$1 AND stage=$2 AND claim_token=$3 AND fence=$4 AND status='running' AND lease_until>clock_timestamp() RETURNING fence",[claim.tenantId,claim.stage,claim.token,claim.fence,this.leaseMs]);
    if(!r.rows.length) throw lost();
  }
  async quarantine(claim,reason) {
    const db=await this.pool.connect();let failed=false;
    try {
      await db.query('BEGIN');
      const row=(await db.query("SELECT status FROM governed_outbound_stage_schedule WHERE tenant_id=$1 AND stage=$2 AND run_id=$3 AND fence=$4 FOR UPDATE",[claim.tenantId,claim.stage,claim.runId,claim.fence])).rows[0];
      if(!row||!['running','uncertain'].includes(row.status))throw lost();
      await db.query("UPDATE governed_outbound_stage_schedule SET status='uncertain',reason=$5,claim_token=NULL,lease_until=NULL,updated_at=clock_timestamp() WHERE tenant_id=$1 AND stage=$2 AND run_id=$3 AND fence=$4",[claim.tenantId,claim.stage,claim.runId,claim.fence,reason]);
      await db.query("UPDATE governed_outbound_stage_runs SET status='uncertain',reason=$5,finished_at=clock_timestamp() WHERE tenant_id=$1 AND stage=$2 AND run_id=$3 AND fence=$4",[claim.tenantId,claim.stage,claim.runId,claim.fence,reason]);
      await db.query('COMMIT');
    } catch(error){failed=true;await db.query('ROLLBACK').catch(()=>{});throw error;} finally {db.release(failed);}
  }
  async finish(claim, outcome, reason=null, evidenceRefs={}) {
    if(!['completed','held','uncertain','retry'].includes(outcome)) throw Error('invalid_stage_outcome');
    const {tenantId,stage,token,fence,runId}=claim;
    const db=await this.pool.connect();
    let failed=false;
    try {
      await db.query('BEGIN');
      const row=(await db.query("SELECT * FROM governed_outbound_stage_schedule WHERE tenant_id=$1 AND stage=$2 AND claim_token=$3 AND fence=$4 AND status='running' AND lease_until>clock_timestamp() FOR UPDATE",[tenantId,stage,token,fence])).rows[0];
      if(!row) throw lost();
      if(outcome==='retry' && row.execution_started_at) throw Error('in_flight_retry_forbidden');
      const terminal=outcome==='completed'||outcome==='held';
      await db.query("UPDATE governed_outbound_stage_runs SET status=$2,reason=$3,evidence_refs=$7::jsonb,finished_at=clock_timestamp() WHERE run_id=$1 AND tenant_id=$4 AND stage=$5 AND fence=$6",[runId,outcome,reason,tenantId,stage,fence,JSON.stringify(evidenceRefs)]);
      // Completed/known policy-held cycles schedule the next observation. Held
      // means the handler returned a safe policy stop, never business success.
      await db.query(`UPDATE governed_outbound_stage_schedule SET status=$3,reason=$4,claim_token=NULL,lease_until=NULL,
        due_at=CASE WHEN $5 THEN clock_timestamp()+$6*interval '1 millisecond' WHEN $3='retry' THEN clock_timestamp()+$7*interval '1 millisecond' ELSE due_at END,
        run_id=CASE WHEN $5 THEN NULL ELSE run_id END,attempts=CASE WHEN $5 THEN 0 ELSE attempts END,
        execution_started_at=CASE WHEN $5 THEN NULL ELSE execution_started_at END,
        last_completed_at=CASE WHEN $8='completed' THEN clock_timestamp() ELSE last_completed_at END,last_outcome=$8,updated_at=clock_timestamp()
        WHERE tenant_id=$1 AND stage=$2`,[tenantId,stage,terminal?'due':outcome,reason,terminal,STAGES[stage],Math.min(60000,1000*2**claim.attempt),outcome]);
      await db.query('COMMIT');
    } catch(error) {failed=true;await db.query('ROLLBACK').catch(()=>{});throw error;} finally {db.release(failed);}
  }
}
module.exports={PostgresStageStore,STAGES,identity,boundedStorePool};
