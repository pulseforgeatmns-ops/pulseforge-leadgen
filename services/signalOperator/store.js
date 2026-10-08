'use strict';
const {createHash}=require('node:crypto');
const {buildOperatorAlert}=require('../../packages/signal-v1/operator/alertOutbox');
const {extractSolanaContractAddresses}=require('../../packages/signal-v1/prospective/caExtraction');
class OperatorStore {
  constructor(pool,{channelId,now=()=>new Date()}={}) {this.pool=pool;this.channelId=channelId;this.now=now;}
  async enqueue(event) {
    const now=this.now();
    if (!/^\d+$/.test(String(this.channelId)) || !new RegExp(`^telegram:${this.channelId}:\\d+$`).test(event.externalMessageId)
      || !extractSolanaContractAddresses(event.extractedCa||'').includes(event.extractedCa)
      || !Number.isFinite(Date.parse(event.occurredAt)) || !Number.isFinite(Date.parse(event.ingestedAt))
      || Date.parse(event.occurredAt)>+now || Date.parse(event.ingestedAt)>+now) throw new Error('invalid_operator_event');
    const evidence={id:createHash('sha256').update(JSON.stringify([event.sourceId,event.externalMessageId,event.extractedCa])).digest('hex'),
      sourceId:event.sourceId,externalMessageId:event.externalMessageId,extractedCa:event.extractedCa,
      occurredAt:event.occurredAt,ingestedAt:event.ingestedAt,provenance:event.provenance};
    const alert=buildOperatorAlert({evidence,approvedChannelId:this.channelId});
    const minimal={...evidence,provenance:{dataClass:'EMPIRICAL',telegramChannelId:String(this.channelId)}};
    const client=await this.pool.connect();
    try {
      await client.query('BEGIN');
      await client.query('INSERT INTO signal_operator_events(id,payload) VALUES($1,$2::jsonb) ON CONFLICT DO NOTHING',[alert.id,JSON.stringify(minimal)]);
      await client.query(`INSERT INTO signal_operator_alert_outbox(id,source_id,external_message_id,token_address,evidence_id,operator_event_id,payload,next_attempt_at)
        VALUES($1,$2,$3,$4,$1,$1,$5::jsonb,$6) ON CONFLICT DO NOTHING`,
      [alert.id,alert.sourceId,alert.externalMessageId,alert.tokenAddress,JSON.stringify(alert),now]);
      await client.query('COMMIT');
      return alert.id;
    } catch(err) {await client.query('ROLLBACK');throw err;} finally {client.release();}
  }
  async claim({since=null}={}) {
    const now=this.now();
    // Never retry past the conservative 10m window (documented provider TTL >=15m).
    await this.pool.query(`UPDATE signal_operator_alert_outbox SET delivery_state='UNKNOWN',lease_until=NULL,last_error='retry_window_expired'
      WHERE operator_event_id IS NOT NULL AND delivery_state IN ('PENDING','SENDING')
      AND first_attempt_at <= $1::timestamptz - interval '10 minutes'`,[now]);
    const result=await this.pool.query(`WITH candidate AS (
      SELECT id FROM signal_operator_alert_outbox WHERE operator_event_id IS NOT NULL
      AND ($2::timestamptz IS NULL OR (payload->>'occurredAt')::timestamptz >= $2)
      AND ((delivery_state='PENDING' AND next_attempt_at <= $1) OR (delivery_state='SENDING' AND lease_until <= $1))
      AND attempts < 6 ORDER BY created_at,id FOR UPDATE SKIP LOCKED LIMIT 1)
      UPDATE signal_operator_alert_outbox o SET delivery_state='SENDING',attempts=o.attempts+1,
      first_attempt_at=COALESCE(first_attempt_at,$1),lease_until=$1::timestamptz+interval '30 seconds'
      FROM candidate WHERE o.id=candidate.id RETURNING o.*`,[now,since]);
    return result.rows[0] || null;
  }
  async setPayload(id,payload,attempt) {
    const result=await this.pool.query("UPDATE signal_operator_alert_outbox SET payload=$2::jsonb WHERE id=$1 AND delivery_state='SENDING' AND attempts=$3",[id,JSON.stringify(payload),attempt]);
    if(!result.rowCount)throw new Error('operator_lease_lost');
  }
  async accepted(id,receipt,attempt) {
    const result=await this.pool.query(`UPDATE signal_operator_alert_outbox SET delivery_state='ACCEPTED',transport_receipt=$2,
      accepted_at=$3,lease_until=NULL,last_error=NULL WHERE id=$1 AND delivery_state='SENDING' AND attempts=$4`,[id,receipt,this.now(),attempt]);
    if(!result.rowCount)throw new Error('operator_lease_lost');
  }
  async failed(row,reason) {
    const terminal=reason==='relay_rejected_or_duplicate' || row.attempts>=6;
    await this.pool.query(`UPDATE signal_operator_alert_outbox SET delivery_state=$2,last_error=$3,
      next_attempt_at=$4,lease_until=NULL WHERE id=$1 AND delivery_state='SENDING' AND attempts=$5`,
    [row.id,terminal?'UNKNOWN':'PENDING',reason,new Date(+this.now()+Math.min(60000,1000*2**row.attempts)),row.attempts]);
  }
  async recordReceipt(id,stage,receipt) {
    if (!['RECEIVED','DISPLAYED'].includes(stage) || typeof receipt!=='string' || !receipt || receipt.length>512) throw new Error('invalid_delivery_receipt');
    const column=stage==='RECEIVED'?'received_at':'displayed_at';
    const client=await this.pool.connect();
    try {
      await client.query('BEGIN');
      const result=await client.query(`UPDATE signal_operator_alert_outbox SET ${column}=COALESCE(${column},$2)
        WHERE id=$1 AND delivery_state='ACCEPTED' RETURNING id`,[id,this.now()]);
      if (!result.rowCount) throw new Error('alert_not_transport_accepted');
      await client.query('INSERT INTO signal_operator_receipts(alert_id,stage,receipt) VALUES($1,$2,$3) ON CONFLICT DO NOTHING',[id,stage,receipt]);
      await client.query('COMMIT');
    }catch(err){await client.query('ROLLBACK');throw err;}finally{client.release();}
  }
}
module.exports={OperatorStore};
