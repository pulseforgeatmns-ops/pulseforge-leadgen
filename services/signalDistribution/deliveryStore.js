'use strict';
const {createHash} = require('node:crypto');
function canonical(value) {
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === 'object') return Object.fromEntries(Object.keys(value).sort().map(k => [k,canonical(value[k])]));
  return value;
}
const hash = value => createHash('sha256').update(JSON.stringify(canonical(value))).digest('hex');
function validateScope({tenantId, namespace, channel}) {
  if (!/^\d+$/.test(String(tenantId || '')) || !['operational','test'].includes(namespace)
    || (channel !== undefined && !['sms','email'].includes(channel))) throw new Error('invalid_delivery_scope');
}
const SEVERE_REASONS = new Set(['SEVERE_PRICE_DRAWDOWN_WITH_DISTRIBUTION_EVIDENCE',
  'SEVERE_PRICE_DRAWDOWN_WITH_SELL_PRESSURE','SEVERE_PRICE_DRAWDOWN_WITH_RESERVE_DECLINE']);
function criticalReasons(event) {
  return [...new Set((event.reasons || []).map(r => typeof r === 'string' ? r : r.code)
    .filter(reason => SEVERE_REASONS.has(reason)))].sort();
}
function newCriticalEvidence(event, prior) {
  if (event.kind !== 'RISK_TRANSITION' || event.state !== 'CRITICAL'
    || event.coverage?.observationFreshness !== 'FRESH' || event.coverage?.pair !== 'PINNED'
    || event.coverage?.sourceFreshness === 'STALE') return false;
  const reasons = criticalReasons(event);
  if (!reasons.length) return false;
  return !prior || prior.state !== 'CRITICAL' || reasons.some(r => !criticalReasons(prior).includes(r));
}
class DeliveryStore {
  constructor(pool, {now = () => new Date(), cooldownMs = 300000} = {}) {
    if (!Number.isFinite(cooldownMs) || cooldownMs < 0) throw new Error('invalid_cooldown');
    this.pool = pool; this.now = now; this.cooldownMs = cooldownMs;
  }
  async enqueue(event, channel) {
    const scope = {tenantId:event.tenantId, namespace:event.namespace, channel}; validateScope(scope);
    if (typeof event.id !== 'string' || !event.id || typeof event.monitorId !== 'string' || !event.monitorId
      || !['RISK_TRANSITION','MILESTONE_2X'].includes(event.kind)) throw new Error('invalid_distribution_event');
    const id = hash([String(event.tenantId), event.namespace, event.id, channel]);
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      // Serialize all channel decisions for a monitor, including concurrent workers.
      await client.query('SELECT pg_advisory_xact_lock(hashtextextended($1,0))',
        [JSON.stringify([String(event.tenantId),event.namespace,event.monitorId,channel])]);
      const existing = await client.query('SELECT id,status FROM signal_distribution_deliveries WHERE id=$1',[id]);
      if (existing.rows[0]) { await client.query('COMMIT'); return existing.rows[0]; }
      // Verify the event belongs to this operational record; callers cannot forge a transition.
      const verified = await client.query(`SELECT payload FROM signal_distribution_transitions
        WHERE id=$1 AND monitor_id=$2 AND tenant_id=$3 AND namespace=$4`,
      [event.id,event.monitorId,String(event.tenantId),event.namespace]);
      if (!verified.rows[0] || hash(verified.rows[0].payload) !== hash(event)) throw new Error('transition_not_persisted');
      const prior = await client.query(`SELECT payload FROM signal_distribution_deliveries
        WHERE tenant_id=$1 AND namespace=$2 AND monitor_id=$3 AND channel=$4
        AND payload->>'kind'='RISK_TRANSITION'
        AND status IN ('PENDING','SENDING','ACCEPTED','UNKNOWN') AND created_at > $5
        ORDER BY created_at DESC,id DESC LIMIT 100`,
      [String(event.tenantId),event.namespace,event.monitorId,channel,new Date(+this.now()-this.cooldownMs)]);
      const previousCritical = prior.rows.map(r=>r.payload).filter(p=>p.kind==='RISK_TRANSITION' && p.state==='CRITICAL');
      const comparison = previousCritical.length ? {state:'CRITICAL',reasons:previousCritical.flatMap(criticalReasons)} : prior.rows[0]?.payload;
      const suppressed = event.kind !== 'MILESTONE_2X' && Boolean(prior.rows.length && !newCriticalEvidence(event, comparison));
      const status = event.namespace === 'test' ? 'TEST_ONLY' : suppressed ? 'SUPPRESSED' : 'PENDING';
      await client.query(`INSERT INTO signal_distribution_deliveries
        (id,tenant_id,namespace,monitor_id,transition_id,channel,payload,status,suppression_reason,created_at)
        VALUES($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9,$10)`,
      [id,String(event.tenantId),event.namespace,event.monitorId,event.id,channel,JSON.stringify(event),status,
        suppressed ? 'cooldown' : null,this.now()]);
      await client.query('COMMIT'); return {id,status};
    } catch (err) { await client.query('ROLLBACK'); throw err; } finally { client.release(); }
  }
  async syncTransitions({tenantId, namespace, channels = ['email','sms']}) {
    validateScope({tenantId,namespace}); channels.forEach(channel=>validateScope({tenantId,namespace,channel}));
    const rows = await this.pool.query(`SELECT t.payload FROM signal_distribution_transitions t
      WHERE t.tenant_id=$1 AND t.namespace=$2 AND EXISTS (SELECT 1 FROM unnest($3::text[]) c(channel)
      WHERE NOT EXISTS (SELECT 1 FROM signal_distribution_deliveries d
        WHERE d.tenant_id=t.tenant_id AND d.namespace=t.namespace AND d.transition_id=t.id AND d.channel=c.channel))
      ORDER BY t.created_at,t.id LIMIT 100`,[String(tenantId),namespace,channels]);
    for (const row of rows.rows) for (const channel of channels) await this.enqueue(row.payload,channel);
    return {processed:rows.rows.length};
  }
  async claim({tenantId,namespace,channel}) {
    validateScope({tenantId,namespace,channel}); if (namespace !== 'operational') return null;
    // A crashed worker may already have sent. Mark UNKNOWN instead of re-sending.
    await this.pool.query(`UPDATE signal_distribution_deliveries SET status='UNKNOWN',lease_until=NULL
      WHERE tenant_id=$1 AND namespace=$2 AND channel=$3 AND status='SENDING' AND lease_until<=$4`,
    [String(tenantId),namespace,channel,this.now()]);
    await this.pool.query(`UPDATE signal_distribution_deliveries d SET status='EXPIRED',suppression_reason='expired_stale_or_superseded'
      FROM signal_distribution_monitors m,signal_distribution_transitions t
      WHERE d.tenant_id=$1 AND d.namespace=$2 AND d.channel=$3 AND d.status='PENDING'
      AND m.id=d.monitor_id AND m.tenant_id=d.tenant_id AND m.namespace=d.namespace
      AND t.id=d.transition_id AND t.tenant_id=d.tenant_id AND t.namespace=d.namespace
      AND (m.expires_at <= $4 OR t.created_at <= $4::timestamptz-interval '5 minutes'
        OR (d.payload->>'kind'='RISK_TRANSITION' AND m.state->>'riskState' IS DISTINCT FROM d.payload->>'state'))`,
    [String(tenantId),namespace,channel,this.now()]);
    const r = await this.pool.query(`WITH candidate AS (SELECT d.id FROM signal_distribution_deliveries d
      JOIN signal_distribution_monitors m ON m.id=d.monitor_id AND m.tenant_id=d.tenant_id AND m.namespace=d.namespace
      JOIN signal_distribution_transitions t ON t.id=d.transition_id AND t.tenant_id=d.tenant_id AND t.namespace=d.namespace
      WHERE d.tenant_id=$1 AND d.namespace=$2 AND d.channel=$3 AND d.status='PENDING'
      AND m.expires_at>$4 AND t.created_at>$4::timestamptz-interval '5 minutes'
      AND (d.payload->>'kind'<>'RISK_TRANSITION' OR m.state->>'riskState'=d.payload->>'state')
      ORDER BY d.created_at,d.id FOR UPDATE OF d SKIP LOCKED LIMIT 1)
      UPDATE signal_distribution_deliveries d SET status='SENDING',attempts=attempts+1,
      lease_until=$4::timestamptz+interval '30 seconds' FROM candidate c WHERE d.id=c.id RETURNING d.*`,
    [String(tenantId),namespace,channel,this.now()]); return r.rows[0] || null;
  }
  async accepted(id, receipt) {
    if (typeof receipt !== 'string' || !receipt || receipt.length>512) throw new Error('invalid_receipt');
    const r = await this.pool.query(`UPDATE signal_distribution_deliveries SET status='ACCEPTED',provider_receipt=$2,
      accepted_at=$3,lease_until=NULL WHERE id=$1 AND status='SENDING' RETURNING id`,[id,receipt,this.now()]);
    if (!r.rowCount) throw new Error('delivery_lease_lost');
  }
  async unknown(id) {
    await this.pool.query("UPDATE signal_distribution_deliveries SET status='UNKNOWN',lease_until=NULL WHERE id=$1 AND status='SENDING'",[id]);
  }
  async getDeliveries({tenantId,namespace,monitorId}) {
    validateScope({tenantId,namespace});
    const r = await this.pool.query(`SELECT id,monitor_id,transition_id,channel,status,suppression_reason,attempts,
      created_at,accepted_at FROM signal_distribution_deliveries
      WHERE tenant_id=$1 AND namespace=$2 AND monitor_id=$3 ORDER BY created_at DESC,id LIMIT 100`,
    [String(tenantId),namespace,monitorId]); return r.rows;
  }
}
module.exports = {DeliveryStore, newCriticalEvidence, validateScope};
