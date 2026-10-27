'use strict';
const KINDS = new Set(['watch','transaction','observation','candle','event','annotation','balance','cursor','quote','status','paper']);
const MUTABLE = new Set(['watch','balance','cursor','status']);
class DashboardStore {
  constructor(pool,{tenantId,wallet}) {
    if(!/^\d+$/.test(tenantId||'') || !wallet) throw new Error('dashboard_scope_required');
    Object.assign(this,{pool,tenantId:String(tenantId),wallet});
  }
  async put(kind,tokenAddress,recordId,payload,{mutable=false}={}) {
    if(!KINDS.has(kind)||mutable&&!MUTABLE.has(kind))throw new Error('immutable_evidence');
    const occurredAt=payload.occurredAt||payload.observedAt||payload.blockTime;
    if(!Number.isFinite(Date.parse(occurredAt))||!Number.isFinite(Date.parse(payload.knownAt)))throw new Error('evidence_times_required');
    const update=mutable?'DO UPDATE SET occurred_at=EXCLUDED.occurred_at,known_at=EXCLUDED.known_at,payload=EXCLUDED.payload':'DO NOTHING';
    await this.pool.query(`INSERT INTO signal_dashboard_records(tenant_id,wallet,kind,token_address,record_id,occurred_at,known_at,payload)
      VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb) ON CONFLICT(tenant_id,wallet,kind,token_address,record_id) ${update}`,
    [this.tenantId,this.wallet,kind,tokenAddress,recordId,occurredAt,payload.knownAt,JSON.stringify(payload)]);
  }
  async list(kind,tokenAddress,{asOf=new Date().toISOString(),limit=5000,ascending=true}={}) {
    if(!KINDS.has(kind)||!Number.isInteger(limit)||limit<1||limit>20000)throw new Error('invalid_dashboard_query');
    const r=await this.pool.query(`SELECT payload FROM signal_dashboard_records WHERE tenant_id=$1 AND wallet=$2 AND kind=$3
      AND ($4::text IS NULL OR token_address=$4) AND occurred_at<=$5 AND known_at<=$5
      ORDER BY occurred_at ${ascending?'ASC':'DESC'},known_at ${ascending?'ASC':'DESC'},record_id LIMIT $6`,[this.tenantId,this.wallet,kind,tokenAddress??null,asOf,limit]);
    return r.rows.map(x=>x.payload);
  }
  async latest(kind,ca,options={}) {return (await this.list(kind,ca,{...options,ascending:false,limit:1}))[0]||null;}
  async peakChronology(ca,start,asOf) {
    if(!start)return {priceUsd:null,at:null,drawdown:null,chronology:[]};
    const r=await this.pool.query(`WITH base AS (
      SELECT occurred_at,known_at,payload,(payload->>'priceUsd')::double precision AS price
      FROM signal_dashboard_records WHERE tenant_id=$1 AND wallet=$2 AND kind='observation' AND token_address=$3
      AND occurred_at>=$4 AND occurred_at<=$5 AND known_at<=$5 AND jsonb_typeof(payload->'priceUsd')='number'
    ), ranked AS (SELECT *,max(price) OVER(ORDER BY occurred_at,known_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS previous_peak FROM base)
    SELECT max(price) AS peak,(array_agg(price ORDER BY occurred_at DESC,known_at DESC))[1] AS current,
      jsonb_agg(jsonb_build_object('priceUsd',price,'observedAt',occurred_at,'knownAt',known_at,'provider',payload->>'provider') ORDER BY occurred_at,known_at)
      FILTER(WHERE previous_peak IS NULL OR price>previous_peak) AS chronology FROM ranked`,[this.tenantId,this.wallet,ca,start,asOf]);
    const row=r.rows[0],chronology=row.chronology||[];
    return {priceUsd:row.peak??null,at:chronology.at(-1)?.observedAt||null,drawdown:row.peak>0?(row.current-row.peak)/row.peak:null,chronology};
  }
  async lock(fn,lane='market') {
    const c=await this.pool.connect();
    const key=`signal-dashboard:${this.tenantId}:${this.wallet}:${lane}`;
    try {
      const r=await c.query('SELECT pg_try_advisory_lock(hashtextextended($1,0)) AS locked',[key]);
      if(!r.rows[0].locked)return {skipped:true};
      try{return await fn();}finally{await c.query('SELECT pg_advisory_unlock(hashtextextended($1,0))',[key]);}
    } finally {c.release();}
  }
}
class DurableBudget {
  constructor(pool){this.pool=pool;}
  async take(provider,gapMs) {
    const r=await this.pool.query(`INSERT INTO signal_dashboard_provider_budget(provider,next_at) VALUES($1,clock_timestamp()+$2*interval '1 millisecond')
      ON CONFLICT(provider) DO UPDATE SET next_at=clock_timestamp()+$2*interval '1 millisecond'
      WHERE signal_dashboard_provider_budget.next_at<=clock_timestamp() RETURNING provider`,[provider,gapMs]);
    if(!r.rowCount)throw new Error('dashboard_budget_deferred');
  }
  async backoff(provider,ms) {await this.pool.query(`INSERT INTO signal_dashboard_provider_budget(provider,next_at) VALUES($1,clock_timestamp()+$2*interval '1 millisecond')
    ON CONFLICT(provider) DO UPDATE SET next_at=GREATEST(signal_dashboard_provider_budget.next_at,EXCLUDED.next_at)`,[provider,Math.max(1000,Math.min(ms,3600000))]);}
}
module.exports={DashboardStore,DurableBudget};
