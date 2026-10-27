'use strict';
const {createHash}=require('node:crypto');
class ZoneStore{
 constructor(pool,{tenantId,wallet}){Object.assign(this,{pool,tenantId:String(tenantId),wallet});}
 async append(zone){if(zone.paper?.mode!=='PAPER_OBSERVATION'||zone.paper.validated!==false||zone.paper.automaticExecution!==false||zone.provenance?.namespace!=='signal-dashboard-zone-research'||!['WATCHING','CONDITIONS_OBSERVED','INVALIDATED','EXPIRED'].includes(zone.state)||!Number.isFinite(Date.parse(zone.knownAt))||!Number.isFinite(Date.parse(zone.occurredAt)))throw Error('invalid_paper_zone_record');const eventId=createHash('sha256').update(JSON.stringify(zone)).digest('hex');
  await this.pool.query('INSERT INTO signal_dashboard_zone_events(tenant_id,wallet,token_address,zone_id,event_id,occurred_at,known_at,payload) VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb) ON CONFLICT DO NOTHING',[this.tenantId,this.wallet,zone.tokenAddress,zone.id,eventId,zone.occurredAt,zone.knownAt,JSON.stringify(zone)]);
 }
 async latest(ca,asOf=new Date().toISOString()){
  const r=await this.pool.query(`SELECT payload FROM (SELECT DISTINCT ON(zone_id) zone_id,known_at,payload FROM signal_dashboard_zone_events WHERE tenant_id=$1 AND wallet=$2 AND token_address=$3 AND occurred_at<=$4 AND known_at<=$4 ORDER BY zone_id,known_at DESC,event_id DESC) z ORDER BY known_at DESC LIMIT 100`,[this.tenantId,this.wallet,ca,asOf]);return r.rows.map(r=>r.payload);
 }
 async history(ca,zoneId,asOf=new Date().toISOString()){
  const r=await this.pool.query('SELECT payload FROM signal_dashboard_zone_events WHERE tenant_id=$1 AND wallet=$2 AND token_address=$3 AND zone_id=$4 AND occurred_at<=$5 AND known_at<=$5 ORDER BY known_at,event_id LIMIT 100',[this.tenantId,this.wallet,ca,zoneId,asOf]);return r.rows.map(r=>r.payload);
 }
}
module.exports={ZoneStore};
