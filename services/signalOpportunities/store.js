'use strict';
const {hash,DEFINITION,VERSION_HASH}=require('./definition');
class OpportunityStore {
 constructor(pool,{tenantId,wallet}){Object.assign(this,{pool,tenantId:String(tenantId),wallet});}
 async latest(ca,asOf=new Date().toISOString()){
  const r=await this.pool.query(`SELECT payload FROM signal_opportunity_decisions WHERE tenant_id=$1 AND wallet=$2 AND token_address=$3 AND known_at<=$4 ORDER BY known_at DESC,id DESC LIMIT 1`,[this.tenantId,this.wallet,ca,asOf]);return r.rows[0]?.payload||null;
 }
 async list(asOf=new Date().toISOString()){
  const r=await this.pool.query(`SELECT payload FROM (SELECT DISTINCT ON(token_address) token_address,known_at,payload FROM signal_opportunity_decisions
    WHERE tenant_id=$1 AND wallet=$2 AND known_at<=$3 ORDER BY token_address,known_at DESC,id DESC) latest ORDER BY known_at DESC LIMIT 200`,[this.tenantId,this.wallet,asOf]);return r.rows.map(r=>r.payload);
 }
 async history(ca,asOf=new Date().toISOString()){
  const r=await this.pool.query(`SELECT payload FROM signal_opportunity_decisions WHERE tenant_id=$1 AND wallet=$2 AND token_address=$3 AND known_at<=$4 ORDER BY known_at DESC,id DESC LIMIT 100`,[this.tenantId,this.wallet,ca,asOf]);return r.rows.map(r=>r.payload);
 }
 async append(decision,{held=false}={}){
  if(decision.definitionVersion!==DEFINITION.version||decision.definitionHash!==VERSION_HASH||decision.readOnly!==true||decision.provenance?.namespace!=='signal-opportunities-live')throw Error('invalid_opportunity_definition');
  const c=await this.pool.connect();try{
   await c.query('BEGIN');await c.query('SELECT pg_advisory_xact_lock(hashtextextended($1,0))',[`opportunity:${this.tenantId}:${this.wallet}:${decision.tokenAddress}`]);
   const old=(await c.query('SELECT payload FROM signal_opportunity_decisions WHERE tenant_id=$1 AND wallet=$2 AND token_address=$3 ORDER BY known_at DESC,id DESC LIMIT 1',[this.tenantId,this.wallet,decision.tokenAddress])).rows[0]?.payload;
   if(old&&Date.parse(old.knowledgeAt)>=Date.parse(decision.knowledgeAt)){await c.query('ROLLBACK');return {created:false};}
   const same=old&&hash({...old,id:null,knowledgeAt:null,knownAt:null,occurredAt:null})===hash({...decision,id:null,knowledgeAt:null,knownAt:null,occurredAt:null});
   if(same){await c.query('ROLLBACK');return {created:false};}
   await c.query('INSERT INTO signal_opportunity_decisions(tenant_id,wallet,token_address,id,definition_version,known_at,payload) VALUES($1,$2,$3,$4,$5,$6,$7::jsonb) ON CONFLICT DO NOTHING',[this.tenantId,this.wallet,decision.tokenAddress,decision.id,decision.definitionVersion,decision.knowledgeAt,JSON.stringify(decision)]);
   const changed=old?.opportunityState!==decision.opportunityState;
   const transition=changed&&(decision.opportunityState==='HOT'||decision.opportunityState==='OPPORTUNITY'&&old?.opportunityState==='WATCH'||decision.opportunityState==='AVOID'&&held);
   if(transition){for(const channel of decision.opportunityState==='HOT'?['sms','email']:['email']){
    const notificationId=hash([decision.id,channel]);
    // One alert per token/state/channel per hour; HOT has a separate key and
    // bypasses earlier OPPORTUNITY notifications. Serialized with the decision.
    const recent=await c.query(`SELECT id FROM signal_opportunity_notifications WHERE tenant_id=$1 AND wallet=$2 AND token_address=$3 AND channel=$4 AND state=$5 AND created_at>clock_timestamp()-interval '1 hour' LIMIT 1`,[this.tenantId,this.wallet,decision.tokenAddress,channel,decision.opportunityState]);
    if(!recent.rowCount)await c.query(`INSERT INTO signal_opportunity_notifications(tenant_id,wallet,id,token_address,channel,decision_id,state,payload) VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb) ON CONFLICT DO NOTHING`,[this.tenantId,this.wallet,notificationId,decision.tokenAddress,channel,decision.id,decision.opportunityState,JSON.stringify(decision)]);
   }}
   await c.query('COMMIT');return {created:true,transition:changed};
  }catch(e){await c.query('ROLLBACK');throw e;}finally{c.release();}
 }
 async claim(channel){
  // A timed-out send has an ambiguous receipt. Never automatically retry it.
  await this.pool.query(`UPDATE signal_opportunity_notifications SET status='UNKNOWN' WHERE tenant_id=$1 AND wallet=$2 AND channel=$3 AND status='SENDING' AND claimed_at<clock_timestamp()-interval '1 minute'`,[this.tenantId,this.wallet,channel]);
  await this.pool.query(`UPDATE signal_opportunity_notifications SET status='EXPIRED' WHERE tenant_id=$1 AND wallet=$2 AND channel=$3 AND status='PENDING' AND created_at<clock_timestamp()-interval '5 minutes'`,[this.tenantId,this.wallet,channel]);
  const r=await this.pool.query(`WITH candidate AS(SELECT id FROM signal_opportunity_notifications n WHERE tenant_id=$1 AND wallet=$2 AND channel=$3 AND status='PENDING'
   AND NOT EXISTS(SELECT 1 FROM signal_opportunity_decisions d WHERE d.tenant_id=n.tenant_id AND d.wallet=n.wallet AND d.token_address=n.token_address AND d.known_at>(n.payload->>'knowledgeAt')::timestamptz AND d.payload->>'opportunityState'<>n.state)
   ORDER BY created_at,id FOR UPDATE SKIP LOCKED LIMIT 1)
   UPDATE signal_opportunity_notifications n SET status='SENDING',claimed_at=clock_timestamp() FROM candidate c WHERE n.tenant_id=$1 AND n.wallet=$2 AND n.id=c.id RETURNING n.*`,[this.tenantId,this.wallet,channel]);return r.rows[0]||null;
 }
 async enqueueTransportTest(testId){
  if(typeof testId!=='string'||!/^signal-opportunities-[a-z0-9-]{1,60}$/.test(testId))throw Error('invalid_transport_test_id');
  for(const channel of ['sms','email']){const id=hash([this.tenantId,this.wallet,testId,channel]),payload={id,kind:'OPERATIONAL_TRANSPORT_TEST',testId};
   await this.pool.query(`INSERT INTO signal_opportunity_notifications(tenant_id,wallet,id,token_address,channel,decision_id,state,payload) VALUES($1,$2,$3,'__transport_test__',$4,$3,'TRANSPORT_TEST',$5::jsonb) ON CONFLICT DO NOTHING`,[this.tenantId,this.wallet,id,channel,JSON.stringify(payload)]);
  }
 }
 async finish(row,status,receipt=null){if(!['ACCEPTED','UNKNOWN','REJECTED'].includes(status))throw Error('invalid_delivery_state');await this.pool.query(`UPDATE signal_opportunity_notifications SET status=$4,receipt=$5 WHERE tenant_id=$1 AND wallet=$2 AND id=$3 AND status='SENDING'`,[this.tenantId,this.wallet,row.id,status,receipt]);if(row.payload?.kind==='OPERATIONAL_TRANSPORT_TEST')console.log('[signal-opportunity-delivery-test]',JSON.stringify({testId:row.payload.testId,channel:row.channel,status,receipt}));}
 async deliveryHealth(){const r=await this.pool.query('SELECT channel,status,count(*)::int AS count FROM signal_opportunity_notifications WHERE tenant_id=$1 AND wallet=$2 AND created_at>clock_timestamp()-interval \'24 hours\' GROUP BY channel,status',[this.tenantId,this.wallet]);return r.rows;}
 async acceptedReceipts(channel){const r=await this.pool.query(`SELECT * FROM signal_opportunity_notifications WHERE tenant_id=$1 AND wallet=$2 AND channel=$3 AND status='ACCEPTED' AND claimed_at>clock_timestamp()-interval '15 minutes' ORDER BY claimed_at LIMIT 5`,[this.tenantId,this.wallet,channel]);return r.rows;}
 async confirm(row,status){if(!['DELIVERED','UNDELIVERED'].includes(status))throw Error('invalid_provider_delivery_state');await this.pool.query(`UPDATE signal_opportunity_notifications SET status=$4 WHERE tenant_id=$1 AND wallet=$2 AND id=$3 AND status='ACCEPTED'`,[this.tenantId,this.wallet,row.id,status]);if(row.payload?.kind==='OPERATIONAL_TRANSPORT_TEST')console.log('[signal-opportunity-delivery-test]',JSON.stringify({testId:row.payload.testId,channel:row.channel,status,receipt:row.receipt}));}
}
module.exports={OpportunityStore};
