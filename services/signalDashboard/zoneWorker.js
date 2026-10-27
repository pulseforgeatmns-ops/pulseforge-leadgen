'use strict';
const {discover,evolve,VERSION}=require('./zones');
function createZoneWorker({store,zoneStore,now=()=>new Date()}){
 return {async tick(){if(!zoneStore)return;return store.lock(async()=>{
  const asOf=now().toISOString();for(const watch of await store.list('watch',null,{limit:3})){
   const ca=watch.tokenAddress;const [candles,observations,cursors,transactions,previous]=await Promise.all([store.list('candle',ca,{asOf,ascending:false,limit:10000}),store.list('observation',ca,{asOf,ascending:false,limit:1500}),store.list('cursor',ca,{asOf}),store.list('transaction',ca,{asOf,ascending:false,limit:100}),zoneStore.latest(ca,asOf)]);
   const current=observations[0],pairId=watch.poolAddress||current?.pairId,assessment=cursors.find(c=>c.key==='risk:'+ca);
   const risk=assessment&&Date.parse(asOf)-Date.parse(assessment.knownAt)<=30000?assessment.state?.riskState||'UNKNOWN':'UNKNOWN';
   for(const zone of previous){const next=evolve(zone,{candles,observations,risk,pairId,asOf});if(next)await zoneStore.append(next);}
   const active=previous.some(z=>['WATCHING','CONDITIONS_OBSERVED'].includes(z.state)&&Date.parse(z.expiresAt)>Date.parse(asOf)),cooldown=previous.some(z=>Date.parse(asOf)-Date.parse(z.createdAt)<30*60000);
   let result={missing:[],reason:'An existing zone is being followed.'};
   if(!active&&!cooldown){result=discover({tokenAddress:ca,pairId,candles,observations,risk,transactions,asOf});if(result.zone&&!previous.some(z=>z.id===result.zone.id))await zoneStore.append(result.zone);}
   await store.put('status','*','zone-watch:'+ca,{component:'zone-watch:'+ca,state:'PARTIAL',version:VERSION,mode:'PAPER_OBSERVATION',missing:result.missing,reason:result.zone?'A new research watch area was recorded.':result.reason||result.missing.join(' '),occurredAt:asOf,knownAt:asOf},{mutable:true});
  }
 },'zones');}};
}
module.exports={createZoneWorker};
