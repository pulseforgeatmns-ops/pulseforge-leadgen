'use strict';
// Dedicated process. No web-server import, execution authority or automatic DDL.
const {config}=require('../services/signalDashboard/config');
const {DashboardStore,DurableBudget}=require('../services/signalDashboard/store');
const {isValidSolanaAddress:address}=require('../packages/signal-v1/acquisition/solanaAddress');
async function main(env=process.env) {
  const c=config(env);if(!c.enabled)return null;
  const pool=require('../db'),store=new DashboardStore(pool,c),budget=new DurableBudget(pool);
  await pool.query('SELECT tenant_id FROM signal_dashboard_records LIMIT 0');
  await pool.query('SELECT provider FROM signal_dashboard_provider_budget LIMIT 0');
  const watches=JSON.parse(env.SIGNAL_DASHBOARD_WATCH_TOKENS||'[]');
  if(!Array.isArray(watches)||watches.length>3||watches.some(w=>!address(w.tokenAddress)||w.poolAddress&&!address(w.poolAddress)))throw new Error('invalid_dashboard_watch_config');
  for(const w of watches){
    const old=await store.latest('watch',w.tokenAddress),at=new Date().toISOString();
    await store.put('watch',w.tokenAddress,'watch',{tokenAddress:w.tokenAddress,symbol:typeof w.symbol==='string'?w.symbol.slice(0,30):null,
      poolAddress:old?.poolAddress||w.poolAddress||null,occurredAt:at,knownAt:at},{mutable:true});
  }
  let feed=null;
  if(env.SIGNAL_DASHBOARD_FEED_URL&&env.SIGNAL_OPERATOR_FEED_TOKEN) {
    const u=new URL(env.SIGNAL_DASHBOARD_FEED_URL);
    if(u.protocol!=='http:'||!u.hostname.endsWith('.railway.internal')||u.pathname!=='/feed'||u.username||u.password||u.search||u.hash)throw new Error('invalid_dashboard_private_feed');
    feed=async()=>{
      await budget.take('signal-dashboard-feed',30000);
      const r=await fetch(u,{headers:{authorization:`Bearer ${env.SIGNAL_OPERATOR_FEED_TOKEN}`},redirect:'error',signal:AbortSignal.timeout(5000)});
      if(!r.ok)throw new Error('feed_unavailable');const b=await r.json();return b.calls;
    };
  }
  const registry={};
  try{for(const r of (await pool.query('SELECT source_id,cluster_id,cluster_relationship_status FROM signal_source_registry WHERE active=true')).rows)
    registry[r.source_id]={clusterId:r.cluster_id,relationship:r.cluster_relationship_status};}catch{/* Missing registry stays UNKNOWN. */}
  const researchReader=async()=>require('../services/signalDashboard/researchBridge').readEmpiricalEvents(pool,(await store.list('watch',null)).map(w=>w.tokenAddress),{registry});
  const collectors=require('../services/signalDashboard/collector').createCollectors({store,budget,env,feed,researchReader,registry});
  collectors.start();
  const stop=()=>collectors.stop();process.once('SIGTERM',stop);process.once('SIGINT',stop);
  return {worker:collectors,stop};
}
if(require.main===module)main().catch(()=>{console.error('[signal-dashboard] configuration or schema unavailable');process.exitCode=1;});
module.exports={main};
