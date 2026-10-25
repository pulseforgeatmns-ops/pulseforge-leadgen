'use strict';
const {Pool}=require('pg');
const {createSignalApp,resolveOwner}=require('./app');
const {config}=require('../../services/signalDashboard/config');
const {DashboardStore,DurableBudget}=require('../../services/signalDashboard/store');
const {DashboardService}=require('../../services/signalDashboard/service');
const {createCollectors}=require('../../services/signalDashboard/collector');
const {isValidSolanaAddress:address}=require('../../packages/signal-v1/acquisition/solanaAddress');
async function boot({env=process.env,pool:providedPool,listen=true,collect=true}={}){
 const pool=providedPool||new Pool({connectionString:env.DATABASE_URL,ssl:env.DATABASE_SSL==='false'?false:{rejectUnauthorized:false},max:6,connectionTimeoutMillis:5000,query_timeout:10000});
 await pool.query('SELECT tenant_id FROM signal_dashboard_records LIMIT 0');await pool.query('SELECT sid FROM signal_dashboard_session LIMIT 0');await pool.query('SELECT ip_hash FROM signal_dashboard_auth_attempts LIMIT 0');
 const owner=await resolveOwner(pool,env);
 const cfg=config({...env,SIGNAL_DASHBOARD_TENANT_ID:env.SIGNAL_DASHBOARD_TENANT_ID||String(owner.client_id??1),SIGNAL_DASHBOARD_OWNER_USER_ID:String(owner.id)});
 if(!cfg.enabled)throw Error('signal_dashboard_disabled');
 const store=new DashboardStore(pool,cfg),service=new DashboardService(store),budget=new DurableBudget(pool);
 const watches=JSON.parse(env.SIGNAL_DASHBOARD_WATCH_TOKENS||'[]');
 if(!Array.isArray(watches)||!watches.length||watches.length>3||watches.some(w=>!address(w.tokenAddress)||w.poolAddress&&!address(w.poolAddress)))throw Error('invalid_signal_watch_configuration');
 for(const watch of watches){const old=await store.latest('watch',watch.tokenAddress),at=new Date().toISOString();
  await store.put('watch',watch.tokenAddress,'watch',{tokenAddress:watch.tokenAddress,symbol:typeof watch.symbol==='string'?watch.symbol.slice(0,30):null,poolAddress:old?.poolAddress||watch.poolAddress||null,occurredAt:at,knownAt:at},{mutable:true});}
 const registry={};try{for(const row of (await pool.query('SELECT source_id,cluster_id,cluster_relationship_status FROM signal_source_registry WHERE active=true')).rows)registry[row.source_id]={clusterId:row.cluster_id,relationship:row.cluster_relationship_status};}catch{}
 let feed=null;
 if(env.SIGNAL_DASHBOARD_FEED_URL&&env.SIGNAL_OPERATOR_FEED_TOKEN){
  const u=new URL(env.SIGNAL_DASHBOARD_FEED_URL);if(u.protocol!=='http:'||!u.hostname.endsWith('.railway.internal')||u.pathname!=='/feed'||u.username||u.password||u.search||u.hash)throw Error('invalid_private_caller_feed');
  feed=async()=>{await budget.take('signal-dashboard-feed',30000);const r=await fetch(u,{headers:{authorization:`Bearer ${env.SIGNAL_OPERATOR_FEED_TOKEN}`},redirect:'error',signal:AbortSignal.timeout(5000)});if(!r.ok)throw Error('private_feed_unavailable');const j=await r.json();return j.calls;};
 }
 const researchReader=()=>require('../../services/signalDashboard/researchBridge').readEmpiricalEvents(pool,watches.map(w=>w.tokenAddress),{registry});
 const collectors=createCollectors({store,budget,env,feed,researchReader,registry});
 const app=createSignalApp({pool,service,config:cfg,sessionSecret:env.SESSION_SECRET,coreAppUrl:env.SIGNAL_CORE_APP_URL});
 let server;if(listen)server=app.listen(Number(env.PORT||3000),'0.0.0.0',()=>console.log('[signal-dashboard] ready: isolated authenticated service, public-wallet observation only'));
 if(collect)collectors.start();
 const stop=()=>{collectors.stop();server?.close();};process.once('SIGTERM',stop);process.once('SIGINT',stop);
 return {app,service,store,collectors,stop};
}
if(require.main===module)boot().catch(error=>{const allowed=['signal_owner_identity_unresolved','explicit_signal_schema_approval_required','signal_dashboard_disabled','signal_session_secret_required','invalid_private_caller_feed'];console.error('[signal-dashboard] startup blocked:',allowed.includes(error.message)?error.message:'configuration_or_schema_unavailable');process.exitCode=1;});
module.exports={boot};
