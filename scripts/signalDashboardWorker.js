'use strict';
// Dedicated process. No web-server import, execution authority or automatic DDL.
const {config}=require('../services/signalDashboard/config');
const {DashboardStore,DurableBudget}=require('../services/signalDashboard/store');
const {DexScreenerMarketProvider,GeckoCandleProvider}=require('../services/signalDashboard/market');
const {HeliusWalletWatchProvider}=require('../services/signalDashboard/wallet');
const {BirdeyeHistoricalQuoteProvider}=require('../services/signalDashboard/quotes');
const {createDashboardWorker}=require('../services/signalDashboard/worker');
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
  const marketProvider=new DexScreenerMarketProvider({budget});
  const walletProvider=env.SIGNAL_DASHBOARD_HELIUS_API_KEY?new HeliusWalletWatchProvider({apiKey:env.SIGNAL_DASHBOARD_HELIUS_API_KEY,rpcUrl:env.SIGNAL_DASHBOARD_RPC_URL,budget}):null;
  const quoteProvider=env.SIGNAL_DASHBOARD_BIRDEYE_API_KEY?new BirdeyeHistoricalQuoteProvider({apiKey:env.SIGNAL_DASHBOARD_BIRDEYE_API_KEY,budget}):null;
  // Public Gecko is unavailable here unless a shared durable budget is injected by an integration.
  const candleProvider=env.SIGNAL_DASHBOARD_COINGECKO_PRO_KEY?new GeckoCandleProvider({budget,proKey:env.SIGNAL_DASHBOARD_COINGECKO_PRO_KEY}):null;
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
  const worker=createDashboardWorker({store,marketProvider,walletProvider,quoteProvider,candleProvider,feed,researchReader,registry});
  let stopped=false,timer;
  const tick=async()=>{const startedAt=Date.now();try{await worker.pollOnce();}catch{console.error('[signal-dashboard] observation cycle unavailable');}finally{if(!stopped)timer=setTimeout(tick,Math.max(1000,10000-(Date.now()-startedAt)));}};
  await tick();
  const stop=()=>{stopped=true;clearTimeout(timer);};process.once('SIGTERM',stop);process.once('SIGINT',stop);
  return {worker,stop};
}
if(require.main===module)main().catch(()=>{console.error('[signal-dashboard] configuration or schema unavailable');process.exitCode=1;});
module.exports={main};
