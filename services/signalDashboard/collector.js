'use strict';
const {SolanaRpcWalletWatchProvider}=require('./rpcWallet');
const {HeliusWalletWatchProvider}=require('./wallet');
const {DexScreenerMarketProvider,GeckoCandleProvider}=require('./market');
const {BirdeyeHistoricalQuoteProvider}=require('./quotes');
const {createDashboardWorker}=require('./worker');
function createCollectors({store,budget,env=process.env,fetchFn=globalThis.fetch,now=()=>new Date(),researchReader=null,feed=null,registry={}}){
 const marketProvider=new DexScreenerMarketProvider({budget,fetchFn,now});
 const walletProvider=env.SIGNAL_DASHBOARD_HELIUS_API_KEY?new HeliusWalletWatchProvider({apiKey:env.SIGNAL_DASHBOARD_HELIUS_API_KEY,rpcUrl:env.SIGNAL_DASHBOARD_RPC_URL,budget,fetchFn,now}):null;
 const quoteProvider=env.SIGNAL_DASHBOARD_BIRDEYE_API_KEY?new BirdeyeHistoricalQuoteProvider({apiKey:env.SIGNAL_DASHBOARD_BIRDEYE_API_KEY,budget,fetchFn,now}):null;
 const candleProvider=env.SIGNAL_DASHBOARD_COINGECKO_PRO_KEY?new GeckoCandleProvider({proKey:env.SIGNAL_DASHBOARD_COINGECKO_PRO_KEY,budget,fetchFn,now}):null;
 let publicWallet=null,stopped=false;const timers=[];
 const market=createDashboardWorker({store,marketProvider,quoteProvider,candleProvider,feed,researchReader,registry,now});
 async function walletTick(){
  if(!publicWallet){const watches=await store.list('watch',null);if(!watches.length)return;
   publicWallet=walletProvider||new SolanaRpcWalletWatchProvider({tokenAddresses:watches.map(w=>w.tokenAddress),rpcUrl:env.SIGNAL_DASHBOARD_RPC_URL,budget,fetchFn,now});}
  return store.lock(async()=>{
   const worker=createDashboardWorker({store,walletProvider:publicWallet,now,maxHistoryPages:Number(env.SIGNAL_DASHBOARD_MAX_HISTORY_PAGES||50)});
   // Alternate backfill, head catch-up and balances. Provider request budgets
   // can defer a step; a later cycle resumes the persisted cursor.
   const turn=walletTick.turn=(walletTick.turn||0)+1;
   if(turn%3===1)await worker.historyTick();
   else if(turn%3===2)await worker.headTick();
   else for(const w of await store.list('watch',null)){
    const balance=await publicWallet.getBalance({wallet:store.wallet,tokenAddress:w.tokenAddress});
    await store.put('balance',w.tokenAddress,idForBalance(balance),{...balance,occurredAt:balance.observedAt,knownAt:now().toISOString()});
   }
  },'wallet');
 }
 function idForBalance(b){return require('./wallet').id([b.tokenAddress,b.slot,b.observedAt]);}
 async function tick(fn,ms,component){if(stopped)return;const began=Date.now();try{await fn();}catch{const at=now().toISOString();try{await store.put('status','*',component,{component,state:'UNKNOWN',reason:'OBSERVATION_UNAVAILABLE_OR_BUDGET_DEFERRED',occurredAt:at,knownAt:at},{mutable:true});}catch{}}
  if(!stopped){const timer=setTimeout(()=>tick(fn,ms,component),Math.max(1000,ms-(Date.now()-began)));timer.unref?.();timers.push(timer);if(timers.length>20)timers.splice(0,timers.length-20);}}
 return {marketTick:()=>market.pollOnce(),walletTick,start(){tick(()=>market.pollOnce(),10000,'market-collector');tick(walletTick,5000,'wallet-collector');},stop(){stopped=true;timers.forEach(clearTimeout);}};
}
module.exports={createCollectors};
