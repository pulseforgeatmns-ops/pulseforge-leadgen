'use strict';
const {id,SOL,USDC}=require('./wallet');
const {feedEvents}=require('./events');
const {INTERVALS}=require('./market');
const {evaluate}=require('../signalDistribution/risk');
function createDashboardWorker({store,marketProvider,walletProvider,candleProvider=null,quoteProvider=null,
  feed=null,researchReader=null,registry={},now=()=>new Date(),maxActive=3,maxHistoryPages=50,includeCurrentMark=true,processFeed=true}) {
  if(!Number.isInteger(maxHistoryPages)||maxHistoryPages<1||maxHistoryPages>1000)throw new Error('invalid_history_page_limit');
  let running=false;
  const status=async(component,state,reason=null)=>{const at=now().toISOString();await store.put('status','*',component,{component,state,reason,occurredAt:at,knownAt:at},{mutable:true});};
  async function persistPage(page) {
    const watched=new Set((await store.list('watch',null)).map(w=>w.tokenAddress));
    for(const t of page.transactions)if(watched.has(t.tokenAddress))await store.put('transaction',t.tokenAddress,t.id,t);
  }
  async function historyTick() {
    let cursor=(await store.list('cursor',null)).find(x=>x.key==='history');
    if(cursor?.exhausted)return;
    if((cursor?.pages||0)>=maxHistoryPages){await status('wallet-history','PARTIAL','BACKFILL_PAGE_LIMIT_REACHED');return;}
    const page=await walletProvider.getHistoryPage({wallet:store.wallet,before:cursor?.before||null});
    await persistPage(page);
    const at=now().toISOString();
    if(page.nextBefore&&page.nextBefore===cursor?.before)throw new Error('wallet_pagination_stalled');
    await store.put('cursor','*','history',{key:'history',before:page.nextBefore||cursor?.before||null,
      pages:(cursor?.pages||0)+1,exhausted:page.exhausted,coverage:page.coverage,occurredAt:at,knownAt:at},{mutable:true});
    const head=(await store.list('cursor',null)).find(x=>x.key==='head');
    if(!head&&(page.signatures[0]||page.exhausted))await store.put('cursor','*','head',{key:'head',lastSeen:page.signatures[0]||null,before:null,caughtUp:true,occurredAt:at,knownAt:at},{mutable:true});
  }
  async function headTick() {
    const cursor=(await store.list('cursor',null)).find(x=>x.key==='head');if(!cursor)return;
    const page=await walletProvider.getHistoryPage({wallet:store.wallet,before:cursor.before||null});
    await persistPage(page);
    const found=page.signatures.includes(cursor.lastSeen), caughtUp=found||page.exhausted;
    const candidate=cursor.candidate||page.signatures[0]||null,at=now().toISOString();
    if(!caughtUp&&!page.nextBefore)throw new Error('wallet_head_pagination_stalled');
    await store.put('cursor','*','head',{key:'head',lastSeen:caughtUp?(candidate||cursor.lastSeen):cursor.lastSeen,
      candidate:caughtUp?null:candidate,before:caughtUp?null:page.nextBefore,caughtUp,occurredAt:at,knownAt:at},{mutable:true});
  }
  async function feedTick() {
    if(researchReader){try{for(const e of await researchReader())await store.put('event',e.tokenAddress,e.id,e);}catch{await status('research-bridge','UNKNOWN','EXISTING_EMPIRICAL_EVIDENCE_UNAVAILABLE');}}
    if(!feed)return status('feed','UNKNOWN','EMPIRICAL_FEED_NOT_CONFIGURED');
    const calls=await feed();
    if(!Array.isArray(calls)||calls.length>10000)throw new Error('invalid_caller_feed');
    const watched=new Set((await store.list('watch',null)).map(w=>w.tokenAddress));
    for(const call of calls)for(const e of feedEvents(call,{now:now(),registry}))if(watched.has(e.tokenAddress))await store.put('event',e.tokenAddress,e.id,e);
    await status('feed','PARTIAL','Only retained empirical messages with explicit CA; completeness is unknown');
  }
  async function quoteTick() {
    if(!quoteProvider)return;
    const [txs,quotes]=await Promise.all([store.list('transaction',null),store.list('quote',null)]);
    const exists=new Set(quotes.filter(x=>x.priceUsd!=null||+now()-Date.parse(x.knownAt)<300000).map(x=>`${x.asset}:${x.blockTime}`));
    for(const t of txs)for(const asset of [...new Set([...(t.quoteAsset?[t.quoteAsset]:[]),...(t.feesLamports&&t.feesLamports!=='0'?[SOL]:[])])]) {
      if(exists.has(`${asset}:${t.blockTime}`))continue;
      const q=await quoteProvider.getQuote(asset,t.blockTime);
      const at=now().toISOString();
      await store.put('quote','*',id([asset,t.blockTime,at]),q||{asset,blockTime:t.blockTime,priceUsd:null,quality:'UNKNOWN',occurredAt:t.blockTime,knownAt:at});
      return;
    }
  }
  async function currentMarkTick(){
    if(!marketProvider)return;
    const mark=(await store.list('quote',null,{ascending:false,limit:100})).find(q=>q.purpose==='CURRENT_MARK'&&q.asset===USDC);
    if(mark&&+now()-Date.parse(mark.knownAt)<55000)return;
    const o=await marketProvider.getSnapshot({tokenAddress:USDC});
    const price=Number(o.priceUsd);if(o.tokenAddress!==USDC||!Number.isFinite(price)||price<=0)throw Error('quote_mark_unavailable');
    const at=now().toISOString();await store.put('quote','*',id(['mark',USDC,at]),{asset:USDC,purpose:'CURRENT_MARK',priceUsd:price.toFixed(18),occurredAt:at,knownAt:at,provider:o.provider,sourceTimestamp:o.providerTimestamp||null,quality:'PARTIAL'});
  }
  async function captureCandles(watch,previous){
          const all=await store.list('candle',watch.tokenAddress,{ascending:false,limit:10000});
          const missing=['15m','1m','5m','1h','4h'].find(k=>!all.some(c=>c.interval===k));
          const lastMinute=all.find(c=>c.interval==='1m');
          const interval=missing||(!lastMinute||+now()-Date.parse(lastMinute.knownAt)>=10000?'1m':null);
          if(!interval)return;
          const revisions=new Map();
          for(const c of all){const key=[c.provider,c.pairId,c.interval,c.time].join(':');if(!revisions.has(key))revisions.set(key,c);}
          for(const c of await candleProvider.getCandles({tokenAddress:watch.tokenAddress,poolAddress:watch.poolAddress||previous?.pairId,interval})){
            const key=[c.provider,c.pairId,c.interval,c.time].join(':'),prior=revisions.get(key);
            if(prior&&['open','high','low','close','volume','complete'].every(field=>prior[field]===c[field]))continue;
            await store.put('candle',watch.tokenAddress,id([c.provider,c.pairId,c.interval,c.time,c.knownAt]),c);
          }
    await status('candles:'+watch.tokenAddress,'PARTIAL','Provider candles; source freshness and gaps remain disclosed');
  }
  let candleOffset=0;
  async function candleTick(){
    if(!candleProvider)return;
    return store.lock(async()=>{
      const watches=await store.list('watch',null,{limit:100});
      if(!watches.length)return;if(watches.length>maxActive)throw Error('dashboard_active_capacity_exceeded');
      const watch=watches[candleOffset++%watches.length],previous=await store.latest('observation',watch.tokenAddress);
      try{await captureCandles(watch,previous);}catch{await status('candles:'+watch.tokenAddress,'UNKNOWN','HISTORICAL_PROVIDER_UNAVAILABLE_OR_BUDGET_DEFERRED');}
    },'candles');
  }
  async function pollOnce() {
    if(running)return {skipped:true};running=true;
    try{return await store.lock(async()=>{
      const watches=await store.list('watch',null,{limit:100});
      if(watches.length>maxActive)throw new Error('dashboard_active_capacity_exceeded');
      const attempt=async(component,fn)=>{try{await fn();}catch{await status(component,'UNKNOWN','OBSERVATION_UNAVAILABLE_OR_RATE_LIMITED');}};
      if(includeCurrentMark)await attempt('quote-mark',currentMarkTick);
      for(const watch of watches) {
        const previous=await store.latest('observation',watch.tokenAddress);
        // Receipt timestamps follow the poll start. Allow timer/network jitter
        // without accidentally skipping every other ten-second poll.
        if(marketProvider&&(!previous||+now()-Date.parse(previous.observedAt)>=8000))await attempt('market:'+watch.tokenAddress,async()=>{
          const snapshot=await marketProvider.getSnapshot({tokenAddress:watch.tokenAddress,poolAddress:watch.poolAddress||previous?.pairId});
          await store.put('observation',watch.tokenAddress,id([snapshot.provider,snapshot.pairId,snapshot.observedAt]),snapshot);
          const key='risk:'+watch.tokenAddress,prior=(await store.list('cursor',null)).find(c=>c.key===key);
          const evaluated=evaluate(prior?.state||{},snapshot,{now:now()});
          if(!evaluated.ignored){
            if(prior?.state?.riskState!==evaluated.state.riskState || JSON.stringify(prior?.state?.reasons)!==JSON.stringify(evaluated.state.reasons)){
              const event={id:id([watch.tokenAddress,snapshot.observedAt,evaluated.state.riskState]),tokenAddress:watch.tokenAddress,
                category:'DISTRIBUTION_'+evaluated.state.riskState,occurredAt:snapshot.observedAt,knownAt:snapshot.knownAt,
                source:'distribution-v1.0',evidence:evaluated.state.reasons,provenance:{...snapshot.provenance,provider:snapshot.provider}};
              await store.put('event',watch.tokenAddress,event.id,event);
            }
            await store.put('cursor',watch.tokenAddress,key,{key,state:evaluated.state,occurredAt:snapshot.observedAt,knownAt:snapshot.knownAt},{mutable:true});
          }
          if(!watch.poolAddress)await store.put('watch',watch.tokenAddress,'watch',{...watch,poolAddress:snapshot.pairId,occurredAt:now().toISOString(),knownAt:now().toISOString()},{mutable:true});
          await status('market:'+watch.tokenAddress,'PARTIAL','Provider source timestamp unavailable');
        });
        const balance=walletProvider?await store.latest('balance',watch.tokenAddress):null;
        if(walletProvider&&(!balance||+now()-Date.parse(balance.observedAt)>=30000))await attempt('balance:'+watch.tokenAddress,async()=>{
          const b=await walletProvider.getBalance({wallet:store.wallet,tokenAddress:watch.tokenAddress});
          await store.put('balance',watch.tokenAddress,id([b.slot,b.observedAt]),{...b,occurredAt:b.observedAt,knownAt:now().toISOString()});
        });
        if(candleProvider)await attempt('candles:'+watch.tokenAddress,()=>captureCandles(watch,previous));
      }
      if(walletProvider){await attempt('wallet-history',historyTick);await attempt('wallet-head',headTick);}
      await attempt('quotes',quoteTick);
      const lastFeed=(await store.list('status','*')).find(s=>s.component==='feed');
      if(processFeed&&(!lastFeed||+now()-Date.parse(lastFeed.knownAt)>=30000))await attempt('feed',feedTick);
      return {observed:true};
    });}finally{running=false;}
  }
  return {pollOnce,historyTick,headTick,feedTick,quoteTick,currentMarkTick,candleTick};
}
module.exports={createDashboardWorker};
