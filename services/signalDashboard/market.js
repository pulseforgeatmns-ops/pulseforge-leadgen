'use strict';
const {number,count} = require('../signalDistribution/marketProvider');
const {isValidSolanaAddress:address} = require('../../packages/signal-v1/acquisition/solanaAddress');
const INTERVALS = {'1m':60,'5m':300,'15m':900,'1h':3600,'4h':14400};
class MarketProvider {
  async getSnapshot(){throw new Error('market_provider_not_implemented');}
  async getCandles(){throw new Error('candle_provider_not_implemented');}
}
class DexScreenerMarketProvider extends MarketProvider {
  constructor({fetchFn=globalThis.fetch,budget,now=()=>new Date()}={}){super();Object.assign(this,{fetchFn,budget,now});}
  async getSnapshot({tokenAddress,poolAddress}) {
    if(!address(tokenAddress)||poolAddress&&!address(poolAddress))throw new Error('invalid_market_address');
    await this.budget.take('dexscreener',1000);
    const url=poolAddress?`https://api.dexscreener.com/latest/dex/pairs/solana/${poolAddress}`:`https://api.dexscreener.com/token-pairs/v1/solana/${tokenAddress}`;
    const r=await this.fetchFn(url,{redirect:'error',signal:AbortSignal.timeout(5000)});
    if(r.status===429){await this.budget.backoff('dexscreener',60000);throw new Error('market_rate_limited');}
    if(!r.ok)throw new Error('market_provider_unavailable');
    const json=await r.json(),pairs=Array.isArray(json)?json:json.pairs;
    const eligible=(Array.isArray(pairs)?pairs:[]).filter(p=>p.chainId==='solana'&&p.baseToken?.address===tokenAddress&&address(p.pairAddress)&&(!poolAddress||p.pairAddress===poolAddress))
      .sort((a,b)=>(number(b.liquidity?.usd)??-1)-(number(a.liquidity?.usd)??-1));
    const p=eligible[0];if(!p||number(p.priceUsd)===null)throw new Error('market_pair_or_price_unavailable');
    const now=this.now().toISOString(), t=p.txns?.m5||{};
    return {tokenAddress,pairId:p.pairAddress,symbol:p.baseToken.symbol||null,name:p.baseToken.name||null,priceUsd:number(p.priceUsd),
      marketCapUsd:number(p.marketCap),liquidityUsd:number(p.liquidity?.usd),volume5mUsd:number(p.volume?.m5),volume24hUsd:number(p.volume?.h24),
      change24hPercent:number(p.priceChange?.h24,true),buys5m:count(t.buys),sells5m:count(t.sells),
      buyVolume5mUsd:null,sellVolume5mUsd:null,buyers5m:null,sellers5m:null,
      pairCreatedAt:Number.isSafeInteger(p.pairCreatedAt)?new Date(p.pairCreatedAt).toISOString():null,tokenAgeSeconds:null,
      observedAt:now,occurredAt:now,knownAt:now,providerTimestamp:null,provider:'dexscreener',
      provenance:{dataClass:'EMPIRICAL',endpoint:poolAddress?'pair':'token-pairs',timestampBasis:'response_received',sourceFreshness:'UNKNOWN',liquidityScope:'pool_reserve'}};
  }
}
class GeckoCandleProvider extends MarketProvider {
  constructor({fetchFn=globalThis.fetch,budget,proKey=null,publicBudget=null,now=()=>new Date()}={}){super();Object.assign(this,{fetchFn,budget,proKey,publicBudget,now});}
  async getCandles({tokenAddress,poolAddress,interval,before=this.now().toISOString()}) {
    if(!address(tokenAddress)||!address(poolAddress)||!INTERVALS[interval])throw new Error('invalid_candle_request');
    const paid=Boolean(this.proKey);
    if(!paid && (!this.publicBudget || !await this.publicBudget.consume('existing')))throw new Error('shared_gecko_budget_unavailable');
    await this.budget.take(paid?'coingecko-pro':'geckoterminal-dashboard',paid?1000:10000);
    const seconds=INTERVALS[interval], frame=seconds>=3600?'hour':'minute',aggregate=seconds/(frame==='hour'?3600:60);
    const root=paid?'https://pro-api.coingecko.com/api/v3/onchain':'https://api.geckoterminal.com/api/v2';
    const url=`${root}/networks/solana/pools/${poolAddress}/ohlcv/${frame}?aggregate=${aggregate}&limit=300&currency=usd&token=${tokenAddress}&include_empty_intervals=false&before_timestamp=${Math.floor(Date.parse(before)/1000)}`;
    const r=await this.fetchFn(url,{headers:{Accept:'application/json;version=20230203',...(paid?{'x-cg-pro-api-key':this.proKey}:{})},redirect:'error',signal:AbortSignal.timeout(8000)});
    if(r.status===429){await this.budget.backoff(paid?'coingecko-pro':'geckoterminal-dashboard',60000);throw new Error('candles_rate_limited');}
    if(!r.ok)throw new Error('candles_unavailable');
    const body=await r.json(),rows=body?.data?.attributes?.ohlcv_list;
    if(!Array.isArray(rows))throw new Error('invalid_candle_response');
    const knownAt=this.now().toISOString(),provider=paid?'coingecko-pro':'geckoterminal';
    return rows.flatMap(row=>{
      if(!Array.isArray(row)||row.length<6||!row.every(x=>typeof x==='number'&&Number.isFinite(x)))return[];
      const [time,open,high,low,close,volume]=row;
      if(time*1000>Date.parse(before)||time*1000>+this.now()||[open,high,low,close,volume].some(x=>x<0)||high<Math.max(open,close)||low>Math.min(open,close))return[];
      return [{time,open,high,low,close,volume,interval,tokenAddress,pairId:poolAddress,provider,knownAt,observedAt:knownAt,occurredAt:new Date(time*1000).toISOString(),
        providerTimestamp:null,quality:'PARTIAL',coverage:'PROVIDER_OHLCV',complete:(time+seconds)*1000<=Date.parse(before),
        provenance:{dataClass:'EMPIRICAL',emptyIntervals:false,sourceFreshness:'UNKNOWN'}}];
    }).sort((a,b)=>a.time-b.time);
  }
}
function observedCandles(observations,interval,asOf=new Date().toISOString()) {
  const seconds=INTERVALS[interval];if(!seconds)throw new Error('invalid_interval');
  const buckets=new Map();
  for(const o of [...observations].sort((a,b)=>Date.parse(a.observedAt)-Date.parse(b.observedAt))) {
    if(Date.parse(o.observedAt)>Date.parse(asOf)||Date.parse(o.knownAt)>Date.parse(asOf)||!Number.isFinite(o.priceUsd))continue;
    const time=Math.floor(Date.parse(o.observedAt)/1000/seconds)*seconds;
    const c=buckets.get(time)||{time,open:o.priceUsd,high:o.priceUsd,low:o.priceUsd,close:o.priceUsd,volume:null,interval,
      provider:o.provider,pairId:o.pairId,quality:'PARTIAL',coverage:'SAMPLED_OBSERVATIONS_ONLY',complete:false,observedAt:o.observedAt,knownAt:o.knownAt};
    c.high=Math.max(c.high,o.priceUsd);c.low=Math.min(c.low,o.priceUsd);c.close=o.priceUsd;c.observedAt=o.observedAt;c.knownAt=o.knownAt;buckets.set(time,c);
  }
  return [...buckets.values()]; // Gaps remain gaps; never fabricate empty candles.
}
function rollupCandles(candles,interval) {
  const seconds=INTERVALS[interval],buckets=new Map();
  for(const c of [...candles].sort((a,b)=>a.time-b.time)){
    const time=Math.floor(c.time/seconds)*seconds;
    const b=buckets.get(time)||{...c,time,interval,coverage:'PROVIDER_OHLCV_ROLLUP',quality:'PARTIAL',complete:false,samples:0};
    b.high=Math.max(b.high,c.high);b.low=Math.min(b.low,c.low);b.close=c.close;
    b.volume=b.samples===0?c.volume:b.volume==null||c.volume==null?null:b.volume+c.volume;
    b.samples++;b.knownAt=c.knownAt;b.observedAt=c.observedAt;buckets.set(time,b);
  }
  return [...buckets.values()];
}
module.exports={rollupCandles,MarketProvider,DexScreenerMarketProvider,GeckoCandleProvider,observedCandles,INTERVALS};
