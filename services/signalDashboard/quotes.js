'use strict';
const {dec,str,mul,div}=require('./decimal');
const {SOL}=require('./wallet');
class HistoricalQuoteProvider {
  async getQuote(){throw new Error('historical_quote_not_implemented');}
}
class BirdeyeHistoricalQuoteProvider extends HistoricalQuoteProvider {
  constructor({apiKey,budget,fetchFn=globalThis.fetch,now=()=>new Date()}){super();Object.assign(this,{apiKey,budget,fetchFn,now});}
  async getQuote(asset,blockTime) {
    const end=Math.floor(Date.parse(blockTime)/1000);
    await this.budget.take('birdeye-historical',1000);
    const u=new URL('https://public-api.birdeye.so/defi/history_price');
    for(const [k,v]of Object.entries({address:asset,address_type:'token',type:'1m',time_from:end-60,time_to:end}))u.searchParams.set(k,v);
    const r=await this.fetchFn(u.href,{headers:{'X-API-KEY':this.apiKey,'x-chain':'solana'},redirect:'error',signal:AbortSignal.timeout(5000)});
    if(r.status===429){await this.budget.backoff('birdeye-historical',60000);throw new Error('quote_rate_limited');}
    if(!r.ok)throw new Error('quote_unavailable');
    const b=await r.json();
    const points=(b?.success===true&&Array.isArray(b.data?.items)?b.data.items:[]).filter(x=>Number.isSafeInteger(x.unixTime)&&x.unixTime<=end&&x.unixTime>=end-60&&Number.isFinite(x.value)&&x.value>0).sort((a,b)=>a.unixTime-b.unixTime);
    const p=points.at(-1);if(!p)return null;
    return {asset,blockTime,priceUsd:p.value.toFixed(18).replace(/0+$/,'').replace(/\.$/,''),asOf:new Date(p.unixTime*1000).toISOString(),
      provider:'birdeye-historical',quality:'PARTIAL',basis:'Historical minute price observation; USD conversion is an estimate, native consideration remains exact.',
      occurredAt:blockTime,knownAt:this.now().toISOString(),raw:b};
  }
}
function valueTransactions(transactions,quotes) {
  const map=new Map(quotes.map(q=>[`${q.asset}:${q.blockTime}`,q]));
  return transactions.map(t=>{
    const q=map.get(`${t.quoteAsset}:${t.blockTime}`),fee=map.get(`${SOL}:${t.blockTime}`);
    const valid=x=>x&&Date.parse(x.asOf)<=Date.parse(t.blockTime)&&Date.parse(t.blockTime)-Date.parse(x.asOf)<=60000;
    const valueUsd=t.valueUsd??(t.quoteAmount!=null&&valid(q)?str(mul(dec(t.quoteAmount),dec(q.priceUsd))):null);
    const feesUsd=t.feesUsd??(t.feesLamports==='0'?'0':t.feesLamports!=null&&valid(fee)?str(mul(dec(t.feesLamports)*1n/1000000000n,dec(fee.priceUsd))):null);
    return {...t,valueUsd,feesUsd,executionPriceUsd:str(div(dec(valueUsd),dec(t.tokenAmount))),
      valuationQuality:t.valueUsd!=null?'PARTIAL':q?.quality||'UNKNOWN',valuation:q||null,feeValuation:fee||null};
  });
}
module.exports={HistoricalQuoteProvider,BirdeyeHistoricalQuoteProvider,valueTransactions};
