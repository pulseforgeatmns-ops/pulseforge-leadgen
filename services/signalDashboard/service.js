'use strict';
const {account}=require('./accounting');
const {metric,knownBy}=require('./quality');
const {peaks,distribution}=require('./analytics');
const {valueTransactions}=require('./quotes');
const {observedCandles,rollupCandles,INTERVALS}=require('./market');
const {id}=require('./wallet');
const {dec,str,mul,fromNumber}=require('./decimal');
class DashboardService {
  constructor(store,{now=()=>new Date()}={}){Object.assign(this,{store,now});this.cache=new Map();}
  async detail(ca) {
    const old=this.cache.get(ca);if(old&&+this.now()-old.at<2000)return old.promise;
    const promise=this.buildDetail(ca).catch(err=>{this.cache.delete(ca);throw err;});
    if(this.cache.size>100)this.cache.clear();this.cache.set(ca,{at:+this.now(),promise});return promise;
  }
  async buildDetail(ca) {
    const asOf=this.now().toISOString(), store=this.store;
    const [watches,transactions,observations,events,annotations,balances,cursors,quotes,statuses]=await Promise.all(
      ['watch','transaction','observation','event','annotation','balance','cursor','quote','status'].map(k=>store.list(k,k==='cursor'||k==='quote'||k==='status'?null:ca,{asOf,ascending:k!=='observation',limit:k==='observation'?10000:5000})));
    const watch=watches[0];if(!watch)return null;
    observations.reverse();
    const current=observations.at(-1)||null;
    const at=+this.now(),balance=balances.at(-1)||null;
    const history=cursors.find(x=>x.key==='history'),latestCursor=cursors.find(x=>x.key==='head');
    const valued=valueTransactions(transactions,quotes);
    // Exhausted paginated history + reconciled finalized balance is still only observed coverage.
    const caughtUp=latestCursor?.caughtUp===true;
    const historyComplete=Boolean(history?.exhausted&&caughtUp&&balance&&at-Date.parse(balance.observedAt)<=60000
      &&transactions.length<5000&&valued.every(t=>t.slot<=balance.slot));
    const ledger=account(valued,{priceUsd:current?.priceUsd!=null?fromNumber(current.priceUsd):null,
      balance:balance?.tokens??null,asOf,historyComplete});
    if(ledger.reconciliation.state!=='MATCH'){ledger.realizedPnl=null;ledger.unrealizedPnl=null;ledger.totalPnl=null;ledger.netCapitalAtRisk=null;}
    if(!transactions.length&&!historyComplete) {
      for(const key of ['tokensRemaining','tokensPurchased','tokensSold','grossCapitalDeployed','grossProceedsRealized','realizedCash','feesUsd','netCashFlow','positionValue','remainingCostBasis'])ledger[key]=null;
    }
    if(balance){ledger.tokensRemaining=balance.tokens;ledger.positionValue=current?.priceUsd!=null?str(mul(dec(balance.tokens),dec(fromNumber(current.priceUsd)))):null;}
    ledger.quality='PARTIAL'; // Indexer history, USD estimates and unknown fees remain disclosed.
    const riskProjection=cursors.find(x=>x.key==='risk:'+ca);
    const risk=distribution(observations,events,asOf,riskProjection?.state);
    const firstEntry=valued.filter(t=>t.type==='BUY').sort((a,b)=>Date.parse(a.blockTime)-Date.parse(b.blockTime))[0]?.blockTime||null;
    const firstCall=events.filter(e=>['CALLER','FIRST_CALLER'].includes(e.category)).sort((a,b)=>Date.parse(a.occurredAt)-Date.parse(b.occurredAt))[0]?.occurredAt||null;
    const peak=peaks(observations,firstEntry,firstCall,asOf);
    if(store.peakChronology){[peak.postEntry,peak.postCall]=await Promise.all([store.peakChronology(ca,firstEntry,asOf),store.peakChronology(ca,firstCall,asOf)]);}
    const market={};
    for(const field of ['priceUsd','marketCapUsd','liquidityUsd','volume5mUsd','volume24hUsd','change24hPercent','buyVolume5mUsd','sellVolume5mUsd','buyers5m','sellers5m','buys5m','sells5m','tokenAgeSeconds'])market[field]=metric(current?.[field]??null,current||{},at);
    const accounting={};
    for(const key of ['positionValue','tokensRemaining','grossCapitalDeployed','grossProceedsRealized','realizedCash','netCashFlow','tokensPurchased','tokensSold','weightedAverageAcquisitionPrice','remainingCostBasis','realizedPnl','unrealizedPnl','totalPnl','feesUsd','netCapitalAtRisk'])
      accounting[key]=metric(ledger[key],{provider:'observed-wallet FIFO',observedAt:key==='positionValue'?current?.observedAt:balance?.observedAt,partial:true,reason:ledger.issues.join(', ')||'Observed indexer history and USD conversion estimates'},at);
    const markers=ledger.transactions.map(t=>{
      const atEvents=events.filter(e=>knownBy(e,t.blockTime));
      const historicalRisk=[...events.filter(e=>e.category.startsWith('DISTRIBUTION_')),...risk.transitions].filter(e=>knownBy(e,t.blockTime)).at(-1);
      const historicalRiskState=historicalRisk?.category.replace('DISTRIBUTION_','')||'UNKNOWN';
      const research=atEvents.filter(e=>e.researchState).at(-1);
      return {id:t.id,category:t.type,occurredAt:t.blockTime,knownAt:t.knownAt,time:Math.floor(Date.parse(t.blockTime)/1000),
        source:'solana finalized transaction',evidence:{signature:t.signature,tokenAmount:t.tokenAmount,valueUsd:t.valueUsd,
          quoteAmount:t.quoteAmount,quoteAsset:t.quoteAsset,executionPriceUsd:t.executionPriceUsd,feesUsd:t.feesUsd,positionAfter:t.positionAfter,
          lifecycle:t.lifecycle,signalState:research?.researchState||'UNKNOWN',distributionRisk:historicalRiskState},
        provenance:{provider:t.provenance.provider,commitment:t.provenance.commitment,valuation:t.valuation||null}};
    });
    const milestones=ledger.transactions.filter((t,i,all)=>t.lifecycle==='DE_RISK'&&all[i-1]?.lifecycle!=='DE_RISK').map(t=>({
      id:id(['de-risk',t.id]),category:'DE_RISK_MILESTONE',occurredAt:t.blockTime,knownAt:asOf,source:'FIFO trade analytics',
      evidence:'Observed proceeds cover observed deployed capital and observable fees',provenance:{transactionSignature:t.signature,quality:ledger.quality,derived:true}}));
    const timeline=[...events,...markers,...milestones,...annotations,...risk.transitions].sort((a,b)=>Date.parse(a.occurredAt)-Date.parse(b.occurredAt)||String(a.id).localeCompare(String(b.id)));
    const research=events.filter(e=>e.researchState).at(-1);
    return {tokenAddress:ca,symbol:current?.symbol||watch.symbol||ca.slice(0,8),walletConfigured:true,asOf,lastUpdated:current?.observedAt||balance?.observedAt||null,
      market,accounting,peak,risk,signalState:metric(research?.researchState||null,{provider:'signal empirical events',partial:true,observedAt:research?.knownAt},at),
      lifecycle:ledger.lifecycle,ledger:{...ledger,transactions:undefined,rawLots:undefined},
      coverage:{historyExhausted:history?.exhausted||false,headCaughtUp:caughtUp,indexerCoverage:'PARTIAL',reconciliation:ledger.reconciliation,
        walletObservedAt:balance?.observedAt||null,balanceSlot:balance?.slot||null,transactionLimitReached:transactions.length>=5000,
        observationWindowTruncated:observations.length>=10000,telegram:statuses.find(x=>x.component==='feed')||{state:'UNKNOWN'},statuses},
      // Bounded initial mobile payload; full paginated evidence is available through subresources.
      events:timeline.slice(-250),eventCount:timeline.length,pollAfterMs:10000,chartIntervals:Object.keys(INTERVALS),readOnly:true};
  }
  async positions(){
    const watches=await this.store.list('watch',null,{limit:100});
    const positions=[];
    for(const w of watches){const p=await this.detail(w.tokenAddress);if(p)positions.push({tokenAddress:p.tokenAddress,symbol:p.symbol,accounting:p.accounting,risk:p.risk.state,drawdown:p.peak.postEntry.drawdown,lifecycle:p.lifecycle,asOf:p.asOf});}
    // Operational CA events only. No cohort outcomes, returns, precision or performance are read.
    const events=await this.store.list('event',null,{ascending:false,limit:200});
    const seen=new Set(),liveSignals=[];
    for(const e of events){if(seen.has(e.tokenAddress)||liveSignals.length>=30)continue;seen.add(e.tokenAddress);
      const observation=await this.store.latest('observation',e.tokenAddress);
      liveSignals.push({tokenAddress:e.tokenAddress,source:e.source,occurredAt:e.occurredAt,researchState:e.researchState||'UNKNOWN',
        marketCap:metric(observation?.marketCapUsd??null,observation||{},+this.now()),liquidity:metric(observation?.liquidityUsd??null,observation||{},+this.now()),convergence:events.some(x=>x.tokenAddress===e.tokenAddress&&x.category==='INDEPENDENT_CONVERGENCE')?'INDEPENDENT_CONVERGENCE':'UNKNOWN',risk:positions.find(p=>p.tokenAddress===e.tokenAddress)?.risk||'UNKNOWN',ageSeconds:Math.max(0,(+this.now()-Date.parse(e.occurredAt))/1000),performanceBlinded:true});}
    return {positions,liveSignals,asOf:this.now().toISOString(),pollAfterMs:10000,readOnly:true};
  }
  async transactions(ca){const [txs,quotes]=await Promise.all([this.store.list('transaction',ca),this.store.list('quote',null)]);
    return valueTransactions(txs,quotes).map(({provenance,...t})=>({...t,provenance:{provider:provenance.provider,commitment:provenance.commitment}}));}
  async candles(ca,interval){
    if(!INTERVALS[interval])throw new Error('invalid_interval');
    const asOf=this.now().toISOString(), [observations,stored]=await Promise.all([
      this.store.list('observation',ca,{asOf,ascending:false,limit:10000}),this.store.list('candle',ca,{asOf,ascending:false,limit:10000})]);
    const pair=observations[0]?.pairId,latest=new Map();
    for(const c of stored.filter(c=>c.interval===interval&&c.pairId===pair).reverse())latest.set(c.time,c);
    if(interval!=='1m'){
      const minuteRevisions=new Map();for(const c of stored.filter(c=>c.interval==='1m'&&c.pairId===pair).reverse())minuteRevisions.set(c.time,c);
      for(const c of rollupCandles([...minuteRevisions.values()],interval))if(!latest.get(c.time)?.complete)latest.set(c.time,c);
    }
    const sampled=observedCandles(observations.filter(o=>o.pairId===pair),interval,asOf);
    // Samples never overwrite a provider OHLCV candle or imply all trades were observed.
    for(const c of sampled)if(!latest.has(c.time))latest.set(c.time,c);
    return {interval,candles:[...latest.values()].sort((a,b)=>a.time-b.time).slice(-500),asOf,quality:latest.size?'PARTIAL':'UNKNOWN',
      sourceFreshness:'UNKNOWN',emptyIntervals:'UNAVAILABLE',pollAfterMs:10000};
  }
  async annotate(ca,text,userId){
    if(typeof text!=='string'||!text.trim()||text.length>2000)throw new Error('invalid_annotation');
    if(!await this.store.latest('watch',ca))throw new Error('position_not_found');
    const now=this.now().toISOString(),annotation={id:id([ca,now,userId,text]),tokenAddress:ca,category:'OPERATOR_NOTE',
      occurredAt:now,knownAt:now,source:'operator',evidence:text.trim(),provenance:{type:'OPERATOR_ANNOTATION',userId:String(userId)}};
    await this.store.put('annotation',ca,annotation.id,annotation);this.cache.delete(ca);return annotation;
  }
}
module.exports={DashboardService};
