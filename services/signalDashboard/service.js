'use strict';
const {quoteAccount}=require('./quoteAccounting');
const {USDC}=require('./wallet');
const {account}=require('./accounting');
const {metric,knownBy}=require('./quality');
const {peaks,distribution}=require('./analytics');
const {valueTransactions}=require('./quotes');
const {observedCandles,rollupCandles,INTERVALS}=require('./market');
const {id}=require('./wallet');
const {dec,str,mul,div,fromNumber}=require('./decimal');
class DashboardService {
  constructor(store,{now=()=>new Date(),zoneStore=null,opportunities=null}={}){Object.assign(this,{store,now,zoneStore,opportunities});this.cache=new Map();}
  async detail(ca) {
    const old=this.cache.get(ca);if(old&&+this.now()-old.at<2000)return old.promise;
    const promise=this.buildDetail(ca).catch(err=>{this.cache.delete(ca);throw err;});
    if(this.cache.size>100)this.cache.clear();this.cache.set(ca,{at:+this.now(),promise});return promise;
  }
  async buildDetail(ca) {
    const asOf=this.now().toISOString(), store=this.store;
    const [watches,transactions,observations,events,annotations,balances,cursors,quotes,statuses]=await Promise.all(
      ['watch','transaction','observation','event','annotation','balance','cursor','quote','status'].map(k=>store.list(k,k==='cursor'||k==='quote'||k==='status'?null:ca,{asOf,ascending:k!=='observation',limit:k==='observation'?10000:5000})));
    const opportunity=this.opportunities?await this.opportunities.latest(ca,asOf):null;
    const paperEvents=await store.list('paper','*',{asOf,ascending:true,limit:10000}),paperToken=paperEvents.filter(e=>e.tokenAddress===ca);
    const watch=watches[0]||(opportunity?{tokenAddress:ca,symbol:opportunity.symbol}:null);if(!watch)return null;
    observations.reverse();
    const current=observations.at(-1)||null;
    const at=+this.now(),balance=balances.at(-1)||null;
    const history=cursors.find(x=>x.key==='history'),latestCursor=cursors.find(x=>x.key==='head');
    const valued=valueTransactions(transactions,quotes);
    // Exhausted paginated history + reconciled finalized balance is still only observed coverage.
    const caughtUp=latestCursor?.caughtUp===true&&at-Date.parse(latestCursor.knownAt)<=120000;
    const historyComplete=Boolean(history?.exhausted&&caughtUp&&balance&&at-Date.parse(balance.observedAt)<=60000
      &&transactions.length<5000&&valued.every(t=>t.slot<=balance.slot));
    const ledger=account(valued,{priceUsd:current?.priceUsd!=null?fromNumber(current.priceUsd):null,
      balance:balance?.tokens??null,asOf,historyComplete});
    if(ledger.reconciliation.state!=='MATCH'){ledger.realizedPnl=null;ledger.unrealizedPnl=null;ledger.totalPnl=null;ledger.netCapitalAtRisk=null;}
    if(!transactions.length&&!historyComplete) {
      for(const key of ['tokensRemaining','tokensPurchased','tokensSold','grossCapitalDeployed','grossProceedsRealized','realizedCash','feesUsd','netCashFlow','positionValue','remainingCostBasis'])ledger[key]=null;
    }
    if(balance){ledger.tokensRemaining=balance.tokens;ledger.positionValue=current?.priceUsd!=null?str(mul(dec(balance.tokens),dec(fromNumber(current.priceUsd)))):null;}
    const quoteMark=quotes.filter(q=>q.asset===USDC&&q.purpose==='CURRENT_MARK'&&q.priceUsd!=null&&at-Date.parse(q.knownAt)<120000).at(-1);
    const priceQuote=current?.priceUsd!=null&&quoteMark?str(div(dec(fromNumber(current.priceUsd)),dec(quoteMark.priceUsd))):null;
    const native=quoteAccount(valued,{asOf,historyComplete,balance:balance?.tokens??null,priceQuote});
    if(native.ledger.reconciliation.state!=='MATCH')for(const key of ['realizedPnl','unrealizedPnl','totalPnl','netCapitalAtRisk'])native.ledger[key]=null;
    if(!transactions.length&&!historyComplete)for(const key of ['grossCapitalDeployed','grossProceedsRealized','realizedCash','positionValue','tokensRemaining','netCashFlow'])native.ledger[key]=null;
    const quoteAccounting={symbol:native.symbol,asset:native.asset,cashBasis:native.cashBasis,feeItemization:native.feeItemization,quality:'PARTIAL'};
    for(const key of ['grossCapitalDeployed','grossProceedsRealized','realizedCash','realizedPnl','unrealizedPnl','totalPnl','netCapitalAtRisk','remainingCostBasis','positionValue','netCashFlow','weightedAverageAcquisitionPrice'])quoteAccounting[key]=metric(native.ledger[key],{provider:'finalized quote-token wallet changes / FIFO',partial:true,observedAt:balance?.observedAt,reason:native.ledger.issues.join(', ')||native.cashBasis},at);
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
    const nativeTransactions=new Map(native.transactions.map(t=>[t.id,t]));
    const nativeAnalytics=native.ledger.realizedPnl!==null;
    const markers=ledger.transactions.map(t=>{
      const atEvents=events.filter(e=>knownBy(e,t.blockTime));
      const historicalRisk=[...events.filter(e=>e.category.startsWith('DISTRIBUTION_')),...risk.transitions].filter(e=>knownBy(e,t.blockTime)).at(-1);
      const historicalRiskState=historicalRisk?.category.replace('DISTRIBUTION_','')||'UNKNOWN';
      const research=atEvents.filter(e=>e.researchState).at(-1);
      return {id:t.id,category:t.type,occurredAt:t.blockTime,knownAt:t.knownAt,time:Math.floor(Date.parse(t.blockTime)/1000),
        source:'solana finalized transaction',evidence:{signature:t.signature,tokenAmount:t.tokenAmount,valueUsd:t.valueUsd,
          quoteAmount:t.quoteAmount,quoteAsset:t.quoteAsset,executionPriceUsd:t.executionPriceUsd,executionPriceQuote:t.executionPriceQuote,feesUsd:t.feesUsd,positionAfter:t.positionAfter,
          lifecycle:nativeAnalytics?nativeTransactions.get(t.id)?.lifecycle||t.lifecycle:t.lifecycle,signalState:research?.researchState||'UNKNOWN',distributionRisk:historicalRiskState},
        provenance:{provider:t.provenance.provider,commitment:t.provenance.commitment,valuation:t.valuation||null}};
    });
    const milestoneTransactions=nativeAnalytics?native.transactions:ledger.transactions;
    const milestones=milestoneTransactions.filter((t,i,all)=>t.lifecycle==='DE_RISK'&&all[i-1]?.lifecycle!=='DE_RISK').map(t=>({
      id:id(['de-risk',t.id]),category:'DE_RISK_MILESTONE',occurredAt:t.blockTime,knownAt:asOf,source:'FIFO trade analytics',
      evidence:'Observed money received covers observed money put in, including fees in the observed cash changes',provenance:{transactionSignature:t.signature,quality:ledger.quality,unit:nativeAnalytics?native.symbol:'USD',derived:true}}));
    const paperMarkers=paperToken.map(e=>({id:'paper:'+e.id,category:e.side==='BUY'?'PAPER_BUY':'PAPER_SELL',occurredAt:e.occurredAt,knownAt:e.knownAt,source:'Signal paper account',evidence:{...e.evidence,fillPrice:e.fillPrice,notionalUsd:e.notionalUsd,tokens:e.tokens,action:e.action},provenance:e.provenance}));
    const timeline=[...events,...markers,...paperMarkers,...milestones,...annotations,...risk.transitions].sort((a,b)=>Date.parse(a.occurredAt)-Date.parse(b.occurredAt)||String(a.id).localeCompare(String(b.id)));
    const research=events.filter(e=>e.researchState).at(-1);
    const recommendation=require('./recommendation').buildRecommendation({risk,current,accounting,asOf});
    return {tokenAddress:ca,symbol:current?.symbol||watch.symbol||ca.slice(0,8),walletConfigured:true,asOf,lastUpdated:current?.observedAt||balance?.observedAt||null,
      market,accounting,quoteAccounting,peak,risk,recommendation,signalState:metric(research?.researchState||null,{provider:'signal empirical events',partial:true,observedAt:research?.knownAt},at),
      lifecycle:quoteAccounting.totalPnl.value!==null?native.ledger.lifecycle:ledger.lifecycle,ledger:{...ledger,transactions:undefined,rawLots:undefined},
      coverage:{historyExhausted:history?.exhausted||false,headCaughtUp:caughtUp,indexerCoverage:'PARTIAL',reconciliation:ledger.reconciliation,
        walletObservedAt:balance?.observedAt||null,balanceSlot:balance?.slot||null,transactionLimitReached:transactions.length>=5000,
        observationWindowTruncated:observations.length>=10000,telegram:statuses.find(x=>x.component==='feed')||{state:'UNKNOWN'},statuses},
      // Bounded initial mobile payload; full paginated evidence is available through subresources.
      events:timeline.slice(-250),eventCount:timeline.length,opportunity,paperPosition:this.paperPosition(paperToken,current),positionTracked:Boolean(watches[0]),researchZones:await this.zones(ca,asOf,statuses,6),pollAfterMs:10000,chartIntervals:Object.keys(INTERVALS),readOnly:true};
  }
  async zones(ca,asOf=this.now().toISOString(),statuses=null,limit=20){
    const {VERSION,RULES}=require('./zones');
    const checked=statuses||(await this.store.list('status','*',{asOf}));const status=checked.find(s=>s.component==='zone-watch:'+ca);
    const rows=this.zoneStore?await this.zoneStore.latest(ca,asOf):[];
    const delayed=status&&Date.parse(asOf)-Date.parse(status.knownAt)>90000;
    return {version:VERSION,rules:RULES,mode:'PAPER_OBSERVATION',validated:false,execution:false,asOf,lastCheckedAt:status?.knownAt||null,quality:delayed?'DELAYED':status?.state||'UNKNOWN',missing:[...(status?.missing||[]),...(delayed?['The research evaluator has not checked recent observations. Saved zones are historical, not a current assessment.']:[])],reason:status?.reason||'The research collector has not evaluated this position yet.',
      counts:{recordedInWindow:rows.length,conditionsObserved:rows.filter(z=>z.paper.reference).length,invalidated:rows.filter(z=>z.state==='INVALIDATED').length,expired:rows.filter(z=>z.state==='EXPIRED').length},
      zones:rows.slice(0,limit).map(({inputSnapshot,...zone})=>zone),windowLimit:100};
  }
  paperPosition(events,current){let tokens=0,cost=0,realized=0;for(const e of events){if(e.side==='BUY'){tokens+=Number(e.tokens);cost+=Number(e.notionalUsd);}else if(e.side==='SELL'&&tokens>0){const basis=cost;realized+=Number(e.notionalUsd)-basis;tokens=0;cost=0;}}const mark=Number(current?.priceUsd),value=tokens&&Number.isFinite(mark)?tokens*mark:null;return {mode:'PAPER',tokens,costBasisUsd:cost,positionValueUsd:value,realizedPnlUsd:realized,unrealizedPnlUsd:value==null?null:value-cost,events:events.slice(-50),quality:events.length?'OBSERVED':'EMPTY'};}
  async paperPortfolio(){const asOf=this.now().toISOString(),events=await this.store.list('paper','*',{asOf,ascending:true,limit:10000}),status=(await this.store.list('status','*',{asOf})).find(s=>s.component==='paper-account')||null;let cash=10000,realized=0,trades=0;const positions=new Map();for(const e of events){if(e.side==='BUY'){cash-=Number(e.notionalUsd);positions.set(e.tokenAddress,{tokenAddress:e.tokenAddress,tokens:Number(e.tokens),costBasisUsd:Number(e.notionalUsd),entryFillPrice:Number(e.fillPrice),enteredAt:e.knownAt});trades++;}else if(e.side==='SELL'){cash+=Number(e.notionalUsd);const p=positions.get(e.tokenAddress);if(p){realized+=Number(e.notionalUsd)-p.costBasisUsd;positions.delete(e.tokenAddress);}trades++;}}let openValue=0;const open=[];for(const p of positions.values()){const o=await this.store.latest('observation',p.tokenAddress);const mark=Number(o?.priceUsd),value=Number.isFinite(mark)?p.tokens*mark:null;if(value!=null)openValue+=value;open.push({...p,markPriceUsd:Number.isFinite(mark)?mark:null,positionValueUsd:value,unrealizedPnlUsd:value==null?null:value-p.costBasisUsd});}return {mode:'PROSPECTIVE_PAPER',startingCashUsd:10000,cashUsd:cash,openPositionValueUsd:openValue,equityUsd:cash+openValue,realizedPnlUsd:realized,totalPnlUsd:cash+openValue-10000,trades,openPositions:open,events:events.slice(-100).reverse(),status,asOf};}
  async positions(){
    const watches=await this.store.list('watch',null,{limit:100});
    const positions=[];
    for(const w of watches){const p=await this.detail(w.tokenAddress);if(p)positions.push({tokenAddress:p.tokenAddress,symbol:p.symbol,market:p.market,accounting:p.accounting,quoteAccounting:p.quoteAccounting,risk:p.risk.state,riskEvidence:{for:p.risk.for,unknown:p.risk.unknown},drawdown:p.peak.postEntry.drawdown,lifecycle:p.lifecycle,signalState:p.signalState,lastUpdated:p.lastUpdated,reconciliation:p.coverage.reconciliation.state,asOf:p.asOf});}
    // Operational CA events only. No cohort outcomes, returns, precision or performance are read.
    const events=await this.store.list('event',null,{ascending:false,limit:200});
    const seen=new Set(),liveSignals=[];
    for(const e of events){if(seen.has(e.tokenAddress)||liveSignals.length>=30)continue;seen.add(e.tokenAddress);
      const observation=await this.store.latest('observation',e.tokenAddress);
      liveSignals.push({tokenAddress:e.tokenAddress,source:e.source,occurredAt:e.occurredAt,researchState:e.researchState||'UNKNOWN',
        marketCap:metric(observation?.marketCapUsd??null,observation||{},+this.now()),liquidity:metric(observation?.liquidityUsd??null,observation||{},+this.now()),convergence:events.some(x=>x.tokenAddress===e.tokenAddress&&x.category==='INDEPENDENT_CONVERGENCE')?'INDEPENDENT_CONVERGENCE':'UNKNOWN',risk:positions.find(p=>p.tokenAddress===e.tokenAddress)?.risk||'UNKNOWN',ageSeconds:Math.max(0,(+this.now()-Date.parse(e.occurredAt))/1000),performanceBlinded:true});}
    return {positions,liveSignals,opportunityDiscovery:await this.discovery(),paperPortfolio:await this.paperPortfolio(),asOf:this.now().toISOString(),pollAfterMs:10000,readOnly:true};
  }
  async discovery(){
    const asOf=this.now().toISOString(),rows=this.opportunities?await this.opportunities.list(asOf):[];
    const statuses=await this.store.list('status','*',{asOf});
    const sourceHealth=statuses.find(s=>s.component==='opportunity-engine')||null,capacity=statuses.find(s=>s.component==='opportunity-capacity')||null;
    const age=sourceHealth?Date.parse(asOf)-Date.parse(sourceHealth.knownAt):Infinity;
    const opportunities=rows.map(p=>({...p,evaluationDelayed:Date.parse(asOf)-Date.parse(p.knowledgeAt)>90000,
      quality:Date.parse(asOf)-Date.parse(p.knowledgeAt)>90000?'DELAYED':p.quality}));
    return {enabled:Boolean(this.opportunities),opportunities,sourceHealth,capacity,leadLag:require('../signalOpportunities/graph').leadLag(opportunities.map(p=>p.callerSequence)),quality:age>90000?'DELAYED':sourceHealth?.state||'UNKNOWN',
      sourcePerformance:statuses.find(s=>s.component==='source-performance')||{state:'UNKNOWN'},delivery:this.opportunities?await this.opportunities.deliveryHealth():[],asOf,readOnly:true};
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
    // Only the currently open bar can incorporate subsequently received real
    // prices. Preserve the provider open/volume and disclose partial coverage.
    for(const c of sampled){const old=latest.get(c.time);if(!old)latest.set(c.time,c);
      else if(!old.complete&&(c.time+INTERVALS[interval])*1000>Date.parse(asOf)&&Date.parse(c.knownAt)>Date.parse(old.knownAt))latest.set(c.time,{...old,high:Math.max(old.high,c.high),low:Math.min(old.low,c.low),close:c.close,knownAt:c.knownAt,observedAt:c.observedAt,complete:false,coverage:'PROVIDER_AND_SAMPLED_OBSERVATIONS',quality:'PARTIAL',volumeObservedAt:old.knownAt});}
    return {interval,candles:[...latest.values()].sort((a,b)=>a.time-b.time).slice(-500),asOf,quality:latest.size?'PARTIAL':'UNKNOWN',
      sourceFreshness:'UNKNOWN',emptyIntervals:'UNAVAILABLE',pollAfterMs:10000};
  }
  async overrideZone(ca,zoneId,{lower,upper},userId){
    lower=Number(lower);upper=Number(upper);if(!Number.isFinite(lower)||!Number.isFinite(upper)||lower<=0||upper<=lower)throw new Error('invalid_zone_bounds');
    if(upper/lower>1.5)throw new Error('zone_too_wide');
    const current=(await this.zoneStore.latest(ca)).find(z=>z.id===zoneId);if(!current)throw new Error('zone_not_found');
    const now=this.now().toISOString(),override={...current,lower,upper,knownAt:now,occurredAt:now,lastCheckedAt:now,
      confidence:null,why:[`Operator replaced the working range with ${lower}–${upper} after reviewing repeated price reversals in this area.`,...(current.why||[])],
      provenance:{...current.provenance,type:'OPERATOR_OVERRIDE',userId:String(userId),replaces:{lower:current.lower,upper:current.upper,knownAt:current.knownAt}},
      operatorOverride:{lower,upper,userId:String(userId),at:now,computedBounds:{lower:current.lower,upper:current.upper}}};
    await this.zoneStore.append(override);this.cache.delete(ca);return override;
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
