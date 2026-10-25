'use strict';
const {createHash}=require('node:crypto');
const VERSION='pullback-watch-v1';
const RULES=Object.freeze({intervalSeconds:300,lookbackBars:24,pivotSeparation:3,lowTolerance:.015,minimumBounce:.03,approachDistance:.05,invalidationDistance:.015,expiresMinutes:120,cooldownMinutes:30,liquidityInvalidation:.2,liquidityConfirmation:.1,maxObservationAgeMs:30000,maxBarAgeMs:600000});
const empirical=x=>x?.provenance?.dataClass==='EMPIRICAL'&&!x.provenance.synthetic&&!x.provenance.testOnly;
const stamp=x=>Date.parse(x),valid=x=>typeof x==='number'&&Number.isFinite(x),hash=x=>createHash('sha256').update(JSON.stringify(x)).digest('hex');
const researchOnly={mode:'PAPER_OBSERVATION',validated:false,automaticExecution:false,netReturn:null,fees:'UNKNOWN',slippage:'UNKNOWN',fill:'NO_EXECUTABLE_FILL_ASSUMED'};
function closedBars(rows,asOf){
 const end=stamp(asOf),minutes=new Map(),bars=new Map();
 for(const c of rows){const seconds=c.interval==='1m'?60:c.interval==='5m'?300:null;
  if(!seconds||!empirical(c)||stamp(c.knownAt)<(c.time+seconds)*1000||c.complete!==true||!['PROVIDER_OHLCV','PROVIDER_OHLCV_ROLLUP_FULL'].includes(c.coverage)||!Number.isSafeInteger(c.time)||c.time%seconds||stamp(c.knownAt)>end||!Number.isFinite(stamp(c.knownAt))||(c.time+seconds)*1000>end||![c.open,c.high,c.low,c.close].every(valid)||c.low<=0||c.high<Math.max(c.open,c.close)||c.low>Math.min(c.open,c.close))continue;
  const map=seconds===60?minutes:bars,old=map.get(c.time);if(!old||stamp(c.knownAt)>stamp(old.knownAt))map.set(c.time,c);
 }
 const groups=new Map();for(const c of minutes.values()){const t=Math.floor(c.time/300)*300,a=groups.get(t)||[];a.push(c);groups.set(t,a);}
 for(const [time,a]of groups){a.sort((a,b)=>a.time-b.time);if(a.length!==5||a.some((c,i)=>c.time!==time+i*60)||new Set(a.map(c=>c.pairId)).size!==1)continue;
  const knownAt=new Date(Math.max(...a.map(c=>stamp(c.knownAt)))).toISOString(),old=bars.get(time);
  if(!old||stamp(knownAt)>stamp(old.knownAt))bars.set(time,{...a[0],time,interval:'5m',open:a[0].open,high:Math.max(...a.map(c=>c.high)),low:Math.min(...a.map(c=>c.low)),close:a.at(-1).close,volume:a.every(c=>valid(c.volume))?a.reduce((s,c)=>s+c.volume,0):null,knownAt,coverage:'PROVIDER_OHLCV_ROLLUP_FULL',provenance:{dataClass:'EMPIRICAL',derived:true,minuteSources:a.map(c=>({time:c.time,provider:c.provider,knownAt:c.knownAt}))},complete:true});
 }
 return [...bars.values()].sort((a,b)=>a.time-b.time);
}
function latestObservation(rows,asOf,pairId){return rows.filter(o=>empirical(o)&&o.pairId===pairId&&stamp(o.observedAt)<=stamp(asOf)&&stamp(o.knownAt)<=stamp(asOf)).sort((a,b)=>stamp(a.observedAt)-stamp(b.observedAt)).at(-1);}
function coverage({bars,observation,risk,asOf}){
 const missing=[];if(bars.length<RULES.lookbackBars)missing.push('At least two hours of completed five-minute provider candles are needed.');
 if(!bars.length||stamp(asOf)-(bars.at(-1).time+300)*1000>RULES.maxBarAgeMs)missing.push('Recent completed candles are unavailable.');
 if(!observation||!valid(observation.priceUsd)||observation.priceUsd<=0||stamp(asOf)-stamp(observation.observedAt)>RULES.maxObservationAgeMs)missing.push('A recent price observation is needed.');
 if(!observation||!valid(observation.liquidityUsd)||observation.liquidityUsd<=0)missing.push('A positive, observed trading-pool value is needed.');
 if(!risk||risk==='UNKNOWN')missing.push('A current distribution assessment is needed.');
 return missing;
}
function discover({tokenAddress,pairId,candles,observations,risk,transactions=[],asOf}){
 if(!Number.isFinite(stamp(asOf)))throw Error('invalid_zone_time');
 const bars=closedBars(candles.filter(c=>c.pairId===pairId),asOf).slice(-RULES.lookbackBars),observation=latestObservation(observations,asOf,pairId),missing=coverage({bars,observation,risk,asOf});
 if(missing.length||['HIGH','CRITICAL'].includes(risk))return {zone:null,missing:[...missing,...(['HIGH','CRITICAL'].includes(risk)?['Elevated distribution risk blocks new watch zones.']:[])]};
 if(bars.some((c,i)=>i&&c.time-bars[i-1].time!==300))return {zone:null,missing:['This method needs consecutive completed five-minute candles; a recent interval is missing.']};
 const pivots=[];for(let i=1;i<bars.length-1;i++)if(bars[i].low<bars[i-1].low&&bars[i].low<=bars[i+1].low)pivots.push(i);
 for(let b=pivots.length-1;b>0;b--)for(let a=b-1;a>=0;a--){const i=pivots[a],j=pivots[b],one=bars[i],two=bars[j];
  if(j-i<RULES.pivotSeparation||Math.abs(one.low-two.low)/Math.min(one.low,two.low)>RULES.lowTolerance)continue;
  const lower=Math.min(one.low,two.low),upper=Math.max(one.low,two.low)*(1+RULES.lowTolerance),invalidBelow=lower*(1-RULES.invalidationDistance);
  if(!bars.slice(i+1,j).some(c=>c.high>=upper*(1+RULES.minimumBounce)))continue;
  if(bars.slice(j+1).some(c=>c.close<invalidBelow)||observation.priceUsd<lower||observation.priceUsd>upper*(1+RULES.approachDistance))continue;
  const id=hash([VERSION,tokenAddress,pairId,one.time,two.time]),kind=transactions.some(t=>t.type==='SELL'&&stamp(t.blockTime)<=stamp(asOf)&&stamp(t.knownAt)<=stamp(asOf))?'RE_ENTRY':'ENTRY';
  return {missing:[],zone:{id,version:VERSION,rules:RULES,tokenAddress,pairId,kind,state:'WATCHING',createdAt:asOf,knownAt:asOf,occurredAt:asOf,expiresAt:new Date(stamp(asOf)+RULES.expiresMinutes*60000).toISOString(),lower,upper,invalidBelow,
   why:['Two separated lows appeared in a similar price area.','A completed candle between those lows showed a rebound of at least 3%.','The latest observed price is close to that area.'],
   waiting:['Two new, completed five-minute candles must test and then close above the watch area.','Trading-pool value must remain within 10% of the starting observation.','Recent price and distribution observations must remain available.'],
   unknown:['Provider update timing is not independently verified.','Dollar buy/sell volume, large-holder history and developer identity may be unavailable.','This method has no demonstrated predictive edge.'],
   baseline:{liquidityUsd:observation.liquidityUsd,priceUsd:observation.priceUsd,observedAt:observation.observedAt,knownAt:observation.knownAt,provider:observation.provider},
   inputSnapshot:{candles:bars.map(c=>({time:c.time,open:c.open,high:c.high,low:c.low,close:c.close,volume:c.volume,knownAt:c.knownAt,provider:c.provider,coverage:c.coverage})),risk,hash:hash(bars)},
   provenance:{dataClass:'DERIVED_RESEARCH',namespace:'signal-dashboard-zone-research',methodVersion:VERSION},
   paper:{...researchOnly,checkpoints:[]},lastCheckedAt:asOf}};
 }
 return {zone:null,missing:[],reason:'No qualifying pullback pattern in the available completed candles.'};
}
function evolve(zone,{candles,observations,risk,pairId,asOf}){
 if(!Number.isFinite(stamp(asOf)))throw Error('invalid_zone_time');
 if(zone.version!==VERSION)return null;
 const z=structuredClone(zone);if(stamp(asOf)<stamp(zone.knownAt))return null;
 const bars=closedBars(candles.filter(c=>c.pairId===z.pairId),asOf),o=latestObservation(observations,asOf,z.pairId),recent=bars.filter(c=>c.time*1000>=stamp(z.createdAt));
 const active=['WATCHING','CONDITIONS_OBSERVED'].includes(z.state);let changed=false;
 if(active){let reason=null,state=null;
  if(pairId!==z.pairId){reason='The observed trading pool changed.';state='INVALIDATED';}
  else if(recent.some(c=>c.close<z.invalidBelow)){reason='A completed five-minute candle closed below the invalidation level.';state='INVALIDATED';}
  else if(o&&stamp(asOf)-stamp(o.observedAt)<=RULES.maxObservationAgeMs&&valid(o.liquidityUsd)&&o.liquidityUsd<z.baseline.liquidityUsd*(1-RULES.liquidityInvalidation)){reason='Observed trading-pool value fell at least 20% from the starting observation.';state='INVALIDATED';}
  else if(['HIGH','CRITICAL'].includes(risk)){reason='Distribution risk became high or critical.';state='INVALIDATED';}
  else if(stamp(asOf)>=stamp(z.expiresAt)){reason='The two-hour observation window ended.';state='EXPIRED';}
  if(state){z.state=state;z.closedAt=asOf;z.stateReason=reason;changed=true;}
  else if(z.state==='WATCHING'){
   const missing=coverage({bars,observation:o,risk,asOf}),last=recent.slice(-2);
   const held=last.length===2&&last[1].time-last[0].time===300&&last[0].low<=z.upper&&last[0].low>=z.invalidBelow&&last.every(c=>c.close>=z.upper);
   if(!missing.length&&held&&o.liquidityUsd>=z.baseline.liquidityUsd*(1-RULES.liquidityConfirmation)&&o.priceUsd>=z.upper&&o.priceUsd<=z.upper*(1+RULES.approachDistance)){
    z.state='CONDITIONS_OBSERVED';z.observedAt=asOf;z.stateReason='Two new completed candles tested and held above the area, with the required current observations available.';
    z.paper.reference={priceUsd:o.priceUsd,observedAt:o.observedAt,knownAt:o.knownAt,recordedAt:asOf,provider:o.provider};z.waiting=['The pattern conditions were observed. This remains an unvalidated paper observation.'];changed=true;
   }
  }
 }
 if(z.paper.reference){const ref=z.paper.reference;
  for(const minutes of [15,60]){if(z.paper.checkpoints.some(c=>c.minutes===minutes)||stamp(asOf)<stamp(ref.recordedAt)+minutes*60000)continue;
   const due=stamp(ref.recordedAt)+minutes*60000,eligible=observations.filter(o=>empirical(o)&&o.pairId===z.pairId&&stamp(o.observedAt)>=due&&stamp(o.observedAt)<=due+120000&&stamp(o.observedAt)<=stamp(asOf)&&stamp(o.knownAt)<=stamp(asOf)&&valid(o.priceUsd)&&o.priceUsd>0).sort((a,b)=>stamp(a.observedAt)-stamp(b.observedAt));
   const at=eligible[0];if(!at&&stamp(asOf)<=due+120000)continue;
   const window=observations.filter(o=>empirical(o)&&o.pairId===z.pairId&&stamp(o.observedAt)>=stamp(ref.recordedAt)&&stamp(o.observedAt)<=(at?stamp(at.observedAt):due)&&stamp(o.knownAt)<=stamp(asOf)&&valid(o.priceUsd)&&o.priceUsd>0);
   z.paper.checkpoints.push({minutes,state:at?'OBSERVED':'UNKNOWN',observedAt:at?.observedAt||null,knownAt:asOf,grossPriceChange:at?(at.priceUsd-ref.priceUsd)/ref.priceUsd:null,maximumObservedDecline:at&&window.length?Math.min(0,...window.map(o=>(o.priceUsd-ref.priceUsd)/ref.priceUsd)):null,netReturn:null,fees:'UNKNOWN',slippage:'UNKNOWN',reason:at?'Observed price comparison; not a trade fill or net return.':'No comparable price observation in the two-minute checkpoint window.'});changed=true;
  }
 }
 if(!changed)return null;z.lastCheckedAt=asOf;z.knownAt=asOf;z.occurredAt=asOf;return z;
}
module.exports={VERSION,RULES,closedBars,discover,evolve,coverage};
