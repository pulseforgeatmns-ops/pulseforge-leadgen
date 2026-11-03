'use strict';
// Read-only prospective cohort report. Input is retained live events and snapshots.
// No historical imports, holdout access, order placement, or scoring mutations.
const {evaluatePostConfirmation}=require('./postConfirmationResearch');
const VERSION='signal-prospective-post-confirmation-v1';
function time(value){const t=Date.parse(value);return Number.isFinite(t)?t:null;}
function reportProspectiveEntries({calls=[],observations=[],asOf,delayMs=60000,maxEntryLagMs=120000,feeBps=50,slippageBps=100,liquidityFloorUsd=10000}={}){
 const now=time(asOf);if(now===null)throw Error('as_of_required');
 const byToken=new Map();
 for(const c of calls){
  if(c.dataClass==='HISTORICAL_BACKFILL'||c.partition==='HISTORICAL_HOLDOUT'||c.partition==='HISTORICAL_DISCOVERY')continue;
  if(c.category!=='CALLER'||!c.tokenAddress||!time(c.knownAt)||time(c.knownAt)>now)continue;
  const a=byToken.get(c.tokenAddress)||[];a.push(c);byToken.set(c.tokenAddress,a);
 }
 const obsBy=new Map();
 for(const o of observations){
  if(o.dataClass==='HISTORICAL_BACKFILL'||!o.tokenAddress||time(o.knownAt)===null||time(o.knownAt)>now)continue;
  const a=obsBy.get(o.tokenAddress)||[];a.push(o);obsBy.set(o.tokenAddress,a);
 }
 const records=[],exclusions={NO_AMPLIFIER:0,UNOBSERVABLE_CONFIRMATION:0,NO_EXECUTABLE_ENTRY:0,INSUFFICIENT_MARKET_DATA:0};
 for(const [tokenAddress,events] of byToken){
  const sorted=events.slice().sort((a,b)=>time(a.knownAt)-time(b.knownAt));
  const first=sorted.find(x=>x.sourceRole!=='AMPLIFIER');
  if(!first)continue;
  const amplifier=sorted.find(x=>x.sourceRole==='AMPLIFIER'&&time(x.knownAt)>=time(first.knownAt));
  if(!amplifier){exclusions.NO_AMPLIFIER++;continue;}
  const knownAt=amplifier.knownAt;
  const observedAt=amplifier.occurredAt;
  if(time(observedAt)===null||time(knownAt)===null||time(knownAt)<time(observedAt)){exclusions.UNOBSERVABLE_CONFIRMATION++;continue;}
  // Reuse guarded evaluator with an in-memory discovery-only adapter; never persist this record.
  const result=evaluatePostConfirmation({record:{dataClass:'HISTORICAL_BACKFILL',partition:'HISTORICAL_DISCOVERY',tokenAddress},confirmation:{observedAt,knownAt},observations:obsBy.get(tokenAddress)||[],delayMs,maxEntryLagMs,feeBps,slippageBps,liquidityFloorUsd});
  if(result.status!=='EVALUATED'){exclusions[result.status]=(exclusions[result.status]||0)+1;continue;}
  const outcomes={};
  for(const [h,v] of Object.entries(result.outcomes)){
   const matured=now>=time(result.entryAt)+Number.parseInt(h,10)*60000;
   outcomes[h]=matured?v:{status:'PENDING'};
   if(matured&&v.status==='INSUFFICIENT_MARKET_DATA')exclusions.INSUFFICIENT_MARKET_DATA++;
  }
  records.push({tokenAddress,firstKnownAt:first.knownAt,amplifierSource:amplifier.source,amplifierKnownAt:knownAt,entryAt:result.entryAt,entryPriceUsd:result.entryPriceUsd,outcomes,quality:result.quality});
 }
 const summary={};
 for(const h of ['10m','30m','60m']){
  const complete=records.map(r=>r.outcomes[h]).filter(o=>o.status==='OBSERVED_PARTIAL');
  summary[h]={observed:complete.length,positive:complete.filter(o=>o.netLastPct>0).length,hit2x:complete.filter(o=>o.reached2x).length,pending:records.filter(r=>r.outcomes[h].status==='PENDING').length};
 }
 return {version:VERSION,asOf,mode:'PROSPECTIVE_READ_ONLY',quality:'OBSERVATION_BASED_NOT_EXECUTION_PROOF',eligibleEntries:records.length,exclusions,summary,records};
}
module.exports={VERSION,reportProspectiveEntries};
