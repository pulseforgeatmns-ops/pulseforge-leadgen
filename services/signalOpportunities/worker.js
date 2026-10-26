'use strict';
const {DEFINITION,hash}=require('./definition');
const {SOURCES,RELATIONSHIPS,VERSION}=require('./sourceUniverse');
const {evaluateOpportunity,empirical}=require('./evaluator');
const {feedEvents}=require('../signalDashboard/events');
const {evaluate:distribution}=require('../signalDistribution/risk');
const {deliver}=require('./notifications');
function sourcesFromHealth(health,at){
 const runtime=new Map((health?.sources||[]).map(s=>[s.sourceId,s]));
 return SOURCES.map(s=>{const r=runtime.get(s.sourceId);return {...s,active:r?.active===true,available:r?.available===true,
  correlatedWith:RELATIONSHIPS.filter(x=>x.a===s.sourceId||x.b===s.sourceId).map(x=>x.a===s.sourceId?x.b:x.a),
  identityStatus:r?.identityStatus==='VERIFIED'?'VERIFIED':'CANDIDATE',channelId:r?.channelId||null,verifiedAt:r?.verifiedAt||null,
  reason:r?.reason||(!r?'NOT_CONFIGURED':null),knowledgeAt:at};});
}
function createOpportunityWorker({store,opportunities,feed,marketProvider,transports=[],now=()=>new Date(),maxTokens=120,transportTestId=null,relationshipGraph={version:'relationship-graph-v1.0.0',edges:RELATIONSHIPS}}){
 let running=false,rotation=0;
 const status=async(state,reason,extra={})=>{const at=now().toISOString();await store.put('status','*','opportunity-engine',{component:'opportunity-engine',state,reason,...extra,occurredAt:at,knownAt:at},{mutable:true});};
 async function ingest(payload){
  if(!payload||!Array.isArray(payload.calls)||payload.calls.length>10000)throw Error('invalid_opportunity_feed');
  const at=now().toISOString(),sources=sourcesFromHealth(payload.health,at),registry={};
  for(const s of sources)registry[s.sourceId]={clusterId:s.clusterId,relationship:s.clusterRelationshipStatus};
  const verified=new Map(sources.filter(s=>s.identityStatus==='VERIFIED').map(s=>[s.sourceId,s]));
  let accepted=0;
  const activity=new Map(sources.map(s=>[s.sourceId,{sourceId:s.sourceId,active:s.active,available:s.available,channelId:s.channelId||null,acceptedCalls:0,lastObservedAt:null,lastIngestedAt:null,lastKnowledgeAt:null,lastLatencyMs:null}]));
  for(const call of payload.calls){const s=verified.get(call.sourceId);if(!s||String(call.communityId)!==String(s.channelId))continue;
   const edit=Date.parse(call.provenance?.messageEditAt),ingested=Date.parse(call.ingestedAt),occurred=Date.parse(call.occurredAt);
   if(!Number.isFinite(ingested)||!Number.isFinite(occurred)||Number.isFinite(edit)&&edit>Date.parse(at))continue;
   const knowledgeAt=new Date(Math.max(ingested,occurred,Number.isFinite(edit)?edit:0)).toISOString();
   if(!empirical({...call,knowledgeAt},at,'knowledgeAt'))continue;
   for(const event of feedEvents(call,{now:now(),registry})){
    const e={...event,sourceRole:s.sourceRole,clusterId:s.clusterId,sourceUniverseVersion:VERSION,ingestedAt:call.ingestedAt,
      knowledgeAt:event.knownAt,forwarding:call.forwarding||null,forwardedFrom:call.forwardedFrom||null,
      machineClaims:s.sourceRole==='MACHINE_INTELLIGENCE'?require('./machineClaims').machineClaims(call.rawText||call.text):null,
      // Import knowledge is the latest clock, preserving first live decision time.
      provenance:{...event.provenance,telegramChannelId:s.channelId,telegramMessageId:call.provenance.telegramMessageId,rawText:call.rawText||call.text,
        identityVerifiedAt:s.verifiedAt,originalKnowledgeAt:knowledgeAt}};
    // Edits produce revisions, never overwrite the original raw message.
    e.id=hash([event.id,call.provenance?.messageEditAt||null]);
    await store.put('event',e.tokenAddress,e.id,e);if(e.category==='CALLER')accepted++;
   }
  }
  await status('PARTIAL','Prospective collection only; source and market coverage may be incomplete.',{sources,sourceHealth:payload.health?.expansionHealth||null,medianIngestionLatencyMs:payload.health?.medianIngestionLatencyMs??null,acceptedFromRetainedFeed:accepted,sourceActivity:[...activity.values()],productiveSources:[...activity.values()].filter(x=>x.acceptedCalls>0).length,lastAcceptedCallAt:[...activity.values()].map(x=>x.lastKnowledgeAt).filter(Boolean).sort().at(-1)||null,feedCheckedAt:at});
  await store.put('status','*','feed',{component:'feed',state:'PARTIAL',reason:'Verified source universe; completeness is unknown',occurredAt:at,knownAt:at},{mutable:true});
  return sources;
 }
 async function tick(){
  if(running)return {skipped:true};running=true;
  try{return await store.lock(async()=>{
   if(!feed){await status('UNKNOWN','CALLER_FEED_NOT_CONFIGURED');return;}
   const sources=await ingest(await feed()),asOf=now().toISOString();
   const events=await store.list('event',null,{ascending:false,limit:20000});
   const calls=events.filter(e=>e.category==='CALLER'&&Date.parse(asOf)-Date.parse(e.knownAt)<=86400000);
   const recent=calls.filter(e=>Date.parse(asOf)-Date.parse(e.occurredAt)<=DEFINITION.windowMs);
   const counts=new Map();for(const c of recent)counts.set(c.tokenAddress,(counts.get(c.tokenAddress)||0)+1);
   const priority=[...counts].sort((a,b)=>b[1]-a[1]||a[0].localeCompare(b[0])).slice(0,maxTokens).map(x=>x[0]);
   // The documented public endpoint accepts up to 30 addresses per request.
   // Round robin preserves coverage while sharing existing provider budgets.
   if(priority.length&&marketProvider){const batch=Array.from({length:Math.min(30,priority.length)},(_,i)=>priority[(rotation+i)%priority.length]);rotation=(rotation+batch.length)%priority.length;
    try{const snapshots=marketProvider.getSnapshots?await marketProvider.getSnapshots(batch):[await marketProvider.getSnapshot({tokenAddress:batch[0]})];
     for(const snapshot of snapshots)if(batch.includes(snapshot.tokenAddress))await store.put('observation',snapshot.tokenAddress,hash([snapshot.tokenAddress,snapshot.observedAt]),snapshot);
    }catch{}
   }
   const old=await opportunities.list(asOf),tokens=[...new Set([...priority,...old.map(p=>p.tokenAddress)])].slice(0,200);
   const held=new Set((await store.list('balance',null,{ascending:false,limit:500})).filter(b=>Number(b.tokens)>0&&Date.parse(asOf)-Date.parse(b.observedAt)<90000).map(b=>b.tokenAddress));
   for(const ca of tokens){
    const observations=await store.list('observation',ca,{ascending:false,limit:500});
    const riskCursor=(await store.list('cursor',ca)).find(c=>c.key==='opportunity-risk:'+ca);
    const last=observations[0];let riskState=riskCursor?.state;
    if(last){const ev=distribution(riskState||{},last,{now:now()});if(!ev.ignored){riskState=ev.state;await store.put('cursor',ca,'opportunity-risk:'+ca,{key:'opportunity-risk:'+ca,state:riskState,occurredAt:asOf,knownAt:asOf},{mutable:true});}}
    const risk=last&&riskState?{state:riskState.riskState,occurredAt:last.observedAt,knownAt:last.knownAt,provenance:last.provenance}:null;
    const decision=evaluateOpportunity({tokenAddress:ca,calls:calls.filter(c=>c.tokenAddress===ca),observations,events:events.filter(e=>e.tokenAddress===ca),sources,
      relationships:relationshipGraph.edges.map(r=>({...r,knowledgeAt:r.knowledgeAt||sources.find(s=>s.sourceId===r.a)?.verifiedAt||asOf})),asOf,risk,previous:old.find(p=>p.tokenAddress===ca)});
    if(!decision)continue;
    decision.relationshipVersion=relationshipGraph.version;
    const saved=await opportunities.append(decision,{held:held.has(ca)});
    if(saved.created&&saved.transition){const e={id:hash(['opportunity-state',decision.id]),tokenAddress:ca,category:'OPPORTUNITY_'+decision.opportunityState,occurredAt:decision.knowledgeAt,knownAt:decision.knowledgeAt,source:decision.definitionVersion,evidence:decision,provenance:decision.provenance};await store.put('event',ca,e.id,e);}
   }
   if(transportTestId&&transports.every(t=>t.ready))await opportunities.enqueueTransportTest(transportTestId);
   await deliver(opportunities,transports);
   const at=now().toISOString();await store.put('status','*','opportunity-capacity',{component:'opportunity-capacity',state:counts.size>maxTokens?'PARTIAL':'OBSERVED',
     discoveredTokens:counts.size,evaluatedTokens:tokens.length,marketEnrichmentLimit:maxTokens,deferredTokens:Math.max(0,counts.size-maxTokens),
     sourceCandidateCount:SOURCES.length,liveSources:sources.filter(s=>s.active).length,requiredLiveSources:10,
     coverage:'Bounded retained feed; no completeness claim',alerts:transports.map(t=>({channel:t.channel,configured:t.ready})),occurredAt:at,knownAt:at},{mutable:true});
   return {evaluated:tokens.length};
  },'opportunities');}catch{await status('UNKNOWN','OPPORTUNITY_COLLECTION_UNAVAILABLE');return {unavailable:true};}finally{running=false;}
 }
 return {tick,ingest};
}
module.exports={createOpportunityWorker,sourcesFromHealth};
