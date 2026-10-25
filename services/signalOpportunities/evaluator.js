'use strict';
const {DEFINITION:D,VERSION_HASH,hash}=require('./definition');
const {clusterEvidence,sequence}=require('./graph');
const {promotionIntensity}=require('../signalDashboard/events');
const num=x=>typeof x==='number'&&Number.isFinite(x)&&x>=0?x:null;
function empirical(x,asOf,time='knownAt'){return x?.provenance?.dataClass==='EMPIRICAL'&&!x.provenance.synthetic&&!x.provenance.testOnly
 &&Number.isFinite(Date.parse(x[time]))&&Number.isFinite(Date.parse(x.occurredAt||x.observedAt))&&Date.parse(x[time])<=Date.parse(asOf)&&Date.parse(x.occurredAt||x.observedAt)<=Date.parse(x[time])&&Date.parse(x[time])<=Date.parse(asOf);}
function evaluateOpportunity({tokenAddress,calls=[],observations=[],events=[],sources=[],relationships=[],asOf=new Date().toISOString(),risk=null,previous=null}){
 const end=Date.parse(asOf);if(!Number.isFinite(end))throw Error('invalid_opportunity_time');
 const registry=new Map(sources.map(s=>[s.sourceId,s]));
 const accepted=calls.filter(c=>c.tokenAddress===tokenAddress&&empirical(c,asOf,'knowledgeAt')&&registry.get(c.sourceId)?.identityStatus==='VERIFIED');
 if(!accepted.length)return null;
 const dedupe=new Map();for(const c of accepted)dedupe.set(c.sourceId+':'+c.externalMessageId,c);
 const all=[...dedupe.values()].sort((a,b)=>Date.parse(a.knowledgeAt)-Date.parse(b.knowledgeAt));
 const recent=all.filter(c=>end-Date.parse(c.occurredAt)<=D.windowMs&&end-Date.parse(c.knowledgeAt)<=D.windowMs);
 const rows=observations.filter(o=>o.tokenAddress===tokenAddress&&empirical(o,asOf)).sort((a,b)=>Date.parse(a.observedAt)-Date.parse(b.observedAt));
 const latest=rows.at(-1),fresh=latest&&end-Date.parse(latest.observedAt)<=D.maxMarketAgeMs;
 const current=fresh?latest:null;
 const base=rows.filter(o=>current&&o.pairId===current.pairId&&Date.parse(current.observedAt)-Date.parse(o.observedAt)>=240000&&Date.parse(current.observedAt)-Date.parse(o.observedAt)<=420000).at(-1);
 const graph=clusterEvidence(recent,sources,relationships,asOf),increases=[],decreases=[],unknowns=[];
 let score=0;const add=(code,points,text,dimension)=>{score+=points;(points>=0?increases:decreases).push({code,points,text,dimension});};
 if(recent.length)add('EMPIRICAL_CALL',D.weights.observed,'A verified source posted this contract address.','source');else unknowns.push('No caller observation in the last 30 minutes.');
 if(graph.independentClusters.length>=2)add('INDEPENDENT_CONVERGENCE',D.weights.independent,'At least two source clusters have recorded independence evidence.','convergence');
 else unknowns.push('Independent source convergence is not established.');
 const span=recent.length?Math.max(...recent.map(c=>Date.parse(c.occurredAt)))-Math.min(...recent.map(c=>Date.parse(c.occurredAt))):null;
 if(graph.independentClusters.length>=2&&span<=D.rapidMs)add('RAPID_CONVERGENCE',D.weights.rapid,'Independent source activity converged within 12 minutes.','convergence');
 if(graph.correlatedEcho)add('CORRELATED_ECHO',D.weights.echo,'Repeated, forwarded, or related posts do not add independent confirmation.','source');
 if(num(current?.liquidityUsd)!==null&&current.liquidityUsd>=D.minLiquidity)add('LIQUIDITY_THRESHOLD',D.weights.liquidity,'Observed pool value meets the $20,000 V1 threshold; exit depth is not proven.','liquidity');
 else unknowns.push(current?.liquidityUsd!=null?'Pool value is below the V1 threshold.':'Current pool liquidity is unknown.');
 const ratio=(a,b)=>num(a)!==null&&num(b)>0?a/b-1:null;
 const priceMomentum=ratio(current?.priceUsd,base?.priceUsd),volumeMomentum=ratio(current?.volume5mUsd,base?.volume5mUsd),buyerMomentum=ratio(current?.buyers5m,base?.buyers5m),liquidityChange=ratio(current?.liquidityUsd,base?.liquidityUsd);
 if(priceMomentum!==null&&priceMomentum>=D.minMomentum&&priceMomentum<D.maxExtension)add('PRICE_ACCELERATION',D.weights.price,'Price increased 3% to 60% against a comparable five-minute observation.','price');
 if(priceMomentum>=D.maxExtension)add('VERTICAL_EXTENSION',D.weights.extended,'Price is already at least 60% above the comparable five-minute observation.','price');
 if(volumeMomentum!==null&&volumeMomentum>=D.volumeRatio-1)add('VOLUME_ACCELERATION',D.weights.volume,'Rolling five-minute traded volume increased at least 25%.','volume');
 if(buyerMomentum!==null&&buyerMomentum>=0.25)add('BUYER_ACCELERATION',D.weights.buyers,'Observed unique buyers increased at least 25%.','buyers');
 if(liquidityChange!==null&&liquidityChange<=-0.2)add('LIQUIDITY_DETERIORATION',D.weights.liquidityLoss,'Observed pool value declined at least 20%.','liquidity');
 const promotion=promotionIntensity(events,asOf);
 if(promotion.rising&&(priceMomentum!==null&&priceMomentum<0||liquidityChange!==null&&liquidityChange<-0.1))add('PROMOTION_DIVERGENCE',D.weights.promotionDivergence,'Promotion rose while price or pool value deteriorated.','promotion');
 // Only empirical, time-bounded risk evaluations may affect the score.
 const credibleRisk=risk&&empirical(risk,asOf)&&end-Date.parse(risk.knownAt)<=D.maxMarketAgeMs?risk:null;
 const avoid=credibleRisk&&['HIGH','CRITICAL'].includes(credibleRisk.state)||!credibleRisk&&previous?.opportunityState==='AVOID';
 if(avoid)add('DISTRIBUTION_RISK',D.weights.distribution,credibleRisk?'Distribution V1 records strong evidence against entry.':'An earlier strong distribution warning remains unresolved; current evidence is unavailable.','distribution');
 for(const [known,text]of [[current,'Current market snapshot is unavailable or delayed.'],[base,'Comparable five-minute momentum baseline is missing.'],[num(current?.buyers5m)!==null,'Unique buyers and sellers are unknown; transaction counts are not buyers.']])if(!known)unknowns.push(text);
 unknowns.push('Bundled ownership, holder concentration, developer identity and whale distribution are unknown.','Retail saturation and future amplifier arrival are unknown.','Canonical source quality is unavailable to this blinded live lane.');
 if(!current?.providerTimestamp)unknowns.push('The market provider does not disclose its own update timestamp.');
 if(graph.unknownPairs.length)unknowns.push(`${graph.unknownPairs.length} source-cluster relationships remain UNKNOWN.`);
 const favorable=new Set(increases.filter(x=>x.dimension!=='source').map(x=>x.dimension));
 const opportunityScore=Math.max(0,Math.min(100,score));
 let opportunityState=recent.length>1||opportunityScore>=D.watch?'WATCH':'OBSERVED';
 if(fresh&&recent.length&&opportunityScore>=D.opportunity&&favorable.size>=3)opportunityState='OPPORTUNITY';
 if(fresh&&graph.independentClusters.length>=3&&opportunityScore>=D.hot&&favorable.size>=5&&!decreases.length)opportunityState='HOT';
 if(avoid)opportunityState='AVOID';
 const amplifierSources=sources.filter(s=>s.sourceRole==='AMPLIFIER'),seen=new Set(all.map(c=>c.sourceId));
 const amplifiers=amplifierSources.map(s=>({sourceId:s.sourceId,status:seen.has(s.sourceId)?'OBSERVED':s.active&&s.available?'NOT_YET_OBSERVED':'COVERAGE_UNKNOWN'}));
 const front=all.find(c=>c.sourceId==='telegram-front-runners');
 const beforeFR=front?all.filter(c=>Date.parse(c.knowledgeAt)<Date.parse(front.knowledgeAt)&&Date.parse(c.occurredAt)<Date.parse(front.occurredAt)):[];
 const frCoverage=amplifiers.find(a=>a.sourceId==='telegram-front-runners')?.status;
 const preFront=beforeFR.length>0||!front&&frCoverage==='NOT_YET_OBSERVED'&&all[0].sourceId!=='telegram-front-runners';
 const callerSequence=sequence(all,rows,asOf).map(x=>({...x,tokenAddress}));
 const result={tokenAddress,symbol:current?.symbol||null,firstSeenAt:all[0].knowledgeAt,knowledgeAt:asOf,occurredAt:asOf,knownAt:asOf,
  definitionVersion:D.version,definitionHash:VERSION_HASH,sourceUniverseVersion:sources[0]?.universeVersion||null,
  sources:[...new Set(all.map(c=>c.sourceId))],independentClusters:graph.independentClusters,sourceRoles:[...new Set(all.map(c=>c.sourceRole))],
  marketCap:num(current?.marketCapUsd),liquidity:num(current?.liquidityUsd),tokenAge:num(current?.tokenAgeSeconds),priceMomentum,volumeMomentum,
  buyers:num(current?.buyers5m),sellers:num(current?.sellers5m),promotionIntensity:promotion,distributionRisk:credibleRisk?.state||'UNKNOWN',
  evidence:{increases,decreases,market:current||null,momentumBaseline:base||null,risk:credibleRisk||null,relationshipEvidence:relationships.filter(r=>r.evidence&&(!r.knowledgeAt||Date.parse(r.knowledgeAt)<=end))},unknowns,opportunityState,opportunityScore,graph,callerSequence,amplifiers,
  preFrontRunners:preFront,frontRunnersLeadMs:beforeFR.length?Date.parse(front.occurredAt)-Date.parse(beforeFR[0].occurredAt):null,
  amplifierPotential:'Arrival is not predicted.',quality:fresh?'PARTIAL':'DELAYED',readOnly:true,
  provenance:{dataClass:'EMPIRICAL',derived:true,namespace:'signal-opportunities-live',researchEvaluation:false}};
 result.id=hash([tokenAddress,D.version,asOf]);return result;
}
module.exports={evaluateOpportunity,empirical};
