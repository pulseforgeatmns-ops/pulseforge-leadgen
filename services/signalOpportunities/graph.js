'use strict';
// Unknown edges never establish independence. Independence must hold for every
// pair of the returned cluster set, after collapsing all correlation edges.
function clusterEvidence(calls,sources,relationships,asOf){
 const available=relationships.filter(r=>r.evidence&&(!r.knowledgeAt||Date.parse(r.knowledgeAt)<=Date.parse(asOf)));
 const parent=new Map();const find=x=>{if(!parent.has(x))parent.set(x,x);if(parent.get(x)!==x)parent.set(x,find(parent.get(x)));return parent.get(x);};
 const join=(a,b)=>{parent.set(find(a),find(b));};
 for(const s of sources){find(s.sourceId);if(s.clusterId)join(s.sourceId,'cluster:'+s.clusterId);}
 for(const r of available)if(r.status==='CORRELATED')join(r.a,r.b);
 const direct=calls.filter(c=>c.sourceRole!=='MACHINE_INTELLIGENCE'&&!c.forwarding&&!c.forwardedFrom);
 const groups=new Map();for(const c of direct){const k=find(c.sourceId);if(!groups.has(k))groups.set(k,[]);groups.get(k).push(c.sourceId);}
 const entries=[...groups].map(([key,members])=>({key,members:[...new Set(members)]})).sort((a,b)=>a.key.localeCompare(b.key));
 const independent=(a,b)=>a.members.some(x=>b.members.some(y=>available.some(r=>r.status==='INDEPENDENT'&&(r.a===x&&r.b===y||r.a===y&&r.b===x))));
 // Bounded exact maximum clique for the <= 21 source V1 universe.
 let best=[];function visit(chosen,remaining){if(chosen.length+remaining.length<=best.length)return;if(chosen.length>best.length)best=chosen;
  for(let i=0;i<remaining.length;i++){const n=remaining[i];visit([...chosen,n],remaining.slice(i+1).filter(m=>independent(n,m)));}}
 visit([],entries);
 return {independentClusters:best.length>=2?best.map(g=>g.key):[],observedClusterCount:entries.length,
  unknownPairs:entries.flatMap((a,i)=>entries.slice(i+1).filter(b=>!independent(a,b)).map(b=>[a.key,b.key])),
  correlatedEcho:calls.filter(c=>c.sourceRole!=='MACHINE_INTELLIGENCE').length>entries.length,
  forwardingObserved:calls.some(c=>c.forwarding||c.forwardedFrom)};
}
function sequence(calls,observations,asOf){
 const end=Date.parse(asOf),first=new Map();
 for(const c of [...calls].sort((a,b)=>Date.parse(a.occurredAt)-Date.parse(b.occurredAt)||a.id.localeCompare(b.id))) {
  if(first.has(c.sourceId)||Date.parse(c.knowledgeAt)>end)continue;
  const ms=observations.filter(o=>o.tokenAddress===c.tokenAddress&&o.provenance?.dataClass==='EMPIRICAL'&&Date.parse(o.knownAt)<=Date.parse(c.knowledgeAt)&&Date.parse(o.observedAt)<=Date.parse(c.occurredAt)&&Date.parse(c.occurredAt)-Date.parse(o.observedAt)<=90000).sort((a,b)=>Date.parse(b.observedAt)-Date.parse(a.observedAt))[0];
  first.set(c.sourceId,{sourceId:c.sourceId,role:c.sourceRole,clusterId:c.clusterId,occurredAt:c.occurredAt,knowledgeAt:c.knowledgeAt,
   marketCap:ms?.marketCapUsd??null,marketCapBasis:ms?'PRIOR_EMPIRICAL_SNAPSHOT':'UNKNOWN',forwarding:c.forwarding||null,externalMessageId:c.externalMessageId,provenance:c.provenance});
 }
 return [...first.values()];
}
function leadLag(sequences,{minSamples=10}={}){
 const pairs=new Map();
 for(const seq of sequences){for(let i=0;i<seq.length;i++)for(let j=i+1;j<seq.length;j++){
  const a=seq[i],b=seq[j];if(a.forwarding||b.forwarding||a.role==='MACHINE_INTELLIGENCE'||b.role==='MACHINE_INTELLIGENCE'||a.sourceId===b.sourceId)continue;
  const delta=Date.parse(b.occurredAt)-Date.parse(a.occurredAt);if(delta===0)continue;
  const names=[a.sourceId,b.sourceId].sort(),key=names.join(':');const p=pairs.get(key)||{a:names[0],b:names[1],samples:[],tokens:new Set()};
  const token=a.tokenAddress||seq.tokenAddress;if(token&&p.tokens.has(token))continue;if(token)p.tokens.add(token);
  p.samples.push(a.sourceId===names[0]?delta:-delta);pairs.set(key,p);
 }}
 return [...pairs.values()].map(p=>{const wins=p.samples.filter(x=>x>0).length,ordered=[...p.samples].sort((a,b)=>a-b),n=ordered.length;return {
  a:p.a,b:p.b,sampleSize:n,aPrecedesBRate:wins/n,medianLagMs:ordered[Math.floor(n/2)],
  confidence:n<minSamples?'INSUFFICIENT':Math.max(wins,n-wins)/n>=0.8?'REPEATED_ASSOCIATION':'MIXED',causality:'NOT_ESTABLISHED'};});
}
module.exports={clusterEvidence,sequence,leadLag};
