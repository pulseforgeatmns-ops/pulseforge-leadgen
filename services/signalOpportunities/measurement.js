'use strict';
// Deliberately not imported by the operational evaluator. Canonical outcomes
// remain blinded; this helper accepts only released, completed prior evidence.
function median(xs){if(!xs.length)return null;const a=[...xs].sort((a,b)=>a-b),m=Math.floor(a.length/2);return a.length%2?a[m]:(a[m-1]+a[m])/2;}
function sourcePerformance(sourceId,completed,asOf){
 const n=Date.parse(asOf),seen=new Set();
 const rows=completed.filter(r=>{
  if(r.sourceId!==sourceId||r.status!=='COMPLETED'||r.dataClass!=='EMPIRICAL'||r.synthetic||r.testOnly||r.blinded!==false||r.prospective!==true)return false;
  if(!Number.isFinite(Date.parse(r.completedAt))||!Number.isFinite(Date.parse(r.knowledgeAt))||!Number.isFinite(Date.parse(r.startedAt))||Date.parse(r.completedAt)>=n||Date.parse(r.knowledgeAt)>=n||Date.parse(r.startedAt)>Date.parse(r.completedAt))return false;
  if(!r.observationId||seen.has(r.observationId))return false;seen.add(r.observationId);return true;
 });
 const valid=x=>typeof x==='number'&&Number.isFinite(x),values=k=>rows.map(r=>r[k]).filter(valid);
 const rate=k=>{const a=rows.filter(r=>typeof r[k]==='boolean');return {value:a.length?a.filter(r=>r[k]).length/a.length:null,denominator:a.length};};
 return {sourceId,asOf,sampleSize:rows.length,twoXBefore30PercentRate:rate('twoXBefore30Percent'),threeXRate:rate('threeX'),
  medianMfe:median(values('mfe')),medianMae:median(values('mae')),medianTimeTo2xSeconds:median(values('timeTo2xSeconds')),
  version:'source-performance-prior-empirical-v1',promotionalClaimsUsed:false};
}
module.exports={sourcePerformance,median};
