'use strict';
// An aggregator's text is empirical evidence that it made a claim. It is not
// direct evidence that the named caller posted, nor a canonical ROI record.
function machineClaims(text){
 const rows=[];for(const line of String(text||'').slice(0,12000).split('\n')){
  const match=line.match(/^\s*(\d+)\.\s*(.{1,160}?)\s+at\s+\$([\d,.]+)\s*([KMB])?\s*mc\b/i);
  if(!match)continue;const value=Number(match[3].replace(/,/g,''))*({K:1e3,M:1e6,B:1e9}[match[4]?.toUpperCase()]||1);
  rows.push({reportedOrder:Number(match[1]),callerDisplay:match[2].trim(),reportedMarketCap:Number.isFinite(value)?value:null,
    marketCapBasis:'AGGREGATOR_CLAIM',verifiedOriginalCall:false,originalTimestamp:null,performanceScore:null});
 }
 return {kind:'AGGREGATOR_ASSERTIONS',reportedCalls:rows,canonicalCallerEvidence:false,roiImported:false};
}
module.exports={machineClaims};
