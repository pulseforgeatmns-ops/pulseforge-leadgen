'use strict';
const HORIZONS=[5,15,30,60,240,1440];
function reconstructMarket(record,observations){
 if(record.dataClass!=='HISTORICAL_BACKFILL')throw Error('historical_backfill_required');
 const t0=Date.parse(record.occurredAt);const rows=(observations||[]).filter(o=>o.tokenAddress===record.addresses?.[0]&&Date.parse(o.occurredAt)>=t0).sort((a,b)=>Date.parse(a.occurredAt)-Date.parse(b.occurredAt));
 const entry=rows[0]||null;if(!entry)return {status:'HISTORICAL_MARKET_UNAVAILABLE',entry:null,outcomes:[]};
 const p0=Number(entry.priceUsd);const outcomes=HORIZONS.map(minutes=>{const cutoff=t0+minutes*60000;const slice=rows.filter(o=>Date.parse(o.occurredAt)<=cutoff&&Number(o.priceUsd)>0);if(!slice.length)return {minutes,status:'UNAVAILABLE'};const ps=slice.map(o=>Number(o.priceUsd));const max=Math.max(...ps),min=Math.min(...ps);const milestone=(multiple)=>{const hit=slice.find(o=>Number(o.priceUsd)>=multiple*p0);if(!hit)return {minutes:null,maeBeforePct:null};const pre=slice.filter(o=>Date.parse(o.occurredAt)<=Date.parse(hit.occurredAt)).map(o=>Number(o.priceUsd));return {minutes:(Date.parse(hit.occurredAt)-Date.parse(entry.occurredAt))/60000,maeBeforePct:(Math.min(...pre)/p0-1)*100};};const m15=milestone(1.5),m2=milestone(2),m3=milestone(3);return {minutes,status:'AVAILABLE',mfePct:(max/p0-1)*100,maePct:(min/p0-1)*100,endReturnPct:(ps.at(-1)/p0-1)*100,timeTo1_5xMinutes:m15.minutes,maeBefore1_5xPct:m15.maeBeforePct,timeTo2xMinutes:m2.minutes,maeBefore2xPct:m2.maeBeforePct,timeTo3xMinutes:m3.minutes,maeBefore3xPct:m3.maeBeforePct};});
 return {status:'AVAILABLE',entry:{occurredAt:entry.occurredAt,priceUsd:p0,provider:entry.provider||null},outcomes};
}
function attentionSequence(record,allRecords){const token=record.addresses?.[0],t0=Date.parse(record.occurredAt);return (allRecords||[]).filter(r=>r!==record&&r.addresses?.includes(token)&&Date.parse(r.occurredAt)>=t0).sort((a,b)=>Date.parse(a.occurredAt)-Date.parse(b.occurredAt)).map(r=>({sourceId:r.sourceId,occurredAt:r.occurredAt,lagMinutes:(Date.parse(r.occurredAt)-t0)/60000,partition:r.partition||null}));}
module.exports={HORIZONS,reconstructMarket,attentionSequence};
