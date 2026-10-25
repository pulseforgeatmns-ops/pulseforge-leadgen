'use strict';
const CA='11111111111111111111111111111111',W='So11111111111111111111111111111111111111112',POOL='2'.repeat(32);
const at='2026-10-24T14:00:00.000Z';
const later=(ms=0)=>new Date(Date.parse(at)+ms).toISOString();
class MemoryStore {
 constructor(){this.tenantId='1';this.wallet=W;this.rows=new Map();}
 async put(kind,ca,key,payload,{mutable=false}={}){const k=[kind,ca,key].join(':');if(mutable||!this.rows.has(k))this.rows.set(k,{kind,ca,payload:structuredClone(payload)});}
 async list(kind,ca,{asOf='2099-01-01',ascending=true,limit=5000}={}){return [...this.rows.values()].filter(x=>x.kind===kind&&(ca==null||x.ca===ca)&&Date.parse(x.payload.knownAt)<=Date.parse(asOf)&&Date.parse(x.payload.occurredAt||x.payload.observedAt||x.payload.blockTime)<=Date.parse(asOf)).sort((a,b)=>{const d=Date.parse(a.payload.occurredAt||a.payload.observedAt||a.payload.blockTime)-Date.parse(b.payload.occurredAt||b.payload.observedAt||b.payload.blockTime)||Date.parse(a.payload.knownAt)-Date.parse(b.payload.knownAt);return ascending?d:-d;}).slice(0,limit).map(x=>structuredClone(x.payload));}
 async latest(kind,ca,options={}){return (await this.list(kind,ca,{...options,ascending:false,limit:1}))[0]||null;}
 async lock(fn){return fn();}
}
const observation=(ms=0,extra={})=>({tokenAddress:CA,pairId:POOL,priceUsd:1,marketCapUsd:100000,liquidityUsd:10000,volume5mUsd:100,buys5m:5,sells5m:5,
 observedAt:later(ms),occurredAt:later(ms),knownAt:later(ms),providerTimestamp:null,provider:'test-only',provenance:{dataClass:'EMPIRICAL'},...extra});
const tx=(i,type='BUY',extra={})=>({id:'t'+i,signature:'sig'+i,tokenAddress:CA,type,tokenAmount:'10',valueUsd:'10',feesUsd:'0',slot:i,blockTime:later(i*1000),occurredAt:later(i*1000),knownAt:later(i*1000),provenance:{provider:'test-only',commitment:'finalized',rawTransaction:{secret:'not-a-secret-do-not-expose-raw'}},...extra});
async function seed(store){await store.put('watch',CA,'watch',{tokenAddress:CA,symbol:'SYNTHETIC TEST',occurredAt:at,knownAt:at});}
module.exports={MemoryStore,CA,W,POOL,at,later,observation,tx,seed};
