'use strict';
const {CA,POOL,at}=require('./signalDashboardFixtures');
const {discover}=require('../../services/signalDashboard/zones');
function researchFixture({tokenAddress=CA,pairId=POOL,createdAt=new Date(Date.parse(at)+10).toISOString()}={}){
 const end=Math.floor(Date.parse(createdAt)/300000)*300000,start=end-24*300000;
 const candles=Array.from({length:24},(_,i)=>({time:start/1000+i*300,interval:'5m',open:1.05,high:1.12,low:i===5?1:i===12?1.005:1.03,close:1.04,volume:10,complete:true,coverage:'PROVIDER_OHLCV',pairId,provider:'test-only',provenance:{dataClass:'EMPIRICAL'},knownAt:new Date(start+(i+1)*300000).toISOString()}));
 const observation={tokenAddress,pairId,priceUsd:1.03,liquidityUsd:1000,observedAt:createdAt,occurredAt:createdAt,knownAt:createdAt,provider:'test-only',provenance:{dataClass:'EMPIRICAL'}};
 const args={tokenAddress,pairId,candles,observations:[observation],risk:'WATCH',transactions:[],asOf:createdAt};return {args,zone:discover(args).zone,observation,candles};
}
module.exports={researchFixture};
