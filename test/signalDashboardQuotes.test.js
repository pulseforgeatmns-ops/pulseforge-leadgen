'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {BirdeyeHistoricalQuoteProvider,valueTransactions}=require('../services/signalDashboard/quotes');
const {SOL,USDC}=require('../services/signalDashboard/wallet');
const {at,later,tx}=require('./helpers/signalDashboardFixtures');
test('historical quote selects only actual preceding observations; never future price',async()=>{
 const p=new BirdeyeHistoricalQuoteProvider({apiKey:'test',budget:{take:async()=>{}},now:()=>new Date(later(1000)),fetchFn:async()=>({ok:true,json:async()=>({success:true,data:{items:[{unixTime:Date.parse(at)/1000-90,value:100},{unixTime:Date.parse(at)/1000+1,value:999}]}})})});
 const q=await p.getQuote(SOL,at);assert.equal(q.priceUsd,'100');assert.equal(q.asOf,later(-30000));assert.equal(q.quality,'PARTIAL');
 const t=tx(1,'BUY',{blockTime:at,valueUsd:null,feesUsd:null,quoteAmount:'0.2',quoteAsset:SOL,feesLamports:'5000'});
 const [v]=valueTransactions([t],[q]);assert.equal(v.valueUsd,'20');assert.equal(v.feesUsd,'0.0005');assert.equal(v.executionPriceUsd,'2');
 assert.equal(valueTransactions([t],[{...q,asOf:later(1)}])[0].valueUsd,null);
});
test('USDC quantity remains USDC until historical USD valuation is available',()=>{
 const t=tx(1,'BUY',{valueUsd:null,quoteAmount:'20',quoteAsset:USDC,feesUsd:null,feesLamports:null});assert.equal(valueTransactions([t],[])[0].valueUsd,null);
});
