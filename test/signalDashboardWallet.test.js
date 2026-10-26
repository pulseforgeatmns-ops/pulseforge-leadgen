'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {normalizeTransaction,HeliusWalletWatchProvider,SOL,USDC}=require('../services/signalDashboard/wallet');
const CA='11111111111111111111111111111111',W='So11111111111111111111111111111111111111112';
const at='2026-10-24T14:00:00.000Z';
const leg=(mint,tokenAmount,decimals=6)=>({mint,userAccount:W,tokenAccount:CA,rawTokenAmount:{tokenAmount,decimals}});
function swap(sign=1){const delta=String(sign*10000000),target=leg(CA,'10000000'),quote=leg(USDC,'20000000');
 return {signature:'3'.repeat(88),timestamp:Date.parse(at)/1000,slot:2,transactionError:null,fee:5000,feePayer:W,type:'SWAP',
  accountData:[{tokenBalanceChanges:[leg(CA,delta)]}],events:{swap:{tokenInputs:sign>0?[quote]:[target],tokenOutputs:sign>0?[target]:[quote]}}};}
const price=async(asset,blockTime)=>({priceUsd:asset===USDC?'1':'100',asOf:blockTime,provider:'test-only',quality:'PARTIAL'});
test('classifies exact wallet swap legs BUY/SELL with blockchain time, amount, quote, fees',async()=>{
 for(const sign of [1,-1]){const [row]=await normalizeTransaction(swap(sign),W,{now:new Date(at),quotePrice:price});assert.equal(row.type,sign>0?'BUY':'SELL');assert.equal(row.tokenAmount,'10');assert.equal(row.valueUsd,'20');assert.equal(row.executionPriceUsd,'2');assert.equal(row.feesUsd,'0.0005');assert.equal(row.blockTime,at);}
});
test('transfer in/out and self-transfers never invent buy/sell',async()=>{
 for(const sign of [1,-1]){const x=swap(sign);x.type='TRANSFER';delete x.events;const [row]=await normalizeTransaction(x,W,{now:new Date(at)});assert.equal(row.type,sign>0?'TRANSFER_IN':'TRANSFER_OUT');assert.equal(row.valueUsd,null);}
 const x=swap();x.accountData[0].tokenBalanceChanges.push(leg(CA,'-10000000'));assert.deepEqual(await normalizeTransaction(x,W,{now:new Date(at)}),[]);
});
test('failed/future transactions, unowned legs and multi-asset swaps cannot become buys',async()=>{
 assert.deepEqual(await normalizeTransaction({...swap(),transactionError:{error:'failed'}},W,{now:new Date(at)}),[]);
 assert.deepEqual(await normalizeTransaction(swap(),W,{now:new Date(Date.parse(at)-1)}),[]);
 const x=swap();x.events.swap.tokenInputs[0].userAccount=CA;assert.equal((await normalizeTransaction(x,W,{now:new Date(at)}))[0].type,'TRANSFER_IN');
 const multi=swap();multi.events.swap.tokenOutputs.push(leg('2'.repeat(32),'1'));assert.equal((await normalizeTransaction(multi,W,{now:new Date(at)}))[0].type,'TRANSFER_IN');
});
test('native SOL consideration is exact; missing or future historical USD stays unknown',async()=>{
 const x=swap();x.events.swap.tokenInputs=[];x.events.swap.nativeInput={account:W,amount:'1000000000'};
 const [p]=await normalizeTransaction(x,W,{now:new Date(at)});assert.equal(p.type,'BUY');assert.equal(p.quoteAsset,SOL);assert.equal(p.quoteAmount,'1');assert.equal(p.valueUsd,null);
 const [f]=await normalizeTransaction(x,W,{now:new Date(at),quotePrice:async()=>({priceUsd:'100',asOf:new Date(Date.parse(at)+1).toISOString()})});assert.equal(f.valueUsd,null);
});
test('public-wallet provider HTTP transport is read-only and RPC signing methods are rejected',async()=>{
 const calls=[],budget={take:async()=>{},backoff:async()=>{}};
 const p=new HeliusWalletWatchProvider({apiKey:'server-only-test',budget,fetchFn:async(url,o)=>{calls.push({url,o});return{ok:true,json:async()=>String(url).includes('/transactions')?[]:{result:{context:{slot:5},value:[]}}};},now:()=>new Date(at)});
 const page=await p.getHistoryPage({wallet:W});assert.equal(page.exhausted,true);assert.equal(new URL(calls[0].url).searchParams.get('token-accounts'),'balanceChanged');assert.equal(calls[0].o.method,'GET');
 assert.equal((await p.getBalance({wallet:W,tokenAddress:CA})).tokens,'0');assert.equal(JSON.parse(calls[1].o.body).method,'getTokenAccountsByOwner');
 await assert.rejects(()=>p.rpc('sendTransaction',[]),/forbidden/);await assert.rejects(()=>p.rpc('signTransaction',[]),/forbidden/);assert.equal(calls.length,2);
});
test('rate limiting records backoff and malformed balances are never a zero balance',async()=>{
 let backs=0;const p=new HeliusWalletWatchProvider({apiKey:'test',budget:{take:async()=>{},backoff:async()=>{backs++;}},fetchFn:async()=>({status:429})});
 await assert.rejects(()=>p.getHistoryPage({wallet:W}),/rate_limited/);assert.equal(backs,1);
 p.fetchFn=async()=>({ok:true,json:async()=>({result:{value:[{account:{data:{parsed:{info:{owner:CA,mint:CA}}}}}]}})});
 await assert.rejects(()=>p.getBalance({wallet:W,tokenAddress:CA}),/identity_mismatch/);
});
module.exports={swap,leg,CA,W,at,price};
