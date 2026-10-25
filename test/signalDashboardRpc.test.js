'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),{createHash}=require('node:crypto');
const {SolanaRpcWalletWatchProvider,normalizeRpcTransaction,PUMP_SWAP}=require('../services/signalDashboard/rpcWallet');
const {quoteAccount}=require('../services/signalDashboard/quoteAccounting');
const {USDC}=require('../services/signalDashboard/wallet');
const {CA,W,POOL,at,later}=require('./helpers/signalDashboardFixtures');
const alphabet='123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';
function encode58(bytes){let n=BigInt('0x'+bytes.toString('hex')),out='';while(n){out=alphabet[Number(n%58n)]+out;n/=58n;}return out;}
function parsed(sign=1,signature='3'.repeat(88),extra={}){
 const b=(mint,amount,i)=>({mint,owner:W,accountIndex:i,uiTokenAmount:{amount,decimals:6}});
 const side=sign>0?'buy':'sell';return {version:1,blockTime:Date.parse(at)/1000,slot:5,
  transaction:{signatures:[signature],message:{accountKeys:[{pubkey:POOL},{pubkey:W},{pubkey:CA}],instructions:[{programId:PUMP_SWAP,accounts:[CA],data:encode58(createHash('sha256').update('global:'+side).digest().subarray(0,8))}]}},
  meta:{err:null,fee:5000,preBalances:[1,10,0],postBalances:[1,10,0],preTokenBalances:[b(CA,sign>0?'0':'10000000',2),b(USDC,sign>0?'20000000':'0',1)],postTokenBalances:[b(CA,sign>0?'10000000':'0',2),b(USDC,sign>0?'0':'20000000',1)],innerInstructions:[]},...extra};
}
test('RPC v1 parser proves PumpSwap side and exact net quote cash; sponsored fees are separate',()=>{
 for(const sign of [1,-1]){const [r]=normalizeRpcTransaction(parsed(sign),W,{knownAt:later(1000),signatureMetadata:{slot:5,transactionIndex:21}});assert.equal(r.type,sign>0?'BUY':'SELL');assert.equal(r.tokenAmount,'10');assert.equal(r.quoteAmount,'20');assert.equal(r.quoteAsset,USDC);assert.equal(r.valueUsd,null);assert.equal(r.feesLamports,'0');assert.equal(r.walletLamportDelta,'0');assert.equal(r.transactionIndex,21);assert.equal(r.blockTime,at);}
});
test('failed, future and unsupported versions cannot create observed trades',()=>{
 const x=parsed();x.meta.err={failed:true};assert.deepEqual(normalizeRpcTransaction(x,W,{knownAt:later(1000)}),[]);assert.deepEqual(normalizeRpcTransaction(parsed(),W,{knownAt:later(-1)}),[]);assert.deepEqual(normalizeRpcTransaction(parsed(1,undefined,{version:2}),W,{knownAt:later(1000)}),[]);
});
test('unrecognized program, unrelated mint and opposite-side instruction are transfers, not invented swaps',()=>{
 for(const mutate of [x=>x.transaction.message.instructions[0].programId=POOL,x=>x.transaction.message.instructions[0].accounts=[POOL],x=>x.transaction.message.instructions[0].data=encode58(createHash('sha256').update('global:sell').digest().subarray(0,8))]){const x=parsed();mutate(x);assert.equal(normalizeRpcTransaction(x,W,{knownAt:later(1000)})[0].type,'TRANSFER_IN');}
});
test('RPC cursor does not acknowledge signatures before their transaction evidence has been persisted',async()=>{
 const signature='3'.repeat(88),calls=[],budget={take:async()=>{},backoff:async()=>{}};
 const p=new SolanaRpcWalletWatchProvider({tokenAddresses:[CA],budget,now:()=>new Date(later(1000)),fetchFn:async(_url,o)=>{const reqs=JSON.parse(o.body);calls.push(...reqs);return{ok:true,json:async()=>reqs.map(r=>({id:r.id,result:r.method==='getTokenAccountsByOwner'?{context:{slot:6},value:[{pubkey:POOL,account:{data:{parsed:{info:{owner:W,mint:CA,tokenAmount:{amount:'10000000',decimals:6}}}}}}]}:r.method==='getSignaturesForAddress'?[{signature,slot:5,err:null,transactionIndex:7}]:parsed(1,signature)}))};}});
 const page1=await p.getHistoryPage({wallet:W});assert.equal(page1.signatures.length,0);assert.equal(page1.exhausted,false);
 const page2=await p.getHistoryPage({wallet:W,before:page1.nextBefore});assert.equal(page2.transactions.length,1);assert.deepEqual(page2.signatures,[signature]);assert.equal(page2.exhausted,true);
 assert.equal(calls.find(r=>r.method==='getTransaction').params[1].maxSupportedTransactionVersion,1);
 await assert.rejects(()=>p.requestBatch([{method:'sendTransaction',params:[]}]),/forbidden/);await assert.rejects(()=>p.requestBatch([{method:'signTransaction',params:[]}]),/forbidden/);
});
test('null transaction stays pending instead of disappearing from history',async()=>{
 const p=new SolanaRpcWalletWatchProvider({tokenAddresses:[CA],budget:{take:async()=>{}},fetchFn:async()=>({ok:true,json:async()=>[{id:0,result:null}]}),now:()=>new Date(later(1000))});
 const cursor={v:1,wallet:W,streams:[{address:W,exhausted:true}],pending:[{signature:'3'.repeat(88),slot:5,err:null}]};
 const page=await p.getHistoryPage({wallet:W,before:cursor});assert.equal(page.signatures.length,0);assert.equal(page.nextBefore.pending.length,1);assert.equal(page.exhausted,false);
});
test('a page containing only failed transactions drains without issuing an empty RPC batch',async()=>{
 let requests=0;const p=new SolanaRpcWalletWatchProvider({tokenAddresses:[CA],budget:{take:async()=>{}},fetchFn:async()=>{requests++;throw Error('unexpected request');}});
 const cursor={v:1,wallet:W,streams:[{address:W,exhausted:true}],pending:[{signature:'3'.repeat(88),slot:5,err:{failed:true}}]};
 const page=await p.getHistoryPage({wallet:W,before:cursor});assert.equal(page.exhausted,true);assert.equal(page.signatures.length,1);assert.equal(page.transactions.length,0);assert.equal(requests,0);
 const done=await p.getHistoryPage({wallet:W,before:page.nextBefore});assert.equal(done.exhausted,true);assert.equal(requests,0);
});
test('USDC accounting never relabels quote cash as USD and computes FIFO in the declared unit',()=>{
 const rows=[{...normalizeRpcTransaction(parsed(),W,{knownAt:later(1000)})[0],slot:1,transactionIndex:1},
  {...normalizeRpcTransaction(parsed(-1,'4'.repeat(88)),W,{knownAt:later(2000)})[0],slot:2,transactionIndex:1,tokenAmount:'5',quoteAmount:'20'}];
 const result=quoteAccount(rows,{balance:'5',historyComplete:true,asOf:later(3000),priceQuote:'3'});assert.equal(result.symbol,'USDC');assert.equal(result.ledger.grossCapitalDeployed,'20');assert.equal(result.ledger.realizedCash,'20');assert.equal(result.ledger.realizedPnl,'10');assert.equal(result.ledger.totalPnl,'15');assert.equal(result.ledger.netCapitalAtRisk,'0');assert.equal(rows[0].valueUsd,null);
});
module.exports={parsed};
