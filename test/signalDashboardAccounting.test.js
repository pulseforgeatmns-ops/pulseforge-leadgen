'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {account}=require('../services/signalDashboard/accounting');
const {dec,str,raw}=require('../services/signalDashboard/decimal');
const CA='So11111111111111111111111111111111111111112',start=Date.parse('2026-10-24T12:00:00Z');
function tx(i,type,amount,cash,extra={}){return {id:String(i),signature:'sig'+i,tokenAddress:CA,type,tokenAmount:amount,valueUsd:cash,feesUsd:'0',blockTime:new Date(start+i*60000).toISOString(),knownAt:new Date(start+i*60000).toISOString(),slot:i,...extra};}
const opts={historyComplete:true,priceUsd:'3',balance:'60',asOf:new Date(start+10000000).toISOString()};
test('fixed decimal values preserve raw Solana quantities deterministically',()=>{assert.equal(str(raw('9007199254740993123',9)),'9007199254.740993123');assert.throws(()=>dec('.1'),/invalid_decimal/);});
test('FIFO partial sell and reentry produces reconciled realized/unrealized/total P&L',()=>{
 const trades=[tx(1,'BUY','100','100'),tx(2,'SELL','60','120'),tx(3,'BUY','20','40')];
 const p=account(trades,opts);assert.equal(p.grossCapitalDeployed,'140');assert.equal(p.grossProceedsRealized,'120');assert.equal(p.tokensRemaining,'60');
 assert.equal(p.realizedPnl,'60');assert.equal(p.remainingCostBasis,'80');assert.equal(p.unrealizedPnl,'100');assert.equal(p.totalPnl,'160');assert.equal(p.netCapitalAtRisk,'20');
 assert.equal(p.lifecycle,'RE_ENTRY');assert.equal(p.reconciliation.state,'MATCH');assert.equal(p.remainingLots.length,2);
 assert.deepEqual(p,account([...trades].reverse(),opts));
});
test('FIFO preserves remaining pennies, observable fees and principal recovery',()=>{
 const p=account([tx(1,'BUY','3','1',{feesUsd:'0.03'}),tx(2,'SELL','1','1',{feesUsd:'0.01'}),tx(3,'SELL','2','2',{feesUsd:'0.02'})],{...opts,balance:'0'});
 assert.equal(p.remainingCostBasis,'0');assert.equal(p.totalPnl,'1.94');assert.equal(p.feesUsd,'0.06');assert.equal(p.lifecycle,'EXIT');assert.equal(p.netCapitalAtRisk,'0');
});
test('duplicate signatures are idempotent and different token accounts are netted upstream',()=>{
 const b=tx(1,'BUY','100','100');assert.deepEqual(account([b,b],opts),account([b],opts));
 const conflict={...b,tokenAmount:'2'};assert.equal(account([b,conflict],opts).realizedPnl,null);
});
test('transfers do not invent purchases, proceeds or acquisition basis',()=>{
 const p=account([tx(1,'TRANSFER_IN','10',null),tx(2,'SELL','5','15')],{...opts,balance:'5'});assert.equal(p.realizedPnl,null);assert.equal(p.unrealizedPnl,null);assert.equal(p.tokensPurchased,'0');assert.equal(p.remainingCostBasis,null);
 const out=account([tx(1,'BUY','10','10'),tx(2,'TRANSFER_OUT','3',null)],{...opts,balance:'7'});assert.equal(out.grossProceedsRealized,'0');assert.equal(out.remainingCostBasis,'7');
});
test('missing consideration, fees and incomplete history never become zero P&L',()=>{
 const p=account([tx(1,'BUY','10',null,{feesUsd:null}),tx(2,'SELL','3','12')],{...opts,balance:'7'});assert.equal(p.grossCapitalDeployed,null);assert.equal(p.feesUsd,null);assert.equal(p.totalPnl,null);
 assert.equal(account([tx(1,'BUY','10','10')],{...opts,historyComplete:false}).totalPnl,null);
});
test('balance mismatch, excessive sales and ambiguous same-slot ordering are disclosed',()=>{
 assert.equal(account([tx(1,'BUY','10','10')],opts).reconciliation.state,'MISMATCH');
 assert.ok(account([tx(2,'SELL','3','12')],opts).issues.includes('DISPOSAL_EXCEEDS_OBSERVED_LOTS'));
 const p=account([tx(1,'BUY','10','10',{slot:1}),tx(2,'SELL','3','12',{slot:1})],opts);assert.ok(p.issues.includes('SAME_SLOT_ORDER_UNKNOWN'));assert.equal(p.totalPnl,null);
});
test('historical accounting sees neither future trades nor late-imported transactions',()=>{
 const asOf=new Date(start+90000).toISOString();const p=account([tx(1,'BUY','10','10'),tx(2,'SELL','3','12'),tx(0,'BUY','10','10',{knownAt:new Date(start+999999).toISOString()})],{...opts,asOf,balance:'10'});
 assert.equal(p.transactions.length,1);assert.equal(p.tokensSold,'0');assert.equal(p.tokensRemaining,'10');
});
module.exports={tx,opts,CA,start};
