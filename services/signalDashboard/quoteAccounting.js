'use strict';
const {account}=require('./accounting');
const {USDC}=require('./wallet');
function quoteAccount(transactions,options={}){
 const asset=options.quoteAsset||USDC;
 const rows=transactions.map(t=>({...t,valueUsd:t.quoteAsset===asset?t.quoteAmount:null,
  // Net USDC wallet changes already include token-denominated fees. Additional
  // native SOL expenses are never assumed to be zero when the wallet paid them.
  feesUsd:t.quoteCashBasis==='NET_WALLET_DELTA'&&t.walletLamportDelta==='0'?'0':t.feesLamports==='0'?'0':null}));
 const ledger=account(rows,{...options,priceUsd:options.priceQuote??null});
 return {asset,symbol:asset===USDC?'USDC':asset,quality:'PARTIAL',cashBasis:'Net wallet quote-token changes; USD conversion is separate',
  feeItemization:'UNKNOWN: net cash includes embedded token fees; sponsored network fees are identified separately',
  ledger:{...ledger,feesUsd:undefined,positionValue:ledger.positionValue,transactions:undefined,rawLots:undefined},transactions:ledger.transactions};
}
module.exports={quoteAccount};
