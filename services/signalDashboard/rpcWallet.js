'use strict';
const {createHash}=require('node:crypto');
const {WalletWatchProvider,id,USDC,SOL}=require('./wallet');
const {raw,str,div}=require('./decimal');
const {isValidSolanaAddress:address,BASE58}=require('../../packages/signal-v1/acquisition/solanaAddress');
const PUMP_SWAP='pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA';
// Pump's published IDL: pump-fun/pump-public-docs/idl/pump_amm.json.
const TRADE_NAMES={buy:'BUY',buy_exact_quote_in:'BUY',buy_v2:'BUY',buy_exact_quote_in_v2:'BUY',sell:'SELL',sell_v2:'SELL'};
const DISCRIMINATORS=new Map(Object.entries(TRADE_NAMES).map(([name,side])=>[createHash('sha256').update('global:'+name).digest().subarray(0,8).toString('hex'),side]));
function decode58(s){if(typeof s!=='string'||s.length>4096)throw Error('invalid_instruction_data');let n=0n;for(const c of s){const i=BASE58.indexOf(c);if(i<0)throw Error('invalid_base58');n=n*58n+BigInt(i);}const hex=n.toString(16).padStart(2,'0');const b=n?Buffer.from(hex.length%2?'0'+hex:hex,'hex'):Buffer.alloc(0);return Buffer.concat([Buffer.alloc(s.length-s.replace(/^1+/,'').length),b]);}
function tradeSide(instruction,mint,ownedAccounts){
 if(instruction.programId!==PUMP_SWAP||!Array.isArray(instruction.accounts)||!instruction.accounts.some(a=>a===mint||ownedAccounts.has(a)))return null;
 try{return DISCRIMINATORS.get(decode58(instruction.data).subarray(0,8).toString('hex'))||null;}catch{return null;}
}
function normalizeRpcTransaction(tx,wallet,{knownAt=new Date().toISOString(),signatureMetadata={}}={}){
 if(!tx?.meta||tx.meta.err!==null||!Number.isSafeInteger(tx.blockTime)||!Number.isSafeInteger(tx.slot)||tx.blockTime*1000>Date.parse(knownAt)||!['legacy',0,1].includes(tx.version))return[];
 const keys=tx.transaction?.message?.accountKeys?.map(x=>typeof x==='string'?x:x.pubkey),signature=tx.transaction?.signatures?.[0];
 if(!keys||!signature||signatureMetadata.signature&&signatureMetadata.signature!==signature||signatureMetadata.slot!=null&&signatureMetadata.slot!==tx.slot)throw Error('rpc_transaction_identity_mismatch');
 const deltas=new Map(),owned=new Map();
 for(const [sign,field]of [[-1n,'preTokenBalances'],[1n,'postTokenBalances']])for(const b of tx.meta[field]||[]){
  if(b.owner!==wallet)continue;const mint=b.mint;if(!address(mint))throw Error('invalid_balance_mint');
  deltas.set(mint,(deltas.get(mint)||0n)+sign*raw(b.uiTokenAmount.amount,b.uiTokenAmount.decimals));
  const set=owned.get(mint)||new Set();set.add(keys[b.accountIndex]);owned.set(mint,set);
 }
 const instructions=[...(tx.transaction.message.instructions||[]),...(tx.meta.innerInstructions||[]).flatMap(g=>g.instructions||[])];
 const targetMints=[...deltas].filter(([mint,q])=>q!==0n&&![USDC,SOL].includes(mint));
 const wi=keys.indexOf(wallet),walletLamports=wi>=0&&Number.isSafeInteger(tx.meta.preBalances?.[wi])&&Number.isSafeInteger(tx.meta.postBalances?.[wi])?String(tx.meta.postBalances[wi]-tx.meta.preBalances[wi]):null;
 const fee=keys[0]===wallet&&Number.isSafeInteger(tx.meta.fee)?String(tx.meta.fee):keys[0]!==wallet?'0':null;
 return targetMints.map(([tokenAddress,delta])=>{
  const expected=delta>0n?'BUY':'SELL',sides=instructions.map(ix=>tradeSide(ix,tokenAddress,owned.get(tokenAddress)||new Set())).filter(Boolean);
  const swap=targetMints.length===1&&sides.length>0&&sides.every(s=>s===expected);
  const quotes=[...deltas].filter(([mint,q])=>[USDC,SOL].includes(mint)&&q!==0n),q=delta>0n?delta:-delta;
  const consideration=swap&&quotes.length===1&&quotes[0][1]*delta<0n?quotes[0]:null;
  const quoteAmount=consideration?str(consideration[1]>0n?consideration[1]:-consideration[1]):null;
  const blockTime=new Date(tx.blockTime*1000).toISOString();
  return {id:id([wallet,signature,tokenAddress]),wallet,signature,tokenAddress,type:swap?expected:delta>0n?'TRANSFER_IN':'TRANSFER_OUT',
   tokenAmount:str(q),quoteAmount,quoteAsset:consideration?.[0]||null,quoteCashBasis:consideration?'NET_WALLET_DELTA':null,
   executionPriceQuote:consideration?str(div(consideration[1]>0n?consideration[1]:-consideration[1],q)):null,
   valueUsd:null,executionPriceUsd:null,feesUsd:fee==='0'?'0':null,feesLamports:fee,walletLamportDelta:walletLamports,
   blockTime,occurredAt:blockTime,knownAt,slot:tx.slot,transactionIndex:Number.isSafeInteger(signatureMetadata.transactionIndex)?signatureMetadata.transactionIndex:null,
   classification:swap?'VERIFIED_PUMPSWAP_INSTRUCTION_AND_WALLET_DELTAS':'WALLET_TOKEN_TRANSFER_OR_UNSUPPORTED_PROGRAM',
   provenance:{provider:'solana-rpc',dataClass:'EMPIRICAL',commitment:'finalized',transactionVersion:tx.version,rawTransaction:tx,
    quoteScope:'Net wallet token changes; platform fees may be embedded and are not separately itemized',networkFeePayer:keys[0]}};
 });
}
class SolanaRpcWalletWatchProvider extends WalletWatchProvider{
 constructor({tokenAddresses,rpcUrl='https://api.mainnet-beta.solana.com',budget,fetchFn=globalThis.fetch,now=()=>new Date(),pageSize=100,maxTransactions=5}={}){
  super();const u=new URL(rpcUrl);
  if(u.protocol!=='https:'||u.username||u.password||!Array.isArray(tokenAddresses)||tokenAddresses.length<1||tokenAddresses.length>3||tokenAddresses.some(x=>!address(x))||!budget)throw Error('invalid_rpc_wallet_configuration');
  Object.assign(this,{tokenAddresses:[...new Set(tokenAddresses)],rpcUrl:u.href,budget,fetchFn,now,pageSize:Math.min(100,pageSize),maxTransactions:Math.min(5,maxTransactions),balances:new Map()});
 }
 async requestBatch(calls){
  if(!calls.length)return [];
  if(calls.length>5||calls.some(x=>!['getTokenAccountsByOwner','getSignaturesForAddress','getTransaction'].includes(x.method)))throw Error('wallet_rpc_method_forbidden');
  await this.budget.take('solana-rpc-public',Math.max(1000,calls.length*1000));
  const response=await this.fetchFn(this.rpcUrl,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(calls.map((x,i)=>({jsonrpc:'2.0',id:i,...x}))),redirect:'error',signal:AbortSignal.timeout(10000)});
  if(response.status===429){await this.budget.backoff('solana-rpc-public',60000);throw Error('rpc_rate_limited');}
  if(!response.ok)throw Error('rpc_unavailable');const json=await response.json();
  if(!Array.isArray(json)||json.length!==calls.length||new Set(json.map(x=>x.id)).size!==calls.length)throw Error('invalid_rpc_batch');
  const result=calls.map((_,i)=>json.find(x=>x.id===i));if(result.some(x=>!x||x.error))throw Error('rpc_result_unavailable');return result.map(x=>x.result);
 }
 async getBalance({wallet,tokenAddress}){
  if(!address(wallet)||!this.tokenAddresses.includes(tokenAddress))throw Error('wallet_scope_mismatch');
  const cache=this.balances.get(wallet+tokenAddress);if(cache&&+this.now()-Date.parse(cache.observedAt)<30000)return structuredClone(cache);
  const [r]=await this.requestBatch([{method:'getTokenAccountsByOwner',params:[wallet,{mint:tokenAddress},{encoding:'jsonParsed',commitment:'finalized'}]}]);
  if(!Array.isArray(r?.value)||!Number.isSafeInteger(r.context?.slot))throw Error('invalid_rpc_balance');let tokens=0n;const accounts=[];
  for(const a of r.value){const b=a.account?.data?.parsed?.info;if(b?.owner!==wallet||b?.mint!==tokenAddress||!address(a.pubkey))throw Error('rpc_balance_identity_mismatch');tokens+=raw(b.tokenAmount.amount,b.tokenAmount.decimals);accounts.push(a.pubkey);}
  const observedAt=this.now().toISOString(),balance={tokenAddress,tokens:str(tokens),accounts,slot:r.context.slot,observedAt,provider:'solana-rpc',providerTimestamp:null};this.balances.set(wallet+tokenAddress,balance);return structuredClone(balance);
 }
 async getHistoryPage({wallet,before=null}){
  if(!address(wallet))throw Error('invalid_wallet');
  if(before&&(before.v!==1||before.wallet!==wallet||!Array.isArray(before.streams)||!Array.isArray(before.pending)))throw Error('invalid_rpc_cursor');
  let state=before?structuredClone(before):null;
  if(!state){
   const accounts=[];
   for(const tokenAddress of this.tokenAddresses)accounts.push(...(await this.getBalance({wallet,tokenAddress})).accounts);
   const addresses=[...new Set([wallet,...accounts])];if(addresses.length>5)throw Error('rpc_token_account_capacity_exceeded');
   state={v:1,wallet,streams:addresses.map(address=>({address,before:null,exhausted:false})),pending:[]};
  }
  const transactions=[],signatures=[];
  if(state.pending.length){
   const selected=state.pending.slice(0,this.maxTransactions),successful=selected.filter(x=>x.err===null);
   const results=await this.requestBatch(successful.map(x=>({method:'getTransaction',params:[x.signature,{encoding:'jsonParsed',commitment:'finalized',maxSupportedTransactionVersion:1}]})));
   const resolved=new Set();let i=0;
   for(const meta of selected){if(meta.err!==null){resolved.add(meta.signature);signatures.push(meta.signature);continue;}
    const tx=results[i++];if(!tx)continue;
    transactions.push(...normalizeRpcTransaction(tx,wallet,{knownAt:this.now().toISOString(),signatureMetadata:meta}));resolved.add(meta.signature);signatures.push(meta.signature);
   }
   state.pending=state.pending.filter(x=>!resolved.has(x.signature));
  }else{
   const streams=state.streams.filter(x=>!x.exhausted);
   const pages=await this.requestBatch(streams.map(s=>({method:'getSignaturesForAddress',params:[s.address,{limit:this.pageSize,commitment:'finalized',...(s.before?{before:s.before}:{})}]})));
   const found=new Map();pages.forEach((page,i)=>{
    if(!Array.isArray(page)||page.length>this.pageSize)throw Error('invalid_signature_page');const stream=streams[i];
    for(const row of page){if(typeof row.signature!=='string'||!Number.isSafeInteger(row.slot))throw Error('invalid_signature_metadata');found.set(row.signature,row);}
    if(page.length&&stream.before===page.at(-1).signature)throw Error('rpc_pagination_stalled');
    stream.before=page.at(-1)?.signature||stream.before;stream.exhausted=page.length<this.pageSize;
   });state.pending=[...found.values()].sort((a,b)=>b.slot-a.slot||(b.transactionIndex??0)-(a.transactionIndex??0)||a.signature.localeCompare(b.signature));
  }
  const exhausted=state.streams.every(x=>x.exhausted)&&!state.pending.length;
  return {transactions,signatures,nextBefore:state,exhausted,coverage:'PARTIAL',reason:'CURRENT_TOKEN_ACCOUNTS_AND_WALLET_ONLY; CLOSED_ACCOUNT_AND_RPC_RETENTION_COVERAGE_UNKNOWN'};
 }
}
module.exports={SolanaRpcWalletWatchProvider,normalizeRpcTransaction,PUMP_SWAP,decode58};
