'use strict';
const {createHash} = require('node:crypto');
const {raw, str, dec, div, mul} = require('./decimal');
const {isValidSolanaAddress} = require('../../packages/signal-v1/acquisition/solanaAddress');
const USDC = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';
const SOL = 'So11111111111111111111111111111111111111112';
class WalletWatchProvider {
  async getHistoryPage() { throw new Error('wallet_provider_not_implemented'); }
  async getBalance() { throw new Error('wallet_provider_not_implemented'); }
}
const id = values => createHash('sha256').update(JSON.stringify(values)).digest('hex');
function amount(leg) { return raw(leg.rawTokenAmount.tokenAmount, leg.rawTokenAmount.decimals); }
async function normalizeTransaction(tx, wallet, {now = new Date(), quotePrice = async () => null} = {}) {
  if (tx.transactionError || !tx.signature || !Number.isSafeInteger(tx.timestamp) || tx.timestamp * 1000 > +now || !Number.isSafeInteger(tx.slot)) return [];
  const blockTime = new Date(tx.timestamp*1000).toISOString(), knownAt = now.toISOString();
  const deltas = new Map();
  for (const data of tx.accountData || []) for (const change of data.tokenBalanceChanges || []) {
    if (change.userAccount !== wallet || !isValidSolanaAddress(change.mint)) continue;
    deltas.set(change.mint, (deltas.get(change.mint) || 0n) + amount(change));
  }
  const swap = tx.type === 'SWAP' ? tx.events?.swap : null;
  const inputs = (swap?.tokenInputs || []).filter(x=>x.userAccount===wallet), outputs = (swap?.tokenOutputs || []).filter(x=>x.userAccount===wallet);
  const rows = [];
  for (const [tokenAddress, delta] of deltas) {
    if (delta===0n || [SOL,USDC].includes(tokenAddress)) continue;
    const receiving = delta > 0n, q = receiving ? delta : -delta;
    const tokenLegs = (receiving ? outputs : inputs).filter(x=>x.mint===tokenAddress);
    const opposite = receiving ? inputs : outputs;
    const otherTargets = (receiving ? outputs : inputs).filter(x=>x.mint!==tokenAddress);
    const quoteLegs = opposite.filter(x=>[USDC,SOL].includes(x.mint));
    const native = receiving ? swap?.nativeInput : swap?.nativeOutput;
    const nativeValid = native?.account === wallet && /^\d+$/.test(String(native.amount)) && BigInt(native.amount)>0n;
    const exactTarget = tokenLegs.length>0 && tokenLegs.reduce((s,x)=>s+(amount(x)<0n?-amount(x):amount(x)),0n)===q;
    const provenSwap = Boolean(swap && exactTarget && !otherTargets.length && opposite.every(x=>[SOL,USDC].includes(x.mint))
      && (quoteLegs.length===1 && !nativeValid || !quoteLegs.length && nativeValid));
    let quoteAmount=null, quoteAsset=null, valueUsd=null, valuation=null, feesUsd=null;
    if (provenSwap) {
      quoteAsset = nativeValid ? SOL : quoteLegs[0].mint;
      quoteAmount = nativeValid ? str(raw(native.amount,9)) : str(amount(quoteLegs[0]) < 0n ? -amount(quoteLegs[0]) : amount(quoteLegs[0]));
      // USDC quantity is preserved, never silently assumed to equal USD.
      valuation = await quotePrice(quoteAsset, blockTime);
      if (valuation?.priceUsd != null && Date.parse(valuation.asOf)<=Date.parse(blockTime) && Date.parse(blockTime)-Date.parse(valuation.asOf)<=60000) {
        valueUsd = str(mul(dec(quoteAmount),dec(valuation.priceUsd)));
      } else valuation = null;
    }
    const feeLamports = tx.feePayer===wallet && Number.isSafeInteger(tx.fee) && tx.fee>=0 ? String(tx.fee) : tx.feePayer!==wallet ? '0' : null;
    const feeQuote = feeLamports==='0' ? {priceUsd:'0',asOf:blockTime} : await quotePrice(SOL,blockTime);
    if (feeLamports!==null && feeQuote?.priceUsd!=null && Date.parse(feeQuote.asOf)<=Date.parse(blockTime) && Date.parse(blockTime)-Date.parse(feeQuote.asOf)<=60000) feesUsd=str(mul(raw(feeLamports,9),dec(feeQuote.priceUsd)));
    const type = provenSwap ? receiving ? 'BUY' : 'SELL' : receiving ? 'TRANSFER_IN' : 'TRANSFER_OUT';
    rows.push({id:id([wallet,tx.signature,tokenAddress]),signature:tx.signature,slot:tx.slot,transactionIndex:tx.transactionIndex??null,
      wallet,tokenAddress,blockTime,occurredAt:blockTime,knownAt,type,tokenAmount:str(q),quoteAmount,quoteAsset,valueUsd,
      executionPriceUsd:str(div(dec(valueUsd),q)),feesLamports:feeLamports,feesUsd,
      classification:provenSwap ? 'ENHANCED_SWAP_EXACT_WALLET_LEGS' : swap ? 'AMBIGUOUS_SWAP_RECORDED_AS_TRANSFER' : 'WALLET_TOKEN_BALANCE_CHANGE',
      provenance:{provider:'helius-enhanced',dataClass:'EMPIRICAL',commitment:'finalized',valuation,feeValuation:feeQuote,rawTransaction:tx}});
  }
  return rows;
}
class HeliusWalletWatchProvider extends WalletWatchProvider {
  constructor({apiKey, rpcUrl, fetchFn=globalThis.fetch, budget, quotePrice, now=()=>new Date()}={}) {
    super();
    if (!apiKey || !budget) throw new Error('wallet_provider_configuration_required');
    const rpc = new URL(rpcUrl || `https://mainnet.helius-rpc.com/?api-key=${encodeURIComponent(apiKey)}`);
    if (rpc.protocol!=='https:' || rpc.username || rpc.password) throw new Error('invalid_readonly_rpc');
    Object.assign(this,{apiKey,rpcUrl:rpc.href,fetchFn,budget,quotePrice,now});
  }
  async request(url, options, lane='helius') {
    await this.budget.take(lane,1000);
    const r = await this.fetchFn(url,{...options,redirect:'error',signal:AbortSignal.timeout(8000)});
    if (r.status===429) { await this.budget.backoff(lane,60000); throw new Error('wallet_rate_limited'); }
    if (!r.ok) throw new Error('wallet_provider_unavailable');
    return r.json();
  }
  async getHistoryPage({wallet,before=null}) {
    if (!isValidSolanaAddress(wallet) || before && !/^[1-9A-HJ-NP-Za-km-z]{64,90}$/.test(before)) throw new Error('invalid_wallet_cursor');
    const url=new URL(`https://api.helius.xyz/v0/addresses/${wallet}/transactions`);
    url.searchParams.set('api-key',this.apiKey);url.searchParams.set('limit','100');
    url.searchParams.set('commitment','finalized');url.searchParams.set('token-accounts','balanceChanged');
    if(before)url.searchParams.set('before-signature',before);
    const data=await this.request(url.href,{method:'GET'});
    if(!Array.isArray(data) || data.length>100)throw new Error('invalid_wallet_history');
    const transactions=[];
    for(const tx of data) transactions.push(...await normalizeTransaction(tx,wallet,{now:this.now(),quotePrice:this.quotePrice}));
    return {transactions,signatures:data.map(x=>x.signature),nextBefore:data.at(-1)?.signature||null,exhausted:data.length===0,
      coverage:'PARTIAL',reason:'INDEXER_HISTORY_EXHAUSTION_IS_NOT_PROOF_OF_COMPLETE_CHAIN_COVERAGE'};
  }
  async rpc(method,params) {
    // The only RPC method exposed by this provider is a balance read.
    if(method!=='getTokenAccountsByOwner')throw new Error('wallet_rpc_method_forbidden');
    const body=await this.request(this.rpcUrl,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({jsonrpc:'2.0',id:1,method,params})},'solana-rpc');
    if(body.error || !body.result)throw new Error('wallet_balance_unavailable');return body.result;
  }
  async getBalance({wallet,tokenAddress}) {
    if(!isValidSolanaAddress(wallet)||!isValidSolanaAddress(tokenAddress))throw new Error('invalid_wallet_address');
    const r=await this.rpc('getTokenAccountsByOwner',[wallet,{mint:tokenAddress},{encoding:'jsonParsed',commitment:'finalized'}]);
    if(!Array.isArray(r.value))throw new Error('invalid_wallet_balance');
    let total=0n;
    for(const x of r.value) {
      const info=x.account?.data?.parsed?.info;
      if(info?.owner!==wallet || info?.mint!==tokenAddress)throw new Error('wallet_balance_identity_mismatch');
      total+=raw(info.tokenAmount.amount,info.tokenAmount.decimals);
    }
    return {tokenAddress,tokens:str(total),slot:r.context?.slot??null,observedAt:this.now().toISOString(),provider:'solana-rpc',providerTimestamp:null};
  }
}
module.exports={WalletWatchProvider,HeliusWalletWatchProvider,normalizeTransaction,id,SOL,USDC};
