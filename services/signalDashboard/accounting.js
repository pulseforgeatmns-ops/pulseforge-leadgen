'use strict';
const {dec, str, mul, div} = require('./decimal');
const validTime = v => Number.isFinite(Date.parse(v));
function order(a, b) {
  return Date.parse(a.blockTime) - Date.parse(b.blockTime) || (a.slot ?? 0) - (b.slot ?? 0)
    || (a.transactionIndex ?? 0) - (b.transactionIndex ?? 0) || a.signature.localeCompare(b.signature);
}
function account(transactions, {priceUsd = null, balance = null, asOf = new Date().toISOString(), historyComplete = false} = {}) {
  const seen = new Map(), issues = new Set(), lots = [], timeline = [];
  for (const t of transactions) {
    if (!validTime(t.blockTime) || !validTime(t.knownAt) || Date.parse(t.blockTime) > Date.parse(asOf) || Date.parse(t.knownAt) > Date.parse(asOf)) continue;
    const key = `${t.signature}:${t.tokenAddress}`;
    if (seen.has(key) && JSON.stringify(seen.get(key)) !== JSON.stringify(t)) issues.add('CONFLICTING_TRANSACTION');
    else seen.set(key, t);
  }
  const txs = [...seen.values()].sort(order);
  let purchased = 0n, sold = 0n, remaining = 0n, deployed = 0n, proceeds = 0n, realized = 0n, fees = 0n;
  let deployedKnown = true, proceedsKnown = true, realizedKnown = true, feesKnown = true, lifecycle = 'DISCOVERY';
  let everEntered = false, hadSell = false;
  if (!historyComplete) issues.add('HISTORY_INCOMPLETE');
  const consume = quantity => {
    let left = quantity, cost = 0n, known = true;
    while (left > 0n && lots.length) {
      const lot = lots[0], take = left < lot.quantity ? left : lot.quantity;
      if (lot.cost === null) known = false;
      else { const portion = take === lot.quantity ? lot.cost : lot.cost * take / lot.quantity; cost += portion; lot.cost -= portion; }
      lot.quantity -= take; left -= take;
      if (!lot.quantity) lots.shift();
    }
    if (left > 0n) { known = false; issues.add('DISPOSAL_EXCEEDS_OBSERVED_LOTS'); }
    return known ? cost : null;
  };
  for (let i = 0; i < txs.length; i++) {
    const t = txs[i], q = dec(t.tokenAmount), cash = dec(t.valueUsd), fee = dec(t.feesUsd);
    if (q === null || q <= 0n || !['BUY','SELL','TRANSFER_IN','TRANSFER_OUT'].includes(t.type)) { issues.add('INVALID_TRANSACTION'); continue; }
    if (i && t.slot != null && t.slot === txs[i-1].slot && t.signature !== txs[i-1].signature && (t.transactionIndex == null || txs[i-1].transactionIndex == null)) issues.add('SAME_SLOT_ORDER_UNKNOWN');
    if (cash !== null && cash < 0n || fee !== null && fee < 0n) throw new Error('negative_trade_value');
    if (fee === null) feesKnown = false; else fees += fee;
    if (t.type === 'BUY') {
      purchased += q; remaining += q;
      if (cash === null) deployedKnown = false; else deployed += cash;
      lots.push({signature:t.signature, acquiredAt:t.blockTime, quantity:q, cost:cash === null || fee === null ? null : cash + fee});
      lifecycle = hadSell ? 'RE_ENTRY' : 'ENTRY'; everEntered = true;
    } else if (t.type === 'TRANSFER_IN') {
      remaining += q; lots.push({signature:t.signature, acquiredAt:t.blockTime, quantity:q, cost:null});
      issues.add('TRANSFER_BASIS_UNKNOWN');
    } else {
      const cost = consume(q); remaining -= q;
      if (t.type === 'SELL') {
        sold += q; hadSell = true;
        if (cash === null) proceedsKnown = false; else proceeds += cash;
        if (cost === null || cash === null || fee === null) realizedKnown = false; else realized += cash - fee - cost;
        lifecycle = remaining === 0n ? 'EXIT' : deployedKnown && proceedsKnown && feesKnown && proceeds >= deployed + fees ? 'DE_RISK' : 'RUNNER';
      } else { issues.add('TRANSFER_OUT_NOT_REALIZATION'); }
    }
    if (remaining < 0n) issues.add('NEGATIVE_OBSERVED_BALANCE');
    timeline.push({...t, positionAfter:str(remaining), lifecycle});
  }
  const remainingCost = lots.some(l => l.cost === null) ? null : lots.reduce((s,l) => s + l.cost, 0n);
  const positionValue = remaining >= 0n ? mul(remaining, dec(priceUsd)) : null;
  const unrealized = positionValue === null || remainingCost === null ? null : positionValue - remainingCost;
  const netCashFlow = deployedKnown && proceedsKnown && feesKnown ? proceeds - deployed - fees : null;
  const reconciliation = balance === null ? {state:'UNKNOWN', walletBalance:null, difference:null}
    : {state:dec(balance) === remaining ? 'MATCH' : 'MISMATCH', walletBalance:balance, difference:str(dec(balance)-remaining)};
  if (reconciliation.state === 'MISMATCH') issues.add('WALLET_BALANCE_MISMATCH');
  const unsafe = !historyComplete || issues.has('SAME_SLOT_ORDER_UNKNOWN') || issues.has('CONFLICTING_TRANSACTION') || issues.has('DISPOSAL_EXCEEDS_OBSERVED_LOTS') || issues.has('NEGATIVE_OBSERVED_BALANCE');
  // Partial ledger sums are useful, but never masquerade as complete P&L.
  return {method:'FIFO', purpose:'trade analytics; not tax accounting', quality:issues.size || !feesKnown || !deployedKnown || !proceedsKnown ? 'PARTIAL' : 'LIVE',
    grossCapitalDeployed:str(deployedKnown ? deployed : null), grossProceedsRealized:str(proceedsKnown ? proceeds : null),
    realizedCash:str(proceedsKnown ? proceeds : null), netCashFlow:str(netCashFlow),
    tokensPurchased:str(purchased), tokensSold:str(sold), tokensRemaining:str(remaining),
    weightedAverageAcquisitionPrice:str(deployedKnown ? div(deployed,purchased) : null),
    remainingCostBasis:str(remainingCost), realizedPnl:str(!unsafe && realizedKnown ? realized : null),
    unrealizedPnl:str(!unsafe ? unrealized : null), totalPnl:str(!unsafe && realizedKnown && unrealized !== null ? realized+unrealized : null),
    positionValue:str(positionValue), feesUsd:str(feesKnown ? fees : null),
    netCapitalAtRisk:str(netCashFlow === null || unsafe ? null : netCashFlow < 0n ? -netCashFlow : 0n),
    lifecycle:everEntered ? lifecycle : 'DISCOVERY', reconciliation, issues:[...issues], transactions:timeline,
    rawLots:txs, remainingLots:lots.map(l=>({...l, quantity:str(l.quantity), cost:str(l.cost)}))};
}
module.exports = {account, order};
