'use strict';
// Public endpoint/schema evidence: https://api.geckoterminal.com/docs/v2/swagger.json
// Reviewed 2026-10-14; documented availability is not live coverage verification.
// Pool reserve changes are not proof of LP withdrawals; addresses are not people.
const { sharedRequestBudget, budgetError } = require('./requestBudget');
const ADDRESS = /^[1-9A-HJ-NP-Za-km-z]{32,44}$/;
const BASE = 'https://api.geckoterminal.com/api/v2/networks/solana';
function number(value, signed = false) {
  if ((typeof value !== 'number' && typeof value !== 'string') || value === '' || (typeof value === 'string' && !value.trim())) return null;
  const n = Number(value);
  return Number.isFinite(n) && (signed || n >= 0) ? n : null;
}
function count(value) { const n = number(value); return Number.isSafeInteger(n) ? n : null; }
function identity(pool, token, pinned) {
  const address = pool?.attributes?.address;
  if (!ADDRESS.test(address || '') || (pinned && pinned !== address)
    || pool?.id !== `solana_${address}`
    || pool?.relationships?.base_token?.data?.id !== `solana_${token}`) {
    throw new Error('distribution_pool_identity_or_orientation_mismatch');
  }
  return address;
}
class GeckoTerminalDistributionProvider {
  constructor({ fetchFn = globalThis.fetch, now = () => new Date(), requestBudget = sharedRequestBudget, timeoutMs = 5000 } = {}) {
    this.fetchFn = fetchFn; this.now = now; this.requestBudget = requestBudget;
    this.timeoutMs = Math.min(10000, Math.max(1, Number(timeoutMs) || 5000));
    this.pinned = null; this.cached = null; this.inflight = null;
  }
  async request(url) {
    if (!await this.requestBudget.consume('distribution')) throw budgetError();
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeoutMs);
    try {
      const response = await this.fetchFn(url, { headers: { Accept: 'application/json;version=20230203' }, signal: controller.signal, redirect: 'error' });
      if (response.status === 429) {
        const seconds = number(response.headers?.get?.('retry-after'));
        await this.requestBudget.backoff(this.now().getTime() + Math.max(60000, Math.min(3600000, (seconds || 60) * 1000)));
        throw new Error('distribution_provider_rate_limited');
      }
      if (!response.ok) throw new Error('distribution_provider_unavailable');
      const json = await response.json();
      if (json?.errors) throw new Error('distribution_provider_unavailable');
      return json;
    } finally { clearTimeout(timer); }
  }
  async getSnapshot({ tokenAddress, poolAddress } = {}) {
    if (!ADDRESS.test(tokenAddress || '') || (poolAddress && !ADDRESS.test(poolAddress))) throw new Error('distribution_invalid_address');
    if (this.pinned && (this.pinned.token !== tokenAddress || (poolAddress && this.pinned.pool !== poolAddress))) throw new Error('distribution_pinned_pair_changed');
    if (this.cached && this.now().getTime() >= Date.parse(this.cached.observedAt)
      && this.now().getTime() - Date.parse(this.cached.observedAt) < 60000) return structuredClone(this.cached);
    if (this.inflight) {
      if (this.inflight.token !== tokenAddress || this.inflight.pool !== poolAddress) throw new Error('distribution_concurrent_pair_changed');
      return structuredClone(await this.inflight.promise);
    }
    const promise = this.capture(tokenAddress, poolAddress);
    this.inflight = { token: tokenAddress, pool: poolAddress, promise };
    try { return structuredClone(await promise); } finally { this.inflight = null; }
  }
  async capture(tokenAddress, poolAddress) {
    let pairId = this.pinned?.pool || poolAddress;
    if (!pairId) {
      const json = await this.request(`${BASE}/tokens/${tokenAddress}/pools?page=1`);
      const pools = (Array.isArray(json?.data) ? json.data : []).filter(p => {
        try { identity(p, tokenAddress); return number(p.attributes.reserve_in_usd) !== null; } catch { return false; }
      }).sort((a, b) => Number(b.attributes.reserve_in_usd) - Number(a.attributes.reserve_in_usd));
      if (!pools.length) throw new Error('distribution_pool_unavailable');
      pairId = identity(pools[0], tokenAddress);
    }
    const json = await this.request(`${BASE}/pools/${pairId}?include_volume_breakdown=true&include_composition=true`);
    identity(json?.data, tokenAddress, pairId);
    const a = json.data.attributes;
    const priceUsd = number(a.base_token_price_usd);
    if (priceUsd === null || priceUsd <= 0) throw new Error('distribution_price_unavailable');
    const observedAt = this.now().toISOString();
    const t = a.transactions?.m5 || {};
    const snapshot = {
      tokenAddress, pairId, observedAt, providerTimestamp: null, provider: 'geckoterminal', freshness: 'UNKNOWN',
      priceUsd, marketCapUsd: number(a.market_cap_usd), liquidityUsd: number(a.reserve_in_usd),
      volume5mUsd: number(a.volume_usd?.m5), buys5m: count(t.buys), sells5m: count(t.sells),
      buyers5m: count(t.buyers), sellers5m: count(t.sellers),
      buyVolume5mUsd: number(a.buy_volume_usd?.m5), sellVolume5mUsd: number(a.sell_volume_usd?.m5),
      netBuyVolume5mUsd: number(a.net_buy_volume_usd?.m5, true),
      baseTokenBalance: number(a.base_token_balance), quoteTokenBalance: number(a.quote_token_balance),
      walletDistributionStatus: 'UNKNOWN', devIdentityStatus: 'UNKNOWN',
      provenance: { dataClass: 'EMPIRICAL', synthetic: false, testOnly: false,
        endpoint: 'pool_details', timestampBasis: 'response_received', sourceTimestamp: null,
        freshness: 'UNKNOWN', liquidityScope: 'pool_reserve', orientation: 'base', windowSeconds: 300 },
    };
    this.pinned = { token: tokenAddress, pool: pairId }; this.cached = snapshot;
    return snapshot;
  }
}
module.exports = { GeckoTerminalDistributionProvider, number, count };
