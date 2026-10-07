'use strict';

const PROVIDER_ID = 'geckoterminal';

/**
 * GeckoTerminal public OHLCV adapter (Solana pools).
 * Minute candles when aggregate=1; does not interpolate finer resolution.
 */
class GeckoTerminalMarketDataProvider {
  constructor(options = {}) {
    this.providerId = PROVIDER_ID;
    /** When false, caller must not substitute a different time range silently. */
    this.honorsRequestedHistoricalRange = true;
    this.baseUrl = options.baseUrl || 'https://api.geckoterminal.com/api/v2';
    this.network = options.network || 'solana';
    this.fetchFn = options.fetchFn || global.fetch;
    this.rateLimitMs = options.rateLimitMs ?? 1200;
    this.maxRetries = options.maxRetries ?? 5;
    this.timeoutMs = options.timeoutMs ?? 10000;
    this._lastFetchAt = 0;
    this.now = options.now || (() => new Date());
  }

  async getTokenSnapshot(tokenAddress, asOf) {
    const pool = await this._resolvePrimaryPool(tokenAddress);
    const end = new Date(asOf);
    const start = new Date(end.getTime() - 60 * 60 * 1000);
    const obs = await this.getHistoricalPrices(tokenAddress, start, end, { poolAddress: pool });
    const latest = obs.length ? obs[obs.length - 1] : null;
    return {
      tokenAddress,
      requestedAt: end,
      asOf: latest?.occurredAt || null,
      occurredAt: latest?.occurredAt || null,
      providerTimestamp: latest?.providerTimestamp || null,
      observedTimestamp: latest?.observedTimestamp || null,
      intervalSeconds: 60,
      provenance: { ...(latest?.provenance || {}), snapshotKind: 'historical_candle' },
      priceUsd: latest?.priceUsd ?? null,
      marketCapUsd: latest?.marketCapUsd ?? null,
      liquidityUsd: latest?.liquidityUsd ?? null,
      provider: PROVIDER_ID,
    };
  }

  // A current API response is a sampled quote, not proof of a fresh trade.
  // This endpoint provides no price timestamp; preserve that as unknown.
  async getLiveTokenSnapshot(tokenAddress) {
    const requestedAt = this.now();
    const json = await this._fetchJson(
      `${this.baseUrl}/networks/${this.network}/tokens/${encodeURIComponent(tokenAddress)}`
    );
    const sampledAt = this.now();
    const attrs = json?.data?.attributes;
    if (!attrs || attrs.address !== tokenAddress) throw new Error('market_token_identity_mismatch');
    const number = value => value != null && value !== '' && Number.isFinite(Number(value))
      && Number(value) >= 0 ? Number(value) : null;
    return {
      tokenAddress,
      priceUsd: number(attrs.price_usd),
      marketCapUsd: number(attrs.market_cap_usd),
      liquidityUsd: number(attrs.total_reserve_in_usd),
      occurredAt: sampledAt,
      sampledAt,
      observedTimestamp: sampledAt,
      providerTimestamp: null,
      intervalSeconds: 0,
      provider: PROVIDER_ID,
      provenance: {
        dataClass: 'EMPIRICAL', snapshotKind: 'live_api_sample', timestampBasis: 'response_received',
        sourceTimestamp: null, requestedAt: requestedAt.toISOString(),
        freshness: 'UNKNOWN', liquidityScope: 'token_total_reserve',
      },
    };
  }

  async getHistoricalPrice(tokenAddress, start, end, options = {}) {
    return this.getHistoricalPrices(tokenAddress, start, end, options);
  }

  async getHistoricalPrices(tokenAddress, start, end, options = {}) {
    const poolAddress = options.poolAddress || (await this._resolvePrimaryPool(tokenAddress));
    const startMs = new Date(start).getTime();
    const endMs = new Date(end).getTime();
    if (endMs <= startMs) return [];

    const resolutionSeconds = options.resolutionSeconds ?? 60;
    const aggregate = Math.max(1, Math.round(resolutionSeconds / 60));

    const collected = [];
    let beforeTimestamp = Math.floor(endMs / 1000);
    const safetyPages = options.maxPages ?? 500;

    for (let page = 0; page < safetyPages; page += 1) {
      const url =
        `${this.baseUrl}/networks/${this.network}/pools/${poolAddress}/ohlcv/minute` +
        `?aggregate=${aggregate}&limit=1000&before_timestamp=${beforeTimestamp}`;
      const json = await this._fetchJson(url);
      if (json.errors && json.errors.length) {
        const msg = json.errors.map(e => e.title || e.detail).join('; ');
        throw new Error(`GeckoTerminal OHLCV error: ${msg}`);
      }

      const list = json?.data?.attributes?.ohlcv_list || [];
      if (!list.length) break;

      for (const row of list) {
        const [ts, , , , close, volume] = row;
        // OHLCV timestamps identify the bar; a close is not known at bar open.
        const providerTimestamp = new Date(ts * 1000);
        const occurredAt = new Date((ts + aggregate * 60) * 1000);
        const tMs = occurredAt.getTime();
        if (tMs < startMs || tMs > endMs) continue;
        collected.push(normalizeObservation({
          tokenAddress,
          occurredAt,
          providerTimestamp,
          observedTimestamp: this.now(),
          priceUsd: close,
          volumeIntervalUsd: volume,
          intervalSeconds: aggregate * 60,
          poolAddress,
        }));
      }

      const oldestTs = list[list.length - 1][0];
      if (oldestTs * 1000 <= startMs) break;
      if (oldestTs >= beforeTimestamp) break;
      beforeTimestamp = oldestTs;
    }

    return dedupeByTimestamp(collected).sort((a, b) => a.occurredAt - b.occurredAt);
  }

  async getLiquidity(tokenAddress, asOf) {
    const pool = await this._resolvePrimaryPool(tokenAddress);
    const url = `${this.baseUrl}/networks/${this.network}/pools/${pool}`;
    void asOf;
    const json = await this._fetchJson(url);
    const reserve = json?.data?.attributes?.reserve_in_usd;
    return {
      liquidityUsd: reserve != null ? Number(reserve) : null,
      provider: PROVIDER_ID,
      poolAddress: pool,
    };
  }

  async _resolvePrimaryPool(tokenAddress) {
    if (this._poolCache && this._poolCache.token === tokenAddress) {
      return this._poolCache.pool;
    }
    const url = `${this.baseUrl}/networks/${this.network}/tokens/${tokenAddress}/pools?page=1`;
    const json = await this._fetchJson(url);
    const pools = json?.data || [];
    if (!pools.length) {
      throw new Error(`No GeckoTerminal pool found for token ${tokenAddress}`);
    }
    pools.sort((a, b) => {
      const liqA = Number(a.attributes?.reserve_in_usd || 0);
      const liqB = Number(b.attributes?.reserve_in_usd || 0);
      return liqB - liqA;
    });
    const pool = pools[0].attributes?.address || pools[0].id?.replace(/^solana_/, '');
    this._poolCache = { token: tokenAddress, pool };
    return pool;
  }

  async _fetchJson(url) {
    let attempt = 0;
    while (attempt <= this.maxRetries) {
      await this._rateLimit();
      const res = await this.fetchFn(url, {
        headers: { Accept: 'application/json' },
        signal: AbortSignal.timeout(this.timeoutMs),
      });
      const text = await res.text();
      let json;
      try {
        json = JSON.parse(text);
      } catch {
        throw new Error(`GeckoTerminal non-JSON response (${res.status}): ${text.slice(0, 200)}`);
      }
      if (res.status === 429 || json?.status?.error_code === 429) {
        attempt += 1;
        const backoff = this.rateLimitMs * (attempt + 1);
        await new Promise(r => setTimeout(r, backoff));
        continue;
      }
      if (!res.ok && !json.errors) {
        throw new Error(`GeckoTerminal HTTP ${res.status}: ${text.slice(0, 200)}`);
      }
      return json;
    }
    throw new Error('GeckoTerminal rate limit exceeded after retries');
  }

  async _rateLimit() {
    const now = Date.now();
    const next = Math.max(now, this._lastFetchAt + this.rateLimitMs);
    this._lastFetchAt = next;
    if (next > now) await new Promise(r => setTimeout(r, next - now));
  }
}

function normalizeObservation({
  tokenAddress,
  occurredAt,
  providerTimestamp,
  observedTimestamp,
  priceUsd,
  volumeIntervalUsd,
  intervalSeconds,
  poolAddress,
}) {
  const price = Number(priceUsd);
  if (!Number.isFinite(price) || price <= 0) return null;
  return {
    tokenAddress,
    occurredAt,
    priceUsd: price,
    marketCapUsd: null,
    liquidityUsd: null,
    volumeIntervalUsd: volumeIntervalUsd != null ? Number(volumeIntervalUsd) : null,
    intervalSeconds,
    provider: PROVIDER_ID,
    externalId: `${poolAddress}:${Math.floor(occurredAt.getTime() / 1000)}`,
    providerTimestamp,
    observedTimestamp,
    provenance: {
      dataClass: 'EMPIRICAL', poolAddress,
      candle: 'close',
      timestampBasis: 'candle_close',
      candleOpenAt: providerTimestamp.toISOString(),
      candleCloseAt: occurredAt.toISOString(),
    },
  };
}

function dedupeByTimestamp(rows) {
  const map = new Map();
  for (const row of rows) {
    if (!row) continue;
    map.set(row.occurredAt.toISOString(), row);
  }
  return [...map.values()];
}

module.exports = {
  GeckoTerminalMarketDataProvider,
  PROVIDER_ID,
};
