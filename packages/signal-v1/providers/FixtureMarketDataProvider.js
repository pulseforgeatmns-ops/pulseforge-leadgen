'use strict';

/**
 * Fixture-backed market data for research replay.
 */
class FixtureMarketDataProvider {
  /**
   * @param {{ pricePaths?: Record<string, { occurredAt: Date|string, price: number }[]> }} fixtures
   */
  constructor(fixtures = {}) {
    this.pricePaths = fixtures.pricePaths || {};
  }

  async getTokenSnapshot(tokenAddress, asOf) {
    const price = await this._priceAt(tokenAddress, asOf);
    return { tokenAddress, asOf, priceUsd: price, liquidityUsd: null, marketCapUsd: null };
  }

  async getHistoricalPrice(tokenAddress, start, end) {
    const path = this.pricePaths[tokenAddress] || [];
    const startMs = new Date(start).getTime();
    const endMs = new Date(end).getTime();
    return path.filter(p => {
      const t = new Date(p.occurredAt).getTime();
      return t >= startMs && t <= endMs;
    });
  }

  async getLiquidity() {
    return { liquidityUsd: null };
  }

  async _priceAt(tokenAddress, asOf) {
    const path = [...(this.pricePaths[tokenAddress] || [])].sort(
      (a, b) => new Date(a.occurredAt) - new Date(b.occurredAt)
    );
    const atMs = new Date(asOf).getTime();
    let price = null;
    for (const point of path) {
      if (new Date(point.occurredAt).getTime() <= atMs) price = point.price;
      else break;
    }
    return price;
  }
}

module.exports = {
  FixtureMarketDataProvider,
};
