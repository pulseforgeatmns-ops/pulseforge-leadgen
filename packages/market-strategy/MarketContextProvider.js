'use strict';

const { assertContextProvider } = require('@pulseforge/reasoning-runtime');
const { evidenceRef } = require('./types');

const SUPPORTED_TYPES = new Set([
  'price_tick',
  'volume_update',
  'news_event',
  'economic_release',
  'volatility_observation',
  'market_session',
  'market_snapshot',
]);

/**
 * MarketContextProvider — translates raw market observations into normalized context.
 * No reasoning, confidence scoring, or ranking.
 */
class MarketContextProvider {
  /**
   * @param {object} [deps]
   * @param {string} [deps.id]
   */
  constructor(deps = {}) {
    this.id = deps.id || 'market-context';
    assertContextProvider(this);
  }

  /**
   * @param {object} input
   * @param {string} input.subjectId
   * @param {import('./types').RawMarketObservation[]} [input.observations]
   * @returns {import('./types').MarketContext}
   */
  build(input) {
    if (!input || !input.subjectId) {
      throw new Error('MarketContextProvider.build requires subjectId');
    }

    const subjectId = String(input.subjectId);
    const symbol = subjectId.toUpperCase();
    const rawObservations =
      input.observations && input.observations.length > 0
        ? input.observations
        : defaultObservations(symbol);

    const observations = [];
    const evidence = [];
    /** @type {Record<string, unknown>} */
    const metrics = {};
    /** @type {object|null} */
    let session = null;

    for (let i = 0; i < rawObservations.length; i++) {
      const raw = rawObservations[i];
      if (!raw || !raw.type || !SUPPORTED_TYPES.has(raw.type)) {
        continue;
      }
      const normalized = normalizeObservation(raw, i);
      observations.push(normalized);
      const ev = observationToEvidence(normalized);
      if (ev) evidence.push(ev);
      mergeMetrics(metrics, raw);
      if (raw.type === 'market_session') {
        session = {
          session: raw.session,
          status: raw.status,
          timestamp: raw.timestamp,
        };
      }
    }

    return {
      subjectId,
      asset: {
        id: subjectId,
        symbol,
        name: symbol,
      },
      observations,
      evidence: evidence.sort((a, b) => String(a.id).localeCompare(String(b.id))),
      claims: [],
      metrics,
      session,
      builtAt: String(input.builtAt || input.asOf || new Date().toISOString()),
      asOf: input.asOf ? String(input.asOf) : undefined,
      repositoryType: input.repositoryType || 'market-fixture',
    };
  }
}

/**
 * @param {string} symbol
 * @returns {import('./types').RawMarketObservation[]}
 */
function defaultObservations(symbol) {
  const now = new Date().toISOString();
  return [
    {
      type: 'market_snapshot',
      asset: symbol,
      price: 67250,
      volume24h: 28500000000,
      changePct: 2.4,
      timestamp: now,
    },
    {
      type: 'price_tick',
      asset: symbol,
      price: 67250,
      venue: 'coinbase',
      timestamp: now,
    },
    {
      type: 'volume_update',
      asset: symbol,
      volume: 1850000000,
      window: '1h',
      timestamp: now,
    },
    {
      type: 'volatility_observation',
      asset: symbol,
      value: 0.62,
      measure: 'realized_24h',
      timestamp: now,
    },
    {
      type: 'news_event',
      headline: `${symbol} ETF inflows reach weekly high`,
      symbols: [symbol],
      sentiment: 0.35,
      timestamp: now,
    },
    {
      type: 'market_session',
      session: 'us_regular',
      status: 'open',
      timestamp: now,
    },
  ];
}

/**
 * @param {import('./types').RawMarketObservation} raw
 * @param {number} index
 */
function normalizeObservation(raw, index) {
  const asset =
    'asset' in raw && raw.asset
      ? String(raw.asset)
      : 'symbols' in raw && raw.symbols && raw.symbols[0]
        ? String(raw.symbols[0])
        : 'unknown';

  const timestamp = String(raw.timestamp || raw.observedAt || new Date().toISOString());
  // Prefer caller-supplied deterministic id (SPEC-018); fall back to type+timestamp+venue.
  const id =
    raw.id ||
    `obs:${raw.type}:${asset}:${timestamp}:${raw.venue || index}`;

  return {
    id: String(id),
    observationType: raw.type,
    asset,
    timestamp,
    payload: { ...raw },
  };
}

/**
 * @param {import('./types').NormalizedObservation} obs
 * @returns {import('./types').MarketEvidenceRef|null}
 */
function observationToEvidence(obs) {
  const p = obs.payload;
  switch (obs.observationType) {
    case 'price_tick':
      return evidenceRef({
        id: `ev:${obs.id}`,
        kind: 'evidence',
        summary: `price_tick:${p.asset}@${p.price}`,
        sourceType: 'price_tick',
        sourceId: p.venue || null,
        confidence: 0.95,
      });
    case 'volume_update':
      return evidenceRef({
        id: `ev:${obs.id}`,
        kind: 'evidence',
        summary: `volume:${p.asset}:${p.window}=${p.volume}`,
        sourceType: 'volume_update',
        confidence: 0.9,
      });
    case 'news_event':
      return evidenceRef({
        id: `ev:${obs.id}`,
        kind: 'evidence',
        summary: `news:${String(p.headline).slice(0, 120)}`,
        sourceType: 'news_event',
        confidence: p.sentiment != null ? Math.abs(Number(p.sentiment)) : 0.6,
      });
    case 'economic_release':
      return evidenceRef({
        id: `ev:${obs.id}`,
        kind: 'evidence',
        summary: `macro:${p.series}:actual=${p.actual}:forecast=${p.forecast}`,
        sourceType: 'economic_release',
        confidence: 0.85,
      });
    case 'volatility_observation':
      return evidenceRef({
        id: `ev:${obs.id}`,
        kind: 'evidence',
        summary: `volatility:${p.asset}:${p.measure}=${p.value}`,
        sourceType: 'volatility_observation',
        confidence: 0.88,
      });
    case 'market_session':
      return evidenceRef({
        id: `ev:${obs.id}`,
        kind: 'evidence',
        summary: `session:${p.session}:${p.status}`,
        sourceType: 'market_session',
        confidence: 1,
      });
    case 'market_snapshot':
      return evidenceRef({
        id: `ev:${obs.id}`,
        kind: 'evidence',
        summary: `snapshot:${p.asset}:price=${p.price}:chg=${p.changePct}%`,
        sourceType: 'market_snapshot',
        confidence: 0.92,
      });
    default:
      return null;
  }
}

/**
 * @param {Record<string, unknown>} metrics
 * @param {import('./types').RawMarketObservation} raw
 */
function mergeMetrics(metrics, raw) {
  if (raw.type === 'market_snapshot') {
    metrics.price = raw.price;
    metrics.volume24h = raw.volume24h;
    metrics.changePct = raw.changePct;
  }
  if (raw.type === 'price_tick') {
    metrics.lastPrice = raw.price;
  }
  if (raw.type === 'volume_update') {
    metrics[`volume_${raw.window}`] = raw.volume;
  }
  if (raw.type === 'volatility_observation') {
    metrics[`volatility_${raw.measure}`] = raw.value;
  }
  if (raw.type === 'news_event') {
    metrics.newsCount = (Number(metrics.newsCount) || 0) + 1;
    if (raw.sentiment != null) {
      const prev = Number(metrics.avgNewsSentiment) || 0;
      const count = Number(metrics.newsCount) || 1;
      metrics.avgNewsSentiment = (prev * (count - 1) + Number(raw.sentiment)) / count;
    }
  }
  if (raw.type === 'economic_release') {
    metrics.lastEconomicRelease = raw.series;
    if (raw.actual != null && raw.forecast != null) {
      metrics.economicSurprise = raw.actual - raw.forecast;
    }
  }
}

/**
 * @param {object} [deps]
 * @returns {MarketContextProvider}
 */
function createMarketContextProvider(deps) {
  return new MarketContextProvider(deps);
}

module.exports = {
  MarketContextProvider,
  createMarketContextProvider,
  SUPPORTED_TYPES,
};
