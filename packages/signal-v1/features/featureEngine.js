'use strict';

const { randomUUID } = require('crypto');
const { FEATURE_VERSION } = require('../types');
const { calculateConvergence } = require('./convergence');
const { CONVERGENCE_WINDOWS_MINUTES } = require('../config/defaultConfig');
const { callStore } = require('../storage/storeUtils');
const { latestMarketContext } = require('../market/marketContext');
const { filterObservationsAtOrBefore } = require('../temporal/temporalFirewall');

const BUY_TYPES = new Set(['WALLET_BUY', 'DEV_BUY', 'WHALE_BUY']);
const SELL_TYPES = new Set(['WALLET_SELL', 'DEV_SELL', 'WHALE_SELL', 'DISTRIBUTION_SIGNAL']);

function latestPayloadEvents(events, types, evaluatedMs) {
  return events
    .filter(e => types.has(e.eventType) && e.occurredAt.getTime() <= evaluatedMs)
    .sort((a, b) => b.occurredAt - a.occurredAt);
}

async function buildFeatureSnapshot(store, tokenAddress, evaluatedAt, options = {}) {
  const evaluated = new Date(evaluatedAt);
  const evaluatedMs = evaluated.getTime();
  const events = await callStore(store, 'getEventsForToken', tokenAddress, {
    maxOccurredAt: evaluated,
    ...(options.eventsFilter || {}),
  });

  const allObservations = store.getMarketObservationsForToken
    ? await callStore(store, 'getMarketObservationsForToken', tokenAddress, {})
    : [];
  const observations = filterObservationsAtOrBefore(allObservations, evaluated);

  const convergence15 = calculateConvergence({
    store,
    tokenAddress,
    evaluatedAt: evaluated,
    windowMinutes: 15,
    events,
  });

  const marketSnapshots = latestPayloadEvents(events, new Set(['MARKET_SNAPSHOT']), evaluatedMs);
  const holderSnapshots = latestPayloadEvents(events, new Set(['HOLDER_SNAPSHOT']), evaluatedMs);
  const marketCtx = latestMarketContext(store, tokenAddress, evaluated);
  const market = marketSnapshots[0]?.payload || {};
  const holders = holderSnapshots[0]?.payload || {};

  const walletBuys = events.filter(
    e => BUY_TYPES.has(e.eventType) && e.occurredAt.getTime() <= evaluatedMs
  );
  const walletSells = events.filter(
    e => SELL_TYPES.has(e.eventType) && e.occurredAt.getTime() <= evaluatedMs
  );

  let profitableWalletBuyCount = 0;
  let profitableWalletSellCount = 0;
  for (const e of walletBuys) {
    if (e.payload && e.payload.profitableWallet === true) profitableWalletBuyCount += 1;
  }
  for (const e of walletSells) {
    if (e.payload && e.payload.profitableWallet === true) profitableWalletSellCount += 1;
  }

  const fiveMinStart = evaluatedMs - 5 * 60 * 1000;
  const recentMarket = events.filter(
    e =>
      e.eventType === 'MARKET_SNAPSHOT' &&
      e.occurredAt.getTime() >= fiveMinStart &&
      e.occurredAt.getTime() <= evaluatedMs
  );
  const uniqueBuyers5m = sumPayload(recentMarket, 'uniqueBuyers5m');
  const uniqueSellers5m = sumPayload(recentMarket, 'uniqueSellers5m');
  const volume5mUsd = sumPayload(recentMarket, 'volume5mUsd');

  const priorWindowStart = evaluatedMs - 10 * 60 * 1000;
  const priorMarket = events.filter(
    e =>
      e.eventType === 'MARKET_SNAPSHOT' &&
      e.occurredAt.getTime() >= priorWindowStart &&
      e.occurredAt.getTime() < fiveMinStart
  );
  const priorVolume = sumPayload(priorMarket, 'volume5mUsd');
  let volumeAcceleration = null;
  if (priorVolume != null && priorVolume > 0 && volume5mUsd != null) {
    volumeAcceleration = volume5mUsd / priorVolume;
  }

  const dexRankEvent = latestPayloadEvents(events, new Set(['DEX_RANK_CHANGE']), evaluatedMs)[0];
  const fomoRankEvent = latestPayloadEvents(events, new Set(['FOMO_RANK_CHANGE']), evaluatedMs)[0];
  const socialAccel = latestPayloadEvents(events, new Set(['SOCIAL_ACCELERATION']), evaluatedMs)[0];
  const amplifier = latestPayloadEvents(events, new Set(['AMPLIFIER_ENTRY']), evaluatedMs)[0];

  const distributionSignals = events.filter(
    e =>
      (e.eventType === 'DISTRIBUTION_SIGNAL' || e.eventType === 'WHALE_SELL' || e.eventType === 'DEV_SELL') &&
      e.occurredAt.getTime() <= evaluatedMs
  );

  const whaleDistributionDetected = distributionSignals.some(
    e => e.eventType === 'WHALE_SELL' || e.payload?.distributionType === 'whale'
  );
  const devDistributionDetected = distributionSignals.some(
    e => e.eventType === 'DEV_SELL' || e.payload?.distributionType === 'dev'
  );
  const smartWalletDistributionDetected = distributionSignals.some(
    e => e.payload?.distributionType === 'smart_wallet' || (e.payload?.profitableWallet === true && SELL_TYPES.has(e.eventType))
  );
  const liquidityDeteriorationDetected = events.some(
    e =>
      e.eventType === 'LIQUIDITY_CHANGE' &&
      e.occurredAt.getTime() <= evaluatedMs &&
      e.payload &&
      e.payload.deltaPct != null &&
      Number(e.payload.deltaPct) <= -15
  );

  const evidenceEventIds = [
    ...new Set([
      ...convergence15.evidenceEventIds,
      marketSnapshots[0]?.id,
      holderSnapshots[0]?.id,
      dexRankEvent?.id,
      fomoRankEvent?.id,
      socialAccel?.id,
      amplifier?.id,
      ...distributionSignals.map(e => e.id),
    ].filter(Boolean)),
  ];

  const features = {
    rawSourceCount: convergence15.rawSourceCount,
    independentClusterCount: convergence15.independentClusterCount,
    convergenceVelocity: convergence15.convergenceVelocity,
    qualityWeightedConvergence: convergence15.qualityWeightedConvergence,

    profitableWalletBuyCount,
    profitableWalletSellCount,
    walletAccumulationScore: scoreWalletAccumulation(profitableWalletBuyCount, profitableWalletSellCount),
    walletDistributionScore: scoreWalletDistribution(smartWalletDistributionDetected, profitableWalletSellCount),

    marketCap: marketCtx.marketCapUsd ?? numOrNull(market.marketCapUsd ?? market.marketCap),
    liquidityUsd: marketCtx.liquidityUsd ?? numOrNull(market.liquidityUsd),
    tokenAgeSeconds: numOrNull(market.tokenAgeSeconds),
    bundledSupplyPct: numOrNull(holders.bundledSupplyPct),
    top10HolderPct: numOrNull(holders.top10HolderPct),
    devHoldingPct: numOrNull(holders.devHoldingPct),
    priceUsd: marketCtx.priceUsd,

    uniqueBuyers5m: numOrNull(uniqueBuyers5m),
    uniqueSellers5m: numOrNull(uniqueSellers5m),
    volume5mUsd: numOrNull(volume5mUsd),
    volumeAcceleration,
    priceAcceleration: numOrNull(market.priceAcceleration),

    fomoRank: numOrNull(fomoRankEvent?.payload?.rank),
    dexRank: numOrNull(dexRankEvent?.payload?.rank),
    socialMentionVelocity: numOrNull(socialAccel?.payload?.mentionVelocity),
    amplifierStage: numOrNull(amplifier?.payload?.stage),

    whaleDistributionDetected,
    devDistributionDetected,
    smartWalletDistributionDetected,
    liquidityDeteriorationDetected,

    knownObservationCount: observations.length,

    convergenceByWindow: Object.fromEntries(
      (options.windows || CONVERGENCE_WINDOWS_MINUTES).map(w => [
        `${w}m`,
        calculateConvergence({ store, tokenAddress, evaluatedAt: evaluated, windowMinutes: w, events }),
      ])
    ),
  };

  return {
    id: randomUUID(),
    tokenAddress,
    evaluatedAt: evaluated,
    featureVersion: options.featureVersion || FEATURE_VERSION,
    features,
    evidenceEventIds,
  };
}

function numOrNull(value) {
  if (value == null || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function sumPayload(events, key) {
  let total = 0;
  let seen = false;
  for (const e of events) {
    if (e.payload && e.payload[key] != null) {
      total += Number(e.payload[key]) || 0;
      seen = true;
    }
  }
  return seen ? total : null;
}

function scoreWalletAccumulation(buys, sells) {
  if (buys === 0 && sells === 0) return null;
  return Math.max(0, Math.min(1, (buys - sells * 0.5) / 3));
}

function scoreWalletDistribution(smartDistribution, profitableSells) {
  if (!smartDistribution && profitableSells === 0) return null;
  return Math.max(0, Math.min(1, (smartDistribution ? 0.7 : 0) + profitableSells * 0.15));
}

module.exports = {
  buildFeatureSnapshot,
};
