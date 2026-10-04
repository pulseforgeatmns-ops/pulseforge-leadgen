'use strict';

const { CALL_EVENT_TYPES } = require('../features/convergence');
const { DEFAULT_RESEARCH_CONFIG } = require('../config/defaultConfig');
const { getSourceQualityAsOf } = require('./sourceQuality');
const { getWalletQualityAsOf } = require('./walletQuality');
const { evaluateStructureGate } = require('./structureGate');

const CALLER_EVENT_TYPES = new Set(['CALL', 'TOKEN_MENTION']);
const BUY_TYPES = new Set(['WALLET_BUY', 'WHALE_BUY', 'DEV_BUY']);
const DISTRIBUTION_TYPES = new Set([
  'WALLET_SELL',
  'WHALE_SELL',
  'DEV_SELL',
  'DISTRIBUTION_SIGNAL',
]);

function isValidCallerEvent(event) {
  if (!CALLER_EVENT_TYPES.has(event.eventType)) return false;
  if (event.payload?.selfPromotion === true && event.payload?.allowSelfPromotionAsCaller !== true) {
    return false;
  }
  return Boolean(event.sourceId || event.sourceType);
}

function clusterIdForEvent(store, event) {
  return event.sourceClusterId || store.getClusterIdForSource(event.sourceId) || event.sourceId;
}

function isAmplifierEvent(store, event) {
  if (event.eventType === 'AMPLIFIER_ENTRY') return true;
  if (!event.sourceId) return false;
  const source = store.sources.get(event.sourceId);
  return source?.metadata?.isAmplifier === true;
}

function hasMaterialDistribution(store, tokenAddress, beforeMs) {
  return store
    .getEventsForToken(tokenAddress, { maxOccurredAt: new Date(beforeMs) })
    .some(
      e =>
        DISTRIBUTION_TYPES.has(e.eventType) &&
        (e.eventType === 'WHALE_SELL' ||
          e.eventType === 'DEV_SELL' ||
          e.payload?.distributionType === 'whale' ||
          e.payload?.distributionType === 'dev')
    );
}

function gatherIndependentConvergence(store, tokenAddress, evaluatedAt, config) {
  const cfg = { ...DEFAULT_RESEARCH_CONFIG, ...config };
  const evaluatedMs = new Date(evaluatedAt).getTime();
  const windowStartMs = evaluatedMs - cfg.independentConvergenceWindowMinutes * 60 * 1000;

  const events = store
    .getEventsForToken(tokenAddress, { maxOccurredAt: evaluatedAt })
    .filter(
      e =>
        CALL_EVENT_TYPES.has(e.eventType) &&
        e.occurredAt.getTime() >= windowStartMs &&
        e.occurredAt.getTime() <= evaluatedMs &&
        isValidCallerEvent(e)
    );

  const rawSourceIds = [];
  const clusterFirst = new Map();
  const clusterLatest = new Map();
  const evidenceEventIds = [];

  for (const event of events) {
    evidenceEventIds.push(event.id);
    if (!event.sourceId) continue;
    rawSourceIds.push(event.sourceId);
    const clusterId = clusterIdForEvent(store, event);
    if (!clusterFirst.has(clusterId)) {
      clusterFirst.set(clusterId, event.occurredAt.getTime());
    }
    clusterLatest.set(clusterId, event.occurredAt.getTime());
  }

  const independentClusterCount = clusterFirst.size;
  const independentTimes = [...clusterFirst.values()].sort((a, b) => a - b);
  const firstCallMs = independentTimes.length ? independentTimes[0] : null;
  const latestQualifyingMs = independentTimes.length
    ? independentTimes[independentTimes.length - 1]
    : null;
  const convergenceDurationMinutes =
    firstCallMs != null && latestQualifyingMs != null
      ? (latestQualifyingMs - firstCallMs) / 60000
      : null;

  return {
    rawSourceCount: rawSourceIds.length,
    uniqueSourceCount: new Set(rawSourceIds.filter(Boolean)).size,
    independentClusterCount,
    firstCallTimestamp: firstCallMs ? new Date(firstCallMs) : null,
    latestQualifyingCallTimestamp: latestQualifyingMs ? new Date(latestQualifyingMs) : null,
    convergenceDurationMinutes,
    evidenceEventIds,
    clusterIds: [...clusterFirst.keys()],
  };
}

function evaluateFirstCaller(store, tokenAddress, evaluatedAt) {
  const events = store
    .getEventsForToken(tokenAddress, { maxOccurredAt: evaluatedAt })
    .filter(e => isValidCallerEvent(e) && e.occurredAt.getTime() <= new Date(evaluatedAt).getTime())
    .sort((a, b) => a.occurredAt - b.occurredAt || a.id.localeCompare(b.id));

  const first = events[0];
  if (!first) return null;

  return {
    occurredAt: first.occurredAt,
    triggerEventIds: [first.id],
    evidenceEventIds: [first.id],
    metadata: {
      sourceId: first.sourceId,
      sourceClusterId: clusterIdForEvent(store, first),
      sourceType: first.sourceType,
      marketState: snapshotMarketState(store, tokenAddress, evaluatedAt),
    },
  };
}

function evaluateIndependentConvergence(store, tokenAddress, evaluatedAt, config) {
  const cfg = { ...DEFAULT_RESEARCH_CONFIG, ...config };
  const conv = gatherIndependentConvergence(store, tokenAddress, evaluatedAt, cfg);
  if (conv.independentClusterCount < cfg.minIndependentClusters) return null;

  return {
    occurredAt: conv.latestQualifyingCallTimestamp || new Date(evaluatedAt),
    triggerEventIds: conv.evidenceEventIds,
    evidenceEventIds: conv.evidenceEventIds,
    metadata: {
      rawSourceCount: conv.rawSourceCount,
      uniqueSourceCount: conv.uniqueSourceCount,
      independentClusterCount: conv.independentClusterCount,
      firstCallTimestamp: conv.firstCallTimestamp?.toISOString(),
      latestQualifyingCallTimestamp: conv.latestQualifyingCallTimestamp?.toISOString(),
      convergenceDurationMinutes: conv.convergenceDurationMinutes,
      marketState: snapshotMarketState(store, tokenAddress, evaluatedAt),
    },
  };
}

function evaluateQualityConvergence(store, tokenAddress, evaluatedAt, config) {
  const cfg = { ...DEFAULT_RESEARCH_CONFIG, ...config };
  const base = evaluateIndependentConvergence(store, tokenAddress, evaluatedAt, cfg);
  if (!base) return null;

  const conv = gatherIndependentConvergence(store, tokenAddress, evaluatedAt, cfg);
  let qualifyingWithQuality = 0;
  const qualityByCluster = [];

  for (const clusterId of conv.clusterIds) {
    const clusterEvents = store
      .getEventsForToken(tokenAddress, { maxOccurredAt: evaluatedAt })
      .filter(
        e =>
          isValidCallerEvent(e) &&
          CALL_EVENT_TYPES.has(e.eventType) &&
          clusterIdForEvent(store, e) === clusterId
      );
    const sourceIds = [...new Set(clusterEvents.map(e => e.sourceId).filter(Boolean))];
    let clusterHasQuality = false;
    for (const sourceId of sourceIds) {
      const q = getSourceQualityAsOf(store, sourceId, evaluatedAt, cfg);
      if (q.quality !== 'unavailable') {
        clusterHasQuality = true;
        qualityByCluster.push({ clusterId, sourceId, quality: q.quality, sampleSize: q.sampleSize });
      }
    }
    if (clusterHasQuality) qualifyingWithQuality += 1;
  }

  if (qualifyingWithQuality < cfg.minQualitySourcesForQualityConvergence) {
    return null;
  }

  return {
    ...base,
    metadata: {
      ...base.metadata,
      qualifyingIndependentSourcesWithQuality: qualifyingWithQuality,
      qualityByCluster,
    },
  };
}

function evaluateWalletConfirmation(store, tokenAddress, evaluatedAt, researchState, config) {
  const cfg = { ...DEFAULT_RESEARCH_CONFIG, ...config };
  const firstCallerAt = researchState.firstCallerAt;
  if (!firstCallerAt) {
    return {
      unavailable: true,
      metadata: { walletConfirmation: 'unavailable', reason: 'no_first_caller' },
    };
  }

  const evaluatedMs = new Date(evaluatedAt).getTime();
  const firstCallerMs = new Date(firstCallerAt).getTime();
  if (evaluatedMs < firstCallerMs) return null;

  if (hasMaterialDistribution(store, tokenAddress, evaluatedMs)) {
    return {
      negative: true,
      metadata: { walletConfirmation: false, reason: 'material_distribution_before_eval' },
    };
  }

  const buys = store
    .getEventsForToken(tokenAddress, { maxOccurredAt: evaluatedAt })
    .filter(
      e =>
        BUY_TYPES.has(e.eventType) &&
        e.occurredAt.getTime() > firstCallerMs &&
        e.occurredAt.getTime() <= evaluatedMs
    );

  if (!buys.length) {
    return {
      negative: true,
      metadata: { walletConfirmation: false, reason: 'no_qualifying_buys' },
    };
  }

  let sawUnavailable = false;
  for (const buy of buys) {
    const wallet = buy.walletAddress || buy.payload?.walletAddress;
    const wq = getWalletQualityAsOf(store, wallet, evaluatedAt, cfg);
    if (wq.quality === 'unavailable') {
      sawUnavailable = true;
      continue;
    }
    if (wq.classification === 'historically_profitable') {
      return {
        occurredAt: buy.occurredAt,
        triggerEventIds: [buy.id],
        evidenceEventIds: [buy.id],
        metadata: {
          walletConfirmation: true,
          walletAddress: wallet,
          walletQuality: wq,
          marketState: snapshotMarketState(store, tokenAddress, evaluatedAt),
        },
      };
    }
  }

  if (sawUnavailable) {
    return {
      unavailable: true,
      metadata: { walletConfirmation: 'unavailable', reason: 'wallet_quality_unavailable' },
    };
  }

  return {
    negative: true,
    metadata: { walletConfirmation: false, reason: 'no_profitable_wallet_buy' },
  };
}

function evaluateStructureGateObservation(features, evaluatedAt) {
  const gate = evaluateStructureGate(features);
  if (gate.result !== 'PASS') return null;

  return {
    occurredAt: new Date(evaluatedAt),
    triggerEventIds: [],
    evidenceEventIds: [],
    metadata: {
      structureGate: gate.result,
      gateComponents: gate.components,
      marketState: {
        marketCap: features.marketCap,
        liquidityUsd: features.liquidityUsd,
      },
    },
  };
}

function evaluateAmplifierArrival(store, tokenAddress, evaluatedAt, researchState) {
  if (!researchState.hasPriorObservation) return null;

  const events = store
    .getEventsForToken(tokenAddress, { maxOccurredAt: evaluatedAt })
    .filter(
      e =>
        isAmplifierEvent(store, e) &&
        e.occurredAt.getTime() <= new Date(evaluatedAt).getTime()
    )
    .sort((a, b) => a.occurredAt - b.occurredAt || a.id.localeCompare(b.id));

  const first = events[0];
  if (!first) return null;

  return {
    occurredAt: first.occurredAt,
    triggerEventIds: [first.id],
    evidenceEventIds: [first.id],
    metadata: {
      amplifierSourceId: first.sourceId,
      amplifierClusterId: clusterIdForEvent(store, first),
      priorObservationState: researchState.priorObservationTypes,
      marketState: snapshotMarketState(store, tokenAddress, evaluatedAt),
    },
  };
}

function evaluateSignalEntry(decisionState, evaluatedAt, snapshotId, explanation) {
  if (decisionState !== 'ENTRY') return null;
  return {
    occurredAt: new Date(evaluatedAt),
    triggerEventIds: [],
    evidenceEventIds: [],
    metadata: {
      strategyState: 'ENTRY',
      featureSnapshotId: snapshotId,
      explanation,
    },
  };
}

function snapshotMarketState(store, tokenAddress, at) {
  const events = store.getEventsForToken(tokenAddress, { maxOccurredAt: at });
  const market = [...events].reverse().find(e => e.eventType === 'MARKET_SNAPSHOT');
  if (!market?.payload) return null;
  return {
    priceUsd: market.payload.priceUsd ?? market.payload.price ?? null,
    marketCapUsd: market.payload.marketCapUsd ?? market.payload.marketCap ?? null,
    liquidityUsd: market.payload.liquidityUsd ?? null,
  };
}

module.exports = {
  evaluateFirstCaller,
  evaluateIndependentConvergence,
  evaluateQualityConvergence,
  evaluateWalletConfirmation,
  evaluateStructureGateObservation,
  evaluateAmplifierArrival,
  evaluateSignalEntry,
  gatherIndependentConvergence,
  isValidCallerEvent,
  clusterIdForEvent,
};
