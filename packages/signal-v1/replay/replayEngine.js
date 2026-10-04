'use strict';

const { createHash } = require('crypto');
const { buildFeatureSnapshot } = require('../features/featureEngine');
const { scoreSignal } = require('../scoring/signalScoring');
const { decideSignalState } = require('../state/stateMachine');
const { PaperPortfolio } = require('../paper/paperPortfolio');
const { STRATEGY_VERSION, FEATURE_VERSION } = require('../types');
const { createSignalAlert } = require('../alerts/signalAlert');
const { callStore } = require('../storage/storeUtils');
const {
  latestMarketContext,
  buildPricePathFromObservations,
} = require('../market/marketContext');
const {
  evaluateEntryOutcomesForDelays,
  DEFAULT_EXECUTION_DELAYS_SECONDS,
} = require('../outcomes/evaluateEntryOutcomes');
const { filterObservationsAtOrBefore } = require('../temporal/temporalFirewall');
const {
  validateHistoricalCoverage,
  resolveReplayStatus,
  buildHistoricalUnavailablePayload,
  HISTORICAL_DATA_UNAVAILABLE,
} = require('../market/historicalCoverage');

/**
 * Chronological replay — snapshots use only events/observations with occurredAt <= step time.
 *
 * @param {object} store
 * @param {object} input
 */
async function replayToken(store, input) {
  const tokenAddress = input.tokenAddress;
  const featureVersion = input.featureVersion || FEATURE_VERSION;
  const strategyVersion = input.strategyVersion || STRATEGY_VERSION;
  const startTime = input.startTime ? new Date(input.startTime) : null;
  const endTime = input.endTime ? new Date(input.endTime) : null;

  const allObservationsRaw = await callStore(store, 'getMarketObservationsForToken', tokenAddress, {
    startTime: input.startTime,
    endTime: input.endTime,
  });

  const coverage =
    startTime && endTime
      ? validateHistoricalCoverage({
          requestedStart: startTime,
          requestedEnd: endTime,
          observations: allObservationsRaw,
          decisionAnchor: input.decisionAnchor,
          intervalSeconds: input.resolutionSeconds || 60,
        })
      : null;

  if (
    coverage &&
    (coverage.status === 'UNAVAILABLE' || coverage.status === 'INVALID_RANGE')
  ) {
    return buildSkippedReplayResult({
      tokenAddress,
      input,
      coverage,
      featureVersion,
      strategyVersion,
      reason: HISTORICAL_DATA_UNAVAILABLE,
    });
  }

  if (coverage && !coverage.hasObservationAtOrAfterDecision) {
    return buildSkippedReplayResult({
      tokenAddress,
      input,
      coverage,
      featureVersion,
      strategyVersion,
      replayStatus: 'INSUFFICIENT_MARKET_DATA',
    });
  }

  if (store.clearReplayArtifacts && input.replaceExisting !== false) {
    await callStore(store, 'clearReplayArtifacts', tokenAddress);
  }

  const events = await callStore(store, 'getEventsForToken', tokenAddress, {
    startTime: input.startTime,
    endTime: input.endTime,
  });

  const allObservations = allObservationsRaw;

  const pricePath =
    input.pricePath ||
    (allObservations.length ? buildPricePathFromObservations(allObservations) : []);

  const stepTimes = uniqueSortedTimes(events, allObservations);
  const paper = new PaperPortfolio(store, input.paperConfig || {});
  let previousState = null;
  const timeline = [];
  let openPosition = null;

  for (const stepAt of stepTimes) {
    const snapshot = await buildFeatureSnapshot(store, tokenAddress, stepAt, {
      featureVersion,
      eventsFilter: { maxOccurredAt: stepAt },
    });
    await callStore(store, 'insertSnapshot', snapshot);
    const scored = scoreSignal(snapshot.features);
    const market = latestMarketContext(store, tokenAddress, stepAt, {
      observations: allObservations,
    });
    const marketPrice = market.priceUsd;

    if (openPosition) {
      paper.markUnrealized(openPosition, marketPrice || openPosition.entryPrice);
      openPosition.unrealizedGainPct =
        marketPrice && openPosition.entryPrice
          ? (marketPrice / openPosition.entryPrice - 1) * 100
          : null;
    }

    const decision = decideSignalState({
      previousState,
      score: scored.score,
      features: snapshot.features,
      position: openPosition,
    });

    const decisionRow = await callStore(store, 'insertDecision', {
      tokenAddress,
      decidedAt: stepAt,
      state: decision.state,
      previousState,
      score: scored.score,
      featureSnapshotId: snapshot.id,
      strategyVersion,
      explanation: {
        state: decision.state,
        score: scored.score,
        reasons: decision.reasons,
        risks: decision.risks,
        components: scored.components,
      },
    });

    if (shouldAlert(previousState, decision.state)) {
      await callStore(
        store,
        'insertAlert',
        createSignalAlert({
          tokenAddress,
          state: decision.state,
          previousState,
          score: scored.score,
          reasons: decision.reasons,
          risks: decision.risks,
          createdAt: stepAt,
        })
      );
    }

    let paperAction = null;
    if (decision.state === 'ENTRY' && !openPosition && pricePath.length) {
      openPosition = paper.openEntry({
        tokenAddress,
        signalAt: stepAt,
        marketPriceAtSignal: marketPrice || pricePath[0].priceUsd,
        entrySnapshotId: snapshot.id,
        pricePath,
      });
      paperAction = { type: 'OPEN', positionId: openPosition.id };
    } else if (decision.state === 'DE_RISK' && openPosition && openPosition.status === 'OPEN') {
      openPosition = paper.reducePosition({
        positionId: openPosition.id,
        sellPct: 50,
        signalAt: stepAt,
        marketPriceAtSignal: marketPrice || openPosition.entryPrice,
        pricePath,
      });
      paperAction = { type: 'DE_RISK', positionId: openPosition.id };
    } else if (decision.state === 'EXIT' && openPosition && openPosition.remainingPct > 0) {
      openPosition = paper.reducePosition({
        positionId: openPosition.id,
        sellPct: openPosition.remainingPct,
        signalAt: stepAt,
        marketPriceAtSignal: marketPrice || openPosition.entryPrice,
        pricePath,
      });
      paperAction = { type: 'EXIT', positionId: openPosition.id };
    }

    timeline.push({
      decidedAt: stepAt,
      state: decision.state,
      score: scored.score,
      decisionId: decisionRow.id,
      snapshotId: snapshot.id,
      priceUsd: marketPrice,
      marketCapUsd: market.marketCapUsd,
      liquidityUsd: market.liquidityUsd,
      reasons: decision.reasons,
      risks: decision.risks,
      paperAction,
    });

    previousState = decision.state;
  }

  const entryStep = timeline.find(t => t.state === 'ENTRY');
  let executionDelayOutcomes = [];
  if (entryStep && allObservations.length) {
    executionDelayOutcomes = evaluateEntryOutcomesForDelays({
      decisionAt: entryStep.decidedAt,
      observations: allObservations,
      executionDelaysSeconds:
        input.executionDelaysSeconds || DEFAULT_EXECUTION_DELAYS_SECONDS,
      config: input.outcomeConfig,
    });

    for (const row of executionDelayOutcomes) {
      if (!row.entry || !row.outcome) continue;
      await callStore(store, 'insertOutcome', {
        tokenAddress,
        observedAt: row.entry.effectiveExecutionAt,
        entryPrice: row.entry.effectivePrice,
        label: row.outcome.label,
        mfe: row.outcome.mfe,
        mae: row.outcome.mae,
        timeTo2xSeconds: row.outcome.timeTo2xSeconds,
        timeToMinus30Seconds: row.outcome.timeToMinus30Seconds,
        return15m: row.outcome.return15m,
        return1h: row.outcome.return1h,
        return6h: row.outcome.return6h,
        return24h: row.outcome.return24h,
        horizonHours: row.outcome.horizonHours,
        decisionAt: entryStep.decidedAt,
        executionDelaySeconds: row.executionDelaySeconds,
        effectiveExecutionAt: row.entry.effectiveExecutionAt,
        effectivePrice: row.entry.effectivePrice,
        dataResolutionSeconds: row.entry.dataResolutionSeconds,
        highestPrice: row.outcome.highestPrice,
        lowestPrice: row.outcome.lowestPrice,
        outcomeTimestamp: row.outcome.outcomeTimestamp,
        metadata: { replay: true },
      });
    }
  }

  let outcome = executionDelayOutcomes.find(r => r.executionDelaySeconds === 30)?.outcome || null;

  if (!outcome && pricePath.length && timeline.length) {
    const legacy = evaluateEntryOutcomesForDelays({
      decisionAt: entryStep ? entryStep.decidedAt : stepTimes[0],
      observations: pricePath.map(p => ({
        occurredAt: p.occurredAt,
        priceUsd: p.priceUsd ?? p.price,
        intervalSeconds: p.intervalSeconds || 60,
      })),
      executionDelaysSeconds: [paper.config.executionDelaySeconds],
      config: input.outcomeConfig,
    });
    outcome = legacy[0]?.outcome || null;
  }

  const digest = hashReplay(tokenAddress, timeline, strategyVersion, featureVersion);
  const hadResolvableOutcomes = executionDelayOutcomes.some(r => r.entry && r.outcome);
  const replayStatus = resolveReplayStatus({
    coverage,
    hadEntry: Boolean(entryStep),
    hadResolvableOutcomes,
    replayRan: true,
  });

  const run = await callStore(store, 'insertReplayRun', {
    tokenAddress,
    startTime: input.startTime || null,
    endTime: input.endTime || null,
    featureVersion,
    strategyVersion,
    completedAt: new Date(),
    summary: {
      steps: timeline.length,
      finalState: previousState,
      outcome,
      executionDelayOutcomes,
      digest,
      observationCount: allObservations.length,
      replayStatus,
      coverage,
    },
  });

  return {
    runId: run.id,
    digest,
    timeline,
    outcome,
    executionDelayOutcomes,
    finalState: previousState,
    replayStatus,
    historicalDataStatus: coverage?.status || null,
    coverage,
    marketHistory: {
      observationCount: allObservations.length,
      start: allObservations[0]?.occurredAt || null,
      end: allObservations[allObservations.length - 1]?.occurredAt || null,
      requestedStart: coverage?.requestedStart || input.startTime || null,
      requestedEnd: coverage?.requestedEnd || input.endTime || null,
    },
  };
}

function buildSkippedReplayResult({
  tokenAddress,
  input,
  coverage,
  featureVersion,
  strategyVersion,
  reason,
  replayStatus,
}) {
  const status =
    replayStatus ||
    (reason === HISTORICAL_DATA_UNAVAILABLE
      ? 'HISTORICAL_DATA_UNAVAILABLE'
      : 'INSUFFICIENT_MARKET_DATA');
  const payload =
    status === 'HISTORICAL_DATA_UNAVAILABLE'
      ? buildHistoricalUnavailablePayload({
          tokenAddress,
          provider: input.provider || 'geckoterminal',
          coverage,
        })
      : null;

  return {
    runId: null,
    digest: null,
    timeline: [],
    outcome: null,
    executionDelayOutcomes: [],
    finalState: null,
    replayStatus: status,
    historicalDataStatus: coverage.status,
    coverage,
    skipped: true,
    error: reason || null,
    unavailablePayload: payload,
    marketHistory: {
      observationCount: coverage.observationCountInWindow || 0,
      start: coverage.providerEarliestObservation,
      end: coverage.providerLatestObservation,
      requestedStart: coverage.requestedStart,
      requestedEnd: coverage.requestedEnd,
    },
    featureVersion,
    strategyVersion,
  };
}

function uniqueSortedTimes(events, observations) {
  const set = new Set([
    ...events.map(e => e.occurredAt.toISOString()),
    ...observations.map(o => o.occurredAt.toISOString()),
  ]);
  return [...set].sort().map(s => new Date(s));
}

function shouldAlert(previousState, nextState) {
  if (!previousState) return ['WATCH', 'ENTRY', 'EXIT'].includes(nextState);
  if (previousState === 'REJECT' && nextState === 'WATCH') return true;
  if (previousState === 'WATCH' && nextState === 'ENTRY') return true;
  if (previousState === 'ENTRY' && nextState === 'DE_RISK') return true;
  if (nextState === 'EXIT') return true;
  return false;
}

function hashReplay(tokenAddress, timeline, strategyVersion, featureVersion) {
  const stableTimeline = timeline.map(step => ({
    decidedAt: new Date(step.decidedAt).toISOString(),
    state: step.state,
    score: step.score,
  }));
  const payload = JSON.stringify({
    tokenAddress,
    timeline: stableTimeline,
    strategyVersion,
    featureVersion,
  });
  return createHash('sha256').update(payload).digest('hex');
}

module.exports = {
  replayToken,
  filterObservationsAtOrBefore,
};
