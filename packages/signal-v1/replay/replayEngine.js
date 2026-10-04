'use strict';

const { createHash } = require('crypto');
const { buildFeatureSnapshot } = require('../features/featureEngine');
const { scoreSignal } = require('../scoring/signalScoring');
const { decideSignalState } = require('../state/stateMachine');
const { PaperPortfolio } = require('../paper/paperPortfolio');
const { labelMarketOutcome } = require('../outcomes/marketOutcomes');
const { STRATEGY_VERSION, FEATURE_VERSION } = require('../types');
const { createSignalAlert } = require('../alerts/signalAlert');
const { evaluateResearchObservationsAtStep } = require('../research/researchObservationEngine');

/**
 * @typedef {object} ReplayInput
 * @property {string} tokenAddress
 * @property {Date|string} [startTime]
 * @property {Date|string} [endTime]
 * @property {string} [featureVersion]
 * @property {string} [strategyVersion]
 * @property {{ occurredAt: Date|string, price: number }[]} [pricePath]
 */

/**
 * Chronological replay — snapshots use only events with occurredAt <= step time.
 *
 * @param {import('../storage/InMemorySignalStore').InMemorySignalStore} store
 * @param {ReplayInput} input
 */
function replayToken(store, input) {
  const tokenAddress = input.tokenAddress;
  const featureVersion = input.featureVersion || FEATURE_VERSION;
  const strategyVersion = input.strategyVersion || STRATEGY_VERSION;

  const events = store.getEventsForToken(tokenAddress, {
    startTime: input.startTime,
    endTime: input.endTime,
  });

  const stepTimes = uniqueSortedTimes(events);
  const paper = new PaperPortfolio(store);
  let previousState = null;
  const timeline = [];
  let openPosition = null;
  const researchTimeline = [];

  for (const stepAt of stepTimes) {
    const snapshot = buildFeatureSnapshot(store, tokenAddress, stepAt, { featureVersion });
    store.insertSnapshot(snapshot);
    const scored = scoreSignal(snapshot.features);
    const marketPrice = latestMarketPrice(store, tokenAddress, stepAt);

    if (openPosition) {
      paper.markUnrealized(openPosition, marketPrice || openPosition.entryPrice);
      openPosition.unrealizedGainPct =
        marketPrice && openPosition.entryPrice
          ? ((marketPrice / openPosition.entryPrice - 1) * 100)
          : null;
    }

    const decision = decideSignalState({
      previousState,
      score: scored.score,
      features: snapshot.features,
      position: openPosition,
    });

    const researchCreated = evaluateResearchObservationsAtStep(store, {
      tokenAddress,
      stepAt,
      snapshot,
      decisionState: decision.state,
      pricePath: input.pricePath,
    });
    if (researchCreated.length) {
      researchTimeline.push(
        ...researchCreated.map(o => ({
          observationType: o.observationType,
          occurredAt: o.occurredAt,
          id: o.id,
        }))
      );
    }

    const decisionRow = store.insertDecision({
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
      store.insertAlert(
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

    if (decision.state === 'ENTRY' && !openPosition && marketPrice) {
      openPosition = paper.openEntry({
        tokenAddress,
        signalAt: stepAt,
        marketPriceAtSignal: marketPrice,
        entrySnapshotId: snapshot.id,
        pricePath: input.pricePath,
      });
    } else if (decision.state === 'DE_RISK' && openPosition && openPosition.status === 'OPEN') {
      openPosition = paper.reducePosition({
        positionId: openPosition.id,
        sellPct: 50,
        signalAt: stepAt,
        marketPriceAtSignal: marketPrice || openPosition.entryPrice,
        pricePath: input.pricePath,
      });
    } else if (decision.state === 'EXIT' && openPosition && openPosition.remainingPct > 0) {
      openPosition = paper.reducePosition({
        positionId: openPosition.id,
        sellPct: openPosition.remainingPct,
        signalAt: stepAt,
        marketPriceAtSignal: marketPrice || openPosition.entryPrice,
        pricePath: input.pricePath,
      });
    }

    timeline.push({
      decidedAt: stepAt,
      state: decision.state,
      score: scored.score,
      decisionId: decisionRow.id,
      snapshotId: snapshot.id,
    });

    previousState = decision.state;
  }

  let outcome = null;
  if (input.pricePath && input.pricePath.length && timeline.length) {
    const entryStep = timeline.find(t => t.state === 'ENTRY');
    const entryPrice = entryStep
      ? latestMarketPrice(store, tokenAddress, entryStep.decidedAt)
      : input.pricePath[0].price;
    if (entryPrice) {
      outcome = labelMarketOutcome({
        entryPrice,
        observedAt: entryStep ? entryStep.decidedAt : stepTimes[0],
        pricePath: input.pricePath,
      });
      store.insertOutcome({
        tokenAddress,
        observedAt: entryStep ? entryStep.decidedAt : stepTimes[0],
        entryPrice,
        label: outcome.label,
        mfe: outcome.mfe,
        mae: outcome.mae,
        timeTo2xSeconds: outcome.timeTo2xSeconds,
        timeToMinus30Seconds: outcome.timeToMinus30Seconds,
        return15m: outcome.return15m,
        return1h: outcome.return1h,
        return6h: outcome.return6h,
        return24h: outcome.return24h,
        horizonHours: outcome.horizonHours,
      });
    }
  }

  const digest = hashReplay(store, tokenAddress, timeline, strategyVersion, featureVersion);
  const run = store.insertReplayRun({
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
      digest,
      researchObservations: researchTimeline.length,
    },
  });

  return {
    runId: run.id,
    digest,
    timeline,
    researchTimeline,
    researchObservations: store.getResearchObservations(tokenAddress),
    outcome,
    finalState: previousState,
  };
}

function uniqueSortedTimes(events) {
  const set = new Set(events.map(e => e.occurredAt.toISOString()));
  return [...set].sort().map(s => new Date(s));
}

function latestMarketPrice(store, tokenAddress, at) {
  const events = store.getEventsForToken(tokenAddress, { maxOccurredAt: at });
  const snaps = events.filter(e => e.eventType === 'MARKET_SNAPSHOT');
  if (!snaps.length) return null;
  const last = snaps[snaps.length - 1];
  const price = last.payload?.priceUsd ?? last.payload?.price;
  return price != null ? Number(price) : null;
}

function shouldAlert(previousState, nextState) {
  if (!previousState) return ['WATCH', 'ENTRY', 'EXIT'].includes(nextState);
  if (previousState === 'REJECT' && nextState === 'WATCH') return true;
  if (previousState === 'WATCH' && nextState === 'ENTRY') return true;
  if (previousState === 'ENTRY' && nextState === 'DE_RISK') return true;
  if (nextState === 'EXIT') return true;
  return false;
}

function hashReplay(store, tokenAddress, timeline, strategyVersion, featureVersion) {
  const stableTimeline = timeline.map(step => ({
    decidedAt: new Date(step.decidedAt).toISOString(),
    state: step.state,
    score: step.score,
  }));
  const research = store
    .getResearchObservations(tokenAddress)
    .map(o => ({
      observationType: o.observationType,
      occurredAt: new Date(o.occurredAt).toISOString(),
      definitionVersion: o.definitionVersion,
    }));

  const payload = JSON.stringify({
    tokenAddress,
    timeline: stableTimeline,
    research,
    strategyVersion,
    featureVersion,
  });
  return createHash('sha256').update(payload).digest('hex');
}

module.exports = {
  replayToken,
};
