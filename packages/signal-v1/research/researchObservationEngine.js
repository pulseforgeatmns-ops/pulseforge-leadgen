'use strict';

const { createHash } = require('crypto');
const { RESEARCH_DEFINITION_VERSION, RESEARCH_OBSERVATION_TYPES } = require('../types');
const { evaluateObservationOutcomes } = require('./observationOutcomes');
const {
  evaluateFirstCaller,
  evaluateIndependentConvergence,
  evaluateQualityConvergence,
  evaluateWalletConfirmation,
  evaluateStructureGateObservation,
  evaluateAmplifierArrival,
  evaluateSignalEntry,
} = require('./observationTriggers');

/**
 * Evaluate research observations at a replay step — does not mutate strategy state.
 *
 * @param {import('../storage/InMemorySignalStore').InMemorySignalStore} store
 * @param {object} ctx
 */
function evaluateResearchObservationsAtStep(store, ctx) {
  const {
    tokenAddress,
    stepAt,
    snapshot,
    decisionState,
    pricePath,
    definitionVersion = RESEARCH_DEFINITION_VERSION,
    researchConfig,
  } = ctx;

  const existing = store.getResearchObservations(tokenAddress, definitionVersion);
  const existingTypes = new Set(existing.map(o => o.observationType));

  const firstCallerObs = existing.find(o => o.observationType === 'FIRST_CALLER');
  const researchState = {
    firstCallerAt: firstCallerObs?.occurredAt || null,
    hasPriorObservation: existing.length > 0,
    priorObservationTypes: existing.map(o => o.observationType),
  };

  const created = [];

  const maybeCreate = (type, payload) => {
    if (!payload || existingTypes.has(type)) return;
    if (payload.unavailable || payload.negative) return;

    const occurredIso = new Date(payload.occurredAt).toISOString();
    const obsId = deterministicId(
      `${tokenAddress}|${type}|${definitionVersion}|${occurredIso}`
    );
    const row = store.insertResearchObservation({
      id: obsId,
      tokenAddress,
      observationType: type,
      occurredAt: payload.occurredAt,
      featureSnapshotId: snapshot.id,
      triggerEventIds: payload.triggerEventIds || [],
      evidenceEventIds: payload.evidenceEventIds || [],
      definitionVersion,
      metadata: payload.metadata || {},
    });
    existingTypes.add(type);
    created.push(row);

    if (pricePath && pricePath.length) {
      const outcomes = evaluateObservationOutcomes({
        observationAt: row.occurredAt,
        pricePath,
      });
      for (const outcome of outcomes) {
        store.insertResearchObservationOutcome({
          id: deterministicId(`${obsId}|${outcome.executionDelaySeconds}`),
          observationId: row.id,
          ...outcome,
        });
      }
    }
  };

  maybeCreate('FIRST_CALLER', evaluateFirstCaller(store, tokenAddress, stepAt));
  maybeCreate(
    'INDEPENDENT_CONVERGENCE',
    evaluateIndependentConvergence(store, tokenAddress, stepAt, researchConfig)
  );
  maybeCreate(
    'QUALITY_CONVERGENCE',
    evaluateQualityConvergence(store, tokenAddress, stepAt, researchConfig)
  );

  const wallet = evaluateWalletConfirmation(
    store,
    tokenAddress,
    stepAt,
    {
      firstCallerAt: researchState.firstCallerAt ||
        evaluateFirstCaller(store, tokenAddress, stepAt)?.occurredAt,
    },
    researchConfig
  );
  if (wallet && !wallet.unavailable && !wallet.negative) {
    maybeCreate('WALLET_CONFIRMATION', wallet);
  }

  maybeCreate(
    'STRUCTURE_GATE',
    evaluateStructureGateObservation(snapshot.features, stepAt)
  );

  maybeCreate(
    'AMPLIFIER_ARRIVAL',
    evaluateAmplifierArrival(store, tokenAddress, stepAt, {
      ...researchState,
      hasPriorObservation: existing.length > 0 || created.length > 0,
      priorObservationTypes: [...researchState.priorObservationTypes, ...created.map(c => c.observationType)],
    })
  );

  maybeCreate(
    'SIGNAL_ENTRY',
    evaluateSignalEntry(decisionState, stepAt, snapshot.id, {
      state: decisionState,
    })
  );

  return created;
}

function listObservationTypesInOrder() {
  return [...RESEARCH_OBSERVATION_TYPES];
}

function deterministicId(seed) {
  return createHash('sha256').update(seed).digest('hex').slice(0, 32);
}

module.exports = {
  evaluateResearchObservationsAtStep,
  listObservationTypesInOrder,
};
