'use strict';

const { InMemorySignalStore } = require('./storage/InMemorySignalStore');
const { seedFrontRunnersFixtures } = require('./fixtures/seedFixtures');
const { getTokenTimeline } = require('./timeline/tokenTimeline');
const { buildFeatureSnapshot } = require('./features/featureEngine');
const { calculateConvergence } = require('./features/convergence');
const { scoreSignal } = require('./scoring/signalScoring');
const { decideSignalState } = require('./state/stateMachine');
const { replayToken } = require('./replay/replayEngine');

const DEFAULT_COHORT_PRICE_PATHS = {
  '2fRDA5f353VXLs2PeLJNqqHqTMhrjJunAXmWWpLkpump': [
    { occurredAt: '2026-09-29T18:00:00Z', price: 0.00012 },
    { occurredAt: '2026-09-29T18:10:00Z', price: 0.00016 },
    { occurredAt: '2026-09-29T18:30:00Z', price: 0.00024 },
    { occurredAt: '2026-09-29T19:00:00Z', price: 0.0003 },
    { occurredAt: '2026-09-29T20:00:00Z', price: 0.00007 },
  ],
  Gymbmn9wwMKe4NnmVceyyfpncp9arbwPfSdBsyY9pump: [
    { occurredAt: '2026-09-28T14:00:00Z', price: 0.00009 },
    { occurredAt: '2026-09-28T14:05:00Z', price: 0.0001 },
    { occurredAt: '2026-09-28T14:30:00Z', price: 0.00015 },
    { occurredAt: '2026-09-29T14:00:00Z', price: 0.0002 },
  ],
  '6iAj2oywQMiD9NeyTcW1S7UtG7e3jSK7Ud5ZJDqJpump': [
    { occurredAt: '2026-09-27T20:00:00Z', price: 0.00004 },
    { occurredAt: '2026-09-27T20:10:00Z', price: 0.000045 },
    { occurredAt: '2026-09-27T21:00:00Z', price: 0.00003 },
  ],
};
const { RESEARCH_CASES } = require('./fixtures/frontRunnersCases');
const { PILOT_COHORT_ID } = require('./fixtures/seedResearchCohort');
const { evaluateCohortLayers } = require('./research/cohortEvaluation');
const { RESEARCH_DEFINITION_VERSION } = require('./types');

class SignalService {
  /**
   * @param {import('./storage/InMemorySignalStore').InMemorySignalStore} [store]
   * @param {{ seedFixtures?: boolean }} [options]
   */
  constructor(store, options = {}) {
    this.store = store || new InMemorySignalStore();
    if (options.seedFixtures !== false) {
      seedFrontRunnersFixtures(this.store);
    }
  }

  listResearchCases() {
    return RESEARCH_CASES;
  }

  getToken(tokenAddress) {
    return this.store.tokens.get(tokenAddress) || null;
  }

  getTokenTimeline(tokenAddress, startTime, endTime) {
    return getTokenTimeline(this.store, tokenAddress, startTime, endTime);
  }

  evaluateAt(tokenAddress, evaluatedAt) {
    const snapshot = buildFeatureSnapshot(this.store, tokenAddress, evaluatedAt);
    this.store.insertSnapshot(snapshot);
    const scored = scoreSignal(snapshot.features);
    const openPosition = this.store.paperPositions.find(
      p => p.tokenAddress === tokenAddress && p.status !== 'CLOSED'
    );
    const decision = decideSignalState({
      previousState: null,
      score: scored.score,
      features: snapshot.features,
      position: openPosition,
    });
    return {
      snapshot,
      score: scored.score,
      components: scored.components,
      decision,
    };
  }

  calculateConvergence(tokenAddress, evaluatedAt, windowMinutes) {
    return calculateConvergence({
      store: this.store,
      tokenAddress,
      evaluatedAt,
      windowMinutes,
    });
  }

  replay(input) {
    return replayToken(this.store, input);
  }

  getLatestDecision(tokenAddress) {
    const rows = this.store.decisions
      .filter(d => d.tokenAddress === tokenAddress)
      .sort((a, b) => b.decidedAt - a.decidedAt);
    return rows[0] || null;
  }

  getOpenPaperPosition(tokenAddress) {
    return (
      this.store.paperPositions.find(
        p => p.tokenAddress === tokenAddress && p.status !== 'CLOSED'
      ) || null
    );
  }

  listResearchCohorts() {
    return this.store.listResearchCohorts();
  }

  getResearchCohort(cohortId) {
    const cohort = this.store.researchCohorts.get(cohortId);
    if (!cohort) return null;
    return {
      ...cohort,
      members: this.store.getCohortMembers(cohortId),
    };
  }

  getTokenResearchObservations(tokenAddress, definitionVersion = RESEARCH_DEFINITION_VERSION) {
    const observations = this.store.getResearchObservations(tokenAddress, definitionVersion);
    return observations.map(obs => ({
      ...obs,
      outcomes: this.store.researchObservationOutcomes.filter(o => o.observationId === obs.id),
    }));
  }

  evaluateResearchCohort(cohortId, executionDelaySeconds = 60, options = {}) {
    if (options.replayMembers !== false) {
      this.replayCohortMembers(
        cohortId,
        options.pricePathsByToken || DEFAULT_COHORT_PRICE_PATHS
      );
    }
    return evaluateCohortLayers(this.store, cohortId, executionDelaySeconds);
  }

  replayCohortMembers(cohortId, pricePathsByToken = {}) {
    const members = this.store.getCohortMembers(cohortId);
    for (const member of members) {
      replayToken(this.store, {
        tokenAddress: member.tokenAddress,
        pricePath: pricePathsByToken[member.tokenAddress] || [],
      });
    }
  }

  getDefaultPilotCohortId() {
    return PILOT_COHORT_ID;
  }
}

module.exports = {
  SignalService,
};
