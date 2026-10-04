'use strict';

const { InMemorySignalStore } = require('./storage/InMemorySignalStore');
const { seedFrontRunnersFixtures } = require('./fixtures/seedFixtures');
const { getTokenTimeline } = require('./timeline/tokenTimeline');
const { buildFeatureSnapshot } = require('./features/featureEngine');
const { calculateConvergence } = require('./features/convergence');
const { scoreSignal } = require('./scoring/signalScoring');
const { decideSignalState } = require('./state/stateMachine');
const { replayToken } = require('./replay/replayEngine');
const { RESEARCH_CASES } = require('./fixtures/frontRunnersCases');

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
}

module.exports = {
  SignalService,
};
