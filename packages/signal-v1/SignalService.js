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
const { ingestHistoricalMarketData } = require('./ingestion/ingestHistoricalMarketData');
const { GeckoTerminalMarketDataProvider } = require('./providers/GeckoTerminalMarketDataProvider');
const { callStore } = require('./storage/storeUtils');
const { resolveResearchWindow } = require('./fixtures/researchWindows');
const { validateHistoricalCoverage } = require('./market/historicalCoverage');

class SignalService {
  /**
   * @param {import('./storage/InMemorySignalStore').InMemorySignalStore|import('./storage/PostgresSignalStore').PostgresSignalStore} [store]
   * @param {{ seedFixtures?: boolean, marketProvider?: object }} [options]
   */
  constructor(store, options = {}) {
    this.store = store || new InMemorySignalStore();
    this.marketProvider = options.marketProvider || new GeckoTerminalMarketDataProvider();
    if (options.seedFixtures !== false && this.store.tokens instanceof Map) {
      seedFrontRunnersFixtures(this.store);
    }
  }

  listResearchCases() {
    return RESEARCH_CASES.map(c => {
      if (!c.tokenAddress) return { ...c };
      const window = resolveResearchWindow(c.tokenAddress);
      return {
        ...c,
        researchWindow: {
          requestedStart: window.startTime,
          requestedEnd: window.endTime,
          anchor: window.anchor,
        },
      };
    });
  }

  async getToken(tokenAddress) {
    if (this.store.tokens instanceof Map) {
      return this.store.tokens.get(tokenAddress) || null;
    }
    return callStore(this.store, 'getToken', tokenAddress);
  }

  getTokenTimeline(tokenAddress, startTime, endTime) {
    return getTokenTimeline(this.store, tokenAddress, startTime, endTime);
  }

  async evaluateAt(tokenAddress, evaluatedAt) {
    const snapshot = await buildFeatureSnapshot(this.store, tokenAddress, evaluatedAt);
    await callStore(this.store, 'insertSnapshot', snapshot);
    const scored = scoreSignal(snapshot.features);
    const openPosition = await this.getOpenPaperPosition(tokenAddress);
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

  async replay(input) {
    const tokenAddress = input.tokenAddress;
    let startTime = input.startTime;
    let endTime = input.endTime;
    let decisionAnchor = input.decisionAnchor;
    if (!startTime || !endTime) {
      const window = resolveResearchWindow(tokenAddress);
      startTime = startTime || window.startTime;
      endTime = endTime || window.endTime;
      decisionAnchor = decisionAnchor || window.anchor;
    }
    return replayToken(this.store, {
      ...input,
      startTime,
      endTime,
      decisionAnchor,
    });
  }

  async ingestHistory(args) {
    const provider = args.provider || this.marketProvider;
    return ingestHistoricalMarketData(this.store, provider, args);
  }

  async ingestResearchToken(tokenAddress) {
    const window = resolveResearchWindow(tokenAddress);
    return this.ingestHistory({
      tokenAddress,
      startTime: window.startTime,
      endTime: window.endTime,
      resolutionSeconds: window.resolutionSeconds,
      decisionAnchor: window.anchor,
    });
  }

  async getLatestDecision(tokenAddress) {
    if (this.store.decisions) {
      const rows = this.store.decisions
        .filter(d => d.tokenAddress === tokenAddress)
        .sort((a, b) => b.decidedAt - a.decidedAt);
      return rows[0] || null;
    }
    return callStore(this.store, 'getLatestDecision', tokenAddress);
  }

  async getOpenPaperPosition(tokenAddress) {
    if (this.store.paperPositions) {
      return (
        this.store.paperPositions.find(
          p => p.tokenAddress === tokenAddress && p.status !== 'CLOSED'
        ) || null
      );
    }
    return callStore(this.store, 'getOpenPaperPosition', tokenAddress);
  }

  async getMarketHistory(tokenAddress, startTime, endTime) {
    let reqStart = startTime;
    let reqEnd = endTime;
    let anchor = null;
    if (!reqStart || !reqEnd) {
      try {
        const window = resolveResearchWindow(tokenAddress);
        reqStart = reqStart || window.startTime;
        reqEnd = reqEnd || window.endTime;
        anchor = window.anchor;
      } catch {
        /* token may lack research anchor */
      }
    }
    const observations = await callStore(this.store, 'getMarketObservationsForToken', tokenAddress, {
      startTime: reqStart,
      endTime: reqEnd,
    });
    const stats = this.store.getLatestMarketIngestionStats
      ? await callStore(this.store, 'getLatestMarketIngestionStats', tokenAddress)
      : null;
    const coverage =
      reqStart && reqEnd
        ? validateHistoricalCoverage({
            requestedStart: reqStart,
            requestedEnd: reqEnd,
            observations,
            decisionAnchor: anchor,
          })
        : stats?.metadata?.coverage || null;
    return {
      observations,
      ingestion: stats,
      coverage,
      historicalDataStatus: coverage?.status || stats?.metadata?.historicalDataStatus || null,
    };
  }

  async getOutcomes(tokenAddress) {
    if (this.store.getOutcomesForToken) {
      return callStore(this.store, 'getOutcomesForToken', tokenAddress);
    }
    return this.store.outcomes.filter(o => o.tokenAddress === tokenAddress);
  }

  async getReplayDetail(tokenAddress) {
    const [decisions, snapshots, outcomes, market] = await Promise.all([
      this.store.getDecisionsForToken
        ? callStore(this.store, 'getDecisionsForToken', tokenAddress)
        : this.store.decisions.filter(d => d.tokenAddress === tokenAddress),
      this.store.getSnapshotsForToken
        ? callStore(this.store, 'getSnapshotsForToken', tokenAddress)
        : this.store.snapshots.filter(s => s.tokenAddress === tokenAddress),
      this.getOutcomes(tokenAddress),
      this.getMarketHistory(tokenAddress),
    ]);
    return { decisions, snapshots, outcomes, market };
  }
}

module.exports = {
  SignalService,
};
