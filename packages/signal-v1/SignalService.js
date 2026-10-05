'use strict';

const { InMemorySignalStore } = require('./storage/InMemorySignalStore');
const { seedFrontRunnersFixtures } = require('./fixtures/seedFixtures');
const { getTokenTimeline } = require('./timeline/tokenTimeline');
const { buildFeatureSnapshot } = require('./features/featureEngine');
const { calculateConvergence } = require('./features/convergence');
const { scoreSignal } = require('./scoring/signalScoring');
const { decideSignalState } = require('./state/stateMachine');
const { replayToken } = require('./replay/replayEngine');
const { ingestHistoricalMarketData } = require('./ingestion/ingestHistoricalMarketData');
const { GeckoTerminalMarketDataProvider } = require('./providers/GeckoTerminalMarketDataProvider');
const { callStore } = require('./storage/storeUtils');
const { resolveResearchWindow } = require('./fixtures/researchWindows');
const { validateHistoricalCoverage } = require('./market/historicalCoverage');

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
const { buildCohortEvaluationArtifact } = require('./research/cohortEvaluationExport');
const { hydrateCohortResearchCache } = require('./research/researchStoreCache');
const {
  buildValidationCohort001,
  buildValidationCohort002,
} = require('./acquisition/researchAcquisitionPipeline');
const { buildValidationCohort003 } = require('./acquisition/empirical/buildValidationCohort003');
const { isAsyncStore } = require('./storage/storeUtils');
const { RESEARCH_DEFINITION_VERSION } = require('./types');
const {
  VALIDATION_COHORT_001_ID,
  VALIDATION_COHORT_002_ID,
  VALIDATION_COHORT_003_ID,
} = require('./acquisition/candidateTypes');
const { resolveCohortDataClass, isEmpiricalDataClass } = require('./research/dataClass');

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

  listResearchCohorts() {
    return this.store.listResearchCohorts();
  }

  async getResearchCohort(cohortId) {
    let cohort = this.store.researchCohorts?.get?.(cohortId);
    if (!cohort && this.store.listResearchCohorts) {
      const list = await callStore(this.store, 'listResearchCohorts');
      cohort = list.find(c => c.id === cohortId);
    }
    if (!cohort) return null;
    const members = await callStore(this.store, 'getCohortMembers', cohortId);
    return {
      ...cohort,
      members,
    };
  }

  getTokenResearchObservations(tokenAddress, definitionVersion = RESEARCH_DEFINITION_VERSION) {
    const observations = this.store.getResearchObservations(tokenAddress, definitionVersion);
    return observations.map(obs => ({
      ...obs,
      outcomes: this.store.researchObservationOutcomes.filter(o => o.observationId === obs.id),
    }));
  }

  async evaluateResearchCohort(cohortId, executionDelaySeconds = 60, options = {}) {
    const cohort = this.store.researchCohorts?.get?.(cohortId);
    const dataClass = resolveCohortDataClass(cohortId, cohort);
    if (options.replayMembers !== false) {
      if (isEmpiricalDataClass(dataClass)) {
        throw new Error(
          'EMPIRICAL cohort evaluation cannot replay members with procedural price paths'
        );
      }
      await this.replayCohortMembers(
        cohortId,
        options.pricePathsByToken || DEFAULT_COHORT_PRICE_PATHS
      );
    }
    if (isAsyncStore(this.store)) {
      await hydrateCohortResearchCache(this.store, cohortId);
    }
    return evaluateCohortLayers(this.store, cohortId, executionDelaySeconds, options);
  }

  async exportResearchCohortEvaluation(cohortId, options = {}) {
    if (isAsyncStore(this.store)) {
      await hydrateCohortResearchCache(this.store, cohortId);
    }
    return buildCohortEvaluationArtifact(this.store, cohortId, options);
  }

  async buildValidationCohort001(options = {}) {
    return buildValidationCohort001(this.store, options);
  }

  async buildValidationCohort002(options = {}) {
    return buildValidationCohort002(this.store, options);
  }

  async buildValidationCohort003(options = {}) {
    return buildValidationCohort003(this.store, options);
  }

  getValidationCohort001Id() {
    return VALIDATION_COHORT_001_ID;
  }

  getValidationCohort002Id() {
    return VALIDATION_COHORT_002_ID;
  }

  getValidationCohort003Id() {
    return VALIDATION_COHORT_003_ID;
  }

  async replayCohortMembers(cohortId, pricePathsByToken = {}) {
    const members = await callStore(this.store, 'getCohortMembers', cohortId);
    for (const member of members) {
      await replayToken(this.store, {
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
