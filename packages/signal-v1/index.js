'use strict';

const { SignalService } = require('./SignalService');
const { InMemorySignalStore } = require('./storage/InMemorySignalStore');
const { PostgresSignalStore } = require('./storage/PostgresSignalStore');
const { createSignalStore } = require('./storage/createSignalStore');
const { ensureSignalSchema } = require('./storage/ensureSignalSchema');
const { ingestHistoricalMarketData } = require('./ingestion/ingestHistoricalMarketData');
const { GeckoTerminalMarketDataProvider } = require('./providers/GeckoTerminalMarketDataProvider');
const { replayToken } = require('./replay/replayEngine');
const { labelMarketOutcome } = require('./outcomes/marketOutcomes');
const { buildFeatureSnapshot } = require('./features/featureEngine');
const { calculateConvergence } = require('./features/convergence');
const { scoreSignal } = require('./scoring/signalScoring');
const { decideSignalState } = require('./state/stateMachine');
const types = require('./types');
const historicalCoverage = require('./market/historicalCoverage');
const { evaluateCohortLayers } = require('./research/cohortEvaluation');
const { evaluateResearchObservationsAtStep } = require('./research/researchObservationEngine');
const { ShadowModeService } = require('./prospective/ShadowModeService');
const {
  runShadowSchedulerTick,
  startShadowScheduler,
  createShadowModeServiceFromStore,
} = require('./prospective/shadowScheduler');
const { assertEmpiricalCohort } = require('./prospective/empiricalGuard');
const { knowledgeAt } = require('./prospective/knowledgeClock');
const prospectiveConstants = require('./prospective/constants');

module.exports = {
  SignalService,
  InMemorySignalStore,
  PostgresSignalStore,
  createSignalStore,
  ingestHistoricalMarketData,
  GeckoTerminalMarketDataProvider,
  ensureSignalSchema,
  replayToken,
  labelMarketOutcome,
  buildFeatureSnapshot,
  calculateConvergence,
  scoreSignal,
  decideSignalState,
  evaluateCohortLayers,
  evaluateResearchObservationsAtStep,
  ShadowModeService,
  runShadowSchedulerTick,
  startShadowScheduler,
  createShadowModeServiceFromStore,
  assertEmpiricalCohort,
  knowledgeAt,
  ...prospectiveConstants,
  ...types,
  ...historicalCoverage,
};
