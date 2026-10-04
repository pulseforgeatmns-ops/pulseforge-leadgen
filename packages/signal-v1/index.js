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
const { evaluateCohortLayers } = require('./research/cohortEvaluation');
const { evaluateResearchObservationsAtStep } = require('./research/researchObservationEngine');

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
  ...types,
  ...historicalCoverage,
};
