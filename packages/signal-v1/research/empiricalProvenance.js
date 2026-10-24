'use strict';

const { CALL_EVENT_TYPES } = require('../features/convergence');
const { FIXTURE_TOKEN_SET } = require('./empiricalConstants');

const EVIDENCE_CLASS = Object.freeze({
  REAL_PROVIDER: 'REAL_PROVIDER',
  HISTORICAL_FIXTURE: 'HISTORICAL_FIXTURE',
  PROCEDURAL_GENERATED: 'PROCEDURAL_GENERATED',
  MANUAL_UNVERIFIED: 'MANUAL_UNVERIFIED',
  UNKNOWN: 'UNKNOWN',
});

const ALLOWED_EMPIRICAL_CALLER = new Set([
  EVIDENCE_CLASS.REAL_PROVIDER,
  EVIDENCE_CLASS.HISTORICAL_FIXTURE,
]);

const ALLOWED_EMPIRICAL_MARKET = new Set([
  EVIDENCE_CLASS.REAL_PROVIDER,
  EVIDENCE_CLASS.HISTORICAL_FIXTURE,
]);

/**
 * @param {object} event
 */
function classifyCallerEventProvenance(event) {
  const p = event.provenance || {};
  if (p.evidenceClass) return p.evidenceClass;
  if (p.backfill === 'callerBackfill') return EVIDENCE_CLASS.PROCEDURAL_GENERATED;
  if (p.procedural === true || p.generated === true) return EVIDENCE_CLASS.PROCEDURAL_GENERATED;
  if (FIXTURE_TOKEN_SET.has(event.tokenAddress) && !p.backfill && !p.evidenceClass) {
    return EVIDENCE_CLASS.HISTORICAL_FIXTURE;
  }
  if (p.manual === true && !p.verified) return EVIDENCE_CLASS.MANUAL_UNVERIFIED;
  if (p.provider && p.externalId) return EVIDENCE_CLASS.REAL_PROVIDER;
  if (event.sourceType === 'telegram' && event.sourceId?.startsWith('src-front-runners')) {
    return EVIDENCE_CLASS.REAL_PROVIDER;
  }
  return EVIDENCE_CLASS.UNKNOWN;
}

/**
 * @param {object} observation
 */
function classifyMarketObservationProvenance(observation) {
  const p = observation.provenance || observation.metadata?.provenance || {};
  if (p.evidenceClass) return p.evidenceClass;
  if (p.procedural === true || p.fixtureGenerated === true) {
    return EVIDENCE_CLASS.PROCEDURAL_GENERATED;
  }
  const provider = observation.provider || p.provider || '';
  if (provider === 'geckoterminal' || provider === 'GeckoTerminalMarketDataProvider') {
    return EVIDENCE_CLASS.REAL_PROVIDER;
  }
  if (provider.includes('fixture') || provider === 'fixture-acquisition') {
    return EVIDENCE_CLASS.PROCEDURAL_GENERATED;
  }
  if (FIXTURE_TOKEN_SET.has(observation.tokenAddress) && !provider) {
    return EVIDENCE_CLASS.HISTORICAL_FIXTURE;
  }
  return EVIDENCE_CLASS.UNKNOWN;
}

function isAllowedEmpiricalCallerClass(evidenceClass) {
  return ALLOWED_EMPIRICAL_CALLER.has(evidenceClass);
}

function isAllowedEmpiricalMarketClass(evidenceClass) {
  return ALLOWED_EMPIRICAL_MARKET.has(evidenceClass);
}

/**
 * @param {import('../storage/InMemorySignalStore').InMemorySignalStore} store
 * @param {string[]} tokenAddresses
 */
function buildContaminationReport(store, tokenAddresses) {
  const counts = {
    realProviderEventCount: 0,
    verifiedHistoricalFixtureCount: 0,
    proceduralEventCount: 0,
    manualUnverifiedEventCount: 0,
    unknownProvenanceCount: 0,
    proceduralMarketObservationCount: 0,
    unknownMarketObservationCount: 0,
  };

  for (const tokenAddress of tokenAddresses) {
    const events = store.getEventsForToken(tokenAddress).filter(e => CALL_EVENT_TYPES.has(e.eventType));
    for (const event of events) {
      const cls = classifyCallerEventProvenance(event);
      if (cls === EVIDENCE_CLASS.REAL_PROVIDER) counts.realProviderEventCount += 1;
      else if (cls === EVIDENCE_CLASS.HISTORICAL_FIXTURE) counts.verifiedHistoricalFixtureCount += 1;
      else if (cls === EVIDENCE_CLASS.PROCEDURAL_GENERATED) counts.proceduralEventCount += 1;
      else if (cls === EVIDENCE_CLASS.MANUAL_UNVERIFIED) counts.manualUnverifiedEventCount += 1;
      else counts.unknownProvenanceCount += 1;
    }

    const market =
      store.getMarketObservationsForToken?.(tokenAddress) ||
      store.marketObservations.filter(o => o.tokenAddress === tokenAddress);
    const marketList = market && typeof market.then === 'function' ? [] : market;
    for (const obs of marketList) {
      const cls = classifyMarketObservationProvenance(obs);
      if (cls === EVIDENCE_CLASS.PROCEDURAL_GENERATED) counts.proceduralMarketObservationCount += 1;
      else if (cls === EVIDENCE_CLASS.UNKNOWN || cls === EVIDENCE_CLASS.MANUAL_UNVERIFIED) {
        counts.unknownMarketObservationCount += 1;
      }
    }
  }

  return counts;
}

module.exports = {
  EVIDENCE_CLASS,
  ALLOWED_EMPIRICAL_CALLER,
  ALLOWED_EMPIRICAL_MARKET,
  classifyCallerEventProvenance,
  classifyMarketObservationProvenance,
  isAllowedEmpiricalCallerClass,
  isAllowedEmpiricalMarketClass,
  buildContaminationReport,
};
