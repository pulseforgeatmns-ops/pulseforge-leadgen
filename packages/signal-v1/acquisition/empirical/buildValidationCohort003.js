'use strict';

const { callStore } = require('../../storage/storeUtils');
const { RESEARCH_DEFINITION_VERSION } = require('../../types');
const { RESEARCH_DATA_CLASS } = require('../../research/dataClass');
const { assertProviderAllowedForDataClass } = require('../../research/empiricalGuard');
const { dedupeCandidates, evaluateCandidateEligibility } = require('../eligibility');
const { selectChronologicalCohort } = require('../deterministicSelection');
const { discoverAndPersistCandidates } = require('../researchAcquisitionPipeline');
const { createHistoricalCallerCatalogProvider } = require('../providers/historicalCallerCatalogProvider');
const { ingestEmpiricalCallerEvents } = require('./ingestEmpiricalCallerEvents');
const { backfillMarketHistory } = require('../backfill/marketBackfill');
const { replayToken } = require('../../replay/replayEngine');
const {
  VALIDATION_COHORT_003_ID,
  VALIDATION_COHORT_003_SELECTION_VERSION,
  VALIDATION_COHORT_003_TARGET_SIZE,
} = require('../candidateTypes');

/**
 * Empirical natural cohort — freeze membership before outcome evaluation.
 *
 * @param {object} store
 * @param {object} [options]
 */
async function buildValidationCohort003(store, options = {}) {
  const cohortId = options.cohortId || VALIDATION_COHORT_003_ID;
  const selectionVersion =
    options.selectionVersion || VALIDATION_COHORT_003_SELECTION_VERSION;
  const targetSize = options.targetSize || VALIDATION_COHORT_003_TARGET_SIZE;
  const dataClass = RESEARCH_DATA_CLASS.EMPIRICAL;

  const catalogProvider =
    options.catalogProvider || createHistoricalCallerCatalogProvider(options.catalogOptions);
  assertProviderAllowedForDataClass(dataClass, catalogProvider.providerId);

  const coverage = catalogProvider.getCoverageBounds?.() || {};
  const discovery = await discoverAndPersistCandidates(store, [catalogProvider]);
  const eligible = discovery.candidates.filter(c => c.status === 'ELIGIBLE');

  const selection = selectChronologicalCohort(eligible, {
    targetSize,
    selectionVersion,
    historicalStart: coverage.earliestMarketEvaluableAt,
  });

  const cohort = {
    id: cohortId,
    name: 'Signal V1 validation cohort 003 (empirical natural)',
    definitionVersion: RESEARCH_DEFINITION_VERSION,
    selectionVersion,
    dataClass,
    frozenAt: null,
    metadata: {
      phase: 'C-empirical',
      dataClass,
      targetSize,
      selectionRule: selection.breakdown.procedure,
      historicalProvider: catalogProvider.providerId,
      historicalProviderVersion: catalogProvider.providerVersion,
      coveragePeriod: {
        start: coverage.earliestMarketEvaluableAt?.toISOString?.() || null,
        end: coverage.latestMarketEvaluableAt?.toISOString?.() || null,
      },
      investigatedProviders: catalogProvider.investigatedProviders?.() || [],
      selectionBreakdown: selection.breakdown,
      discoveryStats: {
        discovered: discovery.discovered,
        unique: discovery.unique,
        eligible: eligible.length,
        rejectedDuplicates: discovery.rejectedDuplicates.length,
      },
      note: 'First caller = earliest call in empirical provider dataset (coverage-limited).',
    },
  };

  await callStore(store, 'upsertResearchCohort', cohort);

  for (const candidate of selection.selected) {
    await updateCandidateStatus(store, candidate.id, 'SELECTED');
    await callStore(store, 'addCohortMember', {
      cohortId,
      tokenAddress: candidate.tokenAddress,
      inclusionReason: 'Chronological empirical selection (outcome-agnostic)',
      provenance: {
        selectionVersion,
        selectionMethod: selection.breakdown.procedure,
        discoveredFrom: candidate.discoveredFrom,
        earliestKnownCallAt: candidate.earliestKnownCallAt,
        dataClass,
      },
    });
  }

  cohort.frozenAt = new Date().toISOString();
  await callStore(store, 'upsertResearchCohort', cohort);

  const rawByToken = indexRawByToken(discovery.candidates);
  const backfill = await empiricalBackfillAndReplay(store, cohortId, {
    rawByToken,
    marketProvider: options.marketProvider,
    researchConfig: { strictClusterIndependence: true, ...(options.researchConfig || {}) },
  });

  return {
    cohort,
    discovery,
    selection,
    backfill,
    dataClass,
  };
}

function indexRawByToken(candidates) {
  const map = new Map();
  for (const c of candidates) {
    map.set(c.tokenAddress, {
      ...c.raw,
      tokenAddress: c.tokenAddress,
      earliestKnownCallAt: c.earliestKnownCallAt,
      acquisitionPayload: c.acquisitionPayload || c.raw?.acquisitionPayload,
    });
  }
  return map;
}

async function updateCandidateStatus(store, id, status) {
  if (store.updateResearchCandidateStatus) {
    await callStore(store, 'updateResearchCandidateStatus', id, status);
    return;
  }
  if (store.researchCandidates?.has(id)) {
    const row = store.researchCandidates.get(id);
    row.status = status;
    store.researchCandidates.set(id, row);
  }
}

async function empiricalBackfillAndReplay(store, cohortId, options = {}) {
  const members = await callStore(store, 'getCohortMembers', cohortId);
  const summary = [];

  for (const member of members) {
    const raw = options.rawByToken?.get(member.tokenAddress) || {
      tokenAddress: member.tokenAddress,
      earliestKnownCallAt: member.provenance?.earliestKnownCallAt,
      acquisitionPayload: {},
    };

    const candidate = {
      tokenAddress: member.tokenAddress,
      earliestKnownCallAt: raw.earliestKnownCallAt || member.provenance?.earliestKnownCallAt,
    };

    const caller = await ingestEmpiricalCallerEvents(store, raw);

    const market = await backfillMarketHistory(store, candidate, raw, {
      marketProvider: options.marketProvider,
      empirical: true,
      allowFixtureFallback: false,
    });

    let replayStatus = 'SKIPPED_NO_MARKET';
    if (market.source === 'live_provider' || market.observationCount > 0) {
      const replay = await replayToken(store, {
        tokenAddress: member.tokenAddress,
        startTime: market.window?.startTime,
        endTime: market.window?.endTime,
        decisionAnchor: market.window?.anchor,
        researchConfig: options.researchConfig,
      });
      replayStatus = replay.replayStatus || (replay.skipped ? 'DATA_INSUFFICIENT' : 'COMPLETE');
    } else if (market.source === 'none') {
      replayStatus = 'DATA_INSUFFICIENT';
    }

    summary.push({
      tokenAddress: member.tokenAddress,
      caller,
      market: {
        historicalDataStatus: market.historicalDataStatus || market.metadata?.historicalDataStatus,
        source: market.source,
      },
      replayStatus,
    });

    const candRow = await findCandidateByToken(store, member.tokenAddress);
    if (candRow) {
      await updateCandidateStatus(
        store,
        candRow.id,
        replayStatus === 'COMPLETE' ? 'REPLAYED' : 'DATA_INSUFFICIENT'
      );
    }
  }

  return { members: members.length, summary };
}

async function findCandidateByToken(store, tokenAddress) {
  if (store.listResearchCandidates) {
    const rows = await callStore(store, 'listResearchCandidates', { tokenAddress });
    return rows[0] || null;
  }
  if (store.researchCandidates) {
    return (
      [...store.researchCandidates.values()].find(c => c.tokenAddress === tokenAddress) || null
    );
  }
  return null;
}

module.exports = {
  buildValidationCohort003,
  empiricalBackfillAndReplay,
};
