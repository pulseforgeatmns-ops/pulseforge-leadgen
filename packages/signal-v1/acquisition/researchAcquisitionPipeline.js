'use strict';

const { createHash } = require('crypto');
const { callStore } = require('../storage/storeUtils');
const { dedupeCandidates, evaluateCandidateEligibility } = require('./eligibility');
const { selectBalancedCohort } = require('./deterministicSelection');
const { backfillCallerEvents } = require('./backfill/callerBackfill');
const { backfillMarketHistory, buildFixturePricePath, persistPricePathAsObservations } = require('./backfill/marketBackfill');
const { backfillStructureEvidence } = require('./backfill/structureBackfill');
const { backfillWalletEvidence } = require('./backfill/walletBackfill');
const { frontRunnersCandidateProvider } = require('./providers/frontRunnersCandidateProvider');
const { proceduralCandidateProvider } = require('./providers/proceduralCandidateProvider');
const { replayToken } = require('../replay/replayEngine');
const {
  VALIDATION_COHORT_001_ID,
  VALIDATION_COHORT_001_SELECTION_VERSION,
  VALIDATION_COHORT_001_TARGET_SIZE,
  VALIDATION_COHORT_001_PER_CATEGORY,
  VALIDATION_COHORT_002_ID,
  VALIDATION_COHORT_002_SELECTION_VERSION,
  VALIDATION_COHORT_002_TARGET_SIZE,
  VALIDATION_COHORT_002_PER_CATEGORY,
} = require('./candidateTypes');
const { RESEARCH_DEFINITION_VERSION } = require('../types');
const { RESEARCH_CASES } = require('../fixtures/frontRunnersCases');
const { proceduralCandidateProviderV2 } = require('./providers/proceduralCandidateProviderV2');

/**
 * @param {object} store
 * @param {import('./providers/interfaces').ResearchCandidateProvider[]} [providers]
 */
async function discoverAndPersistCandidates(store, providers) {
  const activeProviders = providers || [
    frontRunnersCandidateProvider,
    proceduralCandidateProvider,
  ];

  const discovered = [];
  for (const provider of activeProviders) {
    const batch = await provider.discoverCandidates();
    for (const raw of batch) {
      discovered.push({ ...raw, discoveredFrom: raw.discoveredFrom || provider.providerId });
    }
  }

  const { unique, rejectedDuplicates } = dedupeCandidates(discovered);
  const results = { discovered: discovered.length, unique: unique.length, rejectedDuplicates, candidates: [] };

  for (const raw of unique) {
    const eligibility = evaluateCandidateEligibility(raw);
    const status = eligibility.eligible ? 'ELIGIBLE' : 'INELIGIBLE';
    const row = await persistCandidate(store, {
      raw,
      status,
      exclusionReason: eligibility.exclusionReason,
    });
    results.candidates.push(row);
  }

  for (const dup of rejectedDuplicates) {
    await persistCandidate(store, {
      raw: dup.raw,
      status: 'INELIGIBLE',
      exclusionReason: dup.exclusionReason,
    });
  }

  return results;
}

async function persistCandidate(store, { raw, status, exclusionReason }) {
  const id = candidateId(raw);
  const row = {
    id,
    tokenAddress: raw.tokenAddress,
    chain: raw.chain || 'solana',
    discoveredFrom: raw.discoveredFrom,
    earliestKnownCallAt: new Date(raw.earliestKnownCallAt),
    sourceIds: raw.sourceIds || [],
    sourceClusterIds: raw.sourceClusterIds || [],
    selectionCategory: raw.selectionCategory || 'unknown',
    selectionReason: raw.selectionReason || null,
    provenance: raw.provenance || {},
    status,
    exclusionReason,
    acquisitionPayload: raw.acquisitionPayload || {},
    raw,
  };

  if (store.upsertResearchCandidate) {
    await callStore(store, 'upsertResearchCandidate', row);
  } else if (store.researchCandidates) {
    store.researchCandidates.set(id, row);
  }

  await callStore(store, 'upsertToken', {
    tokenAddress: raw.tokenAddress,
    chain: raw.chain || 'solana',
    addressProvenance: raw.provenance?.addressProvenance || 'source-claimed',
    metadata: {
      researchAnchor: new Date(raw.earliestKnownCallAt).toISOString(),
      selectionCategory: raw.selectionCategory,
      slug: raw.provenance?.slug || null,
      acquisitionProvenance: raw.provenance,
    },
  });

  return row;
}

function candidateId(raw) {
  return createHash('sha256')
    .update(`${raw.tokenAddress}|${raw.discoveredFrom}`)
    .digest('hex')
    .slice(0, 32);
}

/**
 * @param {object} store
 * @param {object} [options]
 */
async function buildValidationCohort001(store, options = {}) {
  const cohortId = options.cohortId || VALIDATION_COHORT_001_ID;
  const selectionVersion =
    options.selectionVersion || VALIDATION_COHORT_001_SELECTION_VERSION;
  const perCategory = options.perCategory || VALIDATION_COHORT_001_PER_CATEGORY;
  const targetSize = options.targetSize || VALIDATION_COHORT_001_TARGET_SIZE;
  const freeze = options.freeze !== false;

  const discovery = await discoverAndPersistCandidates(store, options.providers);
  const eligible = discovery.candidates.filter(c => c.status === 'ELIGIBLE');
  const selection = selectBalancedCohort(eligible, {
    perCategory,
    targetSize,
    selectionVersion,
  });

  const cohort = {
    id: cohortId,
    name: 'Signal V1 validation cohort 001',
    definitionVersion: RESEARCH_DEFINITION_VERSION,
    selectionVersion,
    frozenAt: null,
    metadata: {
      phase: 'B',
      targetSize,
      perCategory,
      selectionBreakdown: selection.breakdown,
      discoveryStats: {
        discovered: discovery.discovered,
        unique: discovery.unique,
        eligible: eligible.length,
        rejectedDuplicates: discovery.rejectedDuplicates.length,
      },
    },
  };

  await callStore(store, 'upsertResearchCohort', cohort);

  for (const candidate of selection.selected) {
    await updateCandidateStatus(store, candidate.id, 'SELECTED');
    await callStore(store, 'addCohortMember', {
      cohortId,
      tokenAddress: candidate.tokenAddress,
      inclusionReason: candidate.selectionReason || 'Deterministic validation cohort selection',
      provenance: {
        selectionCategory: candidate.selectionCategory,
        selectionVersion,
        selectionMethod: selection.breakdown.procedure,
        discoveredFrom: candidate.discoveredFrom,
        note: 'Selection metadata only — not used in feature snapshots',
      },
    });
  }

  const rawByToken = indexRawByToken(discovery.candidates);
  const backfill = await backfillAndReplayCohort(store, cohortId, {
    rawByToken,
    marketProvider: options.marketProvider,
  });

  if (freeze) {
    cohort.frozenAt = new Date().toISOString();
    await callStore(store, 'upsertResearchCohort', cohort);
  }

  return {
    cohort,
    discovery,
    selection,
    backfill,
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
      selectionCategory: c.selectionCategory,
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

async function backfillAndReplayCohort(store, cohortId, options = {}) {
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
      earliestKnownCallAt:
        raw.earliestKnownCallAt ||
        member.provenance?.researchAnchor ||
        store.researchCandidates &&
          [...(store.researchCandidates?.values?.() || [])].find(
            c => c.tokenAddress === member.tokenAddress
          )?.earliestKnownCallAt,
    };

    const caller = await backfillCallerEvents(store, candidate, raw);
    const market = await backfillMarketHistory(store, candidate, raw, {
      marketProvider: options.marketProvider,
    });

    if (market.source === 'none' && market.window) {
      const path = buildFixturePricePath(candidate, raw, market.window.anchor);
      await persistPricePathAsObservations(store, member.tokenAddress, path);
    }

    await backfillStructureEvidence(store, candidate, raw);
    await backfillWalletEvidence(store, candidate, raw);

    const replay = await replayToken(store, {
      tokenAddress: member.tokenAddress,
      startTime: market.window?.startTime,
      endTime: market.window?.endTime,
      decisionAnchor: market.window?.anchor,
    });

    summary.push({
      tokenAddress: member.tokenAddress,
      caller,
      market: {
        historicalDataStatus: market.historicalDataStatus || market.metadata?.historicalDataStatus,
        source: market.source,
      },
      replayStatus: replay.replayStatus || replay.skipped ? replay.replayStatus : 'COMPLETE',
    });

    const candRow = await findCandidateByToken(store, member.tokenAddress);
    if (candRow) {
      await updateCandidateStatus(
        store,
        candRow.id,
        replay.skipped ? 'DATA_INSUFFICIENT' : 'REPLAYED'
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

/**
 * Holdout cohort — excludes Validation 001 tokens and Phase A design fixtures.
 *
 * @param {object} store
 * @param {object} [options]
 */
async function buildValidationCohort002(store, options = {}) {
  const cohortId = options.cohortId || VALIDATION_COHORT_002_ID;
  const selectionVersion =
    options.selectionVersion || VALIDATION_COHORT_002_SELECTION_VERSION;
  const perCategory = options.perCategory || VALIDATION_COHORT_002_PER_CATEGORY;
  const targetSize = options.targetSize || VALIDATION_COHORT_002_TARGET_SIZE;
  const freeze = options.freeze !== false;

  const excludeTokens = new Set(options.excludeTokens || []);
  for (const c of RESEARCH_CASES) {
    if (c.tokenAddress) excludeTokens.add(c.tokenAddress);
  }
  try {
    const priorMembers = await callStore(store, 'getCohortMembers', VALIDATION_COHORT_001_ID);
    for (const m of priorMembers) excludeTokens.add(m.tokenAddress);
  } catch {
    /* cohort 001 may not exist yet */
  }

  const discovery = await discoverAndPersistCandidates(store, [proceduralCandidateProviderV2]);
  const eligible = discovery.candidates.filter(
    c => c.status === 'ELIGIBLE' && !excludeTokens.has(c.tokenAddress)
  );

  const selection = selectBalancedCohort(eligible, {
    perCategory,
    targetSize,
    selectionVersion,
  });

  const cohort = {
    id: cohortId,
    name: 'Signal V1 validation cohort 002 (holdout)',
    definitionVersion: RESEARCH_DEFINITION_VERSION,
    selectionVersion,
    frozenAt: null,
    metadata: {
      phase: 'B-holdout',
      targetSize,
      perCategory,
      holdout: true,
      excludedTokenCount: excludeTokens.size,
      selectionBreakdown: selection.breakdown,
      discoveryStats: {
        discovered: discovery.discovered,
        unique: discovery.unique,
        eligible: eligible.length,
        rejectedDuplicates: discovery.rejectedDuplicates.length,
      },
    },
  };

  await callStore(store, 'upsertResearchCohort', cohort);

  for (const candidate of selection.selected) {
    await updateCandidateStatus(store, candidate.id, 'SELECTED');
    await callStore(store, 'addCohortMember', {
      cohortId,
      tokenAddress: candidate.tokenAddress,
      inclusionReason: candidate.selectionReason || 'Deterministic holdout cohort selection',
      provenance: {
        selectionCategory: candidate.selectionCategory,
        selectionVersion,
        selectionMethod: selection.breakdown.procedure,
        discoveredFrom: candidate.discoveredFrom,
        note: 'Holdout — selection metadata only; not used in evidence generation',
      },
    });
  }

  const rawByToken = indexRawByToken(discovery.candidates);
  const backfill = await backfillAndReplayCohort(store, cohortId, {
    rawByToken,
    marketProvider: options.marketProvider,
  });

  if (freeze) {
    cohort.frozenAt = new Date().toISOString();
    await callStore(store, 'upsertResearchCohort', cohort);
  }

  return {
    cohort,
    discovery,
    selection,
    backfill,
    excludedTokens: [...excludeTokens],
  };
}

module.exports = {
  discoverAndPersistCandidates,
  buildValidationCohort001,
  buildValidationCohort002,
  backfillAndReplayCohort,
  findCandidateByToken,
};
