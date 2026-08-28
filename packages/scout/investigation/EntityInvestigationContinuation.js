'use strict';

/**
 * ADR-102 — Investigation Follows Uncertainty.
 *
 * Converts descriptive investigation metadata (missingEvidence, unresolvedHypotheses,
 * recommendedNextInvestigation) into executable entity-scoped investigation tasks.
 *
 * Investigation unit: Candidate + Hypothesis + Evidence Gap
 */

const { asText, READINESS_STATES, QUALIFICATION_STATUSES, PROSPECT_BUCKETS } = require('../../max/scoutAcquisition/Types');
const { INVESTIGATIVE_EVIDENCE } = require('../coverage/EvidenceRequirements');
const { explainProviderForOperator } = require('../coverage/EvidenceProviderAssignment');
const {
  buildHypothesisInvestigationPlan,
  INVESTIGATION_PHASES,
} = require('../coverage/HypothesisInvestigationPlanner');
const {
  collectCandidateBeliefsFromPayload,
  beliefsToPreservedCandidates,
  hydrateCandidateBelief,
} = require('./CandidateBeliefState');

const INVESTIGATION_MODES = Object.freeze({
  INITIAL: 'initial',
  ENTITY_CONTINUATION: 'entity_continuation',
  BROAD_DISCOVERY: 'broad_discovery',
});

/** Map descriptive missing-evidence strings to canonical evidence types and gaps. */
const MISSING_EVIDENCE_PATTERNS = Object.freeze([
  {
    pattern: /decision[- ]?maker|operations decision|buying[- ]?path|contact path/i,
    evidenceType: INVESTIGATIVE_EVIDENCE.DECISION_MAKERS,
    gap: 'decision_maker',
    phase: INVESTIGATION_PHASES.DECISION_MAKERS,
  },
  {
    pattern: /portfolio|property count|str portfolio|manages str/i,
    evidenceType: INVESTIGATIVE_EVIDENCE.PORTFOLIO,
    gap: 'portfolio_size',
    phase: INVESTIGATION_PHASES.IDENTITY,
  },
  {
    pattern: /growth|hiring|expansion|vendor[- ]?change|timing signal/i,
    evidenceType: INVESTIGATIVE_EVIDENCE.GROWTH,
    gap: 'buying_signals',
    phase: INVESTIGATION_PHASES.GROWTH,
  },
  {
    pattern: /cleaning|vendor|outsourc/i,
    evidenceType: INVESTIGATIVE_EVIDENCE.CLEANING,
    gap: 'cleaning_responsibility',
    phase: INVESTIGATION_PHASES.CLEANING,
  },
  {
    pattern: /review|website|enrichment|source[- ]backed|company evidence/i,
    evidenceType: INVESTIGATIVE_EVIDENCE.REVIEWS,
    gap: 'business_fit',
    phase: INVESTIGATION_PHASES.IDENTITY,
  },
  {
    pattern: /segment|operating[- ]model|business fit/i,
    evidenceType: INVESTIGATIVE_EVIDENCE.IDENTITY,
    gap: 'business_fit',
    phase: INVESTIGATION_PHASES.IDENTITY,
  },
  {
    pattern: /buying[- ]readiness|worth contacting/i,
    evidenceType: INVESTIGATIVE_EVIDENCE.BUYING,
    gap: 'buying_signals',
    phase: INVESTIGATION_PHASES.GROWTH,
  },
]);

const HYPOTHESIS_GAP_PATTERNS = Object.freeze([
  { pattern: /decision[- ]?maker|who (?:buys|decides|manages)/i, gap: 'decision_maker', evidenceType: INVESTIGATIVE_EVIDENCE.DECISION_MAKERS },
  { pattern: /portfolio|property|str/i, gap: 'portfolio_size', evidenceType: INVESTIGATIVE_EVIDENCE.PORTFOLIO },
  { pattern: /segment|match|fit|operating model/i, gap: 'business_fit', evidenceType: INVESTIGATIVE_EVIDENCE.IDENTITY },
  { pattern: /contact|worth contacting|timing/i, gap: 'buying_signals', evidenceType: INVESTIGATIVE_EVIDENCE.BUYING },
  { pattern: /vendor|cleaning|outsourc/i, gap: 'cleaning_responsibility', evidenceType: INVESTIGATIVE_EVIDENCE.CLEANING },
]);

function mapMissingEvidenceToGap(text = '') {
  const value = asText(text);
  if (!value) return null;
  for (const row of MISSING_EVIDENCE_PATTERNS) {
    if (row.pattern.test(value)) {
      return {
        evidenceType: row.evidenceType,
        gap: row.gap,
        phase: row.phase,
        label: value,
      };
    }
  }
  return {
    evidenceType: INVESTIGATIVE_EVIDENCE.CONTACT,
    gap: 'contact_path',
    phase: INVESTIGATION_PHASES.DECISION_MAKERS,
    label: value,
  };
}

function mapHypothesisToGap(text = '') {
  const value = asText(text);
  if (!value) return null;
  for (const row of HYPOTHESIS_GAP_PATTERNS) {
    if (row.pattern.test(value)) {
      return {
        evidenceType: row.evidenceType,
        gap: row.gap,
        hypothesis: value,
      };
    }
  }
  return {
    evidenceType: INVESTIGATIVE_EVIDENCE.CONTACT,
    gap: 'contact_path',
    hypothesis: value,
  };
}

function canonicalIdentityKey(row = {}) {
  return (
    asText(
      row.canonicalIdentity ||
        row.candidateId ||
        row.candidate_id ||
        row.id ||
        row.companyId ||
        row.placeId ||
        row.place_id ||
        row._identityKey ||
        row.name
    ) || ''
  ).toLowerCase();
}

function isExplicitlyExcluded(row = {}) {
  if (row.excluded === true) return true;
  if (row.prospectBucket === PROSPECT_BUCKETS.EXCLUDED) return true;
  if (row.qualificationStatus === QUALIFICATION_STATUSES.NOT_QUALIFIED && row.excludedReason) {
    return true;
  }
  const qual = row.qualification?.status || row.qualificationStatus;
  if (qual === 'excluded') return true;
  if (row.investigationState === 'excluded') return true;
  return false;
}

function getFitCandidatesFromPayload(payload = {}) {
  if (Array.isArray(payload.fitCandidates) && payload.fitCandidates.length) {
    return payload.fitCandidates;
  }
  if (Array.isArray(payload.discoveryArtifact?.fitCandidates)) {
    return payload.discoveryArtifact.fitCandidates;
  }
  return [];
}

function getUncertainCandidatesFromPayload(payload = {}) {
  const direct = payload.uncertainCandidates || payload.watchCandidates || [];
  if (direct.length) return direct;
  return payload.discoveryArtifact?.uncertainCandidates || payload.discoveryArtifact?.watchCandidates || [];
}

function getCandidateUniverseFromPayload(payload = {}) {
  if (Array.isArray(payload.candidateUniverse) && payload.candidateUniverse.length) {
    return payload.candidateUniverse;
  }
  if (Array.isArray(payload.discoveryArtifact?.candidateUniverse)) {
    return payload.discoveryArtifact.candidateUniverse;
  }
  return [];
}

function getQualifiedProspectsFromPayload(payload = {}) {
  const ranked = Array.isArray(payload.rankedProspects) ? payload.rankedProspects : [];
  return ranked.filter(
    (row) =>
      row.qualified === true ||
      row.qualificationStatus === QUALIFICATION_STATUSES.QUALIFIED ||
      row.prospectBucket === PROSPECT_BUCKETS.INVESTIGATION_REQUIRED
  );
}

function mapFitCandidateToInvestigationRow(row = {}, index = 0) {
  return {
    rank: index + 1,
    id: row.companyId || row.id || row.candidateId || row.candidate_id || null,
    name: row.name,
    readinessState: row.readinessState || READINESS_STATES.UNKNOWN,
    qualificationStatus: row.qualificationStatus || QUALIFICATION_STATUSES.QUALIFIED,
    prospectBucket: row.prospectBucket || PROSPECT_BUCKETS.INVESTIGATION_REQUIRED,
    evaluation: row.evaluation || {
      investigation: {
        missingEvidence: ['Buying-readiness timing signals'],
        unresolvedHypotheses: [
          'What evidence would most reduce uncertainty about whether this business is worth contacting now?',
        ],
      },
    },
    unknowns: row.unknowns || [],
    recommendedNextInvestigation: row.recommendedNextInvestigation || null,
    website: row.website || row.url || null,
    placeId: row.placeId || row.place_id || null,
    address: row.address || row.location || null,
    canonicalIdentity:
      row.canonicalIdentity || row.companyId || row.id || row.candidateId || row.candidate_id || null,
  };
}

function mapCandidateUniverseRecordToInvestigationCandidate(record = {}, rank = 1) {
  if (record.dedupeStatus === 'duplicate') return null;
  if (isExplicitlyExcluded(record)) return null;

  const id = asText(record.candidateId || record.candidate_id || record.id);
  if (!id) return null;

  const qualification = record.qualification || {};
  const readiness = record.readiness || {};
  const hypotheses = Array.isArray(record.hypotheses) ? record.hypotheses : [];
  const investigationMeta =
    record.investigationState && typeof record.investigationState === 'object'
      ? record.investigationState
      : {};

  const missingEvidence = [
    ...(investigationMeta.missingEvidence || []),
    ...(record.missingEvidence || []),
  ];
  if (!missingEvidence.length) {
    missingEvidence.push('Website / portfolio / review / decision-maker enrichment');
  }

  const unresolvedHypotheses = [
    ...hypotheses,
    ...(investigationMeta.unresolvedHypotheses || []),
    ...(record.unknowns || []),
  ];
  if (!unresolvedHypotheses.length) {
    unresolvedHypotheses.push(
      'What evidence would most reduce uncertainty about whether this business is worth contacting now?'
    );
  }

  const evaluation = record.evaluation
    ? {
        ...record.evaluation,
        investigation: {
          ...(record.evaluation.investigation || {}),
          missingEvidence: missingEvidence.length
            ? missingEvidence
            : record.evaluation.investigation && record.evaluation.investigation.missingEvidence &&
                record.evaluation.investigation.missingEvidence.length
              ? record.evaluation.investigation.missingEvidence
              : ['Website / portfolio / review / decision-maker enrichment'],
          unresolvedHypotheses:
            (record.evaluation.investigation && record.evaluation.investigation.unresolvedHypotheses) ||
            unresolvedHypotheses,
          canonicalGaps:
            (record.evaluation.investigation && record.evaluation.investigation.canonicalGaps) ||
            investigationMeta.canonicalGaps ||
            [],
        },
      }
    : {
        qualification: {
          status: qualification.status || record.qualificationStatus || QUALIFICATION_STATUSES.UNCERTAIN,
        },
        readiness: {
          status: readiness.status || record.readinessState || READINESS_STATES.UNKNOWN,
        },
        investigation: {
          missingEvidence,
          unresolvedHypotheses,
          canonicalGaps: investigationMeta.canonicalGaps || [],
        },
      };

  return {
    rank,
    id,
    candidateId: id,
    companyId: id,
    name: record.name || id,
    canonicalIdentity: record.canonicalIdentity || id,
    placeId: record.placeId || record.place_id || null,
    website: record.website || record.url || null,
    phone: record.phone || null,
    address: record.address || (Array.isArray(record.cities) ? record.cities[0] : null) || record.location || null,
    readinessState: readiness.status || record.readinessState || READINESS_STATES.UNKNOWN,
    qualificationStatus:
      qualification.status || record.qualificationStatus || QUALIFICATION_STATUSES.UNCERTAIN,
    prospectBucket: record.prospectBucket || PROSPECT_BUCKETS.FIT_INVESTIGATION,
    evaluation,
    unknowns: record.unknowns || hypotheses,
    recommendedNextInvestigation: record.recommendedNextInvestigation || null,
    confidence: record.confidence != null ? Number(record.confidence) : 0.5,
    origin: record.origin || 'candidate_universe',
    _identityKey: record.canonicalIdentity || id,
    _preservedFromContinuation: true,
  };
}

function candidateNeedsInvestigation(row = {}) {
  if (isExplicitlyExcluded(row)) return false;
  if (row.prospectBucket === PROSPECT_BUCKETS.EXCLUDED) return false;
  if (row.qualificationStatus === QUALIFICATION_STATUSES.NOT_QUALIFIED) return false;

  const evaluation = row.evaluation || {};
  const investigation = evaluation.investigation || {};
  const missing = [
    ...(investigation.missingEvidence || []),
    ...(row.highestRemainingUnknowns || []).map((u) => (typeof u === 'object' ? u.unknown : u)),
    ...(row.unknowns || []),
  ].filter(Boolean);

  const hypotheses = [
    ...(investigation.unresolvedHypotheses || []),
    ...(row.unknowns || []),
  ].filter(Boolean);

  const hasActionableRecommendation =
    row.recommendedNextInvestigation &&
    row.recommendedNextInvestigation.action &&
    !/no further investigation required/i.test(String(row.recommendedNextInvestigation.action));

  const qualifiedForInvestigation =
    row.qualificationStatus === QUALIFICATION_STATUSES.QUALIFIED ||
    row.qualificationStatus === QUALIFICATION_STATUSES.UNCERTAIN ||
    row.prospectBucket === PROSPECT_BUCKETS.INVESTIGATION_REQUIRED ||
    row.prospectBucket === PROSPECT_BUCKETS.FIT_INVESTIGATION ||
    row.readinessState === READINESS_STATES.UNKNOWN;

  return qualifiedForInvestigation && (missing.length > 0 || hypotheses.length > 0 || hasActionableRecommendation);
}

function extractInvestigationCandidatesFromPayload(payload = {}) {
  const seen = new Set();
  const result = [];

  function addCandidate(row, source) {
    if (!row || isExplicitlyExcluded(row)) return;

    let mapped = row;
    if (source === 'fitCandidates' || source === 'uncertainCandidates') {
      mapped = mapFitCandidateToInvestigationRow(row, result.length);
      if (source === 'uncertainCandidates') {
        mapped.qualificationStatus = row.qualificationStatus || QUALIFICATION_STATUSES.UNCERTAIN;
        mapped.prospectBucket = row.prospectBucket || PROSPECT_BUCKETS.FIT_INVESTIGATION;
      }
    } else if (source === 'candidateUniverse') {
      mapped = mapCandidateUniverseRecordToInvestigationCandidate(row, result.length + 1);
    }

    if (!mapped) return;
    const key = canonicalIdentityKey(mapped);
    if (!key || seen.has(key)) return;
    if (!candidateNeedsInvestigation(mapped)) return;
    seen.add(key);
    result.push(mapped);
  }

  for (const row of payload.rankedProspects || []) addCandidate(row, 'rankedProspects');
  for (const row of getQualifiedProspectsFromPayload(payload)) addCandidate(row, 'qualifiedProspects');
  for (const row of getFitCandidatesFromPayload(payload)) {
    if (
      row.qualified === true ||
      row.qualificationStatus === QUALIFICATION_STATUSES.QUALIFIED ||
      row.readinessState === READINESS_STATES.UNKNOWN
    ) {
      addCandidate(row, 'fitCandidates');
    }
  }
  for (const row of getUncertainCandidatesFromPayload(payload)) addCandidate(row, 'uncertainCandidates');
  for (const row of getCandidateUniverseFromPayload(payload)) addCandidate(row, 'candidateUniverse');

  return result;
}

function buildEntityGapTasksForCandidate(candidate = {}, opts = {}) {
  const { buildCandidateInvestigationTasks } = require('./CandidateInvestigation');
  return buildCandidateInvestigationTasks(candidate, opts);
}

function phaseForEvidenceType(evidenceType) {
  if (evidenceType === INVESTIGATIVE_EVIDENCE.IDENTITY) return INVESTIGATION_PHASES.IDENTITY;
  if (evidenceType === INVESTIGATIVE_EVIDENCE.DECISION_MAKERS) return INVESTIGATION_PHASES.DECISION_MAKERS;
  if (evidenceType === INVESTIGATIVE_EVIDENCE.GROWTH || evidenceType === INVESTIGATIVE_EVIDENCE.BUYING) {
    return INVESTIGATION_PHASES.GROWTH;
  }
  if (evidenceType === INVESTIGATIVE_EVIDENCE.CLEANING) return INVESTIGATION_PHASES.CLEANING;
  return INVESTIGATION_PHASES.IDENTITY;
}

function buildEntityInvestigationPlan(input = {}) {
  const { mission = {}, marketDefinition = {}, priorPayload = {}, opts = {} } = input;
  const candidates = extractInvestigationCandidatesFromPayload(priorPayload);
  const tasks = candidates.flatMap((candidate) => buildEntityGapTasksForCandidate(candidate, opts));

  const evidenceRequirements = tasks.map((task) => ({
    evidenceType: task.evidenceType,
    questionIds: [task.id],
    required: true,
    satisfied: false,
    confidence: 0,
    sources: [],
    entityId: task.entityId,
  }));

  return buildHypothesisInvestigationPlan({
    mission,
    objective: `Continue entity investigation for ${candidates.length} qualified candidate${candidates.length === 1 ? '' : 's'} (ADR-102).`,
    marketDefinition,
    hypotheses: candidates.map((candidate) => ({
      id: `hyp:${candidate.id || candidate.name}`,
      text: candidate.recommendedNextInvestigation?.action || `Investigate ${candidate.name}`,
      entityId: candidate.id,
      gap: 'entity_continuation',
      confidence: candidate.confidence || 0.5,
    })),
    questions: tasks.map((task) => ({
      id: task.id,
      question: task.gap,
      text: task.hypothesis || task.label,
      requiredEvidence: [task.evidenceType],
      hypothesisId: `hyp:${task.entityId}`,
      satisfied: false,
    })),
    evidenceRequirements,
    assignedProviders: tasks.flatMap((task) => task.providers || []),
    satisfiedEvidence: [],
    outstandingEvidence: evidenceRequirements,
    tasks,
    currentPhase: INVESTIGATION_PHASES.DECISION_MAKERS,
    sufficientlyInvestigated: tasks.length === 0,
    rationale:
      'Entity-scoped investigation plan (ADR-102). Tasks preserve prior identities and target unresolved hypotheses.',
    investigationMode: INVESTIGATION_MODES.ENTITY_CONTINUATION,
    preservedCandidateCount: candidates.length,
    entityCandidates: candidates,
  });
}

function extractPreservedCandidatesFromPayload(payload = {}) {
  const beliefs = collectCandidateBeliefsFromPayload(payload);
  return beliefsToPreservedCandidates(beliefs);
}

function countPrimaryCandidateUniverse(payload = {}) {
  return getCandidateUniverseFromPayload(payload).filter((row) => row.dedupeStatus !== 'duplicate').length;
}

function shouldExpandUniverse(priorPayload = {}, opts = {}) {
  if (opts.forceBroadDiscovery === true) return true;
  if (priorPayload.blocked === true) return true;
  if (/REQUEST_DENIED|provider failure|blocked/i.test(String(priorPayload.summary || ''))) return true;

  const universeFromRecords = countPrimaryCandidateUniverse(priorPayload);
  const universeCount =
    universeFromRecords ||
    Number(priorPayload.candidateUniverseCount || 0) ||
    Number(priorPayload.companies && priorPayload.companies.length) ||
    0;
  const qualifiedCount = Number(priorPayload.qualifiedCount || 0);
  const rankedCount = Array.isArray(priorPayload.rankedProspects)
    ? priorPayload.rankedProspects.length
    : 0;
  const investigableCount = extractInvestigationCandidatesFromPayload(priorPayload).length;

  if (universeCount === 0 && qualifiedCount === 0 && rankedCount === 0 && investigableCount === 0) {
    return true;
  }
  if (/coverage insufficient|geographic coverage|expand universe/i.test(String(opts.question || ''))) {
    return true;
  }
  return false;
}

function resolveInvestigationMode(input = {}) {
  const { priorPayload = {}, opts = {} } = input;
  if (opts.investigationContinuation !== true) return INVESTIGATION_MODES.INITIAL;
  if (shouldExpandUniverse(priorPayload, opts)) return INVESTIGATION_MODES.BROAD_DISCOVERY;

  const candidates = extractInvestigationCandidatesFromPayload(priorPayload);
  if (candidates.length > 0) {
    return INVESTIGATION_MODES.ENTITY_CONTINUATION;
  }
  return INVESTIGATION_MODES.BROAD_DISCOVERY;
}

function buildInvestigationContinuationContext(input = {}) {
  const priorPayload = input.priorPayload || input.priorDiscoveryPayload || {};
  const mode = resolveInvestigationMode({ priorPayload, opts: input.opts || {} });
  const entityCandidates = extractInvestigationCandidatesFromPayload(priorPayload);
  const preservedCandidates = extractPreservedCandidatesFromPayload(priorPayload);

  return {
    investigationContinuation: true,
    investigationMode: mode,
    priorDiscoveryPayload: priorPayload,
    preservedCandidates,
    entityCandidates,
    entityTaskCount:
      mode === INVESTIGATION_MODES.ENTITY_CONTINUATION
        ? entityCandidates.reduce(
            (sum, candidate) => sum + buildEntityGapTasksForCandidate(candidate, input.opts || {}).length,
            0
          )
        : 0,
  };
}

function extractPayloadFromDiscoveryContribution(contribution = {}) {
  const payload = contribution.payload || {};
  if (
    payload.rankedProspects ||
    payload.companies ||
    payload.candidateUniverse ||
    payload.candidateUniverseCount != null
  ) {
    return payload;
  }
  if (
    payload.payload &&
    (payload.payload.rankedProspects ||
      payload.payload.companies ||
      payload.payload.candidateUniverse ||
      payload.payload.candidateUniverseCount != null)
  ) {
    return payload.payload;
  }
  return payload;
}

function buildOperatorExplanationsForEntityPlan(plan = {}) {
  return (plan.tasks || []).slice(0, 8).map((task) => ({
    taskId: task.id,
    entityId: task.entityId,
    entityName: task.entityName,
    evidenceType: task.evidenceType,
    gap: task.gap,
    hypothesis: task.hypothesis,
    explanation:
      task.providers && task.providers[0]
        ? explainProviderForOperator(task.providers[0], plan)
        : `Investigate ${task.entityName} for ${task.gap}`,
  }));
}

module.exports = {
  INVESTIGATION_MODES,
  mapMissingEvidenceToGap,
  mapHypothesisToGap,
  canonicalIdentityKey,
  isExplicitlyExcluded,
  getFitCandidatesFromPayload,
  getUncertainCandidatesFromPayload,
  getCandidateUniverseFromPayload,
  getQualifiedProspectsFromPayload,
  mapCandidateUniverseRecordToInvestigationCandidate,
  candidateNeedsInvestigation,
  extractInvestigationCandidatesFromPayload,
  buildEntityGapTasksForCandidate,
  buildEntityInvestigationPlan,
  extractPreservedCandidatesFromPayload,
  countPrimaryCandidateUniverse,
  shouldExpandUniverse,
  resolveInvestigationMode,
  buildInvestigationContinuationContext,
  extractPayloadFromDiscoveryContribution,
  buildOperatorExplanationsForEntityPlan,
  hydrateCandidateBelief,
};
