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
const {
  assignProvidersForRequirements,
  explainProviderForOperator,
} = require('../coverage/EvidenceProviderAssignment');
const {
  buildHypothesisInvestigationPlan,
  buildInvestigationTask,
  INVESTIGATION_PHASES,
} = require('../coverage/HypothesisInvestigationPlanner');

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

function candidateNeedsInvestigation(row = {}) {
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
  const ranked = Array.isArray(payload.rankedProspects) ? payload.rankedProspects : [];
  const fromRanked = ranked.filter(candidateNeedsInvestigation);

  if (fromRanked.length) return fromRanked;

  const fitCandidates = Array.isArray(payload.fitCandidates) ? payload.fitCandidates : [];
  return fitCandidates
    .filter(
      (row) =>
        row.qualified === true ||
        row.qualificationStatus === QUALIFICATION_STATUSES.QUALIFIED ||
        row.readinessState === READINESS_STATES.UNKNOWN
    )
    .map((row, index) => ({
      rank: index + 1,
      id: row.companyId || row.id || null,
      name: row.name,
      readinessState: row.readinessState || READINESS_STATES.UNKNOWN,
      qualificationStatus: row.qualificationStatus || QUALIFICATION_STATUSES.QUALIFIED,
      prospectBucket: PROSPECT_BUCKETS.INVESTIGATION_REQUIRED,
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
    }))
    .filter(candidateNeedsInvestigation);
}

function buildEntityGapTasksForCandidate(candidate = {}, opts = {}) {
  const entityId = asText(candidate.id || candidate.companyId) || `entity-${Date.now()}`;
  const entityName = asText(candidate.name) || entityId;
  const gaps = new Map();

  const evaluation = candidate.evaluation || {};
  const investigation = evaluation.investigation || {};

  for (const text of investigation.missingEvidence || []) {
    const mapped = mapMissingEvidenceToGap(text);
    if (!mapped) continue;
    gaps.set(`${mapped.gap}:${mapped.evidenceType}`, {
      ...mapped,
      source: 'missingEvidence',
    });
  }

  for (const text of investigation.unresolvedHypotheses || []) {
    const mapped = mapHypothesisToGap(text);
    if (!mapped) continue;
    gaps.set(`${mapped.gap}:${mapped.evidenceType}`, {
      evidenceType: mapped.evidenceType,
      gap: mapped.gap,
      phase: phaseForEvidenceType(mapped.evidenceType),
      hypothesis: mapped.hypothesis,
      source: 'unresolvedHypothesis',
    });
  }

  for (const unknown of candidate.unknowns || []) {
    const text = typeof unknown === 'object' ? unknown.text : String(unknown || '');
    const mapped = mapHypothesisToGap(text);
    if (!mapped) continue;
    gaps.set(`${mapped.gap}:${mapped.evidenceType}`, {
      evidenceType: mapped.evidenceType,
      gap: mapped.gap,
      phase: phaseForEvidenceType(mapped.evidenceType),
      hypothesis: mapped.hypothesis || text,
      source: 'unknown',
    });
  }

  const rec = candidate.recommendedNextInvestigation;
  if (rec && rec.action && !/no further investigation required/i.test(rec.action)) {
    const mapped = mapHypothesisToGap(rec.action);
    if (mapped) {
      gaps.set(`${mapped.gap}:${mapped.evidenceType}`, {
        evidenceType: mapped.evidenceType,
        gap: mapped.gap,
        phase: phaseForEvidenceType(mapped.evidenceType),
        hypothesis: rec.action,
        source: 'recommendedNextInvestigation',
        impact: rec.impact || 'high',
        howToVerify: rec.howToVerify || null,
      });
    }
  }

  if (!gaps.size) {
    gaps.set('decision_maker:decision_makers', {
      evidenceType: INVESTIGATIVE_EVIDENCE.DECISION_MAKERS,
      gap: 'decision_maker',
      phase: INVESTIGATION_PHASES.DECISION_MAKERS,
      hypothesis: `Resolve remaining uncertainty for ${entityName}`,
      source: 'default',
    });
  }

  const requirements = [...gaps.values()].map((gap) => ({
    evidenceType: gap.evidenceType,
    questionIds: [`${entityId}:${gap.gap}`],
    required: true,
    satisfied: false,
    confidence: 0,
    sources: [],
  }));

  const assignments = assignProvidersForRequirements(requirements, opts);
  const tasks = [];

  for (const gap of gaps.values()) {
    const providers = assignments.filter((a) => a.evidenceType === gap.evidenceType);
    tasks.push(
      buildInvestigationTask({
        id: `task:${entityId}:${gap.evidenceType}`,
        evidenceType: gap.evidenceType,
        label: `${entityName}: ${gap.hypothesis || gap.label || gap.gap}`,
        providers,
        phase: gap.phase || phaseForEvidenceType(gap.evidenceType),
        mergeStrategy: 'evidence_fusion',
        rationale: `Entity investigation (ADR-102): ${gap.source} → ${gap.gap}`,
        scope: 'entity',
        entityId,
        entityName,
        candidateId: entityId,
        gap: gap.gap,
        hypothesis: gap.hypothesis || gap.label || null,
        impact: gap.impact || 'high',
        howToVerify: gap.howToVerify || null,
        status: 'pending',
      })
    );
  }

  return tasks;
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
  const rows = [];
  const seen = new Set();

  function pushRow(row = {}) {
    const id = asText(row.id || row.companyId) || asText(row.name);
    if (!id || seen.has(id)) return;
    seen.add(id);
    rows.push({
      id: row.id || row.companyId || id,
      name: row.name || id,
      company: row.name || row.company || id,
      industry: row.industry || null,
      location: row.location || row.address || null,
      website: row.website || row.url || null,
      origin: 'prior_discovery',
      _identityKey: row._identityKey || id,
      _preservedFromContinuation: true,
    });
  }

  for (const row of payload.rankedProspects || []) pushRow(row);
  for (const row of payload.companies || []) pushRow(row);
  for (const row of payload.opportunities || []) pushRow(row);
  for (const row of payload.fitCandidates || []) pushRow(row);

  return rows;
}

function shouldExpandUniverse(priorPayload = {}, opts = {}) {
  if (opts.forceBroadDiscovery === true) return true;
  if (priorPayload.blocked === true) return true;
  if (/REQUEST_DENIED|provider failure|blocked/i.test(String(priorPayload.summary || ''))) return true;

  const universeCount =
    Number(priorPayload.candidateUniverseCount || 0) ||
    Number(priorPayload.companies && priorPayload.companies.length) ||
    0;
  const qualifiedCount = Number(priorPayload.qualifiedCount || 0);
  const rankedCount = Array.isArray(priorPayload.rankedProspects)
    ? priorPayload.rankedProspects.length
    : 0;

  if (universeCount === 0 && qualifiedCount === 0 && rankedCount === 0) return true;
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
  if (payload.rankedProspects || payload.companies) return payload;
  if (payload.payload && (payload.payload.rankedProspects || payload.payload.companies)) {
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
  candidateNeedsInvestigation,
  extractInvestigationCandidatesFromPayload,
  buildEntityGapTasksForCandidate,
  buildEntityInvestigationPlan,
  extractPreservedCandidatesFromPayload,
  shouldExpandUniverse,
  resolveInvestigationMode,
  buildInvestigationContinuationContext,
  extractPayloadFromDiscoveryContribution,
  buildOperatorExplanationsForEntityPlan,
};
