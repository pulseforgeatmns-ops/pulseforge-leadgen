'use strict';

const { TENANT_ID } = require('../../../services/leviScoutProductionContract');
const {
  composeReviewBundle,
  formatReviewAnswer,
  loadPersistedResearchRecords,
  loadLastBriefing,
} = require('../../../services/leviScoutMaxReview');
const { inspectApprovedBlueprint, loadTenantUnderstanding } = require('../../../services/leviScoutBlueprintReadiness');
const { buildStructuredResponse } = require('./WorkspaceTypes');

function resolveTenantId(input = {}) {
  const session = input.session || null;
  const sessionCtx = session && session.context && typeof session.context === 'object' ? session.context : {};
  const envelope = input.context && typeof input.context === 'object' ? input.context : {};
  return String(
    input.authorizedTenantId
      || input.tenantId
      || envelope.tenantId
      || sessionCtx.tenantId
      || envelope.clientId
      || sessionCtx.clientId
      || ''
  ).trim();
}

function isOperationalReadQuestion(question, input = {}) {
  const q = String(question || '').trim();
  if (!q) return false;
  try {
    const operating = require('./OperatingEvidenceRetrieval');
    if (operating.shouldRetrieveOperatingEvidence(q)) return true;
  } catch (_) {
    /* classifier unavailable */
  }
  try {
    const { isOperationalReadRequest } = require('./OperationalReadIntent');
    if (isOperationalReadRequest(q, { session: input.session, context: input.context })) return true;
  } catch (_) {
    /* classifier unavailable */
  }
  return false;
}

function shouldHandleLeviScoutReview(input = {}) {
  if (resolveTenantId(input) !== String(TENANT_ID)) return false;
  const question = String(input.question || '').trim();
  if (!question) return false;
  if (isOperationalReadQuestion(question, input)) return false;
  return /\b(?:find|discover(?:y|ed|ing)?|search\s+for|look\s+for|locate|source|prospect\s+for)\b/i.test(question)
    && /\b(?:prospects?|borrowers?|leads?|candidates?|operators?)\b/i.test(question);
}

async function maybeHandleLeviScoutReviewTurn(input = {}) {
  const tenantId = resolveTenantId(input);
  if (tenantId !== String(TENANT_ID)) return null;
  if (isOperationalReadQuestion(input.question, input)) return null;
  if (!input.forceLeviReview && !shouldHandleLeviScoutReview(input)) return null;
  const db = input.db || (input.context && input.context.db) || null;
  const understanding = input.understanding
    || (typeof input.loadUnderstanding === 'function' ? await input.loadUnderstanding({ tenantId }) : null)
    || (db ? await loadTenantUnderstanding({ db, tenantId }) : null);
  const records = Array.isArray(input.reviewRecords)
    ? input.reviewRecords
    : (typeof input.loadReviewRecords === 'function'
      ? await input.loadReviewRecords({ tenantId })
      : await loadPersistedResearchRecords({ db, tenantId }));
  const briefing = input.reviewBriefing !== undefined
    ? input.reviewBriefing
    : await loadLastBriefing({ db, tenantId });
  const bundle = input.reviewBundle || composeReviewBundle({
    tenantId,
    records,
    understanding,
    briefing,
    criteria: input.criteria,
  });
  const readiness = inspectApprovedBlueprint(understanding, tenantId);
  const prose = formatReviewAnswer(bundle);
  const structured = buildStructuredResponse({
    answer: prose,
    reasoning: [
      'Answered from tenant-21 Scout research-only records and the approved Blueprint. The general Scout acquisition loop was not entered.',
      'Property activity, inferred financing need, verified borrower intent, and qualification are separate layers. A permit is not borrower intent.',
      bundle.counts.qualifiedProspects
        ? `${bundle.counts.qualifiedProspects} record(s) meet approved qualification criteria.`
        : 'No record meets approved qualification criteria.',
    ],
    supportingEvidence: (bundle.records || []).slice(0, 10).map((item, index) => ({
      id: `levi-scout-${index + 1}`,
      summary: item.evidenceSummary,
      sourceType: 'scout_research_only',
      limitations: item.permitDoesNotEstablishIntent
        ? ['A permit does not establish borrower intent.']
        : [],
    })),
    contradictingEvidence: [],
    confidence: 0.7,
    nextInvestigations: [
      ...(readiness.questionsForLevi || []),
      ...[...new Set((bundle.researchCandidates || []).map(item => item.nextResearchAction))],
    ].slice(0, 8),
    recommendedActions: [
      { id: 'review_research_candidates', type: 'review', label: 'Review research candidates' },
      { id: 'hold_outreach', type: 'review', label: 'Keep outreach off' },
    ],
    metadata: {
      sourcesUsed: {
        briefing: Boolean(briefing),
        reasoning: true,
        memory: false,
        policy: true,
        knowledge: Boolean(readiness.trusted),
      },
      evidenceCount: bundle.counts.enriched,
      asOf: new Date().toISOString(),
      operationalRead: {
        kind: 'levi_scout_research_review',
        researchCandidates: bundle.counts.researchCandidates,
        qualifiedProspects: bundle.counts.qualifiedProspects,
        verifiableBusinessContacts: bundle.counts.verifiableBusinessContacts,
        outreachAttempted: false,
        paidProvidersUsed: false,
        acquisitionLoop: false,
      },
    },
  });
  if (input.session && input.session.context && typeof input.session.context === 'object') {
    input.session.context.lastLeviScoutReview = {
      counts: bundle.counts,
      questionsForLevi: readiness.questionsForLevi,
    };
  }
  return {
    reason: 'levi_scout_research_review',
    structured,
    prose,
    bundle,
    responseContract: null,
  };
}

module.exports = {
  shouldHandleLeviScoutReview,
  maybeHandleLeviScoutReviewTurn,
  isOperationalReadQuestion,
};
