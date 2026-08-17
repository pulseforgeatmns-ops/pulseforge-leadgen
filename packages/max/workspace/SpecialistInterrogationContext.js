'use strict';

/**
 * SPEC-101 — retrieve specialist cognitive traces before domain routing.
 * Conversation is the primary inspection surface. No specialist rerun.
 */

const specialistDelegation = require('../../../services/specialistDelegation');
const { buildStructuredResponse } = require('./WorkspaceTypes');
const {
  toBusinessEvidenceRefs,
  buildSystemProvenance,
} = require('../scoutAcquisition/InvestigationProvenance');

function defaultService(input = {}) {
  return input.delegationService || specialistDelegation;
}

function resolveTenantId(input = {}) {
  const session = input.session || null;
  const sessionCtx =
    session && session.context && typeof session.context === 'object'
      ? session.context
      : {};
  const envelope =
    input.context && typeof input.context === 'object' ? input.context : {};
  return String(
    input.authorizedTenantId ||
      envelope.tenantId ||
      sessionCtx.tenantId ||
      envelope.clientId ||
      sessionCtx.clientId ||
      ''
  ).trim();
}

function currentServiceArea(input = {}) {
  const session = input.session && input.session.context;
  const envelope = input.context || {};
  const cie = (session && session.clientIntelligence) || envelope.clientIntelligence || {};
  return (
    (session && (session.serviceArea || session.geography)) ||
    cie.geography ||
    cie.targetMarkets ||
    (envelope.businessContext && envelope.businessContext.serviceGeography) ||
    (envelope.targetContext && envelope.targetContext.geography) ||
    null
  );
}

function detectedCompanyCount(input = {}) {
  const session = input.session && input.session.context;
  const envelope = input.context || {};
  return (
    envelope.detectedCompanyCount ||
    envelope.prospectCount ||
    (envelope.operatorProspectList && envelope.operatorProspectList.prospectCount) ||
    (session && session.detectedCompanyCount) ||
    (session && session.prospectCount) ||
    (session && session.operatorProspectList && session.operatorProspectList.prospectCount) ||
    null
  );
}

/**
 * @param {object} input
 * @returns {Promise<object|null>}
 */
async function maybeHandleSpecialistInterrogationTurn(input = {}) {
  const question = String(input.question || '').trim();
  if (!question) return null;

  const service = defaultService(input);
  if (typeof service.classifyOperatorIntent !== 'function' && !specialistDelegation.classifyOperatorIntent) {
    return null;
  }
  const classify = service.classifyOperatorIntent || specialistDelegation.classifyOperatorIntent;
  const listTraces =
    (service.listRecentCognitiveTraces && service.listRecentCognitiveTraces.bind(service)) ||
    specialistDelegation.listRecentCognitiveTraces;
  const resolveReferent = service.resolveRecentReferent || specialistDelegation.resolveRecentReferent;
  const answer = service.answerFromTrace || specialistDelegation.answerFromTrace;
  const formatAmbiguous = service.formatDisambiguation || specialistDelegation.formatDisambiguation;
  const limitation = service.limitationAnswer || specialistDelegation.limitationAnswer;
  const INTENT = service.INTENT || specialistDelegation.INTENT;

  const tenantId = resolveTenantId(input);
  if (!tenantId) return null;

  let traces = [];
  if (typeof listTraces === 'function') {
    traces = await listTraces(
      {
        authorizedTenantId: tenantId,
        tenantId,
        sessionId: input.session && input.session.id,
        operatorQuestion: question,
        limit: 12,
      },
      input.delegationOpts || {}
    );
  }

  const intent = classify(question, traces);
  if (
    !intent ||
    (intent.kind !== INTENT.INTERROGATE && intent.kind !== INTENT.CONTEXT_INSPECTION)
  ) {
    return null;
  }

  if (!traces.length) {
    if (intent.specialist && intent.specialist !== 'scout') {
      const prose = limitation(intent.specialist);
      return buildTurn({
        prose,
        reason: 'specialist_trace_unavailable',
        question,
        intent,
      });
    }
    return null;
  }

  const session = input.session || {};
  const sessionCtx = session.context || {};
  const resolved = resolveReferent({
    traces,
    question,
    specialist: intent.specialist,
    domain: sessionCtx.domainId || (input.context && input.context.domainId),
    objective: sessionCtx.objective || (input.context && input.context.objective),
    recentSpecialist:
      (sessionCtx.lastSpecialistEvaluation && sessionCtx.lastSpecialistEvaluation.specialist) ||
      (sessionCtx.lastScoutEvaluation ? 'scout' : null),
  });

  if (resolved.status === 'ambiguous') {
    const prose = formatAmbiguous(resolved.candidates);
    return buildTurn({
      prose,
      reason: 'specialist_trace_ambiguous',
      question,
      intent,
      traces,
    });
  }

  if (resolved.status !== 'resolved' || !resolved.trace) {
    if (intent.specialist && intent.specialist !== 'scout') {
      return buildTurn({
        prose: limitation(intent.specialist),
        reason: 'specialist_trace_unavailable',
        question,
        intent,
      });
    }
    return null;
  }

  const answered = answer({
    trace: resolved.trace,
    question,
    intent,
    currentServiceArea: currentServiceArea(input),
    detectedCompanyCount: detectedCompanyCount(input),
  });

  if (session && session.context && typeof session.context === 'object') {
    session.context.lastCognitiveTraceId = resolved.trace.traceId;
    session.context.lastSpecialistInterrogation = {
      traceId: resolved.trace.traceId,
      specialist: resolved.trace.specialist,
      topic: answered.topic,
    };
  }

  const investigation = resolved.trace.investigation || null;
  const businessEvidence = toBusinessEvidenceRefs(
    (resolved.trace.result && resolved.trace.result.evidenceRefs) || []
  );
  const inspection = specialistDelegation.inspectionSummary
    ? specialistDelegation.inspectionSummary(resolved.trace)
    : null;
  const provenance = buildSystemProvenance({
    delegationId: resolved.trace.delegation && resolved.trace.delegation.id,
    resultId: resolved.trace.raw && resolved.trace.raw.result && resolved.trace.raw.result.id,
    evaluationId:
      resolved.trace.raw && resolved.trace.raw.evaluation && resolved.trace.raw.evaluation.id,
  });
  if (Array.isArray(provenance)) {
    provenance.push({
      id: 'spec_101',
      kind: 'spec',
      label: 'SPEC-101 specialist result interrogation',
    });
  }

  const structured = buildStructuredResponse({
    answer: answered.prose,
    reasoning: [
      'Inspected the existing specialist cognitive trace instead of starting a new investigation.',
      answered.failureBoundary
        ? `Failure boundary: ${answered.failureBoundary.replace(/_/g, ' ')}.`
        : null,
    ].filter(Boolean),
    supportingEvidence: businessEvidence,
    contradictingEvidence: [],
    confidence: 0.86,
    nextInvestigations: [],
    recommendedActions: [{ id: 'acknowledge', type: 'review', label: 'Continue' }],
    confidenceContributors: [],
    timelineReferences: [],
    relatedEntities: [],
    investigation,
    provenance,
    metadata: {
      sourcesUsed: {
        briefing: false,
        reasoning: true,
        memory: true,
        policy: true,
        knowledge: false,
      },
      evidenceCount: businessEvidence.length,
      asOf: new Date().toISOString(),
      unavailable: [],
      interrogation: true,
      specialistRerun: false,
      traceId: resolved.trace.traceId,
      failureBoundary: answered.failureBoundary || null,
      inspection,
    },
  });
  structured.inspection = inspection;

  return {
    reason: 'specialist_cognitive_trace',
    structured,
    prose: answered.prose,
    trace: resolved.trace,
    interrogation: answered,
  };
}

function buildTurn(input) {
  const structured = buildStructuredResponse({
    answer: input.prose,
    reasoning: ['Specialist interrogation without a new investigation.'],
    supportingEvidence: [],
    contradictingEvidence: [],
    confidence: 0.7,
    nextInvestigations: [],
    recommendedActions: [{ id: 'acknowledge', type: 'review', label: 'Continue' }],
    confidenceContributors: [],
    timelineReferences: [],
    relatedEntities: [],
    metadata: {
      sourcesUsed: {
        briefing: false,
        reasoning: true,
        memory: true,
        policy: true,
        knowledge: false,
      },
      evidenceCount: 0,
      asOf: new Date().toISOString(),
      unavailable: [],
      interrogation: true,
      specialistRerun: false,
    },
  });
  return {
    reason: input.reason,
    structured,
    prose: input.prose,
    traces: input.traces || [],
    interrogation: { topic: input.intent && input.intent.topic, rerun: false },
  };
}

module.exports = {
  maybeHandleSpecialistInterrogationTurn,
};
