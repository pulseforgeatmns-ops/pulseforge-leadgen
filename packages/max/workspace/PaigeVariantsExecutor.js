'use strict';

/**
 * Canonical Paige variant generation at STAGES.PREPARE.
 * Consumes Max prioritization and Scout intelligence — never recipient selection.
 */

const amo = require('../../acquisition-mission');
const {
  SPECIALISTS,
  buildExecutionInput,
  createExecutionResult,
  executeSpecialist,
  EXECUTION_STATUSES,
  MESSAGE_BINDING_SCOPES,
} = amo;
const { unwrapSpecialistPayload } = require('../../acquisition-mission/ContributionSupersession');
const {
  canonicalOutboundIdentity,
} = require('./CanonicalOutboundIdentity');
const {
  evaluatePaigePriorLearningInfluence,
  applyPaigePriorLearningAdjustments,
} = require('./PaigePriorLearningInfluence');
const {
  resolveOutreachSequenceAtPrepare,
} = require('../../acquisition-mission/PreparedOutreachSequence');
const {
  buildCustomerFacingVariantCopy,
} = require('./PaigeCustomerCopy');
const {
  validatePaigeVariantsPayload,
  BLOCKER: COPY_SAFETY_BLOCKER,
} = require('./PaigeCopySafety');
const {
  ANCHOR_CLIENT_ID,
  buildAnchorLifecycleVariant,
} = require('../../../utils/anchorLifecycleEmail');
const {
  validateAnchorCopyDoctrine,
  DOCTRINE_BLOCKER,
} = require('../../../utils/anchorCopyDoctrine');

function lookupCrmRecord(crmByProspectId, prospectId) {
  if (!crmByProspectId || prospectId == null) return null;
  if (crmByProspectId instanceof Map) {
    return crmByProspectId.get(String(prospectId)) || null;
  }
  if (typeof crmByProspectId === 'object') {
    return crmByProspectId[String(prospectId)] || crmByProspectId[prospectId] || null;
  }
  return null;
}

function asText(value) {
  if (value == null) return '';
  return String(value).trim();
}

function resolveCandidateCrmRecord(candidate = {}, identity = {}, crmByProspectId = null) {
  const keys = [
    identity.candidateId,
    identity.companyId,
    identity.placeId,
    candidate.prospectId,
    candidate.id,
  ].filter(Boolean);
  for (const key of keys) {
    const row = lookupCrmRecord(crmByProspectId, key);
    if (row) return row;
  }
  return null;
}

function resolveAnchorSenderName(plan = {}, mission = {}) {
  return asText(plan.senderName || mission.senderName || 'Jacob Maynard') || 'Jacob Maynard';
}

/**
 * SPEC-212 — Generate per-prospect variants bound to candidateId.
 * Each variant contains only that prospect's intelligence, never cross-prospect data.
 */
function buildPerProspectVariants(input = {}) {
  const max = input.max || {};
  const scout = input.scout || {};
  const plan = input.plan || {};
  const mission = input.mission || {};
  const clientId = Number(
    input.clientId
    || plan.clientId
    || mission.clientId
    || mission.tenantId
    || 0
  );
  const crmByProspectId = input.crmByProspectId || null;
  const senderName = resolveAnchorSenderName(plan, mission);
  const useAnchorLifecycle = clientId === ANCHOR_CLIENT_ID;

  // SPEC-212: Use ALL ranked targets, not just [0]
  const candidates = max.rankedTargets || [];
  if (!candidates.length && max.priorities?.length) {
    candidates.push(...max.priorities);
  }

  // Fallback if no candidates available
  if (!candidates.length) {
    return buildFallbackMissionLevelVariant(input);
  }

  const variants = [];

  for (const candidate of candidates) {
    const identity = canonicalOutboundIdentity(candidate);
    const candidateId = identity.candidateId || candidate.name;
    const companyName = candidate.name || candidate.label || 'Company';

    // SPEC-212: Extract ONLY this candidate's intelligence — stored for operator review, never in body
    const candidateRationale = candidate.rationale || candidate.reason || null;
    const candidateFit = candidate.fit != null ? Number(candidate.fit) : 0.7;
    const candidateTiming = candidate.timing != null ? Number(candidate.timing) : 0.5;

    let copy;
    let usedPersonalization = false;
    let scoutPersonalization = null;

    if (useAnchorLifecycle) {
      const crmRecord = resolveCandidateCrmRecord(candidate, identity, crmByProspectId);
      const lifecycle = buildAnchorLifecycleVariant({
        candidate: {
          ...candidate,
          name: companyName,
          scoutPersonalization: candidate.scoutPersonalization || candidate.scout_personalization || null,
        },
        crmRecord,
        senderName,
        plan,
        mission,
      });
      copy = {
        subject: lifecycle.subject,
        body: lifecycle.body,
        cta: lifecycle.cta || 'Want me to send over what we\'d need for a quote?',
      };
      usedPersonalization = lifecycle.usedPersonalization;
      scoutPersonalization = lifecycle.evidence || null;
    } else {
      copy = buildCustomerFacingVariantCopy({
        companyName,
        plan,
        mission: input.mission || {},
      });
    }

    variants.push({
      // SPEC-212: Explicit prospect binding — candidate/company/place stay distinct from CRM UUIDs
      candidateId: String(candidateId),
      companyId: identity.companyId || String(candidateId),
      placeId: identity.placeId || null,
      companyName,
      bindingScope: MESSAGE_BINDING_SCOPES.PROSPECT,
      variantId: `paige_v_${String(candidateId).replace(/\W/g, '_')}`,
      label: `Primary - ${companyName}`,
      subject: copy.subject,
      body: copy.body,
      cta: copy.cta,
      // SPEC-212: Store attributable intelligence for this prospect only
      attributableIntelligence: {
        rationale: candidateRationale,
        fit: candidateFit,
        timing: candidateTiming,
        companyName,
        scoutPersonalization,
        usedPersonalization,
      },
    });
  }

  return variants;
}

/**
 * SPEC-212: Fallback mission-level variant if no candidate list.
 * Explicitly marked as non-prospect-specific via bindingScope.
 */
function buildFallbackMissionLevelVariant(input = {}) {
  const max = input.max || {};
  const scout = input.scout || {};
  const plan = input.plan || {};
  const mission = input.mission || {};
  const topTarget = max.rankedTargets?.[0]?.name
    || max.priorities?.[0]?.name
    || scout.companies?.[0]?.name
    || scout.rankedProspects?.[0]?.name
    || plan.market?.label
    || 'your office';
  const copy = buildCustomerFacingVariantCopy({
    companyName: topTarget,
    plan,
    mission,
  });

  return [{
    bindingScope: MESSAGE_BINDING_SCOPES.MISSION,
    variantId: 'paige_v_mission_fallback',
    label: 'Primary - Mission Level',
    subject: copy.subject,
    body: copy.body,
    cta: copy.cta,
    attributableIntelligence: null,
  }];
}

function buildBasePaigeVariantsPayload(input = {}) {
  const variants = buildPerProspectVariants(input);
  const subjects = variants.map((v) => v.subject);
  const max = input.max || {};
  const scout = input.scout || {};
  const usedPersonalization = variants.some((variant) => variant.attributableIntelligence?.usedPersonalization);

  return {
    variants,
    subjects,
    messaging: variants[0]?.body || null,
    cta: Number(input.clientId || plan.clientId || input.mission?.clientId || input.mission?.tenantId) === ANCHOR_CLIENT_ID
      ? (variants[0]?.cta || 'Want me to send over what we\'d need for a quote?')
      : 'Reply to schedule a walkthrough',
    hypotheses: [
      max.objectiveReason || 'Prioritized targets respond to timing-specific outreach.',
      usedPersonalization
        ? 'Scout-supported business facts improve first-touch relevance when evidence is strong.'
        : null,
      scout.buyingSignals?.[0]
        ? `Signal: ${typeof scout.buyingSignals[0] === 'string'
          ? scout.buyingSignals[0]
          : scout.buyingSignals[0].label}`
        : 'Ops hiring signals indicate receptivity window.',
    ].filter(Boolean),
    experiments: [{
      name: 'prospect_binding',
      variant: 'per_prospect_personalized',
      hypothesis: 'Prospect-bound messages with prospect-specific intelligence increase engagement.',
    }],
    bindingScope: MESSAGE_BINDING_SCOPES.PROSPECT,
  };
}

function unwrapMaxPayload(raw = {}) {
  if (!raw || typeof raw !== 'object') return {};
  if ((Array.isArray(raw.rankedTargets) && raw.rankedTargets.length)
    || (Array.isArray(raw.priorities) && raw.priorities.length)) {
    return raw;
  }
  return unwrapSpecialistPayload(raw) || raw;
}

function extractPaigeUpstreamContext(executionInput = {}) {
  const max = unwrapMaxPayload(
    executionInput.workspaceContext?.max
    || executionInput.specialistInput?.maxPrioritization
    || {}
  );
  if (!max.rankedTargets?.length && executionInput.specialistInput?.rankedTargets?.length) {
    max.rankedTargets = executionInput.specialistInput.rankedTargets;
  }
  const scout = executionInput.workspaceContext?.scout
    || executionInput.specialistInput?.scoutDiscovery
    || {};
  const plan = executionInput.missionPlan
    || executionInput.specialistInput?.structuredMission
    || {};
  return { max, scout, plan };
}

function buildPaigeVariantsPayload(executionInput = {}) {
  const { max, scout, plan } = extractPaigeUpstreamContext(executionInput);
  const mission = executionInput.mission || {};
  const priorLearning = executionInput.memoryContext?.priorLearning || [];
  const clientId = Number(
    executionInput.mission?.clientId
    ?? executionInput.mission?.tenantId
    ?? plan.clientId
    ?? 0
  );
  const crmByProspectId = executionInput.crmByProspectId
    || executionInput.specialistInput?.crmByProspectId
    || null;

  let payload = buildBasePaigeVariantsPayload({
    max,
    scout,
    plan,
    clientId,
    crmByProspectId,
    mission: executionInput.mission || {},
  });

  // SPEC-212: Apply prior learning evaluations to all variants
  // Prior learning is mission-level, but we evaluate against each prospect's context
  const priorLearningEvaluation = evaluatePaigePriorLearningInfluence({
    priorLearning,
    max,
    scout,
    plan,
    channel: 'email',
  });

  // TODO: Refactor applyPaigePriorLearningAdjustments to apply per-prospect
  // For now, apply only to first variant to avoid contamination
  payload = applyPaigePriorLearningAdjustments(payload, priorLearningEvaluation, plan);

  const outreachSequence = resolveOutreachSequenceAtPrepare({
    mission: executionInput.mission || {},
    contributions: executionInput.contributions || [],
    clientId: executionInput.mission?.clientId ?? Number(executionInput.mission?.tenantId),
    variants: payload.variants || [],
    crmByProspectId: executionInput.crmByProspectId || executionInput.specialistInput?.crmByProspectId || null,
  });
  if (outreachSequence) {
    payload.outreachSequence = outreachSequence;
  }

  return {
    payload,
    learningInfluence: priorLearningEvaluation.learningInfluence || [],
  };
}

async function runPaigeVariants(executionInput = {}) {
  const transactionId = executionInput.transactionId;
  const { max, scout, plan } = extractPaigeUpstreamContext(executionInput);

  if (!max || !Object.keys(max).length) {
    return createExecutionResult({
      specialist: SPECIALISTS.PAIGE,
      transactionId,
      status: EXECUTION_STATUSES.BLOCKED,
      reason: 'Max prioritization is required before Paige variant generation.',
      requiredPrecondition: 'max_prioritization',
    });
  }

  const { payload, learningInfluence } = buildPaigeVariantsPayload(executionInput);
  const copySafety = validatePaigeVariantsPayload(payload);
  if (!copySafety.safe) {
    return createExecutionResult({
      specialist: SPECIALISTS.PAIGE,
      transactionId,
      status: EXECUTION_STATUSES.BLOCKED,
      contributions: payload,
      reason: 'Paige generated customer-facing copy containing internal mission or scoring language.',
      requiredPrecondition: COPY_SAFETY_BLOCKER,
      blockers: [{
        code: COPY_SAFETY_BLOCKER,
        label: 'Internal reasoning leakage in customer-facing copy',
        violations: copySafety.violations,
      }],
    });
  }

  const clientId = Number(
    executionInput.mission?.clientId
    ?? executionInput.mission?.tenantId
    ?? plan.clientId
    ?? 0
  );
  if (clientId === ANCHOR_CLIENT_ID) {
    const doctrineViolations = [];
    for (const variant of payload.variants || []) {
      const doctrine = validateAnchorCopyDoctrine({
        subject: variant.subject,
        body: variant.body,
        cta: variant.cta,
      });
      if (!doctrine.ok) {
        doctrineViolations.push({
          candidateId: variant.candidateId || variant.companyId || variant.variantId || null,
          violations: doctrine.violations,
        });
      }
    }
    if (doctrineViolations.length) {
      return createExecutionResult({
        specialist: SPECIALISTS.PAIGE,
        transactionId,
        status: EXECUTION_STATUSES.BLOCKED,
        contributions: payload,
        reason: 'Paige generated Anchor copy that violates the Anchor Copy Doctrine.',
        requiredPrecondition: DOCTRINE_BLOCKER,
        blockers: [{
          code: DOCTRINE_BLOCKER,
          label: 'Anchor copy doctrine violation',
          violations: doctrineViolations,
        }],
      });
    }
  }

  const unknowns = [];
  if (executionInput.memoryContext?.priorLearningRetrievalWarning) {
    unknowns.push({
      unknown: 'Prior OutcomeLearning retrieval',
      reason: executionInput.memoryContext.priorLearningRetrievalWarning,
    });
  }

  return createExecutionResult({
    specialist: SPECIALISTS.PAIGE,
    transactionId,
    status: EXECUTION_STATUSES.SUCCESS,
    confidence: { overall: 0.75, evidence: 0.7, fit: 0.8, completeness: 0.75 },
    evidence: [{
      id: 'ev_paige_0',
      label: 'Max prioritization consumed for messaging',
      source: 'max_prioritization',
      timestamp: new Date().toISOString(),
      provenance: { kind: 'upstream_intelligence', source: 'max' },
    }],
    contributions: payload,
    recommendations: [{ tier: 'suggested', text: 'Review variants before operator approval.' }],
    unknowns,
    nextActions: [{ kind: 'operator_review', label: 'Operator review variants' }],
    learningInfluence,
  });
}

async function runPaigeForAmoMission(mission, opts = {}) {
  if (typeof opts.runPaige === 'function') {
    return opts.runPaige(mission, opts);
  }

  const contributions = opts.contributions
    || (opts.engine && opts.engine.inspect(mission.id, { tenantId: opts.tenantId }).contributions)
    || [];

  const executionInput = buildExecutionInput({
    mission,
    contributions,
    specialist: SPECIALISTS.PAIGE,
    transactionId: opts.transactionId,
    executionContext: opts.executionContext,
    store: opts.engine?.store,
  });

  const result = await executeSpecialist({
    specialist: SPECIALISTS.PAIGE,
    mission,
    contributions,
    transactionId: opts.transactionId,
    store: opts.engine?.store,
    run: () => runPaigeVariants({
      ...executionInput,
      mission,
    }),
    treatErrorsAsBlocked: opts.treatErrorsAsBlocked !== false,
  });

  if (result.status === EXECUTION_STATUSES.BLOCKED || result.status === EXECUTION_STATUSES.FAILED) {
    const reason =
      (result.blocked && result.blocked.reason)
      || result.reason
      || 'Paige variant generation did not complete.';
    const err = new Error(reason);
    err.code = 'tme_paige_blocked';
    throw err;
  }

  return result.contributions;
}

function fixturePaigeVariantsResult(mission, contributions = []) {
  const input = buildExecutionInput({
    mission,
    contributions,
    specialist: SPECIALISTS.PAIGE,
    transactionId: 'fixture_paige',
  });
  return buildPaigeVariantsPayload(input).payload;
}

module.exports = {
  buildPerProspectVariants,
  buildFallbackMissionLevelVariant,
  buildBasePaigeVariantsPayload,
  buildPaigeVariantsPayload,
  runPaigeVariants,
  runPaigeForAmoMission,
  fixturePaigeVariantsResult,
  extractPaigeUpstreamContext,
};
