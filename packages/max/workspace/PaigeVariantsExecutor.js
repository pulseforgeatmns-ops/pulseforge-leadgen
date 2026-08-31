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
const {
  evaluatePaigePriorLearningInfluence,
  applyPaigePriorLearningAdjustments,
} = require('./PaigePriorLearningInfluence');

function asText(value) {
  if (value == null) return '';
  return String(value).trim();
}

/**
 * SPEC-212 — Generate per-prospect variants bound to candidateId.
 * Each variant contains only that prospect's intelligence, never cross-prospect data.
 */
function buildPerProspectVariants(input = {}) {
  const max = input.max || {};
  const scout = input.scout || {};
  const plan = input.plan || {};

  // SPEC-212: Use ALL ranked targets, not just [0]
  const candidates = max.rankedTargets || [];
  if (!candidates.length && max.priorities?.length) {
    candidates.push(...max.priorities);
  }

  // Fallback if no candidates available
  if (!candidates.length) {
    return buildFallbackMissionLevelVariant(input);
  }

  const objective = max.objectives?.[0]?.text || plan.objective || null;
  const marketLabel = plan.market?.label || 'local offices';
  const variants = [];

  for (const candidate of candidates) {
    const candidateId = candidate.id || candidate.companyId || candidate.name;
    const companyName = candidate.name || candidate.label || 'Company';

    // SPEC-212: Extract ONLY this candidate's intelligence
    const candidateRationale = candidate.rationale || candidate.reason || null;
    const candidateFit = candidate.fit != null ? Number(candidate.fit) : 0.7;
    const candidateTiming = candidate.timing != null ? Number(candidate.timing) : 0.5;

    // SPEC-212: Build prospect-specific subject and body
    const subject = `Commercial cleaning walkthrough for ${companyName}`;
    const body = [
      `Hi — we help ${marketLabel} maintain spotless workspaces.`,
      objective ? `Mission focus: ${objective}` : null,
      candidateRationale ? `Why now: ${candidateRationale}` : null,
    ].filter(Boolean).join('\n\n');

    variants.push({
      // SPEC-212: Explicit prospect binding
      candidateId: String(candidateId),
      companyName,
      bindingScope: MESSAGE_BINDING_SCOPES.PROSPECT,
      variantId: `paige_v_${String(candidateId).replace(/\W/g, '_')}`,
      label: `Primary - ${companyName}`,
      subject,
      body,
      cta: 'Reply to schedule a walkthrough',
      // SPEC-212: Store attributable intelligence for this prospect only
      attributableIntelligence: {
        rationale: candidateRationale,
        fit: candidateFit,
        timing: candidateTiming,
        companyName,
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
  const topTarget = max.rankedTargets?.[0]?.name
    || max.priorities?.[0]?.name
    || scout.companies?.[0]?.name
    || scout.rankedProspects?.[0]?.name
    || plan.market?.label
    || 'your office';
  const objective = max.objectives?.[0]?.text || plan.objective || null;
  const subject = `Commercial cleaning walkthrough for ${topTarget}`;
  const body = [
    `Hi — we help ${plan.market?.label || 'local offices'} maintain spotless workspaces.`,
    objective ? `Mission focus: ${objective}` : null,
    max.recommendations?.[0] ? `Why now: ${max.recommendations[0]}` : null,
  ].filter(Boolean).join('\n\n');

  return [{
    bindingScope: MESSAGE_BINDING_SCOPES.MISSION,
    variantId: 'paige_v_mission_fallback',
    label: 'Primary - Mission Level',
    subject,
    body,
    cta: 'Reply to schedule a walkthrough',
    attributableIntelligence: null,
  }];
}

function buildBasePaigeVariantsPayload(input = {}) {
  const variants = buildPerProspectVariants(input);
  const subjects = variants.map((v) => v.subject);
  const max = input.max || {};
  const scout = input.scout || {};

  return {
    variants,
    subjects,
    cta: 'Reply to schedule a walkthrough',
    hypotheses: [
      max.objectiveReason || 'Prioritized targets respond to timing-specific outreach.',
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

function extractPaigeUpstreamContext(executionInput = {}) {
  const max = executionInput.workspaceContext?.max
    || executionInput.specialistInput?.maxPrioritization
    || {};
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
  const priorLearning = executionInput.memoryContext?.priorLearning || [];

  let payload = buildBasePaigeVariantsPayload({ max, scout, plan });

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
