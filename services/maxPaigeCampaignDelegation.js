'use strict';

/**
 * SPEC-094 — Max → Paige Campaign Content Delegation (v1 thin bridge).
 *
 * Max gathers durable campaign/objective context, asks SPEC-093
 * `generateContentRecommendation()`, and presents a review-first
 * recommendation to the operator. Read-only. No publishing, CRM writes,
 * sends, Buffer calls, or account mutation.
 */

const {
  generateContentRecommendation,
  ContentLearningError,
} = require('./contentLearning');

const KIND = 'paige_campaign_content_recommendation';
const SOURCE = 'spec_093_content_learning';

const NEXT_OPTIONS = Object.freeze([
  {
    id: 'accept_direction',
    type: 'review',
    label: 'Accept this direction',
  },
  {
    id: 'revise_direction',
    type: 'review',
    label: 'Revise this direction',
  },
  {
    id: 'ask_another_experiment',
    type: 'review',
    label: 'Ask for another experiment',
  },
  {
    id: 'hold',
    type: 'review',
    label: 'Hold — do not proceed yet',
  },
]);

/** Forbidden autonomy / mutation markers — must never be emitted as true. */
const FORBIDDEN_AUTONOMY_KEYS = Object.freeze([
  'autonomousPublish',
  'autonomous_publish',
  'publish',
  'publishNow',
  'publish_now',
  'send',
  'sendNow',
  'send_now',
  'bufferCall',
  'buffer_call',
  'crmWrite',
  'crm_write',
  'accountMutation',
  'account_mutation',
  'executePublish',
  'execute_publish',
]);

function asText(value) {
  if (value == null) return null;
  const s = String(value).trim();
  return s || null;
}

function asClientId(value) {
  if (value == null || value === '') return null;
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  return Math.trunc(n);
}

function nowIso() {
  return new Date().toISOString();
}

function uniqueIds(ids) {
  return [...new Set((ids || []).map(String).filter(Boolean))];
}

/**
 * Detect operator requests for campaign content / launch runway / Paige.
 * Deterministic keyword gate — not an LLM classifier.
 *
 * @param {string} text
 * @returns {boolean}
 */
function looksLikePaigeCampaignContentRequest(text) {
  const q = String(text || '').trim().toLowerCase();
  if (!q) return false;

  // Explicit Paige ask
  if (/\bask\s+paige\b/.test(q) || /\bpaige\b.*\b(recommend|content|linkedin|experiment)\b/.test(q)) {
    return true;
  }

  const contentCue =
    /\b(content|linkedin|thought\s*leadership|category\s*creation|content\s*experiment|next\s*(post|experiment)|what\s+should\s+(we|i)\s+(post|publish))\b/.test(
      q
    );
  const launchCue =
    /\b(launch\s+runway|public\s+launch|max\s+launch|product\s+launch|reveal\s+max|before\s+(the\s+)?(public\s+)?reveal)\b/.test(
      q
    );
  const recommendCue =
    /\b(recommend|recommendation|suggest|advice|direction|experiment)\b/.test(q);

  if (contentCue && (launchCue || recommendCue || /\bcampaign\b/.test(q))) {
    return true;
  }
  if (launchCue && (contentCue || recommendCue || /\blinkedin\b/.test(q))) {
    return true;
  }
  if (/\bcategory\s*creation\b/.test(q) && (contentCue || recommendCue || launchCue)) {
    return true;
  }
  return false;
}

/**
 * Generic pipeline / status questions must not route to Paige.
 *
 * @param {string} text
 * @returns {boolean}
 */
function looksLikeGenericPipelineQuestion(text) {
  const q = String(text || '').trim().toLowerCase();
  if (!q) return false;
  if (looksLikePaigeCampaignContentRequest(q)) return false;
  return (
    /\b(pipeline|briefing|how\s+many\s+leads|setter\s+queue|what'?s\s+in\s+(the\s+)?pipeline|status\s+update|morning\s+brief)\b/.test(
      q
    ) ||
    /^(what'?s\s+the\s+status|show\s+(me\s+)?(the\s+)?pipeline|any\s+updates)\b/.test(q)
  );
}

/**
 * True when durable campaign/objective context exists or the message
 * itself supplies a campaign/launch objective (without inventing state).
 *
 * @param {object} [context]
 * @param {string} [operatorMessage]
 * @returns {boolean}
 */
function hasCampaignObjectiveContext(context = {}, operatorMessage = '') {
  if (asText(context.campaignId) || asText(context.campaign_id)) return true;
  if (asText(context.interviewId) || asText(context.interview_id)) return true;
  if (asText(context.missionId) || asText(context.mission_id)) return true;
  if (asText(context.objective)) return true;

  const planning = context.campaignPlanning || context.campaign_planning;
  if (planning && typeof planning === 'object') {
    if (asText(planning.objective) || asText(planning.campaignObjective)) return true;
    if (planning.slots && asText(planning.slots.campaign_objective)) return true;
    if (planning.answers && asText(planning.answers.campaign_objective)) return true;
    if (planning.status) return true;
  }

  const preview =
    context.firstCampaignPlanPreview || context.first_campaign_plan_preview;
  if (preview && typeof preview === 'object' && asText(preview.campaignObjective)) {
    return true;
  }

  const outreach =
    context.outreachStrategyPreview || context.outreach_strategy_preview;
  if (outreach && typeof outreach === 'object' && asText(outreach.campaignObjective)) {
    return true;
  }

  const memory = context.campaignMemory || context.campaign_memory;
  if (memory && typeof memory === 'object') {
    if (asText(memory.campaignId) || asText(memory.objective)) return true;
  }

  const mission = context.mission;
  if (mission && typeof mission === 'object') {
    if (asText(mission.objective_text) || asText(mission.objective) || asText(mission.id)) {
      return true;
    }
  }

  const awc = context.activeWorkContext;
  if (awc && typeof awc === 'object') {
    if (awc.target && asText(awc.target.campaignId)) return true;
    if (asText(awc.campaignId)) return true;
  }

  const msg = String(operatorMessage || '');
  if (
    /\b(launch\s+runway|public\s+(max\s+)?launch|max\s+launch|preparing\s+a\s+public|build\s+qualified\s+attention|category\s*creation|reveal\s+max)\b/i.test(
      msg
    )
  ) {
    return true;
  }

  return false;
}

/**
 * Should Max route this turn to Paige content delegation?
 *
 * @param {string} question
 * @param {object} [context]
 * @returns {boolean}
 */
function shouldDelegateToPaige(question, context = {}) {
  if (looksLikeGenericPipelineQuestion(question)) return false;
  if (!looksLikePaigeCampaignContentRequest(question)) return false;
  return hasCampaignObjectiveContext(context, question);
}

function pickChannel(context, operatorMessage) {
  const explicit = asText(context.channel);
  if (explicit) return explicit.toLowerCase();
  const msg = String(operatorMessage || '').toLowerCase();
  if (/\blinkedin\b/.test(msg)) return 'linkedin';
  if (/\bfacebook\b|\bfb\b/.test(msg)) return 'facebook';
  if (/\bgbp\b|google\s+business/.test(msg)) return 'gbp';
  return 'linkedin';
}

function pickTopic(context, operatorMessage) {
  const explicit = asText(context.topic);
  if (explicit) return explicit;
  const msg = String(operatorMessage || '');
  const m =
    msg.match(/\btopic[:\s]+["']?([^"'.\n]{3,80})/i) ||
    msg.match(/\babout\s+["']([^"']{3,80})["']/i);
  return m ? asText(m[1]) : null;
}

function pickAudience(context) {
  return (
    asText(context.audience) ||
    asText(context.audienceType) ||
    asText(context.intendedAudience) ||
    null
  );
}

function pickLearningObjective(context, operatorMessage) {
  const explicit =
    asText(context.learningObjective) || asText(context.learning_objective);
  if (explicit) return explicit;
  const msg = String(operatorMessage || '').toLowerCase();
  if (/\bcategory\s*creation\b/.test(msg)) return 'category_creation';
  if (/\bpartnership\b/.test(msg)) return 'partnership_generation';
  if (/\btalent\b|\bhiring\b/.test(msg)) return 'talent_attraction';
  const preview =
    context.firstCampaignPlanPreview || context.first_campaign_plan_preview;
  if (preview && /\bcategor/.test(String(preview.campaignObjective || '').toLowerCase())) {
    return 'category_creation';
  }
  return 'category_creation';
}

function pickObjective(context, operatorMessage) {
  const explicit = asText(context.objective);
  if (explicit) return explicit;

  const preview =
    context.firstCampaignPlanPreview || context.first_campaign_plan_preview;
  if (preview && asText(preview.campaignObjective)) {
    return asText(preview.campaignObjective);
  }

  const outreach =
    context.outreachStrategyPreview || context.outreach_strategy_preview;
  if (outreach && asText(outreach.campaignObjective)) {
    return asText(outreach.campaignObjective);
  }

  const planning = context.campaignPlanning || context.campaign_planning;
  if (planning) {
    if (asText(planning.objective)) return asText(planning.objective);
    if (asText(planning.campaignObjective)) return asText(planning.campaignObjective);
    if (planning.slots && asText(planning.slots.campaign_objective)) {
      return asText(planning.slots.campaign_objective);
    }
    if (planning.answers && asText(planning.answers.campaign_objective)) {
      return asText(planning.answers.campaign_objective);
    }
  }

  const mission = context.mission;
  if (mission) {
    if (asText(mission.objective_text)) return asText(mission.objective_text);
    if (asText(mission.objective)) return asText(mission.objective);
  }

  const memory = context.campaignMemory || context.campaign_memory;
  if (memory && asText(memory.objective)) return asText(memory.objective);

  const msg = asText(operatorMessage);
  if (msg && msg.length <= 400) return msg;
  if (msg) return msg.slice(0, 400);
  return 'Build qualified attention and category understanding before the public Max reveal.';
}

function pickCampaignId(context) {
  const direct =
    asText(context.campaignId) ||
    asText(context.campaign_id) ||
    null;
  if (direct) return direct;

  const memory = context.campaignMemory || context.campaign_memory;
  if (memory && asText(memory.campaignId)) return asText(memory.campaignId);

  const awc = context.activeWorkContext;
  if (awc) {
    if (awc.target && asText(awc.target.campaignId)) {
      return asText(awc.target.campaignId);
    }
    if (asText(awc.campaignId)) return asText(awc.campaignId);
  }

  const planning = context.campaignPlanning || context.campaign_planning;
  if (planning && asText(planning.campaignId)) return asText(planning.campaignId);

  return null;
}

function pickSource(context) {
  if (context.campaignPlanning || context.firstCampaignPlanPreview) {
    return 'client_intelligence_campaign';
  }
  if (context.mission || context.missionId) return 'mission';
  if (context.activeWorkContext) return 'active_work_context';
  if (context.campaignId || context.campaign_id) return 'explicit_campaign_id';
  return 'operator_message';
}

/**
 * Assemble normalized Paige recommendation request from durable sources.
 * Does not invent campaign state beyond the current request objective.
 *
 * @param {object} input
 * @returns {object|null} request or null when scope/context insufficient
 */
function resolveCampaignContentContext(input = {}) {
  const context = input.context && typeof input.context === 'object' ? input.context : {};
  const operatorMessage = asText(input.operatorMessage || input.question) || '';

  const clientId = asClientId(
    input.clientId ??
      input.tenantId ??
      context.clientId ??
      context.client_id ??
      context.tenantId
  );
  if (clientId == null) {
    return null;
  }

  if (!hasCampaignObjectiveContext(context, operatorMessage)) {
    return null;
  }

  const campaignId = pickCampaignId(context);
  const objective = pickObjective(context, operatorMessage);
  const learningObjective = pickLearningObjective(context, operatorMessage);
  const channel = pickChannel(context, operatorMessage);
  const topic = pickTopic(context, operatorMessage);
  const audience = pickAudience(context);

  const campaignContext = {
    campaignId,
    interviewId: asText(context.interviewId || context.interview_id),
    missionId: asText(
      context.missionId ||
        context.mission_id ||
        (context.mission && context.mission.id)
    ),
    hasCampaignPlanning: Boolean(context.campaignPlanning || context.campaign_planning),
    hasCampaignMemory: Boolean(context.campaignMemory || context.campaign_memory),
    hasFirstCampaignPlanPreview: Boolean(
      context.firstCampaignPlanPreview || context.first_campaign_plan_preview
    ),
    hasOutreachStrategyPreview: Boolean(
      context.outreachStrategyPreview || context.outreach_strategy_preview
    ),
  };

  return {
    tenantId: clientId,
    clientId,
    campaignId,
    source: pickSource(context),
    objective,
    learningObjective,
    topic,
    audience,
    channel,
    campaignContext,
    operatorMessage,
  };
}

/**
 * Normalize SPEC-093 snake_case recommendation into Max-facing camelCase.
 * Preserves raw learning/publication IDs and uncertainty.
 *
 * @param {object} raw
 * @param {object} request
 * @returns {object}
 */
function normalizePaigeRecommendation(raw, request = {}) {
  const supportingLearningIds = uniqueIds(
    raw.supportingLearningIds ||
      raw.supporting_learning_ids ||
      (raw.experiment && raw.experiment.supporting_learning_ids) ||
      []
  );
  const supportingPublicationIds = uniqueIds(
    raw.supportingPublicationIds || raw.supporting_publication_ids || []
  );

  const experiment = raw.experiment
    ? {
        hypothesis: raw.experiment.hypothesis || null,
        objective: raw.experiment.objective || null,
        preserve: Array.isArray(raw.experiment.preserve)
          ? raw.experiment.preserve.map(String)
          : [],
        vary: Array.isArray(raw.experiment.vary)
          ? raw.experiment.vary.map(String)
          : [],
        nextArgument:
          raw.experiment.nextArgument || raw.experiment.next_argument || null,
        expectedSignal:
          raw.experiment.expectedSignal ||
          raw.experiment.expected_signal ||
          [],
        failureSignal:
          raw.experiment.failureSignal || raw.experiment.failure_signal || [],
        supportingLearningIds: uniqueIds(
          raw.experiment.supportingLearningIds ||
            raw.experiment.supporting_learning_ids ||
            supportingLearningIds
        ),
      }
    : null;

  const payload = {
    kind: KIND,
    campaignId:
      asText(raw.campaignId) ||
      asText(raw.campaign_id) ||
      asText(request.campaignId) ||
      null,
    objective: asText(raw.objective) || asText(request.objective),
    recommendedDirection:
      asText(raw.recommendedDirection) ||
      asText(raw.recommended_direction) ||
      null,
    reason: asText(raw.reason) || '',
    confidence:
      raw.confidence == null || !Number.isFinite(Number(raw.confidence))
        ? null
        : Number(raw.confidence),
    uncertainties: Array.isArray(raw.uncertainties)
      ? raw.uncertainties.map(String)
      : [],
    experiment,
    supportingLearningIds,
    supportingPublicationIds,
    alternatives: Array.isArray(raw.alternatives)
      ? raw.alternatives.map((a) => ({
          direction: asText(a.direction) || asText(a.recommended_direction),
          reason: asText(a.reason) || '',
        }))
      : [],
    learnings: Array.isArray(raw.learnings) ? raw.learnings : undefined,
    source: SOURCE,
    generatedAt: asText(raw.generatedAt || raw.generated_at) || nowIso(),
    tenantId: request.tenantId ?? request.clientId ?? null,
    clientId: request.clientId ?? request.tenantId ?? null,
    channel: request.channel || null,
    topic: request.topic || null,
    audience: request.audience || null,
    learningObjective: request.learningObjective || null,
    operatorAuthority: true,
    autonomousPublish: false,
    autonomousStrategyMutation: false,
    reviewFirst: true,
    nextOptions: NEXT_OPTIONS.map((o) => ({ ...o })),
  };

  assertRecommendationIsAdvisoryOnly(payload);
  return payload;
}

/**
 * Guard: recommendation payloads must remain advisory (no publish/send/CRM).
 *
 * @param {object} payload
 */
function assertRecommendationIsAdvisoryOnly(payload) {
  if (!payload || typeof payload !== 'object') {
    throw new ContentLearningError(
      'invalid_recommendation',
      'recommendation payload required'
    );
  }
  for (const key of FORBIDDEN_AUTONOMY_KEYS) {
    if (payload[key] === true) {
      throw new ContentLearningError(
        'autonomy_forbidden',
        `Recommendation must not set ${key}=true`,
        500
      );
    }
  }
  if (payload.autonomousPublish !== false && payload.autonomousPublish != null) {
    throw new ContentLearningError(
      'autonomy_forbidden',
      'autonomousPublish must be false',
      500
    );
  }
}

/**
 * Max-facing operator prose. Paige recommends; Max speaks; nothing executes.
 *
 * @param {object} payload
 * @returns {string}
 */
function formatMaxPaigeCampaignRecommendation(payload) {
  const parts = [];
  parts.push(
    'Paige is recommending a content experiment direction — not publishing or executing anything.'
  );

  if (payload.recommendedDirection) {
    parts.push(`Recommended direction: ${payload.recommendedDirection}`);
  }
  if (payload.reason) {
    parts.push(`Rationale: ${payload.reason}`);
  }
  if (payload.confidence != null) {
    parts.push(`Confidence: ${payload.confidence}.`);
  }
  if (payload.uncertainties && payload.uncertainties.length) {
    parts.push(`Uncertainty: ${payload.uncertainties.join(' ')}`);
  }

  const learningIds = payload.supportingLearningIds || [];
  const pubIds = payload.supportingPublicationIds || [];
  if (learningIds.length || pubIds.length) {
    const evidenceBits = [];
    if (learningIds.length) {
      evidenceBits.push(`learnings ${learningIds.join(', ')}`);
    }
    if (pubIds.length) {
      evidenceBits.push(`publications ${pubIds.join(', ')}`);
    }
    parts.push(`Evidence basis: ${evidenceBits.join('; ')}.`);
  } else {
    parts.push(
      'Evidence basis: no durable learnings/publications cited yet for this scope.'
    );
  }

  if (payload.experiment && payload.experiment.hypothesis) {
    parts.push(`Experiment hypothesis: ${payload.experiment.hypothesis}`);
  }

  parts.push(
    'Next options (review-first): accept direction; revise direction; ask for another experiment; hold.'
  );
  parts.push(
    'Nothing will be published, sent, written to CRM, or pushed to Buffer until you decide.'
  );

  return parts.join(' ');
}

/**
 * Structured Max workspace response for the delegation turn.
 *
 * @param {object} payload
 * @returns {object}
 */
function composeMaxPaigeCampaignStructuredResponse(payload) {
  const answer = formatMaxPaigeCampaignRecommendation(payload);
  const supportingEvidence = (payload.supportingLearningIds || []).map((id) => ({
    id: String(id),
    summary: `SPEC-093 content learning ${id}`,
    sourceType: 'content_learning',
    kind: 'content_learning',
  }));
  for (const id of payload.supportingPublicationIds || []) {
    supportingEvidence.push({
      id: String(id),
      summary: `SPEC-092 content publication ${id}`,
      sourceType: 'content_publication',
      kind: 'content_publication',
    });
  }

  return {
    answer,
    reasoning: [
      'Max delegated campaign content recommendation to Paige (SPEC-093) using durable campaign/objective context.',
      'Paige recommendation is advisory only — operator authority remains absolute.',
      ...(payload.uncertainties || []).map((u) => `Uncertainty: ${u}`),
    ],
    supportingEvidence,
    contradictingEvidence: [],
    confidence: payload.confidence,
    nextInvestigations: (payload.nextOptions || []).map((o) => o.label),
    recommendedActions: (payload.nextOptions || []).map((o) => ({
      id: o.id,
      type: o.type || 'review',
      label: o.label,
      payload: {
        kind: KIND,
        campaignId: payload.campaignId,
        reviewFirst: true,
        autonomousPublish: false,
      },
    })),
    confidenceContributors: [
      'spec_093_content_learning',
      'spec_092_content_outcomes',
    ],
    timelineReferences: [],
    relatedEntities: [],
    metadata: {
      sourcesUsed: {
        briefing: false,
        reasoning: true,
        memory: true,
        policy: false,
        knowledge: false,
      },
      evidenceCount: supportingEvidence.length,
      asOf: payload.generatedAt || nowIso(),
      unavailable: supportingEvidence.length
        ? []
        : ['content_learnings', 'content_publications'],
      executionDomain: 'workspace',
      surface: 'paige_campaign_content_delegation',
      paigeCampaignRecommendation: true,
      autonomousPublish: false,
      reviewFirst: true,
    },
    paigeRecommendation: payload,
  };
}

/**
 * Delegate a campaign content recommendation to Paige.
 *
 * @param {object} input
 * @param {object} [opts] - forwarded to contentLearning (stores, etc.)
 * @returns {Promise<{ ok: boolean, skipped?: boolean, reason?: string, request?: object, recommendation?: object, structured?: object, prose?: string }>}
 */
async function delegateCampaignContentRecommendation(input = {}, opts = {}) {
  const question = asText(input.operatorMessage || input.question) || '';
  const context = input.context && typeof input.context === 'object' ? input.context : {};

  if (!shouldDelegateToPaige(question, { ...context, ...input })) {
    return {
      ok: false,
      skipped: true,
      reason: looksLikeGenericPipelineQuestion(question)
        ? 'generic_pipeline_question'
        : !looksLikePaigeCampaignContentRequest(question)
          ? 'not_content_request'
          : 'missing_campaign_context',
    };
  }

  const request = resolveCampaignContentContext({
    ...input,
    context: { ...context, ...input },
    operatorMessage: question,
  });

  if (!request) {
    return {
      ok: false,
      skipped: true,
      reason: 'missing_campaign_context',
    };
  }

  const generate =
    typeof opts.generateContentRecommendation === 'function'
      ? opts.generateContentRecommendation
      : generateContentRecommendation;

  const raw = await generate(
    {
      tenantId: request.tenantId,
      clientId: request.clientId,
      objective: request.objective,
      learningObjective: request.learningObjective,
      topic: request.topic,
      audience: request.audience,
      channel: request.channel,
      campaignId: request.campaignId,
    },
    opts
  );

  const recommendation = normalizePaigeRecommendation(raw, request);
  assertRecommendationIsAdvisoryOnly(recommendation);

  const structured = composeMaxPaigeCampaignStructuredResponse(recommendation);
  const prose = formatMaxPaigeCampaignRecommendation(recommendation);

  return {
    ok: true,
    skipped: false,
    request,
    recommendation,
    structured,
    prose,
    source: SOURCE,
    kind: KIND,
  };
}

module.exports = {
  KIND,
  SOURCE,
  NEXT_OPTIONS,
  FORBIDDEN_AUTONOMY_KEYS,
  ContentLearningError,
  looksLikePaigeCampaignContentRequest,
  looksLikeGenericPipelineQuestion,
  hasCampaignObjectiveContext,
  shouldDelegateToPaige,
  resolveCampaignContentContext,
  normalizePaigeRecommendation,
  assertRecommendationIsAdvisoryOnly,
  formatMaxPaigeCampaignRecommendation,
  composeMaxPaigeCampaignStructuredResponse,
  delegateCampaignContentRecommendation,
};
