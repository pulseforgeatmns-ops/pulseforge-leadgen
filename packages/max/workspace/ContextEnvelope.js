'use strict';

const { PAGE_TYPES, PAGE_TYPE_SET } = require('./WorkspaceTypes');

/**
 * Normalize and validate MaxContext.
 * Never invents business fields — only coerces shape.
 *
 * @param {object} raw
 * @returns {object}
 */
function normalizeContext(raw) {
  if (!raw || typeof raw !== 'object') {
    throw new Error('MaxContext is required');
  }
  if (raw.tenantId == null || String(raw.tenantId).trim() === '') {
    throw new Error('MaxContext.tenantId is required');
  }

  const page = String(raw.page || PAGE_TYPES.COMMAND_DECK).toLowerCase();
  if (!PAGE_TYPE_SET.has(page)) {
    throw new Error(
      `MaxContext.page must be one of: ${[...PAGE_TYPE_SET].join(', ')}`
    );
  }

  const visibleCards = Array.isArray(raw.visibleCards)
    ? raw.visibleCards.filter((c) => c && typeof c === 'object')
    : [];

  const normalized = {
    page,
    tenantId: String(raw.tenantId),
    companyId:
      raw.companyId != null && String(raw.companyId).trim() !== ''
        ? String(raw.companyId)
        : null,
    recommendationId:
      raw.recommendationId != null && String(raw.recommendationId).trim() !== ''
        ? String(raw.recommendationId)
        : null,
    visibleCards,
    briefing:
      raw.briefing && typeof raw.briefing === 'object' ? raw.briefing : null,
    selectedEntity:
      raw.selectedEntity && typeof raw.selectedEntity === 'object'
        ? {
            id: String(
              raw.selectedEntity.id ||
                raw.selectedEntity.companyId ||
                raw.selectedEntity.recommendationId ||
                ''
            ),
            type: String(raw.selectedEntity.type || 'entity'),
            name: String(
              raw.selectedEntity.name ||
                raw.selectedEntity.companyName ||
                raw.selectedEntity.title ||
                ''
            ),
          }
        : null,
    deck: raw.deck && typeof raw.deck === 'object' ? raw.deck : null,
    asOf:
      (raw.asOf && String(raw.asOf)) ||
      (raw.briefing && raw.briefing.generatedAt) ||
      (raw.deck && raw.deck.meta && raw.deck.meta.generatedAt) ||
      null,
  };

  // Session-level short-lived task memory (not long-term). Preserve when present.
  if (raw.activeWorkContext && typeof raw.activeWorkContext === 'object') {
    normalized.activeWorkContext = raw.activeWorkContext;
  }

  // SPEC-094 / SPEC-095 — preserve durable campaign/objective context.
  // Never invent these; only pass through when the caller supplied them.
  // ContextEnvelope remains a carrier — no SQL/data access here.
  const passthroughKeys = [
    'campaignId',
    'campaign_id',
    'interviewId',
    'interview_id',
    'missionId',
    'mission_id',
    'clientId',
    'client_id',
    'objective',
    'objectiveId',
    'objective_id',
    'learningObjective',
    'learning_objective',
    'topic',
    'audience',
    'channel',
    'campaignPlanning',
    'campaign_planning',
    'campaignMemory',
    'campaign_memory',
    'firstCampaignPlanPreview',
    'first_campaign_plan_preview',
    'outreachStrategyPreview',
    'outreach_strategy_preview',
    'mission',
    'activeObjectives',
    'resolvedObjective',
    'objectiveResolution',
    // SPEC-098 — approved Client Intelligence (carrier only; loaded elsewhere)
    'clientIntelligence',
    'businessBlueprint',
    // SPEC-114 — provisioned tenant workspace + onboarding greeting
    'tenantWorkspace',
    'tenant',
    'tenantName',
    // SPEC-103 — short-lived session cue for advisory follow-ups (Why?)
    'lastClientIntelligenceTurn',
    // SPEC-103C — active conversational reasoning thread (session only)
    'activeClientReasoning',
    // SPEC-098 — specialist delegation evaluation (carrier only)
    'lastSpecialistEvaluation',
    // SPEC-100 — Scout acquisition intelligence loop
    'domainId',
    'acquisitionLoop',
    'lastScoutEvaluation',
    'acquisitionIntelligence',
    'businessContext',
    'targetContext',
    'approvedUnderstanding',
    'operatorDirection',
    // SPEC-096 — specialist direction context
    'action',
    'discussRecommendation',
    'pendingRecommendationId',
    'paigeRecommendation',
    // SPEC-104 — persistent operator context (carrier only; loaded at open)
    'operatorContext',
    'sessionBrief',
    'reviewedBeforeArrival',
  ];
  for (const key of passthroughKeys) {
    if (raw[key] !== undefined) {
      normalized[key] = raw[key];
    }
  }

  return normalized;
}

/**
 * Stable fingerprint for context-switch detection.
 * @param {object} context - normalized MaxContext
 */
function contextFingerprint(context) {
  return [
    context.page,
    context.tenantId,
    context.companyId || '',
    context.recommendationId || '',
    (context.selectedEntity && context.selectedEntity.id) || '',
  ].join('|');
}

/**
 * Human label for the current focus.
 * @param {object} context
 */
function contextFocusLabel(context) {
  if (context.selectedEntity && context.selectedEntity.name) {
    return context.selectedEntity.name;
  }
  if (context.page === PAGE_TYPES.COMPANY && context.companyId) {
    return `company ${context.companyId}`;
  }
  if (context.page === PAGE_TYPES.RECOMMENDATION && context.recommendationId) {
    return `recommendation ${context.recommendationId}`;
  }
  if (context.page === PAGE_TYPES.TIMELINE) return 'the timeline';
  if (context.page === PAGE_TYPES.MARKET) return 'the market view';
  return "today's briefing";
}

module.exports = {
  normalizeContext,
  contextFingerprint,
  contextFocusLabel,
};
