'use strict';

/**
 * Capability Planning — MissionIntent → MissionPlan (SPEC-055 / ADR-039).
 *
 * Deterministic translation of understood intent into executable capabilities.
 * Does not parse natural language — consumes MissionIntent only.
 * For rich multi-sentence campaign-creation requests, reuses IntentParser
 * classification for Notes / Options / Parameters (never for unknown aliases
 * as the primary path).
 */

const { BUILTIN_IDS } = require('../capabilities');
const { MISSION_TYPES } = require('./types');
const {
  INTENT_CATEGORIES,
  INTENT_CONFIDENCE_THRESHOLD,
} = require('./MissionIntent');
const { buildMissionPlan, validateMissionPlan } = require('./MissionPlan');
const { parseIntent } = require('./IntentParser');

/**
 * Deterministic intent → capability / stage mapping.
 * Intents are goals; values are registered capability stage ids.
 */
const INTENT_EXECUTION_MAP = Object.freeze({
  [INTENT_CATEGORIES.CAMPAIGN_EXECUTION]: Object.freeze({
    missionType: MISSION_TYPES.DIRECT_MAIL_EXECUTION,
    execution: Object.freeze([
      {
        stageId: 'direct_mail_execution',
        capabilityId: BUILTIN_IDS.DIRECT_MAIL_EXECUTION,
        label: 'Direct Mail Execution',
      },
    ]),
  }),
  [INTENT_CATEGORIES.CAMPAIGN_REVIEW]: Object.freeze({
    missionType: MISSION_TYPES.CAMPAIGN_REVIEW,
    execution: Object.freeze([
      {
        stageId: 'campaign_review',
        capabilityId: BUILTIN_IDS.CAMPAIGN_REVIEW,
        label: 'Campaign Review',
      },
    ]),
    options: Object.freeze({ review: true, approvalRequired: true }),
  }),
  [INTENT_CATEGORIES.CAMPAIGN_CREATION]: Object.freeze({
    missionType: MISSION_TYPES.CAMPAIGN_CREATION,
    execution: Object.freeze([
      {
        stageId: 'campaign_builder',
        capabilityId: BUILTIN_IDS.CAMPAIGN_BUILDER,
        label: 'Campaign Builder',
      },
    ]),
    options: Object.freeze({ review: true, approvalRequired: true }),
  }),
  [INTENT_CATEGORIES.CAMPAIGN_DIAGNOSTICS]: Object.freeze({
    missionType: MISSION_TYPES.CAMPAIGN_REVIEW,
    execution: Object.freeze([
      {
        stageId: 'campaign_review',
        capabilityId: BUILTIN_IDS.CAMPAIGN_REVIEW,
        label: 'Campaign Review',
      },
      {
        stageId: 'outcome_intelligence',
        capabilityId: BUILTIN_IDS.OUTCOME_INTELLIGENCE,
        label: 'Outcome Intelligence',
      },
    ]),
    options: Object.freeze({ review: true, approvalRequired: true }),
    diagnostics: true,
  }),
  [INTENT_CATEGORIES.DISCOVERY_INVESTIGATION]: Object.freeze({
    missionType: MISSION_TYPES.PROSPECT_DISCOVERY,
    execution: Object.freeze([
      {
        stageId: 'prospect_discovery',
        capabilityId: BUILTIN_IDS.PROSPECT_DISCOVERY,
        label: 'Discovery',
      },
      {
        stageId: 'business_intelligence',
        capabilityId: BUILTIN_IDS.BUSINESS_INTELLIGENCE,
        label: 'Business Intelligence',
      },
      {
        stageId: 'campaign_review',
        capabilityId: BUILTIN_IDS.CAMPAIGN_REVIEW,
        label: 'Campaign Review',
      },
    ]),
    options: Object.freeze({ review: true, approvalRequired: true }),
    diagnostics: true,
  }),
  [INTENT_CATEGORIES.PROSPECT_DISCOVERY]: Object.freeze({
    missionType: MISSION_TYPES.PROSPECT_DISCOVERY,
    execution: Object.freeze([
      {
        stageId: 'prospect_discovery',
        capabilityId: BUILTIN_IDS.PROSPECT_DISCOVERY,
        label: 'Discovery',
      },
    ]),
  }),
  [INTENT_CATEGORIES.GENERATE_MESSAGING]: Object.freeze({
    missionType: MISSION_TYPES.MAIL_PACKAGE_GENERATION,
    execution: Object.freeze([
      {
        stageId: 'sales_intelligence',
        capabilityId: BUILTIN_IDS.SALES_INTELLIGENCE,
        label: 'Sales Intelligence',
      },
      {
        stageId: 'mail_package_generator',
        capabilityId: BUILTIN_IDS.MAIL_PACKAGE_GENERATOR,
        label: 'Mail Package',
      },
    ]),
  }),
  [INTENT_CATEGORIES.BUILD_BUSINESS_INTELLIGENCE]: Object.freeze({
    missionType: MISSION_TYPES.PROSPECT_DISCOVERY,
    execution: Object.freeze([
      {
        stageId: 'business_intelligence',
        capabilityId: BUILTIN_IDS.BUSINESS_INTELLIGENCE,
        label: 'Business Intelligence',
      },
    ]),
  }),
  [INTENT_CATEGORIES.REVIEW_PROSPECT]: Object.freeze({
    missionType: MISSION_TYPES.CAMPAIGN_REVIEW,
    execution: Object.freeze([
      {
        stageId: 'campaign_review',
        capabilityId: BUILTIN_IDS.CAMPAIGN_REVIEW,
        label: 'Campaign Review',
      },
    ]),
    options: Object.freeze({ review: true, approvalRequired: true }),
  }),
  [INTENT_CATEGORIES.GENERATE_PROPOSAL]: Object.freeze({
    missionType: MISSION_TYPES.PROPOSAL_GENERATION,
    execution: Object.freeze([
      {
        stageId: 'proposal_generator',
        capabilityId: BUILTIN_IDS.PROPOSAL_GENERATOR,
        label: 'Proposal Generator',
      },
    ]),
  }),
  [INTENT_CATEGORIES.MAIL_PACKAGE_GENERATION]: Object.freeze({
    missionType: MISSION_TYPES.MAIL_PACKAGE_GENERATION,
    execution: Object.freeze([
      {
        stageId: 'mail_package_generator',
        capabilityId: BUILTIN_IDS.MAIL_PACKAGE_GENERATOR,
        label: 'Mail Package',
      },
    ]),
  }),
  [INTENT_CATEGORIES.EXPORT_CAMPAIGN]: Object.freeze({
    missionType: MISSION_TYPES.CAMPAIGN_REVIEW,
    execution: Object.freeze([
      {
        stageId: 'campaign_review',
        capabilityId: BUILTIN_IDS.CAMPAIGN_REVIEW,
        label: 'Campaign Review',
      },
    ]),
    options: Object.freeze({ review: true }),
  }),
  [INTENT_CATEGORIES.IMPORT_PROSPECT_LIST]: Object.freeze({
    missionType: MISSION_TYPES.PROSPECT_DISCOVERY,
    execution: Object.freeze([
      {
        stageId: 'prospect_discovery',
        capabilityId: BUILTIN_IDS.PROSPECT_DISCOVERY,
        label: 'Discovery',
      },
    ]),
  }),
  [INTENT_CATEGORIES.OUTCOME_INTELLIGENCE]: Object.freeze({
    missionType: MISSION_TYPES.OUTCOME_INTELLIGENCE,
    execution: Object.freeze([
      {
        stageId: 'outcome_intelligence',
        capabilityId: BUILTIN_IDS.OUTCOME_INTELLIGENCE,
        label: 'Outcome Intelligence',
      },
    ]),
  }),
  [INTENT_CATEGORIES.OPERATOR_INBOX]: Object.freeze({
    missionType: MISSION_TYPES.OPERATOR_INBOX,
    execution: Object.freeze([
      {
        stageId: 'operator_inbox',
        capabilityId: BUILTIN_IDS.OPERATOR_INBOX,
        label: 'Operator Inbox',
      },
    ]),
  }),
  [INTENT_CATEGORIES.OPERATOR_HELP]: Object.freeze({
    missionType: MISSION_TYPES.OPERATOR_INBOX,
    execution: Object.freeze([
      {
        stageId: 'operator_inbox',
        capabilityId: BUILTIN_IDS.OPERATOR_INBOX,
        label: 'Operator Inbox',
      },
    ]),
  }),
  [INTENT_CATEGORIES.DIAGNOSTICS]: Object.freeze({
    missionType: MISSION_TYPES.CAMPAIGN_REVIEW,
    execution: Object.freeze([
      {
        stageId: 'campaign_review',
        capabilityId: BUILTIN_IDS.CAMPAIGN_REVIEW,
        label: 'Campaign Review',
      },
    ]),
    options: Object.freeze({ review: true, approvalRequired: true }),
    diagnostics: true,
  }),
});

/**
 * Compile MissionIntent into an executable MissionPlan.
 * @param {object} missionIntent
 * @param {object} [opts]
 * @returns {object} mission_plan (+ missionIntent, missionType, clarification)
 */
function planFromIntent(missionIntent, opts = {}) {
  if (!missionIntent || typeof missionIntent !== 'object') {
    throw new Error('MissionIntent is required for capability planning');
  }

  if (missionIntent.needsClarification) {
    return {
      clarification: true,
      missionIntent,
      missionPlan: null,
      missionType: null,
      suggestedInterpretations: buildSuggestedInterpretations(missionIntent),
    };
  }

  const category =
    missionIntent.intentCategory || missionIntent.matchedIntent;
  const mapping = INTENT_EXECUTION_MAP[category];

  // Rich campaign-creation / multi-sentence: IntentParser fills Notes/Options
  // while Intent Understanding already chose the category (SPEC-050 compat).
  if (
    category === INTENT_CATEGORIES.CAMPAIGN_CREATION &&
    missionIntent.sourceText &&
    shouldUseLegacyParser(missionIntent.sourceText)
  ) {
    const parsed = parseIntent(missionIntent.sourceText, {
      registry: opts.registry || null,
      now: opts.now,
      validate: opts.validate !== false,
    });
    const plan = attachIntent(parsed, missionIntent);
    return {
      clarification: false,
      missionIntent,
      missionPlan: plan,
      missionType: mapping ? mapping.missionType : MISSION_TYPES.CAMPAIGN_CREATION,
      suggestedInterpretations: [],
    };
  }

  if (!mapping) {
    // Unknown with high confidence shouldn't happen; treat as clarification
    return {
      clarification: true,
      missionIntent: {
        ...missionIntent,
        needsClarification: true,
      },
      missionPlan: null,
      missionType: null,
      suggestedInterpretations: buildSuggestedInterpretations(missionIntent),
    };
  }

  const parameters = {
    ...(missionIntent.parameters || {}),
  };
  if (missionIntent.target && missionIntent.target.campaign) {
    parameters.campaign = missionIntent.target.campaign;
  }
  if (missionIntent.target && missionIntent.target.subject) {
    parameters.client = missionIntent.target.subject;
  }

  const options = {
    ...(mapping.options || {}),
    ...(missionIntent.options || {}),
  };
  if (missionIntent.diagnostics || mapping.diagnostics) {
    options.diagnostics = true;
  }

  const subject =
    (missionIntent.target && missionIntent.target.subject) ||
    parameters.client ||
    parameters.subject ||
    null;

  let plan = buildMissionPlan({
    objective: missionIntent.goal || '',
    subject,
    parameters,
    execution: mapping.execution.map((e) => ({ ...e })),
    options,
    notes: [...(missionIntent.notes || [])],
    classifications: [
      {
        text: missionIntent.sourceText || missionIntent.goal,
        category: 'objective',
        detail: {
          objective: missionIntent.goal,
          intentCategory: category,
          confidence: missionIntent.confidence,
          fromIntentUnderstanding: true,
        },
      },
    ],
    sourceText: missionIntent.sourceText,
    createdAt: opts.now || new Date().toISOString(),
  });

  plan = attachIntent(plan, missionIntent);

  if (opts.validate !== false) {
    const validation = validateMissionPlan(plan, opts);
    plan = Object.freeze({
      ...plan,
      validation,
      missionIntent,
    });
    if (!validation.ok && opts.failClosed) {
      const err = new Error(
        `Mission Plan validation failed: ${validation.errors.join('; ')}`
      );
      err.code = 'MISSION_PLAN_INVALID';
      err.validation = validation;
      err.missionPlan = plan;
      err.missionIntent = missionIntent;
      throw err;
    }
  }

  return {
    clarification: false,
    missionIntent,
    missionPlan: plan,
    missionType: mapping.missionType,
    suggestedInterpretations: [],
  };
}

/**
 * Two-stage entry: understand language then plan capabilities.
 * @param {string} text
 * @param {object} [opts]
 * @returns {object}
 */
function planFromOperatorText(text, opts = {}) {
  const { understandIntent } = require('./IntentUnderstanding');
  const missionIntent =
    opts.missionIntent || understandIntent(text, opts);
  return planFromIntent(missionIntent, opts);
}

function attachIntent(plan, missionIntent) {
  return Object.freeze({
    ...plan,
    missionIntent,
  });
}

function shouldUseLegacyParser(sourceText) {
  const text = String(sourceText || '');
  // Multi-sentence campaign builds need Notes / pipeline-through classification
  const units = text
    .split(/(?<=[.!?])\s+|\n+/)
    .map((s) => s.trim())
    .filter(Boolean);
  if (units.length >= 2) return true;
  if (/through\s+sales\s+intelligence/i.test(text)) return true;
  if (/complete\s+pipeline/i.test(text)) return true;
  if (/human\s+test/i.test(text)) return true;
  if (/generated\s+letters?/i.test(text)) return true;
  return false;
}

function buildSuggestedInterpretations(missionIntent) {
  const primary = {
    intent: missionIntent.matchedIntent || missionIntent.intentCategory,
    label: missionIntent.label,
    confidence: missionIntent.confidence,
  };
  const alts = (missionIntent.alternateIntents || []).map((a) => ({
    intent: a.intent || a.category,
    label: a.label,
    confidence: a.confidence,
  }));
  const seen = new Set();
  const out = [];
  for (const item of [primary, ...alts]) {
    if (!item.intent || seen.has(item.intent)) continue;
    if (item.intent === INTENT_CATEGORIES.UNKNOWN && item.confidence < 0.3) {
      continue;
    }
    seen.add(item.intent);
    out.push(item);
  }
  return out;
}

function resolveMissionTypeFromIntent(missionIntent) {
  if (!missionIntent) return null;
  const category =
    missionIntent.intentCategory || missionIntent.matchedIntent;
  const mapping = INTENT_EXECUTION_MAP[category];
  return mapping ? mapping.missionType : null;
}

module.exports = {
  INTENT_EXECUTION_MAP,
  INTENT_CONFIDENCE_THRESHOLD,
  planFromIntent,
  planFromOperatorText,
  resolveMissionTypeFromIntent,
  buildSuggestedInterpretations,
};
