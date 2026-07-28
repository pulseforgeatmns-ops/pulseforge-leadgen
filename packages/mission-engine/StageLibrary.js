'use strict';

/**
 * Stage Library — registered Mission stages (SPEC-041 / ADR-027).
 * Planner selects from this library; never hardcodes stage order.
 * Order comes from dependency resolution.
 */

const { BUILTIN_IDS } = require('../capabilities');
const { MISSION_TYPES, STAGE_LABELS } = require('./types');

/** SPEC-051 bumps planner when artifact resolution prunes acquisition stages. */
const PLANNER_VERSION = '1.1.0';

/**
 * @typedef {object} StageDef
 * @property {string} id
 * @property {string} name
 * @property {string|null} capabilityId - null = planner-managed gate (no execute)
 * @property {string[]} consumes
 * @property {string[]} produces
 * @property {string[]} dependencies - stage ids that must complete first
 * @property {boolean} reviewRequired
 * @property {number} priority - lower runs earlier among equal-dep peers
 * @property {RegExp[]} [outcomePatterns] - objective keywords that select this stage
 */

/** @type {Record<string, StageDef>} */
const STAGE_LIBRARY = Object.freeze({
  prospect_discovery: Object.freeze({
    id: 'prospect_discovery',
    name: 'Discovery',
    capabilityId: BUILTIN_IDS.PROSPECT_DISCOVERY,
    consumes: ['discovery_profile'],
    produces: ['prospect_list'],
    dependencies: [],
    reviewRequired: false,
    priority: 10,
    outcomePatterns: [
      /\bdiscover(y|ing)?\b/i,
      /\bprospect\s+discovery\b/i,
      /\bfind\s+.+\s+prospects?\b/i,
    ],
  }),
  company_enrichment: Object.freeze({
    id: 'company_enrichment',
    name: 'Company Enrichment',
    capabilityId: BUILTIN_IDS.COMPANY_ENRICHMENT,
    consumes: ['prospect_list'],
    produces: ['enriched_list', 'company_intelligence'],
    dependencies: ['prospect_discovery'],
    reviewRequired: false,
    priority: 20,
    outcomePatterns: [
      /\bcompany\s+enrichment\b/i,
      /\benrich(ment|ing)?\b/i,
      /\benrich(ed)?\s+compan(y|ies)\b/i,
    ],
  }),
  knowledge_update: Object.freeze({
    id: 'knowledge_update',
    name: 'Knowledge Update',
    capabilityId: BUILTIN_IDS.KNOWLEDGE_UPDATE,
    consumes: [],
    produces: ['knowledge'],
    dependencies: [],
    reviewRequired: false,
    priority: 25,
    outcomePatterns: [
      /\bknowledge\s+(update|refresh)\b/i,
      /\brefresh\s+knowledge\b/i,
      /\bweekly\s+brief\b/i,
    ],
  }),
  opportunity_ranking: Object.freeze({
    id: 'opportunity_ranking',
    name: 'Opportunity Ranking',
    capabilityId: BUILTIN_IDS.OPPORTUNITY_RANKING,
    consumes: ['prospect_list', 'company_intelligence'],
    produces: ['ranked_prospects'],
    dependencies: ['prospect_discovery'],
    reviewRequired: false,
    priority: 30,
    outcomePatterns: [
      /\brank(ing|ed)?\b/i,
      /\bopportunity\s+ranking\b/i,
      /\bwho\s+should\s+we\s+contact\b/i,
    ],
  }),
  business_intelligence: Object.freeze({
    id: 'business_intelligence',
    name: 'Business Intelligence',
    capabilityId: BUILTIN_IDS.BUSINESS_INTELLIGENCE,
    consumes: ['ranked_prospects'],
    produces: ['business_intelligence_profile'],
    dependencies: ['opportunity_ranking'],
    reviewRequired: false,
    priority: 33,
    outcomePatterns: [
      /\bbusiness\s+intelligence\b/i,
      /\bbusiness\s+profile\b/i,
      /\breason\s+about\s+(the\s+)?business\b/i,
      /\bcompany\s+intelligence\b/i,
      /\banalyze\s+(the\s+)?compan(y|ies)\b/i,
      /\bgenerate\s+(company\s+)?intelligence\b/i,
    ],
  }),
  sales_intelligence: Object.freeze({
    id: 'sales_intelligence',
    name: 'Sales Intelligence',
    capabilityId: BUILTIN_IDS.SALES_INTELLIGENCE,
    consumes: ['ranked_prospects', 'business_intelligence_profile'],
    produces: ['sales_intelligence_profile'],
    dependencies: ['business_intelligence'],
    reviewRequired: false,
    priority: 35,
    outcomePatterns: [
      /\bsales\s+intelligence\b/i,
      /\bmessaging\s+strategy\b/i,
      /\bsales\s+profile\b/i,
      /\bstrategy\s+before\s+language\b/i,
    ],
  }),
  campaign_builder: Object.freeze({
    id: 'campaign_builder',
    name: 'Campaign Builder',
    capabilityId: BUILTIN_IDS.CAMPAIGN_BUILDER,
    consumes: ['ranked_prospects', 'sales_intelligence_profile'],
    produces: ['campaign'],
    dependencies: ['opportunity_ranking'],
    reviewRequired: false,
    priority: 40,
    outcomePatterns: [
      /\bbuild\s+(a\s+)?campaign\b/i,
      /\bcreate\s+(a\s+)?campaign\b/i,
      /\bcampaign\s+builder\b/i,
      /\bprepare\s+(a\s+)?campaign\b/i,
    ],
  }),
  mail_package_generator: Object.freeze({
    id: 'mail_package_generator',
    name: 'Mail Package',
    capabilityId: BUILTIN_IDS.MAIL_PACKAGE_GENERATOR,
    consumes: ['campaign', 'sales_intelligence_profile'],
    produces: ['mail_packages'],
    dependencies: ['campaign_builder'],
    reviewRequired: false,
    priority: 50,
    outcomePatterns: [
      /\bmail\s+packages?\b/i,
      /\bdirect\s+mail\s+packages?\b/i,
      /\bmail\s+merge\b/i,
      /\baddress\s+labels?\b/i,
      /\bgenerate\s+(a\s+)?(mail|direct\s*mail)\s+packages?\b/i,
    ],
  }),
  campaign_review: Object.freeze({
    id: 'campaign_review',
    name: 'Campaign Review',
    capabilityId: BUILTIN_IDS.CAMPAIGN_REVIEW,
    consumes: ['campaign', 'mail_packages'],
    produces: ['approved_campaign', 'review_decision'],
    dependencies: ['campaign_builder'],
    reviewRequired: true,
    priority: 60,
    outcomePatterns: [
      /\bcampaign\s+review\b/i,
      /\breview\s+(the\s+)?campaign\b/i,
      /\bpause\s+at\s+review\b/i,
      /\bapprove\s+(the\s+)?campaign\b/i,
      /\bcampaign\s+approval\b/i,
      // SPEC-050: bare "Review" is an Options flag (via IntentParser), not a stage
      // keyword on free-form guidance like "Review Human Test results".
      /(?:^|[.]\s*)review(?:\s*[.]|$)/i,
    ],
  }),
  ready_to_print: Object.freeze({
    id: 'ready_to_print',
    name: 'Ready To Print',
    /** Planner-managed gate — ensures review produces print package; no separate capability */
    capabilityId: null,
    consumes: ['approved_campaign', 'mail_packages'],
    produces: ['ready_to_print_package'],
    dependencies: ['campaign_review'],
    reviewRequired: true,
    priority: 70,
    outcomePatterns: [
      /\bready\s+to\s+print\b/i,
      /\bprint[- ]ready\b/i,
      /\bproduce\s+ready\s+to\s+print\b/i,
    ],
  }),
  proposal_generator: Object.freeze({
    id: 'proposal_generator',
    name: 'Proposal Generator',
    capabilityId: BUILTIN_IDS.PROPOSAL_GENERATOR,
    consumes: ['discovery_summary'],
    produces: ['proposal'],
    dependencies: [],
    reviewRequired: true,
    priority: 40,
    outcomePatterns: [
      /\b(generate|create|draft|write|build)\s+(a\s+)?(sales\s+)?proposal\b/i,
      /\bproposal\s+for\b/i,
      /\bcommercial\s+growth\s+proposal\b/i,
    ],
  }),
  direct_mail_execution: Object.freeze({
    id: 'direct_mail_execution',
    name: 'Direct Mail Execution',
    capabilityId: BUILTIN_IDS.DIRECT_MAIL_EXECUTION,
    consumes: ['ready_to_print_package', 'approved_campaign'],
    produces: ['execution_record'],
    dependencies: ['campaign_review'],
    reviewRequired: false,
    priority: 80,
    outcomePatterns: [
      /\bdirect\s+mail\s+execution\b/i,
      /\bexecute\s+(the\s+)?(direct\s+)?mail\b/i,
      /\bprint\s+(and\s+)?mail\b/i,
      /\bmark\s+(all\s+)?mailed\b/i,
    ],
  }),
  outcome_intelligence: Object.freeze({
    id: 'outcome_intelligence',
    name: 'Outcome Intelligence',
    capabilityId: BUILTIN_IDS.OUTCOME_INTELLIGENCE,
    consumes: ['execution_record'],
    produces: ['outcome_summary'],
    dependencies: [],
    reviewRequired: true,
    priority: 90,
    outcomePatterns: [
      /\boutcome\s+intelligence\b/i,
      /\bcapture\s+(campaign\s+)?outcomes?\b/i,
      /\bcampaign\s+outcomes?\b/i,
      /\blearnings?\s+from\s+(the\s+)?campaign\b/i,
    ],
  }),
  operator_inbox: Object.freeze({
    id: 'operator_inbox',
    name: 'Operator Inbox',
    capabilityId: BUILTIN_IDS.OPERATOR_INBOX,
    consumes: [],
    produces: ['inbox_items'],
    dependencies: [],
    reviewRequired: false,
    priority: 5,
    outcomePatterns: [
      /\boperator\s+inbox\b/i,
      /\bshow\s+(my\s+)?inbox\b/i,
      /\bopen\s+(the\s+)?inbox\b/i,
      /\boutstanding\s+work\b/i,
    ],
  }),
});

/**
 * Seed stages for a mission type — starting set before keyword augmentation.
 * Planner composes; seeds never replace keyword-selected stages.
 * @type {Record<string, string[]>}
 */
const TYPE_SEED_STAGES = Object.freeze({
  [MISSION_TYPES.CAMPAIGN_CREATION]: [
    'prospect_discovery',
    'company_enrichment',
    'knowledge_update',
    'opportunity_ranking',
    'business_intelligence',
    'sales_intelligence',
    'campaign_builder',
  ],
  [MISSION_TYPES.PROSPECT_DISCOVERY]: [
    'prospect_discovery',
    'company_enrichment',
    'knowledge_update',
    'opportunity_ranking',
    'business_intelligence',
    'sales_intelligence',
  ],
  [MISSION_TYPES.OVERFLOW_PARTNER_SEARCH]: [
    'prospect_discovery',
    'company_enrichment',
    'knowledge_update',
    'opportunity_ranking',
    'business_intelligence',
    'sales_intelligence',
  ],
  [MISSION_TYPES.ACQUISITION_SEARCH]: [
    'prospect_discovery',
    'company_enrichment',
    'knowledge_update',
    'opportunity_ranking',
    'business_intelligence',
    'sales_intelligence',
  ],
  [MISSION_TYPES.COMPETITOR_RESEARCH]: [
    'prospect_discovery',
    'knowledge_update',
    'opportunity_ranking',
  ],
  [MISSION_TYPES.MARKET_RESEARCH]: [
    'prospect_discovery',
    'knowledge_update',
    'opportunity_ranking',
  ],
  [MISSION_TYPES.WEEKLY_BRIEF]: ['knowledge_update'],
  [MISSION_TYPES.KNOWLEDGE_REFRESH]: ['knowledge_update'],
  [MISSION_TYPES.PROPOSAL_GENERATION]: ['proposal_generator'],
  [MISSION_TYPES.MAIL_PACKAGE_GENERATION]: ['mail_package_generator'],
  [MISSION_TYPES.CAMPAIGN_REVIEW]: ['campaign_review'],
  [MISSION_TYPES.DIRECT_MAIL_EXECUTION]: ['direct_mail_execution'],
  [MISSION_TYPES.OUTCOME_INTELLIGENCE]: ['outcome_intelligence'],
  [MISSION_TYPES.OPERATOR_INBOX]: ['operator_inbox'],
});

/**
 * Soft deps: when both ends are selected, enforce edge (may already be in stage.dependencies).
 * Used to insert optional mid-pipeline edges (e.g. mail before review when both present).
 */
const COMPOSITION_EDGES = Object.freeze([
  Object.freeze({
    from: 'mail_package_generator',
    to: 'campaign_review',
    reason: 'Mail packages should be ready before Campaign Review when both are requested',
  }),
  Object.freeze({
    from: 'mail_package_generator',
    to: 'ready_to_print',
    reason: 'Ready To Print consumes mail packages when present',
  }),
  Object.freeze({
    from: 'company_enrichment',
    to: 'opportunity_ranking',
    reason: 'Ranking prefers Company Enrichment when both are selected',
  }),
  Object.freeze({
    from: 'opportunity_ranking',
    to: 'business_intelligence',
    reason: 'Business Intelligence consumes ranked prospects when both are selected',
  }),
  Object.freeze({
    from: 'business_intelligence',
    to: 'sales_intelligence',
    reason: 'Sales Intelligence consumes Business Intelligence when both are selected',
  }),
  Object.freeze({
    from: 'opportunity_ranking',
    to: 'sales_intelligence',
    reason: 'Sales Intelligence consumes ranked prospects when both are selected',
  }),
  Object.freeze({
    from: 'sales_intelligence',
    to: 'campaign_builder',
    reason: 'Campaign Builder consumes Sales Intelligence when both are selected',
  }),
  Object.freeze({
    from: 'sales_intelligence',
    to: 'mail_package_generator',
    reason: 'Mail packages prefer Sales Intelligence messaging when both are selected',
  }),
  Object.freeze({
    from: 'knowledge_update',
    to: 'opportunity_ranking',
    reason: 'Knowledge update precedes ranking when both are selected',
  }),
  Object.freeze({
    from: 'company_enrichment',
    to: 'knowledge_update',
    reason: 'Enrichment feeds knowledge when both are selected',
  }),
]);

/**
 * @param {string} stageId
 * @returns {StageDef|null}
 */
function getStage(stageId) {
  return STAGE_LIBRARY[stageId] || null;
}

/**
 * @returns {StageDef[]}
 */
function listStages() {
  return Object.values(STAGE_LIBRARY).map((s) => ({ ...s }));
}

/**
 * @param {string} missionType
 * @returns {string[]}
 */
function seedStagesForType(missionType) {
  const seed = TYPE_SEED_STAGES[missionType];
  return seed ? [...seed] : [];
}

/**
 * Match objective text against stage outcome patterns.
 * @param {string} objective
 * @returns {{ stageId: string, reason: string }[]}
 */
function matchOutcomeStages(objective) {
  const text = String(objective || '');
  /** @type {{ stageId: string, reason: string }[]} */
  const hits = [];
  for (const stage of Object.values(STAGE_LIBRARY)) {
    const patterns = stage.outcomePatterns || [];
    for (const re of patterns) {
      if (re.test(text)) {
        hits.push({
          stageId: stage.id,
          reason: `Objective matched stage keyword for ${stage.name}`,
        });
        break;
      }
    }
  }
  return hits;
}

/**
 * Operator-facing label for a stage.
 * @param {string} stageId
 */
function stageLabel(stageId) {
  const stage = getStage(stageId);
  if (stage) return stage.name;
  return STAGE_LABELS[stageId] || stageId;
}

module.exports = {
  PLANNER_VERSION,
  STAGE_LIBRARY,
  TYPE_SEED_STAGES,
  COMPOSITION_EDGES,
  getStage,
  listStages,
  seedStagesForType,
  matchOutcomeStages,
  stageLabel,
};
