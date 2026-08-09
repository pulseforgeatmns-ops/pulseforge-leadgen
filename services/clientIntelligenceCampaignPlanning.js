'use strict';

/**
 * SPEC-089 — First Campaign Planning Conversation.
 *
 * After Growth Plan completion, Max helps define the first campaign hypothesis
 * and validation gates. Review-first only: no prospect lists, outreach copy,
 * sends, CRM writes, or account/DNS/GBP/social/tracking changes.
 *
 * Preview synthesis extracts structured fields from operator answers and
 * normalizes each section — never concatenates wrapper phrases with raw
 * transcript text.
 */

const { cleanAvoidPhrase } = require('./clientIntelligenceGrowthDirection');

const ARTIFACT_KIND = 'first_campaign_plan_preview';
const PREVIEW_TITLE = 'First Campaign Plan Preview';
const PREVIEW_DISCLAIMER =
  'Planning preview only. No prospect list, outreach copy, sends, CRM writes, or account changes have been created or launched.';

const CRITERIA_ARTIFACT_KIND = 'prospect_list_criteria_preview';
const CRITERIA_PREVIEW_TITLE = 'Prospect List Criteria Preview';
const CRITERIA_PREVIEW_DISCLAIMER =
  'Criteria preview only. No prospect list has been built, and no outreach copy, sends, CRM writes, or account changes have been created or launched.';

const SECTION_TITLES = Object.freeze({
  campaignObjective: 'Campaign objective',
  targetSegment: 'Target segment',
  marketBound: 'Market bound',
  hypothesis: 'Campaign hypothesis',
  proofAssets: 'Proof assets',
  proofAssetsNeeded: 'Proof assets',
  validationMetrics: 'Validation metrics',
  risksCautions: 'Risks and cautions',
  approvalCheckpoints: 'Approval checkpoints',
  recommendedNextStep: 'Recommended next step',
});

const CRITERIA_SECTION_TITLES = Object.freeze({
  campaignObjective: 'Campaign objective',
  targetSegment: 'Target segment',
  targetSubtype: 'Target subtype',
  marketBound: 'Market bound',
  inclusionCriteria: 'Inclusion criteria',
  exclusionCriteria: 'Exclusion criteria',
  requiredProspectFields: 'Required prospect record fields',
  reviewGate: 'Review gate',
  recommendedNextStep: 'Recommended next step',
});

/** Required planning slots persisted on campaignPlanning.slots */
const SLOT_KEYS = Object.freeze([
  'campaignObjective',
  'targetSegment',
  'targetSubtype',
  'marketBound',
  'campaignHypothesis',
  'proofAssets',
  'validationMetrics',
  'inclusionCriteria',
  'exclusionCriteria',
  'approvalCheckpoints',
  'previewGenerated',
  'previewApproved',
]);

/** Pre-preview ask order. targetSubtype is covered by the targetSegment prompt. */
const PRE_PREVIEW_SLOT_ORDER = Object.freeze([
  'campaignObjective',
  'targetSegment',
  'marketBound',
  'proofAssets',
  'campaignHypothesis',
  'validationMetrics',
  'approvalCheckpoints',
]);

const SLOT_PROMPTS = Object.freeze({
  campaignObjective:
    'What should this first campaign prove? For example: that property managers will take a discovery conversation, request a walkthrough, or ask for an estimate.',
  targetSegment:
    'Confirm the first target segment and subtype. Keep property managers as defined, or name a narrower subtype for the first test.',
  marketBound:
    'Confirm the market bounds for this first test. Stay inside Greater Manchester (or the approved Blueprint market), or name a tighter town cluster.',
  proofAssets:
    'What proof assets are already available for this segment — photos/examples, checklist, response-time promise, references, walkthrough/estimate process — and what is still missing?',
  campaignHypothesis:
    'In one sentence, what is the campaign hypothesis? If we approach [segment] in [market] with [proof], we expect [signal].',
  validationMetrics:
    'What early metrics would prove this is worth pursuing — for example qualified conversations, walkthroughs booked, or estimate requests in the first 30 days?',
  approvalCheckpoints:
    'What approval checkpoints should block list-building or launch? Typical gates: preview sign-off, proof assets ready, readiness gaps cleared, copy review.',
  previewApproved:
    'Does this First Campaign Plan Preview look right to approve, or do you want to revise a specific section?',
  inclusionCriteria:
    'What inclusion criteria should define the prospect list for this first test (who belongs on the list)?',
  exclusionCriteria:
    'What exclusion criteria should keep the wrong accounts out of this first test?',
  inclusionExclusion:
    'What inclusion and exclusion criteria should define the prospect list for this first test? Who belongs on the list, and who should stay out?',
  prospectListCriteria:
    'Before building a prospect list, define what should qualify or disqualify a property manager for this first test.',
});

const PROSPECT_LIST_CRITERIA_STEP = 'prospect_list_criteria';

const DEFAULT_TOWNS = Object.freeze([
  'Bedford',
  'Hooksett',
  'Londonderry',
  'Auburn',
  'Goffstown',
]);

const DEFAULT_PROOF_ASSETS_AVAILABLE = Object.freeze([
  'Clear service mix and commercial cleaning focus',
  'Defined service area',
  'Clear ideal customer profile',
  'Positioning around reliability, responsiveness, and accountability',
  'Walkthrough/estimate process that can be described simply',
]);

const DEFAULT_PROOF_ASSETS_MISSING = Object.freeze([
  'Commercial cleaning checklist',
  'Before/after photos or commercial work examples',
  'Clear response-time promise',
  'References, testimonials, or review proof if available',
  'Reusable walkthrough/estimate process for property managers',
  'Short credibility statement for recurring commercial cleaning',
]);

/** @deprecated Prefer DEFAULT_PROOF_ASSETS_MISSING — kept for older callers. */
const DEFAULT_PROOF_ASSETS = DEFAULT_PROOF_ASSETS_MISSING;

const DEFAULT_VALIDATION_METRICS_PRIMARY = Object.freeze([
  'Qualified replies from property managers',
  'Discovery conversations booked',
  'Walkthroughs or site visits requested',
  'Estimate requests from properties that fit the target',
]);

const DEFAULT_VALIDATION_METRICS_SECONDARY = Object.freeze([
  'Questions about recurring schedule, reliability, response time, or cleaning frustrations',
  'Clarity on which property type responds best',
  'Replies that are not only about lowest price',
]);

/** @deprecated Prefer primary/secondary defaults. */
const DEFAULT_VALIDATION_METRICS = DEFAULT_VALIDATION_METRICS_PRIMARY;

const DEFAULT_INCLUSION_CRITERIA = Object.freeze([
  'Manage offices, mixed-use buildings, small commercial properties, or multi-tenant spaces',
  'Are located in Bedford, Hooksett, Londonderry, Auburn, Goffstown, or nearby Greater Manchester markets',
  'Likely need recurring cleaning weekly or multiple times per week',
  'Value reliability, responsiveness, and accountability',
  'Have a reachable owner, manager, facilities contact, or operations contact',
]);

const DEFAULT_EXCLUSION_CRITERIA = Object.freeze([
  'Large institutional property managers',
  'Highly complex properties',
  'Lowest-price buyers',
  'Properties outside the approved service area',
  'Prospects with no clear decision-maker or contact path',
]);

/** Required CRM fields for a prospect-list record (criteria preview only). */
const DEFAULT_REQUIRED_PROSPECT_FIELDS = Object.freeze([
  'Business / property name',
  'Decision-maker or operations contact name',
  'Email and/or phone',
  'Property type (multi-family, HOA, office, mixed-use, or multi-tenant)',
  'Town / market location',
  'Website or online listing when available',
]);

const DEFAULT_REVIEW_GATE =
  'Operator reviews and approves these prospect-list criteria before any list is built. No outreach copy, sends, CRM writes, or account changes happen at this step.';

/** Polished first-test subtype for property-manager campaigns. */
const DEFAULT_TARGET_SUBTYPE_PROPERTY_MANAGERS =
  'property managers overseeing offices, mixed-use buildings, small commercial properties, or multi-tenant spaces that likely need recurring cleaning weekly or multiple times per week';

const DEFAULT_APPROVAL_BEFORE_LIST = Object.freeze([
  'Campaign plan preview is approved.',
  'Target segment, subtype, and market bound are confirmed.',
  'Inclusion and exclusion criteria are approved.',
  'Proof assets are ready enough for outreach.',
  'High-priority infrastructure gaps are reviewed.',
]);

const DEFAULT_APPROVAL_BEFORE_LAUNCH = Object.freeze([
  'Prospect list criteria are approved.',
  'Prospect list is reviewed.',
  'Outreach copy is reviewed and approved.',
  'Tracking and follow-up process are confirmed.',
  'Capacity and scheduling expectations are confirmed.',
  'Operator gives explicit launch approval.',
]);

/** @deprecated Prefer before-list / before-launch groups. */
const DEFAULT_APPROVAL_CHECKPOINTS = Object.freeze([
  ...DEFAULT_APPROVAL_BEFORE_LIST,
  ...DEFAULT_APPROVAL_BEFORE_LAUNCH,
]);

const VALIDATION_SUCCESS_STATEMENT =
  'A successful first 30 days means a small number of qualified conversations and at least one walkthrough or estimate request.';

/** Known prompt / transcript wrappers that must never be stitched into prose. */
const CAMPAIGN_WRAPPER_PATTERNS = Object.freeze([
  /^the first campaign should prove(?:\s+that)?\s+/i,
  /^this (?:first )?campaign should prove(?:\s+that)?\s+/i,
  /^what should this first campaign prove[?:]?\s*/i,
  /^campaign objective\s*[:\-–—]\s*/i,
  /^prove(?:\s+that)?\s+/i,
  /^for the first test,?\s*/i,
  /^i'?d treat the goal as\s*[:\-–—]?\s*/i,
  /^the goal is\s+(?:to\s+)?/i,
]);

const CONVERSATION_STEPS = Object.freeze([
  'opening',
  'campaign_objective',
  'target_segment',
  'market_bounds',
  'proof_assets',
  'hypothesis',
  'validation_metrics',
  'approval_checkpoints',
  'preview',
]);

const QUESTION_BANK = Object.freeze([
  {
    step: 'campaign_objective',
    prompt:
      'What should this first campaign prove? For example: that property managers will take a discovery conversation, request a walkthrough, or ask for an estimate.',
  },
  {
    step: 'target_segment',
    prompt:
      'Confirm the first target segment and subtype. Keep property managers as defined, or name a narrower subtype for the first test.',
  },
  {
    step: 'market_bounds',
    prompt:
      'Confirm the market bounds for this first test. Stay inside Greater Manchester (or the approved Blueprint market), or name a tighter town cluster.',
  },
  {
    step: 'proof_assets',
    prompt:
      'What proof assets are already available for this segment — photos/examples, checklist, response-time promise, references, walkthrough/estimate process — and what is still missing?',
  },
  {
    step: 'hypothesis',
    prompt:
      'In one sentence, what is the campaign hypothesis? If we approach [segment] in [market] with [proof], we expect [signal].',
  },
  {
    step: 'validation_metrics',
    prompt:
      'What early metrics would prove this is worth pursuing — for example qualified conversations, walkthroughs booked, or estimate requests in the first 30 days?',
  },
  {
    step: 'approval_checkpoints',
    prompt:
      'What approval checkpoints should block list-building or launch? Typical gates: preview sign-off, proof assets ready, readiness gaps cleared, copy review.',
  },
]);

function shortName(name) {
  const s = String(name || '').trim();
  if (!s) return 'the business';
  return s.replace(/\s+/g, ' ');
}

/** Display name for prose (Anchor Cleaning → Anchor). */
function displayName(name) {
  const s = shortName(name);
  if (!s || s === 'the business') return 'the business';
  return s.replace(/\s+Cleaning$/i, '') || s;
}

/**
 * Strip known campaign / interview wrapper fragments so section normalizers
 * never concatenate "Prove that" + "The first campaign should prove…".
 */
function stripCampaignWrappers(text) {
  let s = String(text || '')
    .replace(/\s+/g, ' ')
    .trim();
  if (!s) return '';
  let prev;
  do {
    prev = s;
    for (const re of CAMPAIGN_WRAPPER_PATTERNS) {
      s = s.replace(re, '').trim();
    }
    // Also strip mid-string residual wrappers after a peel left them hanging.
    s = s
      .replace(/\bthe first campaign should prove(?:\s+that)?\s+/gi, '')
      .replace(/\bthis (?:first )?campaign should prove(?:\s+that)?\s+/gi, '')
      .replace(/^(?:that|to)\s+/i, '')
      .trim();
  } while (s && s !== prev);
  return s;
}

/**
 * Peel unlabeled include/exclude clauses from prose (common when Max or the
 * operator stitches criteria into the objective answer).
 * Returns { text, inclusion[], exclusion[] }.
 */
function peelInlineIncludeExclude(text) {
  let s = String(text || '').replace(/\s+/g, ' ').trim();
  const inclusion = [];
  const exclusion = [];
  if (!s) return { text: '', inclusion, exclusion };

  const whoClause =
    '(?:property managers who|accounts who|customers who|managers who|who)';
  const includeSource =
    `(?:(?:we|you|operators?)\\s+should\\s+|should\\s+|must\\s+)?(?:include|includes)\\s+${whoClause}\\s+([^.;]+?)(?=\\s+(?:and\\s+)?(?:exclude|avoid)\\b|[.;]|$)`;
  const excludeSource =
    `(?:(?:and\\s+)?(?:(?:we|you|operators?)\\s+should\\s+|should\\s+|must\\s+)?)?(?:exclude|excludes|avoid)\\s+${whoClause}\\s+([^.;]+?)(?=[.;]|$)`;

  for (const m of s.matchAll(new RegExp(includeSource, 'gi'))) {
    const item = String(m[1] || '')
      .replace(/\s+/g, ' ')
      .replace(/\s+and\s*$/i, '')
      .trim();
    if (item.length > 4) {
      inclusion.push(item.charAt(0).toUpperCase() + item.slice(1));
    }
  }
  for (const m of s.matchAll(new RegExp(excludeSource, 'gi'))) {
    const item = String(m[1] || '')
      .replace(/\s+/g, ' ')
      .trim();
    if (item.length > 4) {
      exclusion.push(item.charAt(0).toUpperCase() + item.slice(1));
    }
  }

  s = s
    .replace(new RegExp(includeSource, 'gi'), ' ')
    .replace(new RegExp(excludeSource, 'gi'), ' ')
    .replace(
      /\b(?:we|you|operators?)\s+should\s+(?:include|exclude)\b[^.;]*/gi,
      ' '
    )
    .replace(/\s*(?:and|,)\s*(?=[.;]|$)/g, '')
    .replace(/\s+/g, ' ')
    .replace(/\s*[.;]\s*$/g, '')
    .trim();

  return { text: s, inclusion, exclusion };
}

function objectiveHasCrossSectionBleed(text) {
  const s = String(text || '');
  return (
    /\binclude(?:s|d)?\b/i.test(s) ||
    /\bexclude(?:s|d)?\b/i.test(s) ||
    /\bprimary signals?\b/i.test(s) ||
    /\bsecondary signals?\b/i.test(s) ||
    /\bcore validation question\b/i.test(s) ||
    /\bthe first campaign should prove\b/i.test(s) ||
    /\bprove that prove\b/i.test(s)
  );
}

function hypothesisHasCrossSectionBleed(text) {
  const s = String(text || '');
  return (
    /\bprimary signals?\b/i.test(s) ||
    /\bsecondary signals?\b/i.test(s) ||
    /\bvalidation metrics?\b/i.test(s) ||
    /\bqualified replies\b/i.test(s) ||
    /\ba successful first 30 days\b/i.test(s)
  );
}

function sectionSummary(sections, key) {
  const sec = sections && sections[key];
  if (!sec) return '';
  if (typeof sec === 'string') return sec.trim();
  return String(sec.summary || '').trim();
}

function extractBusinessName(blueprint) {
  const sections = (blueprint && blueprint.sections) || {};
  const identity = sectionSummary(sections, 'identity');
  const m = identity.match(/^([^.]+?)(?:\s+is\b|\s+provides\b|,|\.|$)/i);
  if (m && m[1] && m[1].length < 80) return shortName(m[1]);
  if (identity) {
    const first = identity.split(/[.!?]/)[0];
    if (first && first.length < 80) return shortName(first);
  }
  return 'the business';
}

/**
 * Gather prior approved artifacts so Max does not re-run the interview.
 */
function buildCampaignPlanningContext(session, blueprint, opts = {}) {
  const state = (session && session.interview_state) || {};
  const growth = state.growthConversation || {};
  const gd = state.initialGrowthDirection || opts.growthDirection || null;
  const preview =
    state.firstGrowthPlanPreview ||
    growth.firstGrowthPlanPreview ||
    growth.first_growth_plan_preview ||
    null;
  const ranking =
    state.segmentRanking ||
    growth.segmentRanking ||
    growth.segment_ranking ||
    null;
  const validationTarget =
    state.validationTarget ||
    growth.validationTarget ||
    growth.validation_target ||
    null;
  const readinessReport =
    state.growthInfrastructureReadinessReport || null;
  const growthWork = state.growthWork || null;
  const sections = (blueprint && blueprint.sections) || {};

  const primarySegment =
    (preview && (preview.primarySegmentDisplay || preview.primary_segment)) ||
    (growth.firstSegmentDecision &&
      growth.firstSegmentDecision.primarySegmentDisplay) ||
    (gd && gd.segmentsToInspect && gd.segmentsToInspect[0]) ||
    'property managers';
  const secondarySegment =
    (preview &&
      (preview.secondarySegmentDisplay || preview.secondary_segment)) ||
    (growth.firstSegmentDecision &&
      growth.firstSegmentDecision.secondarySegmentDisplay) ||
    'professional offices';
  const targetMarket =
    (preview && preview.primaryArea) ||
    (gd && gd.primaryArea) ||
    sectionSummary(sections, 'targetMarkets')
      .split(/including|,|—|-/)[0]
      .trim() ||
    'Greater Manchester';
  const subtype =
    (validationTarget && validationTarget.best_fit_subtype) ||
    (validationTarget &&
      validationTarget.sections &&
      validationTarget.sections.bestFirstType &&
      validationTarget.sections.bestFirstType.body) ||
    (preview && preview.first_subtype_to_test) ||
    null;
  const proofFromPrior =
    (preview && preview.credibility_proof_needed) ||
    (validationTarget && validationTarget.credibility_proof_needed) ||
    null;

  const completedTaskIds =
    (growthWork && Array.isArray(growthWork.completedTaskIds)
      ? growthWork.completedTaskIds
      : []) || [];

  const townsFromGd = Array.isArray(gd && gd.towns)
    ? gd.towns.map((t) => String(t).trim()).filter(Boolean)
    : [];
  const townsFromMarkets = extractTownsFromMarketSummary(
    sectionSummary(sections, 'targetMarkets')
  );
  const towns = (townsFromGd.length ? townsFromGd : townsFromMarkets).slice(0, 6);
  const avoidPhrase = extractAvoidPhrase(sectionSummary(sections, 'avoidCustomers'));

  return {
    businessName: shortName(
      (preview && preview.businessName) ||
        (gd && gd.businessName) ||
        extractBusinessName(blueprint)
    ),
    primarySegment: humanizeSegment(primarySegment),
    secondarySegment: humanizeSegment(secondarySegment),
    targetMarket: String(targetMarket || 'Greater Manchester').trim(),
    towns: towns.length ? towns : [...DEFAULT_TOWNS],
    avoidPhrase,
    subtype: subtype ? String(subtype).trim() : null,
    proofFromPrior: proofFromPrior ? String(proofFromPrior).trim() : null,
    segmentRanking: ranking,
    validationTarget,
    firstGrowthPlanPreview: preview,
    initialGrowthDirection: gd,
    readinessReport,
    completedSetupChecklist: completedTaskIds.length > 0,
    completedTaskIds,
    readinessOverallStatus:
      (readinessReport && readinessReport.overallStatus) || null,
    blueprintId: (blueprint && blueprint.id) || null,
    blueprintVersion: (blueprint && blueprint.version) || null,
  };
}

function extractTownsFromMarketSummary(summary) {
  const s = String(summary || '');
  const found = [];
  for (const town of DEFAULT_TOWNS) {
    if (new RegExp(`\\b${town}\\b`, 'i').test(s)) found.push(town);
  }
  return found;
}

function extractAvoidPhrase(summary) {
  const cleaned = cleanAvoidPhrase(summary);
  if (!cleaned) return 'buyers focused only on the lowest price';
  let s = cleaned
    .replace(/^customers?\s+who\s+/i, '')
    .replace(/^who\s+/i, '')
    .replace(/\.$/, '')
    .trim();
  // Final guard against residual Blueprint/Growth Direction wrapper bleed.
  s = s
    .replace(/^the business prefers to avoid\s+/i, '')
    .replace(/^anchor(?:\s+cleaning)?\s+should avoid\s+/i, '')
    .replace(/\bthe business prefers to avoid\b/gi, '')
    .replace(/\banchor(?:\s+cleaning)?\s+should avoid\b/gi, '')
    .replace(/\s{2,}/g, ' ')
    .trim();
  if (!s || /\bshould avoid\b/i.test(s) || /prefers to avoid/i.test(s)) {
    return 'buyers focused only on the lowest price';
  }
  return s || 'buyers focused only on the lowest price';
}

function humanizeStatusLabel(status) {
  const raw = String(status || '')
    .trim()
    .toLowerCase()
    .replace(/_/g, ' ');
  if (!raw) return 'unknown';
  if (raw === 'not ready') return 'not ready';
  if (raw === 'partial') return 'partial';
  if (raw === 'ready') return 'ready';
  return raw;
}

function naturalList(items) {
  const list = (items || []).map((x) => String(x).trim()).filter(Boolean);
  if (!list.length) return '';
  if (list.length === 1) return list[0];
  if (list.length === 2) return `${list[0]} and ${list[1]}`;
  return `${list.slice(0, -1).join(', ')}, and ${list[list.length - 1]}`;
}

function isPropertyManagerFocus(context, answers) {
  const blob = [
    context && context.primarySegment,
    context && context.subtype,
    answerText(answers, 'opening'),
    answerText(answers, 'target_segment'),
  ]
    .filter(Boolean)
    .join(' ')
    .toLowerCase();
  return /property manager/.test(blob);
}

function humanizeSegment(value) {
  const s = String(value || '')
    .trim()
    .replace(/_/g, ' ')
    .replace(/\s+/g, ' ');
  if (!s) return s;
  const lower = s.toLowerCase();
  if (lower === 'property managers' || lower === 'property manager') {
    return 'property managers';
  }
  if (lower === 'professional offices' || lower === 'professional office') {
    return 'professional offices';
  }
  return lower;
}

function buildCampaignPlanningOpening(context) {
  const ctx = context || {};
  const name = shortName(ctx.businessName || 'the business');
  const primary = ctx.primarySegment || 'property managers';
  const market = ctx.targetMarket || 'Greater Manchester';
  const secondary = ctx.secondarySegment || 'professional offices';

  return [
    `Great. ${name} is ready to plan the first campaign. I’ll keep this review-first: no prospect list, outreach copy, or launch steps yet.`,
    ``,
    `We’re carrying forward the approved focus: ${primary} in ${market}, with ${secondary} as a secondary path.`,
    ``,
    `Before anything gets built, I want to define the campaign hypothesis and what would prove this is worth pursuing.`,
    ``,
    `Should we plan around ${primary} exactly as defined, or do you want to narrow the first test further?`,
  ].join('\n');
}

function detectPreviewRequest(userMessage) {
  const s = String(userMessage || '').toLowerCase();
  return (
    /\b(preview|wrap|summarize|summary|enough|produce (the )?plan|campaign plan)\b/.test(
      s
    ) || /\b(show|give) me (the )?(first )?campaign plan\b/.test(s)
  );
}

function nextQuestion(stepId) {
  const idx = QUESTION_BANK.findIndex((q) => q.step === stepId);
  if (idx < 0) return QUESTION_BANK[0];
  return QUESTION_BANK[idx + 1] || null;
}

function stepAfterOpening() {
  return 'campaign_objective';
}

function emptySlots() {
  return {
    campaignObjective: null,
    targetSegment: null,
    targetSubtype: null,
    marketBound: null,
    campaignHypothesis: null,
    proofAssets: null,
    validationMetrics: null,
    inclusionCriteria: null,
    exclusionCriteria: null,
    requiredProspectFields: null,
    reviewGate: null,
    approvalCheckpoints: null,
    previewGenerated: false,
    previewApproved: false,
  };
}

function isSlotSatisfied(slots, key) {
  if (!slots) return false;
  if (key === 'previewGenerated' || key === 'previewApproved') {
    return Boolean(slots[key]);
  }
  const v = slots[key];
  if (v == null) return false;
  if (typeof v === 'boolean') return v;
  if (Array.isArray(v)) return v.length > 0;
  return String(v).trim().length > 0;
}

function seedSlotsFromContext(context, priorSlots) {
  const slots = { ...emptySlots(), ...(priorSlots || {}) };
  const ctx = context || {};

  if (!isSlotSatisfied(slots, 'targetSegment') && ctx.primarySegment) {
    slots.targetSegment = humanizeSegment(ctx.primarySegment);
  }
  if (!isSlotSatisfied(slots, 'targetSubtype') && ctx.subtype) {
    slots.targetSubtype = String(ctx.subtype).trim();
  }
  if (!isSlotSatisfied(slots, 'marketBound') && ctx.targetMarket) {
    slots.marketBound = String(ctx.targetMarket).trim();
  }
  if (!isSlotSatisfied(slots, 'proofAssets') && ctx.proofFromPrior) {
    slots.proofAssets = String(ctx.proofFromPrior).trim();
  }
  // Preserve boolean flags from prior session state.
  slots.previewGenerated = Boolean(
    (priorSlots && priorSlots.previewGenerated) || slots.previewGenerated
  );
  slots.previewApproved = Boolean(
    (priorSlots && priorSlots.previewApproved) || slots.previewApproved
  );
  return slots;
}

function detectReviseIntent(userMessage) {
  return /\b(revise|change|update|edit|redo|rework|instead)\b/i.test(
    String(userMessage || '')
  );
}

function detectRevisedSlotKeys(userMessage) {
  const s = String(userMessage || '').toLowerCase();
  const keys = [];
  if (/\bobjective\b/.test(s)) keys.push('campaignObjective');
  if (/\bsegment\b|\bsubtype\b/.test(s)) {
    keys.push('targetSegment', 'targetSubtype');
  }
  if (/\bmarket\b|\btown\b|\bgeograph/.test(s)) keys.push('marketBound');
  if (/\bproof\b|\basset\b/.test(s)) keys.push('proofAssets');
  if (/\bhypoth/.test(s)) keys.push('campaignHypothesis');
  if (/\bmetric\b|\bsignal\b/.test(s)) keys.push('validationMetrics');
  if (/\bapproval\b|\bcheckpoint\b|\bgate\b/.test(s)) {
    keys.push('approvalCheckpoints');
  }
  if (/\binclusion\b|\binclude\b/.test(s)) keys.push('inclusionCriteria');
  if (/\bexclusion\b|\bexclude\b|\bavoid\b/.test(s)) {
    keys.push('exclusionCriteria');
  }
  return uniqueStrings(keys);
}

function stripCriteriaClauses(text) {
  return String(text || '')
    .replace(
      /(?:^|\n|;|\.)\s*(?:inclusion(?:\s+criteria)?|include|includes?)\s*[:-]?\s*[\s\S]*?(?=(?:^|\n|;|\.)\s*(?:exclusion(?:\s+criteria)?|exclude|excludes?|avoid)\s*[:\-]|$)/gi,
      ' '
    )
    .replace(
      /(?:^|\n|;|\.)\s*(?:exclusion(?:\s+criteria)?|exclude|excludes?|avoid)\s*[:-]?\s*[\s\S]*$/gi,
      ' '
    )
    .replace(/\s+/g, ' ')
    .trim();
}

const CRITERIA_SECTION_STOP =
  'inclusion(?:\\s+criteria)?|include|includes?|exclusion(?:\\s+criteria)?|exclude|excludes?|avoid|each\\s+prospect\\s+record\\s+should\\s+include|required\\s+(?:prospect\\s+)?(?:record\\s+)?fields?|review\\s+gate';

function extractLabeledCriteria(text, labels) {
  const s = String(text || '');
  const labelRe = labels.map((l) => l.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('|');
  const re = new RegExp(
    `(?:^|[\\n;.]\\s*)(?:${labelRe})\\s*[:\\-]?\\s*([\\s\\S]+?)(?=(?:[\\n;.]\\s*)(?:${CRITERIA_SECTION_STOP})\\s*[:\\-]|$)`,
    'i'
  );
  const m = s.match(re);
  if (!m || !m[1]) return null;
  const value = peelRecordFieldsClause(m[1]).replace(/\s+/g, ' ').trim();
  return value.length > 2 ? value : null;
}

/**
 * Peel "Each prospect record should include …" / required-fields clauses so
 * they never land inside exclusion/inclusion bullets.
 */
function peelRecordFieldsClause(text) {
  return String(text || '')
    .replace(
      /(?:^|\n|;|\.)\s*(?:each\s+prospect\s+record\s+should\s+include|required\s+(?:prospect\s+)?(?:record\s+)?fields?)\s*[:\-–—]?\s*[\s\S]*$/i,
      ' '
    )
    .replace(/\s+/g, ' ')
    .trim();
}

function extractRequiredProspectFieldsText(text) {
  const s = String(text || '');
  const re = new RegExp(
    `(?:^|[\\n;.]\\s*)(?:each\\s+prospect\\s+record\\s+should\\s+include|required\\s+(?:prospect\\s+)?(?:record\\s+)?fields?)\\s*[:\\-–—]?\\s*([\\s\\S]+?)(?=(?:[\\n;.]\\s*)(?:${CRITERIA_SECTION_STOP}|recommended\\s+next\\s+step)\\s*[:\\-]|$)`,
    'i'
  );
  const m = s.match(re);
  if (!m || !m[1]) return null;
  const value = m[1].replace(/\s+/g, ' ').trim();
  return value.length > 2 ? value : null;
}

function extractInclusionCriteria(text) {
  return (
    extractLabeledCriteria(text, [
      'inclusion criteria',
      'inclusion',
      'include',
      'includes',
      'must include',
    ]) || null
  );
}

function extractExclusionCriteria(text) {
  return (
    extractLabeledCriteria(text, [
      'exclusion criteria',
      'exclusion',
      'exclude',
      'excludes',
      'avoid',
      'must exclude',
    ]) || null
  );
}

function looksLikeObjective(text) {
  const s = String(text || '');
  return (
    /\b(prove|proof that|objective|walkthrough|estimate|discover(?:y)? conversation|validate|worth pursuing)\b/i.test(
      s
    ) || /\b(first campaign|this campaign)\b/i.test(s)
  );
}

function looksLikeHypothesis(text) {
  return /\b(if we|hypothesis|we expect|should create|with proof)\b/i.test(
    String(text || '')
  );
}

function looksLikeMarket(text) {
  return /\b(greater manchester|manchester|bedford|hooksett|londonderry|auburn|goffstown|market|town cluster|service area)\b/i.test(
    String(text || '')
  );
}

/**
 * Capture a geography phrase only — never trailing objective / criteria prose
 * after a market name (e.g. "Greater Manchester will engage…").
 */
function extractMarketBoundPhrase(text) {
  const s = String(text || '').trim();
  if (!s) return null;
  const named = s.match(
    /\b(Greater Manchester(?:\s+NH)?|Manchester(?:\s+NH)?)\b/i
  );
  if (named) {
    // If the operator listed towns nearby, keep the compact cluster form.
    const towns = [];
    for (const t of DEFAULT_TOWNS) {
      if (new RegExp(`\\b${t}\\b`, 'i').test(s)) towns.push(t);
    }
    if (towns.length >= 2) {
      return `${towns.join(', ')} inside ${named[1]}`;
    }
    return named[1];
  }
  const townHits = DEFAULT_TOWNS.filter((t) =>
    new RegExp(`\\b${t}\\b`, 'i').test(s)
  );
  if (townHits.length) return townHits.join(', ');
  return null;
}

function looksLikeProof(text) {
  return /\b(checklist|photo|photos|example|reference|testimonial|response[- ]?time|proof asset|walkthrough\/estimate|service area)\b/i.test(
    String(text || '')
  );
}

function looksLikeMetrics(text) {
  return /\b(metric|conversation|walkthrough|estimate request|repl(?:y|ies)|signal|30 days|booked)\b/i.test(
    String(text || '')
  );
}

function looksLikeCheckpoints(text) {
  return /\b(approval|checkpoint|sign[- ]?off|gate|before (?:any )?list|before launch)\b/i.test(
    String(text || '')
  );
}

function looksLikeSegmentOrSubtype(text) {
  return /\b(property managers?|professional offices?|subtype|hoa|multi[- ]family|segment|as defined|exactly as)\b/i.test(
    String(text || '')
  );
}

function extractSubtypeFromText(text, context) {
  const s = String(text || '').trim();
  if (!s) return null;
  // Only treat spaced dash / em-dash as a segment—subtype separator.
  // Never match the hyphen inside "multi-family".
  const dash = s.match(/\s[—–-]\s+(.+)$/);
  if (dash && dash[1] && dash[1].length > 3) {
    const afterDash = peelRecordFieldsClause(
      stripCriteriaClauses(dash[1])
    ).trim();
    if (
      afterDash.length > 3 &&
      !/^(inclusion|exclusion|include|exclude)\b/i.test(afterDash) &&
      !looksLikeObjective(afterDash)
    ) {
      return afterDash;
    }
  }
  // Narrow subtype phrases only — never pull objective prose that happens
  // to mention multi-family / HOA.
  const subtype = s.match(
    /\b((?:multi[- ]family|hoa)(?:\s*\/\s*(?:hoa|multi[- ]family))?[^.]{0,60}?)(?=\s+(?:with|in|for|that)\b|[.,;]|$)/i
  );
  if (subtype) {
    const candidate = String(subtype[1] || '')
      .replace(/\s+/g, ' ')
      .trim();
    if (
      candidate.length >= 8 &&
      candidate.length <= 90 &&
      !/\bwill\b|\bprove\b|\brequest\b|\binclude\b|\bexclude\b/i.test(candidate)
    ) {
      return candidate;
    }
  }
  if (
    context &&
    context.subtype &&
    /\bas defined\b|\bexactly as\b|\bkeep\b/i.test(s)
  ) {
    return String(context.subtype).trim();
  }
  return null;
}

const SOFT_PREVIEW_SLOTS = Object.freeze([
  'proofAssets',
  'campaignHypothesis',
  'validationMetrics',
  'approvalCheckpoints',
]);

function nextMissingPrePreviewSlot(slots, opts = {}) {
  const skipSoft = Boolean(opts && opts.skipSoft);
  const soft = new Set(SOFT_PREVIEW_SLOTS);
  for (const key of PRE_PREVIEW_SLOT_ORDER) {
    if (skipSoft && soft.has(key)) continue;
    if (key === 'targetSegment') {
      // Subtype is enrichment from prior artifacts or user text — do not
      // block the flow when segment is already satisfied.
      if (!isSlotSatisfied(slots, 'targetSegment')) return 'targetSegment';
      continue;
    }
    if (!isSlotSatisfied(slots, key)) return key;
  }
  return null;
}

function slotToStep(slotKey) {
  const map = {
    campaignObjective: 'campaign_objective',
    targetSegment: 'target_segment',
    targetSubtype: 'target_segment',
    marketBound: 'market_bounds',
    proofAssets: 'proof_assets',
    campaignHypothesis: 'hypothesis',
    validationMetrics: 'validation_metrics',
    approvalCheckpoints: 'approval_checkpoints',
    inclusionCriteria: PROSPECT_LIST_CRITERIA_STEP,
    exclusionCriteria: PROSPECT_LIST_CRITERIA_STEP,
    inclusionExclusion: PROSPECT_LIST_CRITERIA_STEP,
    prospectListCriteria: PROSPECT_LIST_CRITERIA_STEP,
    previewApproved: 'preview',
    previewGenerated: 'preview',
  };
  return map[slotKey] || slotKey || 'opening';
}

function markCampaignPlanPreviewApproved(preview, slots, opts = {}) {
  const base =
    preview && typeof preview === 'object' && Object.keys(preview).length
      ? preview
      : null;
  if (!base) {
    return {
      preview: null,
      slots: { ...slots, previewApproved: true, previewGenerated: true },
    };
  }
  return {
    preview: {
      ...base,
      status: 'approved',
      approvedAt: base.approvedAt || new Date().toISOString(),
    },
    slots: { ...slots, previewApproved: true, previewGenerated: true },
  };
}

function prospectListCriteriaPrompt(context) {
  const primary = humanizeSegment(
    (context && context.primarySegment) || 'property managers'
  );
  if (/property manager/i.test(primary)) {
    return SLOT_PROMPTS.prospectListCriteria;
  }
  return `Before building a prospect list, define what should qualify or disqualify ${primary} for this first test.`;
}

function syncAnswersFromSlots(answers, slots) {
  const next = { ...(answers || {}) };
  const write = (step, raw) => {
    if (raw == null || String(raw).trim() === '') return;
    next[step] = { raw: String(raw).trim(), at: new Date().toISOString() };
  };

  write('campaign_objective', slots.campaignObjective);
  if (isSlotSatisfied(slots, 'targetSegment')) {
    const segmentRaw = isSlotSatisfied(slots, 'targetSubtype')
      ? `${slots.targetSegment} — ${slots.targetSubtype}`
      : slots.targetSegment;
    write('target_segment', segmentRaw);
  }
  write('market_bounds', slots.marketBound);
  write('proof_assets', slots.proofAssets);
  write('hypothesis', slots.campaignHypothesis);
  write('validation_metrics', slots.validationMetrics);
  write('approval_checkpoints', slots.approvalCheckpoints);
  write('inclusion_criteria', slots.inclusionCriteria);
  write('exclusion_criteria', slots.exclusionCriteria);
  return next;
}

/**
 * Infer and merge slot values from free-text. Fills multiple slots when
 * the operator packs several answers into one message.
 */
function extractSlotsFromMessage(userMessage, slots, context, currentAsk) {
  const text = String(userMessage || '').trim();
  const next = { ...emptySlots(), ...(slots || {}) };
  if (!text) return next;

  if (detectReviseIntent(text)) {
    for (const key of detectRevisedSlotKeys(text)) {
      if (key === 'previewGenerated' || key === 'previewApproved') continue;
      next[key] = null;
      if (key === 'targetSegment') next.targetSubtype = null;
    }
  }

  const inclusion = extractInclusionCriteria(text);
  const exclusion = extractExclusionCriteria(text);
  if (inclusion) next.inclusionCriteria = inclusion;
  if (exclusion) next.exclusionCriteria = exclusion;

  if (next.previewGenerated && !next.previewApproved) {
    if (
      /\b(approve|approved|looks good|lgtm|accept|yes)\b/i.test(text) &&
      !detectReviseIntent(text)
    ) {
      next.previewApproved = true;
    }
  }

  const keepAsDefined = /\bas defined\b|\bexactly as\b|\bas-is\b|\bkeep (it |them )?as\b/i.test(
    text
  );
  if (keepAsDefined) {
    if (context && context.primarySegment) {
      next.targetSegment = humanizeSegment(context.primarySegment);
    }
    if (context && context.subtype) {
      next.targetSubtype = String(context.subtype).trim();
    }
    if (context && context.targetMarket && !isSlotSatisfied(next, 'marketBound')) {
      next.marketBound = String(context.targetMarket).trim();
    }
  }

  const objectiveBody = stripCriteriaClauses(text);
  const ask = currentAsk || null;

  const postPreviewAsk =
    ask === 'previewApproved' ||
    ask === 'prospectListCriteria' ||
    ask === 'inclusionCriteria' ||
    ask === 'exclusionCriteria' ||
    ask === 'inclusionExclusion';
  const shouldFillObjective =
    !postPreviewAsk &&
    (ask === 'campaignObjective' ||
      (looksLikeObjective(objectiveBody) &&
        objectiveBody.length > 12 &&
        !looksLikeHypothesis(objectiveBody) &&
        // Opening confirmations like "as defined" are not objectives.
        ask !== 'opening'));
  if (shouldFillObjective && objectiveBody.length > 12) {
    // Avoid treating pure include/exclude answers as the objective.
    if (!/^(inclusion|exclusion|include|exclude)\b/i.test(objectiveBody)) {
      next.campaignObjective = objectiveBody;
    }
  }

  if (
    ask === 'targetSegment' ||
    ask === 'targetSubtype' ||
    looksLikeSegmentOrSubtype(text)
  ) {
    if (keepAsDefined) {
      // already seeded above
    } else if (/property managers?/i.test(text)) {
      next.targetSegment = 'property managers';
      const subtype = extractSubtypeFromText(text, context);
      if (subtype) next.targetSubtype = subtype;
    } else if (/professional offices?/i.test(text)) {
      next.targetSegment = 'professional offices';
      const subtype = extractSubtypeFromText(text, context);
      if (subtype) next.targetSubtype = subtype;
    } else if (ask === 'targetSegment' || ask === 'targetSubtype') {
      const subtype = extractSubtypeFromText(text, context);
      if (subtype) next.targetSubtype = subtype;
      if (!isSlotSatisfied(next, 'targetSegment') && text.length > 8) {
        next.targetSegment = stripTrailingMarket(
          text,
          (context && context.targetMarket) || ''
        );
      }
    }
  }

  const answeringOther =
    ask &&
    ![
      'opening',
      'marketBound',
      'proofAssets',
      'campaignHypothesis',
      'validationMetrics',
      'approvalCheckpoints',
      'targetSegment',
      'targetSubtype',
      'previewApproved',
      'prospectListCriteria',
      'inclusionCriteria',
      'exclusionCriteria',
      'inclusionExclusion',
      null,
      undefined,
    ].includes(ask);

  if (ask === 'marketBound' || (!answeringOther && looksLikeMarket(text))) {
    if (ask === 'marketBound' && text.length > 8 && !inclusion && !exclusion) {
      const cleaned = stripCriteriaClauses(text) || text;
      // Keep geography only — never stitch objective tails like
      // "Greater Manchester will engage in qualified conversations…".
      next.marketBound = extractMarketBoundPhrase(cleaned) || cleaned;
    } else if (looksLikeMarket(text) && (ask === 'marketBound' || ask === 'opening')) {
      const marketHit = extractMarketBoundPhrase(text);
      if (marketHit) next.marketBound = marketHit;
      else if (ask === 'marketBound') {
        next.marketBound = stripCriteriaClauses(text) || text;
      }
    }
  }

  if (ask === 'proofAssets' || (!answeringOther && looksLikeProof(text))) {
    if (ask === 'proofAssets' || (looksLikeProof(text) && ask === 'opening')) {
      const body = stripCriteriaClauses(text);
      if (body.length > 8) next.proofAssets = body;
    }
  }

  if (
    ask === 'campaignHypothesis' ||
    (!answeringOther && looksLikeHypothesis(text))
  ) {
    const body = stripCriteriaClauses(text);
    if (
      body.length > 12 &&
      (ask === 'campaignHypothesis' || looksLikeHypothesis(body))
    ) {
      next.campaignHypothesis = body;
    }
  }

  if (
    ask === 'validationMetrics' ||
    (!answeringOther && looksLikeMetrics(text))
  ) {
    const body = stripCriteriaClauses(text);
    if (
      body.length > 4 &&
      (ask === 'validationMetrics' || /\d/.test(body) || /metric/i.test(body))
    ) {
      next.validationMetrics = body;
    }
  }

  if (
    ask === 'approvalCheckpoints' ||
    (!answeringOther && looksLikeCheckpoints(text))
  ) {
    const body = stripCriteriaClauses(text);
    if (body.length > 6) next.approvalCheckpoints = body;
  }

  const requiredFieldsText = extractRequiredProspectFieldsText(text);
  if (requiredFieldsText) {
    next.requiredProspectFields = requiredFieldsText;
  }

  if (ask === 'inclusionCriteria' && !inclusion && text.length > 4) {
    next.inclusionCriteria =
      peelRecordFieldsClause(stripCriteriaClauses(text) || text) || text;
  }
  if (ask === 'exclusionCriteria' && !exclusion && text.length > 4) {
    next.exclusionCriteria =
      peelRecordFieldsClause(stripCriteriaClauses(text) || text) || text;
  }
  if (ask === 'inclusionExclusion' || ask === 'prospectListCriteria') {
    if (!inclusion && !exclusion) {
      // Split "include X / exclude Y" style without labels if possible.
      const parts = text.split(/\b(?:exclude|avoid|disqualify)\b/i);
      if (parts.length >= 2) {
        next.inclusionCriteria = peelRecordFieldsClause(
          parts[0]
            .replace(
              /^(?:include|inclusion(?:\s+criteria)?|qualify)\s*[:\-]?\s*/i,
              ''
            )
            .trim()
        );
        next.exclusionCriteria = peelRecordFieldsClause(
          parts.slice(1).join(' ').trim()
        );
      }
    }
  }

  // Never treat an approval acknowledgement as a planning-slot rewrite.
  if (
    ask === 'previewApproved' &&
    /\b(approve|approved|looks good|lgtm|accept|yes)\b/i.test(text) &&
    !detectReviseIntent(text)
  ) {
    next.previewApproved = true;
  }

  return next;
}

function promptForSlot(slotKey, context) {
  if (slotKey === 'targetSegment') {
    const primary =
      (context && context.primarySegment) || 'property managers';
    return SLOT_PROMPTS.targetSegment.replace(/property managers/i, primary);
  }
  return SLOT_PROMPTS[slotKey] || SLOT_PROMPTS.campaignObjective;
}

function criteriaSlotsReady(slots) {
  return (
    isSlotSatisfied(slots, 'inclusionCriteria') &&
    isSlotSatisfied(slots, 'exclusionCriteria')
  );
}

/**
 * Normalize criteria into clean bullets.
 * Never split on commas inside locations/roles — only newlines, bullets,
 * semicolons, or sentence boundaries.
 */
function normalizeCriteriaList(value) {
  if (Array.isArray(value)) {
    return uniqueStrings(
      value
        .map((x) => peelRecordFieldsClause(String(x || '').trim()))
        .filter(Boolean)
    ).slice(0, 8);
  }
  const s = peelRecordFieldsClause(String(value || '').trim());
  if (!s) return [];
  const listed = s
    .split(/\n|;|•|\u2022|(?<=\.)\s+(?=[A-Z])/g)
    .map((x) => x.replace(/^[-–—*\d.)\s]+/, '').trim())
    .filter((x) => x.length > 2);
  if (listed.length > 1) return uniqueStrings(listed).slice(0, 8);
  return [s];
}

function parseProspectFieldList(text) {
  return String(text || '')
    .split(/\n|;|•|\u2022|,(?=\s)/g)
    .map((x) => x.replace(/^[-–—*\d.)\s]+/, '').replace(/\.$/, '').trim())
    .filter((x) => x.length > 2 && x.length < 80)
    .filter((x) => !/^(each prospect|required)/i.test(x))
    // Reject proof-asset / criteria bleed that is not a CRM field label.
    .filter(
      (x) =>
        !/\b(photos?\/examples?|response-time|checklist|inclusion|exclusion|walkthrough)\b/i.test(
          x
        )
    )
    .map((x) => x.charAt(0).toUpperCase() + x.slice(1));
}

function normalizeRequiredProspectFields(value, answers, context) {
  if (Array.isArray(value) && value.length >= 3) {
    return uniqueStrings(
      value.map((x) => String(x).trim().replace(/\.$/, ''))
    ).slice(0, 10);
  }
  const fromSlot = String(value || '').trim();
  const candidates = [
    fromSlot,
    extractRequiredProspectFieldsText(fromSlot),
    // Prefer the active criteria answers only — never the full planning blob
    // (proof assets / hypothesis text can look like field lists).
    extractRequiredProspectFieldsText(
      [
        answerText(answers, 'inclusion_criteria'),
        answerText(answers, 'exclusion_criteria'),
        answerText(answers, 'prospect_list_criteria'),
        answerText(answers, 'campaign_objective'),
      ]
        .filter(Boolean)
        .join('\n')
    ),
  ].filter(Boolean);

  for (const labeled of candidates) {
    const fields = uniqueStrings(parseProspectFieldList(labeled)).slice(0, 10);
    if (fields.length >= 3) return fields;
  }
  return [...DEFAULT_REQUIRED_PROSPECT_FIELDS];
}

function normalizeReviewGate(value) {
  const s = String(value || '').replace(/\s+/g, ' ').trim();
  if (
    s.length >= 24 &&
    /approv|review|before\b/i.test(s) &&
    !/will engage|recurring clea/i.test(s)
  ) {
    return s.endsWith('.') ? s : `${s}.`;
  }
  return DEFAULT_REVIEW_GATE;
}

function looksTruncatedArtifactText(text) {
  const s = String(text || '').trim();
  if (!s) return true;
  if (/\b-\s*[A-Z]?[a-z]{0,2}$/.test(s)) return true; // e.g. "- Li"
  if (/\b(clea|manag|propert|conversat)$/i.test(s)) return true;
  if (/inclusion\s*:|exclusion\s*:/i.test(s)) return true;
  return false;
}

function buildProspectListCriteriaPreview(context, slots, opts = {}) {
  const ctx = context || {};
  const s = slots || {};
  const name = shortName(ctx.businessName || 'the business');
  const answers = opts.answers || syncAnswersFromSlots({}, s);
  const prior =
    opts.priorPreview && typeof opts.priorPreview === 'object'
      ? opts.priorPreview
      : null;

  // Structured synthesis only — never stitch raw slot/transcript fragments.
  const fields = extractCampaignPlanFields(ctx, answers);
  const slotInclusion = normalizeCriteriaList(s.inclusionCriteria);
  const slotExclusion = normalizeCriteriaList(s.exclusionCriteria);
  if (slotInclusion.length >= 3) fields.inclusionCriteria = slotInclusion;
  if (slotExclusion.length >= 3) fields.exclusionCriteria = slotExclusion;

  const objectivePart = normalizeObjectiveSection(ctx, answers, fields);
  const segmentPart = normalizeTargetSegmentSection(ctx, answers, fields);
  const marketBound = normalizeMarketBoundSection(ctx, {
    ...fields,
    marketBound:
      extractMarketBoundPhrase(s.marketBound || fields.marketBound || '') ||
      fields.marketBound,
  });

  const pickText = (priorVal, synthesized, fallback) => {
    if (priorVal && !looksTruncatedArtifactText(priorVal)) return priorVal;
    if (synthesized && !looksTruncatedArtifactText(synthesized)) {
      return synthesized;
    }
    return fallback || synthesized || priorVal || null;
  };

  const pickList = (slotList, priorList, synthesized) => {
    if (Array.isArray(slotList) && slotList.length >= 3) return slotList;
    if (Array.isArray(priorList) && priorList.length >= 3) return priorList;
    return synthesized || [];
  };

  const campaignObjective = pickText(
    prior && prior.campaignObjective,
    objectivePart.campaignObjective,
    defaultObjectiveParagraph(ctx, answers)
  );
  const targetSegment = pickText(
    prior && prior.targetSegment,
    segmentPart.targetSegment,
    defaultTargetSegmentBody(ctx)
  );
  const targetSubtype = pickText(
    prior && prior.targetSubtype,
    segmentPart.targetSubtype,
    defaultTargetSubtype(ctx, answers)
  );
  const market = pickText(
    prior && prior.marketBound,
    marketBound,
    defaultMarketBound(ctx)
  );
  const inclusionCriteria = pickList(
    slotInclusion,
    prior && prior.inclusionCriteria,
    segmentPart.inclusionCriteria
  );
  const exclusionCriteria = pickList(
    slotExclusion,
    prior && prior.exclusionCriteria,
    segmentPart.exclusionCriteria
  );
  const requiredProspectFields = normalizeRequiredProspectFields(
    s.requiredProspectFields ||
      (prior && prior.requiredProspectFields) ||
      null,
    answers,
    ctx
  );
  const reviewGate = normalizeReviewGate(
    s.reviewGate || (prior && prior.reviewGate) || null
  );

  return {
    kind: CRITERIA_ARTIFACT_KIND,
    title: CRITERIA_PREVIEW_TITLE,
    businessName: name,
    campaignObjective,
    targetSegment,
    targetSubtype,
    marketBound: market,
    inclusionCriteria,
    exclusionCriteria,
    requiredProspectFields,
    reviewGate,
    sectionTitles: { ...CRITERIA_SECTION_TITLES },
    planningOnly: true,
    prospectListGenerated: false,
    outreachCopyGenerated: false,
    accountChangesMade: false,
    campaignsGenerated: false,
    status: 'draft',
    disclaimer: CRITERIA_PREVIEW_DISCLAIMER,
    recommendedNextStep:
      'Review and approve these prospect-list criteria before any list is built. No outreach copy or launch steps yet.',
    generatedAt: new Date().toISOString(),
    blueprintId: opts.blueprintId || ctx.blueprintId || null,
    blueprintVersion: opts.blueprintVersion || ctx.blueprintVersion || null,
  };
}

function formatProspectListCriteriaPreviewMessage(preview) {
  const p = preview || {};
  const titles = p.sectionTitles || CRITERIA_SECTION_TITLES;
  const lines = [p.title || CRITERIA_PREVIEW_TITLE, ''];

  lines.push(`1. ${titles.campaignObjective}`);
  lines.push(p.campaignObjective || '—');
  lines.push('');

  lines.push(`2. ${titles.targetSegment}`);
  lines.push(p.targetSegment || '—');
  lines.push('');

  lines.push(`3. ${titles.targetSubtype}`);
  lines.push(p.targetSubtype || '—');
  lines.push('');

  lines.push(`4. ${titles.marketBound}`);
  lines.push(p.marketBound || '—');
  lines.push('');

  lines.push(`5. ${titles.inclusionCriteria}`);
  for (const item of p.inclusionCriteria || []) lines.push(`- ${item}`);
  if (!(p.inclusionCriteria || []).length) lines.push('- —');
  lines.push('');

  lines.push(`6. ${titles.exclusionCriteria}`);
  for (const item of p.exclusionCriteria || []) lines.push(`- ${item}`);
  if (!(p.exclusionCriteria || []).length) lines.push('- —');
  lines.push('');

  lines.push(`7. ${titles.requiredProspectFields}`);
  for (const item of p.requiredProspectFields || []) lines.push(`- ${item}`);
  if (!(p.requiredProspectFields || []).length) lines.push('- —');
  lines.push('');

  lines.push(`8. ${titles.reviewGate}`);
  lines.push(p.reviewGate || '—');
  lines.push('');

  lines.push(`9. ${titles.recommendedNextStep}`);
  lines.push(p.recommendedNextStep || '—');
  lines.push('');

  lines.push(p.disclaimer || CRITERIA_PREVIEW_DISCLAIMER);
  return lines.join('\n').trim();
}

function answerText(answers, step) {
  const a = answers && answers[step];
  if (!a) return '';
  return String(a.raw || a || '').trim();
}

function splitList(text) {
  const s = String(text || '').trim();
  if (!s) return [];
  return s
    .split(/\n|;|•|\u2022|(?<=\.)\s+(?=[A-Z])|,(?=\s)/g)
    .map((x) => x.replace(/^[-–—*\d.)\s]+/, '').trim())
    .filter((x) => x.length > 2)
    .slice(0, 12);
}

function uniqueStrings(items) {
  const out = [];
  const seen = new Set();
  for (const item of items || []) {
    const key = String(item || '')
      .trim()
      .toLowerCase();
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(String(item).trim());
  }
  return out;
}

function allAnswerBlob(answers) {
  const keys = [
    'opening',
    'campaign_objective',
    'target_segment',
    'market_bounds',
    'proof_assets',
    'hypothesis',
    'validation_metrics',
    'approval_checkpoints',
    'inclusion_criteria',
    'exclusion_criteria',
  ];
  return keys
    .map((k) => answerText(answers, k))
    .filter(Boolean)
    .join('\n\n');
}

function stripLabeledBlocks(text, labelGroups) {
  let s = String(text || '');
  for (const labels of labelGroups) {
    const labelRe = labels
      .map((l) => l.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'))
      .join('|');
    const re = new RegExp(
      `(?:^|\\n|;|\\.)\\s*(?:${labelRe})\\s*[:\\-–—]?\\s*[\\s\\S]*?(?=(?:\\n\\s*(?:[A-Z][\\w ]{2,40}:|Include|Exclude|Primary|Secondary|Available|Still|Before|Core validation|If\\b))|$)`,
      'gi'
    );
    s = s.replace(re, '\n');
  }
  return s.replace(/\s+/g, ' ').trim();
}

function extractLabeledSection(text, labels) {
  const s = String(text || '');
  if (!s) return '';
  const labelRe = labels
    .map((l) => l.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'))
    .join('|');
  const re = new RegExp(
    `(?:^|[\\n;.]\\s*)(?:${labelRe})\\s*[:\\-–—]?\\s*([\\s\\S]+?)(?=(?:[\\n.]\\s*)(?:inclusion(?:\\s+criteria)?|exclusion(?:\\s+criteria)?|include\\b|exclude\\b|avoid\\b|primary(?:\\s+signals?)?|secondary(?:\\s+signals?)?|available(?:\\s+or\\s+close\\s+to\\s+ready)?|still\\s+needed|before\\s+(?:list|launch)|core validation|campaign (?:objective|hypothesis)|target segment|market bound|proof assets|validation metrics|approval checkpoints|if\\s+\\w+)\\b|$)`,
    'i'
  );
  const m = s.match(re);
  if (!m || !m[1]) return '';
  return m[1].replace(/\s+/g, ' ').trim();
}

function extractBulletBlock(text, labels) {
  const s = String(text || '');
  if (!s) return [];
  const labelRe = labels
    .map((l) => l.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'))
    .join('|');
  const re = new RegExp(
    `(?:^|\\n)\\s*(?:${labelRe})\\s*[:\\-–—]?\\s*\\n?([\\s\\S]+?)(?=(?:\\n\\s*(?:${labelRe}|Include|Exclude|Primary|Secondary|Available|Still|Before|Core validation|If\\b)\\b)|$)`,
    'i'
  );
  const m = s.match(re);
  if (!m || !m[1]) {
    const inline = extractLabeledSection(text, labels);
    return inline ? splitList(inline.split(/\n\s*\n/)[0] || inline) : [];
  }
  const body = m[1];
  // Prefer explicit bullet lines so the next unlabeled answer paragraph
  // (joined into the blob) cannot leak into this section.
  const bulletLines = body
    .split(/\n/)
    .map((l) => l.trim())
    .filter((l) => /^[-–—•*\u2022]/.test(l) || /^\d+[.)]\s+/.test(l));
  if (bulletLines.length) {
    return bulletLines
      .map((x) => x.replace(/^[-–—*•\u2022\d.)\s]+/, '').trim())
      .filter((x) => x.length > 2)
      .slice(0, 12);
  }
  const firstPara = body.split(/\n\s*\n/)[0] || body;
  return splitList(firstPara);
}

function looksLikeMetricItem(item) {
  const s = String(item || '').trim();
  if (s.length < 16) return false;
  if (/sign[- ]?off|checkpoint|approved|preview/i.test(s)) return false;
  return /\b(repl(?:y|ies)|conversation|walkthrough|estimate|signal|question|booked|request|clarity|price)\b/i.test(
    s
  );
}

function looksLikeProofAssetItem(item) {
  const s = String(item || '').trim();
  if (s.length < 8) return false;
  return /\b(checklist|photo|example|response|reference|testimonial|service area|walkthrough|estimate|positioning|reliability|credibility|service mix|ideal customer|icp|review)\b/i.test(
    s
  );
}

function polishProofAssetLabel(item, { missing = false } = {}) {
  let s = String(item || '').trim();
  if (!s) return s;
  s = s
    .replace(/^the positioning is clear:\s*/i, '')
    .replace(/\s*\/\s*/g, ', ')
    .replace(/\s+/g, ' ')
    .trim();
  const lower = s.toLowerCase();
  if (/positioning|reliability.*responsiveness|responsiveness.*accountability/.test(lower)) {
    return 'Positioning around reliability, responsiveness, and accountability';
  }
  if (/checklist/.test(lower)) return 'Commercial cleaning checklist';
  if (/before\/after|photo|example|commercial work/.test(lower)) {
    return missing
      ? 'Before/after photos or commercial work examples'
      : 'Before/after photos or commercial work examples';
  }
  if (/response[- ]?time/.test(lower)) return 'Clear response-time promise';
  if (/reference|testimonial|review proof/.test(lower)) {
    return 'References, testimonials, or review proof if available';
  }
  if (/service mix|commercial cleaning focus/.test(lower)) {
    return 'Clear service mix and commercial cleaning focus';
  }
  if (/service area/.test(lower)) return 'Defined service area';
  if (/ideal customer|icp/.test(lower)) return 'Clear ideal customer profile';
  if (/credibility statement/.test(lower)) {
    return 'Short credibility statement for recurring commercial cleaning';
  }
  if (/walkthrough|estimate/.test(lower)) {
    return missing
      ? 'Reusable walkthrough/estimate process for property managers'
      : 'Walkthrough/estimate process that can be described simply';
  }
  // Drop truncated fragment tails.
  if (/^(reliability|responsiveness)\b/i.test(s) && s.length < 40) {
    return 'Positioning around reliability, responsiveness, and accountability';
  }
  return s.charAt(0).toUpperCase() + s.slice(1);
}

function polishCheckpointItem(item) {
  let s = String(item || '').trim();
  if (!s) return '';
  // Fix fragmented checklist sentences: "Target segment. subtype. and market…"
  s = s
    .replace(
      /\bTarget segment\.\s*subtype\.\s*and\b/gi,
      'Target segment, subtype, and'
    )
    .replace(
      /\bInclusion\.\s*(?:and\s+)?exclusion criteria\b/gi,
      'Inclusion and exclusion criteria'
    )
    .replace(
      /\bProof assets?\.\s*(?:are\s+)?ready\b/gi,
      'Proof assets are ready'
    )
    .replace(/\b([A-Za-z][A-Za-z ]+?)\.\s+([a-z]+)\.\s+and\b/g, '$1, $2, and')
    // Collapse remaining mid-item "Word. word." fragments into commas.
    .replace(/\b([A-Za-z][A-Za-z-]{1,24})\.\s+([a-z][a-z-]{1,24})\b/g, '$1, $2')
    .replace(/\s+/g, ' ')
    .trim();
  if (!/\.$/.test(s)) s = `${s}.`;
  return s.charAt(0).toUpperCase() + s.slice(1);
}

function looksFragmentedCheckpoint(item) {
  return /\b[A-Za-z][A-Za-z ]+\.\s+[a-z]+\./.test(String(item || ''));
}

function defaultTargetSegmentBody(context) {
  const market = (context && context.targetMarket) || 'Greater Manchester';
  if (/property manager/i.test(String(context && context.primarySegment))) {
    return (
      `Small to mid-sized local property managers in ${market} who oversee ` +
      `offices, mixed-use buildings, small commercial properties, or multi-tenant spaces ` +
      `that likely need recurring cleaning weekly or multiple times per week.`
    );
  }
  const primary = (context && context.primarySegment) || 'the focus segment';
  return `Small to mid-sized local ${primary} in ${market}, aligned with the approved Blueprint first focus.`;
}

function defaultTargetSubtype(context, answers) {
  if (isPropertyManagerFocus(context, answers)) {
    return DEFAULT_TARGET_SUBTYPE_PROPERTY_MANAGERS;
  }
  return null;
}

/**
 * Reject subtype values that are actually exclusion summaries or thin dash
 * suffixes from conversational answers (e.g. "multi-family / HOA subtype",
 * "price buyers, properties outside …").
 */
function looksLikeExclusionBleedSubtype(text) {
  const s = String(text || '').trim();
  if (!s) return true;
  if (
    /price buyers|lowest-price|service area|decision-maker|decision maker|no clear|institutional property|highly complex|prospects with/i.test(
      s
    )
  ) {
    return true;
  }
  // Joined exclusion-style lists: "X, Y, and Z"
  if (
    /,/.test(s) &&
    /\band\b/.test(s) &&
    /\b(buyers|properties|prospects)\b/i.test(s) &&
    !/overseeing|mixed-use|multi-tenant/i.test(s)
  ) {
    return true;
  }
  return false;
}

function normalizeTargetSubtype(context, answers, fields) {
  const raw = String((fields && fields.targetSubtype) || '')
    .replace(/\s+/g, ' ')
    .trim();
  if (
    raw &&
    /property managers overseeing/i.test(raw) &&
    /mixed-use|multi-tenant/i.test(raw) &&
    !looksLikeExclusionBleedSubtype(raw)
  ) {
    return raw.replace(/\.$/, '');
  }
  // Thin dash-suffix answers ("multi-family / HOA subtype") and exclusion
  // bleed must never render under Subtype.
  if (!raw || raw.length < 48 || looksLikeExclusionBleedSubtype(raw)) {
    return defaultTargetSubtype(context, answers);
  }
  if (isPropertyManagerFocus(context, answers)) {
    return defaultTargetSubtype(context, answers);
  }
  return raw.replace(/\.$/, '');
}

/**
 * Strip internal labels / awkward joins so the target segment never opens with
 * lowercase keys like "property managers — …".
 */
function sanitizeTargetSegmentText(text, context) {
  let s = stripCampaignWrappers(String(text || ''))
    .replace(/\s+/g, ' ')
    .trim();
  if (!s) return defaultTargetSegmentBody(context);

  s = s.replace(
    /^(?:the\s+)?property[_\s-]*managers?\s*[—–:-]+\s*/i,
    ''
  );
  s = s.replace(/^(?:segment|primary|target)\s*[—–:-]+\s*/i, '');
  // Strip inclusion/exclusion bleed if still present.
  s = stripLabeledBlocks(s, [
    ['include property managers who', 'include', 'inclusion criteria'],
    ['exclude property managers who', 'exclude', 'exclusion criteria', 'avoid'],
  ]);

  if (/^small to mid-sized/i.test(s)) {
    s = s
      .replace(/\bwho manage\b/i, 'who oversee')
      .replace(/\boverseeing\b/i, 'who oversee');
    const market = (context && context.targetMarket) || 'Greater Manchester';
    const mRe = new RegExp(
      `^(Small to mid-sized local property managers) who oversee (.+?) in ${market.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\.?$`,
      'i'
    );
    const m = s.match(mRe);
    if (m) {
      s = `${m[1]} in ${market} who oversee ${m[2]}.`;
    }
    if (
      /property manager/i.test(String(context && context.primarySegment)) &&
      !/mixed-use|multi-tenant/i.test(s)
    ) {
      return defaultTargetSegmentBody(context);
    }
  }

  if (
    !s ||
    /^[a-z]/.test(s) ||
    /^(property managers?|professional offices?)\b/i.test(s)
  ) {
    return defaultTargetSegmentBody(context);
  }

  if (!/\.$/.test(s)) s = `${s}.`;
  return s.charAt(0).toUpperCase() + s.slice(1);
}

function stripFirstPersonArtifactLanguage(text) {
  return String(text || '')
    .replace(
      /\bFor the first test,\s*I'd treat the goal as:\s*/gi,
      'Core validation question:\n'
    )
    .replace(/\bI'd\b/g, 'Max would')
    .replace(/\bI would\b/g, 'Max would')
    .replace(/\bI want\b/gi, 'The goal is')
    .replace(
      /not just ignore the outreach or shop on price/gi,
      'rather than ignoring the outreach or responding only on price'
    )
    .replace(
      /ignore the outreach or shop on price/gi,
      'ignoring the outreach or responding only on price'
    );
}

function ensureProveObjective(substance) {
  let s = stripCampaignWrappers(substance);
  s = stripFirstPersonArtifactLanguage(s);
  s = s
    .replace(/^core validation question[:\s]*/i, '')
    .replace(/\s+/g, ' ')
    .replace(/\.$/, '')
    .trim();
  if (!s) return '';
  // Avoid "Prove that The …" capitalization stitch.
  const body = /^[A-Z]/.test(s) ? s.charAt(0).toLowerCase() + s.slice(1) : s;
  return `Prove that ${body}.`;
}

function defaultObjectiveParagraph(context, answers) {
  const market = (context && context.targetMarket) || 'Greater Manchester';
  if (isPropertyManagerFocus(context, answers)) {
    return `Prove that small to mid-sized property managers in ${market} will engage in qualified conversations about recurring cleaning.`;
  }
  const segment = (context && context.primarySegment) || 'the focus segment';
  return `Prove that ${segment} in ${market} will engage in qualified conversations about recurring service.`;
}

function defaultCoreValidationQuestion(context, answers) {
  const name = displayName((context && context.businessName) || 'the business');
  if (isPropertyManagerFocus(context, answers)) {
    return `Can ${name} create qualified property-manager conversations that turn into walkthroughs or estimates?`;
  }
  return `Can ${name} create qualified conversations that turn into walkthroughs or estimates?`;
}

function defaultInclusionCriteria(context, answers) {
  if (isPropertyManagerFocus(context, answers)) {
    return [...DEFAULT_INCLUSION_CRITERIA];
  }
  return [
    'Match the approved Blueprint ideal-customer profile',
    `Are located in ${(context && context.targetMarket) || 'the approved market'}`,
    'Have a reachable decision-maker or operations contact',
  ];
}

function defaultExclusionCriteria(context, answers) {
  if (isPropertyManagerFocus(context, answers)) {
    const name = displayName((context && context.businessName) || 'the business');
    const poss =
      name === 'the business'
        ? "the business's"
        : /s$/i.test(name)
          ? `${name}'`
          : `${name}'s`;
    return [
      'Large institutional property managers',
      'Highly complex properties',
      'Lowest-price buyers',
      `Properties outside ${poss} service area`,
      'Prospects with no clear decision-maker or contact path',
    ];
  }
  const avoid = extractAvoidPhrase(
    (context && context.avoidPhrase) || 'buyers focused only on the lowest price'
  );
  const lowestPrice = /lowest price|price only|cheapest/i.test(avoid)
    ? 'Lowest-price buyers'
    : avoid.charAt(0).toUpperCase() + avoid.slice(1);
  return [
    lowestPrice,
    'Properties outside the approved service area',
    'Prospects with no clear decision-maker or contact path',
  ];
}

function looksLikePolishedExclusionItem(item) {
  const s = String(item || '').trim();
  if (!s || s.length < 8) return false;
  if (/prefers to avoid|should avoid|the business prefers/i.test(s)) return false;
  return /^(Large institutional|Highly complex|Lowest-price|Properties outside|Prospects with)\b/i.test(
    s
  );
}

function defaultMarketBound(context) {
  const market = (context && context.targetMarket) || 'Greater Manchester';
  const towns = (
    context && Array.isArray(context.towns) && context.towns.length
      ? context.towns
      : DEFAULT_TOWNS
  ).slice(0, 5);
  return (
    `Start with ${naturalList(towns)}. Keep ${market} in scope, ` +
    `but keep the first test tight enough to learn quickly.`
  );
}

function defaultHypothesis(context, answers) {
  const name = displayName((context && context.businessName) || 'the business');
  if (isPropertyManagerFocus(context, answers)) {
    return (
      `If ${name} approaches small to mid-sized local property managers with clear proof of ` +
      `reliability, responsiveness, and a simple walkthrough path, the campaign should produce ` +
      `qualified conversations and at least some walkthrough or estimate interest within the ` +
      `first validation window.`
    );
  }
  const segment =
    (context && context.primarySegment) || 'the focus segment';
  const market = (context && context.targetMarket) || 'Greater Manchester';
  return (
    `If ${name} approaches ${segment} in ${market} with clear proof of reliability, ` +
    `responsiveness, and a simple walkthrough path, the campaign should produce qualified ` +
    `conversations and at least some walkthrough or estimate interest within the first validation window.`
  );
}

function defaultRisks(context, answers, fields) {
  const risks = [];
  const readiness = context && context.readinessOverallStatus;
  if (readiness && readiness !== 'ready') {
    risks.push(
      'Some growth infrastructure items may still need review before launch.'
    );
  } else if (!(context && context.completedSetupChecklist)) {
    risks.push(
      'Some growth infrastructure items may still need review before launch.'
    );
  }
  const missing =
    (fields && fields.proofAssetsMissing) ||
    DEFAULT_PROOF_ASSETS_MISSING;
  if (missing.length) {
    risks.push('Proof assets are incomplete.');
  }
  risks.push('Market demand has not been validated yet.');
  risks.push('Strong response could create capacity or scheduling pressure.');
  risks.push('Lowest-price buyers should not define the test.');
  return uniqueStrings(risks).slice(0, 6);
}

/**
 * Extract structured campaign-plan fields from operator answers.
 * Cross-section bleed (criteria in objective, metrics in hypothesis) is
 * peeled into the matching field arrays — never left as raw transcript.
 */
function extractCampaignPlanFields(context, answers) {
  const blob = allAnswerBlob(answers);
  const objRaw = answerText(answers, 'campaign_objective');
  const segmentRaw = answerText(answers, 'target_segment');
  const marketRaw = answerText(answers, 'market_bounds');
  const proofRaw = answerText(answers, 'proof_assets');
  const hypRaw = answerText(answers, 'hypothesis');
  const metricsRaw = answerText(answers, 'validation_metrics');
  const approvalRaw = answerText(answers, 'approval_checkpoints');
  const inclusionRaw = answerText(answers, 'inclusion_criteria');
  const exclusionRaw = answerText(answers, 'exclusion_criteria');

  const inclusionFromLabels = [
    ...extractBulletBlock(blob, [
      'include property managers who',
      'include',
      'includes',
      'inclusion criteria',
      'must include',
    ]),
    ...splitList(inclusionRaw),
  ];
  const exclusionFromLabels = [
    ...extractBulletBlock(blob, [
      'exclude property managers who',
      'exclude',
      'excludes',
      'exclusion criteria',
      'must exclude',
      'avoid',
    ]),
    ...splitList(exclusionRaw),
  ];

  let objectiveSubstance = objRaw;
  // Peel criteria / metrics / core-question blocks out of the objective answer.
  const coreFromObj =
    extractLabeledSection(objRaw, [
      'core validation question',
      'core question',
    ]) ||
    extractLabeledSection(blob, ['core validation question', 'core question']);
  objectiveSubstance = stripLabeledBlocks(objectiveSubstance, [
    ['include property managers who', 'include', 'inclusion criteria'],
    ['exclude property managers who', 'exclude', 'exclusion criteria', 'avoid'],
    ['primary signals', 'primary', 'secondary signals', 'secondary', 'validation metrics'],
    ['core validation question', 'core question'],
  ]);
  objectiveSubstance = stripCampaignWrappers(
    stripFirstPersonArtifactLanguage(objectiveSubstance)
  );
  // Unlabeled prose: "We should include … and exclude …"
  const peeledObjective = peelInlineIncludeExclude(objectiveSubstance);
  objectiveSubstance = peeledObjective.text;
  inclusionFromLabels.push(...peeledObjective.inclusion);
  exclusionFromLabels.push(...peeledObjective.exclusion);

  // Hypothesis: keep If/then only — peel metrics.
  let hypothesisSubstance = hypRaw;
  const metricsFromHypPrimary = extractBulletBlock(hypRaw, [
    'primary signals',
    'primary',
  ]);
  const metricsFromHypSecondary = extractBulletBlock(hypRaw, [
    'secondary signals',
    'secondary',
  ]);
  hypothesisSubstance = stripLabeledBlocks(hypothesisSubstance, [
    ['primary signals', 'primary', 'secondary signals', 'secondary', 'validation metrics'],
    ['include', 'exclude', 'inclusion criteria', 'exclusion criteria'],
  ]);
  hypothesisSubstance = stripCampaignWrappers(
    stripFirstPersonArtifactLanguage(hypothesisSubstance)
  );
  // Drop residual unlabeled metric sentences that still cling to the If/then.
  hypothesisSubstance = hypothesisSubstance
    .replace(
      /\s*(?:Primary|Secondary)\s+signals?\s*[:\-–—]?\s*[^.]*(?:\.|$)/gi,
      ''
    )
    .replace(/\s+/g, ' ')
    .trim();

  const available = [
    ...extractBulletBlock(proofRaw, [
      'available or close to ready',
      'available',
      'already have',
      'ready',
    ]),
    ...extractBulletBlock(blob, ['available or close to ready']),
  ]
    .map((x) => polishProofAssetLabel(x, { missing: false }))
    .filter(looksLikeProofAssetItem);

  const missing = [
    ...extractBulletBlock(proofRaw, [
      'still needed or should be packaged',
      'still needed',
      'missing',
      'need',
      'needed',
    ]),
    ...extractBulletBlock(blob, ['still needed or should be packaged']),
  ]
    .map((x) => polishProofAssetLabel(x, { missing: true }))
    .filter(looksLikeProofAssetItem);

  // If proof answer is a flat list without labels, split on need/missing cues.
  if (!available.length && !missing.length && proofRaw) {
    const parts = splitList(proofRaw);
    for (const part of parts) {
      if (/missing|still need|need to|don't have|do not have/i.test(part)) {
        const label = polishProofAssetLabel(part, { missing: true });
        if (looksLikeProofAssetItem(label)) missing.push(label);
      } else {
        const label = polishProofAssetLabel(part, { missing: false });
        if (looksLikeProofAssetItem(label)) available.push(label);
      }
    }
  }

  const primaryMetrics = uniqueStrings([
    ...extractBulletBlock(metricsRaw, ['primary signals', 'primary']),
    ...extractBulletBlock(blob, ['primary signals']),
    ...metricsFromHypPrimary,
  ]).filter(looksLikeMetricItem);
  const secondaryMetrics = uniqueStrings([
    ...extractBulletBlock(metricsRaw, ['secondary signals', 'secondary']),
    ...extractBulletBlock(blob, ['secondary signals']),
    ...metricsFromHypSecondary,
  ]).filter(looksLikeMetricItem);

  // Flat metrics answer with no labels — only trust when it already looks like
  // a real metric list (never a thin "3 conversations" fragment).
  if (!primaryMetrics.length && metricsRaw) {
    const cleanedMetrics = stripLabeledBlocks(metricsRaw, [
      ['a successful first 30 days', 'success means'],
    ]);
    const listed = splitList(cleanedMetrics).filter(looksLikeMetricItem);
    for (const item of listed) {
      if (/secondary|not only about|which .* responds/i.test(item)) {
        secondaryMetrics.push(item);
      } else {
        primaryMetrics.push(item);
      }
    }
  }

  const beforeList = extractBulletBlock(approvalRaw, [
    'before list-building',
    'before list building',
    'before any list',
  ]).map(polishCheckpointItem);
  const beforeLaunch = extractBulletBlock(approvalRaw, [
    'before launch',
    'before go-live',
  ]).map(polishCheckpointItem);
  let flatCheckpoints = [];
  if (!beforeList.length && !beforeLaunch.length && approvalRaw) {
    flatCheckpoints = splitList(approvalRaw).map(polishCheckpointItem);
  }

  const subtypeDash = segmentRaw.match(/[—–-]\s*(.+)$/);
  const targetSubtype =
    (subtypeDash && subtypeDash[1] && subtypeDash[1].trim()) ||
    (context && context.subtype) ||
    null;

  const marketTownsOnly = (() => {
    let m = String(marketRaw || '').trim();
    if (!m) return '';
    // Drop non-geography bleed.
    m = stripLabeledBlocks(m, [
      ['include', 'exclude', 'primary', 'secondary', 'proof'],
    ]);
    return m.replace(/\s+/g, ' ').trim();
  })();

  return {
    objective: objectiveSubstance,
    coreValidationQuestion: coreFromObj
      ? stripCampaignWrappers(coreFromObj).replace(/\?*$/, '?')
      : '',
    inclusionCriteria: uniqueStrings(inclusionFromLabels).slice(0, 8),
    exclusionCriteria: uniqueStrings(
      exclusionFromLabels.map((item) => {
        const cleaned = extractAvoidPhrase(item);
        // If cleaner collapsed a wrapper-only phrase, keep polished item text.
        if (
          cleaned === 'buyers focused only on the lowest price' &&
          !/lowest price|price/i.test(item)
        ) {
          return polishCheckpointItem(item).replace(/\.$/, '');
        }
        return cleaned.charAt(0).toUpperCase() + cleaned.slice(1);
      })
    ).slice(0, 8),
    targetSegment: segmentRaw,
    targetSubtype: targetSubtype ? String(targetSubtype).trim() : null,
    marketBound: marketTownsOnly,
    hypothesis: hypothesisSubstance,
    proofAssetsAvailable: uniqueStrings(available).slice(0, 8),
    proofAssetsMissing: uniqueStrings(missing).slice(0, 8),
    validationMetricsPrimary: uniqueStrings(primaryMetrics).slice(0, 8),
    validationMetricsSecondary: uniqueStrings(secondaryMetrics).slice(0, 8),
    approvalCheckpointsBeforeListBuilding: uniqueStrings(beforeList).slice(0, 8),
    approvalCheckpointsBeforeLaunch: uniqueStrings(beforeLaunch).slice(0, 8),
    approvalCheckpointsFlat: uniqueStrings(flatCheckpoints).slice(0, 12),
    notes: null,
  };
}

function normalizeObjectiveSection(context, answers, fields) {
  const substance = fields && fields.objective;
  let objective;
  if (substance && substance.length > 24 && !objectiveHasCrossSectionBleed(substance)) {
    objective = ensureProveObjective(substance);
    // Prefer the polished Anchor default when the cleaned substance is still
    // a thin fragment or still contains stitched wrapper / cross-section residue.
    if (
      objectiveHasCrossSectionBleed(objective) ||
      /prove that prove/i.test(objective) ||
      objective.length < 40
    ) {
      objective = defaultObjectiveParagraph(context, answers);
    }
  } else {
    objective = defaultObjectiveParagraph(context, answers);
  }
  let core =
    (fields && fields.coreValidationQuestion) ||
    defaultCoreValidationQuestion(context, answers);
  core = stripCampaignWrappers(String(core || ''))
    .replace(/^core validation question[:\s]*/i, '')
    .replace(/\?*$/, '?')
    .trim();
  if (!core || core.length < 12) {
    core = defaultCoreValidationQuestion(context, answers);
  }
  return {
    objective,
    // Structured fields only — do not bundle core into campaignObjective.
    coreValidationQuestion: core,
    campaignObjective: objective,
  };
}

function normalizeTargetSegmentSection(context, answers, fields) {
  const opening = answerText(answers, 'opening');
  const segmentAnswer = (fields && fields.targetSegment) || '';
  const keepAsDefined =
    /\bas defined\b|\bexactly as\b|\bas-is\b|\bkeep (it |them )?as\b/i.test(
      `${opening} ${segmentAnswer}`
    );
  const awkwardJoin = /property[_\s-]*managers?\s*[—–:-]\s*/i.test(
    `${segmentAnswer} ${fields && fields.targetSubtype}`
  );
  let targetSegment;
  if (
    !segmentAnswer ||
    keepAsDefined ||
    awkwardJoin ||
    segmentAnswer.length < 48
  ) {
    const subtype = fields && fields.targetSubtype;
    if (subtype && /^small to mid-sized/i.test(subtype) && !keepAsDefined) {
      targetSegment = sanitizeTargetSegmentText(subtype, context);
    } else {
      targetSegment = defaultTargetSegmentBody(context);
    }
  } else {
    targetSegment = sanitizeTargetSegmentText(segmentAnswer, context);
  }

  const inclusionCriteria =
    fields && fields.inclusionCriteria && fields.inclusionCriteria.length >= 3
      ? fields.inclusionCriteria
      : defaultInclusionCriteria(context, answers);

  // Property-manager first campaigns use the polished exclusion set. Operator
  // lists only win when every item already matches that shape (never raw
  // transcript / avoid-wrapper bleed, and never as Subtype content).
  let exclusionCriteria = defaultExclusionCriteria(context, answers);
  if (
    fields &&
    Array.isArray(fields.exclusionCriteria) &&
    fields.exclusionCriteria.length >= 3 &&
    fields.exclusionCriteria.every(looksLikePolishedExclusionItem)
  ) {
    exclusionCriteria = fields.exclusionCriteria;
  }

  return {
    targetSegment,
    targetSubtype: normalizeTargetSubtype(context, answers, fields),
    inclusionCriteria,
    exclusionCriteria,
  };
}

function normalizeMarketBoundSection(context, fields) {
  const raw = fields && fields.marketBound;
  if (raw && /bedford|hooksett|londonderry|auburn|goffstown|manchester/i.test(raw)) {
    // Keep towns/cluster language only; if the operator already wrote the
    // polished form, use it after light cleanup.
    let s = String(raw).replace(/\s+/g, ' ').trim();
    if (/^start with\b/i.test(s)) {
      if (!/\.$/.test(s)) s = `${s}.`;
      if (!/tight enough to learn quickly/i.test(s) && /greater manchester/i.test(s)) {
        s = s.replace(/\.$/, '') + ', but keep the first test tight enough to learn quickly.';
      }
      return s;
    }
    const towns = (
      context && Array.isArray(context.towns) && context.towns.length
        ? context.towns
        : DEFAULT_TOWNS
    ).slice(0, 5);
    const mentioned = towns.filter((t) => new RegExp(`\\b${t}\\b`, 'i').test(s));
    if (mentioned.length >= 3) {
      return defaultMarketBound({ ...context, towns: mentioned });
    }
  }
  return defaultMarketBound(context);
}

function normalizeHypothesisSection(context, answers, fields) {
  let h = (fields && fields.hypothesis) || '';
  h = stripFirstPersonArtifactLanguage(h).replace(/\s+/g, ' ').trim();
  const market = (context && context.targetMarket) || 'Greater Manchester';
  const name = displayName((context && context.businessName) || 'the business');
  if (h) {
    h = h.replace(
      new RegExp(
        `\\bin\\s+${market.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\s+in\\s+${market.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}`,
        'gi'
      ),
      `in ${market}`
    );
    // Rewrite thin first-person hypotheses onto the business subject.
    h = h.replace(/^if\s+we\s+approach\b/i, `If ${name} approaches`);
    // Thin "with proof, we expect X" sketches are not artifact-ready — use default.
    if (
      /\bwith proof,\s*we expect\b/i.test(h) ||
      h.length < 80 ||
      hypothesisHasCrossSectionBleed(h)
    ) {
      return defaultHypothesis(context, answers);
    }
    // Require If/then shape without metric/criteria bleed.
    if (/^if\b/i.test(h) && !hypothesisHasCrossSectionBleed(h)) {
      if (!/\.$/.test(h)) h = `${h}.`;
      return h;
    }
  }
  return defaultHypothesis(context, answers);
}

function looksTruncatedProofAsset(item) {
  const s = String(item || '').trim();
  if (!s) return true;
  if (/the positioning is clear:/i.test(s)) return true;
  if (/\/\s*Responsiveness\b/i.test(s)) return true;
  if (/^(reliability|responsiveness)\b/i.test(s) && s.length < 48) return true;
  if (/\breliability\s*\/\s*responsiveness\b/i.test(s) && s.length < 60) {
    return true;
  }
  return false;
}

function normalizeProofAssetsSection(fields) {
  // Trust operator lists only when they are complete enough and free of
  // truncated transcript fragments; otherwise use polished defaults.
  const rawAvailable = (fields && fields.proofAssetsAvailable) || [];
  const rawMissing = (fields && fields.proofAssetsMissing) || [];
  const availableClean = rawAvailable.filter((x) => !looksTruncatedProofAsset(x));
  const missingClean = rawMissing.filter((x) => !looksTruncatedProofAsset(x));
  const available =
    availableClean.length >= 4
      ? availableClean
      : [...DEFAULT_PROOF_ASSETS_AVAILABLE];
  const missing =
    missingClean.length >= 4 ? missingClean : [...DEFAULT_PROOF_ASSETS_MISSING];
  return {
    proofAssetsAvailable: uniqueStrings(available).slice(0, 8),
    proofAssetsMissing: uniqueStrings(missing).slice(0, 8),
  };
}

function defaultPrimaryValidationMetrics(context, answers) {
  if (!isPropertyManagerFocus(context, answers)) {
    return [
      'Qualified replies',
      'Discovery conversations booked',
      'Walkthroughs or site visits requested',
      'Estimate requests from properties that fit the target',
    ];
  }
  const name = displayName((context && context.businessName) || 'the business');
  const poss =
    name === 'the business' ? 'the' : /s$/i.test(name) ? `${name}'` : `${name}'s`;
  return [
    'Qualified replies from property managers',
    'Discovery conversations booked',
    'Walkthroughs or site visits requested',
    `Estimate requests from properties that fit ${poss} target`,
  ];
}

function normalizeValidationMetricsSection(context, answers, fields) {
  const primary =
    fields &&
    fields.validationMetricsPrimary &&
    fields.validationMetricsPrimary.length >= 3 &&
    fields.validationMetricsPrimary.every(looksLikeMetricItem)
      ? fields.validationMetricsPrimary
      : defaultPrimaryValidationMetrics(context, answers);
  const secondary =
    fields &&
    fields.validationMetricsSecondary &&
    fields.validationMetricsSecondary.length >= 2 &&
    fields.validationMetricsSecondary.every(looksLikeMetricItem)
      ? fields.validationMetricsSecondary
      : [...DEFAULT_VALIDATION_METRICS_SECONDARY];
  return {
    validationMetricsPrimary: uniqueStrings(primary).slice(0, 8),
    validationMetricsSecondary: uniqueStrings(secondary).slice(0, 8),
    validationSuccessStatement: VALIDATION_SUCCESS_STATEMENT,
  };
}

function normalizeApprovalCheckpointsSection(fields) {
  const polishList = (items) =>
    (items || [])
      .map(polishCheckpointItem)
      .filter((x) => x && !looksFragmentedCheckpoint(x));

  let beforeList = polishList(
    fields && fields.approvalCheckpointsBeforeListBuilding
  );
  let beforeLaunch = polishList(
    fields && fields.approvalCheckpointsBeforeLaunch
  );

  if (beforeList.length < 3) beforeList = [...DEFAULT_APPROVAL_BEFORE_LIST];
  if (beforeLaunch.length < 3) {
    beforeLaunch = [...DEFAULT_APPROVAL_BEFORE_LAUNCH];
  }

  return {
    approvalCheckpointsBeforeList: uniqueStrings(beforeList),
    approvalCheckpointsBeforeListBuilding: uniqueStrings(beforeList),
    approvalCheckpointsBeforeLaunch: uniqueStrings(beforeLaunch),
    approvalCheckpoints: uniqueStrings([...beforeList, ...beforeLaunch]),
  };
}

function resolveRecommendedNextStep() {
  return 'Review and approve this preview. After approval, Max can help define prospect-list criteria before any list is built.';
}

function buildFirstCampaignPlanPreview(context, answers, opts = {}) {
  const ctx = context || {};
  const ans = answers || {};
  const fields = extractCampaignPlanFields(ctx, ans);

  const objectivePart = normalizeObjectiveSection(ctx, ans, fields);
  const segmentPart = normalizeTargetSegmentSection(ctx, ans, fields);
  const marketBound = normalizeMarketBoundSection(ctx, fields);
  const hypothesis = normalizeHypothesisSection(ctx, ans, fields);
  const proofPart = normalizeProofAssetsSection(fields);
  const metricsPart = normalizeValidationMetricsSection(ctx, ans, fields);
  const approvalPart = normalizeApprovalCheckpointsSection(fields);
  const risksCautions = defaultRisks(ctx, ans, proofPart);
  const name = shortName(ctx.businessName || 'the business');

  return {
    kind: ARTIFACT_KIND,
    title: PREVIEW_TITLE,
    businessName: name,
    // Structured fields (canonical contract for renderers)
    campaignObjective: objectivePart.campaignObjective,
    coreValidationQuestion: objectivePart.coreValidationQuestion,
    targetSegment: segmentPart.targetSegment,
    targetSubtype: segmentPart.targetSubtype,
    inclusionCriteria: segmentPart.inclusionCriteria,
    exclusionCriteria: segmentPart.exclusionCriteria,
    marketBound,
    campaignHypothesis: hypothesis,
    proofAssetsAvailable: proofPart.proofAssetsAvailable,
    proofAssetsMissing: proofPart.proofAssetsMissing,
    validationMetricsPrimary: metricsPart.validationMetricsPrimary,
    validationMetricsSecondary: metricsPart.validationMetricsSecondary,
    risks: risksCautions,
    approvalCheckpointsBeforeList:
      approvalPart.approvalCheckpointsBeforeList,
    approvalCheckpointsBeforeLaunch:
      approvalPart.approvalCheckpointsBeforeLaunch,
    recommendedNextStep: resolveRecommendedNextStep(),
    // Compat aliases for older UI / callers
    objective: objectivePart.objective,
    hypothesis,
    proofAssetsNeeded: proofPart.proofAssetsMissing,
    validationMetrics: [
      ...metricsPart.validationMetricsPrimary,
      ...metricsPart.validationMetricsSecondary,
    ],
    validationSuccessStatement: metricsPart.validationSuccessStatement,
    risksCautions,
    approvalCheckpointsBeforeListBuilding:
      approvalPart.approvalCheckpointsBeforeListBuilding,
    approvalCheckpoints: approvalPart.approvalCheckpoints,
    targetSegmentAvoid: null,
    notes: fields.notes || null,
    sectionTitles: { ...SECTION_TITLES },
    planningOnly: true,
    directional: true,
    campaignsGenerated: false,
    prospectListGenerated: false,
    outreachCopyGenerated: false,
    accountChangesMade: false,
    status: 'draft',
    disclaimer: PREVIEW_DISCLAIMER,
    inputs: {
      hasApprovedBlueprint: Boolean(ctx.blueprintId),
      hasInitialGrowthDirection: Boolean(ctx.initialGrowthDirection),
      hasSegmentRanking: Boolean(ctx.segmentRanking),
      hasValidationTarget: Boolean(ctx.validationTarget),
      hasFirstGrowthPlanPreview: Boolean(ctx.firstGrowthPlanPreview),
      hasReadinessReport: Boolean(ctx.readinessReport),
      hasCompletedSetupChecklist: Boolean(ctx.completedSetupChecklist),
    },
    context: {
      primarySegment: ctx.primarySegment || null,
      secondarySegment: ctx.secondarySegment || null,
      targetMarket: ctx.targetMarket || null,
      towns: ctx.towns || null,
      readinessOverallStatus: ctx.readinessOverallStatus || null,
      readinessOverallStatusLabel: humanizeStatusLabel(
        ctx.readinessOverallStatus
      ),
    },
    generatedAt: new Date().toISOString(),
    blueprintId: opts.blueprintId || ctx.blueprintId || null,
    blueprintVersion: opts.blueprintVersion || ctx.blueprintVersion || null,
  };
}

function formatMultilineSection(text) {
  return String(text || '—')
    .split(/\n/)
    .map((line) => line.trimEnd())
    .join('\n');
}

function formatBulletBlock(label, items) {
  const lines = [label];
  const list = items || [];
  if (!list.length) {
    lines.push('- —');
    return lines;
  }
  for (const item of list) lines.push(`- ${item}`);
  return lines;
}

function formatFirstCampaignPlanPreviewMessage(preview) {
  const p = preview || {};
  const titles = p.sectionTitles || SECTION_TITLES;
  const lines = [p.title || PREVIEW_TITLE, ''];

  // Render from structured fields only — never dump raw transcript/notes here.
  const objective = p.campaignObjective || p.objective || '—';
  const coreQ = p.coreValidationQuestion || '';
  const hypothesis = p.campaignHypothesis || p.hypothesis || '—';
  const risks = p.risks || p.risksCautions || [];
  const beforeList =
    p.approvalCheckpointsBeforeList ||
    p.approvalCheckpointsBeforeListBuilding ||
    [];
  const beforeLaunch = p.approvalCheckpointsBeforeLaunch || [];

  lines.push(`1. ${titles.campaignObjective}`);
  lines.push(formatMultilineSection(objective));
  if (coreQ) {
    lines.push('');
    lines.push('Core validation question:');
    lines.push(coreQ);
  }
  lines.push('');

  lines.push(`2. ${titles.targetSegment}`);
  lines.push(p.targetSegment || '—');
  if (p.targetSubtype) {
    lines.push(`Subtype: ${p.targetSubtype}`);
  }
  lines.push('');
  const includeLabel = /property manager/i.test(
    String((p.context && p.context.primarySegment) || p.targetSegment || '')
  )
    ? 'Include property managers who:'
    : 'Include accounts who:';
  const excludeLabel = /property manager/i.test(
    String((p.context && p.context.primarySegment) || p.targetSegment || '')
  )
    ? 'Exclude property managers who:'
    : 'Exclude accounts who:';
  lines.push(...formatBulletBlock(includeLabel, p.inclusionCriteria));
  lines.push('');
  lines.push(...formatBulletBlock(excludeLabel, p.exclusionCriteria));
  lines.push('');

  lines.push(`3. ${titles.marketBound}`);
  lines.push(p.marketBound || '—');
  lines.push('');

  lines.push(`4. ${titles.hypothesis}`);
  lines.push(hypothesis);
  lines.push('');

  lines.push(`5. ${titles.proofAssets || titles.proofAssetsNeeded}`);
  lines.push(
    ...formatBulletBlock('Available or close to ready:', p.proofAssetsAvailable)
  );
  lines.push('');
  lines.push(
    ...formatBulletBlock(
      'Still needed or should be packaged:',
      p.proofAssetsMissing || p.proofAssetsNeeded
    )
  );
  lines.push('');

  lines.push(`6. ${titles.validationMetrics}`);
  lines.push(
    ...formatBulletBlock('Primary signals:', p.validationMetricsPrimary)
  );
  lines.push('');
  lines.push(
    ...formatBulletBlock('Secondary signals:', p.validationMetricsSecondary)
  );
  lines.push('');
  lines.push(p.validationSuccessStatement || VALIDATION_SUCCESS_STATEMENT);
  lines.push('');

  lines.push(`7. ${titles.risksCautions}`);
  for (const item of risks) lines.push(`- ${item}`);
  if (!risks.length) lines.push('- —');
  lines.push('');

  lines.push(`8. ${titles.approvalCheckpoints}`);
  lines.push(...formatBulletBlock('Before list-building:', beforeList));
  lines.push('');
  lines.push(...formatBulletBlock('Before launch:', beforeLaunch));
  lines.push('');

  lines.push(`9. ${titles.recommendedNextStep}`);
  lines.push(p.recommendedNextStep || '—');
  lines.push('');

  if (p.notes) {
    lines.push('Notes:');
    lines.push(formatMultilineSection(p.notes));
    lines.push('');
  }

  lines.push(p.disclaimer || PREVIEW_DISCLAIMER);

  return lines.join('\n').trim();
}

function produceCampaignPlanPreviewResult(ctx, answers, slots, opts, leadIn) {
  const preview = buildFirstCampaignPlanPreview(ctx, answers, {
    blueprintId: opts.blueprintId || ctx.blueprintId,
    blueprintVersion: opts.blueprintVersion || ctx.blueprintVersion,
  });
  const nextSlots = { ...slots, previewGenerated: true };
  const lines = [];
  if (leadIn) lines.push(leadIn, '');
  lines.push(formatFirstCampaignPlanPreviewMessage(preview));

  // If inclusion/exclusion were already captured, continue straight into the
  // Prospect List Criteria Preview instead of looping on planning questions.
  if (criteriaSlotsReady(nextSlots)) {
    const criteriaPreview = buildProspectListCriteriaPreview(ctx, nextSlots, {
      blueprintId: opts.blueprintId || ctx.blueprintId,
      blueprintVersion: opts.blueprintVersion || ctx.blueprintVersion,
      answers,
      priorPreview: preview,
    });
    lines.push('');
    lines.push(
      'I also captured your prospect-list criteria. Here is the Prospect List Criteria Preview — still planning-only.'
    );
    lines.push('');
    lines.push(formatProspectListCriteriaPreviewMessage(criteriaPreview));
    return {
      message: lines.join('\n'),
      step: 'prospect_list_criteria_preview',
      answers,
      slots: nextSlots,
      preview,
      criteriaPreview,
      intent: 'produce_criteria_preview',
      currentAsk: null,
    };
  }

  lines.push('');
  lines.push(SLOT_PROMPTS.previewApproved);
  return {
    message: lines.join('\n'),
    step: 'preview',
    answers,
    slots: nextSlots,
    preview,
    criteriaPreview: null,
    intent: 'produce_preview',
    currentAsk: 'previewApproved',
  };
}

function produceCriteriaPreviewResult(ctx, answers, slots, opts, leadIn) {
  const priorPreview = opts.priorPreview || null;
  const criteriaPreview = buildProspectListCriteriaPreview(ctx, slots, {
    blueprintId: opts.blueprintId || ctx.blueprintId,
    blueprintVersion: opts.blueprintVersion || ctx.blueprintVersion,
    answers,
    priorPreview,
  });
  const approved = markCampaignPlanPreviewApproved(
    priorPreview,
    { ...slots, previewApproved: true, previewGenerated: true },
    opts
  );
  const lines = [];
  if (leadIn) lines.push(leadIn, '');
  lines.push(formatProspectListCriteriaPreviewMessage(criteriaPreview));
  return {
    message: lines.join('\n'),
    step: 'prospect_list_criteria_preview',
    answers,
    slots: approved.slots,
    preview: approved.preview,
    criteriaPreview,
    intent: 'produce_criteria_preview',
    previewApproved: true,
  };
}

/**
 * Deterministic reply for the campaign planning conversation.
 * Slot/state-driven: infers answered slots from prior artifacts and user text,
 * skips satisfied slots, and only re-asks on explicit revise/change/update.
 *
 * @returns {{ message: string, step: string, answers: object, slots: object, preview: object|null, criteriaPreview: object|null, intent: string|null }}
 */
function buildCampaignPlanningReply(userMessage, state, context, opts = {}) {
  const prior = state || {};
  const ctx = context || prior.context || {};
  const priorPreview =
    opts.priorPreview ||
    prior.firstCampaignPlanPreview ||
    prior.preview ||
    null;
  const priorSlots = seedSlotsFromContext(ctx, prior.slots || {});
  if (prior.previewGenerated || priorSlots.previewGenerated) {
    priorSlots.previewGenerated = true;
  }
  if (prior.previewApproved || priorSlots.previewApproved) {
    priorSlots.previewApproved = true;
  }
  if (
    prior.status === 'preview_ready' ||
    prior.step === 'preview' ||
    prior.step === PROSPECT_LIST_CRITERIA_STEP ||
    prior.step === 'prospect_list_criteria_preview' ||
    (priorPreview && priorPreview.status === 'approved')
  ) {
    // Session already produced a campaign plan preview.
    priorSlots.previewGenerated = true;
  }
  if (priorPreview && priorPreview.status === 'approved') {
    priorSlots.previewApproved = true;
  }

  const currentStep =
    prior.step && prior.step !== 'opening' ? prior.step : 'opening';
  const currentAsk =
    prior.currentAsk ||
    (currentStep === 'opening'
      ? 'opening'
      : currentStep === 'preview' ||
          currentStep === PROSPECT_LIST_CRITERIA_STEP ||
          currentStep === 'prospect_list_criteria_preview'
        ? priorSlots.previewApproved
          ? criteriaSlotsReady(priorSlots)
            ? null
            : 'prospectListCriteria'
          : 'previewApproved'
        : Object.keys(SLOT_PROMPTS).find((k) => slotToStep(k) === currentStep) ||
          nextMissingPrePreviewSlot(priorSlots));

  const answers = { ...(prior.answers || {}) };
  answers[currentStep] = {
    raw: String(userMessage || '').trim(),
    at: new Date().toISOString(),
  };

  let slots = extractSlotsFromMessage(
    userMessage,
    priorSlots,
    ctx,
    currentAsk
  );
  // Re-seed any still-empty context-backed slots after extraction/revise clears.
  slots = seedSlotsFromContext(ctx, slots);
  // Preserve satisfied values that extractors should not wipe unless revise.
  if (!detectReviseIntent(userMessage)) {
    for (const key of SLOT_KEYS) {
      if (
        key === 'previewGenerated' ||
        key === 'previewApproved' ||
        isSlotSatisfied(slots, key)
      ) {
        continue;
      }
      if (isSlotSatisfied(priorSlots, key)) {
        slots[key] = priorSlots[key];
      }
    }
  } else {
    // Keep non-revised prior values.
    const revised = new Set(detectRevisedSlotKeys(userMessage));
    for (const key of SLOT_KEYS) {
      if (revised.has(key)) continue;
      if (!isSlotSatisfied(slots, key) && isSlotSatisfied(priorSlots, key)) {
        slots[key] = priorSlots[key];
      }
    }
    if (revised.has('campaignObjective') || revised.size === 0) {
      // If revise with no specific section, only clear when extractor cleared.
    }
  }

  slots.previewGenerated = Boolean(
    slots.previewGenerated || priorSlots.previewGenerated
  );
  slots.previewApproved = Boolean(
    slots.previewApproved || priorSlots.previewApproved
  );

  const syncedAnswers = syncAnswersFromSlots(answers, slots);
  // Keep the raw current utterance on the active step for audit.
  syncedAnswers[currentStep] = answers[currentStep];

  const wantPreview =
    detectPreviewRequest(userMessage) || opts.forcePreview;

  // --- Post-preview path: never re-ask objective/segment ---
  if (slots.previewGenerated) {
    if (detectReviseIntent(userMessage) && detectRevisedSlotKeys(userMessage).length) {
      const missing = nextMissingPrePreviewSlot(slots);
      if (missing) {
        return {
          message: [
            `Understood — we'll revise that section.`,
            ``,
            promptForSlot(missing, ctx),
          ].join('\n'),
          step: slotToStep(missing),
          answers: syncedAnswers,
          slots: { ...slots, previewGenerated: false, previewApproved: false },
          preview: null,
          criteriaPreview: null,
          intent: 'revise',
          currentAsk: missing,
        };
      }
    }

    const replyOpts = { ...opts, priorPreview };
    if (criteriaSlotsReady(slots)) {
      return produceCriteriaPreviewResult(
        ctx,
        syncedAnswers,
        { ...slots, previewApproved: true },
        replyOpts,
        slots.previewApproved
          ? 'Approved. Here is the Prospect List Criteria Preview — still planning-only.'
          : 'Thanks — I captured the prospect-list criteria. Here is the Prospect List Criteria Preview — still planning-only.'
      );
    }

    if (!slots.previewApproved) {
      // Soft-ack if they answered something else (e.g. criteria) while preview pending approval.
      if (
        isSlotSatisfied(slots, 'inclusionCriteria') ||
        isSlotSatisfied(slots, 'exclusionCriteria')
      ) {
        if (criteriaSlotsReady(slots)) {
          return produceCriteriaPreviewResult(
            ctx,
            syncedAnswers,
            { ...slots, previewApproved: true },
            replyOpts,
            'Thanks — I captured the prospect-list criteria. Here is the Prospect List Criteria Preview — still planning-only.'
          );
        }
        return {
          message: [
            `Noted.`,
            ``,
            prospectListCriteriaPrompt(ctx),
          ].join('\n'),
          step: PROSPECT_LIST_CRITERIA_STEP,
          answers: syncedAnswers,
          slots,
          preview: priorPreview,
          criteriaPreview: null,
          intent: 'advance',
          currentAsk: 'prospectListCriteria',
          previewApproved: false,
        };
      }
      return {
        message: [
          `Thanks — the First Campaign Plan Preview is ready for review.`,
          ``,
          SLOT_PROMPTS.previewApproved,
        ].join('\n'),
        step: 'preview',
        answers: syncedAnswers,
        slots,
        preview: priorPreview,
        criteriaPreview: null,
        intent: 'await_approval',
        currentAsk: 'previewApproved',
        previewApproved: false,
      };
    }

    // Approved preview — advance to prospect-list criteria. Do not re-ask
    // objective/segment/market/proof/hypothesis/metrics/checkpoints.
    const approved = markCampaignPlanPreviewApproved(priorPreview, slots, opts);
    return {
      message: prospectListCriteriaPrompt(ctx),
      step: PROSPECT_LIST_CRITERIA_STEP,
      answers: syncedAnswers,
      slots: approved.slots,
      preview: approved.preview,
      criteriaPreview: null,
      intent: 'preview_approved',
      currentAsk: 'prospectListCriteria',
      previewApproved: true,
    };
  }

  // --- Opening: confirm focus, then ask first missing slot ---
  if (currentStep === 'opening') {
    const missing = nextMissingPrePreviewSlot(slots) || 'campaignObjective';
    return {
      message: [
        `Got it — we'll plan from the approved focus${
          /\bnarrow\b/i.test(String(userMessage || ''))
            ? ', with your narrower first test noted'
            : ' as defined'
        }.`,
        ``,
        promptForSlot(missing, ctx),
      ].join('\n'),
      step: slotToStep(missing),
      answers: syncedAnswers,
      slots,
      preview: null,
      criteriaPreview: null,
      intent: 'advance',
      currentAsk: missing,
    };
  }

  // --- Pre-preview: ask only unsatisfied slots ---
  // When inclusion/exclusion are already captured, soft slots can use preview
  // defaults so we do not loop — next output is the criteria preview path.
  const skipSoft = criteriaSlotsReady(slots) || wantPreview;
  const missing = nextMissingPrePreviewSlot(slots, { skipSoft });

  if (!missing || wantPreview) {
    return produceCampaignPlanPreviewResult(
      ctx,
      syncedAnswers,
      slots,
      opts,
      'Thanks — I have enough to draft the First Campaign Plan Preview.'
    );
  }

  // If the operator already gave inclusion/exclusion early, acknowledge once
  // but keep collecting remaining planning slots (do not loop objective).
  const filledNote = (() => {
    const filled = [];
    if (
      isSlotSatisfied(slots, 'campaignObjective') &&
      currentAsk === 'campaignObjective'
    ) {
      filled.push('campaign objective');
    }
    if (
      (isSlotSatisfied(slots, 'inclusionCriteria') ||
        isSlotSatisfied(slots, 'exclusionCriteria')) &&
      (extractInclusionCriteria(userMessage) ||
        extractExclusionCriteria(userMessage))
    ) {
      filled.push('prospect-list criteria');
    }
    if (!filled.length) {
      return `Noted for ${String(currentAsk || currentStep).replace(/_/g, ' ')}.`;
    }
    return `Noted — captured ${filled.join(' and ')}.`;
  })();

  return {
    message: [filledNote, '', promptForSlot(missing, ctx)].join('\n'),
    step: slotToStep(missing),
    answers: syncedAnswers,
    slots,
    preview: null,
    criteriaPreview: null,
    intent: 'advance',
    currentAsk: missing,
  };
}

function containsForbiddenCampaignPlanningLanguage(text) {
  const s = String(text || '');
  return (
    /prospect list (?:is|was) (?:ready|built|generated)|I (?:built|generated|created) a prospect list/i.test(
      s
    ) ||
    /campaign is live|launching outreach now|I (?:sent|am sending) (?:the )?emails/i.test(
      s
    ) ||
    /I (?:changed|updated|modified) (?:your )?(?:DNS|GBP|Google Business|tracking pixel|CRM)/i.test(
      s
    ) ||
    /here(?:'| i)?s the cold email copy to send/i.test(s)
  );
}

module.exports = {
  ARTIFACT_KIND,
  PREVIEW_TITLE,
  PREVIEW_DISCLAIMER,
  CRITERIA_ARTIFACT_KIND,
  CRITERIA_PREVIEW_TITLE,
  CRITERIA_PREVIEW_DISCLAIMER,
  SECTION_TITLES,
  CRITERIA_SECTION_TITLES,
  CONVERSATION_STEPS,
  QUESTION_BANK,
  SLOT_KEYS,
  PRE_PREVIEW_SLOT_ORDER,
  PROSPECT_LIST_CRITERIA_STEP,
  SLOT_PROMPTS,
  DEFAULT_PROOF_ASSETS,
  DEFAULT_PROOF_ASSETS_AVAILABLE,
  DEFAULT_PROOF_ASSETS_MISSING,
  DEFAULT_VALIDATION_METRICS,
  DEFAULT_VALIDATION_METRICS_PRIMARY,
  DEFAULT_VALIDATION_METRICS_SECONDARY,
  DEFAULT_INCLUSION_CRITERIA,
  DEFAULT_EXCLUSION_CRITERIA,
  DEFAULT_REQUIRED_PROSPECT_FIELDS,
  DEFAULT_REVIEW_GATE,
  DEFAULT_TARGET_SUBTYPE_PROPERTY_MANAGERS,
  DEFAULT_APPROVAL_CHECKPOINTS,
  DEFAULT_APPROVAL_BEFORE_LIST,
  DEFAULT_APPROVAL_BEFORE_LAUNCH,
  VALIDATION_SUCCESS_STATEMENT,
  buildCampaignPlanningContext,
  buildCampaignPlanningOpening,
  buildCampaignPlanningReply,
  buildFirstCampaignPlanPreview,
  formatFirstCampaignPlanPreviewMessage,
  extractCampaignPlanFields,
  stripCampaignWrappers,
  peelInlineIncludeExclude,
  buildProspectListCriteriaPreview,
  formatProspectListCriteriaPreviewMessage,
  emptySlots,
  seedSlotsFromContext,
  extractSlotsFromMessage,
  isSlotSatisfied,
  nextMissingPrePreviewSlot,
  detectPreviewRequest,
  detectReviseIntent,
  containsForbiddenCampaignPlanningLanguage,
  extractBusinessName,
  extractAvoidPhrase,
  stepAfterOpening,
  humanizeSegment,
  humanizeStatusLabel,
  displayName,
  sanitizeTargetSegmentText,
  stripFirstPersonArtifactLanguage,
};
