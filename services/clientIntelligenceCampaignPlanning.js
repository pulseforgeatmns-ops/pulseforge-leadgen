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
const {
  ARTIFACT_KINDS,
  resolveCampaignArtifactAction,
  shouldBlockCriteriaQuestionReplay,
  isBannedCriteriaReplayQuestion,
  looksLikeProspectListDraftRequest,
  looksLikeScoutHandoffBriefRequest,
  looksLikeLiveSourcingApproval,
  classifyProspectAcquisitionIntent,
  PROSPECT_ACQUISITION_INTENTS,
  looksLikeReviseCriteriaRequest,
  shouldForceProspectListDraft,
  inferApprovedArtifactsFromMessage,
} = require('./clientIntelligenceReasoning');
const {
  buildArtifactSynthesisContext,
  shortBusinessName,
  containsRawPromptFragment,
  findRawPromptFragments,
  asEmbeddablePhrase,
} = require('./maxSynthesis');

const ARTIFACT_KIND = 'first_campaign_plan_preview';
const PREVIEW_TITLE = 'First Campaign Plan Preview';
const PREVIEW_DISCLAIMER =
  'Planning preview only. No prospect list, outreach copy, sends, CRM writes, or account changes have been created or launched.';

const CRITERIA_ARTIFACT_KIND = 'prospect_list_criteria_preview';
const CRITERIA_PREVIEW_TITLE = 'Prospect List Criteria Preview';
const CRITERIA_PREVIEW_DISCLAIMER =
  'Criteria preview only. No prospect list has been built, and no outreach copy, sends, CRM writes, or account changes have been created or launched.';

const BUILD_PROPOSAL_ARTIFACT_KIND = 'prospect_list_build_proposal';
const BUILD_PROPOSAL_TITLE = 'Prospect List Build Proposal';
const BUILD_PROPOSAL_DISCLAIMER =
  'Build proposal only. No prospect list has been built, and no outreach copy, sends, CRM writes, or account changes have been created or launched.';

const DRAFT_ARTIFACT_KIND = 'reviewable_prospect_list_draft';
const DRAFT_TITLE = 'Reviewable Prospect List Draft';
const DRAFT_DISCLAIMER =
  'Reviewable list draft only. No outreach copy, sends, CRM writes, or account/DNS/GBP/social/tracking changes have been made.';

const LIVE_SOURCING_BOUNDARY_MESSAGE =
  'I cannot perform live sourcing in this environment yet.';

const LIVE_PROSPECT_LIST_KIND = 'live_sourced_prospect_list';
const LIVE_PROSPECT_LIST_TITLE = 'Live Public-Source Prospect List';

const SCOUT_HANDOFF_BRIEF_KIND = 'scout_handoff_brief';
const SCOUT_HANDOFF_BRIEF_TITLE = 'Scout Handoff Brief';
const SCOUT_HANDOFF_BRIEF_DISCLAIMER =
  'Scout Handoff Brief only. Max did not build a prospect list. Scout inspects public sources; no outreach copy, sends, CRM writes, or account changes have been created or launched.';

/** Explicit post-build-proposal planning states (SPEC-091 continuation). */
const CAMPAIGN_PLANNING_STATES = Object.freeze({
  PROSPECT_LIST_CRITERIA_APPROVED: 'prospect_list_criteria_approved',
  PROSPECT_LIST_BUILD_PROPOSAL_APPROVED: 'prospect_list_build_proposal_approved',
  PROSPECT_LIST_DRAFT_REQUESTED: 'prospect_list_draft_requested',
  PROSPECT_LIST_DRAFT_GENERATED: 'prospect_list_draft_generated',
  PROSPECT_LIST_DRAFT_REVIEWED: 'prospect_list_draft_reviewed',
  SCOUT_HANDOFF_BRIEF: 'scout_handoff_brief',
  LIVE_SOURCING_APPROVED: 'live_sourcing_approved',
  LIVE_SOURCING_UNAVAILABLE: 'live_sourcing_unavailable',
  LIVE_SOURCING_GENERATED: 'live_sourcing_generated',
});

const SCOUT_HANDOFF_SECTION_TITLES = Object.freeze({
  campaignObjective: 'Campaign objective',
  targetSegmentSubtype: 'Target segment / subtype',
  marketBounds: 'Market bounds',
  inclusionCriteria: 'Inclusion criteria',
  exclusionCriteria: 'Exclusion criteria',
  requiredProspectFields: 'Required prospect fields',
  sourceTypes: 'Source types Scout should inspect',
  evidenceRequired: 'Evidence Scout must attach',
  confidenceRules: 'Confidence rules',
  reviewGate: 'Review gate',
  guardrails: 'Guardrails',
});

const DEFAULT_SCOUT_SOURCE_TYPES = Object.freeze([
  'Public business directories and local listings for the approved segment',
  'Company websites and about/contact pages that confirm location and role signals',
  'Public property / facility / office manager listings when relevant to the segment',
  'Other openly published local-market sources — no private or gated scrapes',
]);

const DEFAULT_SCOUT_EVIDENCE = Object.freeze([
  'Source URL for each prospect record',
  'Location / market-town evidence matching approved bounds',
  'Segment or subtype signal from the public source',
  'Fit rationale grounded in visible public facts (not invented)',
  'Any disqualifying risk or uncertainty noted on the record',
]);

const DEFAULT_SCOUT_CONFIDENCE_RULES = Object.freeze([
  'High — source URL + in-market location + clear segment/subtype + reachable decision-maker signal',
  'Medium — source URL + in-market location + segment fit, but thin contact or subtype evidence',
  'Low / review_required — missing source URL, weak market match, or ambiguous fit; do not treat as ready',
]);

const DEFAULT_SCOUT_HANDOFF_GUARDRAILS = Object.freeze([
  'Max does not build or fabricate the prospect list in this step',
  'Scout inspects public sources only',
  'No outreach copy, sends, CRM writes, or account/DNS/GBP/social/tracking changes',
  'Operator reviews Scout’s returned batch before any enrichment expansion or outreach',
]);

const DRAFT_SECTION_TITLES = Object.freeze({
  batchSummary: 'Batch summary',
  draftRows: 'Prospect record fields (awaiting live public sources)',
  reviewNotes: 'Review notes',
  guardrails: 'Guardrails',
  recommendedNextStep: 'Recommended next step',
});

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

const BUILD_PROPOSAL_SECTION_TITLES = Object.freeze({
  approachSummary: 'Approach summary',
  sourcingStrategy: 'Sourcing strategy',
  enrichmentPlan: 'Enrichment plan',
  qualityGates: 'Quality gates',
  firstBatchPlan: 'First batch plan',
  reviewCheckpoints: 'Review checkpoints before build',
  whatWeWillNotDo: 'What we will not do yet',
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
  'criteriaApproved',
  'criteriaGenerated',
  'buildProposalApproved',
  'buildProposalGenerated',
  'draftRequested',
  'draftGenerated',
  'liveSourcingApproved',
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
const PROSPECT_LIST_CRITERIA_APPROVED_STEP =
  CAMPAIGN_PLANNING_STATES.PROSPECT_LIST_CRITERIA_APPROVED;
const PROSPECT_LIST_BUILD_PROPOSAL_STEP = 'prospect_list_build_proposal';
const PROSPECT_LIST_BUILD_PROPOSAL_APPROVED_STEP =
  CAMPAIGN_PLANNING_STATES.PROSPECT_LIST_BUILD_PROPOSAL_APPROVED;
const PROSPECT_LIST_DRAFT_REQUESTED_STEP =
  CAMPAIGN_PLANNING_STATES.PROSPECT_LIST_DRAFT_REQUESTED;
const PROSPECT_LIST_DRAFT_GENERATED_STEP =
  CAMPAIGN_PLANNING_STATES.PROSPECT_LIST_DRAFT_GENERATED;
const PROSPECT_LIST_DRAFT_REVIEWED_STEP =
  CAMPAIGN_PLANNING_STATES.PROSPECT_LIST_DRAFT_REVIEWED;

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
  'Company or property manager name',
  'Website or source URL',
  'Location',
  'Segment/subtype',
  'Why they fit',
  'Any disqualifying risk or uncertainty',
  'Suggested contact role',
  'Confidence level',
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
    criteriaGenerated: false,
    criteriaApproved: false,
    buildProposalGenerated: false,
    buildProposalApproved: false,
    draftRequested: false,
    draftGenerated: false,
  };
}

function isSlotSatisfied(slots, key) {
  if (!slots) return false;
  if (
    key === 'previewGenerated' ||
    key === 'previewApproved' ||
    key === 'criteriaGenerated' ||
    key === 'criteriaApproved' ||
    key === 'buildProposalGenerated' ||
    key === 'buildProposalApproved' ||
    key === 'draftRequested' ||
    key === 'draftGenerated'
  ) {
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

function peelReviewGateClause(text) {
  return String(text || '')
    .replace(
      /(?:^|\n|;|\.)\s*(?:review\s+gate|approval\s+gate)\s*[:\-–—]?\s*[\s\S]*$/i,
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
    const cleaned = uniqueStrings(
      value
        .map((x) =>
          peelReviewGateClause(peelRecordFieldsClause(String(x || '').trim()))
        )
        .filter(Boolean)
        .filter((x) => !isGeographyFragmentItem(x))
    ).slice(0, 8);
    return looksLikeMalformedCriteriaList(cleaned) ? [] : cleaned;
  }
  const s = peelReviewGateClause(
    peelRecordFieldsClause(String(value || '').trim())
  );
  if (!s) return [];
  const listed = s
    .split(/\n|;|•|\u2022|(?<=\.)\s+(?=[A-Z])/g)
    .map((x) => x.replace(/^[-–—*\d.)\s]+/, '').trim())
    .filter((x) => x.length > 2)
    .filter((x) => !isGeographyFragmentItem(x));
  if (listed.length > 1) {
    const uniq = uniqueStrings(listed).slice(0, 8);
    return looksLikeMalformedCriteriaList(uniq) ? [] : uniq;
  }
  // Single prose blob with dash-stitched locations is not a clean bullet.
  if (looksLikeMalformedCriteriaList([s])) return [];
  return [s];
}

function looksLikeValidationSignalField(item) {
  const s = String(item || '').trim();
  if (!s) return true;
  return (
    /\b(questions about|recurring service|reliability|responsiveness|scheduling|cleaning frustrations|vague interest|no next step|qualified repl(?:y|ies)|walkthroughs? booked|estimate requests?|primary signals?|secondary signals?|validation metrics?)\b/i.test(
      s
    ) ||
    /^(reliability|responsiveness|scheduling|clarity on)\b/i.test(s) ||
    /^or\b/i.test(s)
  );
}

function looksLikeProspectRecordField(item) {
  const s = String(item || '').trim();
  if (!s || s.length < 3 || s.length > 80) return false;
  if (looksLikeValidationSignalField(s)) return false;
  if (/^(each prospect|required|inclusion|exclusion|review gate)\b/i.test(s)) {
    return false;
  }
  // Accept known record-field labels and close operator variants.
  return (
    /\b(company|property manager|business|website|source url|url|location|town|market|segment|subtype|why they fit|fit|disqualif|risk|uncertainty|contact role|decision-?maker|confidence|email|phone|name)\b/i.test(
      s
    ) || DEFAULT_REQUIRED_PROSPECT_FIELDS.some((f) => f.toLowerCase() === s.toLowerCase())
  );
}

function parseProspectFieldList(text) {
  return String(text || '')
    .split(/\n|;|•|\u2022|,(?=\s)/g)
    .map((x) => x.replace(/^[-–—*\d.)\s]+/, '').replace(/\.$/, '').trim())
    .filter((x) => x.length > 2 && x.length < 80)
    .filter((x) => !/^(each prospect|required)/i.test(x))
    .filter(looksLikeProspectRecordField)
    .map((x) => x.charAt(0).toUpperCase() + x.slice(1));
}

/**
 * Required prospect record fields only — never validation metrics, secondary
 * signals, or campaign-objective prose. Section 7 always uses the canonical
 * prospect-record field list for the criteria preview.
 */
function normalizeRequiredProspectFields(value) {
  // Guard: if a caller passes validation-signal text/arrays, discard them.
  if (Array.isArray(value)) {
    const hasValidationBleed = value.some((item) =>
      looksLikeValidationSignalField(item)
    );
    if (hasValidationBleed) return [...DEFAULT_REQUIRED_PROSPECT_FIELDS];
    const cleaned = uniqueStrings(
      value
        .map((x) => String(x || '').trim().replace(/\.$/, ''))
        .filter(looksLikeProspectRecordField)
    );
    const defaultKeys = DEFAULT_REQUIRED_PROSPECT_FIELDS.map((d) =>
      d.toLowerCase()
    );
    const isCanonical =
      cleaned.length === DEFAULT_REQUIRED_PROSPECT_FIELDS.length &&
      cleaned.every((item) => defaultKeys.includes(item.toLowerCase()));
    if (isCanonical) return cleaned;
  } else if (value) {
    const raw = String(value).trim();
    if (looksLikeValidationSignalField(raw)) {
      return [...DEFAULT_REQUIRED_PROSPECT_FIELDS];
    }
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
  // Clear comma-split inclusion/exclusion from extractCampaignPlanFields so
  // geography prose cannot win over polished defaults.
  const fields = extractCampaignPlanFields(ctx, answers);
  fields.inclusionCriteria = [];
  fields.exclusionCriteria = [];

  const slotInclusion = normalizeCriteriaList(s.inclusionCriteria);
  const slotExclusion = normalizeCriteriaList(
    peelReviewGateClause(s.exclusionCriteria)
  );

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

  const pickCriteriaList = (slotList, priorList, synthesized) => {
    if (looksLikeCleanCriteriaList(slotList)) return slotList;
    if (looksLikeCleanCriteriaList(priorList)) return priorList;
    if (looksLikeCleanCriteriaList(synthesized)) return synthesized;
    return synthesized && synthesized.length
      ? synthesized
      : defaultInclusionCriteria(ctx, answers);
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
  const inclusionCriteria = pickCriteriaList(
    slotInclusion,
    prior && prior.inclusionCriteria,
    segmentPart.inclusionCriteria
  );
  const exclusionCriteria = (() => {
    if (looksLikeCleanCriteriaList(slotExclusion)) return slotExclusion;
    if (looksLikeCleanCriteriaList(prior && prior.exclusionCriteria)) {
      return prior.exclusionCriteria;
    }
    return segmentPart.exclusionCriteria;
  })();
  const requiredProspectFields = normalizeRequiredProspectFields(
    s.requiredProspectFields ||
      (prior && prior.requiredProspectFields) ||
      null
  );
  const reviewGate = normalizeReviewGate(
    s.reviewGate || (prior && prior.reviewGate) || null
  );

  // Recompute phrases from the fields actually selected for this artifact so
  // downstream Build Proposal / UI embeddings share one normalization path.
  const finalSynthesis = buildArtifactSynthesisContext({
    context: ctx,
    normalizedFacts: opts.normalizedFacts || null,
    priorArtifact: {
      businessName: name,
      campaignObjective,
      targetSegment,
      targetSubtype,
      marketBound: market,
      inclusionCriteria,
      exclusionCriteria,
      coreValidationQuestion:
        (prior && prior.coreValidationQuestion) ||
        objectivePart.coreValidationQuestion,
    },
    slots: s,
    answers,
  });

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
    synthesisPhrases: { ...finalSynthesis.phrases },
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

function markCriteriaPreviewApproved(preview, slots = {}) {
  const base =
    preview && typeof preview === 'object' && Object.keys(preview).length
      ? preview
      : null;
  const nextSlots = {
    ...slots,
    previewApproved: true,
    previewGenerated: true,
    criteriaGenerated: true,
    criteriaApproved: true,
  };
  if (!base) {
    return { criteriaPreview: null, slots: nextSlots };
  }
  return {
    criteriaPreview: {
      ...base,
      status: 'approved',
      approvedAt: base.approvedAt || new Date().toISOString(),
    },
    slots: nextSlots,
  };
}

/**
 * Planning-only proposal for HOW we would build the first prospect list.
 * Does not generate a list, outreach, sends, or account changes.
 *
 * Synthesis uses Max Synthesis Layer phrases only — never pastes raw prior
 * artifact paragraphs into wrapper sentences.
 */
function buildProspectListBuildProposal(context, slots, opts = {}) {
  const ctx = context || {};
  const s = slots || {};
  const answers = opts.answers || syncAnswersFromSlots({}, s);
  const criteria = opts.priorCriteriaPreview || null;

  const synthesis = buildArtifactSynthesisContext({
    context: ctx,
    normalizedFacts: opts.normalizedFacts || null,
    priorCriteriaPreview: criteria,
    priorCampaignPreview: opts.priorCampaignPreview || null,
    slots: s,
    answers,
  });
  const { phrases, facts } = synthesis;
  const name = shortBusinessName(
    facts.businessName || ctx.businessName || 'the business'
  );

  const segmentPhrase = phrases.targetSegmentPhrase;
  const subtypePhrase = phrases.targetSubtypePhrase;
  const marketPhrase = phrases.marketBoundPhrase;

  // Compact noun for "15–25 property managers in …"
  const segmentNoun = /property managers?/i.test(segmentPhrase)
    ? 'property managers'
    : asEmbeddablePhrase(humanizeSegment(ctx.primarySegment || segmentPhrase)) ||
      'accounts';

  // Town list without the ", with Greater Manchester kept in scope" tail.
  const marketTownsOnly = String(marketPhrase || '')
    .replace(/,?\s*with\s+.+?\s+kept in scope\.?$/i, '')
    .trim();

  // Subtype → "firms that manage offices…" (avoid duplicating full subtype sentence).
  let manageClause = '';
  if (subtypePhrase) {
    const overs = subtypePhrase.match(
      /overseeing\s+(.+?)(?:\s+that likely need.*)?$/i
    );
    if (overs && overs[1]) {
      manageClause = `that manage ${asEmbeddablePhrase(overs[1])}`;
    }
  }

  const focusSegment = /small to mid-sized/i.test(segmentPhrase)
    ? 'small to mid-sized local firms'
    : segmentPhrase;

  const approachSummary = [
    `For ${name}'s first test, I would build a small, reviewable batch of 15–25 ${segmentNoun} in ${marketTownsOnly || marketPhrase}.`,
    manageClause
      ? `The list should focus on ${focusSegment} ${manageClause}.`
      : `The list should focus on ${focusSegment}.`,
  ].join(' ');

  const inclusion =
    (criteria && criteria.inclusionCriteria) ||
    normalizeCriteriaList(s.inclusionCriteria) ||
    defaultInclusionCriteria(ctx, answers);
  const exclusion =
    (criteria && criteria.exclusionCriteria) ||
    normalizeCriteriaList(s.exclusionCriteria) ||
    defaultExclusionCriteria(ctx, answers);

  const sourcingStrategy = [
    `Start from local business directories and market sources for ${segmentPhrase} across ${marketTownsOnly || marketPhrase}.`,
    'Prefer sources that expose decision-maker role signals (property / facility / office manager) over generic company dumps.',
    'De-duplicate by company name + market before enrichment so the first batch stays tight.',
  ];

  const enrichmentPlan = [
    'Enrich only records that already pass inclusion criteria.',
    'Prioritize required prospect fields: decision-maker name/role, company, market town, phone and/or email when available, and a source note.',
    'Leave thin records flagged for review rather than inventing contacts.',
  ];

  const qualityGates = [
    ...(inclusion || [])
      .slice(0, 4)
      .map((item) => `Include only when: ${asEmbeddablePhrase(item) || item}`),
    ...(exclusion || [])
      .slice(0, 4)
      .map((item) => `Exclude when: ${asEmbeddablePhrase(item) || item}`),
    'Drop national chains, bargain-only buyers, and out-of-market accounts before the batch is presented.',
  ].filter(Boolean);

  const firstBatchPlan = {
    size: '15–25 accounts for the first reviewable batch',
    marketFocus: marketPhrase,
    segmentFocus: segmentPhrase,
    successSignal:
      'Enough qualified contacts to test reply/walkthrough interest without over-building.',
  };

  const reviewCheckpoints = [
    'Operator reviews the Prospect List Build Proposal before any list generation.',
    'Approved criteria remain the filter set — no silent widening mid-build.',
    'First batch is presented for review before enrichment expansion or outreach planning.',
    'No outreach copy, sends, CRM writes, or account/DNS/GBP/social/tracking changes without explicit approval.',
  ];

  const whatWeWillNotDo = [
    'Build or export a full prospect list yet',
    'Write or send outreach copy',
    'Write to CRM or activate Scout/Composer',
    'Change DNS, GBP, social, tracking, or account settings',
  ];

  const proposal = {
    kind: BUILD_PROPOSAL_ARTIFACT_KIND,
    title: BUILD_PROPOSAL_TITLE,
    businessName: name,
    approachSummary,
    sourcingStrategy,
    enrichmentPlan,
    qualityGates,
    firstBatchPlan,
    reviewCheckpoints,
    whatWeWillNotDo,
    /** Phrase-safe fields from Max Synthesis Layer (for UI / downstream). */
    synthesisPhrases: { ...phrases },
    sectionTitles: { ...BUILD_PROPOSAL_SECTION_TITLES },
    planningOnly: true,
    prospectListGenerated: false,
    outreachCopyGenerated: false,
    accountChangesMade: false,
    campaignsGenerated: false,
    status: 'draft',
    disclaimer: BUILD_PROPOSAL_DISCLAIMER,
    recommendedNextStep:
      'Review and approve this build approach before any prospect list is generated. No outreach, sends, or account changes yet.',
    generatedAt: new Date().toISOString(),
    blueprintId: opts.blueprintId || ctx.blueprintId || null,
    blueprintVersion: opts.blueprintVersion || ctx.blueprintVersion || null,
    basedOnCriteriaStatus: (criteria && criteria.status) || 'approved',
  };

  // Hard guard: never ship banned raw-prompt stitching.
  const rendered = formatProspectListBuildProposalMessage(proposal);
  const hits = findRawPromptFragments(rendered);
  if (hits.length) {
    // Re-normalize market/segment focus if somehow instruction leads leaked.
    proposal.firstBatchPlan = {
      ...proposal.firstBatchPlan,
      marketFocus: phrases.marketBoundPhrase,
      segmentFocus: phrases.targetSegmentPhrase,
    };
    proposal.approachSummary = approachSummary.replace(/\bStart with\b/gi, '').replace(/\bProve that\b/gi, '');
  }

  return proposal;
}

function formatProspectListBuildProposalMessage(proposal) {
  const p = proposal || {};
  const titles = p.sectionTitles || BUILD_PROPOSAL_SECTION_TITLES;
  const lines = [p.title || BUILD_PROPOSAL_TITLE, ''];

  lines.push(`1. ${titles.approachSummary}`);
  lines.push(p.approachSummary || '—');
  lines.push('');

  lines.push(`2. ${titles.sourcingStrategy}`);
  for (const item of p.sourcingStrategy || []) lines.push(`- ${item}`);
  if (!(p.sourcingStrategy || []).length) lines.push('- —');
  lines.push('');

  lines.push(`3. ${titles.enrichmentPlan}`);
  for (const item of p.enrichmentPlan || []) lines.push(`- ${item}`);
  if (!(p.enrichmentPlan || []).length) lines.push('- —');
  lines.push('');

  lines.push(`4. ${titles.qualityGates}`);
  for (const item of p.qualityGates || []) lines.push(`- ${item}`);
  if (!(p.qualityGates || []).length) lines.push('- —');
  lines.push('');

  lines.push(`5. ${titles.firstBatchPlan}`);
  const batch = p.firstBatchPlan || {};
  lines.push(`- Size: ${batch.size || '—'}`);
  lines.push(`- Market focus: ${batch.marketFocus || '—'}`);
  lines.push(`- Segment focus: ${batch.segmentFocus || '—'}`);
  lines.push(`- Success signal: ${batch.successSignal || '—'}`);
  lines.push('');

  lines.push(`6. ${titles.reviewCheckpoints}`);
  for (const item of p.reviewCheckpoints || []) lines.push(`- ${item}`);
  if (!(p.reviewCheckpoints || []).length) lines.push('- —');
  lines.push('');

  lines.push(`7. ${titles.whatWeWillNotDo}`);
  for (const item of p.whatWeWillNotDo || []) lines.push(`- ${item}`);
  if (!(p.whatWeWillNotDo || []).length) lines.push('- —');
  lines.push('');

  lines.push(`8. ${titles.recommendedNextStep}`);
  lines.push(p.recommendedNextStep || '—');
  lines.push('');

  lines.push(p.disclaimer || BUILD_PROPOSAL_DISCLAIMER);
  return lines.join('\n').trim();
}

/**
 * Schema-only first prospect list batch draft.
 * Never fabricates illustrative placeholder companies — live public-source
 * rows are produced only after live_sourcing_approved (or a capability
 * boundary is returned when live sourcing is unavailable).
 * Never writes CRM, sends outreach, or changes accounts/DNS/GBP/social/tracking.
 */
function buildReviewableProspectListDraft(context, slots, opts = {}) {
  const ctx = context || {};
  const s = slots || {};
  const criteria = opts.priorCriteriaPreview || null;
  const build = opts.priorBuildProposal || null;
  const synthesis = buildArtifactSynthesisContext({
    context: ctx,
    normalizedFacts: opts.normalizedFacts || null,
    priorCriteriaPreview: criteria,
    priorCampaignPreview: opts.priorCampaignPreview || null,
    slots: s,
    answers: opts.answers || {},
  });
  const { phrases, facts } = synthesis;
  const name = shortBusinessName(
    facts.businessName || ctx.businessName || 'the business'
  );
  const segmentNoun = /property managers?/i.test(phrases.targetSegmentPhrase || '')
    ? 'property managers'
    : asEmbeddablePhrase(humanizeSegment(ctx.primarySegment || 'accounts')) ||
      'accounts';
  const marketPhrase = phrases.marketBoundPhrase || ctx.targetMarket || 'the approved market';
  const marketTownsOnly = String(marketPhrase || '')
    .replace(/,?\s*with\s+.+?\s+kept in scope\.?$/i, '')
    .trim();

  const inclusion =
    (criteria && criteria.inclusionCriteria) ||
    normalizeCriteriaList(s.inclusionCriteria) ||
    defaultInclusionCriteria(ctx, opts.answers || {});
  const exclusion =
    (criteria && criteria.exclusionCriteria) ||
    normalizeCriteriaList(s.exclusionCriteria) ||
    defaultExclusionCriteria(ctx, opts.answers || {});

  const batchSize =
    (build &&
      build.firstBatchPlan &&
      build.firstBatchPlan.size) ||
    '15–25 accounts for the first reviewable batch';

  const requiredFields = [
    'company / property manager name',
    'website / source URL',
    'location',
    'segment / subtype',
    'fit rationale',
    'risk / uncertainty',
    'suggested contact role',
    'confidence',
  ];

  return {
    kind: DRAFT_ARTIFACT_KIND,
    title: DRAFT_TITLE,
    businessName: name,
    batchSummary: [
      `First reviewable prospect list batch plan for ${name}.`,
      `Target: ${segmentNoun} in ${marketTownsOnly || marketPhrase}.`,
      `Planned batch size: ${batchSize}.`,
      'No company rows are included yet — this draft defines the review schema only.',
      `Each live-sourced record will include: ${requiredFields.join('; ')}.`,
    ].join(' '),
    // Never emit illustrative / fabricated company rows.
    draftRows: [],
    requiredProspectFields: requiredFields,
    inclusionCriteria: inclusion,
    exclusionCriteria: exclusion,
    reviewNotes: [
      'Schema-only draft: no fabricated company names or sample rows.',
      'Approve live public-source sourcing to fill 15–25 real prospect records with source URLs.',
      'Until live sourcing runs, treat this as a field/plan checklist — not a prospect list.',
    ],
    guardrails: [
      'Reviewable list draft only',
      'No fabricated company rows',
      'No outreach copy generated',
      'No sends',
      'No CRM writes',
      'No account, DNS, GBP, social, or tracking changes',
    ],
    sectionTitles: { ...DRAFT_SECTION_TITLES },
    planningOnly: true,
    reviewOnly: true,
    prospectListGenerated: false,
    liveListGenerated: false,
    liveSourcingApproved: Boolean(s.liveSourcingApproved),
    outreachCopyGenerated: false,
    accountChangesMade: false,
    crmWritesMade: false,
    campaignsGenerated: false,
    status: 'draft',
    disclaimer: DRAFT_DISCLAIMER,
    recommendedNextStep:
      'Approve live public-source sourcing to build 15–25 real prospects with source URLs, or revise criteria/build approach first. No outreach or CRM writes yet.',
    generatedAt: new Date().toISOString(),
    blueprintId: opts.blueprintId || ctx.blueprintId || null,
    blueprintVersion: opts.blueprintVersion || ctx.blueprintVersion || null,
    basedOnCriteriaStatus: (criteria && criteria.status) || 'approved',
    basedOnBuildProposalStatus: (build && build.status) || 'approved',
  };
}

/**
 * Whether CIE campaign planning can run live public-source sourcing now.
 * Default: unavailable unless a sync liveSourcingFn (or explicit flag) is injected.
 */
function isLivePublicSourcingSupported(opts = {}) {
  if (opts.liveSourcingSupported === false) return false;
  if (opts.liveSourcingSupported === true) return true;
  if (typeof opts.liveSourcingFn === 'function') return true;
  return false;
}

function normalizeLiveProspectRecord(row, idx) {
  const r = row || {};
  return {
    id: r.id || `live-prospect-${idx + 1}`,
    placeholder: false,
    companyName:
      r.companyName ||
      r.name ||
      r.propertyManagerName ||
      r.company ||
      null,
    website: r.website || r.sourceUrl || r.url || null,
    sourceUrl: r.sourceUrl || r.website || r.url || null,
    location: r.location || r.marketTown || r.address || null,
    marketTown: r.marketTown || r.location || null,
    segment: r.segment || null,
    subtype: r.subtype || r.segmentSubtype || null,
    fitReason: r.fitReason || r.fitRationale || r.rationale || null,
    disqualifyRisk: r.disqualifyRisk || r.risk || r.uncertainty || null,
    contactRole: r.contactRole || r.suggestedContactRole || null,
    confidence: r.confidence || 'review_required',
    sourceNote: r.sourceNote || 'Public source',
  };
}

function formatLiveSourcedProspectListMessage(list) {
  const p = list || {};
  const lines = [p.title || LIVE_PROSPECT_LIST_TITLE, ''];
  lines.push(p.summary || 'Live public-source prospect records:');
  lines.push('');
  for (const row of p.prospects || []) {
    lines.push(
      `- ${row.companyName || 'Unknown'} | ${row.location || '—'} | ${
        row.sourceUrl || row.website || '—'
      } | ${row.fitReason || '—'} | confidence: ${row.confidence || '—'}`
    );
  }
  if (!(p.prospects || []).length) {
    lines.push('- (no prospects returned)');
  }
  lines.push('');
  lines.push('Guardrails:');
  for (const g of p.guardrails || []) lines.push(`- ${g}`);
  lines.push('');
  if (p.disclaimer) lines.push(p.disclaimer);
  return lines.join('\n').trim();
}

function produceLiveSourcingResult(ctx, answers, slots, opts, leadIn) {
  const liveApprovedSlots = {
    ...slots,
    previewApproved: true,
    criteriaGenerated: true,
    criteriaApproved: true,
    buildProposalGenerated: true,
    buildProposalApproved: true,
    draftRequested: true,
    liveSourcingApproved: true,
  };

  if (!isLivePublicSourcingSupported(opts)) {
    const message = [
      leadIn || null,
      LIVE_SOURCING_BOUNDARY_MESSAGE,
      '',
      'Live public-source sourcing is approved, but sourcing tooling is not available in this environment.',
      'No fabricated company rows were generated.',
      'No outreach copy, sends, CRM writes, or account/DNS/GBP/social/tracking changes were made.',
    ]
      .filter(Boolean)
      .join('\n');
    return {
      message,
      step: CAMPAIGN_PLANNING_STATES.LIVE_SOURCING_UNAVAILABLE,
      answers,
      slots: liveApprovedSlots,
      preview: opts.priorPreview || null,
      criteriaPreview: opts.priorCriteriaPreview || null,
      buildProposal: opts.priorBuildProposal || null,
      prospectListDraft: opts.priorProspectListDraft || null,
      liveProspectList: null,
      intent: 'live_sourcing_unavailable',
      previewApproved: true,
      criteriaApproved: true,
      buildProposalApproved: true,
      liveSourcingApproved: true,
      planningState: CAMPAIGN_PLANNING_STATES.LIVE_SOURCING_UNAVAILABLE,
      currentAsk: null,
    };
  }

  let raw = [];
  try {
    raw = opts.liveSourcingFn({
      context: ctx,
      slots: liveApprovedSlots,
      answers,
      priorCriteriaPreview: opts.priorCriteriaPreview || null,
      priorBuildProposal: opts.priorBuildProposal || null,
    });
  } catch (_err) {
    raw = [];
  }
  if (!Array.isArray(raw)) raw = [];
  const prospects = raw
    .map((row, idx) => normalizeLiveProspectRecord(row, idx))
    .filter((row) => row.companyName);

  if (!prospects.length) {
    const message = [
      leadIn || null,
      LIVE_SOURCING_BOUNDARY_MESSAGE,
      '',
      'Live sourcing was attempted but returned no usable public-source prospect records.',
      'No fabricated company rows were generated.',
      'No outreach copy, sends, CRM writes, or account/DNS/GBP/social/tracking changes were made.',
    ]
      .filter(Boolean)
      .join('\n');
    return {
      message,
      step: CAMPAIGN_PLANNING_STATES.LIVE_SOURCING_UNAVAILABLE,
      answers,
      slots: liveApprovedSlots,
      preview: opts.priorPreview || null,
      criteriaPreview: opts.priorCriteriaPreview || null,
      buildProposal: opts.priorBuildProposal || null,
      prospectListDraft: opts.priorProspectListDraft || null,
      liveProspectList: null,
      intent: 'live_sourcing_unavailable',
      previewApproved: true,
      criteriaApproved: true,
      buildProposalApproved: true,
      liveSourcingApproved: true,
      planningState: CAMPAIGN_PLANNING_STATES.LIVE_SOURCING_UNAVAILABLE,
      currentAsk: null,
    };
  }

  const list = {
    kind: LIVE_PROSPECT_LIST_KIND,
    title: LIVE_PROSPECT_LIST_TITLE,
    summary: `Live public-source batch of ${prospects.length} real prospects. Review-only — no outreach or CRM writes.`,
    prospects,
    guardrails: [
      'Public sources only',
      'No outreach copy generated',
      'No sends',
      'No CRM writes',
      'No account, DNS, GBP, social, or tracking changes',
    ],
    liveListGenerated: true,
    liveSourcingApproved: true,
    outreachCopyGenerated: false,
    accountChangesMade: false,
    crmWritesMade: false,
    status: 'review',
    disclaimer:
      'Live-sourced review list only. No outreach copy, sends, CRM writes, or account changes have been made.',
    generatedAt: new Date().toISOString(),
  };

  const lines = [];
  if (leadIn) lines.push(leadIn, '');
  lines.push(formatLiveSourcedProspectListMessage(list));

  return {
    message: lines.join('\n'),
    step: CAMPAIGN_PLANNING_STATES.LIVE_SOURCING_GENERATED,
    answers,
    slots: {
      ...liveApprovedSlots,
      draftGenerated: true,
    },
    preview: opts.priorPreview || null,
    criteriaPreview: opts.priorCriteriaPreview || null,
    buildProposal: opts.priorBuildProposal || null,
    prospectListDraft: opts.priorProspectListDraft || null,
    liveProspectList: list,
    intent: 'produce_live_sourced_prospects',
    previewApproved: true,
    criteriaApproved: true,
    buildProposalApproved: true,
    liveSourcingApproved: true,
    planningState: CAMPAIGN_PLANNING_STATES.LIVE_SOURCING_GENERATED,
    currentAsk: null,
  };
}

/**
 * Planning handoff for Scout — uses approved campaign/list criteria.
 * Does not perform live sourcing and must not hit the sourcing capability boundary.
 */
function buildScoutHandoffBrief(context, slots, opts = {}) {
  const ctx = context || {};
  const s = slots || {};
  const answers = opts.answers || syncAnswersFromSlots({}, s);
  const criteria =
    opts.priorCriteriaPreview ||
    buildProspectListCriteriaPreview(ctx, s, {
      answers,
      priorPreview: opts.priorPreview || null,
      blueprintId: opts.blueprintId,
      blueprintVersion: opts.blueprintVersion,
    });
  const name = shortName(
    (criteria && criteria.businessName) || ctx.businessName || 'the business'
  );

  const campaignObjective =
    (criteria && criteria.campaignObjective) ||
    defaultObjectiveParagraph(ctx, answers);
  const targetSegment =
    (criteria && criteria.targetSegment) || defaultTargetSegmentBody(ctx);
  const targetSubtype =
    (criteria && criteria.targetSubtype) || defaultTargetSubtype(ctx, answers);
  const marketBounds =
    (criteria && criteria.marketBound) || defaultMarketBound(ctx);
  const inclusionCriteria =
    (criteria && criteria.inclusionCriteria) ||
    normalizeCriteriaList(s.inclusionCriteria) ||
    defaultInclusionCriteria(ctx, answers);
  const exclusionCriteria =
    (criteria && criteria.exclusionCriteria) ||
    normalizeCriteriaList(s.exclusionCriteria) ||
    defaultExclusionCriteria(ctx, answers);
  const requiredProspectFields = normalizeRequiredProspectFields(
    (criteria && criteria.requiredProspectFields) ||
      s.requiredProspectFields ||
      null
  );
  const reviewGate =
    'Operator reviews Scout’s returned prospect batch against this brief before outreach, enrichment expansion, CRM writes, or launch. Max does not source prospects in this step.';

  const targetSegmentSubtype = [targetSegment, targetSubtype]
    .filter(Boolean)
    .join(' — ');

  return {
    kind: SCOUT_HANDOFF_BRIEF_KIND,
    title: SCOUT_HANDOFF_BRIEF_TITLE,
    businessName: name,
    campaignObjective,
    targetSegment,
    targetSubtype,
    targetSegmentSubtype,
    marketBounds,
    marketBound: marketBounds,
    inclusionCriteria,
    exclusionCriteria,
    requiredProspectFields,
    sourceTypes: [...DEFAULT_SCOUT_SOURCE_TYPES],
    evidenceRequired: [...DEFAULT_SCOUT_EVIDENCE],
    confidenceRules: [...DEFAULT_SCOUT_CONFIDENCE_RULES],
    reviewGate,
    guardrails: [...DEFAULT_SCOUT_HANDOFF_GUARDRAILS],
    sectionTitles: { ...SCOUT_HANDOFF_SECTION_TITLES },
    planningOnly: true,
    prospectListGenerated: false,
    liveSourcingPerformed: false,
    outreachCopyGenerated: false,
    accountChangesMade: false,
    crmWritesMade: false,
    campaignsGenerated: false,
    status: 'draft',
    disclaimer: SCOUT_HANDOFF_BRIEF_DISCLAIMER,
    recommendedNextStep:
      'Hand this brief to Scout. Scout inspects public sources and returns evidenced prospects for operator review. Max does not live-source here.',
    generatedAt: new Date().toISOString(),
    blueprintId: opts.blueprintId || ctx.blueprintId || null,
    blueprintVersion: opts.blueprintVersion || ctx.blueprintVersion || null,
    basedOnCriteriaStatus: (criteria && criteria.status) || 'approved',
  };
}

function formatScoutHandoffBriefMessage(brief) {
  const p = brief || {};
  const titles = p.sectionTitles || SCOUT_HANDOFF_SECTION_TITLES;
  const lines = [p.title || SCOUT_HANDOFF_BRIEF_TITLE, ''];

  lines.push(`1. ${titles.campaignObjective}`);
  lines.push(p.campaignObjective || '—');
  lines.push('');

  lines.push(`2. ${titles.targetSegmentSubtype}`);
  lines.push(
    p.targetSegmentSubtype ||
      [p.targetSegment, p.targetSubtype].filter(Boolean).join(' — ') ||
      '—'
  );
  lines.push('');

  lines.push(`3. ${titles.marketBounds}`);
  lines.push(p.marketBounds || p.marketBound || '—');
  lines.push('');

  lines.push(`4. ${titles.inclusionCriteria}`);
  for (const item of p.inclusionCriteria || []) lines.push(`- ${item}`);
  if (!(p.inclusionCriteria || []).length) lines.push('- —');
  lines.push('');

  lines.push(`5. ${titles.exclusionCriteria}`);
  for (const item of p.exclusionCriteria || []) lines.push(`- ${item}`);
  if (!(p.exclusionCriteria || []).length) lines.push('- —');
  lines.push('');

  lines.push(`6. ${titles.requiredProspectFields}`);
  for (const item of p.requiredProspectFields || []) lines.push(`- ${item}`);
  if (!(p.requiredProspectFields || []).length) lines.push('- —');
  lines.push('');

  lines.push(`7. ${titles.sourceTypes}`);
  for (const item of p.sourceTypes || []) lines.push(`- ${item}`);
  if (!(p.sourceTypes || []).length) lines.push('- —');
  lines.push('');

  lines.push(`8. ${titles.evidenceRequired}`);
  for (const item of p.evidenceRequired || []) lines.push(`- ${item}`);
  if (!(p.evidenceRequired || []).length) lines.push('- —');
  lines.push('');

  lines.push(`9. ${titles.confidenceRules}`);
  for (const item of p.confidenceRules || []) lines.push(`- ${item}`);
  if (!(p.confidenceRules || []).length) lines.push('- —');
  lines.push('');

  lines.push(`10. ${titles.reviewGate}`);
  lines.push(p.reviewGate || '—');
  lines.push('');

  lines.push(`11. ${titles.guardrails}`);
  for (const item of p.guardrails || []) lines.push(`- ${item}`);
  if (!(p.guardrails || []).length) lines.push('- —');
  lines.push('');

  if (p.recommendedNextStep) {
    lines.push(`Recommended next step`);
    lines.push(p.recommendedNextStep);
    lines.push('');
  }

  lines.push(p.disclaimer || SCOUT_HANDOFF_BRIEF_DISCLAIMER);
  return lines.join('\n').trim();
}

function produceScoutHandoffBriefResult(ctx, answers, slots, opts, leadIn) {
  const briefSlots = {
    ...slots,
    previewApproved: true,
    criteriaGenerated: true,
    criteriaApproved: true,
    buildProposalGenerated: Boolean(
      slots.buildProposalGenerated ||
        slots.buildProposalApproved ||
        opts.priorBuildProposal
    ),
    buildProposalApproved: Boolean(
      slots.buildProposalApproved ||
        (opts.priorBuildProposal &&
          opts.priorBuildProposal.status === 'approved')
    ),
  };

  const brief = buildScoutHandoffBrief(ctx, briefSlots, {
    answers,
    priorPreview: opts.priorPreview || null,
    priorCriteriaPreview: opts.priorCriteriaPreview || null,
    priorBuildProposal: opts.priorBuildProposal || null,
    blueprintId: opts.blueprintId,
    blueprintVersion: opts.blueprintVersion,
  });

  const lines = [];
  if (leadIn) lines.push(leadIn, '');
  lines.push(formatScoutHandoffBriefMessage(brief));

  return {
    message: lines.join('\n'),
    step: CAMPAIGN_PLANNING_STATES.SCOUT_HANDOFF_BRIEF,
    answers,
    slots: briefSlots,
    preview: opts.priorPreview || null,
    criteriaPreview: opts.priorCriteriaPreview || null,
    buildProposal: opts.priorBuildProposal || null,
    prospectListDraft: opts.priorProspectListDraft || null,
    scoutHandoffBrief: brief,
    liveProspectList: null,
    intent: PROSPECT_ACQUISITION_INTENTS.CREATE_SCOUT_HANDOFF_BRIEF,
    previewApproved: true,
    criteriaApproved: true,
    buildProposalApproved: Boolean(briefSlots.buildProposalApproved),
    liveSourcingApproved: false,
    planningState: CAMPAIGN_PLANNING_STATES.SCOUT_HANDOFF_BRIEF,
    currentAsk: null,
  };
}

function formatReviewableProspectListDraftMessage(draft) {
  const p = draft || {};
  const titles = p.sectionTitles || DRAFT_SECTION_TITLES;
  const lines = [p.title || DRAFT_TITLE, ''];

  lines.push(`1. ${titles.batchSummary}`);
  lines.push(p.batchSummary || '—');
  lines.push('');

  lines.push(`2. ${titles.draftRows}`);
  for (const row of p.draftRows || []) {
    lines.push(
      `- ${row.companyName} | ${row.marketTown} | ${row.contactRole} | ${row.fitReason}`
    );
  }
  if (!(p.draftRows || []).length) lines.push('- —');
  lines.push('');

  lines.push(`3. ${titles.reviewNotes}`);
  for (const item of p.reviewNotes || []) lines.push(`- ${item}`);
  if (!(p.reviewNotes || []).length) lines.push('- —');
  lines.push('');

  lines.push(`4. ${titles.guardrails}`);
  for (const item of p.guardrails || []) lines.push(`- ${item}`);
  if (!(p.guardrails || []).length) lines.push('- —');
  lines.push('');

  lines.push(`5. ${titles.recommendedNextStep}`);
  lines.push(p.recommendedNextStep || '—');
  lines.push('');

  lines.push(p.disclaimer || DRAFT_DISCLAIMER);
  return lines.join('\n').trim();
}

function markBuildProposalApproved(proposal, slots) {
  const base =
    proposal && typeof proposal === 'object' && Object.keys(proposal).length
      ? proposal
      : null;
  const nextSlots = {
    ...slots,
    previewApproved: true,
    previewGenerated: true,
    criteriaGenerated: true,
    criteriaApproved: true,
    buildProposalGenerated: true,
    buildProposalApproved: true,
  };
  if (!base) {
    return { buildProposal: null, slots: nextSlots };
  }
  return {
    buildProposal: {
      ...base,
      status: 'approved',
      approvedAt: base.approvedAt || new Date().toISOString(),
    },
    slots: nextSlots,
  };
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

/**
 * Split inclusion/exclusion criteria without comma-splitting locations or roles.
 * Commas inside "Bedford, Hooksett, Londonderry…" must stay in one bullet.
 */
function splitCriteriaItems(text) {
  const s = String(text || '').trim();
  if (!s) return [];
  return s
    .split(/\n|;|•|\u2022|(?<=\.)\s+(?=[A-Z])/g)
    .map((x) => x.replace(/^[-–—*\d.)\s]+/, '').trim())
    .filter((x) => x.length > 2)
    .slice(0, 12);
}

const CRITERIA_TOWN_FRAGMENTS = new Set([
  'bedford',
  'hooksett',
  'londonderry',
  'auburn',
  'goffstown',
  'manchester',
]);

function isGeographyFragmentItem(item) {
  const s = String(item || '')
    .trim()
    .toLowerCase()
    .replace(/\.$/, '');
  if (!s) return true;
  if (CRITERIA_TOWN_FRAGMENTS.has(s)) return true;
  if (/^(or|and)\s+nearby\b/.test(s)) return true;
  return false;
}

/**
 * True when a criteria list looks like comma-split geography fragments or
 * dash-stitched prose rather than clean inclusion/exclusion bullets.
 */
function looksLikeMalformedCriteriaList(items) {
  const list = (items || [])
    .map((x) => String(x || '').trim())
    .filter(Boolean);
  if (!list.length) return true;

  const townOnlyCount = list.filter(isGeographyFragmentItem).length;
  if (townOnlyCount >= 1 && list.length <= 8 && townOnlyCount >= Math.min(2, list.length)) {
    return true;
  }
  if (list.length === 1 && isGeographyFragmentItem(list[0])) return true;

  const shortFragments = list.filter((item) => item.length < 12).length;
  if (shortFragments >= 2) return true;

  if (
    list.some((item) =>
      /each prospect record|required prospect|review gate/i.test(item)
    )
  ) {
    return true;
  }

  // "Segment prose - Located in Bedford, Hooksett…" stitched into one bullet
  // or split into broken pieces.
  if (
    list.some(
      (item) =>
        /\s[-–—]\s+located in\b/i.test(item) ||
        (/^located in\b/i.test(item) && /bedford|hooksett/i.test(item)) ||
        (/^small to mid-sized\b/i.test(item) && /\s[-–—]\s+/.test(item))
    )
  ) {
    return true;
  }

  if (list.some((item) => looksTruncatedArtifactText(item))) return true;
  return false;
}

function looksLikeCleanCriteriaList(items) {
  return (
    Array.isArray(items) &&
    items.length >= 3 &&
    !looksLikeMalformedCriteriaList(items)
  );
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
  const criteriaLabels = /include|exclude|inclusion|exclusion|avoid/i.test(
    labels.join(' ')
  );
  const splitFn = criteriaLabels ? splitCriteriaItems : splitList;
  if (!m || !m[1]) {
    const inline = extractLabeledSection(text, labels);
    return inline ? splitFn(inline.split(/\n\s*\n/)[0] || inline) : [];
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
  return splitFn(firstPara);
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

  // Never comma-split inclusion/exclusion — towns/roles stay in one bullet.
  const inclusionFromLabels = [
    ...extractBulletBlock(blob, [
      'include property managers who',
      'include',
      'includes',
      'inclusion criteria',
      'must include',
    ]),
    ...splitCriteriaItems(peelRecordFieldsClause(inclusionRaw)),
  ].filter((item) => !looksLikeMalformedCriteriaList([item]));
  const exclusionFromLabels = [
    ...extractBulletBlock(blob, [
      'exclude property managers who',
      'exclude',
      'excludes',
      'exclusion criteria',
      'must exclude',
      'avoid',
    ]),
    ...splitCriteriaItems(
      peelRecordFieldsClause(peelReviewGateClause(exclusionRaw))
    ),
  ].filter(
    (item) =>
      !looksLikeMalformedCriteriaList([item]) &&
      !/each prospect record|review gate/i.test(item)
  );

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

  const inclusionCriteria = looksLikeCleanCriteriaList(
    fields && fields.inclusionCriteria
  )
    ? fields.inclusionCriteria
    : defaultInclusionCriteria(context, answers);

  // Property-manager first campaigns use the polished exclusion set. Operator
  // lists only win when every item already matches that shape (never raw
  // transcript / avoid-wrapper bleed, and never as Subtype content).
  let exclusionCriteria = defaultExclusionCriteria(context, answers);
  if (
    looksLikeCleanCriteriaList(fields && fields.exclusionCriteria) &&
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

  const synthesis = buildArtifactSynthesisContext({
    context: ctx,
    normalizedFacts: opts.normalizedFacts || null,
    priorArtifact: {
      businessName: name,
      campaignObjective: objectivePart.campaignObjective,
      coreValidationQuestion: objectivePart.coreValidationQuestion,
      targetSegment: segmentPart.targetSegment,
      targetSubtype: segmentPart.targetSubtype,
      marketBound,
      proofAssetsAvailable: proofPart.proofAssetsAvailable,
      validationMetricsPrimary: metricsPart.validationMetricsPrimary,
      exclusionCriteria: segmentPart.exclusionCriteria,
    },
    answers: ans,
  });

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
    synthesisPhrases: { ...synthesis.phrases },
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
    priorPreview: opts.priorCriteriaPreview || null,
  });
  const approved = markCampaignPlanPreviewApproved(
    priorPreview,
    {
      ...slots,
      previewApproved: true,
      previewGenerated: true,
      criteriaGenerated: true,
    },
    opts
  );
  const lines = [];
  if (leadIn) lines.push(leadIn, '');
  lines.push(formatProspectListCriteriaPreviewMessage(criteriaPreview));
  return {
    message: lines.join('\n'),
    step: 'prospect_list_criteria_preview',
    answers,
    slots: {
      ...approved.slots,
      criteriaGenerated: true,
    },
    preview: approved.preview,
    criteriaPreview,
    buildProposal: null,
    intent: 'produce_criteria_preview',
    previewApproved: true,
  };
}

function produceBuildProposalResult(ctx, answers, slots, opts, leadIn) {
  const priorPreview = opts.priorPreview || null;
  const priorCriteria = opts.priorCriteriaPreview || null;
  const approvedPreview = markCampaignPlanPreviewApproved(
    priorPreview,
    {
      ...slots,
      previewApproved: true,
      previewGenerated: true,
      criteriaGenerated: true,
      criteriaApproved: true,
    },
    opts
  );
  const approvedCriteria = markCriteriaPreviewApproved(
    priorCriteria ||
      buildProspectListCriteriaPreview(ctx, approvedPreview.slots, {
        blueprintId: opts.blueprintId || ctx.blueprintId,
        blueprintVersion: opts.blueprintVersion || ctx.blueprintVersion,
        answers,
      }),
    approvedPreview.slots
  );
  const buildProposal = buildProspectListBuildProposal(
    ctx,
    approvedCriteria.slots,
    {
      blueprintId: opts.blueprintId || ctx.blueprintId,
      blueprintVersion: opts.blueprintVersion || ctx.blueprintVersion,
      answers,
      priorCriteriaPreview: approvedCriteria.criteriaPreview,
    }
  );
  const lines = [];
  if (leadIn) lines.push(leadIn, '');
  lines.push(formatProspectListBuildProposalMessage(buildProposal));
  return {
    message: lines.join('\n'),
    step: PROSPECT_LIST_BUILD_PROPOSAL_STEP,
    answers,
    slots: {
      ...approvedCriteria.slots,
      buildProposalGenerated: true,
    },
    preview: approvedPreview.preview,
    criteriaPreview: approvedCriteria.criteriaPreview,
    buildProposal,
    prospectListDraft: null,
    intent: 'produce_build_proposal',
    previewApproved: true,
    criteriaApproved: true,
    planningState: PROSPECT_LIST_CRITERIA_APPROVED_STEP,
  };
}

function produceProspectListDraftResult(ctx, answers, slots, opts, leadIn) {
  const priorPreview = opts.priorPreview || null;
  const priorCriteria = opts.priorCriteriaPreview || null;
  const priorBuild = opts.priorBuildProposal || null;
  const approvedPreview = markCampaignPlanPreviewApproved(
    priorPreview,
    {
      ...slots,
      previewApproved: true,
      previewGenerated: true,
      criteriaGenerated: true,
      criteriaApproved: true,
      buildProposalGenerated: true,
      buildProposalApproved: true,
      draftRequested: true,
      draftGenerated: true,
    },
    opts
  );
  const approvedCriteria = markCriteriaPreviewApproved(
    priorCriteria ||
      buildProspectListCriteriaPreview(ctx, approvedPreview.slots, {
        blueprintId: opts.blueprintId || ctx.blueprintId,
        blueprintVersion: opts.blueprintVersion || ctx.blueprintVersion,
        answers,
      }),
    approvedPreview.slots
  );
  const approvedBuild = markBuildProposalApproved(
    priorBuild ||
      buildProspectListBuildProposal(ctx, approvedCriteria.slots, {
        blueprintId: opts.blueprintId || ctx.blueprintId,
        blueprintVersion: opts.blueprintVersion || ctx.blueprintVersion,
        answers,
        priorCriteriaPreview: approvedCriteria.criteriaPreview,
      }),
    approvedCriteria.slots
  );
  const draft = buildReviewableProspectListDraft(
    ctx,
    approvedBuild.slots,
    {
      blueprintId: opts.blueprintId || ctx.blueprintId,
      blueprintVersion: opts.blueprintVersion || ctx.blueprintVersion,
      answers,
      priorCriteriaPreview: approvedCriteria.criteriaPreview,
      priorBuildProposal: approvedBuild.buildProposal,
    }
  );
  const lines = [];
  if (leadIn) lines.push(leadIn, '');
  lines.push(formatReviewableProspectListDraftMessage(draft));
  return {
    message: lines.join('\n'),
    step: PROSPECT_LIST_DRAFT_GENERATED_STEP,
    answers,
    slots: {
      ...approvedBuild.slots,
      draftRequested: true,
      draftGenerated: true,
    },
    preview: approvedPreview.preview,
    criteriaPreview: approvedCriteria.criteriaPreview,
    buildProposal: approvedBuild.buildProposal,
    prospectListDraft: draft,
    intent: 'produce_prospect_list_draft',
    previewApproved: true,
    criteriaApproved: true,
    buildProposalApproved: true,
    planningState: PROSPECT_LIST_DRAFT_GENERATED_STEP,
    currentAsk: null,
  };
}

function acknowledgeCriteriaHold(ctx, answers, slots, opts, note) {
  const priorPreview = opts.priorPreview || null;
  const priorCriteria = opts.priorCriteriaPreview || null;
  return {
    message:
      note ||
      'The Prospect List Criteria Preview is already available. Approve it, ask how I would approach building the first list, or request a revision.',
    step: priorCriteria ? 'prospect_list_criteria_preview' : 'prospect_list_criteria',
    answers,
    slots,
    preview: priorPreview,
    criteriaPreview: priorCriteria,
    buildProposal: opts.priorBuildProposal || null,
    prospectListDraft: opts.priorProspectListDraft || null,
    intent: 'hold_criteria',
    previewApproved: Boolean(slots.previewApproved),
    currentAsk: null,
  };
}

function acknowledgeBuildProposalApproval(ctx, answers, slots, opts, note) {
  const priorPreview = opts.priorPreview || null;
  const priorCriteria = opts.priorCriteriaPreview || null;
  const approved = markBuildProposalApproved(
    opts.priorBuildProposal || null,
    {
      ...slots,
      criteriaApproved: true,
      criteriaGenerated: true,
      buildProposalGenerated: true,
      buildProposalApproved: true,
    }
  );
  return {
    message:
      note ||
      'Build proposal approved. Ask me to generate the first reviewable prospect list batch when ready.',
    step: PROSPECT_LIST_BUILD_PROPOSAL_APPROVED_STEP,
    answers,
    slots: approved.slots,
    preview: priorPreview,
    criteriaPreview: priorCriteria,
    buildProposal: approved.buildProposal,
    prospectListDraft: opts.priorProspectListDraft || null,
    intent: 'build_proposal_approved',
    previewApproved: true,
    criteriaApproved: true,
    buildProposalApproved: true,
    planningState: PROSPECT_LIST_BUILD_PROPOSAL_APPROVED_STEP,
    currentAsk: null,
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
  if (prior.criteriaGenerated || priorSlots.criteriaGenerated) {
    priorSlots.criteriaGenerated = true;
  }
  if (prior.criteriaApproved || priorSlots.criteriaApproved) {
    priorSlots.criteriaApproved = true;
  }
  if (
    prior.buildProposalApproved ||
    priorSlots.buildProposalApproved ||
    (prior.prospectListBuildProposal &&
      prior.prospectListBuildProposal.status === 'approved') ||
    (prior.buildProposal && prior.buildProposal.status === 'approved')
  ) {
    priorSlots.buildProposalApproved = true;
    priorSlots.buildProposalGenerated = true;
  }
  if (
    prior.prospectListBuildProposal ||
    prior.buildProposal ||
    prior.step === PROSPECT_LIST_BUILD_PROPOSAL_STEP ||
    prior.step === PROSPECT_LIST_BUILD_PROPOSAL_APPROVED_STEP
  ) {
    priorSlots.buildProposalGenerated = true;
  }
  if (
    prior.draftRequested ||
    priorSlots.draftRequested ||
    prior.step === PROSPECT_LIST_DRAFT_REQUESTED_STEP ||
    prior.step === PROSPECT_LIST_DRAFT_GENERATED_STEP
  ) {
    priorSlots.draftRequested = true;
  }
  if (
    prior.draftGenerated ||
    priorSlots.draftGenerated ||
    prior.prospectListDraft ||
    prior.reviewableProspectListDraft
  ) {
    priorSlots.draftGenerated = true;
    priorSlots.draftRequested = true;
  }
  if (
    prior.liveSourcingApproved ||
    priorSlots.liveSourcingApproved ||
    (prior.reasoningMemory && prior.reasoningMemory.liveSourcingApproved)
  ) {
    priorSlots.liveSourcingApproved = true;
  }
  if (
    prior.prospectListCriteriaPreview ||
    prior.criteriaPreview
  ) {
    priorSlots.criteriaGenerated = true;
    if (
      (prior.prospectListCriteriaPreview &&
        prior.prospectListCriteriaPreview.status === 'approved') ||
      (prior.criteriaPreview && prior.criteriaPreview.status === 'approved')
    ) {
      priorSlots.criteriaApproved = true;
    }
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

  // Scout Handoff Brief — planning artifact; never live-sourcing / capability boundary.
  if (
    looksLikeScoutHandoffBriefRequest(userMessage) ||
    classifyProspectAcquisitionIntent(userMessage) ===
      PROSPECT_ACQUISITION_INTENTS.CREATE_SCOUT_HANDOFF_BRIEF
  ) {
    const answersEarly = { ...(prior.answers || {}) };
    const syncedEarly = syncAnswersFromSlots(answersEarly, {
      ...priorSlots,
      previewGenerated: true,
      previewApproved: true,
      criteriaGenerated: true,
      criteriaApproved: true,
    });
    return produceScoutHandoffBriefResult(
      ctx,
      syncedEarly,
      {
        ...priorSlots,
        previewGenerated: true,
        previewApproved: true,
        criteriaGenerated: true,
        criteriaApproved: true,
      },
      {
        ...opts,
        priorPreview,
        priorCriteriaPreview:
          opts.priorCriteriaPreview ||
          prior.prospectListCriteriaPreview ||
          prior.criteriaPreview ||
          null,
        priorBuildProposal:
          opts.priorBuildProposal ||
          prior.prospectListBuildProposal ||
          prior.buildProposal ||
          null,
        priorProspectListDraft:
          opts.priorProspectListDraft ||
          prior.prospectListDraft ||
          prior.reviewableProspectListDraft ||
          null,
      },
      'Creating the Scout Handoff Brief from approved campaign/list criteria. Scout will inspect public sources — Max is not live-sourcing here.'
    );
  }

  // Live sourcing only when the operator asks Max to source real prospects now.
  // Sticky liveSourcingApproved alone must not block Scout handoff / other artifacts.
  if (looksLikeLiveSourcingApproval(userMessage)) {
    const answersEarly = { ...(prior.answers || {}) };
    const syncedEarly = syncAnswersFromSlots(answersEarly, {
      ...priorSlots,
      liveSourcingApproved: true,
      previewGenerated: true,
      previewApproved: true,
      criteriaGenerated: true,
      criteriaApproved: true,
      buildProposalGenerated: true,
      buildProposalApproved: true,
    });
    return produceLiveSourcingResult(
      ctx,
      syncedEarly,
      {
        ...priorSlots,
        liveSourcingApproved: true,
        previewGenerated: true,
        previewApproved: true,
        criteriaGenerated: true,
        criteriaApproved: true,
        buildProposalGenerated: true,
        buildProposalApproved: true,
      },
      {
        ...opts,
        priorPreview,
        priorCriteriaPreview:
          opts.priorCriteriaPreview ||
          prior.prospectListCriteriaPreview ||
          prior.criteriaPreview ||
          null,
        priorBuildProposal:
          opts.priorBuildProposal ||
          prior.prospectListBuildProposal ||
          prior.buildProposal ||
          null,
        priorProspectListDraft:
          opts.priorProspectListDraft ||
          prior.prospectListDraft ||
          prior.reviewableProspectListDraft ||
          null,
      },
      null
    );
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
        key === 'criteriaGenerated' ||
        key === 'criteriaApproved' ||
        key === 'buildProposalGenerated' ||
        key === 'buildProposalApproved' ||
        key === 'draftRequested' ||
        key === 'draftGenerated' ||
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
  slots.criteriaGenerated = Boolean(
    slots.criteriaGenerated || priorSlots.criteriaGenerated
  );
  slots.criteriaApproved = Boolean(
    slots.criteriaApproved || priorSlots.criteriaApproved
  );
  slots.buildProposalGenerated = Boolean(
    slots.buildProposalGenerated || priorSlots.buildProposalGenerated
  );
  slots.buildProposalApproved = Boolean(
    slots.buildProposalApproved || priorSlots.buildProposalApproved
  );
  slots.draftRequested = Boolean(
    slots.draftRequested || priorSlots.draftRequested
  );
  slots.draftGenerated = Boolean(
    slots.draftGenerated || priorSlots.draftGenerated
  );

  // If the operator declares approvals + asks for a reviewable draft, force
  // post-preview progression even when session slots were reset/stale.
  {
    const inferredEarly = inferApprovedArtifactsFromMessage(
      opts.reasoningMemory ||
        (opts.reasoningState && opts.reasoningState.reasoningMemory) ||
        {},
      userMessage
    );
    if (
      shouldForceProspectListDraft(userMessage, inferredEarly, {
        priorCriteriaPreview:
          opts.priorCriteriaPreview ||
          prior.prospectListCriteriaPreview ||
          prior.criteriaPreview ||
          null,
        priorBuildProposal:
          opts.priorBuildProposal ||
          prior.prospectListBuildProposal ||
          prior.buildProposal ||
          null,
      })
    ) {
      slots.previewGenerated = true;
      slots.previewApproved = true;
      slots.criteriaGenerated = true;
      slots.criteriaApproved = true;
      slots.buildProposalGenerated = true;
      slots.buildProposalApproved = true;
      slots.draftRequested = true;
    }
  }

  const syncedAnswers = syncAnswersFromSlots(answers, slots);
  // Keep the raw current utterance on the active step for audit.
  syncedAnswers[currentStep] = answers[currentStep];

  const wantPreview =
    detectPreviewRequest(userMessage) || opts.forcePreview;

  // --- Post-preview path: never re-ask objective/segment ---
  if (slots.previewGenerated) {
    // Draft requests always beat revise/slot-reopen logic.
    const earlyForceDraft =
      shouldForceProspectListDraft(
        userMessage,
        opts.reasoningMemory ||
          (opts.reasoningState && opts.reasoningState.reasoningMemory) ||
          {},
        {
          priorCriteriaPreview:
            opts.priorCriteriaPreview ||
            prior.prospectListCriteriaPreview ||
            prior.criteriaPreview ||
            null,
          priorBuildProposal:
            opts.priorBuildProposal ||
            prior.prospectListBuildProposal ||
            prior.buildProposal ||
            null,
        }
      ) || looksLikeProspectListDraftRequest(userMessage);

    if (
      !earlyForceDraft &&
      detectReviseIntent(userMessage) &&
      detectRevisedSlotKeys(userMessage).length
    ) {
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

    const replyOpts = {
      ...opts,
      priorPreview,
      priorCriteriaPreview:
        opts.priorCriteriaPreview ||
        prior.prospectListCriteriaPreview ||
        prior.criteriaPreview ||
        null,
      priorBuildProposal:
        opts.priorBuildProposal ||
        prior.prospectListBuildProposal ||
        prior.buildProposal ||
        null,
      priorProspectListDraft:
        opts.priorProspectListDraft ||
        prior.prospectListDraft ||
        prior.reviewableProspectListDraft ||
        null,
    };

    // Sync approvals declared in the operator message into slot/memory state.
    const inferredMemory = inferApprovedArtifactsFromMessage(
      opts.reasoningMemory ||
        (opts.reasoningState && opts.reasoningState.reasoningMemory) ||
        {},
      userMessage
    );
    if (
      (inferredMemory.approvedArtifacts || []).includes(
        ARTIFACT_KINDS.PROSPECT_LIST_CRITERIA_PREVIEW
      ) ||
      (inferredMemory.approvedArtifacts || []).includes(
        ARTIFACT_KINDS.PROSPECT_CRITERIA
      )
    ) {
      slots.criteriaApproved = true;
      slots.criteriaGenerated = true;
    }
    if (
      (inferredMemory.approvedArtifacts || []).includes(
        ARTIFACT_KINDS.PROSPECT_LIST_BUILD_PROPOSAL
      )
    ) {
      slots.buildProposalApproved = true;
      slots.buildProposalGenerated = true;
    }
    if (
      looksLikeLiveSourcingApproval(userMessage) ||
      (inferredMemory.liveSourcingApproved &&
        looksLikeProspectListDraftRequest(userMessage))
    ) {
      slots.liveSourcingApproved = true;
    }

    // Scout Handoff Brief wins — never block on live-sourcing capability.
    if (
      looksLikeScoutHandoffBriefRequest(userMessage) ||
      classifyProspectAcquisitionIntent(userMessage) ===
        PROSPECT_ACQUISITION_INTENTS.CREATE_SCOUT_HANDOFF_BRIEF
    ) {
      return produceScoutHandoffBriefResult(
        ctx,
        syncedAnswers,
        {
          ...slots,
          previewApproved: true,
          criteriaGenerated: true,
          criteriaApproved: true,
        },
        replyOpts,
        'Creating the Scout Handoff Brief from approved campaign/list criteria. Scout will inspect public sources — Max is not live-sourcing here.'
      );
    }

    // HARD GUARD — explicit live sourcing request never regenerates placeholders.
    // Sticky approval alone is not enough; require a live-sourcing ask (or a
    // draft ask after live approval, which must not emit placeholders).
    if (
      looksLikeLiveSourcingApproval(userMessage) ||
      (slots.liveSourcingApproved &&
        looksLikeProspectListDraftRequest(userMessage))
    ) {
      return produceLiveSourcingResult(
        ctx,
        syncedAnswers,
        {
          ...slots,
          previewApproved: true,
          criteriaGenerated: true,
          criteriaApproved: true,
          buildProposalGenerated: true,
          buildProposalApproved: true,
          liveSourcingApproved: true,
        },
        replyOpts,
        null
      );
    }

    // HARD GUARD — draft request + approved criteria + approved build proposal
    // wins over revise-criteria / criteria-question fallbacks.
    if (
      shouldForceProspectListDraft(userMessage, inferredMemory, {
        priorCriteriaPreview: replyOpts.priorCriteriaPreview,
        priorBuildProposal: replyOpts.priorBuildProposal,
      }) ||
      (looksLikeProspectListDraftRequest(userMessage) &&
        slots.criteriaApproved &&
        (slots.buildProposalApproved || replyOpts.priorBuildProposal))
    ) {
      return produceProspectListDraftResult(
        ctx,
        syncedAnswers,
        {
          ...slots,
          previewApproved: true,
          criteriaGenerated: true,
          criteriaApproved: true,
          buildProposalGenerated: true,
          buildProposalApproved: true,
          draftRequested: true,
        },
        replyOpts,
        'Generating the first reviewable prospect list draft — review-only. No outreach, sends, CRM writes, or account changes.'
      );
    }

    // Never re-ask criteria once approved unless operator explicitly revises criteria.
    // "reviewable prospect list draft" is NOT a revise-criteria request.
    if (
      slots.criteriaApproved &&
      looksLikeReviseCriteriaRequest(userMessage)
    ) {
      return {
        message: [
          `Understood — we'll revise the prospect-list criteria.`,
          ``,
          prospectListCriteriaPrompt(ctx),
        ].join('\n'),
        step: PROSPECT_LIST_CRITERIA_STEP,
        answers: syncedAnswers,
        slots: {
          ...slots,
          criteriaApproved: false,
          criteriaGenerated: false,
          buildProposalApproved: false,
          draftGenerated: false,
        },
        preview: priorPreview,
        criteriaPreview: null,
        buildProposal: null,
        prospectListDraft: null,
        intent: 'revise_criteria',
        currentAsk: 'prospectListCriteria',
        previewApproved: true,
      };
    }

    if (criteriaSlotsReady(slots) || slots.criteriaApproved || replyOpts.priorCriteriaPreview) {
      const priorCriteria = replyOpts.priorCriteriaPreview;
      const criteriaAlreadyShown = Boolean(
        priorCriteria ||
          prior.step === 'prospect_list_criteria_preview' ||
          prior.step === PROSPECT_LIST_CRITERIA_APPROVED_STEP ||
          prior.step === PROSPECT_LIST_BUILD_PROPOSAL_STEP ||
          prior.step === PROSPECT_LIST_BUILD_PROPOSAL_APPROVED_STEP ||
          prior.step === PROSPECT_LIST_DRAFT_REQUESTED_STEP ||
          prior.step === PROSPECT_LIST_DRAFT_GENERATED_STEP ||
          slots.criteriaGenerated ||
          slots.criteriaApproved
      );

      if (!criteriaAlreadyShown) {
        return produceCriteriaPreviewResult(
          ctx,
          syncedAnswers,
          { ...slots, previewApproved: true, criteriaGenerated: true },
          replyOpts,
          'Thanks — I captured the prospect-list criteria. Here is the Prospect List Criteria Preview — still planning-only.'
        );
      }

      // Criteria already shown — classify intent and advance (never silent replay).
      const artifactAction =
        opts.artifactAction ||
        resolveCampaignArtifactAction({
          userMessage,
          messageClass: opts.messageClass || null,
          state: opts.reasoningState || { reasoningMemory: opts.reasoningMemory },
          priorCriteriaPreview: priorCriteria,
          priorBuildProposal: replyOpts.priorBuildProposal,
          priorProspectListDraft: replyOpts.priorProspectListDraft,
          step: prior.step || 'prospect_list_criteria_preview',
        });

      if (artifactAction.action === 'replay_criteria') {
        return produceCriteriaPreviewResult(
          ctx,
          syncedAnswers,
          { ...slots, previewApproved: true, criteriaGenerated: true },
          replyOpts,
          'Here is the Prospect List Criteria Preview again — still planning-only.'
        );
      }

      if (artifactAction.action === 'emit_scout_handoff_brief') {
        return produceScoutHandoffBriefResult(
          ctx,
          syncedAnswers,
          {
            ...slots,
            previewApproved: true,
            criteriaGenerated: true,
            criteriaApproved: true,
          },
          replyOpts,
          artifactAction.note ||
            'Creating the Scout Handoff Brief from approved campaign/list criteria. Scout will inspect public sources — Max is not live-sourcing here.'
        );
      }

      if (artifactAction.action === 'emit_live_sourcing') {
        return produceLiveSourcingResult(
          ctx,
          syncedAnswers,
          {
            ...slots,
            previewApproved: true,
            criteriaGenerated: true,
            criteriaApproved: true,
            buildProposalGenerated: true,
            buildProposalApproved: true,
            liveSourcingApproved: true,
          },
          replyOpts,
          artifactAction.note
        );
      }

      if (artifactAction.action === 'emit_prospect_list_draft') {
        return produceProspectListDraftResult(
          ctx,
          syncedAnswers,
          {
            ...slots,
            previewApproved: true,
            criteriaGenerated: true,
            criteriaApproved: true,
            buildProposalGenerated: true,
            buildProposalApproved: true,
            draftRequested: true,
          },
          replyOpts,
          artifactAction.note ||
            'Generating the first reviewable prospect list draft — review-only. No outreach, sends, CRM writes, or account changes.'
        );
      }

      if (artifactAction.action === 'emit_build_proposal') {
        return produceBuildProposalResult(
          ctx,
          syncedAnswers,
          {
            ...slots,
            previewApproved: true,
            criteriaGenerated: true,
            criteriaApproved: true,
          },
          replyOpts,
          artifactAction.note ||
            'Approved. Here is the Prospect List Build Proposal — still planning-only. No list will be built until you approve this approach.'
        );
      }

      if (artifactAction.action === 'ack_build_approval') {
        return acknowledgeBuildProposalApproval(
          ctx,
          syncedAnswers,
          {
            ...slots,
            criteriaGenerated: true,
            criteriaApproved: true,
            buildProposalGenerated: true,
            buildProposalApproved: true,
          },
          replyOpts,
          artifactAction.note
        );
      }

      if (artifactAction.action === 'ack_approval' || artifactAction.action === 'hold') {
        // Progression guard: never fall back to the criteria question once
        // criteria + build proposal are approved.
        const mem =
          (artifactAction.memory ||
            opts.reasoningMemory ||
            (opts.reasoningState && opts.reasoningState.reasoningMemory) ||
            {});
        if (
          shouldBlockCriteriaQuestionReplay(mem) ||
          (slots.criteriaApproved &&
            (slots.buildProposalApproved || replyOpts.priorBuildProposal))
        ) {
          if (
            looksLikeProspectListDraftRequest(userMessage) ||
            artifactAction.action === 'hold'
          ) {
            if (looksLikeProspectListDraftRequest(userMessage)) {
              return produceProspectListDraftResult(
                ctx,
                syncedAnswers,
                {
                  ...slots,
                  previewApproved: true,
                  criteriaGenerated: true,
                  criteriaApproved: true,
                  buildProposalGenerated: true,
                  buildProposalApproved: true,
                  draftRequested: true,
                },
                replyOpts,
                'Generating the first reviewable prospect list draft — review-only.'
              );
            }
            return acknowledgeBuildProposalApproval(
              ctx,
              syncedAnswers,
              {
                ...slots,
                criteriaApproved: true,
                buildProposalApproved: Boolean(
                  slots.buildProposalApproved || replyOpts.priorBuildProposal
                ),
              },
              replyOpts,
              artifactAction.note ||
                'Criteria and build proposal are approved. Ask me to generate the first reviewable prospect list batch when ready.'
            );
          }
        }
        return acknowledgeCriteriaHold(
          ctx,
          syncedAnswers,
          {
            ...slots,
            criteriaGenerated: true,
            ...(artifactAction.approveKind === ARTIFACT_KINDS.PROSPECT_CRITERIA ||
            artifactAction.approveKind ===
              ARTIFACT_KINDS.PROSPECT_LIST_CRITERIA_PREVIEW
              ? { criteriaApproved: true }
              : {}),
          },
          replyOpts,
          artifactAction.note
        );
      }

      // Default: if build already exists, prefer draft path over replaying build.
      if (replyOpts.priorBuildProposal || slots.buildProposalGenerated) {
        if (looksLikeProspectListDraftRequest(userMessage)) {
          return produceProspectListDraftResult(
            ctx,
            syncedAnswers,
            {
              ...slots,
              previewApproved: true,
              criteriaGenerated: true,
              criteriaApproved: true,
              buildProposalGenerated: true,
              buildProposalApproved: true,
              draftRequested: true,
            },
            replyOpts,
            'Generating the first reviewable prospect list draft — review-only.'
          );
        }
        return acknowledgeBuildProposalApproval(
          ctx,
          syncedAnswers,
          {
            ...slots,
            criteriaApproved: true,
            buildProposalGenerated: true,
          },
          replyOpts,
          'Prospect List Build Proposal is ready. Approve it, or ask me to generate the first reviewable prospect list batch.'
        );
      }

      // Default: treat as build-proposal advance once criteria exist.
      return produceBuildProposalResult(
        ctx,
        syncedAnswers,
        {
          ...slots,
          previewApproved: true,
          criteriaGenerated: true,
          criteriaApproved: true,
        },
        replyOpts,
        'Here is the Prospect List Build Proposal — still planning-only.'
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

    // Approved preview — advance to prospect-list criteria only when criteria
    // are not already approved. Progression guard blocks the banned question
    // once criteria + build proposal live in approvedArtifacts.
    const reasoningMem =
      opts.reasoningMemory ||
      (opts.reasoningState && opts.reasoningState.reasoningMemory) ||
      {};
    if (
      slots.criteriaApproved ||
      shouldBlockCriteriaQuestionReplay(reasoningMem)
    ) {
      if (looksLikeProspectListDraftRequest(userMessage)) {
        return produceProspectListDraftResult(
          ctx,
          syncedAnswers,
          {
            ...slots,
            previewApproved: true,
            criteriaApproved: true,
            buildProposalApproved: true,
            draftRequested: true,
          },
          replyOpts,
          'Generating the first reviewable prospect list draft — review-only.'
        );
      }
      if (replyOpts.priorBuildProposal) {
        return acknowledgeBuildProposalApproval(
          ctx,
          syncedAnswers,
          { ...slots, criteriaApproved: true },
          replyOpts,
          'Criteria already approved. Approve the build proposal or ask for the first reviewable prospect list draft.'
        );
      }
      return produceBuildProposalResult(
        ctx,
        syncedAnswers,
        {
          ...slots,
          previewApproved: true,
          criteriaGenerated: true,
          criteriaApproved: true,
        },
        replyOpts,
        'Criteria already approved. Here is the Prospect List Build Proposal — still planning-only.'
      );
    }

    const approved = markCampaignPlanPreviewApproved(priorPreview, slots, opts);
    const criteriaMessage = prospectListCriteriaPrompt(ctx);
    if (
      shouldBlockCriteriaQuestionReplay(reasoningMem) &&
      isBannedCriteriaReplayQuestion(criteriaMessage)
    ) {
      return produceBuildProposalResult(
        ctx,
        syncedAnswers,
        { ...approved.slots, criteriaApproved: true },
        replyOpts,
        'Continuing from approved criteria — here is the Prospect List Build Proposal.'
      );
    }
    return {
      message: criteriaMessage,
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
  BUILD_PROPOSAL_ARTIFACT_KIND,
  BUILD_PROPOSAL_TITLE,
  BUILD_PROPOSAL_DISCLAIMER,
  DRAFT_ARTIFACT_KIND,
  DRAFT_TITLE,
  DRAFT_DISCLAIMER,
  LIVE_SOURCING_BOUNDARY_MESSAGE,
  LIVE_PROSPECT_LIST_KIND,
  LIVE_PROSPECT_LIST_TITLE,
  SCOUT_HANDOFF_BRIEF_KIND,
  SCOUT_HANDOFF_BRIEF_TITLE,
  SCOUT_HANDOFF_BRIEF_DISCLAIMER,
  SCOUT_HANDOFF_SECTION_TITLES,
  SECTION_TITLES,
  CRITERIA_SECTION_TITLES,
  BUILD_PROPOSAL_SECTION_TITLES,
  DRAFT_SECTION_TITLES,
  CAMPAIGN_PLANNING_STATES,
  CONVERSATION_STEPS,
  QUESTION_BANK,
  SLOT_KEYS,
  PRE_PREVIEW_SLOT_ORDER,
  PROSPECT_LIST_CRITERIA_STEP,
  PROSPECT_LIST_CRITERIA_APPROVED_STEP,
  PROSPECT_LIST_BUILD_PROPOSAL_STEP,
  PROSPECT_LIST_BUILD_PROPOSAL_APPROVED_STEP,
  PROSPECT_LIST_DRAFT_REQUESTED_STEP,
  PROSPECT_LIST_DRAFT_GENERATED_STEP,
  PROSPECT_LIST_DRAFT_REVIEWED_STEP,
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
  buildProspectListBuildProposal,
  formatProspectListBuildProposalMessage,
  buildReviewableProspectListDraft,
  formatReviewableProspectListDraftMessage,
  buildScoutHandoffBrief,
  formatScoutHandoffBriefMessage,
  produceScoutHandoffBriefResult,
  isLivePublicSourcingSupported,
  produceLiveSourcingResult,
  formatLiveSourcedProspectListMessage,
  markCriteriaPreviewApproved,
  markBuildProposalApproved,
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
  // Max Synthesis Layer re-exports for tests / callers
  buildArtifactSynthesisContext,
  containsRawPromptFragment,
  findRawPromptFragments,
};
