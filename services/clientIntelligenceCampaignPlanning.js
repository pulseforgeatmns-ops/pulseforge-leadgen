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

/**
 * Deterministic reply for the campaign planning conversation.
 *
 * @returns {{ message: string, step: string, answers: object, preview: object|null, intent: string|null }}
 */
function buildCampaignPlanningReply(userMessage, state, context, opts = {}) {
  const prior = state || {};
  const currentStep =
    prior.step && prior.step !== 'opening' ? prior.step : 'opening';
  const answers = { ...(prior.answers || {}) };
  answers[currentStep] = {
    raw: String(userMessage || '').trim(),
    at: new Date().toISOString(),
  };

  const ctx = context || prior.context || {};
  const wantPreview =
    detectPreviewRequest(userMessage) ||
    currentStep === 'approval_checkpoints' ||
    opts.forcePreview;

  if (wantPreview) {
    const preview = buildFirstCampaignPlanPreview(ctx, answers, {
      blueprintId: opts.blueprintId || ctx.blueprintId,
      blueprintVersion: opts.blueprintVersion || ctx.blueprintVersion,
    });
    return {
      message: [
        `Thanks — I have enough to draft the First Campaign Plan Preview.`,
        ``,
        formatFirstCampaignPlanPreviewMessage(preview),
      ].join('\n'),
      step: 'preview',
      answers,
      preview,
      intent: 'produce_preview',
    };
  }

  // Advance from opening into the question bank.
  if (currentStep === 'opening') {
    const nxt = QUESTION_BANK[0];
    return {
      message: [
        `Got it — we'll plan from the approved focus${
          /\bnarrow\b/i.test(String(userMessage || ''))
            ? ', with your narrower first test noted'
            : ' as defined'
        }.`,
        ``,
        nxt.prompt,
      ].join('\n'),
      step: nxt.step,
      answers,
      preview: null,
      intent: 'advance',
    };
  }

  const nxt = nextQuestion(currentStep);
  if (!nxt) {
    const preview = buildFirstCampaignPlanPreview(ctx, answers, {
      blueprintId: opts.blueprintId || ctx.blueprintId,
      blueprintVersion: opts.blueprintVersion || ctx.blueprintVersion,
    });
    return {
      message: formatFirstCampaignPlanPreviewMessage(preview),
      step: 'preview',
      answers,
      preview,
      intent: 'produce_preview',
    };
  }

  return {
    message: [
      `Noted for ${currentStep.replace(/_/g, ' ')}.`,
      ``,
      nxt.prompt,
    ].join('\n'),
    step: nxt.step,
    answers,
    preview: null,
    intent: 'advance',
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
  SECTION_TITLES,
  CONVERSATION_STEPS,
  QUESTION_BANK,
  DEFAULT_PROOF_ASSETS,
  DEFAULT_PROOF_ASSETS_AVAILABLE,
  DEFAULT_PROOF_ASSETS_MISSING,
  DEFAULT_VALIDATION_METRICS,
  DEFAULT_VALIDATION_METRICS_PRIMARY,
  DEFAULT_VALIDATION_METRICS_SECONDARY,
  DEFAULT_INCLUSION_CRITERIA,
  DEFAULT_EXCLUSION_CRITERIA,
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
  detectPreviewRequest,
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
