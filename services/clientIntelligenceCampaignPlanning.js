'use strict';

/**
 * SPEC-089 — First Campaign Planning Conversation.
 *
 * After Growth Plan completion, Max helps define the first campaign hypothesis
 * and validation gates. Review-first only: no prospect lists, outreach copy,
 * sends, CRM writes, or account/DNS/GBP/social/tracking changes.
 */

const ARTIFACT_KIND = 'first_campaign_plan_preview';
const PREVIEW_TITLE = 'First Campaign Plan Preview';
const PREVIEW_DISCLAIMER =
  'Planning preview only. No prospect list, outreach copy, sends, CRM writes, or account changes have been created or launched.';

const SECTION_TITLES = Object.freeze({
  campaignObjective: 'Campaign objective',
  targetSegment: 'Target segment',
  marketBound: 'Market bound',
  hypothesis: 'Campaign hypothesis',
  proofAssetsNeeded: 'Proof assets needed',
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

const DEFAULT_PROOF_ASSETS = Object.freeze([
  'Commercial cleaning checklist',
  'Before/after photos or examples',
  'Clear response-time expectation',
  'References or testimonials if available',
  'Clear service area',
  'Walkthrough/estimate process',
]);

const DEFAULT_VALIDATION_METRICS = Object.freeze([
  'Qualified replies',
  'Decision-maker conversations',
  'Walkthroughs or site visits booked',
  'Estimate or proposal requests',
  'Evidence about which property-manager subtype responds best',
]);

const DEFAULT_APPROVAL_CHECKPOINTS = Object.freeze([
  'Operator approves this campaign plan preview.',
  'Proof assets are confirmed ready.',
  'High-priority infrastructure gaps are reviewed.',
  'Prospect list creation is approved separately.',
  'Outreach copy is approved separately.',
  'Launch is approved separately.',
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
  const s = String(summary || '').trim();
  if (!s) return 'buyers focused only on the lowest price';
  return s
    .replace(/^who only care about\s+/i, '')
    .replace(/^customers who only care about\s+/i, '')
    .replace(/\.$/, '')
    .trim() || 'buyers focused only on the lowest price';
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

function stripTrailingMarket(text, market) {
  const s = String(text || '').trim();
  const m = String(market || '').trim();
  if (!s || !m) return s;
  const re = new RegExp(
    `\\s+(?:in|across|within|around)\\s+${m.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\.?$`,
    'i'
  );
  return s.replace(re, '').trim();
}

function containsMarket(text, market) {
  const s = String(text || '').toLowerCase();
  const m = String(market || '').toLowerCase();
  return Boolean(m) && s.includes(m);
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
    .split(/\n|;|,(?=\s)|•|\u2022/g)
    .map((x) => x.replace(/^[-–—*\d.)\s]+/, '').trim())
    .filter((x) => x.length > 2)
    .slice(0, 8);
}

function polishProofAssetLabel(item) {
  const s = String(item || '').trim();
  if (!s) return s;
  const lower = s.toLowerCase();
  if (/checklist/.test(lower)) return 'Commercial cleaning checklist';
  if (/before\/after|photo|example/.test(lower)) {
    return 'Before/after photos or examples';
  }
  if (/response[- ]?time/.test(lower)) return 'Clear response-time expectation';
  if (/reference|testimonial/.test(lower)) {
    return 'References or testimonials if available';
  }
  if (/service area/.test(lower)) return 'Clear service area';
  if (/walkthrough|estimate/.test(lower)) return 'Walkthrough/estimate process';
  return s.charAt(0).toUpperCase() + s.slice(1);
}

function defaultProofAssets() {
  return [...DEFAULT_PROOF_ASSETS];
}

function defaultValidationMetrics(context, answers) {
  if (isPropertyManagerFocus(context, answers)) {
    return [...DEFAULT_VALIDATION_METRICS];
  }
  return [
    'Qualified replies',
    'Decision-maker conversations',
    'Walkthroughs or site visits booked',
    'Estimate or proposal requests',
    'Evidence about which subtype responds best',
  ];
}

function defaultApprovalCheckpoints() {
  return [...DEFAULT_APPROVAL_CHECKPOINTS];
}

function defaultRisks(context, answers) {
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
  risks.push('Market demand has not been validated yet.');
  risks.push(
    'Strong response could create capacity or scheduling pressure.'
  );
  if (isPropertyManagerFocus(context, answers)) {
    risks.push('Lowest-price buyers should not define the test.');
  } else {
    const avoid = (context && context.avoidPhrase) || 'lowest-price buyers';
    risks.push(`${avoid.charAt(0).toUpperCase()}${avoid.slice(1)} should not define the test.`);
  }
  const proof = answerText(answers, 'proof_assets');
  if (/missing|need|don't have|do not have|none|still/i.test(proof)) {
    risks.splice(
      1,
      0,
      'Proof assets are incomplete — credibility gaps can weaken the first test.'
    );
  }
  return uniqueStrings(risks).slice(0, 6);
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

function defaultTargetSegmentBody(context) {
  const market = (context && context.targetMarket) || 'Greater Manchester';
  if (/property manager/i.test(String(context && context.primarySegment))) {
    return `Small to mid-sized local property managers in ${market} who oversee offices, mixed-use buildings, small commercial properties, or multi-tenant spaces.`;
  }
  const primary = (context && context.primarySegment) || 'the focus segment';
  return `Small to mid-sized local ${primary} in ${market}, aligned with the approved Blueprint first focus.`;
}

/**
 * Strip internal labels / awkward joins so the target segment never opens with
 * lowercase keys like "property managers — …".
 */
function sanitizeTargetSegmentText(text, context) {
  let s = String(text || '').replace(/\s+/g, ' ').trim();
  if (!s) return defaultTargetSegmentBody(context);

  // "property managers — Small to mid-sized…" / "property_managers - …"
  s = s.replace(
    /^(?:the\s+)?property[_\s-]*managers?\s*[—–:-]+\s*/i,
    ''
  );
  s = s.replace(/^(?:segment|primary|target)\s*[—–:-]+\s*/i, '');

  // If a validation subtype body already has the polished sentence, normalize it.
  if (/^small to mid-sized/i.test(s)) {
    s = s
      .replace(/\bwho manage\b/i, 'who oversee')
      .replace(/\boverseeing\b/i, 'who oversee');
    // Prefer "who oversee … in Market" → "in Market who oversee …"
    const market = (context && context.targetMarket) || 'Greater Manchester';
    const mRe = new RegExp(
      `^(Small to mid-sized local property managers) who oversee (.+?) in ${market.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\.?$`,
      'i'
    );
    const m = s.match(mRe);
    if (m) {
      s = `${m[1]} in ${market} who oversee ${m[2]}.`;
    }
    // Incomplete polish → canonical default for property-manager focus.
    if (
      /property manager/i.test(String(context && context.primarySegment)) &&
      !/mixed-use|multi-tenant/i.test(s)
    ) {
      return defaultTargetSegmentBody(context);
    }
  }

  if (!s || /^[a-z]/.test(s) || /^(property managers?|professional offices?)\b/i.test(s)) {
    return defaultTargetSegmentBody(context);
  }

  return s.charAt(0).toUpperCase() + s.slice(1);
}

function defaultTargetSegmentAvoid(context) {
  const avoidRaw =
    (context && context.avoidPhrase) || 'buyers focused only on the lowest price';
  const avoid = String(avoidRaw).replace(/^./, (c) => c.toLowerCase());
  if (/property manager/i.test(String(context && context.primarySegment))) {
    return `Avoid large institutional property managers, overly complex properties, and ${avoid}.`;
  }
  return `Avoid overly complex accounts and ${avoid}.`;
}

function resolveTargetSegment(context, answers) {
  const segmentAnswer = answerText(answers, 'target_segment');
  const opening = answerText(answers, 'opening');
  const market = (context && context.targetMarket) || 'Greater Manchester';
  const subtype = (context && context.subtype) || '';

  // Prefer polished default for "as defined", thin, or awkward "segment — subtype" joins.
  const keepAsDefined = /\bas defined\b|\bexactly as\b|\bas-is\b|\bkeep (it |them )?as\b/i.test(
    `${opening} ${segmentAnswer}`
  );
  const awkwardJoin = /property[_\s-]*managers?\s*[—–:-]\s*/i.test(
    `${segmentAnswer} ${subtype}`
  );
  if (!segmentAnswer || keepAsDefined || awkwardJoin || segmentAnswer.length < 48) {
    // If subtype is already a polished sentence, sanitize rather than re-prefix.
    if (subtype && /^small to mid-sized/i.test(subtype) && !keepAsDefined) {
      return sanitizeTargetSegmentText(subtype, context);
    }
    return defaultTargetSegmentBody(context);
  }
  return sanitizeTargetSegmentText(
    stripTrailingMarket(segmentAnswer, market),
    context
  );
}

function resolveTargetSegmentAvoid(context, answers) {
  return defaultTargetSegmentAvoid(context);
}

function resolveMarketBound(context, answers) {
  const marketAnswer = answerText(answers, 'market_bounds');
  const market = (context && context.targetMarket) || 'Greater Manchester';
  const towns =
    (context && Array.isArray(context.towns) && context.towns.length
      ? context.towns
      : DEFAULT_TOWNS
    ).slice(0, 5);

  if (marketAnswer && marketAnswer.length > 24) {
    return marketAnswer;
  }
  return `${market}, with early attention on ${naturalList(towns)}.`;
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

function resolveObjective(context, answers) {
  const objRaw = answerText(answers, 'campaign_objective');
  const obj = stripFirstPersonArtifactLanguage(objRaw);
  const name = shortName((context && context.businessName) || 'the business');
  const market = (context && context.targetMarket) || 'Greater Manchester';
  const pm = isPropertyManagerFocus(context, answers);

  if (obj && obj.length > 80 && /core validation question|core question/i.test(obj)) {
    return stripFirstPersonArtifactLanguage(
      obj.replace(/Core question:/gi, 'Core validation question:')
    );
  }

  let proveLine = obj && obj.length > 20
    ? (/^prove\b/i.test(obj)
        ? obj.replace(/\.$/, '')
        : `Prove that ${obj.replace(/^prove that\s+/i, '').replace(/\.$/, '')}`)
    : pm
      ? `Prove that small to mid-sized property managers in ${market} are willing to have a real conversation about recurring cleaning, rather than ignoring the outreach or responding only on price`
      : `Prove that ${context.primarySegment || 'the focus segment'} in ${market} will take a real conversation about recurring service, rather than ignoring the outreach or responding only on price`;

  // Prefer the tighter closing clause on the prove line.
  if (
    !/rather than ignoring the outreach or responding only on price/i.test(
      proveLine
    )
  ) {
    if (/not just ignore the outreach or shop on price/i.test(proveLine)) {
      proveLine = proveLine.replace(
        /,??\s*not just ignore the outreach or shop on price\.?$/i,
        ', rather than ignoring the outreach or responding only on price'
      );
    } else if (pm || /property manager/i.test(proveLine)) {
      proveLine = `${proveLine.replace(/\.$/, '')}, rather than ignoring the outreach or responding only on price`;
    }
  }

  const strong = pm
    ? 'The strongest signal would be a walkthrough or estimate request from a qualified property manager. Good early signals include positive replies, questions about recurring service, reliability, responsiveness, scheduling, or current cleaning frustrations.'
    : 'The strongest signal would be a walkthrough or estimate request from a qualified decision-maker. Good early signals include positive replies and questions about recurring service, reliability, or responsiveness.';

  const weak = pm
    ? `Weak signals include price-only replies, vague interest with no next step, or responses from properties outside ${name}'s service area or beyond current operational fit.`
    : `Weak signals include price-only replies, vague interest with no next step, or responses outside ${name}'s service area or operational fit.`;

  const core = pm
    ? `Can ${name} create qualified property-manager conversations that turn into walkthroughs or estimates?`
    : `Can ${name} create qualified conversations that turn into walkthroughs or estimates?`;

  return stripFirstPersonArtifactLanguage(
    [
      `${proveLine.replace(/\.$/, '')}.`,
      '',
      strong,
      '',
      weak,
      '',
      'Core validation question:',
      core,
    ].join('\n')
  );
}

function resolveHypothesis(context, answers) {
  const h = answerText(answers, 'hypothesis');
  const name = shortName((context && context.businessName) || 'the business');
  const market = (context && context.targetMarket) || 'Greater Manchester';
  const pm = isPropertyManagerFocus(context, answers);

  if (h) {
    // Drop duplicated geography if the segment clause already includes the market.
    let cleaned = h.replace(/\s+/g, ' ').trim();
    const dup = new RegExp(
      `(local\\s+)?property managers(?:\\s+in\\s+${market.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')})?\\s+in\\s+${market.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}`,
      'i'
    );
    cleaned = cleaned.replace(dup, (match) => {
      if (/in\s+/i.test(match) && (match.match(/in\s+/gi) || []).length > 1) {
        return match.replace(
          new RegExp(`\\s+in\\s+${market.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, 'i'),
          ''
        );
      }
      return match;
    });
    // Generic "X in Market in Market"
    cleaned = cleaned.replace(
      new RegExp(
        `\\bin\\s+${market.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\s+in\\s+${market.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}`,
        'gi'
      ),
      `in ${market}`
    );
    return cleaned;
  }

  if (pm) {
    return `If ${name} approaches local property managers with clear proof of reliability, responsiveness, and a simple walkthrough path, the campaign should create qualified conversations and at least some walkthrough or estimate interest within the validation window.`;
  }
  const segment = stripTrailingMarket(
    resolveTargetSegment(context, answers),
    market
  );
  const segmentClause = containsMarket(segment, market)
    ? segment
    : `${segment} in ${market}`;
  return `If ${name} approaches ${segmentClause} with clear proof of reliability, responsiveness, and a simple walkthrough path, the campaign should create qualified conversations and at least some walkthrough or estimate interest within the validation window.`;
}

function resolveProofAssets(context, answers) {
  const proof = answerText(answers, 'proof_assets');
  if (proof) {
    const listed = splitList(proof).map(polishProofAssetLabel);
    if (listed.length >= 3) return uniqueStrings(listed).slice(0, 6);
  }
  return defaultProofAssets();
}

function resolveValidationMetrics(context, answers) {
  const m = answerText(answers, 'validation_metrics');
  if (m) {
    const listed = splitList(m);
    if (listed.length >= 3) return listed.slice(0, 6);
  }
  return defaultValidationMetrics(context, answers);
}

function resolveApprovalCheckpoints(answers) {
  const a = answerText(answers, 'approval_checkpoints');
  if (a) {
    const listed = splitList(a);
    // Prefer the concise review-first gate list unless the operator gave a full set.
    if (listed.length >= 5) {
      return listed.map((item) =>
        /\.$/.test(item) ? item : `${item}.`
      );
    }
  }
  return defaultApprovalCheckpoints();
}

function resolveRecommendedNextStep() {
  return 'Review and approve the campaign plan preview. After approval, Max can help define the prospect-list criteria before any list is built.';
}

function buildFirstCampaignPlanPreview(context, answers, opts = {}) {
  const ctx = context || {};
  const ans = answers || {};
  const objective = resolveObjective(ctx, ans);
  const targetSegment = sanitizeTargetSegmentText(
    resolveTargetSegment(ctx, ans),
    ctx
  );
  const targetSegmentAvoid = resolveTargetSegmentAvoid(ctx, ans);
  const marketBound = resolveMarketBound(ctx, ans);
  const hypothesis = stripFirstPersonArtifactLanguage(
    resolveHypothesis(ctx, ans)
  );
  const proofAssetsNeeded = resolveProofAssets(ctx, ans);
  const validationMetrics = resolveValidationMetrics(ctx, ans);
  const approvalCheckpoints = resolveApprovalCheckpoints(ans);
  const risksCautions = defaultRisks(ctx, ans);
  const name = shortName(ctx.businessName || 'the business');

  return {
    kind: ARTIFACT_KIND,
    title: PREVIEW_TITLE,
    businessName: name,
    campaignObjective: objective,
    targetSegment,
    targetSegmentAvoid,
    marketBound,
    hypothesis,
    proofAssetsNeeded,
    validationMetrics,
    risksCautions,
    approvalCheckpoints,
    recommendedNextStep: resolveRecommendedNextStep(),
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

function formatFirstCampaignPlanPreviewMessage(preview) {
  const p = preview || {};
  const titles = p.sectionTitles || SECTION_TITLES;
  const lines = [p.title || PREVIEW_TITLE, ''];

  lines.push(`1. ${titles.campaignObjective}`);
  lines.push(formatMultilineSection(p.campaignObjective));
  lines.push('');

  lines.push(`2. ${titles.targetSegment}`);
  lines.push(p.targetSegment || '—');
  if (p.targetSegmentAvoid) {
    lines.push('');
    lines.push(p.targetSegmentAvoid);
  }
  lines.push('');

  lines.push(`3. ${titles.marketBound}`);
  lines.push(p.marketBound || '—');
  lines.push('');

  lines.push(`4. ${titles.hypothesis}`);
  lines.push(p.hypothesis || '—');
  lines.push('');

  lines.push(`5. ${titles.proofAssetsNeeded}`);
  for (const item of p.proofAssetsNeeded || []) lines.push(`- ${item}`);
  if (!(p.proofAssetsNeeded || []).length) lines.push('- —');
  lines.push('');

  lines.push(`6. ${titles.validationMetrics}`);
  for (const item of p.validationMetrics || []) lines.push(`- ${item}`);
  if (!(p.validationMetrics || []).length) lines.push('- —');
  lines.push('');

  lines.push(`7. ${titles.risksCautions}`);
  for (const item of p.risksCautions || []) lines.push(`- ${item}`);
  if (!(p.risksCautions || []).length) lines.push('- —');
  lines.push('');

  lines.push(`8. ${titles.approvalCheckpoints}`);
  for (const item of p.approvalCheckpoints || []) lines.push(`- ${item}`);
  if (!(p.approvalCheckpoints || []).length) lines.push('- —');
  lines.push('');

  lines.push(`9. ${titles.recommendedNextStep}`);
  lines.push(p.recommendedNextStep || '—');
  lines.push('');

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
  DEFAULT_VALIDATION_METRICS,
  DEFAULT_APPROVAL_CHECKPOINTS,
  buildCampaignPlanningContext,
  buildCampaignPlanningOpening,
  buildCampaignPlanningReply,
  buildFirstCampaignPlanPreview,
  formatFirstCampaignPlanPreviewMessage,
  detectPreviewRequest,
  containsForbiddenCampaignPlanningLanguage,
  extractBusinessName,
  stepAfterOpening,
  humanizeSegment,
  humanizeStatusLabel,
  sanitizeTargetSegmentText,
  stripFirstPersonArtifactLanguage,
};
