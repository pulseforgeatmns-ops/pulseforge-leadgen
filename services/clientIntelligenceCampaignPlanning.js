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
  'Planning preview only — no prospect list, outreach copy, sends, CRM writes, or account/DNS/GBP/social/tracking changes. This is not an approved or launched campaign.';

const SECTION_TITLES = Object.freeze({
  campaignObjective: 'Campaign objective',
  targetSegment: 'Target segment',
  marketBound: 'Market bound',
  hypothesis: 'Hypothesis',
  proofAssetsNeeded: 'Proof assets needed',
  validationMetrics: 'Validation metrics',
  risksCautions: 'Risks/cautions',
  approvalCheckpoints: 'Approval checkpoints',
  recommendedNextStep: 'Recommended next step',
});

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

  return {
    businessName: shortName(
      (preview && preview.businessName) ||
        (gd && gd.businessName) ||
        extractBusinessName(blueprint)
    ),
    primarySegment: humanizeSegment(primarySegment),
    secondarySegment: humanizeSegment(secondarySegment),
    targetMarket: String(targetMarket || 'Greater Manchester').trim(),
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

function defaultProofAssets(context) {
  if (context && context.proofFromPrior) {
    return splitList(context.proofFromPrior.replace(/\band\b/gi, ',')).length
      ? splitList(context.proofFromPrior.replace(/\band\b/gi, ','))
      : [
          'service checklist',
          'photos/examples',
          'clear response-time expectation',
          'service area',
          'walkthrough/estimate process',
        ];
  }
  return [
    'service checklist',
    'photos/examples',
    'clear response-time expectation',
    'service area',
    'walkthrough/estimate process',
  ];
}

function defaultValidationMetrics() {
  return [
    'Qualified conversations with decision-makers',
    'Walkthroughs or site visits booked',
    'Estimate / proposal requests',
    'Clear signal on which subtype responds best',
  ];
}

function defaultApprovalCheckpoints() {
  return [
    'Operator approves First Campaign Plan Preview',
    'Proof assets confirmed ready for the chosen segment',
    'High-priority infrastructure gaps reviewed',
    'No prospect list or outreach copy until preview sign-off',
    'No launch or account changes without explicit approval',
  ];
}

function defaultRisks(context, answers) {
  const risks = [];
  const readiness = context && context.readinessOverallStatus;
  if (readiness && readiness !== 'ready') {
    risks.push(
      `Infrastructure readiness is ${readiness} — capture/convert gaps may leak demand if outreach starts too early.`
    );
  }
  if (!(context && context.completedSetupChecklist)) {
    risks.push(
      'Setup checklist may still have open items — confirm Growth Plan tasks before any later build step.'
    );
  }
  const proof = answerText(answers, 'proof_assets');
  if (/missing|need|don't have|do not have|none|still/i.test(proof)) {
    risks.push(
      'Proof assets are incomplete — credibility gaps can weaken the first test.'
    );
  }
  risks.push(
    'This preview does not validate market demand; it only defines how the first test would be judged.'
  );
  risks.push(
    'Capacity or scheduling strain could appear if response is stronger than expected.'
  );
  return risks.slice(0, 6);
}

function resolveTargetSegment(context, answers) {
  const opening = answerText(answers, 'opening');
  const segmentAnswer = answerText(answers, 'target_segment');
  const primary = (context && context.primarySegment) || 'property managers';
  const subtype = (context && context.subtype) || null;

  const narrow =
    /\bnarrow\b|\bonly\b|\bfocus on\b|\bsmaller\b|\bspecific\b|\bsubtype\b/i.test(
      `${opening} ${segmentAnswer}`
    ) && !/\bas defined\b|\bexactly as\b|\bas-is\b|\bkeep (it |them )?as\b/i.test(
      `${opening} ${segmentAnswer}`
    );

  if (segmentAnswer) {
    return segmentAnswer.length > 160
      ? `${primary}${subtype ? ` — ${subtype}` : ''}`
      : segmentAnswer;
  }
  if (narrow && opening) {
    return `${primary} (narrowed: ${opening})`;
  }
  if (subtype) return `${primary} — ${subtype}`;
  return primary;
}

function resolveMarketBound(context, answers) {
  const marketAnswer = answerText(answers, 'market_bounds');
  if (marketAnswer) return marketAnswer;
  return (context && context.targetMarket) || 'Greater Manchester';
}

function resolveObjective(context, answers) {
  const obj = answerText(answers, 'campaign_objective');
  if (obj) return obj;
  const primary = (context && context.primarySegment) || 'property managers';
  const market = (context && context.targetMarket) || 'Greater Manchester';
  return `Validate that ${primary} in ${market} will take a discovery conversation and request a walkthrough or estimate — before any larger outreach build.`;
}

function resolveHypothesis(context, answers) {
  const h = answerText(answers, 'hypothesis');
  if (h) return h;
  const segment = resolveTargetSegment(context, answers);
  const market = resolveMarketBound(context, answers);
  return `If we approach ${segment} in ${market} with clear commercial-cleaning proof and a simple walkthrough offer, we should see qualified conversations and walkthrough or estimate requests within the first validation window.`;
}

function resolveProofAssets(context, answers) {
  const proof = answerText(answers, 'proof_assets');
  if (proof) {
    const listed = splitList(proof);
    return listed.length ? listed : [proof];
  }
  return defaultProofAssets(context);
}

function resolveValidationMetrics(answers) {
  const m = answerText(answers, 'validation_metrics');
  if (m) {
    const listed = splitList(m);
    return listed.length ? listed : [m];
  }
  return defaultValidationMetrics();
}

function resolveApprovalCheckpoints(answers) {
  const a = answerText(answers, 'approval_checkpoints');
  if (a) {
    const listed = splitList(a);
    return listed.length ? listed : [a];
  }
  return defaultApprovalCheckpoints();
}

function buildFirstCampaignPlanPreview(context, answers, opts = {}) {
  const ctx = context || {};
  const ans = answers || {};
  const objective = resolveObjective(ctx, ans);
  const targetSegment = resolveTargetSegment(ctx, ans);
  const marketBound = resolveMarketBound(ctx, ans);
  const hypothesis = resolveHypothesis(ctx, ans);
  const proofAssetsNeeded = resolveProofAssets(ctx, ans);
  const validationMetrics = resolveValidationMetrics(ans);
  const approvalCheckpoints = resolveApprovalCheckpoints(ans);
  const risksCautions = defaultRisks(ctx, ans);
  const name = shortName(ctx.businessName || 'the business');

  return {
    kind: ARTIFACT_KIND,
    title: PREVIEW_TITLE,
    businessName: name,
    campaignObjective: objective,
    targetSegment,
    marketBound,
    hypothesis,
    proofAssetsNeeded,
    validationMetrics,
    risksCautions,
    approvalCheckpoints,
    recommendedNextStep:
      'Operator reviews this First Campaign Plan Preview. Do not build a prospect list, write outreach copy, send messages, or change accounts until the preview is approved.',
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
      readinessOverallStatus: ctx.readinessOverallStatus || null,
    },
    generatedAt: new Date().toISOString(),
    blueprintId: opts.blueprintId || ctx.blueprintId || null,
    blueprintVersion: opts.blueprintVersion || ctx.blueprintVersion || null,
  };
}

function formatFirstCampaignPlanPreviewMessage(preview) {
  const p = preview || {};
  const titles = p.sectionTitles || SECTION_TITLES;
  const lines = [p.title || PREVIEW_TITLE, ''];

  lines.push(`1. ${titles.campaignObjective}`);
  lines.push(p.campaignObjective || '—');
  lines.push('');

  lines.push(`2. ${titles.targetSegment}`);
  lines.push(p.targetSegment || '—');
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
        ``,
        `This stays planning-only. Nothing is launched, and no prospect list or outreach copy is created from this preview.`,
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
};
