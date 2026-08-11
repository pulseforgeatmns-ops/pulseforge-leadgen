'use strict';

/**
 * Max Synthesis Layer — CampaignSynthesisContext + durable Campaign Memory.
 *
 * Before any Growth / Campaign artifact is rendered, Max builds a synthesis
 * context that merges:
 *   1. Approved workflow facts (Blueprint → Launch Gate chain)
 *   2. Durable operator learnings (tested subjects, relationship statuses,
 *      personalization rules, voice / differentiators)
 *
 * Renderers MUST consume this context — never raw prior paragraphs alone.
 * Guardrails unchanged: no sends, CRM writes, exports, or account changes.
 */

const { ARTIFACT_KINDS, nextReviewArtifactKind } = require('../clientIntelligenceReasoning');
const {
  buildArtifactSynthesisContext,
  shortBusinessName,
} = require('./ArtifactSynthesisContext');

/** Durable defaults — Anchor commercial-cleaning operator learnings. */
const DEFAULT_OPERATOR_LEARNINGS = Object.freeze({
  tested_subject_line_pattern: '{{business_name}} - commercial cleaning',
  tested_subject_line_performance:
    '56.6% open rate vs 23.9% for other formats',
  keyrenter_status: 'existing_relationship_nurture',
  cedar_status: 'source_verification_required',
  personalization_rule: 'do not use street addresses by default',
  personalization_preference:
    'use town, company, property type, portfolio cue, or public role signal',
  copy_voice: 'calm, professional, reliable, easy to work with',
  copy_differentiator:
    'reliability, responsiveness, accountability, fewer vendor-chasing headaches',
});

const STREET_ADDRESS_PERSONALIZATION_RE =
  /\b(?:\d{1,6}\s+[A-Za-z0-9.'-]+(?:\s+(?:St|Street|Ave|Avenue|Rd|Road|Dr|Drive|Ln|Lane|Blvd|Boulevard|Way|Ct|Court|Cir|Circle|Hwy|Highway|Pkwy|Parkway)\.?)|(?:(?:use|reference|mention|include|personalize\s+(?:with|using|from)|recommend)\s+(?:the\s+)?(?:street|mailing|physical)\s+address))\b/i;

const GENERIC_SUBJECT_LINE_RES = Object.freeze([
  /^Quick question about cleaning reliability/i,
  /— responsive commercial cleaning for property managers$/i,
  /^Worth a brief chat about recurring cleaning coverage\?/i,
]);

const FULL_TOWN_LIST_IN_BODY_RE =
  /across\s+Bedford,\s*Hooksett,\s*Londonderry,\s*Auburn(?:,\s*or\s*Goffstown)?/i;

const FIRST_PERSON_WORK_WITH_RE = /\bI work with\b/i;

function nowIso() {
  return new Date().toISOString();
}

function emptyCampaignMemory() {
  return {
    operatorLearnings: { ...DEFAULT_OPERATOR_LEARNINGS },
    learningHistory: [],
    approvedFactsSnapshot: null,
    updatedAt: null,
  };
}

/**
 * Ensure session-scoped campaign memory exists and carries default learnings.
 * Merges prior values without wiping operator overrides.
 */
function ensureCampaignMemory(state = {}) {
  const existing =
    (state && state.campaignMemory) ||
    (state &&
      state.campaignPlanning &&
      state.campaignPlanning.campaignMemory) ||
    null;
  if (existing && typeof existing === 'object') {
    return {
      ...emptyCampaignMemory(),
      ...existing,
      operatorLearnings: {
        ...DEFAULT_OPERATOR_LEARNINGS,
        ...(existing.operatorLearnings || {}),
      },
      learningHistory: [...(existing.learningHistory || [])],
    };
  }
  return emptyCampaignMemory();
}

/**
 * Persist or update a single operator learning key.
 */
function upsertOperatorLearning(memory, key, value, source = 'operator') {
  const next = ensureCampaignMemory({ campaignMemory: memory });
  const k = String(key || '').trim();
  if (!k) return next;
  const prev = next.operatorLearnings[k];
  next.operatorLearnings[k] = value;
  next.learningHistory.push({
    key: k,
    value,
    previous: prev != null ? prev : null,
    source,
    at: nowIso(),
  });
  if (next.learningHistory.length > 100) {
    next.learningHistory = next.learningHistory.slice(-100);
  }
  next.updatedAt = nowIso();
  return next;
}

/**
 * Merge a partial learnings map into durable campaign memory.
 */
function mergeOperatorLearnings(memory, learnings = {}, source = 'operator') {
  let next = ensureCampaignMemory({ campaignMemory: memory });
  const entries = Object.entries(learnings || {});
  for (const [key, value] of entries) {
    if (value == null || value === '') continue;
    if (next.operatorLearnings[key] === value) continue;
    next = upsertOperatorLearning(next, key, value, source);
  }
  return next;
}

/**
 * Apply relationship / source-verification statuses from an approved Batch Review.
 */
function applyBatchReviewLearnings(memory, review) {
  let next = ensureCampaignMemory({ campaignMemory: memory });
  if (!review || typeof review !== 'object') return next;

  const existing = review.existingRelationship || [];
  const sourceVer = review.sourceVerificationRequired || [];
  const batch = review.approvedBatch || {};

  const hasKeyrenter = [...existing, ...(batch.excludedExistingRelationship || [])]
    .some((row) => /keyrenter/i.test(typeof row === 'string' ? row : row.companyName || row.company || ''));
  const hasCedar = [...sourceVer, ...(batch.excludedSourceVerification || [])]
    .some((row) => /cedar/i.test(typeof row === 'string' ? row : row.companyName || row.company || ''));

  if (hasKeyrenter) {
    next = upsertOperatorLearning(
      next,
      'keyrenter_status',
      'existing_relationship_nurture',
      'batch_review'
    );
  }
  if (hasCedar) {
    next = upsertOperatorLearning(
      next,
      'cedar_status',
      'source_verification_required',
      'batch_review'
    );
  }
  return next;
}

function candidateName(row) {
  if (!row) return '';
  if (typeof row === 'string') return row;
  return String(row.companyName || row.company || '').trim();
}

function isKeyrenterName(name) {
  return /\bkeyrenter\b/i.test(String(name || ''));
}

function isCedarName(name) {
  return /\bcedar\b/i.test(String(name || ''));
}

/**
 * Resolve workflow step + next allowed artifact from session / memory state.
 */
function resolveWorkflowPosition(opts = {}) {
  const step =
    opts.currentStep ||
    opts.step ||
    (opts.slots && opts.slots.planningState) ||
    null;
  const memory = opts.reasoningMemory || null;
  const nextFromMemory =
    (memory && memory.nextRecommendedArtifact) || null;

  let nextAllowedArtifact = nextFromMemory || opts.nextAllowedArtifact || null;

  if (!nextAllowedArtifact && step) {
    const stepToKind = {
      prospect_batch_review: ARTIFACT_KINDS.PROSPECT_BATCH_REVIEW,
      prospect_batch_1_approved: ARTIFACT_KINDS.OUTREACH_STRATEGY_PREVIEW,
      outreach_strategy_preview: ARTIFACT_KINDS.OUTREACH_STRATEGY_PREVIEW,
      outreach_strategy_preview_approved: ARTIFACT_KINDS.OUTREACH_COPY_PLAN,
      outreach_copy_plan: ARTIFACT_KINDS.OUTREACH_COPY_PLAN,
      outreach_copy_plan_approved: ARTIFACT_KINDS.OUTREACH_DRAFT_PREVIEW,
      outreach_draft_preview: ARTIFACT_KINDS.OUTREACH_DRAFT_PREVIEW,
      outreach_draft_preview_approved: ARTIFACT_KINDS.OUTREACH_LAUNCH_GATE,
      outreach_launch_gate: ARTIFACT_KINDS.OUTREACH_LAUNCH_GATE,
    };
    const mapped = stepToKind[step];
    if (mapped) {
      // If current step is an approved_* step, mapped is already the next artifact.
      if (/_approved$/.test(step) || step === 'prospect_batch_1_approved') {
        nextAllowedArtifact = mapped;
      } else if (
        opts.approvedArtifacts &&
        opts.approvedArtifacts.includes(mapped)
      ) {
        nextAllowedArtifact = nextReviewArtifactKind(mapped);
      } else {
        nextAllowedArtifact = mapped;
      }
    }
  }

  return {
    currentStep: step,
    nextAllowedArtifact,
  };
}

/**
 * Collect approved workflow facts from session blobs / opts.
 */
function collectApprovedWorkflowFacts(opts = {}) {
  const blueprint =
    opts.approvedBlueprint ||
    opts.blueprint ||
    (opts.context && opts.context.approvedBlueprint) ||
    null;
  const review = opts.approvedReview || opts.prospectBatchReview || null;
  const batch = (review && review.approvedBatch) || opts.approvedBatch || null;
  const strategy =
    opts.approvedOutreachStrategy ||
    opts.outreachStrategyPreview ||
    null;
  const copyPlan =
    opts.approvedOutreachCopyPlan ||
    opts.outreachCopyPlan ||
    null;
  const criteria =
    opts.priorCriteriaPreview ||
    opts.prospectListCriteriaPreview ||
    null;
  const campaignObjective =
    (criteria && criteria.campaignObjective) ||
    (strategy && strategy.campaignObjective) ||
    (copyPlan && copyPlan.campaignObjective) ||
    opts.campaignObjective ||
    null;

  const batch1Candidates = batch
    ? (batch.candidates || []).map((c) => ({
        companyName: candidateName(c),
        town: c.town || c.city || null,
        role: c.jobTitle || c.role || c.contactTitle || null,
        ...c,
      }))
    : [];

  const excludedCandidates = {
    sourceVerificationRequired: [
      ...((batch && batch.excludedSourceVerification) || []),
      ...((review && review.sourceVerificationRequired) || []).map(candidateName),
    ].filter(Boolean),
    existingRelationshipNurture: [
      ...((batch && batch.excludedExistingRelationship) || []),
      ...((review && review.existingRelationship) || []).map(candidateName),
    ].filter(Boolean),
    optionalExpansion: [
      ...((batch && batch.excludedOptionalExpansion) || []),
      ...((review && review.optionalExpansion) || []).map(candidateName),
    ].filter(Boolean),
    rejected: [
      ...((batch && batch.excludedRejected) || []),
      ...((review && review.rejected) || []).map(candidateName),
    ].filter(Boolean),
  };

  return {
    approvedBlueprint: blueprint,
    approvedCampaignObjective: campaignObjective,
    approvedBatch1Candidates: batch1Candidates,
    excludedCandidates,
    approvedOutreachStrategyPreview:
      strategy && strategy.status === 'approved' ? strategy : strategy || null,
    approvedOutreachCopyPlan:
      copyPlan && copyPlan.status === 'approved' ? copyPlan : copyPlan || null,
    prospectBatchReview: review,
  };
}

/**
 * Expand tested subject pattern with business name token.
 */
function expandSubjectPattern(pattern, businessName) {
  const name = shortBusinessName(businessName || 'the business');
  return String(pattern || '')
    .replace(/\{\{\s*business_name\s*\}\}/gi, name)
    .replace(/\{\{\s*name\s*\}\}/gi, name)
    .trim();
}

/**
 * Build subject line list from campaign learnings.
 * When a tested winner exists, return ONLY that pattern — never generic options.
 */
function resolveSubjectLines(campaignCtx) {
  const learnings = (campaignCtx && campaignCtx.learnings) || {};
  const pattern = learnings.tested_subject_line_pattern;
  const name =
    (campaignCtx &&
      campaignCtx.phrase &&
      campaignCtx.phrase('businessNamePhrase')) ||
    (campaignCtx && campaignCtx.facts && campaignCtx.facts.businessName) ||
    (campaignCtx && campaignCtx.businessName) ||
    'the business';

  if (pattern) {
    return {
      subjectOptions: [expandSubjectPattern(pattern, name)],
      usedTestedWinner: true,
      testedPattern: pattern,
      performance: learnings.tested_subject_line_performance || null,
      sectionTitle: 'Subject line (tested winner)',
    };
  }

  const market =
    (campaignCtx && campaignCtx.phrase && campaignCtx.phrase('outreachMarketPhrase')) ||
    (campaignCtx && campaignCtx.phrases && campaignCtx.phrases.outreachMarketPhrase) ||
    'Greater Manchester';
  const shortName = shortBusinessName(name);
  return {
    subjectOptions: [
      `Quick question about cleaning reliability in ${market}`,
      `${shortName} — responsive commercial cleaning for property managers`,
      'Worth a brief chat about recurring cleaning coverage?',
    ],
    usedTestedWinner: false,
    testedPattern: null,
    performance: null,
    sectionTitle: 'Subject line options',
  };
}

/**
 * First-touch opener voice: company-led ("Anchor helps...") unless sender
 * identity explicitly supports first-person.
 */
function resolveSenderVoiceLine(campaignCtx, audiencePhrase) {
  const audience = audiencePhrase || 'property managers';
  const name =
    shortBusinessName(
      (campaignCtx && campaignCtx.businessName) ||
        (campaignCtx && campaignCtx.facts && campaignCtx.facts.businessName) ||
        'the business'
    );
  const senderIdentity =
    (campaignCtx && campaignCtx.senderIdentity) ||
    (campaignCtx &&
      campaignCtx.approved &&
      campaignCtx.approved.senderIdentity) ||
    null;
  const supportsFirstPerson = Boolean(
    senderIdentity &&
      (senderIdentity.useFirstPerson === true ||
        senderIdentity.voice === 'first_person')
  );

  if (supportsFirstPerson) {
    return {
      opener: `I work with ${audience} across {{town}} who want reliable commercial cleaning without chasing vendors.`,
      usesFirstPerson: true,
    };
  }

  return {
    opener: `${name} helps ${audience} across {{town}} who want reliable commercial cleaning without chasing vendors.`,
    usesFirstPerson: false,
  };
}

function rejectsStreetAddressPersonalization(text) {
  return STREET_ADDRESS_PERSONALIZATION_RE.test(String(text || ''));
}

/**
 * Build a personalization note that obeys durable operator rules.
 */
function buildPersonalizationNote(campaignCtx, prospect = {}) {
  const learnings = (campaignCtx && campaignCtx.learnings) || {};
  const notes = [];
  const town =
    prospect.town ||
    prospect.city ||
    (prospect.location && String(prospect.location).split(',')[0]) ||
    null;
  const role = prospect.jobTitle || prospect.role || prospect.contactTitle || null;
  const company = candidateName(prospect) || 'Prospect';

  // Never recommend street addresses by default.
  if (
    learnings.personalization_rule &&
    /street address/i.test(learnings.personalization_rule)
  ) {
    // preference path only
  }

  if (town && !rejectsStreetAddressPersonalization(town)) {
    notes.push(`Reference {{town}} (${town}) when it matches their public location.`);
  } else {
    notes.push('Reference {{town}} when a public location town is clear.');
  }

  notes.push(`Company cue: ${company}.`);
  if (role) {
    notes.push(`Address the ${role} when a public decision-maker is clear.`);
  }
  notes.push(
    'Use property type or portfolio cue only when publicly evident — never a street address by default.'
  );
  notes.push(
    'Lean on reliability, responsiveness, accountability, or fewer vendor-chasing headaches when those signals are present.'
  );

  const note = notes.join(' ');
  if (rejectsStreetAddressPersonalization(note)) {
    return note.replace(STREET_ADDRESS_PERSONALIZATION_RE, '[town]').replace(
      /\s+/g,
      ' '
    );
  }
  return note;
}

/**
 * Filter cold Batch 1 candidates against durable relationship learnings.
 * Keyrenter never appears in cold Batch 1; Cedar stays source-verification.
 */
function filterColdBatchCandidates(campaignCtx, candidates = []) {
  const learnings = (campaignCtx && campaignCtx.learnings) || {};
  const keyrenterStatus = learnings.keyrenter_status;
  const cedarStatus = learnings.cedar_status;
  const excluded = {
    keyrenter: [],
    cedar: [],
  };

  const cold = [];
  for (const c of candidates || []) {
    const name = candidateName(c);
    if (
      keyrenterStatus === 'existing_relationship_nurture' &&
      isKeyrenterName(name)
    ) {
      excluded.keyrenter.push(name);
      continue;
    }
    if (
      cedarStatus === 'source_verification_required' &&
      isCedarName(name)
    ) {
      excluded.cedar.push(name);
      continue;
    }
    cold.push(c);
  }
  return { coldCandidates: cold, excludedByLearnings: excluded };
}

/**
 * Detect whether a stored Outreach Draft Preview conflicts with campaign memory.
 */
function findCampaignMemoryDraftConflicts(preview, campaignCtx) {
  if (!preview || typeof preview !== 'object') return [];
  const hits = [];
  const learnings = (campaignCtx && campaignCtx.learnings) || {};
  const body = String(
    preview.firstTouchBody ||
      (preview.firstTouchDraft && preview.firstTouchDraft.body) ||
      ''
  );
  const subjects = Array.isArray(preview.subjectOptions)
    ? preview.subjectOptions
    : [];
  const personalization = Array.isArray(preview.personalizationByProspect)
    ? preview.personalizationByProspect
    : [];
  const batchNames = Array.isArray(preview.batchProspects)
    ? preview.batchProspects
    : [];

  if (learnings.tested_subject_line_pattern) {
    const expected = expandSubjectPattern(
      learnings.tested_subject_line_pattern,
      preview.businessName ||
        (campaignCtx && campaignCtx.businessName) ||
        'the business'
    );
    const hasWinner = subjects.some(
      (s) => String(s).trim().toLowerCase() === expected.toLowerCase()
    );
    if (!hasWinner) hits.push('missing_tested_subject_line');
    const hasGeneric = subjects.some((s) =>
      GENERIC_SUBJECT_LINE_RES.some((re) => re.test(String(s)))
    );
    if (hasGeneric || subjects.length > 1) {
      hits.push('generic_subject_options_with_tested_winner');
    }
  }

  if (FULL_TOWN_LIST_IN_BODY_RE.test(body) || !/\{\{\s*town\s*\}\}/i.test(body)) {
    if (FULL_TOWN_LIST_IN_BODY_RE.test(body) || /across\s+[A-Z][a-z]+,\s*[A-Z]/.test(body)) {
      hits.push('full_town_list_in_body');
    } else if (!/\{\{\s*town\s*\}\}/i.test(body)) {
      hits.push('missing_town_token');
    }
  }

  const senderIdentity =
    campaignCtx && campaignCtx.senderIdentity
      ? campaignCtx.senderIdentity
      : null;
  const allowFirstPerson = Boolean(
    senderIdentity &&
      (senderIdentity.useFirstPerson === true ||
        senderIdentity.voice === 'first_person')
  );
  if (!allowFirstPerson && FIRST_PERSON_WORK_WITH_RE.test(body)) {
    hits.push('first_person_work_with');
  }

  for (const row of personalization) {
    const note = row && row.personalizationNote;
    if (rejectsStreetAddressPersonalization(note)) {
      hits.push('street_address_personalization');
      break;
    }
  }

  if (learnings.keyrenter_status === 'existing_relationship_nurture') {
    if (batchNames.some(isKeyrenterName) || personalization.some((r) => isKeyrenterName(r.companyName))) {
      hits.push('keyrenter_in_cold_batch');
    }
  }
  if (learnings.cedar_status === 'source_verification_required') {
    if (batchNames.some(isCedarName) || personalization.some((r) => isCedarName(r.companyName))) {
      hits.push('cedar_in_cold_batch');
    }
  }

  return hits;
}

function outreachDraftPreviewConflictsWithCampaignMemory(preview, campaignCtx) {
  return findCampaignMemoryDraftConflicts(preview, campaignCtx).length > 0;
}

/**
 * Build the shared CampaignSynthesisContext consumed by all Growth/Campaign
 * artifact renderers.
 *
 * @param {object} opts
 * @returns {object} frozen CampaignSynthesisContext
 */
function buildCampaignSynthesisContext(opts = {}) {
  const artifactCtx = buildArtifactSynthesisContext(opts);
  const campaignMemory = ensureCampaignMemory(opts.state || opts);
  let memory = campaignMemory;

  // Seed / refresh from Batch Review when available.
  const review =
    opts.approvedReview ||
    opts.prospectBatchReview ||
    (opts.state && opts.state.prospectBatchReview) ||
    null;
  if (review) {
    memory = applyBatchReviewLearnings(memory, review);
  }

  // Allow explicit overrides for this render.
  if (opts.operatorLearnings && typeof opts.operatorLearnings === 'object') {
    memory = mergeOperatorLearnings(
      memory,
      opts.operatorLearnings,
      opts.learningSource || 'explicit'
    );
  }

  const approved = collectApprovedWorkflowFacts({
    ...opts,
    approvedReview: review,
    outreachStrategyPreview:
      opts.approvedOutreachStrategy ||
      opts.outreachStrategyPreview ||
      (opts.state && opts.state.outreachStrategyPreview),
    outreachCopyPlan:
      opts.approvedOutreachCopyPlan ||
      opts.outreachCopyPlan ||
      (opts.state && opts.state.outreachCopyPlan),
  });

  const workflow = resolveWorkflowPosition({
    currentStep: opts.currentStep || opts.step,
    step: opts.step,
    slots: opts.slots,
    reasoningMemory: opts.reasoningMemory || (opts.state && opts.state.reasoningMemory),
    approvedArtifacts:
      (opts.reasoningMemory && opts.reasoningMemory.approvedArtifacts) ||
      (opts.state &&
        opts.state.reasoningMemory &&
        opts.state.reasoningMemory.approvedArtifacts) ||
      [],
    nextAllowedArtifact: opts.nextAllowedArtifact,
  });

  const businessName =
    artifactCtx.phrase('businessNamePhrase') ||
    (artifactCtx.facts && artifactCtx.facts.businessName) ||
    (opts.context && opts.context.businessName) ||
    'the business';

  const learnings = Object.freeze({ ...memory.operatorLearnings });

  const ctx = {
    // ArtifactSynthesisContext surface
    facts: artifactCtx.facts,
    phrases: artifactCtx.phrases,
    evidence: artifactCtx.evidence,
    phrase: artifactCtx.phrase,
    embed: artifactCtx.embed,
    containsRawPromptFragment: artifactCtx.containsRawPromptFragment,
    findRawPromptFragments: artifactCtx.findRawPromptFragments,
    rawDisplayAllowed: false,

    // Campaign-specific
    kind: 'campaign_synthesis_context',
    businessName: shortBusinessName(businessName),
    approved: Object.freeze({ ...approved }),
    learnings,
    workflow: Object.freeze({ ...workflow }),
    campaignMemory: memory,
    senderIdentity: opts.senderIdentity || null,

    // Renderer helpers
    resolveSubjectLines() {
      return resolveSubjectLines(ctx);
    },
    resolveSenderVoiceLine(audiencePhrase) {
      return resolveSenderVoiceLine(ctx, audiencePhrase);
    },
    buildPersonalizationNote(prospect) {
      return buildPersonalizationNote(ctx, prospect);
    },
    filterColdBatchCandidates(candidates) {
      return filterColdBatchCandidates(ctx, candidates);
    },
    rejectsStreetAddressPersonalization,
    expandSubjectPattern(pattern) {
      return expandSubjectPattern(pattern, ctx.businessName);
    },
  };

  return Object.freeze(ctx);
}

module.exports = {
  DEFAULT_OPERATOR_LEARNINGS,
  STREET_ADDRESS_PERSONALIZATION_RE,
  GENERIC_SUBJECT_LINE_RES,
  emptyCampaignMemory,
  ensureCampaignMemory,
  upsertOperatorLearning,
  mergeOperatorLearnings,
  applyBatchReviewLearnings,
  buildCampaignSynthesisContext,
  resolveSubjectLines,
  resolveSenderVoiceLine,
  buildPersonalizationNote,
  filterColdBatchCandidates,
  rejectsStreetAddressPersonalization,
  expandSubjectPattern,
  findCampaignMemoryDraftConflicts,
  outreachDraftPreviewConflictsWithCampaignMemory,
  collectApprovedWorkflowFacts,
  resolveWorkflowPosition,
  isKeyrenterName,
  isCedarName,
  candidateName,
};
