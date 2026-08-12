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
  looksLikeProspectBatchReviewRequest,
  looksLikeProspectBatchReviewCorrection,
  looksLikeProspectBatchReviewApproval,
  looksLikeOutreachStrategyPreviewRequest,
  looksLikeOutreachStrategyPreviewApproval,
  looksLikeOutreachCopyPlanRequest,
  looksLikeOutreachCopyPlanApproval,
  looksLikeOutreachDraftPreviewRequest,
  looksLikeOutreachDraftPreviewApproval,
  looksLikeOutreachLaunchGateRequest,
  looksLikeOutreachLaunchGateApproval,
  hasActiveProspectBatchReview,
  hasActiveOutreachStrategyPreview,
  hasOutreachStrategyPreview,
  hasOutreachCopyPlan,
  hasOutreachDraftPreview,
  hasOutreachLaunchGate,
  canEmitOutreachStrategyPreview,
  canEmitOutreachCopyPlan,
  canEmitOutreachDraftPreview,
  canEmitOutreachLaunchGate,
  isProspectBatchReviewAlreadyApproved,
  isOutreachStrategyPreviewAlreadyApproved,
  isOutreachCopyPlanAlreadyApproved,
  isOutreachDraftPreviewAlreadyApproved,
  isOutreachLaunchGateAlreadyApproved,
  hasCompletedScoutCandidateBatch,
  looksLikeApproval,
  looksLikeApprovalLead,
  looksLikeScoutHandoffBriefRequest,
  looksLikeHandBriefToScoutRequest,
  looksLikeExecuteExistingScoutWorkRequest,
  extractWorkRequestIdFromMessage,
  looksLikeLiveSourcingApproval,
  classifyProspectAcquisitionIntent,
  PROSPECT_ACQUISITION_INTENTS,
  looksLikeReviseCriteriaRequest,
  shouldForceProspectListDraft,
  inferApprovedArtifactsFromMessage,
} = require('./clientIntelligenceReasoning');
const {
  applyMapsOnlyDowngradeToBatch,
  mergePreservedNhCandidatesFromPriorBatch,
  isNhMarketCandidateRow,
  PRIORITY_TOWNS_NH,
  NEARBY_FILL_TOWNS_NH,
  EXTENDED_REVIEW_TOWNS_NH,
  CANDIDATE_STATUS,
  REJECTION_REASON,
} = require('./scoutQualityGate');
const {
  buildOperatorReviewDigest,
  formatOperatorReviewArtifactMessage,
  formatOperatorReviewEvidenceMessage,
  VIEW_EVIDENCE_LABEL,
  PROSPECT_BATCH_REVIEW_DIGEST_SECTION_ORDER,
} = require('./operatorReviewDigest');
const {
  buildArtifactSynthesisContext,
  buildCampaignSynthesisContext,
  ensureCampaignMemory,
  applyBatchReviewLearnings,
  mergeOperatorLearnings,
  findCampaignMemoryDraftConflicts,
  outreachDraftPreviewConflictsWithCampaignMemory,
  rejectsStreetAddressPersonalization,
  shortBusinessName,
  containsRawPromptFragment,
  findRawPromptFragments,
  asEmbeddablePhrase,
  normalizeObjectivePhrase,
  naturalList: synthesisNaturalList,
  DEFAULT_TOWNS: SYNTHESIS_DEFAULT_TOWNS,
  DEFAULT_OPERATOR_LEARNINGS,
  RESPONSE_MODES,
  PRIORITY_ORDER,
  emptyCampaignWorkingState,
  ensureCampaignWorkingState,
  looksLikeOperatorWorkflowRevision,
  looksLikeForceRebuildConfirmation,
  parseOperatorChatDirectives,
  applyOperatorDirectivesToWorkingState,
  markDirectivesApplied,
  recordRejectedOutput,
  countRejectedFingerprint,
  selectResponseMode,
  draftOutputFingerprint,
  validateOutreachDraftAgainstInstructions,
  buildStaleSourceDiagnostic,
  identifyStaleInjectionSources,
  markAwaitingForceRebuild,
  markForceRebuildBypass,
  clearForceRebuildBypass,
  buildFollowUpEmailDrafts,
  formatOperatorChatDraftResponse,
  CONVERSATION_MODES,
  applyConversationalPolicy,
  formatApprovedLaunchGateConversational,
  looksLikeExecutionRequest,
  composeExecutionConfirmation,
} = require('./maxSynthesis');
const {
  SCOUT_HANDOFF_KIND,
  SCOUT_HANDOFF_STATUSES,
  SCOUT_HANDOFF_UI_STATUS,
  SCOUT_SOURCING_NOT_WIRED_MESSAGE,
  COMPLETED_RESULT_GUARDRAILS,
  buildScoutHandoff,
  handBriefToScout,
  handBriefToScoutAsync,
  queueOrExecuteExistingScoutWorkRequest,
  executeScoutWorkRequest,
  isScoutSourcingExecutionWired,
  uiStatusForHandoff,
} = require('./scoutHandoff');

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

const PROSPECT_BATCH_REVIEW_KIND = 'prospect_batch_review';
const PROSPECT_BATCH_REVIEW_TITLE = 'Prospect Batch Review';
const PROSPECT_BATCH_REVIEW_DISCLAIMER =
  'Prospect Batch Review only — from the latest completed Scout result. No outreach copy, sends, CRM writes, exports, or account changes.';
/** @deprecated Prefer buildProspectBatchReviewClosingQuestion(review) — never claim "8 primary-town candidates". */
const PROSPECT_BATCH_REVIEW_CLOSING_QUESTION =
  'Do you want to approve the accepted cold first-pass candidates, include source-verification accounts after verification, and keep existing-relationship accounts in nurture?';
const PROSPECT_BATCH_REVIEW_SECTION_TITLES = Object.freeze({
  acceptedFirstPass: 'Accepted cold first-pass candidates',
  sourceVerificationRequired:
    'Source-verification required primary-town candidates',
  optionalExpansion: 'Optional expansion candidates',
  existingRelationship: 'Existing relationship / nurture',
  rejected: 'Rejected candidates',
});

/** Operator relationship classification — never treat as cold outreach. */
const RELATIONSHIP_STATUS = Object.freeze({
  EXISTING_RELATIONSHIP: 'existing_relationship',
});

/**
 * Known / parseable existing-relationship patterns (Anchor NH property managers).
 * Applied only when the operator provides a relationship override / correction,
 * or when an override is already stored on the active Prospect Batch Review.
 */
const KNOWN_RELATIONSHIP_OVERRIDE_PATTERNS = Object.freeze([
  {
    matchRe: /\bkeyrenter\b/i,
    companyName: 'Keyrenter New England Property Management',
    // Stable identity — never match generic "Property Management" rows.
    domains: Object.freeze([
      'keyrenternewengland.com',
      'keyrenter.com',
    ]),
    domainRe: /(?:^|\.)keyrenter(?:newengland)?\.com\b/i,
    relationship: RELATIONSHIP_STATUS.EXISTING_RELATIONSHIP,
    reason:
      'Operator relationship override — existing_relationship / nurture, not a cold prospect. Do not include in campaign outreach.',
  },
]);

/** Tokens that never establish company identity by themselves. */
const GENERIC_COMPANY_IDENTITY_TOKENS = Object.freeze([
  'llc',
  'inc',
  'corp',
  'ltd',
  'co',
  'company',
  'group',
  'partners',
  'partner',
  'associates',
  'associate',
  'property',
  'properties',
  'management',
  'managers',
  'manager',
  'real',
  'estate',
  'residential',
  'commercial',
  'pm',
  'the',
  'and',
  'of',
  'nh',
]);

const GENERIC_COMPANY_PHRASE_RE =
  /\b(?:property\s+management|property\s+managers?|real\s+estate(?:\s+management)?)\b/gi;

const LIVE_SOURCING_BOUNDARY_MESSAGE =
  'I cannot perform live sourcing in this environment yet.';

const LIVE_PROSPECT_LIST_KIND = 'live_sourced_prospect_list';
const LIVE_PROSPECT_LIST_TITLE = 'Live Public-Source Prospect List';

const SCOUT_HANDOFF_BRIEF_KIND = 'scout_handoff_brief';
const SCOUT_HANDOFF_BRIEF_TITLE = 'Scout Handoff Brief';
const SCOUT_HANDOFF_BRIEF_DISCLAIMER =
  'Scout Handoff Brief only. Max did not build a prospect list and did not hand this brief to Scout yet. No outreach copy, sends, CRM writes, or account changes have been created or launched.';

/** Explicit post-build-proposal planning states (SPEC-091 continuation). */
const CAMPAIGN_PLANNING_STATES = Object.freeze({
  PROSPECT_LIST_CRITERIA_APPROVED: 'prospect_list_criteria_approved',
  PROSPECT_LIST_BUILD_PROPOSAL_APPROVED: 'prospect_list_build_proposal_approved',
  PROSPECT_LIST_DRAFT_REQUESTED: 'prospect_list_draft_requested',
  PROSPECT_LIST_DRAFT_GENERATED: 'prospect_list_draft_generated',
  PROSPECT_LIST_DRAFT_REVIEWED: 'prospect_list_draft_reviewed',
  SCOUT_HANDOFF_BRIEF: 'scout_handoff_brief',
  SCOUT_HANDOFF_APPROVED: 'scout_handoff_approved',
  SCOUT_HANDOFF_QUEUED: 'scout_handoff_queued',
  SCOUT_HANDOFF_IN_PROGRESS: 'scout_handoff_in_progress',
  SCOUT_HANDOFF_COMPLETED: 'scout_handoff_completed',
  SCOUT_HANDOFF_NOT_WIRED: 'scout_handoff_not_wired',
  SCOUT_HANDOFF_FAILED: 'scout_handoff_failed',
  PROSPECT_BATCH_REVIEW: 'prospect_batch_review',
  PROSPECT_BATCH_1_APPROVED: 'prospect_batch_1_approved',
  OUTREACH_STRATEGY_PREVIEW: 'outreach_strategy_preview',
  OUTREACH_STRATEGY_PREVIEW_APPROVED: 'outreach_strategy_preview_approved',
  OUTREACH_COPY_PLAN: 'outreach_copy_plan',
  OUTREACH_COPY_PLAN_APPROVED: 'outreach_copy_plan_approved',
  OUTREACH_DRAFT_PREVIEW: 'outreach_draft_preview',
  OUTREACH_DRAFT_PREVIEW_APPROVED: 'outreach_draft_preview_approved',
  OUTREACH_LAUNCH_GATE: 'outreach_launch_gate',
  LIVE_SOURCING_APPROVED: 'live_sourcing_approved',
  LIVE_SOURCING_UNAVAILABLE: 'live_sourcing_unavailable',
  LIVE_SOURCING_GENERATED: 'live_sourcing_generated',
});

const BATCH_1_APPROVED_MESSAGE =
  'Batch 1 approved. Here is the Outreach Strategy Preview for review.';
const BATCH_1_APPROVED_DISCLAIMER =
  'Review-first only — no outreach copy, sends, CRM writes, exports, or account changes.';
const OUTREACH_STRATEGY_PREVIEW_KIND = 'outreach_strategy_preview';
const OUTREACH_STRATEGY_PREVIEW_TITLE = 'Outreach Strategy Preview';
const OUTREACH_STRATEGY_PREVIEW_DISCLAIMER =
  'Outreach Strategy Preview only — planning angles and guardrails. No final outreach copy, sends, CRM writes, exports, or account changes.';
const OUTREACH_STRATEGY_PREVIEW_CLOSING_QUESTION =
  'Does this Outreach Strategy Preview look right to approve, or do you want to revise a specific section?';
const OUTREACH_STRATEGY_SECTION_TITLES = Object.freeze({
  campaignObjective: 'Campaign objective',
  batch1Scope: 'Batch 1 scope',
  positioning: 'Positioning & differentiators',
  voiceTone: 'Voice & tone',
  outreachApproach: 'Outreach approach',
  proofFraming: 'Proof & offer framing',
  guardrails: 'Guardrails',
  recommendedNextStep: 'Recommended next step',
});
const OUTREACH_COPY_PLAN_KIND = 'outreach_copy_plan';
const OUTREACH_COPY_PLAN_TITLE = 'Outreach Copy Plan';
const OUTREACH_COPY_PLAN_DISCLAIMER =
  'Outreach Copy Plan only — channel sequence, goals, and personalization inputs. No final outreach copy, sends, CRM writes, exports, or account changes.';
const OUTREACH_COPY_PLAN_CLOSING_QUESTION =
  'Does this Outreach Copy Plan look right to approve, or do you want to revise a specific section?';
const OUTREACH_COPY_PLAN_SECTION_TITLES = Object.freeze({
  channelSequence: 'Recommended channel sequence',
  firstTouchGoal: 'First-touch message goal',
  ctaToTest: 'CTA to test',
  personalizationInputs: 'Personalization inputs from Batch 1',
  proofPoints: 'Proof points to use',
  followUpTiming: 'Follow-up timing and purpose',
  approvalGate: 'Approval gate before drafting final copy',
});
const OUTREACH_STRATEGY_APPROVED_MESSAGE =
  'Outreach Strategy Preview approved. Here is the Outreach Copy Plan for review.';
const OUTREACH_COPY_PLAN_APPROVED_MESSAGE =
  'Outreach Copy Plan approved. Here is the Outreach Draft Preview for review.';
const OUTREACH_DRAFT_PREVIEW_APPROVED_MESSAGE =
  'Outreach Draft Preview approved. Here is the Outreach Launch Gate for explicit launch/export/CRM readiness.';
const OUTREACH_DRAFT_PREVIEW_KIND = 'outreach_draft_preview';
const OUTREACH_DRAFT_PREVIEW_TITLE = 'Outreach Draft Preview';
const OUTREACH_DRAFT_PREVIEW_DISCLAIMER =
  'Outreach Draft Preview only — draft copy for review. No sends, CRM writes, exports, or account changes. Launch requires a separate explicit gate.';
const OUTREACH_DRAFT_PREVIEW_CLOSING_QUESTION =
  'Does this Outreach Draft Preview look right to approve, or do you want to revise a specific section?';
const OUTREACH_DRAFT_PREVIEW_SECTION_TITLES = Object.freeze({
  subjectOptions: 'Subject line options',
  firstTouchBody: 'First-touch draft',
  personalizationByProspect: 'Personalization by Batch 1 prospect',
  followUpSketch: 'Follow-up sketch (held until launch gate)',
  approvalGate: 'Approval gate before launch',
});
const OUTREACH_LAUNCH_GATE_KIND = 'outreach_launch_gate';
const OUTREACH_LAUNCH_GATE_TITLE = 'Outreach Launch Gate';
const OUTREACH_LAUNCH_GATE_DISCLAIMER =
  'Outreach Launch Gate only — readiness checkpoint. No sends, CRM writes, exports, or account changes execute automatically.';
const OUTREACH_LAUNCH_GATE_CLOSING_QUESTION =
  'Does this Outreach Launch Gate look right to approve for readiness, or hold before any launch/export/CRM action?';
/** Canonical post-approval status — readiness only; execution remains locked. */
const OUTREACH_LAUNCH_GATE_APPROVED_STATUS = 'approved_readiness_only';
const OUTREACH_LAUNCH_GATE_APPROVED_HEADLINE =
  'Outreach Launch Gate is approved for readiness only.';
const OUTREACH_LAUNCH_GATE_APPROVED_ASK =
  'Which next path do you want to prepare, if any?';
const OUTREACH_LAUNCH_GATE_NEXT_OPTIONS = Object.freeze([
  'prepare a manual-send export for review',
  'create CRM drafts, if explicitly approved',
  'queue sends later, if execution is intentionally enabled',
  'hold with no action',
]);
const OUTREACH_LAUNCH_GATE_OPERATOR_GUIDANCE =
  "I'd keep this held until sender identity and reply handling are confirmed.";
/** Stored OSP artifacts with these fragments must be regenerated, not shown. */
const STALE_OUTREACH_STRATEGY_FRAGMENT_RES = Object.freeze([
  /for Small to mid-sized/,
  /in Start with/,
  /Keep Greater Manchester in scope/i,
  /Carry forward proof already noted/i,
  /Competitive edge is described as/i,
  /This is operator-stated differentiation/i,
  /(?<!\.)\.\.(?!\.)/,
  /differentiators for /i,
  /who oversee offices, mixed-use/i,
  /keep the first test tight enough to learn quickly/i,
]);
/** Shared banned fragments for operator-facing repair across review artifacts. */
const OPERATOR_BANNED_FRAGMENT_RES = Object.freeze([
  /for Small to mid-sized/,
  /in Start with/,
  /Keep Greater Manchester in scope/i,
  /Carry forward proof already noted/i,
  /Competitive edge is described as/i,
  /This is operator-stated differentiation/i,
  /(?<!\.)\.\.(?!\.)/,
]);
/**
 * Stored Outreach Copy Plan artifacts with these fragments must be regenerated
 * before display. Operator-facing sections only — never reuse stale drafts.
 */
const STALE_OUTREACH_COPY_PLAN_FRAGMENT_RES = Object.freeze([
  /prefer\s+Bedford,\s*Hooksett,\s*Londonderry,\s*Auburn/i,
  /Carry forward proof already noted/i,
  /Hold final email\/SMS\/call scripts/i,
  /Competitive edge is described as/i,
  /This is operator-stated differentiation/i,
  /approved Batch 1 record/i,
  /Differentiator to lean on/i,
  /…/,
  /,\s*\.\.\./,
  /(?<!\.)\.\.(?!\.)/,
]);
/**
 * Stored Outreach Draft Preview fragments that are always stale (town list /
 * concrete street personalization / raw prompt stitching). Subject-line and
 * voice conflicts are evaluated against CampaignSynthesisContext separately.
 */
const STALE_OUTREACH_DRAFT_FRAGMENT_RES = Object.freeze([
  /across\s+Bedford,\s*Hooksett,\s*Londonderry,\s*Auburn/i,
  /\b(?:use|reference|mention|include)\s+(?:the\s+)?(?:street|mailing|physical)\s+address\b/i,
  /\b\d{1,6}\s+[A-Za-z0-9.'-]+\s+(?:St|Street|Ave|Avenue|Rd|Road|Dr|Drive)\b/i,
  /for Small to mid-sized/,
  /in Start with/,
  /Carry forward proof already noted/i,
  /(?<!\.)\.\.(?!\.)/,
]);
const DEFAULT_ANCHOR_VOICE =
  'Calm, professional, reliable, direct, and easy to work with — never pushy or hype-driven.';
const DEFAULT_ANCHOR_DIFFERENTIATORS = Object.freeze([
  'Reliability and accountability that property managers can count on',
  'Responsive communication and clear follow-through',
  'Peace of mind for recurring commercial cleaning relationships',
]);

const SCOUT_HANDOFF_SECTION_TITLES = Object.freeze({
  handoffStatus: 'Handoff status',
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
  'Creating this brief does not hand it to Scout — say “Hand this brief to Scout” to approve and queue',
  'Scout inspects public sources only when sourcing execution is wired',
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
  'Are located in Bedford NH, Hooksett NH, Londonderry NH, Auburn NH, Goffstown NH, or nearby Manchester NH (New Hampshire, USA)',
  'Likely need recurring cleaning weekly or multiple times per week',
  'Value reliability, responsiveness, and accountability',
  'Have a reachable owner, manager, facilities contact, or operations contact',
]);

const DEFAULT_EXCLUSION_CRITERIA = Object.freeze([
  'Large institutional property managers',
  'Cleaning companies, maid services, housekeeping, janitorial, carpet cleaning, and cleaning competitors',
  'Highly complex properties',
  'Lowest-price buyers',
  'Properties outside New Hampshire, USA / the approved service area',
  'UK Greater Manchester / Salford / Stockport or other non-US results',
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

  const brandVoice =
    sectionSummary(sections, 'brandVoice') ||
    sectionSummary(sections, 'brand_voice') ||
    null;
  const competitiveAdvantages =
    sectionSummary(sections, 'competitiveAdvantages') ||
    sectionSummary(sections, 'differentiation') ||
    null;

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
    brandVoice: brandVoice || null,
    competitiveAdvantages: competitiveAdvantages || null,
    differentiators: competitiveAdvantages || null,
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
 * Creates a draft Scout handoff object. Does not queue Scout or perform sourcing.
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
    'Operator reviews Scout’s returned prospect batch against this brief before outreach, enrichment expansion, CRM writes, or launch. Creating this brief does not hand it to Scout.';

  const targetSegmentSubtype = [targetSegment, targetSubtype]
    .filter(Boolean)
    .join(' — ');

  const priorHandoff =
    opts.priorScoutHandoff ||
    (opts.priorScoutHandoffBrief && opts.priorScoutHandoffBrief.scoutHandoff) ||
    null;

  const scoutHandoff = buildScoutHandoff(
    {
      ...(priorHandoff || {}),
      campaignObjective,
      targetSegment,
      targetSubtype,
      marketBounds,
      inclusionCriteria,
      exclusionCriteria,
      requiredFields: requiredProspectFields,
      requiredProspectFields,
      sourceTypes: [...DEFAULT_SCOUT_SOURCE_TYPES],
      evidenceRequirements: [...DEFAULT_SCOUT_EVIDENCE],
      evidenceRequired: [...DEFAULT_SCOUT_EVIDENCE],
      confidenceRules: [...DEFAULT_SCOUT_CONFIDENCE_RULES],
      guardrails: [...DEFAULT_SCOUT_HANDOFF_GUARDRAILS],
      status: SCOUT_HANDOFF_STATUSES.DRAFT,
      scoutRan: false,
      sourcingUnavailable: false,
      executionWired: null,
      workRequestId: null,
      workRequest: null,
      candidateBatch: null,
      resultsApproved: false,
    },
    {
      handoffId: priorHandoff && priorHandoff.handoffId,
      createdAt: priorHandoff && priorHandoff.createdAt,
    }
  );

  return {
    kind: SCOUT_HANDOFF_BRIEF_KIND,
    title: SCOUT_HANDOFF_BRIEF_TITLE,
    businessName: name,
    handoffId: scoutHandoff.handoffId,
    scoutHandoff,
    campaignObjective,
    targetSegment,
    targetSubtype,
    targetSegmentSubtype,
    marketBounds,
    marketBound: marketBounds,
    inclusionCriteria,
    exclusionCriteria,
    requiredProspectFields,
    requiredFields: requiredProspectFields,
    sourceTypes: [...DEFAULT_SCOUT_SOURCE_TYPES],
    evidenceRequired: [...DEFAULT_SCOUT_EVIDENCE],
    evidenceRequirements: [...DEFAULT_SCOUT_EVIDENCE],
    confidenceRules: [...DEFAULT_SCOUT_CONFIDENCE_RULES],
    reviewGate,
    guardrails: [...DEFAULT_SCOUT_HANDOFF_GUARDRAILS],
    sectionTitles: { ...SCOUT_HANDOFF_SECTION_TITLES },
    planningOnly: true,
    prospectListGenerated: false,
    liveSourcingPerformed: false,
    scoutRan: false,
    handedToScout: false,
    outreachCopyGenerated: false,
    accountChangesMade: false,
    crmWritesMade: false,
    campaignsGenerated: false,
    status: SCOUT_HANDOFF_STATUSES.DRAFT,
    uiStatus: scoutHandoff.uiStatus || SCOUT_HANDOFF_UI_STATUS.BRIEF_CREATED,
    disclaimer: SCOUT_HANDOFF_BRIEF_DISCLAIMER,
    recommendedNextStep:
      'Say “Hand this brief to Scout” to approve and queue a Scout work request. Creating this brief alone does not start Scout.',
    generatedAt: new Date().toISOString(),
    createdAt: scoutHandoff.createdAt,
    updatedAt: scoutHandoff.updatedAt,
    blueprintId: opts.blueprintId || ctx.blueprintId || null,
    blueprintVersion: opts.blueprintVersion || ctx.blueprintVersion || null,
    basedOnCriteriaStatus: (criteria && criteria.status) || 'approved',
  };
}

function formatScoutHandoffBriefMessage(brief) {
  const p = brief || {};
  const titles = p.sectionTitles || SCOUT_HANDOFF_SECTION_TITLES;
  const lines = [p.title || SCOUT_HANDOFF_BRIEF_TITLE, ''];

  lines.push(`0. ${titles.handoffStatus || 'Handoff status'}`);
  lines.push(
    p.uiStatus ||
      (p.scoutHandoff && p.scoutHandoff.uiStatus) ||
      SCOUT_HANDOFF_UI_STATUS.BRIEF_CREATED
  );
  if (p.handoffId || (p.scoutHandoff && p.scoutHandoff.handoffId)) {
    lines.push(
      `handoffId: ${p.handoffId || p.scoutHandoff.handoffId}`
    );
  }
  lines.push('');

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
  for (const item of p.requiredProspectFields || p.requiredFields || []) {
    lines.push(`- ${item}`);
  }
  if (!((p.requiredProspectFields || p.requiredFields || []).length)) {
    lines.push('- —');
  }
  lines.push('');

  lines.push(`7. ${titles.sourceTypes}`);
  for (const item of p.sourceTypes || []) lines.push(`- ${item}`);
  if (!(p.sourceTypes || []).length) lines.push('- —');
  lines.push('');

  lines.push(`8. ${titles.evidenceRequired}`);
  for (const item of p.evidenceRequired || p.evidenceRequirements || []) {
    lines.push(`- ${item}`);
  }
  if (!((p.evidenceRequired || p.evidenceRequirements || []).length)) {
    lines.push('- —');
  }
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
    priorScoutHandoffBrief: opts.priorScoutHandoffBrief || null,
    priorScoutHandoff: opts.priorScoutHandoff || null,
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
    slots: {
      ...briefSlots,
      scoutHandoffBriefGenerated: true,
    },
    preview: opts.priorPreview || null,
    criteriaPreview: opts.priorCriteriaPreview || null,
    buildProposal: opts.priorBuildProposal || null,
    prospectListDraft: opts.priorProspectListDraft || null,
    scoutHandoffBrief: brief,
    scoutHandoff: brief.scoutHandoff,
    scoutWorkRequest: null,
    scoutCandidateBatch: null,
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

function planningStateForHandoffResult(result) {
  if (!result) return CAMPAIGN_PLANNING_STATES.SCOUT_HANDOFF_NOT_WIRED;
  if (result.sourcingUnavailable) {
    return CAMPAIGN_PLANNING_STATES.SCOUT_HANDOFF_NOT_WIRED;
  }
  if (result.intent === 'scout_handoff_completed') {
    return CAMPAIGN_PLANNING_STATES.SCOUT_HANDOFF_COMPLETED;
  }
  if (result.intent === 'scout_sourcing_failed') {
    return CAMPAIGN_PLANNING_STATES.SCOUT_HANDOFF_FAILED;
  }
  if (result.intent === 'scout_handoff_queued') {
    return CAMPAIGN_PLANNING_STATES.SCOUT_HANDOFF_QUEUED;
  }
  const status = result.handoff && result.handoff.status;
  if (status === SCOUT_HANDOFF_STATUSES.QUEUED) {
    return CAMPAIGN_PLANNING_STATES.SCOUT_HANDOFF_QUEUED;
  }
  if (status === SCOUT_HANDOFF_STATUSES.IN_PROGRESS) {
    return CAMPAIGN_PLANNING_STATES.SCOUT_HANDOFF_IN_PROGRESS;
  }
  if (status === SCOUT_HANDOFF_STATUSES.APPROVED) {
    return CAMPAIGN_PLANNING_STATES.SCOUT_HANDOFF_APPROVED;
  }
  return CAMPAIGN_PLANNING_STATES.SCOUT_HANDOFF_NOT_WIRED;
}

/**
 * Approve the Scout Handoff Brief and create/queue a Scout work request.
 * If Scout sourcing is not wired, returns the capability boundary — no placeholders.
 * When public-source tooling is available, sets shouldExecuteScoutSourcing so the
 * interview layer can await executeScoutWorkRequest / handBriefToScoutAsync.
 */
function produceHandBriefToScoutResult(ctx, answers, slots, opts, leadIn) {
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

  const priorBrief = opts.priorScoutHandoffBrief || null;
  const priorHandoff =
    opts.priorScoutHandoff ||
    (priorBrief && priorBrief.scoutHandoff) ||
    null;

  // Ensure a draft handoff/brief exists from approved criteria before queuing.
  const brief =
    priorBrief && priorBrief.kind === SCOUT_HANDOFF_BRIEF_KIND
      ? {
          ...priorBrief,
          scoutHandoff:
            priorHandoff ||
            priorBrief.scoutHandoff ||
            buildScoutHandoff(priorBrief),
        }
      : buildScoutHandoffBrief(ctx, briefSlots, {
          answers,
          priorPreview: opts.priorPreview || null,
          priorCriteriaPreview: opts.priorCriteriaPreview || null,
          priorBuildProposal: opts.priorBuildProposal || null,
          priorScoutHandoff: priorHandoff,
          blueprintId: opts.blueprintId,
          blueprintVersion: opts.blueprintVersion,
        });

  const handoffFields = brief.scoutHandoff || buildScoutHandoff(brief);
  const result = handBriefToScout(handoffFields, opts);

  const nextBrief = {
    ...brief,
    handoffId: result.handoff.handoffId,
    scoutHandoff: result.handoff,
    status: result.handoff.status,
    uiStatus: result.handoff.uiStatus,
    handedToScout: true,
    scoutRan: Boolean(result.scoutRan),
    liveSourcingPerformed: false,
    prospectListGenerated: false,
    workRequestId: result.workRequest && result.workRequest.workRequestId,
    updatedAt: result.handoff.updatedAt,
    recommendedNextStep: result.sourcingUnavailable
      ? 'Scout sourcing execution is the next build gap — not a Max failure. Wire Scout public-source sourcing to this handoff.'
      : result.scoutRan
        ? 'Review Scout candidates. Approve before Composer / CRM / export use. No outreach or CRM writes yet.'
        : result.shouldExecuteScoutSourcing
          ? 'Scout work request queued — public-source sourcing will run next.'
          : 'Scout work request queued.',
    disclaimer: result.sourcingUnavailable
      ? SCOUT_SOURCING_NOT_WIRED_MESSAGE
      : 'Scout handoff results are review-only. No outreach copy, sends, CRM writes, or account changes have been made.',
  };

  const planningState = planningStateForHandoffResult(result);
  const lines = [];
  if (leadIn) lines.push(leadIn, '');
  lines.push(result.message);

  return {
    message: lines.join('\n'),
    step: planningState,
    answers,
    slots: {
      ...briefSlots,
      scoutHandoffBriefGenerated: true,
      scoutHandoffApproved: true,
      scoutHandoffQueued: Boolean(
        result.workRequest &&
          (result.executionWired || result.sourcingUnavailable)
      ),
    },
    preview: opts.priorPreview || null,
    criteriaPreview: opts.priorCriteriaPreview || null,
    buildProposal: opts.priorBuildProposal || null,
    prospectListDraft: opts.priorProspectListDraft || null,
    scoutHandoffBrief: nextBrief,
    scoutHandoff: result.handoff,
    scoutWorkRequest: result.workRequest,
    scoutCandidateBatch: result.candidateBatch,
    liveProspectList: null,
    intent: result.intent,
    shouldExecuteScoutSourcing: Boolean(result.shouldExecuteScoutSourcing),
    previewApproved: true,
    criteriaApproved: true,
    buildProposalApproved: Boolean(briefSlots.buildProposalApproved),
    liveSourcingApproved: false,
    planningState,
    currentAsk: null,
  };
}

/**
 * Apply an executeScoutWorkRequest / handBriefToScoutAsync result onto a
 * produceHandBriefToScoutResult reply shape (session + UI fields).
 */
function applyScoutExecutionResult(reply, result) {
  if (!reply || !result) return reply;
  const handoff = result.handoff || reply.scoutHandoff;
  const scoutRan = Boolean(
    result.scoutRan ||
      (handoff &&
        (handoff.status === 'completed' ||
          handoff.status === 'failed' ||
          handoff.status === 'failed_quality_gate' ||
          handoff.scoutRan))
  );
  const completed = Boolean(
    handoff &&
      (handoff.status === 'completed' ||
        handoff.status === 'failed' ||
        handoff.status === 'failed_quality_gate' ||
        scoutRan)
  );
  // Prefer executed-result guardrails — never keep draft "Creating this brief…" copy.
  let completedGuardrails = null;
  if (completed) {
    const fromHandoff =
      handoff && Array.isArray(handoff.guardrails) ? handoff.guardrails : null;
    const handoffLooksDraft =
      fromHandoff &&
      fromHandoff.some((g) =>
        /Creating this brief does not hand|when sourcing execution is wired|in this step/i.test(
          String(g || '')
        )
      );
    if (fromHandoff && fromHandoff.length && !handoffLooksDraft) {
      completedGuardrails = [...fromHandoff];
    } else if (
      result.candidateBatch &&
      Array.isArray(result.candidateBatch.guardrails) &&
      result.candidateBatch.guardrails.length
    ) {
      completedGuardrails = [...result.candidateBatch.guardrails];
    } else {
      completedGuardrails = [...COMPLETED_RESULT_GUARDRAILS];
    }
  }
  const brief = reply.scoutHandoffBrief
    ? {
        ...reply.scoutHandoffBrief,
        handoffId: handoff && handoff.handoffId,
        scoutHandoff: handoff,
        status: handoff && handoff.status,
        uiStatus: handoff && handoff.uiStatus,
        scoutRan,
        workRequestId: result.workRequest && result.workRequest.workRequestId,
        updatedAt: handoff && handoff.updatedAt,
        // Once Scout has run, drop draft-brief guardrail / review-gate language.
        guardrails: completed && completedGuardrails
          ? [...completedGuardrails]
          : reply.scoutHandoffBrief.guardrails,
        reviewGate: completed
          ? 'Operator reviews Scout’s returned batch (accepted / review_required / rejected) before any Composer, CRM, export, or outreach use. Rejected and review_required rows are not outreach-ready.'
          : reply.scoutHandoffBrief.reviewGate,
        recommendedNextStep: scoutRan
          ? result.ok
            ? 'Review Scout candidates by status (accepted / review_required / rejected). Approve before Composer / CRM / export use. No outreach or CRM writes yet.'
            : handoff && handoff.status === 'failed_quality_gate'
              ? 'Scout failed quality gate — usable accepted/reviewable property-manager count below minimum. Review rejected audit rows, revise criteria, and retry.'
              : 'Scout sourcing failed — work request preserved. Review failure and retry or revise criteria.'
          : reply.scoutHandoffBrief.recommendedNextStep,
        disclaimer: completed
          ? 'Scout handoff results are review-only. No outreach copy, sends, CRM writes, or account changes have been made. Draft brief instructions no longer apply.'
          : 'Scout handoff results are review-only. No outreach copy, sends, CRM writes, or account changes have been made.',
      }
    : reply.scoutHandoffBrief;

  const planningState = planningStateForHandoffResult(result);
  const leadIn =
    typeof reply.message === 'string' &&
    reply.message.startsWith('Approving the Scout Handoff Brief')
      ? 'Approving the Scout Handoff Brief and creating a Scout work request. Max will not claim Scout inspected sources unless Scout actually ran.'
      : null;
  const message = leadIn
    ? `${leadIn}\n\n${result.message}`
    : result.message || reply.message;

  return {
    ...reply,
    message,
    step: planningState,
    planningState,
    intent: result.intent,
    scoutHandoffBrief: brief,
    scoutHandoff: handoff,
    scoutWorkRequest: result.workRequest || reply.scoutWorkRequest,
    scoutCandidateBatch: result.candidateBatch || null,
    shouldExecuteScoutSourcing: false,
    createdNewHandoff: Boolean(result.createdNewHandoff),
  };
}

/**
 * Execute / retry a preserved Scout work request by ID.
 * Does not create a new handoff, does not re-ask for build-proposal approval,
 * and does not emit placeholder drafts.
 */
function candidateTownToken(row) {
  const hay = [
    row && row.location,
    row && row.address,
    row && row.formatted_address,
    row && row.marketTown,
    row && row.geo && row.geo.town,
  ]
    .filter(Boolean)
    .join(' ');
  const s = String(hay);
  for (const town of PRIORITY_TOWNS_NH) {
    if (new RegExp(`\\b${town}\\b`, 'i').test(s)) return town;
  }
  for (const town of NEARBY_FILL_TOWNS_NH) {
    if (new RegExp(`\\b${town}\\b`, 'i').test(s)) return town;
  }
  for (const town of EXTENDED_REVIEW_TOWNS_NH) {
    if (new RegExp(`\\b${town}\\b`, 'i').test(s)) return town;
  }
  return (row && row.geo && row.geo.town) || null;
}

function isPrimaryTownCandidate(row) {
  const town = candidateTownToken(row);
  return Boolean(
    town && PRIORITY_TOWNS_NH.some((t) => t.toLowerCase() === String(town).toLowerCase())
  );
}

function isExpansionTownCandidate(row) {
  const town = candidateTownToken(row);
  if (!town) return false;
  const lower = String(town).toLowerCase();
  return (
    NEARBY_FILL_TOWNS_NH.some((t) => t.toLowerCase() === lower) ||
    EXTENDED_REVIEW_TOWNS_NH.some((t) => t.toLowerCase() === lower)
  );
}

function normalizeBatchReviewRow(row) {
  const r = row || {};
  const role = r.suggestedContactRole || r.contactRole || null;
  const suggestedContactRole =
    role && !/^suggested contact role:/i.test(String(role))
      ? `Suggested contact role: ${role}`
      : role || 'Suggested contact role: Owner / property manager';
  return {
    company: r.companyName || r.company || '—',
    companyName: r.companyName || r.company || '—',
    location: r.location || r.address || r.marketTown || '—',
    sourceUrl: r.sourceUrl || r.website || r.url || '—',
    whyItFits: r.fitRationale || r.fitReason || r.whyItFits || '—',
    fitRationale: r.fitRationale || r.fitReason || r.whyItFits || '—',
    riskUncertainty: r.risks || r.riskUncertainty || r.statusReason || '—',
    risks: r.risks || r.riskUncertainty || '—',
    suggestedContactRole,
    confidence: r.confidence || 'medium',
    reviewStatus:
      r.reviewStatus || r.status || CANDIDATE_STATUS.REVIEW_REQUIRED,
    status: r.status || CANDIDATE_STATUS.REVIEW_REQUIRED,
    statusReason: r.statusReason || null,
    rejectionReason: r.rejectionReason || null,
    relationship: r.relationship || null,
    doNotOutreach: Boolean(r.doNotOutreach),
  };
}

function companyNameOf(row) {
  return String((row && (row.companyName || row.company)) || '').trim();
}

function candidateSourceUrl(row) {
  return String(
    (row && (row.sourceUrl || row.website || row.url || row.source_url)) || ''
  ).trim();
}

function extractDomainFromUrl(url) {
  const raw = String(url || '').trim();
  if (!raw) return '';
  try {
    const withProto = /^https?:\/\//i.test(raw) ? raw : `https://${raw}`;
    const host = new URL(withProto).hostname.toLowerCase();
    return host.replace(/^www\./, '');
  } catch {
    const m = raw.match(
      /(?:https?:\/\/)?(?:www\.)?([a-z0-9-]+(?:\.[a-z0-9-]+)+)/i
    );
    return m ? String(m[1]).toLowerCase().replace(/^www\./, '') : '';
  }
}

/**
 * Strip legal/generic industry phrasing so identity compares distinctive brands.
 * "Property Management" → "" (no identity). "Keyrenter New England Property Management" → "keyrenter".
 */
function normalizeCompanyIdentity(name) {
  let s = String(name || '')
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^\w\s]/g, ' ')
    .replace(GENERIC_COMPANY_PHRASE_RE, ' ');
  const tokens = s
    .split(/\s+/)
    .map((t) => t.trim())
    .filter(Boolean)
    .filter((t) => !GENERIC_COMPANY_IDENTITY_TOKENS.includes(t))
    .filter((t) => t.length >= 3);
  return tokens.join(' ').trim();
}

function domainsMatch(rowDomain, overrideDomain) {
  const a = String(rowDomain || '')
    .toLowerCase()
    .replace(/^www\./, '');
  const b = String(overrideDomain || '')
    .toLowerCase()
    .replace(/^www\./, '');
  if (!a || !b) return false;
  return a === b || a.endsWith(`.${b}`) || b.endsWith(`.${a}`);
}

/**
 * Match an operator relationship override by stable identity only:
 * source URL/domain and/or normalized distinctive company name.
 * Never promote generic "Property Management" via substring matching.
 */
function relationshipOverrideMatchesRow(row, override) {
  if (!override || !row) return false;

  const rowUrl = candidateSourceUrl(row);
  const rowDomain = extractDomainFromUrl(rowUrl);

  // 1) Source URL / domain identity
  const overrideDomains = Array.isArray(override.domains)
    ? override.domains
    : [];
  if (
    rowDomain &&
    overrideDomains.some((d) => domainsMatch(rowDomain, d))
  ) {
    return true;
  }
  if (override.domainRe && rowUrl && override.domainRe.test(rowUrl)) {
    return true;
  }
  if (override.domainRe && rowDomain && override.domainRe.test(rowDomain)) {
    return true;
  }

  const name = companyNameOf(row);
  if (!name) return false;

  // 2) Distinctive brand token on the company name (e.g. \bkeyrenter\b).
  // Does NOT match bare "Property Management".
  if (override.matchRe && override.matchRe.test(name)) {
    return true;
  }

  // 3) Normalized distinctive identity cores — never substring-match generics.
  if (override.companyName) {
    const needleCore = normalizeCompanyIdentity(override.companyName);
    const hayCore = normalizeCompanyIdentity(name);
    // Generic-only names (e.g. "Property Management") have empty cores.
    if (!needleCore || !hayCore) return false;
    if (needleCore === hayCore) return true;

    const needleTokens = needleCore.split(/\s+/).filter(Boolean);
    const hayTokens = hayCore.split(/\s+/).filter(Boolean);
    if (!needleTokens.length || !hayTokens.length) return false;

    // Candidate is a prefix/alias of the override brand (or vice versa),
    // requiring every distinctive token of the shorter side to appear.
    const shorter =
      hayTokens.length <= needleTokens.length ? hayTokens : needleTokens;
    const longerSet = new Set(
      hayTokens.length <= needleTokens.length ? needleTokens : hayTokens
    );
    if (shorter.every((t) => longerSet.has(t))) return true;
  }

  return false;
}

/**
 * Parse operator relationship overrides from a correction message.
 * Example: "Remove Keyrenter — existing relationship, not a cold prospect."
 * Only explicit brand mentions become overrides — never generic industry phrases.
 */
function parseRelationshipOverridesFromMessage(text) {
  const s = String(text || '');
  if (!s.trim()) return [];
  const out = [];
  for (const known of KNOWN_RELATIONSHIP_OVERRIDE_PATTERNS) {
    if (!known.matchRe.test(s)) continue;
    const isExisting =
      /\bexisting\s+relationship\b/i.test(s) ||
      /\bnot\s+a\s+cold\s+prospect\b/i.test(s) ||
      /\bnurture\b/i.test(s) ||
      /\bremove\b/i.test(s) ||
      /\bexclude\b/i.test(s) ||
      /\bdrop\b/i.test(s) ||
      /\bkeep\b[\s\S]{0,40}\bnurture\b/i.test(s);
    if (!isExisting) continue;
    out.push({
      companyName: known.companyName,
      matchRe: known.matchRe,
      domains: known.domains ? [...known.domains] : [],
      domainRe: known.domainRe || null,
      relationship: RELATIONSHIP_STATUS.EXISTING_RELATIONSHIP,
      reason: known.reason,
      source: 'operator_message',
    });
  }
  return out;
}

function mergeRelationshipOverrides(...lists) {
  const byKey = new Map();
  for (const list of lists) {
    for (const o of list || []) {
      if (!o) continue;
      const key = String(o.companyName || o.matchRe || '')
        .toLowerCase()
        .replace(/\s+/g, ' ')
        .trim();
      if (!key) continue;
      const prev = byKey.get(key) || {};
      byKey.set(key, {
        companyName: o.companyName || prev.companyName || null,
        matchRe: o.matchRe || prev.matchRe || null,
        domains: [
          ...new Set([
            ...(Array.isArray(prev.domains) ? prev.domains : []),
            ...(Array.isArray(o.domains) ? o.domains : []),
          ]),
        ],
        domainRe: o.domainRe || prev.domainRe || null,
        relationship:
          o.relationship ||
          prev.relationship ||
          RELATIONSHIP_STATUS.EXISTING_RELATIONSHIP,
        reason: o.reason || prev.reason || 'Operator relationship override',
        source: o.source || prev.source || 'operator',
      });
    }
  }
  return [...byKey.values()];
}

/**
 * Apply operator relationship overrides: move matching rows out of accepted /
 * review_required into existing_relationship (nurture). Never cold outreach.
 */
function applyRelationshipOverridesToBatch(batch, overrides) {
  const list = Array.isArray(overrides) ? overrides.filter(Boolean) : [];
  if (!list.length) {
    return {
      batch: batch || { candidates: [], rejected: [], groups: {} },
      existingRelationship: [],
      appliedOverrides: [],
    };
  }

  const src = batch || {};
  const groups = src.groups || {
    accepted: (src.candidates || []).filter((c) => c.status === 'accepted'),
    review_required: (src.candidates || []).filter(
      (c) => c.status === 'review_required'
    ),
    rejected: src.rejected || [],
  };

  const existingRelationship = [];
  const appliedOverrides = [];
  const moveIfMatch = (row) => {
    for (const override of list) {
      if (!relationshipOverrideMatchesRow(row, override)) continue;
      const next = {
        ...row,
        status: RELATIONSHIP_STATUS.EXISTING_RELATIONSHIP,
        reviewStatus: RELATIONSHIP_STATUS.EXISTING_RELATIONSHIP,
        relationship: RELATIONSHIP_STATUS.EXISTING_RELATIONSHIP,
        doNotOutreach: true,
        statusReason:
          override.reason ||
          'Operator relationship override — existing_relationship / nurture',
        risks:
          row.risks ||
          'Existing relationship — nurture only; do not include in campaign outreach',
      };
      existingRelationship.push(next);
      appliedOverrides.push({
        companyName: companyNameOf(row),
        relationship: RELATIONSHIP_STATUS.EXISTING_RELATIONSHIP,
        reason: override.reason,
      });
      return true;
    }
    return false;
  };

  const accepted = [];
  for (const row of groups.accepted || []) {
    if (!moveIfMatch(row)) accepted.push(row);
  }
  const reviewRequired = [];
  for (const row of groups.review_required || []) {
    if (!moveIfMatch(row)) reviewRequired.push(row);
  }
  // Also scan flat candidates if groups were incomplete.
  for (const row of src.candidates || []) {
    const name = companyNameOf(row);
    if (
      existingRelationship.some((e) => companyNameOf(e) === name) ||
      accepted.some((e) => companyNameOf(e) === name) ||
      reviewRequired.some((e) => companyNameOf(e) === name) ||
      (groups.rejected || []).some((e) => companyNameOf(e) === name)
    ) {
      continue;
    }
    if (!moveIfMatch(row)) {
      if (row.status === 'accepted') accepted.push(row);
      else if (row.status === 'review_required') reviewRequired.push(row);
    }
  }

  const rejected = groups.rejected || src.rejected || [];
  const nextBatch = {
    ...src,
    candidates: accepted.concat(reviewRequired),
    rejected,
    groups: {
      accepted,
      review_required: reviewRequired,
      rejected,
      existing_relationship: existingRelationship,
    },
    acceptedCount: accepted.length,
    reviewRequiredCount: reviewRequired.length,
    rejectedCount: rejected.length,
    existingRelationshipCount: existingRelationship.length,
  };

  return { batch: nextBatch, existingRelationship, appliedOverrides };
}

function buildProspectBatchReviewClosingQuestion(review) {
  const acceptedCount = (review && review.acceptedFirstPass
    ? review.acceptedFirstPass
    : []
  ).length;
  const sourceRows =
    (review && review.sourceVerificationRequired) || [];
  const nurtureRows = (review && review.existingRelationship) || [];
  const hasCedar = sourceRows.some((r) =>
    /cedar\s+management/i.test(companyNameOf(r))
  );
  const hasKeyrenter = nurtureRows.some((r) =>
    /\bkeyrenter\b/i.test(companyNameOf(r))
  );

  if (hasCedar && hasKeyrenter) {
    return `Do you want to approve the ${acceptedCount} accepted cold first-pass candidates, include Cedar after source verification, and keep Keyrenter as an existing-relationship nurture account?`;
  }
  if (hasKeyrenter && sourceRows.length) {
    return `Do you want to approve the ${acceptedCount} accepted cold first-pass candidates, include ${sourceRows.length} source-verification account${
      sourceRows.length === 1 ? '' : 's'
    } after verification, and keep Keyrenter as an existing-relationship nurture account?`;
  }
  if (hasKeyrenter) {
    return `Do you want to approve the ${acceptedCount} accepted cold first-pass candidates and keep Keyrenter as an existing-relationship nurture account?`;
  }
  if (hasCedar) {
    return `Do you want to approve the ${acceptedCount} accepted cold first-pass candidates and include Cedar after source verification?`;
  }
  const expansion = (review && review.optionalExpansion) || [];
  if (expansion.length) {
    return `Do you want to approve the ${acceptedCount} accepted cold first-pass candidates only, or include expansion candidates after review?`;
  }
  return `Do you want to approve the ${acceptedCount} accepted cold first-pass candidates?`;
}

function formatBatchReviewCandidateBlock(row, index) {
  const r = normalizeBatchReviewRow(row);
  return [
    `${index}. **${r.company}**`,
    `   - Company: ${r.company}`,
    `   - Location: ${r.location}`,
    `   - Source URL: ${r.sourceUrl}`,
    `   - Why it fits: ${r.whyItFits}`,
    `   - Risk/uncertainty: ${r.riskUncertainty}`,
    `   - Suggested contact role: ${String(r.suggestedContactRole).replace(
      /^Suggested contact role:\s*/i,
      ''
    )}`,
    `   - Confidence: ${r.confidence}`,
    `   - Review status: ${r.reviewStatus}`,
    r.relationship
      ? `   - Relationship: ${r.relationship}`
      : null,
    r.doNotOutreach ? `   - Outreach: do not include in campaign outreach` : null,
  ]
    .filter(Boolean)
    .join('\n');
}

function digestCompanyLabel(row) {
  // Held-back / included lines use the full company name for operator trust.
  return companyNameOf(row) || '—';
}

function rejectionHeldBackReason(row) {
  const reason = String(
    (row && (row.rejectionReason || row.statusReason || row.risks)) || ''
  ).toLowerCase();
  if (/institutional|large_institutional|cushman|national\s+firm/i.test(reason)) {
    return 'rejected as too institutional';
  }
  if (/outside.?market|outside.?region/i.test(reason)) {
    return 'rejected — outside market region';
  }
  if (/wrong.?segment/i.test(reason)) {
    return 'rejected — wrong segment';
  }
  return 'rejected';
}

/**
 * Build Operator Review Digest + collapsed evidence for Prospect Batch Review.
 * Digest is the default operator view; full candidate dumps live under View evidence.
 */
function buildProspectBatchReviewOperatorDigest(review) {
  const p = review || {};
  const accepted = p.acceptedFirstPass || [];
  const sourceVerification = p.sourceVerificationRequired || [];
  const expansion = p.optionalExpansion || [];
  const nurture = p.existingRelationship || [];
  const rejected = p.rejected || [];
  const acceptedCount = accepted.length;

  const cedarRows = sourceVerification.filter((r) =>
    /cedar/i.test(companyNameOf(r))
  );
  const otherSourceVerification = sourceVerification.filter(
    (r) => !/cedar/i.test(companyNameOf(r))
  );
  const keyrenterRows = nurture.filter((r) =>
    /\bkeyrenter\b/i.test(companyNameOf(r))
  );
  const otherNurture = nurture.filter(
    (r) => !/\bkeyrenter\b/i.test(companyNameOf(r))
  );

  const heldBack = [];
  cedarRows.forEach((r) => {
    heldBack.push(
      `${digestCompanyLabel(r)} — source verification required`
    );
  });
  otherSourceVerification.forEach((r) => {
    heldBack.push(
      `${digestCompanyLabel(r)} — source verification required`
    );
  });
  keyrenterRows.forEach((r) => {
    heldBack.push(
      `${digestCompanyLabel(r)} — existing relationship / nurture only`
    );
  });
  otherNurture.forEach((r) => {
    heldBack.push(
      `${digestCompanyLabel(r)} — existing relationship / nurture only`
    );
  });
  if (expansion.length) {
    heldBack.push('Optional Manchester candidates — not included yet');
  }
  rejected.forEach((r) => {
    heldBack.push(
      `${digestCompanyLabel(r)} — ${rejectionHeldBackReason(r)}`
    );
  });

  const why = [
    'This keeps Batch 1 focused on clean, net-new prospects in the approved priority towns.',
  ];

  const evidenceSections = [
    {
      title: 'Accepted cold first-pass candidates',
      intro:
        'Primary-town cold prospects — full sourced records for inspection.',
      records: accepted,
    },
    {
      title: 'Source-verification required',
      intro:
        'Primary-town candidates that need source verification before outreach.',
      records: sourceVerification,
    },
  ];
  if (expansion.length) {
    evidenceSections.push({
      title: 'Optional expansion candidates',
      intro: 'Manchester NH / nearby expansion — excluded unless approved.',
      records: expansion,
    });
  }
  evidenceSections.push({
    title: 'Existing relationship / nurture',
    intro: 'Nurture only — do not include in campaign outreach.',
    records: nurture,
  });
  evidenceSections.push({
    title: 'Rejected candidates',
    intro:
      'Large institutional / wrong segment / outside market (audit only).',
    records: rejected,
  });

  const auditNotes = [
    p.workRequestId ? `workRequestId: ${p.workRequestId}` : null,
    `Counts — accepted ${acceptedCount}, source-verification ${sourceVerification.length}, nurture ${nurture.length}, expansion ${expansion.length}, rejected ${rejected.length}`,
    PROSPECT_BATCH_REVIEW_DISCLAIMER,
  ].filter(Boolean);

  return buildOperatorReviewDigest({
    kind: PROSPECT_BATCH_REVIEW_KIND,
    title: PROSPECT_BATCH_REVIEW_TITLE,
    recommendedDecision: `Approve ${acceptedCount} cold prospect${
      acceptedCount === 1 ? '' : 's'
    } as Batch 1.`,
    whyRecommended: why,
    included: accepted.map(digestCompanyLabel),
    heldBack,
    keyWatchouts: [],
    nextStepAfterApproval: OUTREACH_STRATEGY_PREVIEW_TITLE,
    sectionOrder: PROSPECT_BATCH_REVIEW_DIGEST_SECTION_ORDER.slice(),
    sectionTitles: {
      excluded: 'Held back',
    },
    primaryActions: [
      {
        id: 'approve_batch_1',
        label: 'Approve Batch 1',
        style: 'primary',
        message: `Approve the ${acceptedCount} accepted cold first-pass candidates as Batch 1.`,
      },
      {
        id: 'request_changes',
        label: 'Request changes',
        style: 'secondary',
        message: null,
      },
      {
        id: 'view_evidence',
        label: VIEW_EVIDENCE_LABEL,
        style: 'secondary',
        message: null,
      },
    ],
    evidence: {
      collapsedByDefault: true,
      label: VIEW_EVIDENCE_LABEL,
      sections: evidenceSections,
      rejectedOrHeld: sourceVerification.concat(nurture, expansion, rejected),
      auditNotes,
    },
    disclaimer: p.disclaimer || PROSPECT_BATCH_REVIEW_DISCLAIMER,
    meta: {
      acceptedCount,
      sourceVerificationCount: sourceVerification.length,
      nurtureCount: nurture.length,
      expansionCount: expansion.length,
      rejectedCount: rejected.length,
      workRequestId: p.workRequestId || null,
    },
    reviewOnly: true,
  });
}

/**
 * Build Prospect Batch Review from a completed Scout candidate batch.
 * Applies maps-only downgrade (Cedar Management Group → review_required / medium).
 * Applies operator relationship overrides (Keyrenter → existing_relationship / nurture).
 * Hard-filters out-of-state rows from usable sections; can merge prior NH primaries.
 */
function buildProspectBatchReview(batch, opts = {}) {
  const preserved = mergePreservedNhCandidatesFromPriorBatch(
    batch || {},
    opts.preserveFromBatch || opts.priorCompletedScoutCandidateBatch || null
  );
  const downgraded = applyMapsOnlyDowngradeToBatch(preserved || {});

  const messageOverrides = parseRelationshipOverridesFromMessage(
    opts.userMessage || opts.operatorMessage || ''
  );
  const relationshipOverrides = mergeRelationshipOverrides(
    opts.relationshipOverrides || [],
    (opts.priorProspectBatchReview &&
      opts.priorProspectBatchReview.relationshipOverrides) ||
      [],
    messageOverrides
  );

  const {
    batch: overridden,
    existingRelationship: existingFromOverride,
    appliedOverrides,
  } = applyRelationshipOverridesToBatch(downgraded, relationshipOverrides);

  const groups = overridden.groups || {
    accepted: [],
    review_required: [],
    rejected: [],
  };

  const outOfStateRejected = [];
  const nhAccepted = [];
  const nhReview = [];

  for (const row of groups.accepted || []) {
    if (!isNhMarketCandidateRow(row)) {
      outOfStateRejected.push({
        ...row,
        status: CANDIDATE_STATUS.REJECTED,
        statusReason:
          row.statusReason ||
          'Outside New Hampshire market region — outside_market_region',
        rejectionReason:
          row.rejectionReason || REJECTION_REASON.OUTSIDE_MARKET_REGION,
      });
      continue;
    }
    nhAccepted.push(row);
  }
  for (const row of groups.review_required || []) {
    if (!isNhMarketCandidateRow(row)) {
      outOfStateRejected.push({
        ...row,
        status: CANDIDATE_STATUS.REJECTED,
        statusReason:
          row.statusReason ||
          'Outside New Hampshire market region — outside_market_region',
        rejectionReason:
          row.rejectionReason || REJECTION_REASON.OUTSIDE_MARKET_REGION,
      });
      continue;
    }
    nhReview.push(row);
  }

  const accepted = nhAccepted.filter(isPrimaryTownCandidate);
  // Primary-town review_required (e.g. maps-only Cedar) → source verification.
  const sourceVerificationRequired = nhReview.filter(isPrimaryTownCandidate);
  // Optional expansion: Manchester / Derry / nearby review_required only.
  const optionalExpansion = nhReview.filter(isExpansionTownCandidate);
  const rejected = (groups.rejected || []).concat(outOfStateRejected);
  const existingRelationship = (existingFromOverride || []).map((row) => ({
    ...row,
    status: RELATIONSHIP_STATUS.EXISTING_RELATIONSHIP,
    reviewStatus: RELATIONSHIP_STATUS.EXISTING_RELATIONSHIP,
    relationship: RELATIONSHIP_STATUS.EXISTING_RELATIONSHIP,
    doNotOutreach: true,
  }));

  const workRequestId =
    opts.workRequestId ||
    (batch && batch.workRequestId) ||
    (opts.priorScoutWorkRequest && opts.priorScoutWorkRequest.workRequestId) ||
    null;

  const scoutCandidateBatch = {
    ...overridden,
    candidates: nhAccepted.concat(nhReview),
    rejected,
    groups: {
      accepted: nhAccepted,
      review_required: nhReview,
      rejected,
      existing_relationship: existingRelationship,
    },
    acceptedCount: nhAccepted.length,
    reviewRequiredCount: nhReview.length,
    rejectedCount: rejected.length,
    existingRelationshipCount: existingRelationship.length,
  };

  // Preserve pre-override Scout batch so later corrections can re-apply overrides.
  const sourceScoutCandidateBatch =
    (opts.priorProspectBatchReview &&
      opts.priorProspectBatchReview.sourceScoutCandidateBatch) ||
    downgraded;

  const review = {
    kind: PROSPECT_BATCH_REVIEW_KIND,
    title: PROSPECT_BATCH_REVIEW_TITLE,
    status: 'review_only',
    workRequestId,
    sectionTitles: { ...PROSPECT_BATCH_REVIEW_SECTION_TITLES },
    acceptedFirstPass: accepted.map(normalizeBatchReviewRow),
    sourceVerificationRequired: sourceVerificationRequired.map(
      normalizeBatchReviewRow
    ),
    optionalExpansion: optionalExpansion.map(normalizeBatchReviewRow),
    existingRelationship: existingRelationship.map(normalizeBatchReviewRow),
    rejected: rejected.map(normalizeBatchReviewRow),
    counts: {
      accepted: accepted.length,
      sourceVerificationRequired: sourceVerificationRequired.length,
      reviewRequired:
        sourceVerificationRequired.length + optionalExpansion.length,
      optionalExpansion: optionalExpansion.length,
      existingRelationship: existingRelationship.length,
      rejected: rejected.length,
      scoutAccepted: nhAccepted.length,
      scoutReviewRequired: nhReview.length,
      scoutRejected: rejected.length,
    },
    relationshipOverrides,
    appliedOverrides,
    sourceScoutCandidateBatch,
    scoutCandidateBatch,
    disclaimer: PROSPECT_BATCH_REVIEW_DISCLAIMER,
    reviewOnly: true,
    crmWritesMade: false,
    outreachCopyGenerated: false,
    accountChangesMade: false,
    exportMade: false,
  };
  review.closingQuestion = buildProspectBatchReviewClosingQuestion(review);
  review.operatorDigest = buildProspectBatchReviewOperatorDigest(review);
  review.operatorDigest.closingQuestion = review.closingQuestion;
  return review;
}

/**
 * Default operator message: digest first; full evidence collapsed / not dumped.
 * Pass { includeEvidence: true } only when an operator explicitly expands evidence.
 */
function formatProspectBatchReviewMessage(review, opts = {}) {
  const p = review || {};
  const digest =
    p.operatorDigest || buildProspectBatchReviewOperatorDigest(p);
  const includeEvidence = opts.includeEvidence === true;
  const closing =
    p.closingQuestion ||
    buildProspectBatchReviewClosingQuestion(p) ||
    PROSPECT_BATCH_REVIEW_CLOSING_QUESTION;

  return formatOperatorReviewArtifactMessage(digest, {
    includeEvidence,
    closingQuestion: closing,
  });
}

/** Full evidence dump for View evidence (not the default operator view). */
function formatProspectBatchReviewEvidenceMessage(review) {
  const p = review || {};
  const digest =
    p.operatorDigest || buildProspectBatchReviewOperatorDigest(p);
  return formatOperatorReviewEvidenceMessage(digest.evidence);
}

/** @deprecated Prefer formatProspectBatchReviewMessage — kept for callers that need section dumps. */
function formatProspectBatchReviewEvidenceSectionsMessage(review) {
  const p = review || {};
  const titles = p.sectionTitles || PROSPECT_BATCH_REVIEW_SECTION_TITLES;
  const lines = [p.title || PROSPECT_BATCH_REVIEW_TITLE, ''];
  const counts = p.counts || {};

  lines.push(
    `Counts — Accepted cold first-pass: ${
      counts.accepted != null
        ? counts.accepted
        : (p.acceptedFirstPass || []).length
    } · Source-verification required: ${
      counts.sourceVerificationRequired != null
        ? counts.sourceVerificationRequired
        : (p.sourceVerificationRequired || []).length
    } · Existing relationship / nurture: ${
      counts.existingRelationship != null
        ? counts.existingRelationship
        : (p.existingRelationship || []).length
    } · Rejected: ${
      counts.rejected != null ? counts.rejected : (p.rejected || []).length
    }`
  );
  if (p.workRequestId) {
    lines.push(`workRequestId: ${p.workRequestId}`);
  }
  lines.push('');

  let sectionNum = 1;
  const pushSection = (title, intro, rows, extraRowFormatter) => {
    lines.push(`## ${sectionNum}. ${title}`);
    sectionNum += 1;
    if (intro) lines.push(intro);
    lines.push('');
    if ((rows || []).length) {
      rows.forEach((row, i) => {
        lines.push(formatBatchReviewCandidateBlock(row, i + 1));
        if (typeof extraRowFormatter === 'function') {
          const extra = extraRowFormatter(row);
          if (extra) lines.push(extra);
        }
        lines.push('');
      });
    } else {
      lines.push('_None._');
      lines.push('');
    }
  };

  pushSection(
    titles.acceptedFirstPass,
    'Primary-town cold prospects in Bedford NH, Hooksett NH, Londonderry NH, Auburn NH, or Goffstown NH — not existing relationships.',
    p.acceptedFirstPass
  );

  pushSection(
    titles.sourceVerificationRequired,
    'Primary-town candidates that need source verification before outreach (for example maps-only / no company website).',
    p.sourceVerificationRequired
  );

  if ((p.optionalExpansion || []).length) {
    pushSection(
      titles.optionalExpansion,
      'Manchester NH / nearby expansion candidates unless explicitly approved.',
      p.optionalExpansion
    );
  }

  pushSection(
    titles.existingRelationship,
    'Existing relationships — nurture only. Do not include in campaign outreach.',
    p.existingRelationship
  );

  pushSection(
    titles.rejected,
    'Large institutional / wrong segment / outside market candidates (audit only — not outreach-ready).',
    p.rejected,
    (row) =>
      row.rejectionReason || row.statusReason
        ? `   - Rejection reason: ${row.rejectionReason || row.statusReason}`
        : null
  );

  lines.push(p.disclaimer || PROSPECT_BATCH_REVIEW_DISCLAIMER);
  return lines.join('\n').trim();
}

function produceProspectBatchReviewResult(ctx, answers, slots, opts, leadIn) {
  const priorReview = opts.priorProspectBatchReview || null;
  const batch =
    (priorReview && priorReview.sourceScoutCandidateBatch) ||
    opts.priorScoutCandidateBatch ||
    (opts.priorScoutHandoff && opts.priorScoutHandoff.candidateBatch) ||
    (priorReview && priorReview.scoutCandidateBatch) ||
    null;
  const workRequestId =
    opts.workRequestId ||
    extractWorkRequestIdFromMessage(opts.userMessage) ||
    (opts.priorScoutWorkRequest && opts.priorScoutWorkRequest.workRequestId) ||
    (opts.priorScoutHandoff &&
      opts.priorScoutHandoff.workRequest &&
      opts.priorScoutHandoff.workRequest.workRequestId) ||
    (batch && batch.workRequestId) ||
    (opts.priorProspectBatchReview &&
      opts.priorProspectBatchReview.workRequestId) ||
    null;

  if (!hasCompletedScoutCandidateBatch(batch)) {
    return {
      message: [
        leadIn || 'Prospect Batch Review needs the latest completed Scout result.',
        '',
        workRequestId
          ? `No completed Scout candidate batch is loaded for workRequestId=${workRequestId}.`
          : 'No completed Scout candidate batch is loaded in this session.',
        'Max will not invent placeholders or repeat the build proposal.',
        'No outreach copy, sends, CRM writes, or account changes were made.',
      ].join('\n'),
      step: CAMPAIGN_PLANNING_STATES.SCOUT_HANDOFF_COMPLETED,
      answers,
      slots: { ...slots },
      preview: opts.priorPreview || null,
      criteriaPreview: opts.priorCriteriaPreview || null,
      buildProposal: opts.priorBuildProposal || null,
      prospectListDraft: opts.priorProspectListDraft || null,
      scoutHandoffBrief: opts.priorScoutHandoffBrief || null,
      scoutHandoff: opts.priorScoutHandoff || null,
      scoutWorkRequest: opts.priorScoutWorkRequest || null,
      scoutCandidateBatch: batch,
      prospectBatchReview: null,
      liveProspectList: null,
      intent: 'prospect_batch_review_missing_batch',
      planningState: CAMPAIGN_PLANNING_STATES.SCOUT_HANDOFF_COMPLETED,
      currentAsk: null,
      workRequestId,
    };
  }

  const messageOverrides = parseRelationshipOverridesFromMessage(
    opts.userMessage || ''
  );
  const relationshipOverrides = mergeRelationshipOverrides(
    opts.relationshipOverrides || [],
    (opts.priorProspectBatchReview &&
      opts.priorProspectBatchReview.relationshipOverrides) ||
      [],
    messageOverrides
  );

  const review = buildProspectBatchReview(batch, {
    workRequestId,
    priorScoutWorkRequest: opts.priorScoutWorkRequest || null,
    preserveFromBatch:
      opts.preserveFromBatch ||
      opts.priorCompletedScoutCandidateBatch ||
      null,
    relationshipOverrides,
    userMessage: opts.userMessage || '',
    priorProspectBatchReview: opts.priorProspectBatchReview || null,
  });
  const lines = [];
  if (leadIn) lines.push(leadIn, '');
  lines.push(formatProspectBatchReviewMessage(review));

  return {
    message: lines.join('\n'),
    step: CAMPAIGN_PLANNING_STATES.PROSPECT_BATCH_REVIEW,
    answers,
    slots: {
      ...slots,
      scoutHandoffBriefGenerated: true,
      scoutHandoffApproved: true,
      scoutHandoffQueued: true,
      buildProposalApproved: true,
      criteriaApproved: true,
      previewApproved: true,
    },
    preview: opts.priorPreview || null,
    criteriaPreview: opts.priorCriteriaPreview || null,
    buildProposal: opts.priorBuildProposal || null,
    prospectListDraft: opts.priorProspectListDraft || null,
    scoutHandoffBrief: opts.priorScoutHandoffBrief || null,
    scoutHandoff: opts.priorScoutHandoff || null,
    scoutWorkRequest: opts.priorScoutWorkRequest || null,
    scoutCandidateBatch: review.scoutCandidateBatch || batch,
    prospectBatchReview: review,
    liveProspectList: null,
    intent: 'prospect_batch_review',
    previewApproved: true,
    criteriaApproved: true,
    buildProposalApproved: true,
    liveSourcingApproved: false,
    planningState: CAMPAIGN_PLANNING_STATES.PROSPECT_BATCH_REVIEW,
    currentAsk: review.closingQuestion || PROSPECT_BATCH_REVIEW_CLOSING_QUESTION,
    workRequestId,
  };
}

/**
 * Mark Batch 1 (accepted cold first-pass only) approved on a Prospect Batch Review.
 * Cedar / source-verification, Keyrenter / nurture, optional expansion, and rejected
 * stay excluded from the approved cold batch.
 */
function approveProspectBatchReviewBatch1(review, opts = {}) {
  const prior = review || {};
  const approvedCandidates = (prior.acceptedFirstPass || []).map((row) => ({
    ...row,
    batchMembership: 'batch_1',
    approvedInBatch1: true,
  }));
  const approvedAt =
    opts.approvedAt ||
    prior.batch1ApprovedAt ||
    (prior.approvedBatch && prior.approvedBatch.approvedAt) ||
    new Date().toISOString();

  return {
    ...prior,
    kind: PROSPECT_BATCH_REVIEW_KIND,
    title: prior.title || PROSPECT_BATCH_REVIEW_TITLE,
    status: 'batch_1_approved',
    batch1Approved: true,
    batch1ApprovedAt: approvedAt,
    approvedBatch: {
      name: 'Batch 1',
      status: 'approved',
      approvedAt,
      candidateCount: approvedCandidates.length,
      candidates: approvedCandidates,
      // Explicit exclusions — not cold outreach for Batch 1
      excludedSourceVerification: (prior.sourceVerificationRequired || []).map(
        (r) => r.companyName || r.company
      ),
      excludedExistingRelationship: (prior.existingRelationship || []).map(
        (r) => r.companyName || r.company
      ),
      excludedOptionalExpansion: (prior.optionalExpansion || []).map(
        (r) => r.companyName || r.company
      ),
      excludedRejected: (prior.rejected || []).map(
        (r) => r.companyName || r.company
      ),
    },
    acceptedFirstPass: prior.acceptedFirstPass || [],
    sourceVerificationRequired: prior.sourceVerificationRequired || [],
    optionalExpansion: prior.optionalExpansion || [],
    existingRelationship: prior.existingRelationship || [],
    rejected: prior.rejected || [],
    counts: {
      ...(prior.counts || {}),
      accepted: (prior.acceptedFirstPass || []).length,
      approvedBatch1: approvedCandidates.length,
      sourceVerificationRequired: (prior.sourceVerificationRequired || [])
        .length,
      existingRelationship: (prior.existingRelationship || []).length,
      optionalExpansion: (prior.optionalExpansion || []).length,
      rejected: (prior.rejected || []).length,
    },
    closingQuestion: null,
    nextStep: CAMPAIGN_PLANNING_STATES.OUTREACH_STRATEGY_PREVIEW,
    nextStepLabel: 'review outreach strategy preview',
    disclaimer: BATCH_1_APPROVED_DISCLAIMER,
    reviewOnly: true,
    crmWritesMade: false,
    outreachCopyGenerated: false,
    accountChangesMade: false,
    exportMade: false,
    sendsMade: false,
  };
}

function splitDifferentiatorList(text) {
  const raw = String(text || '').trim();
  if (!raw) return [];
  const parts = raw
    .split(/\n|•|\u2022|;|(?<=\.)\s+(?=[A-Z])/)
    .map((p) =>
      p
        .replace(/^customers?\s+choose\s+(?:this\s+business|anchor)\s+for\s+/i, '')
        .replace(/^when\s+a\s+great-fit\s+customer\s+chooses\b[^.]*\.\s*/i, '')
        .replace(/\s+/g, ' ')
        .trim()
        .replace(/\.$/, '')
    )
    .filter((p) => p.length >= 12);
  return parts.slice(0, 5);
}

function normalizeVoiceGuidance(text, businessName) {
  const raw = String(text || '').trim();
  if (!raw) return DEFAULT_ANCHOR_VOICE;
  let s = raw
    .replace(/^brand\s+voice\s+should\s+read\s+as\s+/i, '')
    .replace(/^tone\s+guidance\s+constrains\b[\s\S]*$/i, '')
    .replace(
      new RegExp(
        `^${String(businessName || 'anchor').replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}(?:'?s)?\\s+brand\\s+voice\\s+should\\s+(?:sound|read)\\s+`,
        'i'
      ),
      ''
    )
    .replace(/^as\s+/i, '')
    .replace(/\s+/g, ' ')
    .trim();
  // Drop trailing meta about channels/campaigns.
  s = s
    .replace(/\.\s*Tone guidance constrains[\s\S]*$/i, '')
    .replace(/\.\s*without choosing channels[\s\S]*$/i, '')
    .trim();
  if (s.length < 12) return DEFAULT_ANCHOR_VOICE;
  return s.endsWith('.') ? s : `${s}.`;
}

/**
 * Phrase-safe audience noun for Outreach Strategy wrappers.
 * Never embed raw criteria paragraphs ("Small to mid-sized… who oversee…").
 */
function normalizeOutreachAudiencePhrase(segmentPhrase, ctx = {}) {
  let s = asEmbeddablePhrase(segmentPhrase || '');
  s = s
    .replace(/\bin\s+Greater Manchester(?:\s+NH)?\b.*$/i, '')
    .replace(/\bwho\s+(?:oversee|manage)\b.*$/i, '')
    .replace(/\boverseeing\b.*$/i, '')
    .replace(/\bthat likely need\b.*$/i, '')
    .replace(/\blocal\s+(?=property managers?\b)/i, '')
    .replace(/\s+/g, ' ')
    .trim();
  if (/property managers?/i.test(s) && /small to mid-sized/i.test(s)) {
    return 'small to mid-sized property managers';
  }
  if (/^property managers?\b/i.test(s) || !s) {
    if (/property manager/i.test(String(ctx.primarySegment || segmentPhrase || ''))) {
      return 'small to mid-sized property managers';
    }
    return s || 'the focus segment';
  }
  return s;
}

/**
 * Towns-only market phrase for outreach strategy (no "Start with" / "Keep … in scope").
 */
function normalizeOutreachMarketPhrase(marketBoundPhrase, ctx = {}) {
  const preferred =
    Array.isArray(ctx.towns) && ctx.towns.length
      ? ctx.towns.filter((t) =>
          SYNTHESIS_DEFAULT_TOWNS.some((d) => new RegExp(`^${d}$`, 'i').test(t))
        )
      : [];
  const towns =
    preferred.length >= 2
      ? preferred
      : Array.isArray(ctx.towns) && ctx.towns.length >= 2
        ? ctx.towns.slice(0, 5)
        : [...DEFAULT_TOWNS];

  let fromPhrase = String(marketBoundPhrase || '')
    .replace(/,?\s*with\s+.+?\s+kept in scope\.?$/i, '')
    .replace(/^Start with\s+/i, '')
    .replace(/\.\s*Keep Greater Manchester in scope[\s\S]*$/i, '')
    .replace(/\bKeep Greater Manchester in scope[\s\S]*$/i, '')
    .replace(/\bkeep the first test tight enough to learn quickly\b\.?/gi, '')
    .replace(/[.!?]+$/g, '')
    .replace(/\s+/g, ' ')
    .trim();

  if (containsRawPromptFragment(fromPhrase) || !fromPhrase || fromPhrase.length < 8) {
    return synthesisNaturalList(towns) || naturalList(towns);
  }
  // Prefer canonical town list when the phrase already lists primary towns.
  const listed = DEFAULT_TOWNS.filter((t) =>
    new RegExp(`\\b${t}\\b`, 'i').test(fromPhrase)
  );
  if (listed.length >= 3) {
    return synthesisNaturalList(listed.length >= 5 ? listed : towns);
  }
  return fromPhrase;
}

/**
 * Angle noun phrase from Blueprint differentiators — never a raw paragraph.
 */
function normalizeOutreachAnglePhrase(differentiators, ctx = {}) {
  const blob = [
    ctx.competitiveAdvantages,
    ctx.differentiators,
    ...(Array.isArray(differentiators) ? differentiators : []),
  ]
    .filter(Boolean)
    .join(' ');
  if (/reliab/i.test(blob) && /respons/i.test(blob)) {
    return 'reliability and responsiveness';
  }
  if (/reliab/i.test(blob) && /accountab/i.test(blob)) {
    return 'reliability and accountability';
  }
  if (/respons/i.test(blob) && /accountab/i.test(blob)) {
    return 'responsiveness and accountability';
  }
  return 'reliability and responsiveness';
}

/**
 * First-ask CTA phrase — strategy only, not final copy.
 */
function normalizeOutreachCtaPhrase(ctx = {}, answers = {}, criteria = null) {
  const objectiveBlob = [
    criteria && criteria.campaignObjective,
    answers.campaignObjective,
    ctx.coreValidationQuestion,
  ]
    .filter(Boolean)
    .join(' ');
  if (/walkthrough/i.test(objectiveBlob) || /conversation/i.test(objectiveBlob)) {
    return 'a short conversation or walkthrough to see whether recurring cleaning support is worth discussing';
  }
  return 'a short conversation or walkthrough to see whether recurring cleaning support is worth discussing';
}

function normalizeApprovedBatchPhrase(batch = {}, candidates = []) {
  const count =
    batch.candidateCount != null ? batch.candidateCount : (candidates || []).length;
  if (count === 1) return 'the approved Batch 1 record';
  return 'the approved Batch 1 record';
}

function polishOutreachObjectiveSentence(rawPhrase, ctx = {}, answers = {}) {
  let s = asEmbeddablePhrase(rawPhrase || '');
  if (!s) {
    s = normalizeObjectivePhrase('', {
      primarySegment: ctx.primarySegment,
      coreValidationQuestion: ctx.coreValidationQuestion || answers.coreValidationQuestion,
    });
  }
  if (!s) {
    return defaultObjectiveParagraph(ctx, answers);
  }
  s = s.replace(/[.!?]+$/g, '').trim();
  if (!s) return defaultObjectiveParagraph(ctx, answers);
  const sentence = s.charAt(0).toUpperCase() + s.slice(1);
  return /[.!?]$/.test(sentence) ? sentence : `${sentence}.`;
}

function rejectRawOutreachLines(lines) {
  return (lines || []).map((line) => {
    const text = String(line || '').replace(/\s+/g, ' ').trim();
    if (!text) return text;
    if (containsRawPromptFragment(text) || /(?<!\.)\.\.(?!\.)/.test(text)) {
      // Never emit stitched criteria fragments — fall back to a safe strategy line.
      return 'Keep the first outreach pass tight, reviewable, and validation-oriented.';
    }
    return text.replace(/\.{2,}/g, '.').replace(/\s+/g, ' ').trim();
  });
}

/**
 * Full town list for operator-facing copy — never clipped with ellipses.
 * "Bedford, Hooksett, Londonderry, Auburn, or Goffstown"
 */
function formatTownChoiceList(towns) {
  const list = (Array.isArray(towns) ? towns : [])
    .map((t) => String(t || '').trim())
    .filter(Boolean);
  const unique = [];
  const seen = new Set();
  for (const t of list) {
    const key = t.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    unique.push(t);
  }
  const items = unique.length ? unique : [...DEFAULT_TOWNS];
  if (items.length === 1) return items[0];
  if (items.length === 2) return `${items[0]} or ${items[1]}`;
  return `${items.slice(0, -1).join(', ')}, or ${items[items.length - 1]}`;
}

/** Internal / meta phrasing that must not appear in operator-facing copy-plan sections. */
const OUTREACH_COPY_PLAN_META_RES = Object.freeze([
  /\bcarry forward proof already noted\b/i,
  /\bhold final (?:email|sms|call)\b/i,
  /\bcompetitive edge is described as\b/i,
  /\boperator-stated\b/i,
  /\buntil after strategy approval\b/i,
  /\bdifferentiator to lean on\b/i,
  /\bapproved Blueprint proof assets\b/i,
  /\bdo not invent testimonials\b/i,
]);

function looksLikeOutreachCopyPlanMetaLine(text) {
  const s = String(text || '');
  return OUTREACH_COPY_PLAN_META_RES.some((re) => re.test(s));
}

/**
 * Operator-facing first-touch goal for Outreach Copy Plan section 2.
 * No internal Batch-1-record / hold-scripts / meta strategy language.
 */
function buildOutreachCopyFirstTouchGoal(audiencePhrase) {
  let audience = String(audiencePhrase || '')
    .replace(/\s+/g, ' ')
    .trim();
  if (/property managers?/i.test(audience) || !audience) {
    audience = 'property managers';
  }
  return `Open a low-pressure conversation with approved Batch 1 ${audience} about recurring cleaning support, with the goal of earning a short conversation, walkthrough, or estimate request.`;
}

/**
 * Operator-facing personalization inputs for Outreach Copy Plan section 4.
 * Full town list — no ellipses, no internal process language.
 */
function buildOutreachCopyPersonalizationInputs(towns) {
  return [
    `Prospect town: ${formatTownChoiceList(towns)}.`,
    'Property type or portfolio cue when publicly visible.',
    'Public role or decision-maker title when present.',
    'Any visible signal that reliability, responsiveness, or recurring service may matter.',
  ];
}

/**
 * Operator-facing proof points for Outreach Copy Plan section 5.
 * Never reuse strategy.proofFraming (that section carries planning/meta guardrails).
 */
function buildOutreachCopyProofPoints(businessName) {
  const name = shortBusinessName(businessName || 'the business');
  return [
    'Simple commercial cleaning checklist.',
    'Clear response-time expectation.',
    'Clear service area.',
    'Professional walkthrough / estimate process.',
    'Before/after photos, references, or reviews if available.',
    `${name}'s practical promise: reliable cleaning, responsive communication, and fewer vendor-chasing headaches.`,
  ];
}

/**
 * Flatten Outreach Strategy Preview text for stale-fragment scanning.
 */
function outreachStrategyPreviewTextBlob(preview) {
  if (!preview || typeof preview !== 'object') return '';
  const parts = [
    preview.campaignObjective,
    preview.batch1Scope,
    preview.positioningText,
    preview.voiceTone,
    preview.approachSummary,
    preview.summary,
    ...(Array.isArray(preview.differentiators) ? preview.differentiators : []),
    ...(Array.isArray(preview.outreachApproach) ? preview.outreachApproach : []),
    ...(Array.isArray(preview.proofFraming) ? preview.proofFraming : []),
    preview.targetSegment,
    preview.marketBound,
    preview.outreachAudiencePhrase,
    preview.outreachMarketPhrase,
  ];
  try {
    parts.push(formatOutreachStrategyPreviewMessage(preview));
  } catch (_err) {
    // ignore format errors on partial stubs
  }
  return parts.filter(Boolean).join('\n');
}

/**
 * Detect stored Outreach Strategy Preview artifacts that still contain banned
 * raw-stitched criteria / market fragments from the pre-normalization renderer.
 */
function findStaleOutreachStrategyFragments(preview) {
  const blob = outreachStrategyPreviewTextBlob(preview);
  if (!blob) return [];
  const hits = [];
  for (const re of STALE_OUTREACH_STRATEGY_FRAGMENT_RES) {
    if (re.test(blob)) hits.push(re.source);
  }
  for (const hit of findRawPromptFragments(blob)) {
    if (!hits.includes(hit)) hits.push(hit);
  }
  return hits;
}

function outreachStrategyPreviewLooksStale(preview) {
  if (!hasOutreachStrategyPreview(preview)) return false;
  return findStaleOutreachStrategyFragments(preview).length > 0;
}

/**
 * Rebuild a stale stored Outreach Strategy Preview using phrase-safe synthesis.
 * Preserves Batch 1 / workflow identity fields; never writes CRM or sends.
 */
function repairOutreachStrategyPreview(prior, approvedReview, context, opts = {}) {
  if (!outreachStrategyPreviewLooksStale(prior)) {
    return prior;
  }
  const repaired = buildOutreachStrategyPreview(approvedReview, context, {
    ...opts,
    priorOutreachStrategyPreview: prior,
    reuseExisting: false,
    forceRebuild: true,
  });
  return {
    ...repaired,
    status: 'draft',
    repairedFromStale: true,
    repairedAt: new Date().toISOString(),
    priorGeneratedAt: prior.generatedAt || null,
    workRequestId:
      repaired.workRequestId ||
      prior.workRequestId ||
      (approvedReview && approvedReview.workRequestId) ||
      null,
    blueprintId: repaired.blueprintId || prior.blueprintId || opts.blueprintId || null,
    blueprintVersion:
      repaired.blueprintVersion ||
      prior.blueprintVersion ||
      opts.blueprintVersion ||
      null,
    planningOnly: true,
    reviewFirst: true,
    outreachCopyGenerated: false,
    sendsMade: false,
    crmWritesMade: false,
    exportMade: false,
    accountChangesMade: false,
  };
}

/**
 * Build Outreach Strategy Preview from approved Blueprint, campaign objective,
 * Batch 1 cold prospects, and brand voice/differentiators.
 * Planning angles only — never final outreach copy.
 *
 * Section 5 (Outreach approach) uses Max Synthesis phrase-safe fields only —
 * never concatenates wrapper text with raw prior campaign/criteria answers.
 * Stale stored artifacts with banned fragments are regenerated instead of reused.
 */
function buildOutreachStrategyPreview(approvedReview, context, opts = {}) {
  const prior =
    opts.priorOutreachStrategyPreview &&
    hasOutreachStrategyPreview(opts.priorOutreachStrategyPreview)
      ? opts.priorOutreachStrategyPreview
      : null;
  const priorIsStale = prior ? outreachStrategyPreviewLooksStale(prior) : false;
  if (
    prior &&
    opts.reuseExisting !== false &&
    !opts.forceRebuild &&
    !priorIsStale
  ) {
    return {
      ...prior,
      kind: OUTREACH_STRATEGY_PREVIEW_KIND,
      title: prior.title || OUTREACH_STRATEGY_PREVIEW_TITLE,
      status: prior.status === 'approved' ? 'approved' : 'draft',
      planningOnly: true,
      reviewFirst: true,
      outreachCopyGenerated: false,
      sendsMade: false,
      crmWritesMade: false,
      exportMade: false,
      accountChangesMade: false,
      disclaimer: OUTREACH_STRATEGY_PREVIEW_DISCLAIMER,
    };
  }

  // Stale or forceRebuild: fall through and regenerate with phrase-safe synthesis.

  const ctx = context || {};
  const review = approvedReview || {};
  const batch = review.approvedBatch || {};
  const candidates =
    batch.candidates ||
    review.acceptedFirstPass ||
    [];
  const answers = opts.answers || {};
  const criteria = opts.priorCriteriaPreview || null;
  const campaignPreview = opts.priorPreview || null;

  const synthesis = buildCampaignSynthesisContext({
    context: ctx,
    normalizedFacts: opts.normalizedFacts || null,
    priorCriteriaPreview: criteria,
    priorCampaignPreview: campaignPreview,
    priorArtifact: priorIsStale ? null : prior,
    slots: opts.slots || {},
    answers,
    approvedReview: review,
    state: {
      campaignMemory: opts.campaignMemory || null,
      prospectBatchReview: review,
      reasoningMemory: opts.reasoningMemory || null,
    },
    campaignMemory: opts.campaignMemory || null,
    step: opts.step || CAMPAIGN_PLANNING_STATES.OUTREACH_STRATEGY_PREVIEW,
  });
  const { phrases, facts } = synthesis;
  const name = shortBusinessName(
    facts.businessName || ctx.businessName || 'the business'
  );

  const differentiatorSource =
    ctx.competitiveAdvantages ||
    ctx.differentiators ||
    (prior && !priorIsStale && prior.positioningText) ||
    '';
  const differentiators = (() => {
    const fromBlueprint = splitDifferentiatorList(differentiatorSource);
    if (fromBlueprint.length) return fromBlueprint;
    if (
      prior &&
      Array.isArray(prior.differentiators) &&
      prior.differentiators.length &&
      !prior.differentiators.some((d) => containsRawPromptFragment(d))
    ) {
      return prior.differentiators;
    }
    return [...DEFAULT_ANCHOR_DIFFERENTIATORS];
  })();

  const voiceTone = normalizeVoiceGuidance(
    ctx.brandVoice || (prior && prior.voiceTone) || '',
    name
  );

  const approvedBatchPhrase = normalizeApprovedBatchPhrase(batch, candidates);
  const outreachAudiencePhrase = normalizeOutreachAudiencePhrase(
    phrases.targetSegmentPhrase || facts.targetSegment,
    ctx
  );
  const outreachMarketPhrase = normalizeOutreachMarketPhrase(
    phrases.marketBoundPhrase || facts.marketBound,
    ctx
  );
  const outreachAnglePhrase = normalizeOutreachAnglePhrase(differentiators, ctx);
  const outreachCtaPhrase = normalizeOutreachCtaPhrase(ctx, answers, criteria);

  const campaignObjective = polishOutreachObjectiveSentence(
    phrases.objectivePhrase ||
      facts.campaignObjective ||
      (criteria && criteria.campaignObjective) ||
      (campaignPreview &&
        (campaignPreview.campaignObjective || campaignPreview.objective)) ||
      answers.campaignObjective ||
      (prior && prior.campaignObjective),
    ctx,
    answers
  );

  const towns = Array.isArray(ctx.towns) && ctx.towns.length
    ? ctx.towns
    : [...DEFAULT_TOWNS];

  const batchNames = candidates
    .map((c) => c.companyName || c.company)
    .filter(Boolean);
  const batch1Scope = [
    `${batchNames.length || candidates.length} approved cold first-pass prospects in Batch 1.`,
    batchNames.length
      ? `Accounts: ${batchNames.slice(0, 12).join('; ')}${
          batchNames.length > 12 ? `; +${batchNames.length - 12} more` : ''
        }.`
      : 'Accounts: approved cold first-pass set only.',
    'Cedar / source-verification, existing-relationship nurture, optional expansion, and rejected accounts stay excluded from this cold strategy.',
  ].join(' ');

  const outreachApproach = rejectRawOutreachLines([
    `Lead with ${name}'s ${outreachAnglePhrase} for ${outreachAudiencePhrase} in ${outreachMarketPhrase}.`,
    `Personalize by town, property type, and any public role signal from ${approvedBatchPhrase}.`,
    `Keep the first ask simple: ${outreachCtaPhrase}.`,
    'Treat this as a validation campaign, not a broad launch.',
  ]);

  const proofFraming = rejectRawOutreachLines([
    ctx.proofFromPrior
      ? `Lead with tangible proof already available: ${asEmbeddablePhrase(ctx.proofFromPrior) || 'checklist discipline, response-time expectation, and walkthrough process'}.`
      : 'Lead with tangible proof of reliability (response-time promise, checklist discipline, references, before/after examples) once packaged.',
    'Stay inside approved Blueprint claims — no invented testimonials, pricing, or service promises.',
    'Hold final email/SMS/call scripts until after Outreach Copy Plan approval.',
  ]);

  const guardrails = [
    'No final outreach copy in this step',
    'No sends',
    'No CRM writes',
    'No export',
    'No account, DNS, GBP, social, or tracking changes',
  ];

  const synthesisPhrases = Object.freeze({
    ...phrases,
    approvedBatchPhrase,
    outreachAudiencePhrase,
    outreachMarketPhrase,
    outreachAnglePhrase,
    outreachCtaPhrase,
  });

  const built = {
    kind: OUTREACH_STRATEGY_PREVIEW_KIND,
    title: OUTREACH_STRATEGY_PREVIEW_TITLE,
    status: 'draft',
    businessName: name,
    batchName: batch.name || 'Batch 1',
    approvedCandidateCount:
      batch.candidateCount != null ? batch.candidateCount : candidates.length,
    batchProspects: batchNames,
    campaignObjective,
    batch1Scope,
    positioningText: differentiatorSource || differentiators.join('. '),
    differentiators,
    voiceTone,
    outreachApproach,
    proofFraming,
    guardrails,
    /** Phrase-safe fields — renderers must use these, not raw criteria text. */
    approvedBatchPhrase,
    outreachAudiencePhrase,
    outreachMarketPhrase,
    outreachAnglePhrase,
    outreachCtaPhrase,
    synthesisPhrases,
    targetSegment: outreachAudiencePhrase,
    marketBound: outreachMarketPhrase,
    towns,
    approachSummary: `Review-first outreach strategy for ${name}'s Batch 1 (${
      batch.candidateCount != null ? batch.candidateCount : candidates.length
    } cold prospects) in ${outreachMarketPhrase} — angles and voice only, not live send.`,
    summary: `Review-first outreach strategy for Batch 1 — copy planning only, not live send.`,
    sectionTitles: { ...OUTREACH_STRATEGY_SECTION_TITLES },
    closingQuestion: OUTREACH_STRATEGY_PREVIEW_CLOSING_QUESTION,
    recommendedNextStep:
      'Approve this Outreach Strategy Preview to advance to the Outreach Copy Plan, or name a section to revise. Final outreach copy, sends, CRM writes, exports, and account changes remain blocked.',
    planningOnly: true,
    reviewFirst: true,
    outreachCopyGenerated: false,
    sendsMade: false,
    crmWritesMade: false,
    exportMade: false,
    accountChangesMade: false,
    disclaimer: OUTREACH_STRATEGY_PREVIEW_DISCLAIMER,
    workRequestId:
      opts.workRequestId ||
      (review && review.workRequestId) ||
      (prior && prior.workRequestId) ||
      null,
    blueprintId:
      opts.blueprintId || ctx.blueprintId || (prior && prior.blueprintId) || null,
    blueprintVersion:
      opts.blueprintVersion ||
      ctx.blueprintVersion ||
      (prior && prior.blueprintVersion) ||
      null,
    generatedAt: new Date().toISOString(),
  };

  if (priorIsStale || opts.forceRebuild) {
    built.repairedFromStale = Boolean(priorIsStale);
    if (priorIsStale) {
      built.repairedAt = built.generatedAt;
      built.priorGeneratedAt = prior.generatedAt || null;
    }
  }

  return built;
}

/** @deprecated Use buildOutreachStrategyPreview — kept for callers/tests. */
function buildOutreachStrategyPreviewStub(approvedReview, opts = {}) {
  return buildOutreachStrategyPreview(approvedReview, opts.context || {}, opts);
}

function formatOutreachStrategyPreviewMessage(preview) {
  const p = preview || {};
  const titles = p.sectionTitles || OUTREACH_STRATEGY_SECTION_TITLES;
  const lines = [p.title || OUTREACH_STRATEGY_PREVIEW_TITLE, ''];

  lines.push(`1. ${titles.campaignObjective}`);
  lines.push(p.campaignObjective || '—');
  lines.push('');

  lines.push(`2. ${titles.batch1Scope}`);
  lines.push(p.batch1Scope || '—');
  lines.push('');

  lines.push(`3. ${titles.positioning}`);
  for (const item of p.differentiators || []) lines.push(`- ${item}`);
  if (!(p.differentiators || []).length) {
    lines.push(p.positioningText || '—');
  }
  lines.push('');

  lines.push(`4. ${titles.voiceTone}`);
  lines.push(p.voiceTone || DEFAULT_ANCHOR_VOICE);
  lines.push('');

  lines.push(`5. ${titles.outreachApproach}`);
  for (const item of p.outreachApproach || []) lines.push(`- ${item}`);
  if (!(p.outreachApproach || []).length) lines.push('- —');
  lines.push('');

  lines.push(`6. ${titles.proofFraming}`);
  for (const item of p.proofFraming || []) lines.push(`- ${item}`);
  if (!(p.proofFraming || []).length) lines.push('- —');
  lines.push('');

  lines.push(`7. ${titles.guardrails}`);
  for (const item of p.guardrails || []) lines.push(`- ${item}`);
  if (!(p.guardrails || []).length) lines.push('- —');
  lines.push('');

  lines.push(`8. ${titles.recommendedNextStep}`);
  lines.push(p.recommendedNextStep || '—');
  lines.push('');

  lines.push(p.disclaimer || OUTREACH_STRATEGY_PREVIEW_DISCLAIMER);
  lines.push('');
  lines.push(
    p.closingQuestion || OUTREACH_STRATEGY_PREVIEW_CLOSING_QUESTION
  );
  return lines.join('\n').trim();
}

/**
 * Mark Outreach Strategy Preview approved (immutable snapshot fields).
 */
function approveOutreachStrategyPreview(preview, opts = {}) {
  const prior = preview && typeof preview === 'object' ? preview : {};
  const approvedAt =
    opts.approvedAt || prior.approvedAt || new Date().toISOString();
  return {
    ...prior,
    kind: OUTREACH_STRATEGY_PREVIEW_KIND,
    title: prior.title || OUTREACH_STRATEGY_PREVIEW_TITLE,
    status: 'approved',
    approved: true,
    strategyApproved: true,
    approvedAt,
    planningOnly: true,
    reviewFirst: true,
    outreachCopyGenerated: false,
    sendsMade: false,
    crmWritesMade: false,
    exportMade: false,
    accountChangesMade: false,
    disclaimer: OUTREACH_STRATEGY_PREVIEW_DISCLAIMER,
  };
}

/**
 * Build Outreach Copy Plan from approved Blueprint, campaign objective,
 * Batch 1 cold prospects, approved Outreach Strategy Preview, and brand voice.
 * Planning only — never final outreach copy, sends, CRM writes, or exports.
 * Stale stored artifacts with banned fragments are regenerated instead of reused.
 */
function buildOutreachCopyPlan(approvedStrategy, approvedReview, context, opts = {}) {
  const prior =
    opts.priorOutreachCopyPlan &&
    hasOutreachCopyPlan(opts.priorOutreachCopyPlan)
      ? opts.priorOutreachCopyPlan
      : null;
  const priorIsStale = prior ? outreachCopyPlanLooksStale(prior) : false;
  if (
    prior &&
    opts.reuseExisting !== false &&
    !opts.forceRebuild &&
    !priorIsStale
  ) {
    return {
      ...prior,
      kind: OUTREACH_COPY_PLAN_KIND,
      title: prior.title || OUTREACH_COPY_PLAN_TITLE,
      status: prior.status === 'approved' ? 'approved' : 'draft',
      planningOnly: true,
      reviewFirst: true,
      finalOutreachCopyGenerated: false,
      outreachCopyGenerated: false,
      sendsMade: false,
      crmWritesMade: false,
      exportMade: false,
      accountChangesMade: false,
      disclaimer: OUTREACH_COPY_PLAN_DISCLAIMER,
    };
  }

  // Stale or forceRebuild: fall through and regenerate operator-facing sections.

  const ctx = context || {};
  const strategy = approvedStrategy || {};
  const review = approvedReview || {};
  const batch = review.approvedBatch || {};
  const candidates =
    batch.candidates ||
    review.acceptedFirstPass ||
    [];
  const answers = opts.answers || {};
  const criteria = opts.priorCriteriaPreview || null;
  const campaignPreview = opts.priorPreview || null;

  const synthesis = buildCampaignSynthesisContext({
    context: ctx,
    normalizedFacts: opts.normalizedFacts || null,
    priorCriteriaPreview: criteria,
    priorCampaignPreview: campaignPreview,
    priorArtifact: strategy,
    slots: opts.slots || {},
    answers,
    approvedReview: review,
    approvedOutreachStrategy: strategy,
    state: {
      campaignMemory: opts.campaignMemory || null,
      prospectBatchReview: review,
      outreachStrategyPreview: strategy,
      reasoningMemory: opts.reasoningMemory || null,
    },
    campaignMemory: opts.campaignMemory || null,
    step: opts.step || CAMPAIGN_PLANNING_STATES.OUTREACH_COPY_PLAN,
  });
  const { phrases, facts } = synthesis;
  const name = shortBusinessName(
    facts.businessName ||
      ctx.businessName ||
      strategy.businessName ||
      'the business'
  );

  const differentiators = (() => {
    if (
      Array.isArray(strategy.differentiators) &&
      strategy.differentiators.length
    ) {
      return strategy.differentiators.filter(
        (d) => !containsRawPromptFragment(d)
      );
    }
    const fromBlueprint = splitDifferentiatorList(
      ctx.competitiveAdvantages || ctx.differentiators || ''
    );
    if (fromBlueprint.length) return fromBlueprint;
    return [...DEFAULT_ANCHOR_DIFFERENTIATORS];
  })();

  const voiceTone = normalizeVoiceGuidance(
    strategy.voiceTone || ctx.brandVoice || '',
    name
  );

  const audiencePhrase =
    strategy.outreachAudiencePhrase ||
    normalizeOutreachAudiencePhrase(
      phrases.targetSegmentPhrase || facts.targetSegment,
      ctx
    );
  const marketPhrase =
    strategy.outreachMarketPhrase ||
    normalizeOutreachMarketPhrase(
      phrases.marketBoundPhrase || facts.marketBound,
      ctx
    );
  const anglePhrase =
    strategy.outreachAnglePhrase ||
    normalizeOutreachAnglePhrase(differentiators, ctx);
  const ctaPhrase =
    strategy.outreachCtaPhrase ||
    normalizeOutreachCtaPhrase(ctx, answers, criteria);
  const approvedBatchPhrase =
    strategy.approvedBatchPhrase ||
    normalizeApprovedBatchPhrase(batch, candidates);

  const campaignObjective =
    strategy.campaignObjective ||
    polishOutreachObjectiveSentence(
      phrases.objectivePhrase ||
        facts.campaignObjective ||
        (criteria && criteria.campaignObjective) ||
        answers.campaignObjective,
      ctx,
      answers
    );

  const batchNames = candidates
    .map((c) => c.companyName || c.company)
    .filter(Boolean);
  const towns =
    Array.isArray(strategy.towns) && strategy.towns.length
      ? strategy.towns
      : Array.isArray(ctx.towns) && ctx.towns.length
        ? ctx.towns
        : [...DEFAULT_TOWNS];

  const channelSequence = rejectRawOutreachLines([
    `Email first — short, calm intro aligned to ${name}'s ${anglePhrase} for ${audiencePhrase} in ${marketPhrase}.`,
    'Optional LinkedIn touch only when a public decision-maker profile is clear — still no live send in this step.',
    'Hold phone / SMS until after copy approval and an explicit launch gate.',
  ]);

  const firstTouchGoal = buildOutreachCopyFirstTouchGoal(audiencePhrase);

  const ctaToTest =
    ctaPhrase ||
    'A short discovery conversation about recurring commercial cleaning reliability.';

  // Section 4–5 are operator-facing only: full town lists, no meta/guardrail language.
  // Guardrails live exclusively in approvalGate (section 7).
  const personalizationInputs = rejectRawOutreachLines(
    buildOutreachCopyPersonalizationInputs(towns)
  ).filter((line) => line && !looksLikeOutreachCopyPlanMetaLine(line));

  const proofPoints = rejectRawOutreachLines(
    buildOutreachCopyProofPoints(name)
  ).filter((line) => line && !looksLikeOutreachCopyPlanMetaLine(line));

  const followUpTiming = rejectRawOutreachLines([
    'Follow-up 1 (about 3 business days): restate the same CTA with one fresh personalization detail — still draft-only until approved.',
    'Follow-up 2 (about 7 business days): offer a clear close-the-loop option (reply / book / not now) without pressure.',
    'Purpose: measure opens/replies against the approved validation metrics — not to force a close.',
  ]);

  const approvalGate = rejectRawOutreachLines([
    'Operator must approve this Outreach Copy Plan before any final outreach copy is drafted.',
    'No final email/SMS/call scripts in this step.',
    'No sends, CRM writes, exports, or account/DNS/GBP/social/tracking changes.',
  ]);

  const built = {
    kind: OUTREACH_COPY_PLAN_KIND,
    title: OUTREACH_COPY_PLAN_TITLE,
    status: 'draft',
    businessName: name,
    campaignObjective,
    basedOnStrategyStatus: strategy.status || 'approved',
    basedOnBatchStatus: review.status || 'batch_1_approved',
    approvedCandidateCount:
      batch.candidateCount != null ? batch.candidateCount : candidates.length,
    batchProspects: batchNames,
    channelSequence,
    firstTouchGoal,
    ctaToTest,
    personalizationInputs,
    proofPoints,
    followUpTiming,
    approvalGate,
    differentiators,
    voiceTone,
    approvedBatchPhrase,
    outreachAudiencePhrase: audiencePhrase,
    outreachMarketPhrase: marketPhrase,
    outreachAnglePhrase: anglePhrase,
    outreachCtaPhrase: ctaPhrase,
    towns,
    sectionTitles: { ...OUTREACH_COPY_PLAN_SECTION_TITLES },
    closingQuestion: OUTREACH_COPY_PLAN_CLOSING_QUESTION,
    recommendedNextStep:
      'Approve this Outreach Copy Plan, or name a section to revise. Final outreach copy, sends, CRM writes, exports, and account changes remain blocked until a later explicit gate.',
    summary:
      'Review-first Outreach Copy Plan — sequence and personalization only, not final copy or live send.',
    planningOnly: true,
    reviewFirst: true,
    finalOutreachCopyGenerated: false,
    outreachCopyGenerated: false,
    sendsMade: false,
    crmWritesMade: false,
    exportMade: false,
    accountChangesMade: false,
    disclaimer: OUTREACH_COPY_PLAN_DISCLAIMER,
    workRequestId:
      opts.workRequestId ||
      strategy.workRequestId ||
      (review && review.workRequestId) ||
      (prior && prior.workRequestId) ||
      null,
    blueprintId:
      opts.blueprintId ||
      strategy.blueprintId ||
      ctx.blueprintId ||
      (prior && prior.blueprintId) ||
      null,
    blueprintVersion:
      opts.blueprintVersion ||
      strategy.blueprintVersion ||
      ctx.blueprintVersion ||
      (prior && prior.blueprintVersion) ||
      null,
    generatedAt: new Date().toISOString(),
  };

  if (priorIsStale || opts.forceRebuild) {
    built.repairedFromStale = Boolean(priorIsStale);
    if (priorIsStale) {
      built.repairedAt = built.generatedAt;
      built.priorGeneratedAt = prior.generatedAt || null;
    }
  }

  return built;
}

function formatOutreachCopyPlanMessage(plan) {
  const p = plan || {};
  const titles = p.sectionTitles || OUTREACH_COPY_PLAN_SECTION_TITLES;
  const lines = [p.title || OUTREACH_COPY_PLAN_TITLE, ''];

  lines.push(`1. ${titles.channelSequence}`);
  for (const item of p.channelSequence || []) lines.push(`- ${item}`);
  if (!(p.channelSequence || []).length) lines.push('- —');
  lines.push('');

  lines.push(`2. ${titles.firstTouchGoal}`);
  lines.push(p.firstTouchGoal || '—');
  lines.push('');

  lines.push(`3. ${titles.ctaToTest}`);
  lines.push(p.ctaToTest || '—');
  lines.push('');

  lines.push(`4. ${titles.personalizationInputs}`);
  for (const item of p.personalizationInputs || []) lines.push(`- ${item}`);
  if (!(p.personalizationInputs || []).length) lines.push('- —');
  lines.push('');

  lines.push(`5. ${titles.proofPoints}`);
  for (const item of p.proofPoints || []) lines.push(`- ${item}`);
  if (!(p.proofPoints || []).length) lines.push('- —');
  lines.push('');

  lines.push(`6. ${titles.followUpTiming}`);
  for (const item of p.followUpTiming || []) lines.push(`- ${item}`);
  if (!(p.followUpTiming || []).length) lines.push('- —');
  lines.push('');

  lines.push(`7. ${titles.approvalGate}`);
  for (const item of p.approvalGate || []) lines.push(`- ${item}`);
  if (!(p.approvalGate || []).length) lines.push('- —');
  lines.push('');

  lines.push(p.disclaimer || OUTREACH_COPY_PLAN_DISCLAIMER);
  lines.push('');
  lines.push(p.closingQuestion || OUTREACH_COPY_PLAN_CLOSING_QUESTION);
  return lines.join('\n').trim();
}

/**
 * Approve Outreach Strategy Preview and create/show Outreach Copy Plan.
 * Never re-renders the strategy approval question.
 */
function produceOutreachStrategyApprovalResult(
  ctx,
  answers,
  slots,
  opts,
  leadIn
) {
  const priorStrategy = opts.priorOutreachStrategyPreview || null;
  const priorReview = opts.priorProspectBatchReview || null;
  let review = priorReview;
  if (!isProspectBatchReviewAlreadyApproved(review) && review) {
    review = approveProspectBatchReviewBatch1(review);
  }

  let strategy = priorStrategy;
  if (!hasOutreachStrategyPreview(strategy)) {
    strategy = buildOutreachStrategyPreview(review, ctx, {
      workRequestId: review && review.workRequestId,
      priorCriteriaPreview: opts.priorCriteriaPreview || null,
      priorPreview: opts.priorPreview || null,
      answers,
      blueprintId: opts.blueprintId,
      blueprintVersion: opts.blueprintVersion,
    });
  }
  strategy = approveOutreachStrategyPreview(strategy, {
    approvedAt: opts.approvedAt || new Date().toISOString(),
  });

  return produceOutreachCopyPlanResult(
    ctx,
    answers,
    {
      ...slots,
      previewApproved: true,
      criteriaApproved: true,
      buildProposalApproved: true,
      prospectBatchReviewApproved: true,
      batch1Approved: true,
      outreachStrategyPreviewGenerated: true,
      outreachStrategyPreviewApproved: true,
      strategyApproved: true,
    },
    {
      ...opts,
      priorProspectBatchReview: review,
      priorOutreachStrategyPreview: strategy,
    },
    leadIn || OUTREACH_STRATEGY_APPROVED_MESSAGE
  );
}

/**
 * Create or show Outreach Copy Plan after Outreach Strategy Preview approval.
 * Never re-renders the strategy approval question; never drafts final copy.
 */
function produceOutreachCopyPlanResult(ctx, answers, slots, opts, leadIn) {
  const priorReview = opts.priorProspectBatchReview || null;
  let review = priorReview;
  if (!isProspectBatchReviewAlreadyApproved(review) && review) {
    review = approveProspectBatchReviewBatch1(review);
  }

  let strategy = opts.priorOutreachStrategyPreview || null;
  if (!hasOutreachStrategyPreview(strategy)) {
    if (!review) {
      return {
        message: [
          leadIn || 'Outreach Copy Plan needs an approved Outreach Strategy Preview.',
          '',
          'Approve the Outreach Strategy Preview first.',
          BATCH_1_APPROVED_DISCLAIMER,
        ].join('\n'),
        step: CAMPAIGN_PLANNING_STATES.OUTREACH_STRATEGY_PREVIEW,
        answers,
        slots: { ...slots },
        prospectBatchReview: priorReview,
        outreachStrategyPreview: null,
        outreachCopyPlan: null,
        intent: 'outreach_copy_plan_missing_strategy',
        planningState: CAMPAIGN_PLANNING_STATES.OUTREACH_STRATEGY_PREVIEW,
        currentAsk: OUTREACH_STRATEGY_PREVIEW_CLOSING_QUESTION,
        outreachCopyGenerated: false,
        finalOutreachCopyGenerated: false,
        sendsMade: false,
        crmWritesMade: false,
        exportMade: false,
        accountChangesMade: false,
      };
    }
    strategy = buildOutreachStrategyPreview(review, ctx, {
      workRequestId: review.workRequestId,
      priorCriteriaPreview: opts.priorCriteriaPreview || null,
      priorPreview: opts.priorPreview || null,
      answers,
      blueprintId: opts.blueprintId,
      blueprintVersion: opts.blueprintVersion,
    });
  }
  if (!isOutreachStrategyPreviewAlreadyApproved(strategy)) {
    strategy = approveOutreachStrategyPreview(strategy);
  }

  const existing = opts.priorOutreachCopyPlan || null;
  const alreadyHave = hasOutreachCopyPlan(existing);
  const planWasStale = alreadyHave && outreachCopyPlanLooksStale(existing);
  const outreachCopyPlan = buildOutreachCopyPlan(strategy, review, ctx, {
    workRequestId: (review && review.workRequestId) || strategy.workRequestId,
    priorOutreachCopyPlan: existing,
    priorCriteriaPreview: opts.priorCriteriaPreview || null,
    priorPreview: opts.priorPreview || null,
    answers,
    blueprintId: opts.blueprintId,
    blueprintVersion: opts.blueprintVersion,
    reuseExisting: alreadyHave && !planWasStale,
    forceRebuild: planWasStale,
  });

  const intro = planWasStale
    ? 'Updated the Outreach Copy Plan — repaired stale phrasing from an earlier draft. Showing it for approval or revision. Not re-rendering the Outreach Strategy Preview.'
    : alreadyHave
      ? 'Outreach Copy Plan is already available — showing it for approval or revision. Not re-rendering the Outreach Strategy Preview.'
      : 'Creating the Outreach Copy Plan from the approved Blueprint, campaign objective, Batch 1 cold prospects, Outreach Strategy Preview, and brand voice/differentiators.';

  const message = [
    leadIn || null,
    intro,
    '',
    formatOutreachCopyPlanMessage(outreachCopyPlan),
  ]
    .filter(Boolean)
    .join('\n');

  return {
    message,
    step: CAMPAIGN_PLANNING_STATES.OUTREACH_COPY_PLAN,
    answers,
    slots: {
      ...slots,
      previewApproved: true,
      criteriaApproved: true,
      buildProposalApproved: true,
      prospectBatchReviewApproved: true,
      batch1Approved: true,
      outreachStrategyPreviewGenerated: true,
      outreachStrategyPreviewApproved: true,
      strategyApproved: true,
      outreachCopyPlanGenerated: true,
    },
    preview: opts.priorPreview || null,
    criteriaPreview: opts.priorCriteriaPreview || null,
    buildProposal: opts.priorBuildProposal || null,
    prospectListDraft: opts.priorProspectListDraft || null,
    scoutHandoffBrief: opts.priorScoutHandoffBrief || null,
    scoutHandoff: opts.priorScoutHandoff || null,
    scoutWorkRequest: opts.priorScoutWorkRequest || null,
    scoutCandidateBatch:
      opts.priorScoutCandidateBatch ||
      (review && review.scoutCandidateBatch) ||
      null,
    prospectBatchReview: review,
    outreachStrategyPreview: strategy,
    outreachCopyPlan,
    liveProspectList: null,
    intent: planWasStale
      ? 'repair_outreach_copy_plan'
      : alreadyHave
        ? 'show_outreach_copy_plan'
        : leadIn && /approved/i.test(String(leadIn))
          ? 'outreach_strategy_preview_approved'
          : 'produce_outreach_copy_plan',
    previewApproved: true,
    criteriaApproved: true,
    buildProposalApproved: true,
    prospectBatchReviewApproved: true,
    batch1Approved: true,
    outreachStrategyPreviewApproved: true,
    strategyApproved: true,
    liveSourcingApproved: false,
    planningState: CAMPAIGN_PLANNING_STATES.OUTREACH_COPY_PLAN,
    currentAsk: OUTREACH_COPY_PLAN_CLOSING_QUESTION,
    workRequestId:
      (review && review.workRequestId) || strategy.workRequestId || null,
    outreachCopyGenerated: false,
    finalOutreachCopyGenerated: false,
    sendsMade: false,
    crmWritesMade: false,
    exportMade: false,
    accountChangesMade: false,
    repairedFromStale: Boolean(
      planWasStale || outreachCopyPlan.repairedFromStale
    ),
  };
}

/**
 * Mark Outreach Copy Plan approved (immutable snapshot fields).
 */
function approveOutreachCopyPlan(plan, opts = {}) {
  const prior = plan && typeof plan === 'object' ? plan : {};
  const approvedAt =
    opts.approvedAt || prior.approvedAt || new Date().toISOString();
  return {
    ...prior,
    kind: OUTREACH_COPY_PLAN_KIND,
    title: prior.title || OUTREACH_COPY_PLAN_TITLE,
    status: 'approved',
    approved: true,
    copyPlanApproved: true,
    approvedAt,
    planningOnly: true,
    reviewFirst: true,
    finalOutreachCopyGenerated: false,
    outreachCopyGenerated: false,
    sendsMade: false,
    crmWritesMade: false,
    exportMade: false,
    accountChangesMade: false,
    disclaimer: OUTREACH_COPY_PLAN_DISCLAIMER,
  };
}

/**
 * Flatten text for banned-fragment scanning across operator artifacts.
 */
function operatorArtifactTextBlob(artifact, formatter) {
  if (!artifact || typeof artifact !== 'object') return '';
  const parts = [];
  for (const [key, value] of Object.entries(artifact)) {
    if (value == null) continue;
    if (typeof value === 'string') parts.push(value);
    else if (Array.isArray(value)) {
      for (const item of value) {
        if (typeof item === 'string') parts.push(item);
        else if (item && typeof item === 'object') {
          parts.push(
            item.companyName || '',
            item.personalizationNote || '',
            item.body || '',
            item.label || '',
            item.line || ''
          );
        }
      }
    }
  }
  if (typeof formatter === 'function') {
    try {
      parts.push(formatter(artifact));
    } catch (_err) {
      // ignore format errors on partial stubs
    }
  }
  return parts.filter(Boolean).join('\n');
}

/**
 * Flatten Outreach Copy Plan operator-facing text for stale-fragment scanning.
 * Excludes internal metadata (e.g. approvedBatchPhrase) that is not rendered.
 */
function outreachCopyPlanTextBlob(plan) {
  if (!plan || typeof plan !== 'object') return '';
  const parts = [
    plan.firstTouchGoal,
    plan.ctaToTest,
    plan.campaignObjective,
    plan.summary,
    plan.recommendedNextStep,
    ...(Array.isArray(plan.channelSequence) ? plan.channelSequence : []),
    ...(Array.isArray(plan.personalizationInputs)
      ? plan.personalizationInputs
      : []),
    ...(Array.isArray(plan.proofPoints) ? plan.proofPoints : []),
    ...(Array.isArray(plan.followUpTiming) ? plan.followUpTiming : []),
    ...(Array.isArray(plan.approvalGate) ? plan.approvalGate : []),
  ];
  try {
    parts.push(formatOutreachCopyPlanMessage(plan));
  } catch (_err) {
    // ignore format errors on partial stubs
  }
  return parts.filter(Boolean).join('\n');
}

function findOperatorBannedFragments(blob) {
  const text = String(blob || '');
  if (!text) return [];
  const hits = [];
  for (const re of OPERATOR_BANNED_FRAGMENT_RES) {
    if (re.test(text)) hits.push(re.source);
  }
  for (const hit of findRawPromptFragments(text)) {
    if (!hits.includes(hit)) hits.push(hit);
  }
  return hits;
}

/**
 * Detect stored Outreach Copy Plan artifacts that still contain banned
 * operator-facing fragments from earlier unpolished drafts.
 */
function findStaleOutreachCopyPlanFragments(plan) {
  const blob = outreachCopyPlanTextBlob(plan);
  if (!blob) return [];
  const hits = [];
  for (const re of STALE_OUTREACH_COPY_PLAN_FRAGMENT_RES) {
    if (re.test(blob)) hits.push(re.source);
  }
  for (const hit of findOperatorBannedFragments(blob)) {
    if (!hits.includes(hit)) hits.push(hit);
  }
  for (const re of OUTREACH_COPY_PLAN_META_RES) {
    if (re.test(blob) && !hits.includes(re.source)) hits.push(re.source);
  }
  return hits;
}

function outreachCopyPlanLooksStale(plan) {
  if (!hasOutreachCopyPlan(plan)) return false;
  return findStaleOutreachCopyPlanFragments(plan).length > 0;
}

function repairOutreachCopyPlan(prior, approvedStrategy, approvedReview, context, opts = {}) {
  if (!outreachCopyPlanLooksStale(prior)) return prior;
  const repaired = buildOutreachCopyPlan(approvedStrategy, approvedReview, context, {
    ...opts,
    priorOutreachCopyPlan: prior,
    reuseExisting: false,
    forceRebuild: true,
  });
  return {
    ...repaired,
    status: 'draft',
    repairedFromStale: true,
    repairedAt: repaired.repairedAt || new Date().toISOString(),
    priorGeneratedAt: prior.generatedAt || repaired.priorGeneratedAt || null,
    workRequestId:
      repaired.workRequestId ||
      prior.workRequestId ||
      (approvedReview && approvedReview.workRequestId) ||
      null,
    blueprintId: repaired.blueprintId || prior.blueprintId || opts.blueprintId || null,
    blueprintVersion:
      repaired.blueprintVersion ||
      prior.blueprintVersion ||
      opts.blueprintVersion ||
      null,
    planningOnly: true,
    reviewFirst: true,
    finalOutreachCopyGenerated: false,
    outreachCopyGenerated: false,
    sendsMade: false,
    crmWritesMade: false,
    exportMade: false,
    accountChangesMade: false,
  };
}

/**
 * Flatten Outreach Draft Preview text for stale-fragment / memory scanning.
 * Operator-facing draft sections only — do not scan raw prior objective
 * paragraphs (those may still carry instruction framing like "Prove that").
 */
function outreachDraftPreviewTextBlob(preview) {
  if (!preview || typeof preview !== 'object') return '';
  const parts = [
    preview.firstTouchBody,
    preview.summary,
    ...(Array.isArray(preview.subjectOptions) ? preview.subjectOptions : []),
    ...(Array.isArray(preview.followUpSketch) ? preview.followUpSketch : []),
    ...(Array.isArray(preview.approvalGate) ? preview.approvalGate : []),
    ...(Array.isArray(preview.personalizationByProspect)
      ? preview.personalizationByProspect.map(
          (r) =>
            `${r.companyName || ''} ${r.town || ''} ${r.personalizationNote || ''}`
        )
      : []),
    ...(Array.isArray(preview.batchProspects) ? preview.batchProspects : []),
  ];
  try {
    parts.push(formatOutreachDraftPreviewMessage(preview));
  } catch (_err) {
    // ignore format errors on partial stubs
  }
  return parts.filter(Boolean).join('\n');
}

function findStaleOutreachDraftFragments(preview) {
  const blob = outreachDraftPreviewTextBlob(preview);
  if (!blob) return [];
  const hits = [];
  for (const re of STALE_OUTREACH_DRAFT_FRAGMENT_RES) {
    if (re.test(blob)) hits.push(re.source);
  }
  for (const hit of findOperatorBannedFragments(blob)) {
    if (!hits.includes(hit)) hits.push(hit);
  }
  return hits;
}

/**
 * Build CampaignSynthesisContext for Growth/Campaign artifact renderers.
 */
function buildCampaignContextForRender(approvedReview, context, opts = {}) {
  const ctx = context || {};
  const review = approvedReview || opts.priorProspectBatchReview || null;
  const strategy =
    opts.approvedStrategy ||
    opts.priorOutreachStrategyPreview ||
    null;
  const copyPlan =
    opts.approvedCopyPlan ||
    opts.priorOutreachCopyPlan ||
    null;
  let campaignMemory = ensureCampaignMemory({
    campaignMemory: opts.campaignMemory || null,
  });
  if (review) {
    campaignMemory = applyBatchReviewLearnings(campaignMemory, review);
  }
  if (opts.operatorLearnings) {
    campaignMemory = mergeOperatorLearnings(
      campaignMemory,
      opts.operatorLearnings,
      opts.learningSource || 'explicit'
    );
  }

  return buildCampaignSynthesisContext({
    context: ctx,
    state: {
      campaignMemory,
      prospectBatchReview: review,
      outreachStrategyPreview: strategy,
      outreachCopyPlan: copyPlan,
      reasoningMemory: opts.reasoningMemory || null,
    },
    campaignMemory,
    approvedReview: review,
    approvedOutreachStrategy: strategy,
    approvedOutreachCopyPlan: copyPlan,
    priorCriteriaPreview: opts.priorCriteriaPreview || null,
    priorCampaignPreview: opts.priorPreview || null,
    normalizedFacts: opts.normalizedFacts || null,
    slots: opts.slots || {},
    answers: opts.answers || {},
    currentStep: opts.step || opts.currentStep || null,
    step: opts.step || opts.currentStep || null,
    reasoningMemory: opts.reasoningMemory || null,
    senderIdentity: opts.senderIdentity || null,
    operatorLearnings: opts.operatorLearnings || null,
    blueprint: opts.blueprint || opts.approvedBlueprint || null,
  });
}

function outreachDraftPreviewLooksStale(preview, campaignCtx = null) {
  if (!hasOutreachDraftPreview(preview)) return false;
  if (findStaleOutreachDraftFragments(preview).length > 0) return true;
  if (
    campaignCtx &&
    outreachDraftPreviewConflictsWithCampaignMemory(preview, campaignCtx)
  ) {
    return true;
  }
  return false;
}

function repairOutreachDraftPreview(
  prior,
  approvedCopyPlan,
  approvedStrategy,
  approvedReview,
  context,
  opts = {}
) {
  const campaignCtx =
    opts.campaignSynthesisContext ||
    buildCampaignContextForRender(approvedReview, context, {
      ...opts,
      approvedStrategy,
      approvedCopyPlan,
    });
  if (!outreachDraftPreviewLooksStale(prior, campaignCtx)) {
    return prior;
  }
  const repaired = buildOutreachDraftPreview(
    approvedCopyPlan,
    approvedStrategy,
    approvedReview,
    context,
    {
      ...opts,
      priorOutreachDraftPreview: prior,
      reuseExisting: false,
      forceRebuild: true,
      campaignSynthesisContext: campaignCtx,
      campaignMemory: campaignCtx.campaignMemory,
    }
  );
  return {
    ...repaired,
    status: 'draft',
    repairedFromStale: true,
    repairedAt: new Date().toISOString(),
    priorGeneratedAt: prior.generatedAt || null,
    memoryConflicts: findCampaignMemoryDraftConflicts(prior, campaignCtx),
    workRequestId:
      repaired.workRequestId ||
      prior.workRequestId ||
      (approvedReview && approvedReview.workRequestId) ||
      null,
    blueprintId: repaired.blueprintId || prior.blueprintId || opts.blueprintId || null,
    blueprintVersion:
      repaired.blueprintVersion ||
      prior.blueprintVersion ||
      opts.blueprintVersion ||
      null,
    planningOnly: true,
    reviewFirst: true,
    finalOutreachCopyGenerated: true,
    outreachCopyGenerated: true,
    sendsMade: false,
    crmWritesMade: false,
    exportMade: false,
    accountChangesMade: false,
  };
}

/**
 * Build Outreach Draft Preview after Copy Plan approval.
 * Consumes CampaignSynthesisContext (approved facts + durable operator
 * learnings) — never raw prior text alone. Draft copy for review only —
 * never sends, CRM writes, or exports.
 */
function buildOutreachDraftPreview(
  approvedCopyPlan,
  approvedStrategy,
  approvedReview,
  context,
  opts = {}
) {
  const campaignCtx =
    opts.campaignSynthesisContext ||
    buildCampaignContextForRender(approvedReview, context, {
      ...opts,
      approvedStrategy,
      approvedCopyPlan,
    });

  const prior =
    opts.priorOutreachDraftPreview &&
    hasOutreachDraftPreview(opts.priorOutreachDraftPreview)
      ? opts.priorOutreachDraftPreview
      : null;
  const priorIsStale = prior
    ? outreachDraftPreviewLooksStale(prior, campaignCtx)
    : false;
  if (prior && opts.reuseExisting !== false && !opts.forceRebuild && !priorIsStale) {
    return {
      ...prior,
      kind: OUTREACH_DRAFT_PREVIEW_KIND,
      title: prior.title || OUTREACH_DRAFT_PREVIEW_TITLE,
      status: prior.status === 'approved' ? 'approved' : 'draft',
      planningOnly: true,
      reviewFirst: true,
      sendsMade: false,
      crmWritesMade: false,
      exportMade: false,
      accountChangesMade: false,
      disclaimer: OUTREACH_DRAFT_PREVIEW_DISCLAIMER,
      campaignMemory: campaignCtx.campaignMemory,
    };
  }

  const ctx = context || {};
  const plan = approvedCopyPlan || {};
  const strategy = approvedStrategy || {};
  const review = approvedReview || {};
  const batch = review.approvedBatch || {};
  const rawCandidates =
    batch.candidates || review.acceptedFirstPass || [];
  const { coldCandidates: candidates } =
    campaignCtx.filterColdBatchCandidates(rawCandidates);

  const name = shortBusinessName(
    campaignCtx.businessName ||
      plan.businessName ||
      strategy.businessName ||
      ctx.businessName ||
      'the business'
  );
  const audience =
    plan.outreachAudiencePhrase ||
    strategy.outreachAudiencePhrase ||
    campaignCtx.phrase('targetSegmentPhrase') ||
    'property managers';
  const market =
    plan.outreachMarketPhrase ||
    strategy.outreachMarketPhrase ||
    campaignCtx.phrase('marketBoundPhrase') ||
    'Greater Manchester';
  const cta =
    plan.ctaToTest ||
    plan.outreachCtaPhrase ||
    strategy.outreachCtaPhrase ||
    'a short discovery conversation about recurring commercial cleaning reliability';
  const towns =
    Array.isArray(plan.towns) && plan.towns.length
      ? plan.towns
      : Array.isArray(strategy.towns) && strategy.towns.length
        ? strategy.towns
        : Array.isArray(ctx.towns) && ctx.towns.length
          ? ctx.towns
          : [...DEFAULT_TOWNS];

  const subjectResolution = campaignCtx.resolveSubjectLines();
  const subjectOptions = rejectRawOutreachLines(
    subjectResolution.subjectOptions
  ).filter((line) => line && !looksLikeOutreachCopyPlanMetaLine(line));

  const voiceLine = campaignCtx.resolveSenderVoiceLine(audience);
  const differentiator =
    (campaignCtx.learnings && campaignCtx.learnings.copy_differentiator) ||
    'reliability, responsiveness, accountability, and fewer vendor-chasing headaches';
  const firstTouchBody = [
    `Hi {{first_name}},`,
    '',
    voiceLine.opener,
    '',
    `${name} focuses on clear response-time expectations, a simple cleaning checklist, and a professional walkthrough / estimate process — emphasizing ${differentiator}.`,
    '',
    `Would you be open to ${cta}?`,
    '',
    `Best,`,
    `{{sender_name}}`,
    `${name}`,
  ].join('\n');

  const personalizationByProspect = candidates.map((c) => {
    const company = c.companyName || c.company || 'Prospect';
    const town =
      c.town ||
      c.city ||
      (c.location && String(c.location).split(',')[0]) ||
      null;
    const role = c.jobTitle || c.role || c.contactTitle || null;
    let personalizationNote = campaignCtx.buildPersonalizationNote(c);
    if (rejectsStreetAddressPersonalization(personalizationNote)) {
      personalizationNote =
        'Reference {{town}}, company, property type, portfolio cue, or public role signal only — never a street address by default.';
    }
    return {
      companyName: company,
      town: town || null,
      role: role || null,
      personalizationNote,
    };
  });

  const wantFollowUpDrafts =
    Boolean(
      campaignCtx.learnings &&
        (campaignCtx.learnings.draft_follow_ups === true ||
          campaignCtx.learnings.follow_up_mode === 'drafted_emails')
    ) || opts.draftFollowUps === true;

  const subjectForFollowUps =
    subjectOptions[0] ||
    (campaignCtx.learnings &&
      campaignCtx.learnings.tested_subject_line_pattern) ||
    '{{business_name}} - commercial cleaning';

  let followUpDrafts = null;
  let followUpSketch;
  if (wantFollowUpDrafts) {
    followUpDrafts = buildFollowUpEmailDrafts({
      businessName: name,
      audience,
      cta,
      subject: subjectForFollowUps,
      differentiator,
    });
    followUpSketch = followUpDrafts.map(
      (d) =>
        `${d.label} (${d.timing}): drafted email — subject "${d.subject}". Sends held until Outreach Launch Gate.`
    );
  } else {
    followUpSketch = rejectRawOutreachLines([
      'Follow-up 1 (~3 business days): restate the same CTA with one fresh personalization detail.',
      'Follow-up 2 (~7 business days): offer a clear close-the-loop option (reply / book / not now).',
      'Hold all follow-up sends until after Outreach Launch Gate approval and an explicit launch action.',
    ]);
  }

  const approvalGate = rejectRawOutreachLines([
    'Operator must approve this Outreach Draft Preview before the Outreach Launch Gate.',
    'No sends, CRM writes, exports, or account/DNS/GBP/social/tracking changes in this step.',
    'Launch/export/CRM remains blocked until the explicit Outreach Launch Gate is approved and a separate execute action is taken.',
  ]);

  const includedNames = personalizationByProspect
    .map((p) => p.companyName)
    .filter(Boolean);

  const subjectSectionLabel = subjectResolution.sectionTitle
    ? subjectResolution.sectionTitle
    : OUTREACH_DRAFT_PREVIEW_SECTION_TITLES.subjectOptions;

  const whyRecommended = [
    'Copy Plan is approved — drafts stay inside Batch 1, approved voice, and proof points.',
    'Personalization notes are town/company/role/portfolio cues only — no street addresses by default.',
  ];
  if (subjectResolution.claimTestedWinner && subjectResolution.performance) {
    whyRecommended.unshift(
      `Subject line uses tested winner (${subjectResolution.performance}).`
    );
  }

  const responseMode =
    opts.responseMode ||
    (campaignCtx.learnings &&
      campaignCtx.learnings.response_mode_preference) ||
    RESPONSE_MODES.WORKFLOW_REVIEW_CARD;

  const useWorkflowCard =
    responseMode === RESPONSE_MODES.WORKFLOW_REVIEW_CARD &&
    opts.includeOperatorDigest !== false;

  const operatorDigest = useWorkflowCard
    ? buildOperatorReviewDigest({
        kind: 'outreach_draft_preview_digest',
        title: OUTREACH_DRAFT_PREVIEW_TITLE,
        recommendedDecision: `Approve draft first-touch copy for ${
          includedNames.length || candidates.length
        } Batch 1 cold prospects.`,
        included: [
          `${includedNames.length || candidates.length} first-touch email drafts (shared body + per-prospect personalization notes)`,
          subjectResolution.claimTestedWinner
            ? `Subject line (tested winner): ${subjectOptions[0] || '—'}`
            : `Subject line: ${subjectOptions[0] || '—'}`,
          wantFollowUpDrafts
            ? 'Follow-up 1 and Follow-up 2 drafted emails (sends held)'
            : 'Follow-up sketches (sends held)',
          `CTA: ${cta}`,
        ],
        heldBack: [
          'No live sends',
          'No CRM writes or export',
          'Follow-up sends held until Outreach Launch Gate',
          'Cedar / Keyrenter / optional expansion / rejected candidates remain excluded',
        ],
        whyRecommended,
        nextStepAfterApproval: OUTREACH_LAUNCH_GATE_TITLE,
        primaryActions: [
          {
            id: 'approve_draft_preview',
            label: 'Approve Draft Preview',
            style: 'primary',
          },
          {
            id: 'revise_draft_section',
            label: 'Revise a section',
            style: 'secondary',
          },
        ],
        sectionOrder: [
          'recommendedDecision',
          'included',
          'excluded',
          'whyRecommended',
          'nextStepAfterApproval',
          'primaryActions',
        ],
        sectionTitles: {
          excluded: 'Held back',
        },
        evidence: {
          records: personalizationByProspect,
          sections: [
            { title: subjectSectionLabel, lines: subjectOptions },
            { title: 'First-touch body', lines: firstTouchBody.split('\n') },
          ],
        },
        disclaimer: OUTREACH_DRAFT_PREVIEW_DISCLAIMER,
      })
    : null;

  const sectionTitles = {
    ...OUTREACH_DRAFT_PREVIEW_SECTION_TITLES,
    subjectOptions: subjectSectionLabel,
    followUpSketch: wantFollowUpDrafts
      ? 'Follow-up drafts (held until launch gate)'
      : OUTREACH_DRAFT_PREVIEW_SECTION_TITLES.followUpSketch,
  };

  return {
    kind: OUTREACH_DRAFT_PREVIEW_KIND,
    title: OUTREACH_DRAFT_PREVIEW_TITLE,
    status: 'draft',
    businessName: name,
    campaignObjective:
      campaignCtx.phrase('objectivePhrase') ||
      (campaignCtx.approved && campaignCtx.approved.approvedCampaignObjective) ||
      plan.campaignObjective ||
      strategy.campaignObjective ||
      null,
    basedOnCopyPlanStatus: plan.status || 'approved',
    basedOnStrategyStatus: strategy.status || 'approved',
    basedOnBatchStatus: review.status || 'batch_1_approved',
    approvedCandidateCount:
      candidates.length != null ? candidates.length : includedNames.length,
    batchProspects: includedNames,
    subjectOptions,
    usedTestedSubjectLine: Boolean(subjectResolution.usedTestedWinner),
    keptMergeTokens: Boolean(subjectResolution.keptMergeTokens),
    claimTestedWinner: Boolean(subjectResolution.claimTestedWinner),
    testedSubjectLinePattern:
      (campaignCtx.learnings &&
        campaignCtx.learnings.tested_subject_line_pattern) ||
      null,
    testedSubjectLinePerformance: subjectResolution.claimTestedWinner
      ? (campaignCtx.learnings &&
          campaignCtx.learnings.tested_subject_line_performance) ||
        null
      : null,
    firstTouchBody,
    firstTouchDraft: { body: firstTouchBody, cta },
    personalizationByProspect,
    followUpSketch,
    followUpDrafts: followUpDrafts || undefined,
    approvalGate,
    towns,
    outreachAudiencePhrase: audience,
    outreachMarketPhrase: market,
    ctaToTest: cta,
    operatorDigest,
    responseMode,
    sectionTitles,
    closingQuestion: OUTREACH_DRAFT_PREVIEW_CLOSING_QUESTION,
    recommendedNextStep:
      'Approve this Outreach Draft Preview to advance to the Outreach Launch Gate, or name a section to revise. Sends, CRM writes, exports, and account changes remain blocked.',
    summary:
      'Review-first Outreach Draft Preview — draft copy only, not live send.',
    campaignMemory: campaignCtx.campaignMemory,
    campaignLearnings: { ...campaignCtx.learnings },
    workflowStep:
      (campaignCtx.workflow && campaignCtx.workflow.currentStep) || null,
    nextAllowedArtifact:
      (campaignCtx.workflow && campaignCtx.workflow.nextAllowedArtifact) ||
      null,
    planningOnly: true,
    reviewFirst: true,
    finalOutreachCopyGenerated: true,
    outreachCopyGenerated: true,
    sendsMade: false,
    crmWritesMade: false,
    exportMade: false,
    accountChangesMade: false,
    disclaimer: OUTREACH_DRAFT_PREVIEW_DISCLAIMER,
    workRequestId:
      opts.workRequestId ||
      plan.workRequestId ||
      strategy.workRequestId ||
      (review && review.workRequestId) ||
      null,
    blueprintId:
      opts.blueprintId || plan.blueprintId || strategy.blueprintId || null,
    blueprintVersion:
      opts.blueprintVersion ||
      plan.blueprintVersion ||
      strategy.blueprintVersion ||
      null,
    generatedAt: new Date().toISOString(),
  };
}
function formatOutreachDraftPreviewMessage(preview) {
  const p = preview || {};
  if (
    p.responseMode === RESPONSE_MODES.OPERATOR_CHAT_RESPONSE ||
    (!p.operatorDigest && p.followUpDrafts)
  ) {
    return formatOperatorChatDraftResponse(p, {
      changes: p.operatorChanges || [],
      acknowledgment: p.operatorAcknowledgment || null,
      closingQuestion: p.closingQuestion || OUTREACH_DRAFT_PREVIEW_CLOSING_QUESTION,
    });
  }

  const digest = p.operatorDigest || null;
  const lines = [];
  if (digest) {
    lines.push(
      formatOperatorReviewArtifactMessage(digest, {
        includeEvidence: false,
        closingQuestion: null,
      })
    );
    lines.push('');
    lines.push(VIEW_EVIDENCE_LABEL);
    lines.push('');
  } else {
    lines.push(p.title || OUTREACH_DRAFT_PREVIEW_TITLE, '');
  }

  const titles = p.sectionTitles || OUTREACH_DRAFT_PREVIEW_SECTION_TITLES;
  lines.push(`1. ${titles.subjectOptions}`);
  for (const item of p.subjectOptions || []) lines.push(`- ${item}`);
  if (!(p.subjectOptions || []).length) lines.push('- —');
  lines.push('');

  lines.push(`2. ${titles.firstTouchBody}`);
  lines.push(p.firstTouchBody || (p.firstTouchDraft && p.firstTouchDraft.body) || '—');
  lines.push('');

  lines.push(`3. ${titles.personalizationByProspect}`);
  for (const row of p.personalizationByProspect || []) {
    lines.push(
      `- ${row.companyName}${row.town ? ` (${row.town})` : ''}: ${
        row.personalizationNote || '—'
      }`
    );
  }
  if (!(p.personalizationByProspect || []).length) lines.push('- —');
  lines.push('');

  if (Array.isArray(p.followUpDrafts) && p.followUpDrafts.length) {
    lines.push(`4. ${titles.followUpSketch || 'Follow-up drafts (held until launch gate)'}`);
    for (const d of p.followUpDrafts) {
      lines.push(
        `- ${d.label || `Follow-up ${d.step}`}${d.timing ? ` (${d.timing})` : ''}:`
      );
      if (d.subject) lines.push(`  Subject: ${d.subject}`);
      for (const bodyLine of String(d.body || '').split('\n')) {
        lines.push(`  ${bodyLine}`);
      }
    }
    lines.push('');
  } else {
    lines.push(`4. ${titles.followUpSketch}`);
    for (const item of p.followUpSketch || []) lines.push(`- ${item}`);
    if (!(p.followUpSketch || []).length) lines.push('- —');
    lines.push('');
  }

  lines.push(`5. ${titles.approvalGate}`);
  for (const item of p.approvalGate || []) lines.push(`- ${item}`);
  if (!(p.approvalGate || []).length) lines.push('- —');
  lines.push('');

  lines.push(p.disclaimer || OUTREACH_DRAFT_PREVIEW_DISCLAIMER);
  lines.push('');
  lines.push(p.closingQuestion || OUTREACH_DRAFT_PREVIEW_CLOSING_QUESTION);
  return lines.join('\n').trim();
}

function approveOutreachDraftPreview(preview, opts = {}) {
  const prior = preview && typeof preview === 'object' ? preview : {};
  const approvedAt =
    opts.approvedAt || prior.approvedAt || new Date().toISOString();
  return {
    ...prior,
    kind: OUTREACH_DRAFT_PREVIEW_KIND,
    title: prior.title || OUTREACH_DRAFT_PREVIEW_TITLE,
    status: 'approved',
    approved: true,
    draftPreviewApproved: true,
    approvedAt,
    planningOnly: true,
    reviewFirst: true,
    finalOutreachCopyGenerated: true,
    outreachCopyGenerated: true,
    sendsMade: false,
    crmWritesMade: false,
    exportMade: false,
    accountChangesMade: false,
    disclaimer: OUTREACH_DRAFT_PREVIEW_DISCLAIMER,
  };
}

/**
 * Build Outreach Launch Gate after Draft Preview approval.
 * Readiness only — never auto-executes send/export/CRM.
 */
function buildOutreachLaunchGate(
  approvedDraft,
  approvedCopyPlan,
  approvedStrategy,
  approvedReview,
  context,
  opts = {}
) {
  const prior =
    opts.priorOutreachLaunchGate &&
    hasOutreachLaunchGate(opts.priorOutreachLaunchGate)
      ? opts.priorOutreachLaunchGate
      : null;
  if (prior && opts.reuseExisting !== false && !opts.forceRebuild) {
    const alreadyApproved = isOutreachLaunchGateAlreadyApproved(prior);
    const stale =
      !alreadyApproved &&
      findOperatorBannedFragments(
        operatorArtifactTextBlob(prior, formatOutreachLaunchGateMessage)
      ).length > 0;
    if (!stale) {
      return {
        ...prior,
        kind: OUTREACH_LAUNCH_GATE_KIND,
        title: prior.title || OUTREACH_LAUNCH_GATE_TITLE,
        status: alreadyApproved
          ? OUTREACH_LAUNCH_GATE_APPROVED_STATUS
          : 'draft',
        approved: alreadyApproved || prior.approved === true,
        launchGateApproved: alreadyApproved || prior.launchGateApproved === true,
        launchReady: alreadyApproved || prior.launchReady === true,
        planningOnly: true,
        reviewFirst: !alreadyApproved,
        launched: false,
        sendsMade: false,
        crmWritesMade: false,
        exportMade: false,
        accountChangesMade: false,
        disclaimer: OUTREACH_LAUNCH_GATE_DISCLAIMER,
        ...(alreadyApproved
          ? {
              operatorDigest: null,
              operatorStateSummary: buildOutreachLaunchGateOperatorStateSummary(
                prior
              ),
              closingQuestion: null,
            }
          : {}),
      };
    }
  }

  const draft = approvedDraft || {};
  const plan = approvedCopyPlan || {};
  const strategy = approvedStrategy || {};
  const review = approvedReview || {};
  const batch = review.approvedBatch || {};
  const count =
    draft.approvedCandidateCount != null
      ? draft.approvedCandidateCount
      : (batch.candidates || review.acceptedFirstPass || []).length;
  const name = shortBusinessName(
    draft.businessName ||
      plan.businessName ||
      strategy.businessName ||
      (context && context.businessName) ||
      'the business'
  );

  const readinessChecklist = [
    `Batch 1 cold list locked (${count} prospects) — Cedar, Keyrenter, optional expansion, and rejected accounts excluded.`,
    'Outreach Strategy Preview approved.',
    'Outreach Copy Plan approved.',
    'Outreach Draft Preview approved.',
    'Sender identity, reply-to, and tracking remain unchanged until an explicit execute action.',
  ];

  const blockedActions = [
    'No sends or scheduled launches without an explicit execute action after this gate.',
    'No CRM writes without an explicit execute action after this gate.',
    'No export without an explicit execute action after this gate.',
    'No account, DNS, GBP, social, or tracking changes.',
  ];

  const operatorDigest = buildOperatorReviewDigest({
    kind: 'outreach_launch_gate_digest',
    title: OUTREACH_LAUNCH_GATE_TITLE,
    recommendedDecision:
      'Hold launch until you explicitly approve readiness — then take a separate execute action for send, export, or CRM.',
    included: [
      `${name} Batch 1 draft copy ready for gated launch`,
      `${count} cold prospects in scope`,
      'Approved strategy + copy plan + draft preview behind this gate',
    ],
    heldBack: blockedActions,
    whyRecommended: [
      'Keeps launch/export/CRM behind an explicit final checkpoint.',
      'Approval marks readiness only — it does not send, write CRM, or export.',
    ],
    nextStepAfterApproval:
      'Explicit launch / export / CRM execute action (still blocked until you take it)',
    primaryActions: [
      {
        id: 'approve_launch_gate',
        label: 'Approve Launch Gate (readiness only)',
        style: 'primary',
      },
      { id: 'hold_launch', label: 'Hold — do not approve yet', style: 'secondary' },
    ],
    sectionOrder: [
      'recommendedDecision',
      'included',
      'excluded',
      'whyRecommended',
      'nextStepAfterApproval',
      'primaryActions',
    ],
    sectionTitles: {
      excluded: 'Still blocked',
    },
    evidence: {
      sections: [
        { title: 'Readiness checklist', lines: readinessChecklist },
        { title: 'Blocked until explicit execute', lines: blockedActions },
      ],
    },
    disclaimer: OUTREACH_LAUNCH_GATE_DISCLAIMER,
  });

  return {
    kind: OUTREACH_LAUNCH_GATE_KIND,
    title: OUTREACH_LAUNCH_GATE_TITLE,
    status: 'draft',
    businessName: name,
    approvedCandidateCount: count,
    readinessChecklist,
    blockedActions,
    operatorDigest,
    closingQuestion: OUTREACH_LAUNCH_GATE_CLOSING_QUESTION,
    recommendedNextStep:
      'Approve this Outreach Launch Gate for readiness only, or hold. Even after approval, sends/CRM/export require a separate explicit execute action.',
    summary:
      'Explicit launch/export/CRM gate — readiness checkpoint only, not auto-execute.',
    planningOnly: true,
    reviewFirst: true,
    launchReady: false,
    launchGateApproved: false,
    launched: false,
    sendsMade: false,
    crmWritesMade: false,
    exportMade: false,
    accountChangesMade: false,
    disclaimer: OUTREACH_LAUNCH_GATE_DISCLAIMER,
    workRequestId:
      opts.workRequestId ||
      draft.workRequestId ||
      plan.workRequestId ||
      strategy.workRequestId ||
      (review && review.workRequestId) ||
      null,
    generatedAt: new Date().toISOString(),
  };
}

function buildOutreachLaunchGateOperatorStateSummary(gate, opts = {}) {
  const g = gate || {};
  return {
    kind: 'operator_state_summary',
    conversationMode: CONVERSATION_MODES.OPERATOR_STATE_UPDATE,
    title: g.title || OUTREACH_LAUNCH_GATE_TITLE,
    status: OUTREACH_LAUNCH_GATE_APPROVED_STATUS,
    headline: OUTREACH_LAUNCH_GATE_APPROVED_HEADLINE,
    readinessOnly: true,
    executionLockActive: true,
    notExecuted: [
      'No send executed',
      'No export executed',
      'No CRM write executed',
      'No scheduled launch executed',
      'No account, DNS, GBP, social, or tracking changes executed',
    ],
    nextOptions: [...OUTREACH_LAUNCH_GATE_NEXT_OPTIONS],
    nextOptionsRequireApproval: true,
    reminder: OUTREACH_LAUNCH_GATE_OPERATOR_GUIDANCE,
    includeEvidence: opts.includeEvidence === true,
  };
}

/**
 * Approved-state summary — conversational Operator State Update,
 * never the pre-approval Launch Gate review card.
 */
function formatOutreachLaunchGateApprovedSummary(gate, opts = {}) {
  const composed = formatApprovedLaunchGateConversational(gate, {
    justApproved: opts.justApproved === true,
    gateAlreadyApproved:
      opts.gateAlreadyApproved === true ||
      (opts.justApproved !== true &&
        isOutreachLaunchGateAlreadyApproved(gate)),
    stateChanged: opts.stateChanged === true || opts.justApproved === true,
    leadIn: opts.leadIn || null,
    nextPaths: OUTREACH_LAUNCH_GATE_NEXT_OPTIONS.slice(),
    operatorGuidance:
      opts.operatorGuidance || OUTREACH_LAUNCH_GATE_OPERATOR_GUIDANCE,
    closingAsk: opts.closingAsk || OUTREACH_LAUNCH_GATE_APPROVED_ASK,
  });
  return composed.message;
}

function formatOutreachLaunchGateMessage(gate, opts = {}) {
  const g = gate || {};
  if (isOutreachLaunchGateAlreadyApproved(g)) {
    return formatOutreachLaunchGateApprovedSummary(g, opts);
  }

  const lines = [];
  if (g.operatorDigest) {
    lines.push(
      formatOperatorReviewArtifactMessage(g.operatorDigest, {
        includeEvidence: false,
        closingQuestion: null,
      })
    );
    lines.push('');
    lines.push(VIEW_EVIDENCE_LABEL);
    lines.push('');
  } else {
    lines.push(g.title || OUTREACH_LAUNCH_GATE_TITLE, '');
  }

  lines.push('1. Readiness checklist');
  for (const item of g.readinessChecklist || []) lines.push(`- ${item}`);
  if (!(g.readinessChecklist || []).length) lines.push('- —');
  lines.push('');

  lines.push('2. Still blocked until explicit execute');
  for (const item of g.blockedActions || []) lines.push(`- ${item}`);
  if (!(g.blockedActions || []).length) lines.push('- —');
  lines.push('');

  lines.push(g.disclaimer || OUTREACH_LAUNCH_GATE_DISCLAIMER);
  lines.push('');
  lines.push(g.closingQuestion || OUTREACH_LAUNCH_GATE_CLOSING_QUESTION);
  return lines.join('\n').trim();
}

function approveOutreachLaunchGate(gate, opts = {}) {
  const prior = gate && typeof gate === 'object' ? gate : {};
  const approvedAt =
    opts.approvedAt || prior.approvedAt || new Date().toISOString();
  const operatorStateSummary = buildOutreachLaunchGateOperatorStateSummary(
    prior,
    opts
  );
  return {
    ...prior,
    kind: OUTREACH_LAUNCH_GATE_KIND,
    title: prior.title || OUTREACH_LAUNCH_GATE_TITLE,
    status: OUTREACH_LAUNCH_GATE_APPROVED_STATUS,
    approved: true,
    launchGateApproved: true,
    launchReady: true,
    launched: false,
    approvedAt,
    planningOnly: true,
    reviewFirst: false,
    // Suppress pre-approval workflow card payload after approval.
    operatorDigest: null,
    closingQuestion: null,
    operatorStateSummary,
    // Guardrails: approval = readiness only — never auto-execute.
    sendsMade: false,
    crmWritesMade: false,
    exportMade: false,
    accountChangesMade: false,
    disclaimer: OUTREACH_LAUNCH_GATE_DISCLAIMER,
  };
}

/**
 * Approve Outreach Copy Plan and create/show Outreach Draft Preview.
 */
function produceOutreachCopyPlanApprovalResult(
  ctx,
  answers,
  slots,
  opts,
  leadIn
) {
  const priorPlan = opts.priorOutreachCopyPlan || null;
  const priorStrategy = opts.priorOutreachStrategyPreview || null;
  const priorReview = opts.priorProspectBatchReview || null;
  let review = priorReview;
  if (!isProspectBatchReviewAlreadyApproved(review) && review) {
    review = approveProspectBatchReviewBatch1(review);
  }
  let strategy = priorStrategy;
  if (!hasOutreachStrategyPreview(strategy) && review) {
    strategy = buildOutreachStrategyPreview(review, ctx, {
      priorCriteriaPreview: opts.priorCriteriaPreview || null,
      priorPreview: opts.priorPreview || null,
      answers,
    });
  }
  if (strategy && !isOutreachStrategyPreviewAlreadyApproved(strategy)) {
    strategy = approveOutreachStrategyPreview(strategy);
  }

  let plan = priorPlan;
  if (!hasOutreachCopyPlan(plan) && strategy) {
    plan = buildOutreachCopyPlan(strategy, review, ctx, {
      priorCriteriaPreview: opts.priorCriteriaPreview || null,
      priorPreview: opts.priorPreview || null,
      answers,
    });
  }
  if (plan && outreachCopyPlanLooksStale(plan)) {
    plan = repairOutreachCopyPlan(plan, strategy, review, ctx, {
      priorCriteriaPreview: opts.priorCriteriaPreview || null,
      answers,
    });
  }
  plan = approveOutreachCopyPlan(plan || {}, {
    approvedAt: opts.approvedAt || new Date().toISOString(),
  });

  return produceOutreachDraftPreviewResult(
    ctx,
    answers,
    {
      ...slots,
      outreachStrategyPreviewApproved: true,
      strategyApproved: true,
      outreachCopyPlanGenerated: true,
      outreachCopyPlanApproved: true,
      copyPlanApproved: true,
    },
    {
      ...opts,
      priorProspectBatchReview: review,
      priorOutreachStrategyPreview: strategy,
      priorOutreachCopyPlan: plan,
    },
    leadIn || OUTREACH_COPY_PLAN_APPROVED_MESSAGE
  );
}

/**
 * Create or show Outreach Draft Preview after Copy Plan approval.
 */
function produceOutreachDraftPreviewResult(ctx, answers, slots, opts, leadIn) {
  const priorReview = opts.priorProspectBatchReview || null;
  let review = priorReview;
  if (!isProspectBatchReviewAlreadyApproved(review) && review) {
    review = approveProspectBatchReviewBatch1(review);
  }

  let strategy = opts.priorOutreachStrategyPreview || null;
  if (!hasOutreachStrategyPreview(strategy) && review) {
    strategy = buildOutreachStrategyPreview(review, ctx, {
      priorCriteriaPreview: opts.priorCriteriaPreview || null,
      priorPreview: opts.priorPreview || null,
      answers,
    });
  }
  if (strategy && !isOutreachStrategyPreviewAlreadyApproved(strategy)) {
    strategy = approveOutreachStrategyPreview(strategy);
  }

  let plan = opts.priorOutreachCopyPlan || null;
  if (!hasOutreachCopyPlan(plan)) {
    if (!strategy) {
      return {
        message: [
          leadIn || 'Outreach Draft Preview needs an approved Outreach Copy Plan.',
          '',
          'Approve the Outreach Copy Plan first.',
          BATCH_1_APPROVED_DISCLAIMER,
        ].join('\n'),
        step: CAMPAIGN_PLANNING_STATES.OUTREACH_COPY_PLAN,
        answers,
        slots: { ...slots },
        outreachCopyPlan: null,
        outreachDraftPreview: null,
        intent: 'outreach_draft_preview_missing_copy_plan',
        planningState: CAMPAIGN_PLANNING_STATES.OUTREACH_COPY_PLAN,
        currentAsk: OUTREACH_COPY_PLAN_CLOSING_QUESTION,
        sendsMade: false,
        crmWritesMade: false,
        exportMade: false,
        accountChangesMade: false,
      };
    }
    plan = buildOutreachCopyPlan(strategy, review, ctx, {
      priorCriteriaPreview: opts.priorCriteriaPreview || null,
      priorPreview: opts.priorPreview || null,
      answers,
    });
  }
  if (plan && outreachCopyPlanLooksStale(plan)) {
    plan = repairOutreachCopyPlan(plan, strategy, review, ctx, { answers });
  }
  if (!isOutreachCopyPlanAlreadyApproved(plan)) {
    plan = approveOutreachCopyPlan(plan);
  }

  const existing =
    opts.bypassStoredOutreachDraftPreview || opts.forceRebuild
      ? null
      : opts.priorOutreachDraftPreview || null;
  const alreadyHave = hasOutreachDraftPreview(existing);
  const forceRebuild =
    opts.forceRebuild === true || opts.bypassStoredOutreachDraftPreview === true;
  const responseMode =
    opts.responseMode || RESPONSE_MODES.WORKFLOW_REVIEW_CARD;
  const campaignCtx = buildCampaignContextForRender(review, ctx, {
    ...opts,
    approvedStrategy: strategy,
    approvedCopyPlan: plan,
    answers,
    step: CAMPAIGN_PLANNING_STATES.OUTREACH_DRAFT_PREVIEW,
  });

  let outreachDraftPreview;
  if (
    alreadyHave &&
    !forceRebuild &&
    outreachDraftPreviewLooksStale(existing, campaignCtx)
  ) {
    outreachDraftPreview = repairOutreachDraftPreview(
      existing,
      plan,
      strategy,
      review,
      ctx,
      {
        workRequestId:
          (review && review.workRequestId) || (plan && plan.workRequestId),
        campaignSynthesisContext: campaignCtx,
        campaignMemory: campaignCtx.campaignMemory,
        answers,
        responseMode,
        draftFollowUps: opts.draftFollowUps,
        includeOperatorDigest:
          responseMode === RESPONSE_MODES.WORKFLOW_REVIEW_CARD,
      }
    );
  } else {
    outreachDraftPreview = buildOutreachDraftPreview(
      plan,
      strategy,
      review,
      ctx,
      {
        workRequestId:
          (review && review.workRequestId) || (plan && plan.workRequestId),
        priorOutreachDraftPreview: existing,
        reuseExisting: alreadyHave && !forceRebuild,
        forceRebuild,
        campaignSynthesisContext: campaignCtx,
        campaignMemory: campaignCtx.campaignMemory,
        answers,
        responseMode,
        draftFollowUps: opts.draftFollowUps,
        includeOperatorDigest:
          responseMode === RESPONSE_MODES.WORKFLOW_REVIEW_CARD,
      }
    );
  }

  // Pre-response validation — regenerate once if operator instructions fail.
  let validation = validateOutreachDraftAgainstInstructions(
    outreachDraftPreview,
    {
      learnings: campaignCtx.learnings,
      responseMode,
      message: '',
    }
  );
  if (!validation.ok && !opts._validationRetry) {
    outreachDraftPreview = buildOutreachDraftPreview(
      plan,
      strategy,
      review,
      ctx,
      {
        workRequestId:
          (review && review.workRequestId) || (plan && plan.workRequestId),
        priorOutreachDraftPreview: outreachDraftPreview,
        reuseExisting: false,
        forceRebuild: true,
        campaignSynthesisContext: campaignCtx,
        campaignMemory: campaignCtx.campaignMemory,
        answers,
        responseMode,
        draftFollowUps:
          opts.draftFollowUps ||
          Boolean(
            campaignCtx.learnings &&
              (campaignCtx.learnings.draft_follow_ups ||
                campaignCtx.learnings.follow_up_mode === 'drafted_emails')
          ),
        includeOperatorDigest:
          responseMode === RESPONSE_MODES.WORKFLOW_REVIEW_CARD,
      }
    );
    outreachDraftPreview.repairedFromValidation = true;
    outreachDraftPreview.validationFailures = validation.failures;
    validation = validateOutreachDraftAgainstInstructions(
      outreachDraftPreview,
      {
        learnings: campaignCtx.learnings,
        responseMode,
        message: '',
      }
    );
  }

  if (opts.operatorChanges) {
    outreachDraftPreview.operatorChanges = opts.operatorChanges;
  }
  if (opts.operatorAcknowledgment) {
    outreachDraftPreview.operatorAcknowledgment = opts.operatorAcknowledgment;
  }
  outreachDraftPreview.responseMode = responseMode;

  const isOperatorChat =
    responseMode === RESPONSE_MODES.OPERATOR_CHAT_RESPONSE;
  const intro = isOperatorChat
    ? null
    : alreadyHave && !outreachDraftPreview.repairedFromStale && !forceRebuild
      ? 'Outreach Draft Preview is already available — showing it for approval or revision. Not re-rendering the Outreach Copy Plan.'
      : outreachDraftPreview.repairedFromStale ||
          outreachDraftPreview.repairedFromValidation
        ? 'Updated the Outreach Draft Preview — repaired stale phrasing from an earlier draft. Showing it for approval or revision.'
        : forceRebuild
          ? 'Rebuilt the Outreach Draft Preview from your latest instructions.'
          : 'Creating the Outreach Draft Preview from the approved Copy Plan, Batch 1, and Blueprint.';

  let bodyMessage = formatOutreachDraftPreviewMessage(outreachDraftPreview);
  // Re-validate against the rendered message for chat-mode boilerplate.
  validation = validateOutreachDraftAgainstInstructions(outreachDraftPreview, {
    learnings: campaignCtx.learnings,
    responseMode,
    message: bodyMessage,
  });
  if (!validation.ok && isOperatorChat) {
    // Force conversational formatter (no digest leftovers).
    bodyMessage = formatOperatorChatDraftResponse(outreachDraftPreview, {
      changes: opts.operatorChanges || outreachDraftPreview.operatorChanges || [],
      acknowledgment:
        opts.operatorAcknowledgment ||
        'Got it — I updated the active Outreach Draft Preview from your instructions.',
      closingQuestion: OUTREACH_DRAFT_PREVIEW_CLOSING_QUESTION,
    });
    outreachDraftPreview.operatorDigest = null;
  }

  const message = [leadIn || null, intro, intro ? '' : null, bodyMessage]
    .filter((part) => part != null && part !== '')
    .join('\n');

  return {
    message,
    step: CAMPAIGN_PLANNING_STATES.OUTREACH_DRAFT_PREVIEW,
    answers,
    slots: {
      ...slots,
      previewApproved: true,
      criteriaApproved: true,
      buildProposalApproved: true,
      prospectBatchReviewApproved: true,
      batch1Approved: true,
      outreachStrategyPreviewApproved: true,
      strategyApproved: true,
      outreachCopyPlanGenerated: true,
      outreachCopyPlanApproved: true,
      copyPlanApproved: true,
      outreachDraftPreviewGenerated: true,
    },
    prospectBatchReview: review,
    outreachStrategyPreview: strategy,
    outreachCopyPlan: plan,
    outreachDraftPreview,
    outreachLaunchGate: opts.priorOutreachLaunchGate || null,
    campaignMemory: campaignCtx.campaignMemory,
    campaignWorkingState: opts.campaignWorkingState || null,
    responseMode,
    validation,
    intent: opts.revisionIntent
      ? 'revise_outreach_draft_preview'
      : alreadyHave && !forceRebuild
        ? outreachDraftPreview.repairedFromStale
          ? 'repair_outreach_draft_preview'
          : 'show_outreach_draft_preview'
        : leadIn && /approved/i.test(String(leadIn))
          ? 'outreach_copy_plan_approved'
          : forceRebuild
            ? 'revise_outreach_draft_preview'
            : 'produce_outreach_draft_preview',
    copyPlanApproved: true,
    strategyApproved: true,
    batch1Approved: true,
    planningState: CAMPAIGN_PLANNING_STATES.OUTREACH_DRAFT_PREVIEW,
    currentAsk: OUTREACH_DRAFT_PREVIEW_CLOSING_QUESTION,
    finalOutreachCopyGenerated: true,
    outreachCopyGenerated: true,
    sendsMade: false,
    crmWritesMade: false,
    exportMade: false,
    accountChangesMade: false,
  };
}

/**
 * Apply operator chat corrections to Outreach Draft Preview working state,
 * rebuild from corrected memory, and respond in Operator Chat Response mode.
 */
function produceOutreachDraftPreviewRevisionResult(
  ctx,
  answers,
  slots,
  opts,
  userMessage
) {
  // Confirmed force-rebuild must mutate the execution path — never re-ask.
  if (
    looksLikeForceRebuildConfirmation(userMessage, {
      campaignWorkingState: opts.campaignWorkingState || null,
      awaitingForceRebuildConfirmation: opts.awaitingForceRebuildConfirmation,
      lastResponseMode: opts.lastResponseMode,
    })
  ) {
    return produceOutreachDraftPreviewForceRebuildResult(
      ctx,
      answers,
      slots,
      opts,
      userMessage
    );
  }

  const parsed = parseOperatorChatDirectives(userMessage);
  let campaignMemory = ensureCampaignMemory({
    campaignMemory: opts.campaignMemory || null,
  });
  if (parsed.hasDirectives) {
    campaignMemory = mergeOperatorLearnings(
      campaignMemory,
      parsed.learnings,
      'operator_chat'
    );
  } else {
    // Even without structured parse hits, prefer merge-token subject + chat mode
    // when the operator asked to revise the draft preview.
    campaignMemory = mergeOperatorLearnings(
      campaignMemory,
      {
        subject_keep_merge_tokens: true,
        claim_tested_winner: false,
        response_mode_preference: RESPONSE_MODES.OPERATOR_CHAT_RESPONSE,
      },
      'operator_chat'
    );
  }

  let workingState = ensureCampaignWorkingState({
    campaignWorkingState: opts.campaignWorkingState || null,
  });
  workingState = applyOperatorDirectivesToWorkingState(workingState, parsed, {
    activeArtifactKind: OUTREACH_DRAFT_PREVIEW_KIND,
  });

  // If a prior diagnostic already set bypass, execute force-rebuild immediately.
  if (workingState.bypassStoredOutreachDraftPreview) {
    return produceOutreachDraftPreviewForceRebuildResult(
      ctx,
      answers,
      slots,
      {
        ...opts,
        campaignMemory,
        campaignWorkingState: workingState,
      },
      userMessage
    );
  }

  const priorDraft = opts.priorOutreachDraftPreview || null;
  const fingerprintBefore = draftOutputFingerprint(priorDraft);
  const priorRejects = countRejectedFingerprint(workingState, fingerprintBefore);

  // If the same rejected fingerprint already appeared after a prior correction,
  // stop drafting and run the stale-source diagnostic (once — await confirm).
  if (
    priorRejects >= 1 &&
    (parsed.hasDirectives || workingState.awaitingForceRebuildConfirmation)
  ) {
    const injectionSources = identifyStaleInjectionSources(
      priorDraft,
      '',
      campaignMemory.operatorLearnings
    );
    const diagnostic = buildStaleSourceDiagnostic({
      campaignWorkingState: workingState,
      learnings: campaignMemory.operatorLearnings,
      parsedDirectives: parsed,
      responseMode: RESPONSE_MODES.OPERATOR_CHAT_RESPONSE,
      activeArtifactKind: OUTREACH_DRAFT_PREVIEW_KIND,
      storedArtifactPresent: Boolean(priorDraft),
      winningSource: 'stored_artifact_or_template_overrode_operator_chat',
      validationFailures: [
        {
          code: 'repeated_rejected_output',
          detail: fingerprintBefore,
        },
      ],
      injectionSources,
      staleReason:
        'operator correction did not change the regenerated draft fingerprint',
    });
    workingState = markAwaitingForceRebuild(workingState, diagnostic.diagnostic);
    return {
      message: diagnostic.message,
      step: CAMPAIGN_PLANNING_STATES.OUTREACH_DRAFT_PREVIEW,
      answers,
      slots: { ...slots },
      prospectBatchReview: opts.priorProspectBatchReview || null,
      outreachStrategyPreview: opts.priorOutreachStrategyPreview || null,
      outreachCopyPlan: opts.priorOutreachCopyPlan || null,
      // Do not re-surface the stale draft body in chat — keep artifact reference
      // only for state continuity until force-rebuild clears it.
      outreachDraftPreview: priorDraft,
      campaignMemory,
      campaignWorkingState: workingState,
      responseMode: RESPONSE_MODES.STALE_SOURCE_DIAGNOSTIC,
      staleSourceDiagnostic: diagnostic.diagnostic,
      intent: 'stale_source_diagnostic',
      planningState: CAMPAIGN_PLANNING_STATES.OUTREACH_DRAFT_PREVIEW,
      currentAsk:
        'Confirm force-rebuild from operator instructions only, or name the stale source to bypass.',
      sendsMade: false,
      crmWritesMade: false,
      exportMade: false,
      accountChangesMade: false,
    };
  }

  // Soft revision still force-rebuilds without reusing the stored card body.
  const rebuilt = produceOutreachDraftPreviewResult(
    ctx,
    answers,
    slots,
    {
      ...opts,
      campaignMemory,
      campaignWorkingState: workingState,
      priorOutreachDraftPreview: null,
      forceRebuild: true,
      bypassStoredOutreachDraftPreview: true,
      responseMode: RESPONSE_MODES.OPERATOR_CHAT_RESPONSE,
      draftFollowUps:
        Boolean(
          campaignMemory.operatorLearnings &&
            (campaignMemory.operatorLearnings.draft_follow_ups ||
              campaignMemory.operatorLearnings.follow_up_mode ===
                'drafted_emails')
        ) || parsed.learnings.draft_follow_ups === true,
      operatorLearnings: campaignMemory.operatorLearnings,
      operatorChanges: parsed.changes.length
        ? parsed.changes
        : ['rebuilt Outreach Draft Preview from latest operator instructions'],
      operatorAcknowledgment:
        'Got it — I updated the active Outreach Draft Preview from your instructions.',
      revisionIntent: true,
      includeOperatorDigest: false,
      _validationRetry: false,
    },
    null
  );

  const fingerprintAfter = draftOutputFingerprint(rebuilt.outreachDraftPreview);
  let nextWorking = markDirectivesApplied(workingState, null);
  nextWorking.lastResponseMode = RESPONSE_MODES.OPERATOR_CHAT_RESPONSE;

  // If rebuild still matches the rejected fingerprint, record it; next turn diagnoses.
  if (
    priorDraft &&
    fingerprintAfter === fingerprintBefore &&
    parsed.hasDirectives
  ) {
    nextWorking = recordRejectedOutput(nextWorking, fingerprintAfter);
  }

  // Final validation against the operator-facing message.
  const validation = validateOutreachDraftAgainstInstructions(
    rebuilt.outreachDraftPreview,
    {
      learnings: campaignMemory.operatorLearnings,
      responseMode: RESPONSE_MODES.OPERATOR_CHAT_RESPONSE,
      message: rebuilt.message,
    }
  );

  if (!validation.ok) {
    // One more forced rebuild bypassing stored artifact entirely.
    return produceOutreachDraftPreviewForceRebuildResult(
      ctx,
      answers,
      slots,
      {
        ...opts,
        campaignMemory,
        campaignWorkingState: nextWorking,
        operatorChanges: parsed.changes,
        _fromValidationFailure: true,
        _validationFailures: validation.failures,
      },
      userMessage
    );
  }

  nextWorking = clearForceRebuildBypass(nextWorking);
  return {
    ...rebuilt,
    campaignMemory: rebuilt.campaignMemory || campaignMemory,
    campaignWorkingState: nextWorking,
    responseMode: RESPONSE_MODES.OPERATOR_CHAT_RESPONSE,
    validation,
    intent: 'revise_outreach_draft_preview',
  };
}

/**
 * Confirmed force-rebuild from operator instructions only.
 * Never retrieves, injects, renders, or reuses stored outreach_draft_preview.
 * Never re-asks for force-rebuild confirmation.
 */
function produceOutreachDraftPreviewForceRebuildResult(
  ctx,
  answers,
  slots,
  opts,
  userMessage
) {
  let campaignMemory = ensureCampaignMemory({
    campaignMemory: opts.campaignMemory || null,
  });
  const workingPrior = ensureCampaignWorkingState({
    campaignWorkingState: opts.campaignWorkingState || null,
  });

  // Re-apply the latest stored operator instruction (from the correction turn)
  // plus any directives in this confirmation message.
  const latestInstruction =
    workingPrior.latestOperatorInstruction ||
    (workingPrior.lastStaleDiagnostic &&
      workingPrior.lastStaleDiagnostic.latestOperatorInstruction) ||
    null;
  const instructionText = [latestInstruction, userMessage]
    .filter(Boolean)
    .join('\n');
  const parsedLatest = parseOperatorChatDirectives(instructionText || '');
  const parsedConfirm = parseOperatorChatDirectives(userMessage || '');

  campaignMemory = mergeOperatorLearnings(
    campaignMemory,
    {
      subject_keep_merge_tokens: true,
      claim_tested_winner: false,
      draft_follow_ups: true,
      follow_up_mode: 'drafted_emails',
      personalization_rule: 'do not use street addresses by default',
      personalization_preference:
        'use town, company, property type, portfolio cue, or public role signal',
      response_mode_preference: RESPONSE_MODES.OPERATOR_CHAT_RESPONSE,
      tested_subject_line_pattern:
        (campaignMemory.operatorLearnings &&
          campaignMemory.operatorLearnings.tested_subject_line_pattern) ||
        '{{business_name}} - commercial cleaning',
      ...(parsedLatest.learnings || {}),
      ...(parsedConfirm.learnings || {}),
    },
    'operator_chat_force_rebuild'
  );

  let workingState = markForceRebuildBypass(workingPrior);
  workingState = applyOperatorDirectivesToWorkingState(
    workingState,
    {
      ...parsedLatest,
      rawText: latestInstruction || userMessage,
      hasDirectives: true,
      directives: [
        ...(parsedLatest.directives || []),
        ...(parsedConfirm.directives || []),
        {
          type: 'force_rebuild_bypass',
          value: true,
          source: 'operator_chat',
        },
      ],
      changes: [
        ...(parsedLatest.changes || []),
        'force-rebuild from operator instructions only (stored draft bypassed)',
      ],
    },
    { activeArtifactKind: OUTREACH_DRAFT_PREVIEW_KIND }
  );
  workingState.bypassStoredOutreachDraftPreview = true;
  workingState.awaitingForceRebuildConfirmation = false;

  const changes = [
    'bypassed stored outreach_draft_preview',
    'subject → {{business_name}} - commercial cleaning',
    'no street addresses',
    'drafted Follow-up 1 and Follow-up 2',
    'operator chat response (not workflow card)',
    ...(parsedLatest.changes || []).filter(
      (c) => !/subject →|no street|follow-up|conversational/i.test(c)
    ),
  ];

  const rebuilt = produceOutreachDraftPreviewResult(
    ctx,
    answers,
    slots,
    {
      ...opts,
      // Critical: do not retrieve / inject / reuse stored draft.
      priorOutreachDraftPreview: null,
      forceRebuild: true,
      bypassStoredOutreachDraftPreview: true,
      campaignMemory,
      campaignWorkingState: workingState,
      responseMode: RESPONSE_MODES.OPERATOR_CHAT_RESPONSE,
      draftFollowUps: true,
      operatorLearnings: campaignMemory.operatorLearnings,
      operatorChanges: changes,
      operatorAcknowledgment:
        'Force-rebuild confirmed — rebuilt from operator instructions and Shared Campaign Memory only. Stored Outreach Draft Preview was not used.',
      revisionIntent: true,
      includeOperatorDigest: false,
      _validationRetry: true,
    },
    null
  );

  const validation = validateOutreachDraftAgainstInstructions(
    rebuilt.outreachDraftPreview,
    {
      learnings: {
        ...campaignMemory.operatorLearnings,
        subject_keep_merge_tokens: true,
        claim_tested_winner: false,
        draft_follow_ups: true,
      },
      responseMode: RESPONSE_MODES.OPERATOR_CHAT_RESPONSE,
      message: rebuilt.message,
    }
  );

  if (!validation.ok) {
    const injectionSources = identifyStaleInjectionSources(
      rebuilt.outreachDraftPreview,
      rebuilt.message,
      campaignMemory.operatorLearnings
    );
    const diagnostic = buildStaleSourceDiagnostic({
      campaignWorkingState: workingState,
      learnings: campaignMemory.operatorLearnings,
      parsedDirectives: parsedLatest,
      responseMode: RESPONSE_MODES.OPERATOR_CHAT_RESPONSE,
      activeArtifactKind: OUTREACH_DRAFT_PREVIEW_KIND,
      storedArtifactPresent: false,
      winningSource: 'template_or_renderer_after_forced_rebuild',
      validationFailures: validation.failures,
      injectionSources,
      staleReason: validation.failures.map((f) => f.code).join(', '),
    });
    // Fail closed — do NOT ask for confirmation again, and do NOT return stale draft.
    workingState = {
      ...workingState,
      awaitingForceRebuildConfirmation: false,
      bypassStoredOutreachDraftPreview: true,
      lastResponseMode: RESPONSE_MODES.STALE_SOURCE_DIAGNOSTIC,
      lastStaleDiagnostic: diagnostic.diagnostic,
      updatedAt: new Date().toISOString(),
    };
    const failClosedLines = [
      'Force-rebuild from operator instructions failed closed — stale fields were still injected after bypassing the stored outreach_draft_preview.',
      '',
      'Exact stale injection sources:',
      ...(injectionSources.length
        ? injectionSources.map(
            (s) =>
              `- ${s.source}.${s.field} (${s.reason}): ${JSON.stringify(s.value).slice(0, 120)}`
          )
        : validation.failures.map(
            (f) => `- validation.${f.code}: ${f.detail || ''}`
          )),
      '',
      'Stored outreach_draft_preview was not reused. Workflow card / evidence renderers were not selected.',
      'I am not asking for another force-rebuild confirmation. Fix or bypass the named source above, then retry.',
    ];
    return {
      message: failClosedLines.join('\n'),
      step: CAMPAIGN_PLANNING_STATES.OUTREACH_DRAFT_PREVIEW,
      answers,
      slots: { ...slots },
      prospectBatchReview: opts.priorProspectBatchReview || null,
      outreachStrategyPreview: opts.priorOutreachStrategyPreview || null,
      outreachCopyPlan: opts.priorOutreachCopyPlan || null,
      outreachDraftPreview: null,
      campaignMemory,
      campaignWorkingState: workingState,
      responseMode: RESPONSE_MODES.STALE_SOURCE_DIAGNOSTIC,
      staleSourceDiagnostic: diagnostic.diagnostic,
      validation,
      intent: 'force_rebuild_failed_closed',
      planningState: CAMPAIGN_PLANNING_STATES.OUTREACH_DRAFT_PREVIEW,
      currentAsk:
        'Name the stale source to remove, or retry after the named template/field is fixed.',
      sendsMade: false,
      crmWritesMade: false,
      exportMade: false,
      accountChangesMade: false,
    };
  }

  workingState = clearForceRebuildBypass(workingState);
  workingState.lastResponseMode = RESPONSE_MODES.OPERATOR_CHAT_RESPONSE;

  return {
    ...rebuilt,
    outreachDraftPreview: {
      ...rebuilt.outreachDraftPreview,
      forceRebuiltFromOperatorInstructions: true,
      bypassedStoredArtifact: true,
      operatorDigest: null,
      responseMode: RESPONSE_MODES.OPERATOR_CHAT_RESPONSE,
    },
    campaignMemory: rebuilt.campaignMemory || campaignMemory,
    campaignWorkingState: workingState,
    responseMode: RESPONSE_MODES.OPERATOR_CHAT_RESPONSE,
    validation,
    intent: 'force_rebuild_outreach_draft_preview',
    currentAsk: OUTREACH_DRAFT_PREVIEW_CLOSING_QUESTION,
  };
}

/**
 * Approve Outreach Draft Preview and create/show Launch Gate.
 */
function produceOutreachDraftPreviewApprovalResult(
  ctx,
  answers,
  slots,
  opts,
  leadIn
) {
  const priorDraft = opts.priorOutreachDraftPreview || null;
  let draft = priorDraft;
  if (draft && !isOutreachDraftPreviewAlreadyApproved(draft)) {
    draft = approveOutreachDraftPreview(draft);
  }

  // Ensure upstream approvals.
  let plan = opts.priorOutreachCopyPlan || null;
  if (plan && !isOutreachCopyPlanAlreadyApproved(plan)) {
    plan = approveOutreachCopyPlan(plan);
  }
  let strategy = opts.priorOutreachStrategyPreview || null;
  if (strategy && !isOutreachStrategyPreviewAlreadyApproved(strategy)) {
    strategy = approveOutreachStrategyPreview(strategy);
  }
  let review = opts.priorProspectBatchReview || null;
  if (review && !isProspectBatchReviewAlreadyApproved(review)) {
    review = approveProspectBatchReviewBatch1(review);
  }

  if (!hasOutreachDraftPreview(draft)) {
    const built = produceOutreachDraftPreviewResult(
      ctx,
      answers,
      slots,
      { ...opts, priorOutreachCopyPlan: plan, priorOutreachStrategyPreview: strategy, priorProspectBatchReview: review },
      null
    );
    draft = approveOutreachDraftPreview(built.outreachDraftPreview);
    plan = built.outreachCopyPlan;
    strategy = built.outreachStrategyPreview;
    review = built.prospectBatchReview;
  } else {
    draft = approveOutreachDraftPreview(draft);
  }

  return produceOutreachLaunchGateResult(
    ctx,
    answers,
    {
      ...slots,
      outreachCopyPlanApproved: true,
      copyPlanApproved: true,
      outreachDraftPreviewGenerated: true,
      outreachDraftPreviewApproved: true,
      draftPreviewApproved: true,
    },
    {
      ...opts,
      priorProspectBatchReview: review,
      priorOutreachStrategyPreview: strategy,
      priorOutreachCopyPlan: plan,
      priorOutreachDraftPreview: draft,
    },
    leadIn || OUTREACH_DRAFT_PREVIEW_APPROVED_MESSAGE
  );
}

/**
 * Create or show Outreach Launch Gate after Draft Preview approval.
 */
function produceOutreachLaunchGateResult(ctx, answers, slots, opts, leadIn) {
  let review = opts.priorProspectBatchReview || null;
  let strategy = opts.priorOutreachStrategyPreview || null;
  let plan = opts.priorOutreachCopyPlan || null;
  let draft = opts.priorOutreachDraftPreview || null;

  if (!hasOutreachDraftPreview(draft)) {
    if (!hasOutreachCopyPlan(plan)) {
      return {
        message: [
          leadIn || 'Outreach Launch Gate needs an approved Outreach Draft Preview.',
          '',
          'Approve the Outreach Draft Preview first.',
          BATCH_1_APPROVED_DISCLAIMER,
        ].join('\n'),
        step: CAMPAIGN_PLANNING_STATES.OUTREACH_DRAFT_PREVIEW,
        answers,
        slots: { ...slots },
        outreachLaunchGate: null,
        intent: 'outreach_launch_gate_missing_draft',
        planningState: CAMPAIGN_PLANNING_STATES.OUTREACH_DRAFT_PREVIEW,
        currentAsk: OUTREACH_DRAFT_PREVIEW_CLOSING_QUESTION,
        sendsMade: false,
        crmWritesMade: false,
        exportMade: false,
        accountChangesMade: false,
      };
    }
    if (!isOutreachCopyPlanAlreadyApproved(plan)) {
      plan = approveOutreachCopyPlan(plan);
    }
    draft = buildOutreachDraftPreview(plan, strategy, review, ctx, {
      answers,
    });
  }
  if (!isOutreachDraftPreviewAlreadyApproved(draft)) {
    draft = approveOutreachDraftPreview(draft);
  }

  const existing = opts.priorOutreachLaunchGate || null;
  const alreadyHave = hasOutreachLaunchGate(existing);
  const alreadyApproved = isOutreachLaunchGateAlreadyApproved(existing);
  const outreachLaunchGate = buildOutreachLaunchGate(
    draft,
    plan,
    strategy,
    review,
    ctx,
    {
      priorOutreachLaunchGate: existing,
      reuseExisting: alreadyHave,
    }
  );

  // Approved gates must never re-render the pre-approval review card.
  if (alreadyApproved || isOutreachLaunchGateAlreadyApproved(outreachLaunchGate)) {
    const gate = isOutreachLaunchGateAlreadyApproved(outreachLaunchGate)
      ? {
          ...outreachLaunchGate,
          status: OUTREACH_LAUNCH_GATE_APPROVED_STATUS,
          operatorDigest: null,
          closingQuestion: null,
          operatorStateSummary:
            outreachLaunchGate.operatorStateSummary ||
            buildOutreachLaunchGateOperatorStateSummary(outreachLaunchGate),
        }
      : approveOutreachLaunchGate(outreachLaunchGate, {
          approvedAt: opts.approvedAt || existing.approvedAt,
        });
    const message = formatOutreachLaunchGateApprovedSummary(gate, {
      justApproved: false,
      gateAlreadyApproved: true,
      // Canonical composer owns the acknowledgment — no leadIn restatement.
      leadIn: null,
    });

    return applyConversationalPolicy(
      {
        message,
        step: CAMPAIGN_PLANNING_STATES.OUTREACH_LAUNCH_GATE,
        answers,
        slots: {
          ...slots,
          outreachCopyPlanApproved: true,
          copyPlanApproved: true,
          outreachDraftPreviewGenerated: true,
          outreachDraftPreviewApproved: true,
          draftPreviewApproved: true,
          outreachLaunchGateGenerated: true,
          outreachLaunchGateApproved: true,
          launchGateApproved: true,
          launchReady: true,
        },
        prospectBatchReview: review,
        outreachStrategyPreview: strategy,
        outreachCopyPlan: plan,
        outreachDraftPreview: draft,
        outreachLaunchGate: gate,
        intent: 'outreach_launch_gate_approved',
        draftPreviewApproved: true,
        copyPlanApproved: true,
        strategyApproved: true,
        batch1Approved: true,
        launchGateApproved: true,
        launchReady: true,
        launched: false,
        planningState: CAMPAIGN_PLANNING_STATES.OUTREACH_LAUNCH_GATE,
        currentAsk: OUTREACH_LAUNCH_GATE_APPROVED_ASK,
        responseMode: RESPONSE_MODES.OPERATOR_STATE_SUMMARY,
        conversationMode: CONVERSATION_MODES.OPERATOR_STATE_UPDATE,
        finalOutreachCopyGenerated: true,
        outreachCopyGenerated: true,
        sendsMade: false,
        crmWritesMade: false,
        exportMade: false,
        accountChangesMade: false,
      },
      {
        gateAlreadyApproved: true,
        forceMode: CONVERSATION_MODES.OPERATOR_STATE_UPDATE,
      }
    );
  }

  const intro = alreadyHave
    ? 'Outreach Launch Gate is already available — showing it for readiness approval. Not re-rendering the Outreach Draft Preview.'
    : 'Creating the Outreach Launch Gate from the approved Draft Preview. Launch/export/CRM still require an explicit execute action after this gate.';

  const message = [
    leadIn || null,
    intro,
    '',
    formatOutreachLaunchGateMessage(outreachLaunchGate),
  ]
    .filter(Boolean)
    .join('\n');

  return {
    message,
    step: CAMPAIGN_PLANNING_STATES.OUTREACH_LAUNCH_GATE,
    answers,
    slots: {
      ...slots,
      outreachCopyPlanApproved: true,
      copyPlanApproved: true,
      outreachDraftPreviewGenerated: true,
      outreachDraftPreviewApproved: true,
      draftPreviewApproved: true,
      outreachLaunchGateGenerated: true,
    },
    prospectBatchReview: review,
    outreachStrategyPreview: strategy,
    outreachCopyPlan: plan,
    outreachDraftPreview: draft,
    outreachLaunchGate,
    intent: alreadyHave
      ? 'show_outreach_launch_gate'
      : leadIn && /approved/i.test(String(leadIn))
        ? 'outreach_draft_preview_approved'
        : 'produce_outreach_launch_gate',
    draftPreviewApproved: true,
    copyPlanApproved: true,
    strategyApproved: true,
    batch1Approved: true,
    launchReady: false,
    launched: false,
    planningState: CAMPAIGN_PLANNING_STATES.OUTREACH_LAUNCH_GATE,
    currentAsk: OUTREACH_LAUNCH_GATE_CLOSING_QUESTION,
    responseMode: RESPONSE_MODES.WORKFLOW_REVIEW_CARD,
    finalOutreachCopyGenerated: true,
    outreachCopyGenerated: true,
    sendsMade: false,
    crmWritesMade: false,
    exportMade: false,
    accountChangesMade: false,
  };
}

/**
 * Approve Launch Gate for readiness only — never auto-executes.
 */
function produceOutreachLaunchGateApprovalResult(
  ctx,
  answers,
  slots,
  opts,
  leadIn
) {
  const priorGate = opts.priorOutreachLaunchGate || null;
  const alreadyApproved = isOutreachLaunchGateAlreadyApproved(priorGate);

  // Idempotent: already-approved gate → approved-state summary only.
  if (alreadyApproved) {
    const gate = approveOutreachLaunchGate(priorGate, {
      approvedAt: opts.approvedAt || priorGate.approvedAt,
    });
    const message = formatOutreachLaunchGateApprovedSummary(gate, {
      justApproved: false,
      gateAlreadyApproved: true,
      // Canonical composer owns the acknowledgment — no leadIn restatement.
      leadIn: null,
    });

    return applyConversationalPolicy(
      {
        message,
        step: CAMPAIGN_PLANNING_STATES.OUTREACH_LAUNCH_GATE,
        answers,
        slots: {
          ...slots,
          outreachLaunchGateApproved: true,
          launchGateApproved: true,
          launchReady: true,
        },
        outreachLaunchGate: gate,
        prospectBatchReview: opts.priorProspectBatchReview || null,
        outreachStrategyPreview: opts.priorOutreachStrategyPreview || null,
        outreachCopyPlan: opts.priorOutreachCopyPlan || null,
        outreachDraftPreview: opts.priorOutreachDraftPreview || null,
        intent: 'outreach_launch_gate_approved',
        launchGateApproved: true,
        launchReady: true,
        launched: false,
        planningState: CAMPAIGN_PLANNING_STATES.OUTREACH_LAUNCH_GATE,
        currentAsk: OUTREACH_LAUNCH_GATE_APPROVED_ASK,
        responseMode: RESPONSE_MODES.OPERATOR_STATE_SUMMARY,
        conversationMode: CONVERSATION_MODES.OPERATOR_STATE_UPDATE,
        sendsMade: false,
        crmWritesMade: false,
        exportMade: false,
        accountChangesMade: false,
      },
      {
        gateAlreadyApproved: true,
        forceMode: CONVERSATION_MODES.OPERATOR_STATE_UPDATE,
      }
    );
  }

  const result = produceOutreachLaunchGateResult(
    ctx,
    answers,
    slots,
    opts,
    null
  );
  const gate = approveOutreachLaunchGate(result.outreachLaunchGate, {
    approvedAt: opts.approvedAt || new Date().toISOString(),
  });
  const message = formatOutreachLaunchGateApprovedSummary(gate, {
    justApproved: true,
    stateChanged: true,
    // Optional personality aside only — never restate approval/safety here.
    leadIn:
      leadIn && !/approved for readiness only|nothing external/i.test(leadIn)
        ? leadIn
        : null,
  });

  return applyConversationalPolicy(
    {
      ...result,
      message,
      outreachLaunchGate: gate,
      intent: 'outreach_launch_gate_approved',
      launchGateApproved: true,
      launchReady: true,
      launched: false,
      slots: {
        ...result.slots,
        outreachLaunchGateApproved: true,
        launchGateApproved: true,
        launchReady: true,
      },
      currentAsk: OUTREACH_LAUNCH_GATE_APPROVED_ASK,
      responseMode: RESPONSE_MODES.OPERATOR_STATE_SUMMARY,
      conversationMode: CONVERSATION_MODES.OPERATOR_STATE_UPDATE,
      sendsMade: false,
      crmWritesMade: false,
      exportMade: false,
      accountChangesMade: false,
    },
    {
      justApproved: true,
      stateChanged: true,
      forceMode: CONVERSATION_MODES.OPERATOR_STATE_UPDATE,
    }
  );
}

/**
 * Execution Confirmation mode — explicit safety checkpoint only.
 * Never sends, exports, or writes CRM from this reply.
 */
function produceExecutionConfirmationResult(userMessage, prior = {}, opts = {}) {
  const text = String(userMessage || '');
  let action = 'execute action';
  if (/\bmanual-?send\s+export\b|\bexport\b/i.test(text)) {
    action = 'prepare a manual-send export for operator review';
  } else if (/\bcrm\b/i.test(text)) {
    action = 'create CRM drafts';
  } else if (/\bqueue\s+sends?\b|\bsend/i.test(text)) {
    action = 'queue or send outreach';
  }

  const gate = prior.outreachLaunchGate || opts.priorOutreachLaunchGate || null;
  const count =
    (gate && gate.approvedCandidateCount) ||
    (prior.outreachDraftPreview &&
      prior.outreachDraftPreview.approvedCandidateCount) ||
    'Batch 1 approved candidates';

  const composed = composeExecutionConfirmation({
    action,
    recordsAffected: `${count} campaign records in the approved Batch 1 scope`,
    sender: opts.sender || 'configured campaign sender (not yet confirmed)',
    externalEffects:
      'May create an export file, CRM drafts, or queued sends — irreversible once executed against external systems.',
    holdGuidance:
      "I'd hold until sender identity and reply handling are explicit. I'm not going to move anything externally without your explicit approval.",
  });

  return applyConversationalPolicy(
    {
      message: composed.message,
      step:
        prior.step ||
        CAMPAIGN_PLANNING_STATES.OUTREACH_LAUNCH_GATE,
      answers: opts.answers || prior.answers || {},
      slots: {
        ...(prior.slots || {}),
        ...(opts.slots || {}),
        launchGateApproved: true,
        launchReady: true,
      },
      outreachLaunchGate: gate,
      outreachDraftPreview: prior.outreachDraftPreview || null,
      intent: 'execution_confirmation',
      planningState:
        prior.planningState ||
        CAMPAIGN_PLANNING_STATES.OUTREACH_LAUNCH_GATE,
      currentAsk: 'Do you explicitly approve this execute action?',
      responseMode: RESPONSE_MODES.EXECUTION_CONFIRMATION,
      conversationMode: CONVERSATION_MODES.EXECUTION_CONFIRMATION,
      launchGateApproved: true,
      launchReady: true,
      launched: false,
      executionPending: true,
      sendsMade: false,
      crmWritesMade: false,
      exportMade: false,
      accountChangesMade: false,
    },
    {
      isExecutionRequest: true,
      forceMode: CONVERSATION_MODES.EXECUTION_CONFIRMATION,
    }
  );
}

function formatProspectBatch1ApprovalMessage(approvedReview, opts = {}) {
  const review = approvedReview || {};
  const count =
    (review.approvedBatch && review.approvedBatch.candidateCount) != null
      ? review.approvedBatch.candidateCount
      : (review.acceptedFirstPass || []).length;
  const lines = [
    BATCH_1_APPROVED_MESSAGE,
    '',
    `Approved cold first-pass candidates (Batch 1): ${count}`,
  ];
  const cedar = (review.sourceVerificationRequired || []).find((r) =>
    /cedar/i.test(String(r.companyName || r.company || ''))
  );
  if (cedar) {
    lines.push(
      'Cedar remains source-verification required — not approved into Batch 1.'
    );
  } else if ((review.sourceVerificationRequired || []).length) {
    lines.push(
      `Source-verification required accounts (${
        review.sourceVerificationRequired.length
      }) remain excluded from Batch 1.`
    );
  }
  const keyrenter = (review.existingRelationship || []).find((r) =>
    /keyrenter/i.test(String(r.companyName || r.company || ''))
  );
  if (keyrenter) {
    lines.push(
      'Keyrenter remains existing-relationship / nurture — not cold outreach.'
    );
  } else if ((review.existingRelationship || []).length) {
    lines.push(
      'Existing-relationship / nurture accounts remain excluded from cold outreach.'
    );
  }
  if ((review.optionalExpansion || []).length) {
    lines.push('Optional expansion candidates remain excluded.');
  }
  if ((review.rejected || []).length) {
    lines.push('Rejected candidates remain excluded.');
  }
  lines.push('');
  lines.push(BATCH_1_APPROVED_DISCLAIMER);
  if (opts.repeatAck) {
    lines.unshift(
      'Batch 1 is already approved — not re-rendering the Prospect Batch Review.',
      ''
    );
  }
  return lines.join('\n').trim();
}

function produceProspectBatchReviewApprovalResult(
  ctx,
  answers,
  slots,
  opts,
  leadIn
) {
  const priorReview = opts.priorProspectBatchReview || null;
  const alreadyApproved = isProspectBatchReviewAlreadyApproved(priorReview);

  let review = priorReview;
  if (!review || !review.acceptedFirstPass) {
    // Rebuild from Scout batch so approval still works if only the batch is loaded.
    const batch =
      (priorReview && priorReview.sourceScoutCandidateBatch) ||
      opts.priorScoutCandidateBatch ||
      (opts.priorScoutHandoff && opts.priorScoutHandoff.candidateBatch) ||
      (priorReview && priorReview.scoutCandidateBatch) ||
      null;
    if (hasCompletedScoutCandidateBatch(batch)) {
      review = buildProspectBatchReview(batch, {
        workRequestId:
          opts.workRequestId ||
          (priorReview && priorReview.workRequestId) ||
          null,
        relationshipOverrides:
          (priorReview && priorReview.relationshipOverrides) ||
          opts.relationshipOverrides ||
          [],
        userMessage: opts.userMessage || '',
        priorProspectBatchReview: priorReview,
      });
    }
  }

  if (!review) {
    return {
      message: [
        leadIn || 'Prospect Batch Review approval needs the active review.',
        '',
        'No Prospect Batch Review is loaded to approve as Batch 1.',
        BATCH_1_APPROVED_DISCLAIMER,
      ].join('\n'),
      step: CAMPAIGN_PLANNING_STATES.PROSPECT_BATCH_REVIEW,
      answers,
      slots: { ...slots },
      prospectBatchReview: null,
      outreachStrategyPreview: null,
      intent: 'prospect_batch_review_approval_missing',
      planningState: CAMPAIGN_PLANNING_STATES.PROSPECT_BATCH_REVIEW,
      currentAsk: PROSPECT_BATCH_REVIEW_CLOSING_QUESTION,
    };
  }

  const approvedReview = approveProspectBatchReviewBatch1(review, {
    approvedAt:
      (alreadyApproved &&
        (review.batch1ApprovedAt ||
          (review.approvedBatch && review.approvedBatch.approvedAt))) ||
      new Date().toISOString(),
  });
  const campaignMemory = applyBatchReviewLearnings(
    ensureCampaignMemory({ campaignMemory: opts.campaignMemory || null }),
    approvedReview
  );
  const existingStrategy = opts.priorOutreachStrategyPreview || null;
  const strategyWasStale = outreachStrategyPreviewLooksStale(existingStrategy);
  const outreachStrategyPreview = buildOutreachStrategyPreview(
    approvedReview,
    ctx,
    {
      workRequestId: approvedReview.workRequestId,
      priorOutreachStrategyPreview: existingStrategy,
      priorCriteriaPreview: opts.priorCriteriaPreview || null,
      priorPreview: opts.priorPreview || null,
      answers,
      blueprintId: opts.blueprintId,
      blueprintVersion: opts.blueprintVersion,
      reuseExisting: Boolean(existingStrategy) && !strategyWasStale,
      forceRebuild: strategyWasStale,
      campaignMemory,
    }
  );

  const message = [
    leadIn || null,
    formatProspectBatch1ApprovalMessage(approvedReview, {
      repeatAck: alreadyApproved,
    }),
    formatOutreachStrategyPreviewMessage(outreachStrategyPreview),
  ]
    .filter(Boolean)
    .join('\n\n');

  return {
    message,
    step: CAMPAIGN_PLANNING_STATES.OUTREACH_STRATEGY_PREVIEW,
    answers,
    slots: {
      ...slots,
      scoutHandoffBriefGenerated: true,
      scoutHandoffApproved: true,
      scoutHandoffQueued: true,
      buildProposalApproved: true,
      criteriaApproved: true,
      previewApproved: true,
      prospectBatchReviewApproved: true,
      batch1Approved: true,
      outreachStrategyPreviewGenerated: true,
    },
    preview: opts.priorPreview || null,
    criteriaPreview: opts.priorCriteriaPreview || null,
    buildProposal: opts.priorBuildProposal || null,
    prospectListDraft: opts.priorProspectListDraft || null,
    scoutHandoffBrief: opts.priorScoutHandoffBrief || null,
    scoutHandoff: opts.priorScoutHandoff || null,
    scoutWorkRequest: opts.priorScoutWorkRequest || null,
    scoutCandidateBatch:
      opts.priorScoutCandidateBatch ||
      approvedReview.scoutCandidateBatch ||
      null,
    prospectBatchReview: approvedReview,
    outreachStrategyPreview,
    campaignMemory,
    liveProspectList: null,
    intent: 'prospect_batch_1_approved',
    previewApproved: true,
    criteriaApproved: true,
    buildProposalApproved: true,
    prospectBatchReviewApproved: true,
    batch1Approved: true,
    liveSourcingApproved: false,
    planningState: CAMPAIGN_PLANNING_STATES.OUTREACH_STRATEGY_PREVIEW,
    currentAsk: OUTREACH_STRATEGY_PREVIEW_CLOSING_QUESTION,
    workRequestId: approvedReview.workRequestId || null,
    // Safety flags
    outreachCopyGenerated: false,
    sendsMade: false,
    crmWritesMade: false,
    exportMade: false,
    accountChangesMade: false,
  };
}

/**
 * Create or show Outreach Strategy Preview after Batch 1 approval.
 * Never re-renders Prospect Batch Review; never asks the operator to
 * request the strategy again.
 */
function produceOutreachStrategyPreviewResult(
  ctx,
  answers,
  slots,
  opts,
  leadIn
) {
  const priorReview = opts.priorProspectBatchReview || null;
  let review = priorReview;
  if (!isProspectBatchReviewAlreadyApproved(review)) {
    if (review && (review.acceptedFirstPass || []).length) {
      review = approveProspectBatchReviewBatch1(review);
    } else {
      const batch =
        opts.priorScoutCandidateBatch ||
        (opts.priorScoutHandoff && opts.priorScoutHandoff.candidateBatch) ||
        null;
      if (hasCompletedScoutCandidateBatch(batch)) {
        const built = buildProspectBatchReview(batch, {
          workRequestId: opts.workRequestId || null,
          userMessage: opts.userMessage || '',
          priorProspectBatchReview: priorReview,
        });
        review = approveProspectBatchReviewBatch1(built);
      }
    }
  } else if (!review.approvedBatch) {
    review = approveProspectBatchReviewBatch1(review);
  }

  if (!review) {
    return {
      message: [
        leadIn || 'Outreach Strategy Preview needs an approved Batch 1.',
        '',
        'Approve the accepted cold first-pass candidates as Batch 1 first.',
        BATCH_1_APPROVED_DISCLAIMER,
      ].join('\n'),
      step: CAMPAIGN_PLANNING_STATES.PROSPECT_BATCH_REVIEW,
      answers,
      slots: { ...slots },
      prospectBatchReview: priorReview,
      outreachStrategyPreview: null,
      intent: 'outreach_strategy_preview_missing_batch',
      planningState: CAMPAIGN_PLANNING_STATES.PROSPECT_BATCH_REVIEW,
      currentAsk:
        (priorReview && priorReview.closingQuestion) ||
        PROSPECT_BATCH_REVIEW_CLOSING_QUESTION,
    };
  }

  const existing = opts.priorOutreachStrategyPreview || null;
  const alreadyHave = hasOutreachStrategyPreview(existing);
  const strategyWasStale = alreadyHave && outreachStrategyPreviewLooksStale(existing);
  const outreachStrategyPreview = buildOutreachStrategyPreview(review, ctx, {
    workRequestId: review.workRequestId,
    priorOutreachStrategyPreview: existing,
    priorCriteriaPreview: opts.priorCriteriaPreview || null,
    priorPreview: opts.priorPreview || null,
    answers,
    blueprintId: opts.blueprintId,
    blueprintVersion: opts.blueprintVersion,
    reuseExisting: alreadyHave && !strategyWasStale,
    forceRebuild: strategyWasStale,
  });

  const intro = strategyWasStale
    ? 'Updated the Outreach Strategy Preview — repaired stale phrasing from an earlier draft. Showing it for approval or revision. Not re-rendering the Prospect Batch Review.'
    : alreadyHave
      ? 'Outreach Strategy Preview is already available — showing it for approval or revision. Not re-rendering the Prospect Batch Review.'
      : 'Creating the Outreach Strategy Preview from the approved Blueprint, campaign objective, Batch 1 cold prospects, and brand voice/differentiators.';

  const message = [
    leadIn || null,
    intro,
    '',
    formatOutreachStrategyPreviewMessage(outreachStrategyPreview),
  ]
    .filter(Boolean)
    .join('\n');

  return {
    message,
    step: CAMPAIGN_PLANNING_STATES.OUTREACH_STRATEGY_PREVIEW,
    answers,
    slots: {
      ...slots,
      previewApproved: true,
      criteriaApproved: true,
      buildProposalApproved: true,
      prospectBatchReviewApproved: true,
      batch1Approved: true,
      outreachStrategyPreviewGenerated: true,
    },
    preview: opts.priorPreview || null,
    criteriaPreview: opts.priorCriteriaPreview || null,
    buildProposal: opts.priorBuildProposal || null,
    prospectListDraft: opts.priorProspectListDraft || null,
    scoutHandoffBrief: opts.priorScoutHandoffBrief || null,
    scoutHandoff: opts.priorScoutHandoff || null,
    scoutWorkRequest: opts.priorScoutWorkRequest || null,
    scoutCandidateBatch:
      opts.priorScoutCandidateBatch || review.scoutCandidateBatch || null,
    prospectBatchReview: review,
    outreachStrategyPreview,
    liveProspectList: null,
    intent: strategyWasStale
      ? 'repair_outreach_strategy_preview'
      : alreadyHave
        ? 'show_outreach_strategy_preview'
        : 'produce_outreach_strategy_preview',
    previewApproved: true,
    criteriaApproved: true,
    buildProposalApproved: true,
    prospectBatchReviewApproved: true,
    batch1Approved: true,
    liveSourcingApproved: false,
    planningState: CAMPAIGN_PLANNING_STATES.OUTREACH_STRATEGY_PREVIEW,
    currentAsk: OUTREACH_STRATEGY_PREVIEW_CLOSING_QUESTION,
    workRequestId: review.workRequestId || null,
    outreachCopyGenerated: false,
    sendsMade: false,
    crmWritesMade: false,
    exportMade: false,
    accountChangesMade: false,
    repairedFromStale: Boolean(strategyWasStale || outreachStrategyPreview.repairedFromStale),
  };
}

function produceExecuteExistingScoutWorkRequestResult(
  ctx,
  answers,
  slots,
  opts,
  leadIn
) {
  const workRequestId =
    (opts && opts.workRequestId) ||
    extractWorkRequestIdFromMessage(opts && opts.userMessage) ||
    null;

  // Prefer session-completed Scout batch over re-sourcing or "not found".
  const priorBatch =
    opts.priorScoutCandidateBatch ||
    (opts.priorScoutHandoff && opts.priorScoutHandoff.candidateBatch) ||
    null;
  const wantsRetry = /\bretry|re-?run\b/i.test(String((opts && opts.userMessage) || ''));
  if (hasCompletedScoutCandidateBatch(priorBatch) && !wantsRetry) {
    return produceProspectBatchReviewResult(ctx, answers, slots, opts, leadIn);
  }

  const result = queueOrExecuteExistingScoutWorkRequest({
    workRequestId,
    handoff: opts.priorScoutHandoff || null,
    workRequest:
      (opts.priorScoutHandoff && opts.priorScoutHandoff.workRequest) || null,
    ...opts,
  });

  // Store lost the WR after deploy, but session still has the completed batch.
  if (
    !result.ok &&
    hasCompletedScoutCandidateBatch(priorBatch) &&
    !wantsRetry
  ) {
    return produceProspectBatchReviewResult(
      ctx,
      answers,
      slots,
      opts,
      leadIn ||
        'Scout work request already completed in this session — presenting Prospect Batch Review from that result.'
    );
  }

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

  const planningState = planningStateForHandoffResult(result);
  const lines = [];
  if (leadIn) lines.push(leadIn, '');
  lines.push(result.message || '');

  const priorBrief = opts.priorScoutHandoffBrief || null;
  const nextBrief = priorBrief
    ? {
        ...priorBrief,
        handoffId: result.handoff && result.handoff.handoffId,
        scoutHandoff: result.handoff || priorBrief.scoutHandoff,
        status: (result.handoff && result.handoff.status) || priorBrief.status,
        uiStatus:
          (result.handoff && result.handoff.uiStatus) || priorBrief.uiStatus,
        handedToScout: true,
        scoutRan: Boolean(result.scoutRan),
        liveSourcingPerformed: false,
        prospectListGenerated: false,
        workRequestId:
          (result.workRequest && result.workRequest.workRequestId) ||
          workRequestId,
        updatedAt:
          (result.handoff && result.handoff.updatedAt) || priorBrief.updatedAt,
        recommendedNextStep: result.scoutRan
          ? result.ok
            ? 'Review Scout candidates. Approve before Composer / CRM / export use. No outreach or CRM writes yet.'
            : 'Scout sourcing failed — work request preserved. Review failure and retry or revise criteria.'
          : result.shouldExecuteScoutSourcing
            ? 'Scout work request execution queued — public-source sourcing will run next.'
            : result.sourcingUnavailable
              ? 'Scout sourcing execution is the next build gap — wire Scout public-source sourcing.'
              : priorBrief.recommendedNextStep,
      }
    : null;

  return {
    message: lines.filter((l, i) => !(l === '' && i === lines.length - 1)).join('\n'),
    step: planningState,
    answers,
    slots: {
      ...briefSlots,
      scoutHandoffBriefGenerated: Boolean(
        briefSlots.scoutHandoffBriefGenerated || nextBrief
      ),
      scoutHandoffApproved: Boolean(result.workRequest),
      scoutHandoffQueued: Boolean(
        result.workRequest &&
          (result.executionWired || result.sourcingUnavailable || result.ok === false)
      ),
    },
    preview: opts.priorPreview || null,
    criteriaPreview: opts.priorCriteriaPreview || null,
    buildProposal: opts.priorBuildProposal || null,
    prospectListDraft: opts.priorProspectListDraft || null,
    scoutHandoffBrief: nextBrief,
    scoutHandoff: result.handoff || opts.priorScoutHandoff || null,
    scoutWorkRequest: result.workRequest || null,
    scoutCandidateBatch: result.candidateBatch || null,
    liveProspectList: null,
    intent: result.intent,
    shouldExecuteScoutSourcing: Boolean(result.shouldExecuteScoutSourcing),
    createdNewHandoff: false,
    previewApproved: true,
    criteriaApproved: true,
    buildProposalApproved: Boolean(briefSlots.buildProposalApproved),
    liveSourcingApproved: false,
    planningState,
    currentAsk: null,
    workRequestId:
      (result.workRequest && result.workRequest.workRequestId) || workRequestId,
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
      'Cleaning companies, maid services, housekeeping, janitorial, carpet cleaning, and cleaning competitors',
      'Highly complex properties',
      'Lowest-price buyers',
      `Properties outside ${poss} New Hampshire, USA service area`,
      'UK Greater Manchester / Salford / Stockport or other non-US results',
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
  return /^(Large institutional|Cleaning companies|Highly complex|Lowest-price|Properties outside|UK Greater Manchester|Prospects with)\b/i.test(
    s
  );
}

function defaultMarketBound(context) {
  const rawMarket = (context && context.targetMarket) || 'New Hampshire, USA';
  const market = /greater\s+manchester/i.test(String(rawMarket))
    ? 'New Hampshire, USA (Manchester NH nearby/fill only)'
    : rawMarket;
  const towns = (
    context && Array.isArray(context.towns) && context.towns.length
      ? context.towns
      : DEFAULT_TOWNS
  ).slice(0, 5);
  return (
    `Start with ${naturalList(towns.map((t) => (/NH|New Hampshire/i.test(t) ? t : `${t} NH`)))}. ` +
    `Market is ${market}. Prioritize Bedford NH, Hooksett NH, Londonderry NH, Auburn NH, and Goffstown NH; ` +
    `treat Manchester NH as nearby/review-required unless needed to fill the batch.`
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
  // Durable campaign memory survives step transitions.
  if (!opts.campaignMemory) {
    opts = {
      ...opts,
      campaignMemory:
        prior.campaignMemory ||
        (prior.campaignPlanning && prior.campaignPlanning.campaignMemory) ||
        null,
    };
  }
  if (!opts.campaignWorkingState) {
    opts = {
      ...opts,
      campaignWorkingState:
        prior.campaignWorkingState ||
        (prior.campaignPlanning && prior.campaignPlanning.campaignWorkingState) ||
        null,
    };
  }
  if (!opts.priorOutreachDraftPreview && prior.outreachDraftPreview) {
    opts = {
      ...opts,
      priorOutreachDraftPreview: prior.outreachDraftPreview,
    };
  }
  if (!opts.priorOutreachLaunchGate && prior.outreachLaunchGate) {
    opts = {
      ...opts,
      priorOutreachLaunchGate: prior.outreachLaunchGate,
    };
  }
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

  // Execute / retry an existing Scout work request by ID — never create a new handoff
  // and never fall through to "ask me to generate batch when ready".
  if (
    looksLikeExecuteExistingScoutWorkRequest(userMessage) ||
    classifyProspectAcquisitionIntent(userMessage) ===
      PROSPECT_ACQUISITION_INTENTS.EXECUTE_EXISTING_SCOUT_WORK_REQUEST
  ) {
    const answersEarly = { ...(prior.answers || {}) };
    const syncedEarly = syncAnswersFromSlots(answersEarly, {
      ...priorSlots,
      previewGenerated: true,
      previewApproved: true,
      criteriaGenerated: true,
      criteriaApproved: true,
      buildProposalGenerated: true,
      buildProposalApproved: true,
    });
    return produceExecuteExistingScoutWorkRequestResult(
      ctx,
      syncedEarly,
      {
        ...priorSlots,
        previewGenerated: true,
        previewApproved: true,
        criteriaGenerated: true,
        criteriaApproved: true,
        buildProposalGenerated: true,
        buildProposalApproved: true,
      },
      {
        ...opts,
        userMessage,
        workRequestId: extractWorkRequestIdFromMessage(userMessage),
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
        priorScoutHandoffBrief:
          opts.priorScoutHandoffBrief || prior.scoutHandoffBrief || null,
        priorScoutHandoff:
          opts.priorScoutHandoff ||
          prior.scoutHandoff ||
          (prior.scoutHandoffBrief && prior.scoutHandoffBrief.scoutHandoff) ||
          null,
      },
      'Executing the existing Scout work request. No new handoff will be created.'
    );
  }

  // HARD GUARD — Review-artifact chain: Strategy / Copy Plan / Draft / Launch Gate.
  // Generic rule: approve current → show-or-create next; never re-render approved.
  {
    const priorBatchReviewEarly =
      opts.priorProspectBatchReview || prior.prospectBatchReview || null;
    const priorOutreachEarly =
      opts.priorOutreachStrategyPreview ||
      prior.outreachStrategyPreview ||
      null;
    const priorCopyPlanEarly =
      opts.priorOutreachCopyPlan || prior.outreachCopyPlan || null;
    const priorDraftEarly =
      opts.priorOutreachDraftPreview || prior.outreachDraftPreview || null;
    const priorLaunchGateEarly =
      opts.priorOutreachLaunchGate || prior.outreachLaunchGate || null;
    const approvalOpts = {
      priorProspectBatchReview: priorBatchReviewEarly,
      priorOutreachStrategyPreview: priorOutreachEarly,
      priorOutreachCopyPlan: priorCopyPlanEarly,
      priorOutreachDraftPreview: priorDraftEarly,
      priorOutreachLaunchGate: priorLaunchGateEarly,
      step: prior.step,
      memory: opts.reasoningMemory || null,
      messageClass: opts.messageClass || null,
      state: opts.reasoningState || null,
      slots: priorSlots,
    };
    const acquisitionEarly = classifyProspectAcquisitionIntent(
      userMessage,
      approvalOpts
    );

    const sharedApprovedSlots = {
      ...priorSlots,
      previewGenerated: true,
      previewApproved: true,
      criteriaGenerated: true,
      criteriaApproved: true,
      buildProposalGenerated: true,
      buildProposalApproved: true,
      prospectBatchReviewApproved: true,
      batch1Approved: true,
      outreachStrategyPreviewGenerated: true,
      outreachStrategyPreviewApproved: true,
      strategyApproved: true,
    };

    const sharedReplyOpts = {
      ...opts,
      userMessage,
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
      priorScoutHandoffBrief:
        opts.priorScoutHandoffBrief || prior.scoutHandoffBrief || null,
      priorScoutHandoff:
        opts.priorScoutHandoff ||
        prior.scoutHandoff ||
        (prior.scoutHandoffBrief && prior.scoutHandoffBrief.scoutHandoff) ||
        null,
      priorScoutCandidateBatch:
        opts.priorScoutCandidateBatch ||
        prior.scoutCandidateBatch ||
        (prior.scoutHandoff && prior.scoutHandoff.candidateBatch) ||
        null,
      priorScoutWorkRequest:
        opts.priorScoutWorkRequest || prior.scoutWorkRequest || null,
      priorProspectBatchReview: priorBatchReviewEarly,
      priorOutreachStrategyPreview: priorOutreachEarly,
      priorOutreachCopyPlan: priorCopyPlanEarly,
      priorOutreachDraftPreview: priorDraftEarly,
      priorOutreachLaunchGate: priorLaunchGateEarly,
    };

    // Approve Launch Gate (readiness only).
    if (
      acquisitionEarly ===
        PROSPECT_ACQUISITION_INTENTS.APPROVE_OUTREACH_LAUNCH_GATE ||
      looksLikeOutreachLaunchGateApproval(userMessage, approvalOpts)
    ) {
      const answersEarly = { ...(prior.answers || {}) };
      const syncedEarly = syncAnswersFromSlots(answersEarly, {
        ...sharedApprovedSlots,
        outreachCopyPlanApproved: true,
        copyPlanApproved: true,
        outreachDraftPreviewApproved: true,
        draftPreviewApproved: true,
      });
      return produceOutreachLaunchGateApprovalResult(
        ctx,
        syncedEarly,
        {
          ...sharedApprovedSlots,
          outreachCopyPlanApproved: true,
          copyPlanApproved: true,
          outreachDraftPreviewApproved: true,
          draftPreviewApproved: true,
        },
        sharedReplyOpts,
        null
      );
    }

    // Execution path after readiness approval — confirm only, never auto-run.
    if (
      looksLikeExecutionRequest(userMessage) &&
      (isOutreachLaunchGateAlreadyApproved(priorLaunchGateEarly) ||
        prior.launchGateApproved === true ||
        priorSlots.launchGateApproved === true ||
        sharedApprovedSlots.launchGateApproved === true)
    ) {
      return produceExecutionConfirmationResult(userMessage, prior, {
        ...sharedReplyOpts,
        priorOutreachLaunchGate: priorLaunchGateEarly,
        answers: { ...(prior.answers || {}) },
        slots: { ...sharedApprovedSlots },
      });
    }

    // Approve Draft Preview → Launch Gate.
    if (
      acquisitionEarly ===
        PROSPECT_ACQUISITION_INTENTS.APPROVE_OUTREACH_DRAFT_PREVIEW ||
      looksLikeOutreachDraftPreviewApproval(userMessage, approvalOpts)
    ) {
      const answersEarly = { ...(prior.answers || {}) };
      const syncedEarly = syncAnswersFromSlots(answersEarly, {
        ...sharedApprovedSlots,
        outreachCopyPlanApproved: true,
        copyPlanApproved: true,
      });
      return produceOutreachDraftPreviewApprovalResult(
        ctx,
        syncedEarly,
        {
          ...sharedApprovedSlots,
          outreachCopyPlanApproved: true,
          copyPlanApproved: true,
        },
        sharedReplyOpts,
        OUTREACH_DRAFT_PREVIEW_APPROVED_MESSAGE
      );
    }

    // Approve Copy Plan → Draft Preview.
    if (
      acquisitionEarly ===
        PROSPECT_ACQUISITION_INTENTS.APPROVE_OUTREACH_COPY_PLAN ||
      looksLikeOutreachCopyPlanApproval(userMessage, approvalOpts)
    ) {
      const answersEarly = { ...(prior.answers || {}) };
      const syncedEarly = syncAnswersFromSlots(answersEarly, sharedApprovedSlots);
      return produceOutreachCopyPlanApprovalResult(
        ctx,
        syncedEarly,
        sharedApprovedSlots,
        sharedReplyOpts,
        OUTREACH_COPY_PLAN_APPROVED_MESSAGE
      );
    }

    // Approve strategy → Copy Plan (wins over re-showing strategy).
    if (
      acquisitionEarly ===
        PROSPECT_ACQUISITION_INTENTS.APPROVE_OUTREACH_STRATEGY_PREVIEW ||
      looksLikeOutreachStrategyPreviewApproval(userMessage, approvalOpts)
    ) {
      const answersEarly = { ...(prior.answers || {}) };
      const syncedEarly = syncAnswersFromSlots(answersEarly, sharedApprovedSlots);
      return produceOutreachStrategyApprovalResult(
        ctx,
        syncedEarly,
        sharedApprovedSlots,
        sharedReplyOpts,
        OUTREACH_STRATEGY_APPROVED_MESSAGE
      );
    }

    // Explicit create/show of Launch Gate after draft approval.
    if (
      acquisitionEarly ===
        PROSPECT_ACQUISITION_INTENTS.EMIT_OUTREACH_LAUNCH_GATE ||
      (looksLikeOutreachLaunchGateRequest(userMessage) &&
        canEmitOutreachLaunchGate(approvalOpts)) ||
      (canEmitOutreachLaunchGate(approvalOpts) &&
        (prior.step === CAMPAIGN_PLANNING_STATES.OUTREACH_LAUNCH_GATE ||
          prior.step ===
            CAMPAIGN_PLANNING_STATES.OUTREACH_DRAFT_PREVIEW_APPROVED) &&
        !looksLikeOutreachDraftPreviewRequest(userMessage))
    ) {
      const answersEarly = { ...(prior.answers || {}) };
      const syncedEarly = syncAnswersFromSlots(answersEarly, {
        ...sharedApprovedSlots,
        outreachCopyPlanApproved: true,
        copyPlanApproved: true,
        outreachDraftPreviewApproved: true,
        draftPreviewApproved: true,
      });
      return produceOutreachLaunchGateResult(
        ctx,
        syncedEarly,
        {
          ...sharedApprovedSlots,
          outreachCopyPlanApproved: true,
          copyPlanApproved: true,
          outreachDraftPreviewApproved: true,
          draftPreviewApproved: true,
        },
        sharedReplyOpts,
        null
      );
    }

    // Confirmed force-rebuild from operator instructions only.
    // Must win over revision, silent re-show, and stored artifact reuse.
    if (
      looksLikeForceRebuildConfirmation(userMessage, {
        campaignWorkingState:
          opts.campaignWorkingState || prior.campaignWorkingState || null,
        awaitingForceRebuildConfirmation:
          (opts.campaignWorkingState &&
            opts.campaignWorkingState.awaitingForceRebuildConfirmation) ||
          (prior.campaignWorkingState &&
            prior.campaignWorkingState.awaitingForceRebuildConfirmation) ||
          false,
        lastResponseMode:
          (opts.campaignWorkingState &&
            opts.campaignWorkingState.lastResponseMode) ||
          (prior.campaignWorkingState &&
            prior.campaignWorkingState.lastResponseMode) ||
          null,
      }) &&
      (canEmitOutreachDraftPreview(approvalOpts) ||
        hasOutreachDraftPreview(priorDraftEarly) ||
        prior.step === CAMPAIGN_PLANNING_STATES.OUTREACH_DRAFT_PREVIEW ||
        prior.step === CAMPAIGN_PLANNING_STATES.OUTREACH_COPY_PLAN_APPROVED ||
        ((opts.campaignWorkingState || prior.campaignWorkingState || {})
          .lastResponseMode === RESPONSE_MODES.STALE_SOURCE_DIAGNOSTIC))
    ) {
      const answersEarly = { ...(prior.answers || {}) };
      const syncedEarly = syncAnswersFromSlots(answersEarly, {
        ...sharedApprovedSlots,
        outreachCopyPlanApproved: true,
        copyPlanApproved: true,
      });
      return produceOutreachDraftPreviewForceRebuildResult(
        ctx,
        syncedEarly,
        {
          ...sharedApprovedSlots,
          outreachCopyPlanApproved: true,
          copyPlanApproved: true,
        },
        {
          ...sharedReplyOpts,
          // Explicitly drop stored draft from the reply opts.
          priorOutreachDraftPreview: null,
          campaignMemory: opts.campaignMemory || prior.campaignMemory || null,
          campaignWorkingState:
            opts.campaignWorkingState || prior.campaignWorkingState || null,
        },
        userMessage
      );
    }

    // Operator chat revision of the active Outreach Draft Preview.
    // Must win over silent re-show / template reuse so corrections apply.
    if (
      looksLikeOperatorWorkflowRevision(userMessage, {
        ...approvalOpts,
        priorOutreachDraftPreview: priorDraftEarly,
        outreachDraftPreview: priorDraftEarly,
        step: prior.step,
        messageClass: opts.messageClass || null,
      }) &&
      (canEmitOutreachDraftPreview(approvalOpts) ||
        hasOutreachDraftPreview(priorDraftEarly) ||
        prior.step === CAMPAIGN_PLANNING_STATES.OUTREACH_DRAFT_PREVIEW ||
        prior.step === CAMPAIGN_PLANNING_STATES.OUTREACH_COPY_PLAN_APPROVED)
    ) {
      const answersEarly = { ...(prior.answers || {}) };
      const syncedEarly = syncAnswersFromSlots(answersEarly, {
        ...sharedApprovedSlots,
        outreachCopyPlanApproved: true,
        copyPlanApproved: true,
      });
      return produceOutreachDraftPreviewRevisionResult(
        ctx,
        syncedEarly,
        {
          ...sharedApprovedSlots,
          outreachCopyPlanApproved: true,
          copyPlanApproved: true,
        },
        {
          ...sharedReplyOpts,
          campaignMemory: opts.campaignMemory || prior.campaignMemory || null,
          campaignWorkingState:
            opts.campaignWorkingState || prior.campaignWorkingState || null,
        },
        userMessage
      );
    }

    // Explicit create/show of Draft Preview after copy-plan approval.
    if (
      acquisitionEarly ===
        PROSPECT_ACQUISITION_INTENTS.EMIT_OUTREACH_DRAFT_PREVIEW ||
      (looksLikeOutreachDraftPreviewRequest(userMessage) &&
        canEmitOutreachDraftPreview(approvalOpts)) ||
      (canEmitOutreachDraftPreview(approvalOpts) &&
        (prior.step === CAMPAIGN_PLANNING_STATES.OUTREACH_DRAFT_PREVIEW ||
          prior.step === CAMPAIGN_PLANNING_STATES.OUTREACH_COPY_PLAN_APPROVED) &&
        !looksLikeOutreachCopyPlanRequest(userMessage) &&
        !looksLikeOutreachLaunchGateRequest(userMessage) &&
        !looksLikeForceRebuildConfirmation(userMessage, {
          campaignWorkingState:
            opts.campaignWorkingState || prior.campaignWorkingState || null,
        }) &&
        !looksLikeOperatorWorkflowRevision(userMessage, {
          ...approvalOpts,
          priorOutreachDraftPreview: priorDraftEarly,
          step: prior.step,
          messageClass: opts.messageClass || null,
        }))
    ) {
      const answersEarly = { ...(prior.answers || {}) };
      const syncedEarly = syncAnswersFromSlots(answersEarly, {
        ...sharedApprovedSlots,
        outreachCopyPlanApproved: true,
        copyPlanApproved: true,
      });
      return produceOutreachDraftPreviewResult(
        ctx,
        syncedEarly,
        {
          ...sharedApprovedSlots,
          outreachCopyPlanApproved: true,
          copyPlanApproved: true,
        },
        sharedReplyOpts,
        null
      );
    }

    // Explicit create/show of Outreach Copy Plan after strategy approval.
    if (
      acquisitionEarly === PROSPECT_ACQUISITION_INTENTS.EMIT_OUTREACH_COPY_PLAN ||
      (looksLikeOutreachCopyPlanRequest(userMessage) &&
        canEmitOutreachCopyPlan(approvalOpts) &&
        !canEmitOutreachDraftPreview(approvalOpts)) ||
      (canEmitOutreachCopyPlan(approvalOpts) &&
        !canEmitOutreachDraftPreview(approvalOpts) &&
        (prior.step === CAMPAIGN_PLANNING_STATES.OUTREACH_COPY_PLAN ||
          prior.step ===
            CAMPAIGN_PLANNING_STATES.OUTREACH_STRATEGY_PREVIEW_APPROVED) &&
        !looksLikeOutreachStrategyPreviewRequest(userMessage))
    ) {
      const answersEarly = { ...(prior.answers || {}) };
      const syncedEarly = syncAnswersFromSlots(answersEarly, sharedApprovedSlots);
      return produceOutreachCopyPlanResult(
        ctx,
        syncedEarly,
        sharedApprovedSlots,
        sharedReplyOpts,
        null
      );
    }

    // Explicit create/show of Outreach Strategy Preview after Batch 1 approval.
    // Do not catch strategy approvals or copy-plan asks.
    if (
      acquisitionEarly ===
        PROSPECT_ACQUISITION_INTENTS.EMIT_OUTREACH_STRATEGY_PREVIEW ||
      (looksLikeOutreachStrategyPreviewRequest(userMessage) &&
        canEmitOutreachStrategyPreview(approvalOpts) &&
        !isOutreachStrategyPreviewAlreadyApproved(priorOutreachEarly)) ||
      (isProspectBatchReviewAlreadyApproved(priorBatchReviewEarly) &&
        (prior.step === CAMPAIGN_PLANNING_STATES.OUTREACH_STRATEGY_PREVIEW ||
          prior.step === CAMPAIGN_PLANNING_STATES.PROSPECT_BATCH_1_APPROVED) &&
        !hasOutreachStrategyPreview(priorOutreachEarly) &&
        !looksLikeProspectBatchReviewApproval(userMessage, approvalOpts) &&
        !looksLikeProspectBatchReviewCorrection(userMessage, approvalOpts) &&
        !looksLikeOutreachStrategyPreviewApproval(userMessage, approvalOpts) &&
        !looksLikeOutreachCopyPlanRequest(userMessage) &&
        !looksLikeOutreachDraftPreviewRequest(userMessage) &&
        !looksLikeOutreachLaunchGateRequest(userMessage))
    ) {
      const answersEarly = { ...(prior.answers || {}) };
      const syncedEarly = syncAnswersFromSlots(answersEarly, {
        ...priorSlots,
        previewGenerated: true,
        previewApproved: true,
        criteriaGenerated: true,
        criteriaApproved: true,
        buildProposalGenerated: true,
        buildProposalApproved: true,
        prospectBatchReviewApproved: true,
        batch1Approved: true,
      });
      return produceOutreachStrategyPreviewResult(
        ctx,
        syncedEarly,
        {
          ...priorSlots,
          previewGenerated: true,
          previewApproved: true,
          criteriaGenerated: true,
          criteriaApproved: true,
          buildProposalGenerated: true,
          buildProposalApproved: true,
          prospectBatchReviewApproved: true,
          batch1Approved: true,
        },
        sharedReplyOpts,
        null
      );
    }

    if (
      looksLikeProspectBatchReviewApproval(userMessage, approvalOpts) ||
      acquisitionEarly ===
        PROSPECT_ACQUISITION_INTENTS.APPROVE_PROSPECT_BATCH_REVIEW ||
      (isProspectBatchReviewAlreadyApproved(priorBatchReviewEarly) &&
        !hasOutreachStrategyPreview(priorOutreachEarly) &&
        (looksLikeApproval(userMessage) ||
          looksLikeApprovalLead(userMessage) ||
          /\bapprov(?:e|ed|ing)\b[\s\S]{0,160}\b(?:batch\s*1|accepted\s+cold\s+first[- ]pass|first[- ]pass)\b/i.test(
            userMessage
          )))
    ) {
      // Only short-circuit when there is an active or already-approved review.
      // When strategy already exists, pure approval advances to Copy Plan above.
      if (
        priorBatchReviewEarly ||
        prior.step === CAMPAIGN_PLANNING_STATES.PROSPECT_BATCH_REVIEW ||
        prior.step === CAMPAIGN_PLANNING_STATES.OUTREACH_STRATEGY_PREVIEW ||
        prior.step === CAMPAIGN_PLANNING_STATES.PROSPECT_BATCH_1_APPROVED
      ) {
        const answersEarly = { ...(prior.answers || {}) };
        const syncedEarly = syncAnswersFromSlots(answersEarly, {
          ...priorSlots,
          previewGenerated: true,
          previewApproved: true,
          criteriaGenerated: true,
          criteriaApproved: true,
          buildProposalGenerated: true,
          buildProposalApproved: true,
          prospectBatchReviewApproved: true,
          batch1Approved: true,
        });
        return produceProspectBatchReviewApprovalResult(
          ctx,
          syncedEarly,
          {
            ...priorSlots,
            previewGenerated: true,
            previewApproved: true,
            criteriaGenerated: true,
            criteriaApproved: true,
            buildProposalGenerated: true,
            buildProposalApproved: true,
            prospectBatchReviewApproved: true,
            batch1Approved: true,
          },
          sharedReplyOpts,
          null
        );
      }
    }
  }

  // HARD GUARD — correction against an active Prospect Batch Review.
  // Never fall back to "Build proposal already approved".
  {
    const priorBatchReviewEarly =
      opts.priorProspectBatchReview || prior.prospectBatchReview || null;
    const correctionOpts = {
      priorProspectBatchReview: priorBatchReviewEarly,
      step: prior.step,
      memory: opts.reasoningMemory || null,
      messageClass: opts.messageClass || null,
      state: opts.reasoningState || null,
    };
    if (
      looksLikeProspectBatchReviewCorrection(userMessage, correctionOpts) ||
      classifyProspectAcquisitionIntent(userMessage, correctionOpts) ===
        PROSPECT_ACQUISITION_INTENTS.CORRECT_PROSPECT_BATCH_REVIEW
    ) {
      const answersEarly = { ...(prior.answers || {}) };
      const syncedEarly = syncAnswersFromSlots(answersEarly, {
        ...priorSlots,
        previewGenerated: true,
        previewApproved: true,
        criteriaGenerated: true,
        criteriaApproved: true,
        buildProposalGenerated: true,
        buildProposalApproved: true,
      });
      return produceProspectBatchReviewResult(
        ctx,
        syncedEarly,
        {
          ...priorSlots,
          previewGenerated: true,
          previewApproved: true,
          criteriaGenerated: true,
          criteriaApproved: true,
          buildProposalGenerated: true,
          buildProposalApproved: true,
        },
        {
          ...opts,
          userMessage,
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
          priorScoutHandoffBrief:
            opts.priorScoutHandoffBrief || prior.scoutHandoffBrief || null,
          priorScoutHandoff:
            opts.priorScoutHandoff ||
            prior.scoutHandoff ||
            (prior.scoutHandoffBrief && prior.scoutHandoffBrief.scoutHandoff) ||
            null,
          priorScoutCandidateBatch:
            opts.priorScoutCandidateBatch ||
            prior.scoutCandidateBatch ||
            (prior.scoutHandoff && prior.scoutHandoff.candidateBatch) ||
            null,
          priorScoutWorkRequest:
            opts.priorScoutWorkRequest || prior.scoutWorkRequest || null,
          priorProspectBatchReview: priorBatchReviewEarly,
        },
        'Updated Prospect Batch Review with your relationship correction. No outreach copy, sends, CRM writes, or account changes.'
      );
    }
  }

  // Hand brief to Scout — approve + queue work request (or clear not-wired boundary).
  if (
    looksLikeHandBriefToScoutRequest(userMessage) ||
    classifyProspectAcquisitionIntent(userMessage) ===
      PROSPECT_ACQUISITION_INTENTS.HAND_BRIEF_TO_SCOUT
  ) {
    const answersEarly = { ...(prior.answers || {}) };
    const syncedEarly = syncAnswersFromSlots(answersEarly, {
      ...priorSlots,
      previewGenerated: true,
      previewApproved: true,
      criteriaGenerated: true,
      criteriaApproved: true,
    });
    return produceHandBriefToScoutResult(
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
        priorScoutHandoffBrief:
          opts.priorScoutHandoffBrief || prior.scoutHandoffBrief || null,
        priorScoutHandoff:
          opts.priorScoutHandoff ||
          prior.scoutHandoff ||
          (prior.scoutHandoffBrief && prior.scoutHandoffBrief.scoutHandoff) ||
          null,
      },
      'Approving the Scout Handoff Brief and creating a Scout work request. Max will not claim Scout inspected sources unless Scout actually ran.'
    );
  }

  // Scout Handoff Brief — planning artifact only; does not queue Scout.
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
        priorScoutHandoffBrief:
          opts.priorScoutHandoffBrief || prior.scoutHandoffBrief || null,
        priorScoutHandoff:
          opts.priorScoutHandoff ||
          prior.scoutHandoff ||
          (prior.scoutHandoffBrief && prior.scoutHandoffBrief.scoutHandoff) ||
          null,
      },
      'Creating the Scout Handoff Brief from approved campaign/list criteria. This is planning only — say “Hand this brief to Scout” to approve and queue Scout.'
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

  // Completed Scout batch means campaign planning already advanced past preview /
  // criteria / build proposal — do not fall back to stale pre-preview "advance".
  const priorScoutBatchSeed =
    opts.priorScoutCandidateBatch ||
    prior.scoutCandidateBatch ||
    (prior.scoutHandoff && prior.scoutHandoff.candidateBatch) ||
    null;
  if (
    hasCompletedScoutCandidateBatch(priorScoutBatchSeed) ||
    prior.step === CAMPAIGN_PLANNING_STATES.SCOUT_HANDOFF_COMPLETED ||
    prior.step === CAMPAIGN_PLANNING_STATES.PROSPECT_BATCH_REVIEW
  ) {
    slots.previewGenerated = true;
    slots.previewApproved = true;
    slots.criteriaGenerated = true;
    slots.criteriaApproved = true;
    slots.buildProposalGenerated = true;
    slots.buildProposalApproved = true;
  }
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
      priorProspectBatchReview:
        opts.priorProspectBatchReview || prior.prospectBatchReview || null,
      priorOutreachStrategyPreview:
        opts.priorOutreachStrategyPreview ||
        prior.outreachStrategyPreview ||
        null,
      priorOutreachCopyPlan:
        opts.priorOutreachCopyPlan || prior.outreachCopyPlan || null,
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
      (inferredMemory.approvedArtifacts || []).includes(
        ARTIFACT_KINDS.OUTREACH_STRATEGY_PREVIEW
      )
    ) {
      slots.outreachStrategyPreviewApproved = true;
      slots.strategyApproved = true;
      slots.outreachStrategyPreviewGenerated = true;
      slots.batch1Approved = true;
      slots.prospectBatchReviewApproved = true;
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
      const priorScoutBatchForAction =
        opts.priorScoutCandidateBatch ||
        prior.scoutCandidateBatch ||
        (prior.scoutHandoff && prior.scoutHandoff.candidateBatch) ||
        null;
      const priorScoutHandoffForAction =
        opts.priorScoutHandoff ||
        prior.scoutHandoff ||
        (prior.scoutHandoffBrief && prior.scoutHandoffBrief.scoutHandoff) ||
        null;
      const priorProspectBatchReviewForAction =
        opts.priorProspectBatchReview ||
        prior.prospectBatchReview ||
        null;
      const priorOutreachStrategyForAction =
        opts.priorOutreachStrategyPreview ||
        prior.outreachStrategyPreview ||
        null;
      const artifactAction =
        opts.artifactAction ||
        resolveCampaignArtifactAction({
          userMessage,
          messageClass: opts.messageClass || null,
          state: opts.reasoningState || { reasoningMemory: opts.reasoningMemory },
          priorCriteriaPreview: priorCriteria,
          priorBuildProposal: replyOpts.priorBuildProposal,
          priorProspectListDraft: replyOpts.priorProspectListDraft,
          priorScoutCandidateBatch: priorScoutBatchForAction,
          priorScoutHandoff: priorScoutHandoffForAction,
          priorProspectBatchReview: priorProspectBatchReviewForAction,
          priorOutreachStrategyPreview: priorOutreachStrategyForAction,
          step: prior.step || 'prospect_list_criteria_preview',
          slots,
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

      if (artifactAction.action === 'emit_prospect_batch_review') {
        return produceProspectBatchReviewResult(
          ctx,
          syncedAnswers,
          {
            ...slots,
            previewApproved: true,
            criteriaGenerated: true,
            criteriaApproved: true,
            buildProposalGenerated: true,
            buildProposalApproved: true,
          },
          {
            ...replyOpts,
            userMessage,
            workRequestId: artifactAction.workRequestId || null,
            priorScoutHandoffBrief:
              opts.priorScoutHandoffBrief ||
              prior.scoutHandoffBrief ||
              null,
            priorScoutHandoff:
              opts.priorScoutHandoff ||
              prior.scoutHandoff ||
              (prior.scoutHandoffBrief && prior.scoutHandoffBrief.scoutHandoff) ||
              null,
            priorScoutCandidateBatch:
              opts.priorScoutCandidateBatch ||
              prior.scoutCandidateBatch ||
              (prior.scoutHandoff && prior.scoutHandoff.candidateBatch) ||
              null,
            priorScoutWorkRequest:
              opts.priorScoutWorkRequest || prior.scoutWorkRequest || null,
            priorProspectBatchReview:
              opts.priorProspectBatchReview ||
              prior.prospectBatchReview ||
              null,
            relationshipOverrides: opts.relationshipOverrides || null,
          },
          artifactAction.note ||
            'Prospect Batch Review from the latest completed Scout result. No new batch will be generated.'
        );
      }

      if (
        artifactAction.action === 'approve_prospect_batch_review' ||
        artifactAction.action === 'ack_prospect_batch_approval'
      ) {
        return produceProspectBatchReviewApprovalResult(
          ctx,
          syncedAnswers,
          {
            ...slots,
            previewApproved: true,
            criteriaGenerated: true,
            criteriaApproved: true,
            buildProposalGenerated: true,
            buildProposalApproved: true,
            prospectBatchReviewApproved: true,
            batch1Approved: true,
          },
          {
            ...replyOpts,
            userMessage,
            priorScoutHandoffBrief:
              opts.priorScoutHandoffBrief || prior.scoutHandoffBrief || null,
            priorScoutHandoff:
              opts.priorScoutHandoff ||
              prior.scoutHandoff ||
              (prior.scoutHandoffBrief && prior.scoutHandoffBrief.scoutHandoff) ||
              null,
            priorScoutCandidateBatch:
              opts.priorScoutCandidateBatch ||
              prior.scoutCandidateBatch ||
              (prior.scoutHandoff && prior.scoutHandoff.candidateBatch) ||
              null,
            priorScoutWorkRequest:
              opts.priorScoutWorkRequest || prior.scoutWorkRequest || null,
            priorProspectBatchReview:
              opts.priorProspectBatchReview ||
              prior.prospectBatchReview ||
              null,
            priorOutreachStrategyPreview:
              opts.priorOutreachStrategyPreview ||
              prior.outreachStrategyPreview ||
              null,
          },
          null
        );
      }

      if (artifactAction.action === 'emit_outreach_strategy_preview') {
        return produceOutreachStrategyPreviewResult(
          ctx,
          syncedAnswers,
          {
            ...slots,
            previewApproved: true,
            criteriaGenerated: true,
            criteriaApproved: true,
            buildProposalGenerated: true,
            buildProposalApproved: true,
            prospectBatchReviewApproved: true,
            batch1Approved: true,
          },
          {
            ...replyOpts,
            userMessage,
            priorScoutHandoffBrief:
              opts.priorScoutHandoffBrief || prior.scoutHandoffBrief || null,
            priorScoutHandoff:
              opts.priorScoutHandoff ||
              prior.scoutHandoff ||
              (prior.scoutHandoffBrief && prior.scoutHandoffBrief.scoutHandoff) ||
              null,
            priorScoutCandidateBatch:
              opts.priorScoutCandidateBatch ||
              prior.scoutCandidateBatch ||
              (prior.scoutHandoff && prior.scoutHandoff.candidateBatch) ||
              null,
            priorScoutWorkRequest:
              opts.priorScoutWorkRequest || prior.scoutWorkRequest || null,
            priorProspectBatchReview:
              opts.priorProspectBatchReview ||
              prior.prospectBatchReview ||
              null,
            priorOutreachStrategyPreview:
              opts.priorOutreachStrategyPreview ||
              prior.outreachStrategyPreview ||
              null,
            priorOutreachCopyPlan:
              opts.priorOutreachCopyPlan || prior.outreachCopyPlan || null,
          },
          artifactAction.note || null
        );
      }

      if (
        artifactAction.action === 'approve_outreach_strategy_preview' ||
        artifactAction.action === 'ack_outreach_strategy_approval'
      ) {
        return produceOutreachStrategyApprovalResult(
          ctx,
          syncedAnswers,
          {
            ...slots,
            previewApproved: true,
            criteriaGenerated: true,
            criteriaApproved: true,
            buildProposalGenerated: true,
            buildProposalApproved: true,
            prospectBatchReviewApproved: true,
            batch1Approved: true,
            outreachStrategyPreviewApproved: true,
            strategyApproved: true,
          },
          {
            ...replyOpts,
            userMessage,
            priorScoutHandoffBrief:
              opts.priorScoutHandoffBrief || prior.scoutHandoffBrief || null,
            priorScoutHandoff:
              opts.priorScoutHandoff ||
              prior.scoutHandoff ||
              (prior.scoutHandoffBrief && prior.scoutHandoffBrief.scoutHandoff) ||
              null,
            priorScoutCandidateBatch:
              opts.priorScoutCandidateBatch ||
              prior.scoutCandidateBatch ||
              (prior.scoutHandoff && prior.scoutHandoff.candidateBatch) ||
              null,
            priorScoutWorkRequest:
              opts.priorScoutWorkRequest || prior.scoutWorkRequest || null,
            priorProspectBatchReview:
              opts.priorProspectBatchReview ||
              prior.prospectBatchReview ||
              null,
            priorOutreachStrategyPreview:
              opts.priorOutreachStrategyPreview ||
              prior.outreachStrategyPreview ||
              null,
            priorOutreachCopyPlan:
              opts.priorOutreachCopyPlan || prior.outreachCopyPlan || null,
            priorOutreachDraftPreview:
              opts.priorOutreachDraftPreview || prior.outreachDraftPreview || null,
            priorOutreachLaunchGate:
              opts.priorOutreachLaunchGate || prior.outreachLaunchGate || null,
          },
          artifactAction.note || OUTREACH_STRATEGY_APPROVED_MESSAGE
        );
      }

      if (
        artifactAction.action === 'approve_outreach_copy_plan' ||
        artifactAction.action === 'ack_outreach_copy_plan_approval'
      ) {
        return produceOutreachCopyPlanApprovalResult(
          ctx,
          syncedAnswers,
          {
            ...slots,
            previewApproved: true,
            criteriaApproved: true,
            buildProposalApproved: true,
            prospectBatchReviewApproved: true,
            batch1Approved: true,
            outreachStrategyPreviewApproved: true,
            strategyApproved: true,
            outreachCopyPlanApproved: true,
            copyPlanApproved: true,
          },
          {
            ...replyOpts,
            userMessage,
            priorProspectBatchReview:
              opts.priorProspectBatchReview || prior.prospectBatchReview || null,
            priorOutreachStrategyPreview:
              opts.priorOutreachStrategyPreview ||
              prior.outreachStrategyPreview ||
              null,
            priorOutreachCopyPlan:
              opts.priorOutreachCopyPlan || prior.outreachCopyPlan || null,
            priorOutreachDraftPreview:
              opts.priorOutreachDraftPreview || prior.outreachDraftPreview || null,
            priorOutreachLaunchGate:
              opts.priorOutreachLaunchGate || prior.outreachLaunchGate || null,
          },
          artifactAction.note || OUTREACH_COPY_PLAN_APPROVED_MESSAGE
        );
      }

      if (
        artifactAction.action === 'approve_outreach_draft_preview' ||
        artifactAction.action === 'ack_outreach_draft_preview_approval'
      ) {
        return produceOutreachDraftPreviewApprovalResult(
          ctx,
          syncedAnswers,
          {
            ...slots,
            outreachCopyPlanApproved: true,
            copyPlanApproved: true,
            outreachDraftPreviewApproved: true,
            draftPreviewApproved: true,
          },
          {
            ...replyOpts,
            userMessage,
            priorProspectBatchReview:
              opts.priorProspectBatchReview || prior.prospectBatchReview || null,
            priorOutreachStrategyPreview:
              opts.priorOutreachStrategyPreview ||
              prior.outreachStrategyPreview ||
              null,
            priorOutreachCopyPlan:
              opts.priorOutreachCopyPlan || prior.outreachCopyPlan || null,
            priorOutreachDraftPreview:
              opts.priorOutreachDraftPreview || prior.outreachDraftPreview || null,
            priorOutreachLaunchGate:
              opts.priorOutreachLaunchGate || prior.outreachLaunchGate || null,
          },
          artifactAction.note || OUTREACH_DRAFT_PREVIEW_APPROVED_MESSAGE
        );
      }

      if (artifactAction.action === 'approve_outreach_launch_gate') {
        return produceOutreachLaunchGateApprovalResult(
          ctx,
          syncedAnswers,
          {
            ...slots,
            outreachDraftPreviewApproved: true,
            draftPreviewApproved: true,
            outreachLaunchGateApproved: true,
            launchGateApproved: true,
          },
          {
            ...replyOpts,
            userMessage,
            priorProspectBatchReview:
              opts.priorProspectBatchReview || prior.prospectBatchReview || null,
            priorOutreachStrategyPreview:
              opts.priorOutreachStrategyPreview ||
              prior.outreachStrategyPreview ||
              null,
            priorOutreachCopyPlan:
              opts.priorOutreachCopyPlan || prior.outreachCopyPlan || null,
            priorOutreachDraftPreview:
              opts.priorOutreachDraftPreview || prior.outreachDraftPreview || null,
            priorOutreachLaunchGate:
              opts.priorOutreachLaunchGate || prior.outreachLaunchGate || null,
          },
          artifactAction.note || null
        );
      }

      if (artifactAction.action === 'emit_outreach_copy_plan') {
        return produceOutreachCopyPlanResult(
          ctx,
          syncedAnswers,
          {
            ...slots,
            previewApproved: true,
            criteriaGenerated: true,
            criteriaApproved: true,
            buildProposalGenerated: true,
            buildProposalApproved: true,
            prospectBatchReviewApproved: true,
            batch1Approved: true,
            outreachStrategyPreviewApproved: true,
            strategyApproved: true,
          },
          {
            ...replyOpts,
            userMessage,
            priorScoutHandoffBrief:
              opts.priorScoutHandoffBrief || prior.scoutHandoffBrief || null,
            priorScoutHandoff:
              opts.priorScoutHandoff ||
              prior.scoutHandoff ||
              (prior.scoutHandoffBrief && prior.scoutHandoffBrief.scoutHandoff) ||
              null,
            priorScoutCandidateBatch:
              opts.priorScoutCandidateBatch ||
              prior.scoutCandidateBatch ||
              (prior.scoutHandoff && prior.scoutHandoff.candidateBatch) ||
              null,
            priorScoutWorkRequest:
              opts.priorScoutWorkRequest || prior.scoutWorkRequest || null,
            priorProspectBatchReview:
              opts.priorProspectBatchReview ||
              prior.prospectBatchReview ||
              null,
            priorOutreachStrategyPreview:
              opts.priorOutreachStrategyPreview ||
              prior.outreachStrategyPreview ||
              null,
            priorOutreachCopyPlan:
              opts.priorOutreachCopyPlan || prior.outreachCopyPlan || null,
            priorOutreachDraftPreview:
              opts.priorOutreachDraftPreview || prior.outreachDraftPreview || null,
            priorOutreachLaunchGate:
              opts.priorOutreachLaunchGate || prior.outreachLaunchGate || null,
          },
          artifactAction.note || null
        );
      }

      if (artifactAction.action === 'emit_outreach_draft_preview') {
        return produceOutreachDraftPreviewResult(
          ctx,
          syncedAnswers,
          {
            ...slots,
            outreachStrategyPreviewApproved: true,
            strategyApproved: true,
            outreachCopyPlanApproved: true,
            copyPlanApproved: true,
          },
          {
            ...replyOpts,
            userMessage,
            priorProspectBatchReview:
              opts.priorProspectBatchReview || prior.prospectBatchReview || null,
            priorOutreachStrategyPreview:
              opts.priorOutreachStrategyPreview ||
              prior.outreachStrategyPreview ||
              null,
            priorOutreachCopyPlan:
              opts.priorOutreachCopyPlan || prior.outreachCopyPlan || null,
            priorOutreachDraftPreview:
              opts.priorOutreachDraftPreview || prior.outreachDraftPreview || null,
            priorOutreachLaunchGate:
              opts.priorOutreachLaunchGate || prior.outreachLaunchGate || null,
          },
          artifactAction.note || null
        );
      }

      if (artifactAction.action === 'emit_outreach_launch_gate') {
        return produceOutreachLaunchGateResult(
          ctx,
          syncedAnswers,
          {
            ...slots,
            outreachCopyPlanApproved: true,
            copyPlanApproved: true,
            outreachDraftPreviewApproved: true,
            draftPreviewApproved: true,
          },
          {
            ...replyOpts,
            userMessage,
            priorProspectBatchReview:
              opts.priorProspectBatchReview || prior.prospectBatchReview || null,
            priorOutreachStrategyPreview:
              opts.priorOutreachStrategyPreview ||
              prior.outreachStrategyPreview ||
              null,
            priorOutreachCopyPlan:
              opts.priorOutreachCopyPlan || prior.outreachCopyPlan || null,
            priorOutreachDraftPreview:
              opts.priorOutreachDraftPreview || prior.outreachDraftPreview || null,
            priorOutreachLaunchGate:
              opts.priorOutreachLaunchGate || prior.outreachLaunchGate || null,
          },
          artifactAction.note || null
        );
      }

      if (artifactAction.action === 'hold_outreach_strategy_preview') {
        const priorStrategy =
          opts.priorOutreachStrategyPreview ||
          prior.outreachStrategyPreview ||
          null;
        return {
          message: [
            artifactAction.note ||
              'Outreach Strategy Preview is ready. Approve it to continue to the Outreach Copy Plan.',
            '',
            priorStrategy
              ? formatOutreachStrategyPreviewMessage(priorStrategy)
              : BATCH_1_APPROVED_DISCLAIMER,
          ]
            .filter(Boolean)
            .join('\n'),
          step: CAMPAIGN_PLANNING_STATES.OUTREACH_STRATEGY_PREVIEW,
          answers: syncedAnswers,
          slots: {
            ...slots,
            batch1Approved: true,
            prospectBatchReviewApproved: true,
            outreachStrategyPreviewGenerated: true,
          },
          prospectBatchReview:
            opts.priorProspectBatchReview || prior.prospectBatchReview || null,
          outreachStrategyPreview: priorStrategy,
          outreachCopyPlan: opts.priorOutreachCopyPlan || prior.outreachCopyPlan || null,
          intent: 'hold_outreach_strategy_preview',
          planningState: CAMPAIGN_PLANNING_STATES.OUTREACH_STRATEGY_PREVIEW,
          currentAsk: OUTREACH_STRATEGY_PREVIEW_CLOSING_QUESTION,
          batch1Approved: true,
          outreachCopyGenerated: false,
          finalOutreachCopyGenerated: false,
          sendsMade: false,
          crmWritesMade: false,
          exportMade: false,
          accountChangesMade: false,
        };
      }

      if (artifactAction.action === 'hold_prospect_batch_review') {
        const priorReview =
          opts.priorProspectBatchReview || prior.prospectBatchReview || null;
        return {
          message: [
            artifactAction.note ||
              'Prospect Batch Review is ready. Approve the accepted cold first-pass candidates as Batch 1 to continue.',
            '',
            BATCH_1_APPROVED_DISCLAIMER,
          ].join('\n'),
          step: CAMPAIGN_PLANNING_STATES.PROSPECT_BATCH_REVIEW,
          answers: syncedAnswers,
          slots: {
            ...slots,
            previewApproved: true,
            criteriaApproved: true,
            buildProposalApproved: true,
          },
          preview: replyOpts.priorPreview || null,
          criteriaPreview: replyOpts.priorCriteriaPreview || null,
          buildProposal: replyOpts.priorBuildProposal || null,
          prospectListDraft: replyOpts.priorProspectListDraft || null,
          scoutCandidateBatch:
            opts.priorScoutCandidateBatch || prior.scoutCandidateBatch || null,
          prospectBatchReview: priorReview,
          outreachStrategyPreview: null,
          intent: 'hold_prospect_batch_review',
          planningState: CAMPAIGN_PLANNING_STATES.PROSPECT_BATCH_REVIEW,
          currentAsk:
            (priorReview && priorReview.closingQuestion) ||
            PROSPECT_BATCH_REVIEW_CLOSING_QUESTION,
          outreachCopyGenerated: false,
          crmWritesMade: false,
          accountChangesMade: false,
        };
      }

      if (artifactAction.action === 'execute_existing_scout_work_request') {
        return produceExecuteExistingScoutWorkRequestResult(
          ctx,
          syncedAnswers,
          {
            ...slots,
            previewApproved: true,
            criteriaGenerated: true,
            criteriaApproved: true,
            buildProposalGenerated: true,
            buildProposalApproved: true,
          },
          {
            ...replyOpts,
            userMessage,
            workRequestId: artifactAction.workRequestId || null,
            priorScoutHandoffBrief:
              opts.priorScoutHandoffBrief ||
              prior.scoutHandoffBrief ||
              null,
            priorScoutHandoff:
              opts.priorScoutHandoff ||
              prior.scoutHandoff ||
              (prior.scoutHandoffBrief && prior.scoutHandoffBrief.scoutHandoff) ||
              null,
            priorScoutCandidateBatch:
              opts.priorScoutCandidateBatch ||
              prior.scoutCandidateBatch ||
              (prior.scoutHandoff && prior.scoutHandoff.candidateBatch) ||
              null,
            priorScoutWorkRequest:
              opts.priorScoutWorkRequest || prior.scoutWorkRequest || null,
          },
          artifactAction.note ||
            'Executing the existing Scout work request. No new handoff will be created.'
        );
      }

      if (artifactAction.action === 'hand_brief_to_scout') {
        return produceHandBriefToScoutResult(
          ctx,
          syncedAnswers,
          {
            ...slots,
            previewApproved: true,
            criteriaGenerated: true,
            criteriaApproved: true,
          },
          {
            ...replyOpts,
            priorScoutHandoffBrief:
              opts.priorScoutHandoffBrief ||
              prior.scoutHandoffBrief ||
              null,
            priorScoutHandoff:
              opts.priorScoutHandoff ||
              prior.scoutHandoff ||
              (prior.scoutHandoffBrief && prior.scoutHandoffBrief.scoutHandoff) ||
              null,
          },
          artifactAction.note ||
            'Approving the Scout Handoff Brief and creating a Scout work request.'
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
          {
            ...replyOpts,
            priorScoutHandoffBrief:
              opts.priorScoutHandoffBrief || prior.scoutHandoffBrief || null,
            priorScoutHandoff:
              opts.priorScoutHandoff ||
              prior.scoutHandoff ||
              (prior.scoutHandoffBrief && prior.scoutHandoffBrief.scoutHandoff) ||
              null,
          },
          artifactAction.note ||
            'Creating the Scout Handoff Brief from approved campaign/list criteria. This is planning only — say “Hand this brief to Scout” to approve and queue Scout.'
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
  PROSPECT_BATCH_REVIEW_KIND,
  PROSPECT_BATCH_REVIEW_TITLE,
  PROSPECT_BATCH_REVIEW_DISCLAIMER,
  PROSPECT_BATCH_REVIEW_CLOSING_QUESTION,
  PROSPECT_BATCH_REVIEW_SECTION_TITLES,
  LIVE_SOURCING_BOUNDARY_MESSAGE,
  LIVE_PROSPECT_LIST_KIND,
  LIVE_PROSPECT_LIST_TITLE,
  SCOUT_HANDOFF_BRIEF_KIND,
  SCOUT_HANDOFF_BRIEF_TITLE,
  SCOUT_HANDOFF_BRIEF_DISCLAIMER,
  SCOUT_HANDOFF_SECTION_TITLES,
  SCOUT_HANDOFF_KIND,
  SCOUT_HANDOFF_STATUSES,
  SCOUT_HANDOFF_UI_STATUS,
  SCOUT_SOURCING_NOT_WIRED_MESSAGE,
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
  buildProspectBatchReview,
  buildProspectBatchReviewOperatorDigest,
  formatProspectBatchReviewMessage,
  formatProspectBatchReviewEvidenceMessage,
  formatProspectBatchReviewEvidenceSectionsMessage,
  produceProspectBatchReviewResult,
  produceProspectBatchReviewApprovalResult,
  produceOutreachStrategyPreviewResult,
  produceOutreachStrategyApprovalResult,
  produceOutreachCopyPlanResult,
  produceOutreachCopyPlanApprovalResult,
  produceOutreachDraftPreviewResult,
  produceOutreachDraftPreviewRevisionResult,
  produceOutreachDraftPreviewForceRebuildResult,
  produceOutreachDraftPreviewApprovalResult,
  produceOutreachLaunchGateResult,
  produceOutreachLaunchGateApprovalResult,
  approveProspectBatchReviewBatch1,
  approveOutreachStrategyPreview,
  approveOutreachCopyPlan,
  approveOutreachDraftPreview,
  approveOutreachLaunchGate,
  buildOutreachStrategyPreview,
  buildOutreachStrategyPreviewStub,
  buildOutreachCopyPlan,
  buildOutreachDraftPreview,
  buildOutreachLaunchGate,
  formatOutreachStrategyPreviewMessage,
  formatOutreachCopyPlanMessage,
  formatOutreachDraftPreviewMessage,
  formatOutreachLaunchGateMessage,
  formatOutreachLaunchGateApprovedSummary,
  buildOutreachLaunchGateOperatorStateSummary,
  formatProspectBatch1ApprovalMessage,
  outreachStrategyPreviewLooksStale,
  findStaleOutreachStrategyFragments,
  repairOutreachStrategyPreview,
  outreachCopyPlanLooksStale,
  findStaleOutreachCopyPlanFragments,
  repairOutreachCopyPlan,
  outreachDraftPreviewLooksStale,
  findStaleOutreachDraftFragments,
  repairOutreachDraftPreview,
  buildCampaignContextForRender,
  looksLikeOperatorWorkflowRevision,
  parseOperatorChatDirectives,
  selectResponseMode,
  validateOutreachDraftAgainstInstructions,
  RESPONSE_MODES,
  PRIORITY_ORDER,
  looksLikeForceRebuildConfirmation,
  identifyStaleInjectionSources,
  STALE_OUTREACH_STRATEGY_FRAGMENT_RES,
  STALE_OUTREACH_COPY_PLAN_FRAGMENT_RES,
  STALE_OUTREACH_DRAFT_FRAGMENT_RES,
  OPERATOR_BANNED_FRAGMENT_RES,
  buildProspectBatchReviewClosingQuestion,
  parseRelationshipOverridesFromMessage,
  applyRelationshipOverridesToBatch,
  mergeRelationshipOverrides,
  relationshipOverrideMatchesRow,
  normalizeCompanyIdentity,
  extractDomainFromUrl,
  RELATIONSHIP_STATUS,
  BATCH_1_APPROVED_MESSAGE,
  BATCH_1_APPROVED_DISCLAIMER,
  OUTREACH_STRATEGY_PREVIEW_KIND,
  OUTREACH_STRATEGY_PREVIEW_TITLE,
  OUTREACH_STRATEGY_PREVIEW_DISCLAIMER,
  OUTREACH_STRATEGY_PREVIEW_CLOSING_QUESTION,
  OUTREACH_STRATEGY_APPROVED_MESSAGE,
  OUTREACH_COPY_PLAN_KIND,
  OUTREACH_COPY_PLAN_TITLE,
  OUTREACH_COPY_PLAN_DISCLAIMER,
  OUTREACH_COPY_PLAN_CLOSING_QUESTION,
  OUTREACH_COPY_PLAN_APPROVED_MESSAGE,
  OUTREACH_DRAFT_PREVIEW_KIND,
  OUTREACH_DRAFT_PREVIEW_TITLE,
  OUTREACH_DRAFT_PREVIEW_DISCLAIMER,
  OUTREACH_DRAFT_PREVIEW_CLOSING_QUESTION,
  OUTREACH_DRAFT_PREVIEW_APPROVED_MESSAGE,
  OUTREACH_LAUNCH_GATE_KIND,
  OUTREACH_LAUNCH_GATE_TITLE,
  OUTREACH_LAUNCH_GATE_DISCLAIMER,
  OUTREACH_LAUNCH_GATE_CLOSING_QUESTION,
  OUTREACH_LAUNCH_GATE_APPROVED_STATUS,
  OUTREACH_LAUNCH_GATE_APPROVED_HEADLINE,
  OUTREACH_LAUNCH_GATE_APPROVED_ASK,
  OUTREACH_LAUNCH_GATE_NEXT_OPTIONS,
  OUTREACH_LAUNCH_GATE_OPERATOR_GUIDANCE,
  produceExecutionConfirmationResult,
  CONVERSATION_MODES,
  formatApprovedLaunchGateConversational,
  applyConversationalPolicy,
  looksLikeExecutionRequest,
  buildScoutHandoffBrief,
  formatScoutHandoffBriefMessage,
  produceScoutHandoffBriefResult,
  produceHandBriefToScoutResult,
  produceExecuteExistingScoutWorkRequestResult,
  applyScoutExecutionResult,
  isLivePublicSourcingSupported,
  isScoutSourcingExecutionWired,
  produceLiveSourcingResult,
  formatLiveSourcedProspectListMessage,
  buildScoutHandoff,
  handBriefToScout,
  handBriefToScoutAsync,
  queueOrExecuteExistingScoutWorkRequest,
  executeScoutWorkRequest,
  uiStatusForHandoff,
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
  buildCampaignSynthesisContext,
  ensureCampaignMemory,
  applyBatchReviewLearnings,
  mergeOperatorLearnings,
  findCampaignMemoryDraftConflicts,
  outreachDraftPreviewConflictsWithCampaignMemory,
  rejectsStreetAddressPersonalization,
  DEFAULT_OPERATOR_LEARNINGS,
  containsRawPromptFragment,
  findRawPromptFragments,
};
