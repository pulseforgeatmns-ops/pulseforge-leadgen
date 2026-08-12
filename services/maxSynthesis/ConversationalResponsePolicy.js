'use strict';

/**
 * Max Conversational Response Policy
 *
 * Sits AFTER workflow and state decisions.
 * Core principle: workflow state informs Max’s answer — it is not Max’s answer.
 *
 * Modes:
 *   1. operator_state_update      — state changed / already approved
 *   2. operator_revision_response — operator asked for copy/targeting/memory changes
 *   3. operator_diagnostic        — stale/conflict/missing/failed output
 *   4. formal_review_gate         — first-time gate for approval (card allowed)
 *   5. campaign_ready_summary     — final full readiness state for operator review
 *   6. operator_readiness_check   — summarize / resolve remaining readiness gaps
 *   7. readiness_substep          — operator selected one readiness item; stay in it
 *   8. readiness_field_correction — field fill/correct inside an active substep
 *   9. execution_confirmation     — about to send/export/CRM/account action
 *  10. clarification_needed       — accidental / low-signal / ambiguous input
 *
 * Classification priority for post-approval turns:
 * low-signal ambiguous input → clarification_needed (never re-open full state
 * summary or options). Final campaign-ready / full readiness summary asks →
 * campaign_ready_summary (compose ALL confirmed items; never collapse to the
 * latest substep; never execute). Field correction / substep update inside an
 * active readiness item → readiness_field_correction (never dump Launch Gate
 * operational options). Selected readiness item → readiness_substep (never ask
 * "which readiness item" again). Then discussing options / resolving readiness
 * / selecting a path later ALWAYS beat bare mentions of export/CRM/queue.
 * Execution confirmation only when the operator clearly asks to take or
 * approve a concrete action.
 */

const {
  RESPONSE_MODES,
  PRIORITY_ORDER,
} = require('./OperatorChatResponsiveness');

const CONVERSATION_MODES = Object.freeze({
  OPERATOR_STATE_UPDATE: 'operator_state_update',
  OPERATOR_REVISION_RESPONSE: 'operator_revision_response',
  OPERATOR_DIAGNOSTIC: 'operator_diagnostic',
  FORMAL_REVIEW_GATE: 'formal_review_gate',
  CAMPAIGN_READY_SUMMARY: 'campaign_ready_summary',
  OPERATOR_READINESS_CHECK: 'operator_readiness_check',
  READINESS_SUBSTEP: 'readiness_substep',
  READINESS_FIELD_CORRECTION: 'readiness_field_correction',
  READINESS_SUBSTEP_UPDATE: 'readiness_substep_update',
  EXECUTION_CONFIRMATION: 'execution_confirmation',
  CLARIFICATION_NEEDED: 'clarification_needed',
});

/** Wire/protocol responseMode values used on campaign replies. */
const CONVERSATION_MODE_TO_RESPONSE_MODE = Object.freeze({
  [CONVERSATION_MODES.OPERATOR_STATE_UPDATE]:
    RESPONSE_MODES.OPERATOR_STATE_SUMMARY,
  [CONVERSATION_MODES.OPERATOR_REVISION_RESPONSE]:
    RESPONSE_MODES.OPERATOR_CHAT_RESPONSE,
  [CONVERSATION_MODES.OPERATOR_DIAGNOSTIC]:
    RESPONSE_MODES.STALE_SOURCE_DIAGNOSTIC,
  [CONVERSATION_MODES.FORMAL_REVIEW_GATE]:
    RESPONSE_MODES.WORKFLOW_REVIEW_CARD,
  [CONVERSATION_MODES.CAMPAIGN_READY_SUMMARY]:
    RESPONSE_MODES.CAMPAIGN_READY_SUMMARY || 'campaign_ready_summary',
  [CONVERSATION_MODES.OPERATOR_READINESS_CHECK]:
    RESPONSE_MODES.OPERATOR_READINESS_CHECK || 'operator_readiness_check',
  [CONVERSATION_MODES.READINESS_SUBSTEP]:
    RESPONSE_MODES.READINESS_SUBSTEP || 'readiness_substep',
  [CONVERSATION_MODES.READINESS_FIELD_CORRECTION]:
    RESPONSE_MODES.READINESS_FIELD_CORRECTION || 'readiness_field_correction',
  [CONVERSATION_MODES.READINESS_SUBSTEP_UPDATE]:
    RESPONSE_MODES.READINESS_FIELD_CORRECTION || 'readiness_field_correction',
  [CONVERSATION_MODES.EXECUTION_CONFIRMATION]:
    RESPONSE_MODES.EXECUTION_CONFIRMATION || 'execution_confirmation',
  [CONVERSATION_MODES.CLARIFICATION_NEEDED]:
    RESPONSE_MODES.CLARIFICATION_NEEDED || 'clarification_needed',
});

const RESPONSE_MODE_TO_CONVERSATION_MODE = Object.freeze({
  [RESPONSE_MODES.OPERATOR_STATE_SUMMARY]:
    CONVERSATION_MODES.OPERATOR_STATE_UPDATE,
  [RESPONSE_MODES.OPERATOR_CHAT_RESPONSE]:
    CONVERSATION_MODES.OPERATOR_REVISION_RESPONSE,
  [RESPONSE_MODES.STALE_SOURCE_DIAGNOSTIC]:
    CONVERSATION_MODES.OPERATOR_DIAGNOSTIC,
  [RESPONSE_MODES.WORKFLOW_REVIEW_CARD]:
    CONVERSATION_MODES.FORMAL_REVIEW_GATE,
  campaign_ready_summary: CONVERSATION_MODES.CAMPAIGN_READY_SUMMARY,
  operator_readiness_check: CONVERSATION_MODES.OPERATOR_READINESS_CHECK,
  readiness_substep: CONVERSATION_MODES.READINESS_SUBSTEP,
  readiness_field_correction: CONVERSATION_MODES.READINESS_FIELD_CORRECTION,
  readiness_substep_update: CONVERSATION_MODES.READINESS_FIELD_CORRECTION,
  execution_confirmation: CONVERSATION_MODES.EXECUTION_CONFIRMATION,
  clarification_needed: CONVERSATION_MODES.CLARIFICATION_NEEDED,
});

const CAMPAIGN_READY_SUMMARY_SAFETY_LINE =
  'Nothing has been executed. Readiness confirmation and path selection are not execute approval.';

const CAMPAIGN_READY_SUMMARY_CLOSING =
  'Do not execute anything until the operator explicitly approves the next execute action.';

/**
 * Short tokens that are intentional operator answers — not accidental noise.
 * Single-letter approvals (y/n) stay here; random letters like `v` / `k` do not.
 */
const KNOWN_SHORT_OPERATOR_INTENTS = Object.freeze(
  new Set([
    'y',
    'n',
    'yes',
    'no',
    'ok',
    'okay',
    'hold',
    'wait',
    'stop',
    'go',
    'skip',
    'pass',
    'help',
    'next',
    'back',
  ])
);

const CLARIFICATION_NEEDED_ASK =
  'Do you want to resolve sender identity, reply handling, follow-up tracking, or hold?';

/**
 * Per-item readiness substeps. Once the operator selects one, stay inside it —
 * never re-ask "Which readiness item should we resolve first?"
 */
const READINESS_SUBSTEPS = Object.freeze({
  sender_identity: Object.freeze({
    id: 'sender_identity',
    label: 'sender identity',
    match: /\bsender\s+identity\b/i,
    questions: Object.freeze([
      'What sender name should appear on the email?',
      'What sender email address should be used or reviewed?',
      'Should the signature be from a person, the company, or both?',
    ]),
    closingAsk:
      "Once you answer those, I'll mark sender identity as confirmed or note what still needs review.",
  }),
  reply_handling: Object.freeze({
    id: 'reply_handling',
    label: 'reply handling',
    match: /\breply(?:\s+handling|\s+inbox|\s*[-/]\s*to)\b/i,
    questions: Object.freeze([
      'Which reply inbox / reply-to address should be used?',
      'Who monitors replies?',
      'How should replies be handled before broader rollout?',
    ]),
    closingAsk:
      "Once you answer those, I'll mark reply handling as confirmed or note what still needs review.",
  }),
  follow_up_tracking: Object.freeze({
    id: 'follow_up_tracking',
    label: 'follow-up tracking process',
    match: /\bfollow[- ]?up\s+tracking(?:\s+process)?\b/i,
    questions: Object.freeze([
      'Where should follow-up status be tracked?',
      'Should Follow-up 1 be planned for about 3 business days after first touch?',
      'Should Follow-up 2 be planned for about 7 business days after first touch?',
      'Should all follow-ups remain review-first/manual unless explicitly enabled later?',
    ]),
    closingAsk:
      "Once you answer those, I'll mark follow-up tracking as confirmed or note what still needs review.",
  }),
  operational_path: Object.freeze({
    id: 'operational_path',
    label: 'operational path selection',
    match: /\boperational\s+path(?:\s+selection)?\b/i,
    questions: Object.freeze([
      'Do you want to prepare for manual send, CRM drafts, queued sends later, or hold with no action?',
    ]),
    closingAsk:
      "Once you choose a path, I'll mark operational path as selected. Path selection is not execute approval.",
  }),
  reply_monitoring_batch1: Object.freeze({
    id: 'reply_monitoring_batch1',
    label: 'reply monitoring / Batch 1 review process',
    match:
      /\breply\s+monitoring(?:\s*\/\s*|\s+)(?:Batch\s*1\s+)?review(?:\s+process)?\b|\breply\s+monitoring\b|\bBatch\s*1\s+review\b/i,
    questions: Object.freeze([
      'Who will monitor replies, how should responses be reviewed, and should broader rollout remain blocked until Batch 1 results are reviewed?',
    ]),
    closingAsk:
      "Once you answer those, I'll mark reply monitoring / Batch 1 review as confirmed or note what still needs review.",
  }),
});

const READINESS_SUBSTEP_SAFETY_LINE =
  'Nothing external has happened. Sends, export, and CRM writes remain locked.';

/**
 * Canonical operational path choices. Selecting one confirms readiness only —
 * never prepares export, CRM drafts, or queued sends.
 */
const OPERATIONAL_PATH_CHOICES = Object.freeze({
  manual_send_export: Object.freeze({
    id: 'manual_send_export',
    label: 'manual send export for operator review',
    match:
      /\bmanual[-\s]?send\s+export(?:\s+for\s+(?:operator\s+)?review)?\b|\bprepare\s+(?:a\s+)?manual[-\s]?send\s+export\b/i,
    selectionNote:
      'This is path selection only. No export has been prepared.',
  }),
  crm_drafts: Object.freeze({
    id: 'crm_drafts',
    label: 'CRM drafts',
    match: /\bcrm\s+drafts?\b/i,
    selectionNote:
      'This is path selection only. No CRM drafts have been prepared.',
  }),
  queued_sends_later: Object.freeze({
    id: 'queued_sends_later',
    label: 'queued sends later',
    match: /\bqueue(?:d)?\s+sends?(?:\s+later)?\b/i,
    selectionNote:
      'This is path selection only. No sends have been queued.',
  }),
  hold: Object.freeze({
    id: 'hold',
    label: 'hold with no action',
    match: /\bhold(?:\s+with\s+no\s+action)?\b|\bno\s+action\b/i,
    selectionNote:
      'This is path selection only. No action has been prepared.',
  }),
});

/**
 * Next readiness item prompts after a substep is confirmed.
 * Labels match operator-facing language (not only internal ids).
 */
const READINESS_NEXT_ITEM_PROMPTS = Object.freeze({
  reply_handling: Object.freeze({
    id: 'reply_handling',
    label: 'reply inbox / reply-to handling',
    ask: 'What inbox should receive replies?',
    questions: Object.freeze([
      'What inbox should receive replies?',
      'Should reply-to match the sender address?',
      'Who will monitor replies?',
    ]),
  }),
  operational_path: Object.freeze({
    id: 'operational_path',
    label: 'operational path selection',
    ask: 'Do you want to prepare for manual send, CRM drafts, queued sends later, or hold with no action?',
    questions: Object.freeze([
      'Do you want to prepare for manual send, CRM drafts, queued sends later, or hold with no action?',
    ]),
    /** Path selection only — never execution confirmation. */
    isExecution: false,
  }),
  follow_up_tracking: Object.freeze({
    id: 'follow_up_tracking',
    label: 'follow-up tracking process',
    ask: 'Where should follow-up status be tracked?',
    questions: Object.freeze([
      'Where should follow-up status be tracked?',
      'Should Follow-up 1 be planned for about 3 business days after first touch?',
      'Should Follow-up 2 be planned for about 7 business days after first touch?',
      'Should all follow-ups remain review-first/manual unless explicitly enabled later?',
    ]),
  }),
  reply_monitoring_batch1: Object.freeze({
    id: 'reply_monitoring_batch1',
    label: 'reply monitoring / Batch 1 review process',
    ask: 'Who will monitor replies, how should responses be reviewed, and should broader rollout remain blocked until Batch 1 results are reviewed?',
    questions: Object.freeze([
      'Who will monitor replies, how should responses be reviewed, and should broader rollout remain blocked until Batch 1 results are reviewed?',
    ]),
  }),
});

/** Default unresolved items after Launch Gate readiness-only approval. */
const DEFAULT_UNRESOLVED_READINESS_ITEMS = Object.freeze([
  'Sender identity is not confirmed',
  'Reply handling is not confirmed',
  'Tracking / account settings remain unchanged until an explicit execute action',
]);

/**
 * Canonical readiness concepts — used only for known-state evaluation and
 * dedupe. Distinct operator items must stay distinct (e.g. reply-to handling
 * vs reply monitoring owner/process).
 */
const READINESS_CONCEPT_PATTERNS = Object.freeze([
  {
    id: 'sender_identity',
    match: /\bsender\s+identity\b/i,
  },
  {
    id: 'reply_inbox_handling',
    match: /\breply(?:\s*[-/]\s*|\s+)(?:inbox|to)\b|\breply\s+inbox\b|\breply-?to\s+handling\b/i,
  },
  {
    id: 'operational_path',
    match: /\boperational\s+path\b|\bmanual\s+send\s+vs\b|\bnot\s+chosen\s+yet\b/i,
  },
  {
    id: 'follow_up_tracking_process',
    match: /\bfollow[- ]?up\s+tracking\s+process\b/i,
  },
  {
    id: 'reply_monitoring_owner',
    match: /\breply\s+monitoring\b|\bmonitoring\s+owner(?:\s*\/\s*process)?\b/i,
  },
  {
    id: 'broader_rollout_batch1',
    match: /\bbroader\s+rollout\b|\bBatch\s*1\s+results?\b/i,
  },
  {
    id: 'tracking_account_settings',
    match:
      /\btracking\s*\/\s*account\s+settings\b|\baccount\s+settings\b(?!.*follow[- ]?up)/i,
  },
  {
    id: 'reply_handling_generic',
    match: /\breply\s+handling\b/i,
  },
]);

const READINESS_CHECKLIST_CLOSING_ASK =
  'Which readiness item should we resolve first?';
const READINESS_CHECKLIST_SAFETY_LINE =
  'Nothing external has happened. Sends, export, and CRM writes remain locked.';

/** Sections that make Max sound like a workflow renderer — avoid outside formal gates. */
const RENDERER_SECTION_PATTERNS = Object.freeze([
  /\bRecommended decision\b/i,
  /\bWhat is included\b/i,
  /\bWhy this is recommended\b/i,
  /\bPrimary actions\b/i,
  /\bView evidence\b/i,
  /\bFull sourced records\b/i,
  /\bDoes this look right to approve\b/i,
]);

const FULL_SAFETY_LINES = Object.freeze([
  'No sends',
  'No CRM writes',
  'No export',
  'No account changes',
  'No DNS changes',
  'No GBP changes',
  'No social changes',
  'No tracking changes',
]);

const DEFAULT_NEXT_PATHS = Object.freeze([
  'prepare a manual-send export for review',
  'create CRM drafts, if explicitly approved',
  'queue sends later, if execution is intentionally enabled',
  'hold with no action',
]);

function toConversationMode(responseMode) {
  if (!responseMode) return null;
  if (Object.values(CONVERSATION_MODES).includes(responseMode)) {
    return responseMode;
  }
  return RESPONSE_MODE_TO_CONVERSATION_MODE[responseMode] || null;
}

function toResponseMode(conversationMode) {
  if (!conversationMode) return null;
  if (Object.values(RESPONSE_MODES).includes(conversationMode)) {
    return conversationMode;
  }
  return (
    CONVERSATION_MODE_TO_RESPONSE_MODE[conversationMode] || conversationMode
  );
}

function containsRendererBoilerplate(text) {
  const s = String(text || '');
  return RENDERER_SECTION_PATTERNS.some((re) => re.test(s));
}

/**
 * Compact safety line for ordinary operator turns.
 * Full expanded list is reserved for formal gates / execution confirmation.
 */
function compactSafetyLockLine(opts = {}) {
  if (opts.custom) return String(opts.custom);
  return (
    opts.short ||
    'Nothing external has happened. Sends, export, and CRM writes remain locked.'
  );
}

function expandedSafetyBlock(opts = {}) {
  const lines = opts.lines || FULL_SAFETY_LINES;
  const header =
    opts.header ||
    'Nothing external happens without an explicit execute approval:';
  return [header, ...lines.map((l) => `- ${l}`)].join('\n');
}

/**
 * State-aware approval language.
 */
function approvalLanguageForGate(opts = {}) {
  const gate = opts.gateName || opts.title || 'This gate';
  const status = String(opts.status || '').toLowerCase();
  const approved =
    opts.approved === true ||
    /approved|readiness_only|approved_readiness/.test(status);

  if (opts.executionPending === true) {
    return {
      mode: 'execution_pending',
      ask: 'Do you explicitly approve this execute action?',
      statement: null,
    };
  }

  if (approved) {
    return {
      mode: 'approved',
      statement: `${gate} is approved for readiness only.`,
      ask: 'Which next path do you want to prepare, if any?',
    };
  }

  return {
    mode: 'pending',
    statement: null,
    ask: `Do you approve this ${gate.replace(/^the\s+/i, '')}, or do you want revisions?`,
  };
}

/**
 * Internal assessment — answers the composition questions before responding.
 */
function assessConversationContext(input = {}) {
  const operatorMessage = String(input.operatorMessage || input.text || '').trim();
  const stateChanged = Boolean(
    input.stateChanged ||
      input.justApproved ||
      input.intent === 'outreach_launch_gate_approved' ||
      input.launchGateJustApproved
  );
  const gateAlreadyApproved = Boolean(
    input.gateAlreadyApproved ||
      input.launchGateApproved ||
      input.approvedReadinessOnly ||
      (input.gate &&
        (input.gate.launchGateApproved === true ||
          input.gate.status === 'approved_readiness_only' ||
          input.gate.approved === true))
  );
  const isRevision = Boolean(
    input.isRevision ||
      input.forceOperatorChat ||
      input.messageClass === 'correction' ||
      input.messageClass === 'refinement_feedback' ||
      input.messageClass === 'add_on' ||
      input.messageClass === 'clarification_request'
  );
  const isDiagnostic = Boolean(
    input.isDiagnostic ||
      input.staleDiagnosticRequired ||
      input.intent === 'stale_source_diagnostic' ||
      input.validationFailed
  );
  const isFirstTimeReview = Boolean(
    input.isInitialReviewGate ||
      input.isFirstTimeReview ||
      input.intent === 'produce_outreach_draft_preview' ||
      input.intent === 'produce_outreach_launch_gate' ||
      input.intent === 'show_outreach_launch_gate' ||
      input.intent === 'outreach_copy_plan_approved' ||
      input.intent === 'outreach_draft_preview_approved'
  );
  const lowSignalFromText =
    Boolean(operatorMessage) &&
    looksLikeLowSignalAmbiguousInput(operatorMessage);
  const selectedReadinessItem =
    input.selectedReadinessItem ||
    detectSelectedReadinessItem(operatorMessage);
  const activeReadinessItemId =
    input.activeReadinessItemId ||
    (input.slots && input.slots.activeReadinessItemId) ||
    (selectedReadinessItem && selectedReadinessItem.id) ||
    null;
  const fieldCorrectionFromText =
    Boolean(operatorMessage) &&
    looksLikeReadinessFieldCorrection(operatorMessage, {
      activeReadinessItemId,
      slots: input.slots || null,
      priorFields: input.priorFields || null,
    });
  const campaignReadyFromText = looksLikeCampaignReadySummary(operatorMessage);
  const readinessFromText = looksLikeOperatorReadinessCheck(operatorMessage);
  const nonExecutionFromText = looksLikeNonExecutionIntent(operatorMessage);
  const executionFromText = looksLikeExecutionRequest(operatorMessage);

  // Accidental / low-signal input must never be treated as readiness summary,
  // execution, or a request to re-open the full approved-state options block.
  const isClarificationNeeded = Boolean(
    input.isClarificationNeeded ||
      input.forceClarification ||
      input.intent === 'clarification_needed' ||
      input.messageClass === 'clarification_needed' ||
      (lowSignalFromText &&
        !input.forceCampaignReadySummary &&
        !input.isCampaignReadySummary &&
        !input.forceReadinessCheck &&
        !input.isReadinessCheck &&
        !input.isReadinessSubstep &&
        !input.isReadinessFieldCorrection &&
        !input.isExecutionRequest &&
        !input.executionPending &&
        !isRevision &&
        !isDiagnostic)
  );

  // Final campaign-ready / full readiness summary — compose ALL confirmed
  // items. Beats field correction / substep so Max never collapses to the
  // latest readiness item, and never becomes execution confirmation.
  const isCampaignReadySummary = Boolean(
    !isClarificationNeeded &&
      (input.isCampaignReadySummary ||
        input.forceCampaignReadySummary ||
        input.intent === 'campaign_ready_summary' ||
        campaignReadyFromText)
  );

  // Field fill / correction inside an active readiness substep — never dump
  // Launch Gate operational options or ask which path to prepare.
  const isReadinessFieldCorrection = Boolean(
    !isClarificationNeeded &&
      !isCampaignReadySummary &&
      (input.isReadinessFieldCorrection ||
        input.forceReadinessFieldCorrection ||
        input.intent === 'readiness_field_correction' ||
        input.intent === 'readiness_substep_update' ||
        fieldCorrectionFromText)
  );

  // Operator already picked a readiness item — stay in that substep.
  const isReadinessSubstep = Boolean(
    !isClarificationNeeded &&
      !isCampaignReadySummary &&
      !isReadinessFieldCorrection &&
      (input.isReadinessSubstep ||
        input.forceReadinessSubstep ||
        input.intent === 'readiness_substep' ||
        selectedReadinessItem)
  );

  // Explicit flags still lose to clear readiness / planning language in the
  // operator message — bare option names must never force execute confirm.
  const isReadinessCheck = Boolean(
    !isClarificationNeeded &&
      !isCampaignReadySummary &&
      !isReadinessFieldCorrection &&
      !isReadinessSubstep &&
      (input.isReadinessCheck ||
        input.forceReadinessCheck ||
        readinessFromText)
  );
  const isNonExecutionIntent = Boolean(
    isClarificationNeeded ||
      isCampaignReadySummary ||
      isReadinessFieldCorrection ||
      isReadinessSubstep ||
      isReadinessCheck ||
      input.isNonExecutionIntent ||
      nonExecutionFromText
  );
  const isExecutionRequest = Boolean(
    !isClarificationNeeded &&
      !isCampaignReadySummary &&
      !isReadinessFieldCorrection &&
      !isReadinessSubstep &&
      !isNonExecutionIntent &&
      (input.isExecutionRequest ||
        input.executionPending ||
        executionFromText)
  );

  const shortestUseful =
    isDiagnostic
      ? 'plain_language_problem_then_detail'
      : isClarificationNeeded
        ? 'clarify_ambiguous_input'
        : isCampaignReadySummary
          ? 'full_campaign_ready_summary'
          : isReadinessFieldCorrection
            ? 'ack_field_update_then_next_readiness'
            : isReadinessSubstep
              ? 'selected_readiness_substep_questions'
              : isReadinessCheck
                ? 'unresolved_readiness_then_ask'
                : isExecutionRequest
                  ? 'explicit_execute_confirm'
                  : stateChanged || gateAlreadyApproved
                    ? 'state_then_next_options'
                    : isRevision
                      ? 'ack_change_artifact'
                      : isFirstTimeReview
                        ? 'formal_gate_card'
                        : 'concise_operator_update';

  const mustBlock = isExecutionRequest
    ? FULL_SAFETY_LINES.slice()
    : [
        'No automatic sends',
        'No automatic CRM writes',
        'No automatic export',
        'No account/DNS/GBP/social/tracking changes',
      ];

  return {
    operatorMessage,
    currentState: input.currentState || input.step || input.planningState || null,
    stateChanged,
    gateAlreadyApproved,
    isRevision,
    isDiagnostic,
    isFirstTimeReview,
    isClarificationNeeded,
    isCampaignReadySummary,
    isReadinessFieldCorrection,
    isReadinessSubstep,
    selectedReadinessItem: selectedReadinessItem || null,
    activeReadinessItemId,
    isReadinessCheck,
    isNonExecutionIntent,
    isExecutionRequest,
    shortestUseful,
    mustBlock,
    nextAsk: input.nextAsk || null,
    priorityOrder: PRIORITY_ORDER,
  };
}

/**
 * Accidental / very short / ambiguous operator input with no clear intent.
 * Examples: `v`, `k`, `.`, `?`, random single characters, punctuation-only,
 * or tiny typo fragments. Must NOT classify as state summary, readiness
 * check, or execution confirmation.
 */
function looksLikeLowSignalAmbiguousInput(text) {
  const s = String(text || '').trim();
  if (!s) return true;

  const lower = s.toLowerCase();
  if (KNOWN_SHORT_OPERATOR_INTENTS.has(lower)) return false;

  // Clear multi-word / named intents are never low-signal.
  if (looksLikeCampaignReadySummary(s)) return false;
  if (looksLikeReadinessSubstepSelection(s)) return false;
  if (looksLikeReadinessFieldCorrection(s)) return false;
  if (looksLikeOperatorReadinessCheck(s)) return false;
  if (looksLikeExecutionRequest(s)) return false;
  if (
    /\b(?:approve|approved|revise|revision|export|crm|sender|reply|hold\s+for|options?|summarize|summarise|unresolved|readiness|update|signature|campaign[- ]?ready)\b/i.test(
      s
    )
  ) {
    return false;
  }

  // Single character (letter, digit, or punctuation) — accidental keystroke.
  if (s.length === 1) return true;

  // Punctuation / symbol only (e.g. `...`, `???`, `!!`).
  if (/^[^\p{L}\p{N}]+$/u.test(s) && s.length <= 8) return true;

  // Very short alphanumeric fragment with no clear intent (partial typo).
  if (s.length <= 2 && /^[a-zA-Z0-9]+$/.test(s)) return true;

  return false;
}

/**
 * Planning / deferral / question language that must NOT become execution.
 * Takes priority over bare mentions of export / CRM / queued sends.
 */
function looksLikeNonExecutionIntent(text) {
  const s = String(text || '').trim();
  if (!s) return false;

  if (/\bbefore\s+choosing\b/i.test(s)) return true;
  if (/\bnot\s+yet\b/i.test(s)) return true;
  if (
    /\b(?:hold\s+for\s+now|hold\s+with\s+no\s+action|just\s+hold)\b/i.test(s)
  ) {
    return true;
  }
  // Path selection is readiness confirmation — never execution.
  if (/\bpath\s+selection\s+only\b/i.test(s)) return true;
  if (/\bnot\s+(?:an?\s+)?execute\s+approval\b/i.test(s)) return true;
  if (
    /\b(?:select(?:ed)?|choose|chose|pick(?:ed)?)\s+operational\s+path\b/i.test(
      s
    )
  ) {
    return true;
  }
  // Follow-up tracking answers are readiness confirmation — never execution.
  if (parseFollowUpTrackingFields(s).hasAny) return true;
  // Reply monitoring / Batch 1 review answers are readiness — never execution.
  if (parseReplyMonitoringBatchReviewFields(s).hasAny) return true;
  if (/\b(?:summarize|summarise)\b/i.test(s)) return true;
  if (looksLikeCampaignReadySummary(s)) return true;
  if (
    /\bhelp\s+me\s+(?:decide|resolve|understand|review|choose)\b/i.test(s)
  ) {
    return true;
  }
  if (/\b(?:let'?s\s+)?(?:talk\s+through|discuss)\b/i.test(s)) return true;
  if (/\bwhat(?:'s|\s+is)\s+still\s+unresolved\b/i.test(s)) return true;
  if (/\bwhat(?:'s|\s+is)\s+the\s+safest\s+next\b/i.test(s)) return true;
  if (/\bwhat\s+would\b[\s\S]{0,80}\binvolve\b/i.test(s)) return true;
  if (
    /\bi'?d\s+probably\b/i.test(s) &&
    /\b(?:but\s+)?not\s+yet\b/i.test(s)
  ) {
    return true;
  }
  if (
    /\b(?:review|discuss|decide\s+between)\b/i.test(s) &&
    /\b(?:export|crm|queued?\s+sends?|next\s+(?:path|option|move)s?)\b/i.test(
      s
    ) &&
    !/\b(?:prepare|create|queue|approve|execute|go\s+ahead)\b/i.test(s)
  ) {
    return true;
  }

  // Pure questions without an execute imperative stay conversational.
  if (
    /\?/.test(s) &&
    !/\b(?:approve|execute|prepare|create|queue|go\s+ahead)\b/i.test(s)
  ) {
    return true;
  }

  return false;
}

/**
 * Operator wants the full final campaign-ready / readiness summary —
 * not a single substep, not unresolved-only checklist, not execute.
 */
function looksLikeCampaignReadySummary(text) {
  const s = String(text || '').trim();
  if (!s) return false;

  const explicitCampaignReady =
    /\bcampaign_ready_summary\b/i.test(s) ||
    /\bcampaign[- ]?ready\s+summary\b/i.test(s) ||
    /\bfinal\s+(?:\w+\s+){0,6}(?:campaign[- ]?ready|readiness)\s+summary\b/i.test(
      s
    ) ||
    /\b(?:provide|give|compose|produce|show|send|draft)\b[\s\S]{0,100}\b(?:campaign[- ]?ready|final\s+readiness)\s+summary\b/i.test(
      s
    ) ||
    /\b(?:full|complete|final)\s+readiness\s+summary\b/i.test(s) ||
    /\b(?:summarize|summarise)\b[\s\S]{0,60}\b(?:whole|full|complete|entire)\s+readiness\b/i.test(
      s
    ) ||
    (/\breadiness\s+state\b/i.test(s) &&
      /\b(?:summarize|summarise|summary|whole|full|complete)\b/i.test(s));

  // Unresolved / remaining-gap checklists stay operator_readiness_check
  // unless the operator explicitly asked for a campaign-ready summary.
  const unresolvedGapAsk =
    /\b(?:still\s+)?unresolved\b/i.test(s) ||
    /\bremaining\s+readiness\b/i.test(s) ||
    /\bbefore\s+choosing\b/i.test(s) ||
    /\bhelp\s+me\s+resolve\b[\s\S]{0,48}\b(?:remaining|unresolved|readiness)\b/i.test(
      s
    );
  if (unresolvedGapAsk && !explicitCampaignReady) {
    return false;
  }

  if (explicitCampaignReady) return true;

  // Multi-concept summary requests (lists several readiness items to include).
  const concepts = [
    /\bsender\s+identity\b/i,
    /\breply\s+handling\b|\breply\s+inbox\b|\breply(?:\s*[-/]\s*to)\b/i,
    /\boperational\s+path\b/i,
    /\bfollow[- ]?up\s+tracking\b/i,
    /\breply\s+monitoring\b|\bBatch\s*1\s+review\b/i,
    /\bexecution\s+lock\b/i,
    /\bBatch\s*1\s+(?:scope|approved)\b|\bapproved\s+Batch\s*1\b/i,
  ];
  const hitCount = concepts.filter((re) => re.test(s)).length;
  if (
    hitCount >= 4 &&
    /\b(?:summary|summarize|summarise|include|campaign[- ]?ready)\b/i.test(s) &&
    !/\bnot\s+confirmed\b/i.test(s) &&
    !/\bnot\s+chosen\b/i.test(s)
  ) {
    return true;
  }

  return false;
}

/**
 * Operator wants unresolved readiness gaps summarized / resolved — not execute.
 * Does NOT match when the operator already selected a specific readiness item
 * (that is readiness_substep). Campaign-ready full summaries are separate.
 */
function looksLikeOperatorReadinessCheck(text) {
  const s = String(text || '').trim();
  if (!s) return false;

  // Full final summary is campaign_ready_summary, not unresolved checklist.
  if (looksLikeCampaignReadySummary(s)) return false;

  // Specific item already chosen — not a checklist / "which item" turn.
  if (detectSelectedReadinessItem(s)) return false;

  if (/\bunresolved\b/i.test(s) && /\breadiness\b/i.test(s)) return true;
  if (/\bremaining\s+readiness\b/i.test(s)) return true;
  if (/\breadiness\s+(?:items?|gaps?|checklist)\b/i.test(s)) return true;
  if (
    /\b(?:summarize|summarise|resolve|list)\b/i.test(s) &&
    /\b(?:unresolved|remaining|readiness|gaps?)\b/i.test(s)
  ) {
    return true;
  }
  if (
    /\bbefore\s+choosing\b/i.test(s) &&
    /\b(?:readiness|unresolved|export|crm|queued?\s+sends?)\b/i.test(s)
  ) {
    return true;
  }
  if (/\bwhat(?:'s|\s+is)\s+still\s+unresolved\b/i.test(s)) return true;
  if (
    /\bhelp\s+me\s+resolve\b/i.test(s) &&
    /\b(?:readiness|remaining|unresolved)\b/i.test(s)
  ) {
    return true;
  }
  if (
    /\blet'?s\s+talk\s+through\b/i.test(s) &&
    /\breadiness\b/i.test(s) &&
    !/\b(?:sender|reply|follow[- ]?up)\b/i.test(s)
  ) {
    return true;
  }

  return false;
}

/**
 * True when the operator selected a specific readiness item to resolve now.
 */
function looksLikeReadinessSubstepSelection(text) {
  return Boolean(detectSelectedReadinessItem(text));
}

/**
 * Detect which readiness item the operator selected, if any.
 * @returns {object|null} READINESS_SUBSTEPS entry
 */
function detectSelectedReadinessItem(text) {
  const s = String(text || '').trim();
  if (!s) return null;

  // Full campaign-ready / readiness-state summaries are not a single substep.
  if (looksLikeCampaignReadySummary(s)) return null;

  // Checklist / summarize asks are not a single-item substep selection.
  if (
    /\b(?:summarize|summarise|list)\b/i.test(s) &&
    /\b(?:unresolved|remaining|readiness|gaps?|checklist)\b/i.test(s)
  ) {
    return null;
  }
  if (
    /\bbefore\s+choosing\b/i.test(s) &&
    /\b(?:still\s+unresolved|remaining\s+readiness)\b/i.test(s)
  ) {
    return null;
  }

  // Multiple explicit checklist bullets → checklist turn, not one substep.
  const bulletCount = (s.match(/^(?:[-*•–—]|\d+[.)])\s+\S/gm) || []).length;
  if (bulletCount >= 2) return null;

  // Comma/and lists that mention several readiness concepts are summaries,
  // not a single-item selection.
  const conceptHits = [
    /\bsender\s+identity\b/i,
    /\breply\s+handling\b|\breply\s+inbox\b/i,
    /\boperational\s+path\b/i,
    /\bfollow[- ]?up\s+tracking\b/i,
    /\breply\s+monitoring\b|\bBatch\s*1\s+review\b/i,
  ].filter((re) => re.test(s)).length;
  if (conceptHits >= 3) return null;

  for (const item of Object.values(READINESS_SUBSTEPS)) {
    // Wrap alternations so prefixes/suffixes apply to the whole label match.
    const labelSource = `(?:${item.match.source})`;

    // "Resolve sender identity now" / "focus on reply handling"
    if (
      new RegExp(
        String.raw`\b(?:resolve|confirm|focus\s+on|work\s+on|start(?:\s+with)?|choose|select|talk\s+through|let'?s\s+(?:do|resolve|confirm))\b[\s\S]{0,48}` +
          labelSource,
        'i'
      ).test(s)
    ) {
      return item;
    }

    // "sender identity first/now/next"
    if (
      new RegExp(labelSource + String.raw`[\s\S]{0,24}\b(?:now|first|next)\b`, 'i').test(
        s
      )
    ) {
      return item;
    }

    // "I already selected … sender identity"
    if (
      /\b(?:already\s+selected|i\s+(?:already\s+)?(?:selected|chose|picked)|first\s+readiness\s+item)\b/i.test(
        s
      ) &&
      item.match.test(s)
    ) {
      return item;
    }

    // Bare item label as the whole message.
    if (
      new RegExp(
        `^${item.label.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`,
        'i'
      ).test(s)
    ) {
      return item;
    }
  }

  return null;
}

/**
 * Normalize an email captured from operator text (strip escapes / brackets).
 */
function normalizeCapturedEmail(raw) {
  return String(raw || '')
    .trim()
    .replace(/^<|>$/g, '')
    .replace(/\\@/g, '@')
    .replace(/\\+/g, '');
}

/** Optional list bullet / numbering before a labeled readiness field. */
const READINESS_FIELD_LINE_PREFIX =
  String.raw`(?:^|\n)\s*(?:[-*•–—]|\d+[.)])?\s*`;

/**
 * Parse sender-identity fields from free text (fills or corrections).
 * Applies ALL explicit fields present in the message — not only the first
 * corrected field. Bullet lists and "Sender name:" labels are supported.
 * @returns {{ name: string|null, email: string|null, signature: string|null, updatedFields: string[], hasAny: boolean }}
 */
function parseSenderIdentityFields(text) {
  const s = String(text || '').trim();
  const updatedFields = [];
  let name = null;
  let email = null;
  let signature = null;

  if (!s) {
    return { name, email, signature, updatedFields, hasAny: false };
  }

  const nameMatch = s.match(
    new RegExp(
      READINESS_FIELD_LINE_PREFIX +
        String.raw`(?:sender\s+)?name\s*[:\-–—]\s*([^\n]+)`,
      'i'
    )
  );
  if (nameMatch) {
    name = nameMatch[1].trim().replace(/[.;,]+$/, '');
    updatedFields.push('name');
  } else {
    const nameUpdate = s.match(
      /\b(?:update|change|correct|set|use)\b[\s\S]{0,40}\b(?:sender\s+)?name\b[\s\S]{0,20}\b(?:to|is|=|:)\s*([^\n,;]+)/i
    );
    if (nameUpdate) {
      name = nameUpdate[1].trim().replace(/[.;,]+$/, '');
      updatedFields.push('name');
    }
  }

  // Prefer an explicit labeled email line (including bullets) when present,
  // then fall back to "update … email to …" (value may be on the next line).
  const emailLabeled = s.match(
    new RegExp(
      READINESS_FIELD_LINE_PREFIX +
        String.raw`(?:sender\s+)?email(?:\s+address)?\s*[:\-–—]\s*([^\s\n,;]+@[^\s\n,;]+)`,
      'i'
    )
  );
  const emailUpdate = s.match(
    /\b(?:update|change|correct|set|use|fix)\b[\s\S]{0,48}\b(?:sender\s+)?email(?:\s+address)?\b[\s\S]{0,24}\b(?:to|is|=|:)\s*([^\s\n,;]+@[^\s\n,;]+)/i
  );
  const emailBare = s.match(
    /\b(?:sender\s+)?email(?:\s+address)?\b[\s\S]{0,24}\b(?:to|is|=|:)\s*([^\s\n,;]+@[^\s\n,;]+)/i
  );
  const emailOnly = s.match(
    /\b([A-Z0-9._%+\-]+(?:\\@|@)[A-Z0-9.\-]+\.[A-Z]{2,})\b/i
  );

  // Labeled block wins over update-form when both appear in the same message.
  if (emailLabeled) {
    email = normalizeCapturedEmail(emailLabeled[1]);
    updatedFields.push('email');
  } else if (emailUpdate) {
    email = normalizeCapturedEmail(emailUpdate[1]);
    updatedFields.push('email');
  } else if (emailBare) {
    email = normalizeCapturedEmail(emailBare[1]);
    updatedFields.push('email');
  } else if (
    emailOnly &&
    /\b(?:update|change|correct|set|use)\b[\s\S]{0,48}\b(?:sender\s+)?email/i.test(
      s
    )
  ) {
    email = normalizeCapturedEmail(emailOnly[1]);
    updatedFields.push('email');
  }

  const sigMatch = s.match(
    new RegExp(
      READINESS_FIELD_LINE_PREFIX +
        String.raw`(?:sender\s+)?signature\s*[:\-–—]\s*([^\n]+)`,
      'i'
    )
  );
  if (sigMatch) {
    signature = sigMatch[1].trim().replace(/[.;,]+$/, '');
    updatedFields.push('signature');
  } else {
    const sigUpdate = s.match(
      /\b(?:update|change|correct|set|use)\b[\s\S]{0,40}\bsignature\b[\s\S]{0,20}\b(?:to|is|=|:)\s*([^\n]+)/i
    );
    if (sigUpdate) {
      signature = sigUpdate[1].trim().replace(/[.;,]+$/, '');
      updatedFields.push('signature');
    }
  }

  return {
    name,
    email,
    signature,
    updatedFields: [...new Set(updatedFields)],
    hasAny: updatedFields.length > 0,
  };
}

/**
 * Parse reply-handling fields from free text.
 * Captures reply inbox, reply-to match (yes/no), and monitoring owner.
 */
function parseReplyHandlingFields(text) {
  const s = String(text || '').trim();
  const updatedFields = [];
  let replyInbox = null;
  let sameAsSender = null;
  let monitoringOwner = null;

  if (!s) {
    return {
      replyInbox,
      sameAsSender,
      monitoringOwner,
      updatedFields,
      hasAny: false,
    };
  }

  const inboxMatch = s.match(
    new RegExp(
      READINESS_FIELD_LINE_PREFIX +
        String.raw`reply\s+inbox\s*[:\-–—]\s*([^\n]+)`,
      'i'
    )
  );
  const inboxUpdate = s.match(
    /\b(?:update|change|correct|set|use)\b[\s\S]{0,40}\breply\s+inbox\b[\s\S]{0,24}\b(?:to|is|=|:)\s*([^\n,;]+)/i
  );
  if (inboxMatch) {
    replyInbox = normalizeCapturedEmail(inboxMatch[1]);
    updatedFields.push('reply_inbox');
  } else if (inboxUpdate) {
    replyInbox = normalizeCapturedEmail(inboxUpdate[1]);
    updatedFields.push('reply_inbox');
  }

  // "Reply-to should match the sender address: yes"
  const matchLine = s.match(
    new RegExp(
      READINESS_FIELD_LINE_PREFIX +
        String.raw`reply(?:\s*[-/]?\s*to)?\s+should\s+match(?:\s+the)?\s+sender(?:\s+address)?\s*[:\-–—]\s*([^\n]+)`,
      'i'
    )
  );
  const matchYesNo = s.match(
    /\breply(?:\s*[-/]?\s*to)?\b[\s\S]{0,40}\b(?:should\s+)?match(?:\s+the)?\s+sender\b[\s\S]{0,20}\b(yes|no|true|false)\b/i
  );
  if (matchLine) {
    const v = matchLine[1].trim().toLowerCase();
    if (/^(?:yes|y|true|same)\b/i.test(v)) sameAsSender = true;
    else if (/^(?:no|n|false|different)\b/i.test(v)) sameAsSender = false;
    else if (/\byes\b/i.test(v)) sameAsSender = true;
    else if (/\bno\b/i.test(v)) sameAsSender = false;
    if (sameAsSender !== null) updatedFields.push('same_as_sender');
  } else if (matchYesNo) {
    sameAsSender = /^(?:yes|y|true)$/i.test(matchYesNo[1]);
    updatedFields.push('same_as_sender');
  } else if (/\bsame\s+as\s+(?:the\s+)?sender\b/i.test(s)) {
    sameAsSender = true;
    updatedFields.push('same_as_sender');
  } else if (/\bdifferent\s+from\s+(?:the\s+)?sender\b/i.test(s)) {
    sameAsSender = false;
    updatedFields.push('same_as_sender');
  }

  const ownerMatch = s.match(
    new RegExp(
      READINESS_FIELD_LINE_PREFIX +
        String.raw`(?:reply\s+)?monitoring\s+owner(?:\s*\/\s*process)?\s*[:\-–—]\s*([^\n]+)`,
      'i'
    )
  );
  if (ownerMatch) {
    monitoringOwner = ownerMatch[1].trim().replace(/[.;,]+$/, '');
    updatedFields.push('monitoring_owner');
  }

  return {
    replyInbox,
    sameAsSender,
    monitoringOwner,
    updatedFields: [...new Set(updatedFields)],
    hasAny: updatedFields.length > 0,
  };
}

/**
 * Merge prior reply-handling slots with newly parsed fields.
 */
function mergeReplyHandlingState(prior = {}, parsed = {}) {
  const replyInboxRaw =
    parsed.replyInbox != null && parsed.replyInbox !== ''
      ? parsed.replyInbox
      : prior.replyInbox || prior.reply_inbox || null;
  const replyInbox = replyInboxRaw
    ? normalizeCapturedEmail(replyInboxRaw)
    : null;
  const sameAsSender =
    parsed.sameAsSender != null
      ? parsed.sameAsSender
      : prior.replyToMatchesSender != null
        ? prior.replyToMatchesSender
        : prior.sameAsSender != null
          ? prior.sameAsSender
          : null;
  const monitoringOwner =
    parsed.monitoringOwner != null && parsed.monitoringOwner !== ''
      ? parsed.monitoringOwner
      : prior.replyMonitoringOwner || prior.monitoringOwner || null;
  const confirmed = Boolean(
    replyInbox && sameAsSender !== null && monitoringOwner
  );
  return {
    replyInbox,
    replyToMatchesSender: sameAsSender,
    replyMonitoringOwner: monitoringOwner,
    replyInboxConfirmed: confirmed,
    replyHandlingConfirmed: confirmed,
    missing: [
      !replyInbox ? 'reply inbox' : null,
      sameAsSender === null ? 'reply-to match sender' : null,
      !monitoringOwner ? 'reply monitoring owner' : null,
    ].filter(Boolean),
  };
}

/**
 * Merge prior sender identity slots with newly parsed fields.
 */
function mergeSenderIdentityState(prior = {}, parsed = {}) {
  const name =
    parsed.name != null && parsed.name !== ''
      ? parsed.name
      : prior.senderName || prior.name || null;
  const emailRaw =
    parsed.email != null && parsed.email !== ''
      ? parsed.email
      : prior.senderEmail || prior.email || null;
  const email = emailRaw ? normalizeCapturedEmail(emailRaw) : null;
  const signature =
    parsed.signature != null && parsed.signature !== ''
      ? parsed.signature
      : prior.senderSignature || prior.signature || null;
  const confirmed = Boolean(name && email && signature);
  return {
    senderName: name,
    senderEmail: email,
    senderSignature: signature,
    senderIdentityConfirmed: confirmed,
    missing: [
      !name ? 'sender name' : null,
      !email ? 'sender email' : null,
      !signature ? 'signature' : null,
    ].filter(Boolean),
  };
}

/**
 * Normalize a free-text operational path phrase to a canonical choice.
 * @returns {{ id: string, label: string, selectionNote: string }|null}
 */
function normalizeOperationalPathChoice(raw) {
  const s = String(raw || '').trim();
  if (!s) return null;
  for (const choice of Object.values(OPERATIONAL_PATH_CHOICES)) {
    if (choice.match.test(s)) {
      return {
        id: choice.id,
        label: choice.label,
        selectionNote: choice.selectionNote,
      };
    }
  }
  return null;
}

/**
 * Parse operational path selection from free text.
 * Path selection confirms readiness only — never execute approval.
 * @param {string} text
 * @param {{ allowBarePath?: boolean }} [opts]
 * @returns {{
 *   pathId: string|null,
 *   pathLabel: string|null,
 *   selectionNote: string|null,
 *   isPathSelectionOnly: boolean,
 *   updatedFields: string[],
 *   hasAny: boolean
 * }}
 */
function parseOperationalPathSelection(text, opts = {}) {
  const s = String(text || '').trim();
  const empty = {
    pathId: null,
    pathLabel: null,
    selectionNote: null,
    isPathSelectionOnly: false,
    updatedFields: [],
    hasAny: false,
  };
  if (!s) return empty;

  const isPathSelectionOnly =
    /\bpath\s+selection\s+only\b/i.test(s) ||
    /\bnot\s+(?:an?\s+)?execute\s+approval\b/i.test(s) ||
    /\bnot\s+execution\s+approval\b/i.test(s) ||
    /\bselection\s+only[,.]?\s+not\s+execute\b/i.test(s);

  const hasSelectFraming =
    /\b(?:select(?:ed)?|choose|chose|pick(?:ed)?|set|confirm(?:ed)?)\s+operational\s+path(?:\s+selection)?\b/i.test(
      s
    ) ||
    /\boperational\s+path(?:\s+selection)?\s*(?:is\s+)?[:\-–—]/i.test(s) ||
    isPathSelectionOnly;

  // "Select operational path: manual send export for operator review"
  const labeled = s.match(
    /\b(?:select(?:ed)?|choose|chose|pick(?:ed)?|set|confirm(?:ed)?)\s+operational\s+path(?:\s+selection)?\s*[:\-–—]\s*([^\n.]+)/i
  );
  // "operational path: …" / "operational path is …"
  const bareLabeled = s.match(
    /\boperational\s+path(?:\s+selection)?\s*(?:is\s+)?[:\-–—]\s*([^\n.]+)/i
  );
  const selectedChunk = labeled
    ? labeled[1].trim()
    : bareLabeled
      ? bareLabeled[1].trim()
      : null;

  let choice = selectedChunk
    ? normalizeOperationalPathChoice(selectedChunk)
    : null;

  // Explicit select/path-selection framing with a known path phrase in the message.
  if (!choice && hasSelectFraming) {
    choice = normalizeOperationalPathChoice(s);
  }

  // Inside an active operational-path substep, a bare known path phrase counts.
  if (!choice && opts.allowBarePath) {
    choice = normalizeOperationalPathChoice(s);
  }

  if (!choice) return { ...empty, isPathSelectionOnly };

  return {
    pathId: choice.id,
    pathLabel: choice.label,
    selectionNote: choice.selectionNote,
    isPathSelectionOnly: true,
    updatedFields: ['operational_path'],
    hasAny: true,
  };
}

/**
 * Merge prior operational-path slots with newly parsed selection.
 */
function mergeOperationalPathState(prior = {}, parsed = {}) {
  const pathId =
    parsed.pathId ||
    prior.operationalPathId ||
    prior.pathId ||
    null;
  const pathLabel =
    parsed.pathLabel ||
    prior.operationalPathLabel ||
    prior.pathLabel ||
    (pathId &&
      OPERATIONAL_PATH_CHOICES[pathId] &&
      OPERATIONAL_PATH_CHOICES[pathId].label) ||
    null;
  const selectionNote =
    parsed.selectionNote ||
    (pathId &&
      OPERATIONAL_PATH_CHOICES[pathId] &&
      OPERATIONAL_PATH_CHOICES[pathId].selectionNote) ||
    'This is path selection only. Nothing has been prepared or executed.';
  const confirmed = Boolean(pathId && pathLabel);
  return {
    operationalPathId: pathId,
    operationalPathLabel: pathLabel,
    operationalPathChosen: confirmed,
    selectionNote,
    missing: confirmed ? [] : ['operational path choice'],
  };
}

/**
 * Normalize readiness item ids so concept keys and substep ids align.
 * Operator answers are interpreted relative to the active substep — they do
 * not need to restate the internal key.
 */
function normalizeReadinessItemId(id) {
  const s = String(id || '').trim();
  if (!s) return null;
  if (s === 'follow_up_tracking_process') return 'follow_up_tracking';
  if (s === 'operational_path_selection') return 'operational_path';
  if (s === 'reply_inbox_handling') return 'reply_handling';
  if (s === 'sender') return 'sender_identity';
  if (
    s === 'reply_monitoring' ||
    s === 'reply_monitoring_owner' ||
    s === 'reply_monitoring_batch_review' ||
    s === 'broader_rollout_batch1'
  ) {
    return 'reply_monitoring_batch1';
  }
  return s;
}

/**
 * Canonical display for follow-up tracking location.
 */
function normalizeFollowUpTrackingLocation(raw) {
  let s = String(raw || '')
    .trim()
    .replace(/[.;,]+$/, '')
    .replace(/\s+for\s+now\b/i, '')
    .trim();
  if (!s) return null;
  if (
    /manual[- ]?send\s+export(?:\s*\/\s*|\s+|\/)review\s+sheet/i.test(s)
  ) {
    return 'the manual-send export/review sheet';
  }
  if (/^the\s+/i.test(s) === false && /\bsheet\b/i.test(s)) {
    s = `the ${s}`;
  }
  return s;
}

/**
 * Canonical Follow-up 1 / 2 timing labels.
 */
function normalizeFollowUpTiming(raw, which) {
  const s = String(raw || '').trim();
  if (!s) return null;
  const daysMatch = s.match(/\b(\d+)\s+business\s+days?\b/i);
  if (daysMatch) {
    const n = Number(daysMatch[1]);
    if (which === 1 && n === 3) {
      return 'about 3 business days after first touch';
    }
    if (which === 2 && n === 7) {
      return 'about 7 business days after first touch';
    }
    return `about ${n} business days after first touch`;
  }
  if (which === 1 && /\byes\b/i.test(s)) {
    return 'about 3 business days after first touch';
  }
  if (which === 2 && /\byes\b/i.test(s)) {
    return 'about 7 business days after first touch';
  }
  return s.replace(/[.;,]+$/, '').trim() || null;
}

/**
 * Parse follow-up tracking fields from free text (fills or corrections).
 * Interprets answers relative to the active follow-up tracking substep —
 * operator does not need to restate the readiness key.
 * @returns {{
 *   trackingLocation: string|null,
 *   followUp1Timing: string|null,
 *   followUp2Timing: string|null,
 *   reviewFirstManual: boolean|null,
 *   automaticFollowUpSendsApproved: boolean|null,
 *   updatedFields: string[],
 *   hasAny: boolean
 * }}
 */
function parseFollowUpTrackingFields(text) {
  const s = String(text || '').trim();
  const empty = {
    trackingLocation: null,
    followUp1Timing: null,
    followUp2Timing: null,
    reviewFirstManual: null,
    automaticFollowUpSendsApproved: null,
    updatedFields: [],
    hasAny: false,
  };
  if (!s) return empty;

  const updatedFields = [];
  let trackingLocation = null;
  let followUp1Timing = null;
  let followUp2Timing = null;
  let reviewFirstManual = null;
  let automaticFollowUpSendsApproved = null;

  const locationLabeled = s.match(
    new RegExp(
      READINESS_FIELD_LINE_PREFIX +
        String.raw`(?:follow[- ]?up\s+)?(?:status\s+)?(?:tracked\s+in|tracking\s+location|track(?:ed|ing)?\s+(?:status\s+)?(?:in|location))\s*[:\-–—]\s*([^\n]+)`,
      'i'
    )
  );
  const locationPhrase = s.match(
    /\b(?:follow[- ]?up\s+status\s+)?(?:should\s+be\s+)?tracked\s+in\s+([^\n.]+)/i
  );
  if (locationLabeled) {
    trackingLocation = normalizeFollowUpTrackingLocation(locationLabeled[1]);
  } else if (locationPhrase) {
    trackingLocation = normalizeFollowUpTrackingLocation(locationPhrase[1]);
  } else if (
    /manual[- ]?send\s+export(?:\s*\/\s*|\s+|\/)review\s+sheet/i.test(s)
  ) {
    trackingLocation = normalizeFollowUpTrackingLocation(
      'manual-send export/review sheet'
    );
  }
  if (trackingLocation) updatedFields.push('tracking_location');

  const fu1Labeled = s.match(
    new RegExp(
      READINESS_FIELD_LINE_PREFIX +
        String.raw`follow[- ]?up\s*1\s*(?:timing|planned)?\s*[:\-–—]\s*([^\n]+)`,
      'i'
    )
  );
  const fu1Phrase = s.match(
    /\bfollow[- ]?up\s*1\b[\s\S]{0,120}?\b((?:about\s+)?\d+\s+business\s+days?\s+after\s+first\s+touch)\b/i
  );
  const fu1Bare = s.match(
    /\b(?:about\s+)?3\s+business\s+days?\s+after\s+first\s+touch\b/i
  );
  if (fu1Labeled) {
    followUp1Timing = normalizeFollowUpTiming(fu1Labeled[1], 1);
  } else if (fu1Phrase) {
    followUp1Timing = normalizeFollowUpTiming(fu1Phrase[1], 1);
  } else if (fu1Bare) {
    followUp1Timing = normalizeFollowUpTiming(fu1Bare[0], 1);
  }
  if (followUp1Timing) updatedFields.push('follow_up_1_timing');

  const fu2Labeled = s.match(
    new RegExp(
      READINESS_FIELD_LINE_PREFIX +
        String.raw`follow[- ]?up\s*2\s*(?:timing|planned)?\s*[:\-–—]\s*([^\n]+)`,
      'i'
    )
  );
  const fu2Phrase = s.match(
    /\bfollow[- ]?up\s*2\b[\s\S]{0,120}?\b((?:about\s+)?\d+\s+business\s+days?\s+after\s+first\s+touch)\b/i
  );
  const fu2Bare = s.match(
    /\b(?:about\s+)?7\s+business\s+days?\s+after\s+first\s+touch\b/i
  );
  if (fu2Labeled) {
    followUp2Timing = normalizeFollowUpTiming(fu2Labeled[1], 2);
  } else if (fu2Phrase) {
    followUp2Timing = normalizeFollowUpTiming(fu2Phrase[1], 2);
  } else if (fu2Bare) {
    followUp2Timing = normalizeFollowUpTiming(fu2Bare[0], 2);
  }
  if (followUp2Timing) updatedFields.push('follow_up_2_timing');

  if (
    /\breview[- ]?first(?:\s*\/\s*|\s+|\/)manual\b/i.test(s) ||
    /\bremain\s+review[- ]?first\b/i.test(s) ||
    /\bunless\s+explicitly\s+enabled\s+later\b/i.test(s)
  ) {
    reviewFirstManual = true;
    updatedFields.push('review_first_manual');
  } else if (
    /\b(?:do\s+not|don't)\s+remain\s+review[- ]?first\b/i.test(s) ||
    /\breview[- ]?first(?:\s*\/\s*|\s+|\/)manual\s*[:\-–—]\s*(?:no|false)\b/i.test(
      s
    )
  ) {
    reviewFirstManual = false;
    updatedFields.push('review_first_manual');
  }

  if (
    /\bno\s+automatic\s+follow[- ]?up\s+sends?\s+(?:are\s+)?approved\b/i.test(
      s
    ) ||
    /\bautomatic\s+follow[- ]?up\s+sends?\s+(?:are\s+)?not\s+approved\b/i.test(
      s
    ) ||
    /\b(?:do\s+not|don't)\s+approve\s+automatic\s+follow[- ]?up\b/i.test(s)
  ) {
    automaticFollowUpSendsApproved = false;
    updatedFields.push('automatic_follow_up_sends');
  } else if (
    /\bautomatic\s+follow[- ]?up\s+sends?\s+(?:are\s+)?approved\b/i.test(s) ||
    /\bapprove(?:d)?\s+automatic\s+follow[- ]?up\s+sends?\b/i.test(s)
  ) {
    automaticFollowUpSendsApproved = true;
    updatedFields.push('automatic_follow_up_sends');
  }

  return {
    trackingLocation,
    followUp1Timing,
    followUp2Timing,
    reviewFirstManual,
    automaticFollowUpSendsApproved,
    updatedFields: [...new Set(updatedFields)],
    hasAny: updatedFields.length > 0,
  };
}

/**
 * Merge prior follow-up tracking slots with newly parsed fields.
 */
function mergeFollowUpTrackingState(prior = {}, parsed = {}) {
  const trackingLocation =
    parsed.trackingLocation != null && parsed.trackingLocation !== ''
      ? parsed.trackingLocation
      : prior.followUpTrackingLocation ||
        prior.trackingLocation ||
        null;
  const followUp1Timing =
    parsed.followUp1Timing != null && parsed.followUp1Timing !== ''
      ? parsed.followUp1Timing
      : prior.followUp1Timing || null;
  const followUp2Timing =
    parsed.followUp2Timing != null && parsed.followUp2Timing !== ''
      ? parsed.followUp2Timing
      : prior.followUp2Timing || null;
  const reviewFirstManual =
    parsed.reviewFirstManual != null
      ? parsed.reviewFirstManual
      : prior.followUpReviewFirstManual != null
        ? prior.followUpReviewFirstManual
        : prior.reviewFirstManual != null
          ? prior.reviewFirstManual
          : null;
  const automaticFollowUpSendsApproved =
    parsed.automaticFollowUpSendsApproved != null
      ? parsed.automaticFollowUpSendsApproved
      : prior.automaticFollowUpSendsApproved != null
        ? prior.automaticFollowUpSendsApproved
        : null;
  const confirmed = Boolean(
    trackingLocation &&
      followUp1Timing &&
      followUp2Timing &&
      reviewFirstManual !== null &&
      automaticFollowUpSendsApproved !== null
  );
  return {
    followUpTrackingLocation: trackingLocation,
    followUp1Timing,
    followUp2Timing,
    followUpReviewFirstManual: reviewFirstManual,
    automaticFollowUpSendsApproved,
    followUpTrackingConfirmed: confirmed,
    missing: [
      !trackingLocation ? 'tracking location' : null,
      !followUp1Timing ? 'Follow-up 1 timing' : null,
      !followUp2Timing ? 'Follow-up 2 timing' : null,
      reviewFirstManual === null ? 'review-first/manual status' : null,
      automaticFollowUpSendsApproved === null
        ? 'automatic follow-up send approval'
        : null,
    ].filter(Boolean),
  };
}

/**
 * Canonical reply-monitoring owner display.
 */
function normalizeReplyMonitoringOwner(raw) {
  const s = String(raw || '')
    .trim()
    .replace(/[.;,]+$/, '')
    .trim();
  return s || null;
}

/**
 * Canonical response-review process label.
 */
function normalizeResponseReviewProcess(raw) {
  const s = String(raw || '')
    .trim()
    .replace(/[.;,]+$/, '')
    .trim();
  if (!s) return null;
  if (
    /\breview(?:ed|s)?\s+manually\b/i.test(s) ||
    /\bmanual(?:ly)?\s+(?:review|reviewed)\b/i.test(s)
  ) {
    return 'Responses reviewed manually before any follow-up or broader rollout decision';
  }
  return s;
}

/**
 * Canonical positive-reply handling label.
 */
function normalizePositiveReplyHandling(raw) {
  const s = String(raw || '')
    .trim()
    .replace(/[.;,]+$/, '')
    .trim();
  if (!s) return null;
  if (
    /\bconversation\b/i.test(s) ||
    /\bwalkthrough\b/i.test(s) ||
    /\bopportunit(?:y|ies)\b/i.test(s)
  ) {
    return 'Positive replies handled as conversation/walkthrough opportunities';
  }
  return s;
}

/**
 * Canonical negative / not-now reply handling label.
 */
function normalizeNegativeReplyHandling(raw) {
  const s = String(raw || '')
    .trim()
    .replace(/[.;,]+$/, '')
    .trim();
  if (!s) return null;
  if (/\brespect(?:ed|s)?\b/i.test(s) || /\bnoted?\b/i.test(s)) {
    return 'Negative or not-now replies respected and noted';
  }
  return s;
}

/**
 * Parse reply monitoring / Batch 1 review fields from free text.
 * Interprets answers relative to the active reply-monitoring substep —
 * operator does not need to restate the readiness key.
 * @returns {{
 *   replyMonitoringOwner: string|null,
 *   responseReviewProcess: string|null,
 *   positiveReplyHandling: string|null,
 *   negativeReplyHandling: string|null,
 *   broaderRolloutBlocked: boolean|null,
 *   batch1ReviewBeforeExpansion: boolean|null,
 *   updatedFields: string[],
 *   hasAny: boolean
 * }}
 */
function parseReplyMonitoringBatchReviewFields(text) {
  const s = String(text || '').trim();
  const empty = {
    replyMonitoringOwner: null,
    responseReviewProcess: null,
    positiveReplyHandling: null,
    negativeReplyHandling: null,
    broaderRolloutBlocked: null,
    batch1ReviewBeforeExpansion: null,
    updatedFields: [],
    hasAny: false,
  };
  if (!s) return empty;

  const updatedFields = [];
  let replyMonitoringOwner = null;
  let responseReviewProcess = null;
  let positiveReplyHandling = null;
  let negativeReplyHandling = null;
  let broaderRolloutBlocked = null;
  let batch1ReviewBeforeExpansion = null;

  const ownerLabeled = s.match(
    new RegExp(
      READINESS_FIELD_LINE_PREFIX +
        String.raw`(?:reply\s+)?monitoring\s+owner(?:\s*\/\s*process)?\s*[:\-–—]\s*([^\n]+)`,
      'i'
    )
  );
  const ownerPhrase = s.match(
    /\b(?:who\s+will\s+monitor\s+replies|monitor(?:ing)?\s+owner)\b[\s\S]{0,40}?\b([A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+)+)\b/
  );
  if (ownerLabeled) {
    replyMonitoringOwner = normalizeReplyMonitoringOwner(ownerLabeled[1]);
  } else if (ownerPhrase) {
    replyMonitoringOwner = normalizeReplyMonitoringOwner(ownerPhrase[1]);
  } else if (
    /\bJacob\s+Maynard\b/i.test(s) &&
    /\b(?:monitor(?:ing)?|replies)\b/i.test(s)
  ) {
    replyMonitoringOwner = 'Jacob Maynard';
  }
  if (replyMonitoringOwner) updatedFields.push('reply_monitoring_owner');

  if (
    /\bresponses?\s+should\s+be\s+reviewed\s+manually\b/i.test(s) ||
    /\breview(?:ed|s)?\s+manually\s+before\s+any\s+follow[- ]?up\b/i.test(s) ||
    /\bmanual(?:ly)?\s+(?:review|reviewed)\b[\s\S]{0,80}\bbroader\s+rollout\b/i.test(
      s
    ) ||
    new RegExp(
      READINESS_FIELD_LINE_PREFIX +
        String.raw`response(?:s)?\s+review(?:\s+process)?\s*[:\-–—]\s*([^\n]+)`,
      'i'
    ).test(s)
  ) {
    const labeled = s.match(
      new RegExp(
        READINESS_FIELD_LINE_PREFIX +
          String.raw`response(?:s)?\s+review(?:\s+process)?\s*[:\-–—]\s*([^\n]+)`,
        'i'
      )
    );
    responseReviewProcess = normalizeResponseReviewProcess(
      labeled ? labeled[1] : 'reviewed manually'
    );
    updatedFields.push('response_review_process');
  }

  if (
    /\bpositive\s+replies?\b[\s\S]{0,120}\b(?:conversation|walkthrough|opportunit)/i.test(
      s
    ) ||
    new RegExp(
      READINESS_FIELD_LINE_PREFIX +
        String.raw`positive\s+replies?(?:\s+handling)?\s*[:\-–—]\s*([^\n]+)`,
      'i'
    ).test(s)
  ) {
    const labeled = s.match(
      new RegExp(
        READINESS_FIELD_LINE_PREFIX +
          String.raw`positive\s+replies?(?:\s+handling)?\s*[:\-–—]\s*([^\n]+)`,
        'i'
      )
    );
    positiveReplyHandling = normalizePositiveReplyHandling(
      labeled
        ? labeled[1]
        : 'conversation/walkthrough opportunities'
    );
    updatedFields.push('positive_reply_handling');
  }

  if (
    /\b(?:negative(?:\s+or\s+not[- ]?now)?|not[- ]?now)\s+replies?\b[\s\S]{0,120}\b(?:respect(?:ed|s)?|noted?)\b/i.test(
      s
    ) ||
    new RegExp(
      READINESS_FIELD_LINE_PREFIX +
        String.raw`(?:negative(?:\s+or\s+not[- ]?now)?|not[- ]?now)\s+replies?(?:\s+handling)?\s*[:\-–—]\s*([^\n]+)`,
      'i'
    ).test(s)
  ) {
    const labeled = s.match(
      new RegExp(
        READINESS_FIELD_LINE_PREFIX +
          String.raw`(?:negative(?:\s+or\s+not[- ]?now)?|not[- ]?now)\s+replies?(?:\s+handling)?\s*[:\-–—]\s*([^\n]+)`,
        'i'
      )
    );
    negativeReplyHandling = normalizeNegativeReplyHandling(
      labeled ? labeled[1] : 'respected and noted'
    );
    updatedFields.push('negative_reply_handling');
  }

  if (
    /\bbroader\s+rollout\s+remains?\s+blocked\b/i.test(s) ||
    /\bbroader\s+rollout\b[\s\S]{0,80}\bblocked\s+until\s+Batch\s*1\b/i.test(
      s
    ) ||
    /\bbroader\s+rollout\s+blocked\s*[:\-–—]\s*(?:yes|true|blocked)\b/i.test(s)
  ) {
    broaderRolloutBlocked = true;
    updatedFields.push('broader_rollout_blocked');
  } else if (
    /\bbroader\s+rollout\s+(?:is\s+)?(?:not\s+blocked|unblocked|approved)\b/i.test(
      s
    ) ||
    /\bbroader\s+rollout\s+blocked\s*[:\-–—]\s*(?:no|false)\b/i.test(s)
  ) {
    broaderRolloutBlocked = false;
    updatedFields.push('broader_rollout_blocked');
  }

  if (
    /\bBatch\s*1\s+results?\s+(?:should\s+be\s+)?reviewed\s+before\s+expanding\b/i.test(
      s
    ) ||
    /\bbefore\s+expanding\s+to\s+Cedar\b/i.test(s) ||
    /\bBatch\s*1\s+review\s+(?:required\s+)?before\s+expansion\b/i.test(s) ||
    /\bany\s+new\s+prospect\s+segment\b/i.test(s)
  ) {
    batch1ReviewBeforeExpansion = true;
    updatedFields.push('batch1_review_before_expansion');
  } else if (
    /\bBatch\s*1\s+review\s+(?:before\s+expansion\s+)?(?:not\s+required|optional)\b/i.test(
      s
    ) ||
    /\bBatch\s*1\s+review\s+before\s+expansion\s*[:\-–—]\s*(?:no|false)\b/i.test(
      s
    )
  ) {
    batch1ReviewBeforeExpansion = false;
    updatedFields.push('batch1_review_before_expansion');
  }

  return {
    replyMonitoringOwner,
    responseReviewProcess,
    positiveReplyHandling,
    negativeReplyHandling,
    broaderRolloutBlocked,
    batch1ReviewBeforeExpansion,
    updatedFields: [...new Set(updatedFields)],
    hasAny: updatedFields.length > 0,
  };
}

/**
 * Merge prior reply-monitoring / Batch 1 review slots with newly parsed fields.
 */
function mergeReplyMonitoringBatchReviewState(prior = {}, parsed = {}) {
  const replyMonitoringOwner =
    parsed.replyMonitoringOwner != null && parsed.replyMonitoringOwner !== ''
      ? parsed.replyMonitoringOwner
      : prior.replyMonitoringOwner || prior.monitoringOwner || null;
  const responseReviewProcess =
    parsed.responseReviewProcess != null && parsed.responseReviewProcess !== ''
      ? parsed.responseReviewProcess
      : prior.responseReviewProcess || null;
  const positiveReplyHandling =
    parsed.positiveReplyHandling != null && parsed.positiveReplyHandling !== ''
      ? parsed.positiveReplyHandling
      : prior.positiveReplyHandling || null;
  const negativeReplyHandling =
    parsed.negativeReplyHandling != null && parsed.negativeReplyHandling !== ''
      ? parsed.negativeReplyHandling
      : prior.negativeReplyHandling || null;
  const broaderRolloutBlocked =
    parsed.broaderRolloutBlocked != null
      ? parsed.broaderRolloutBlocked
      : prior.broaderRolloutBlocked != null
        ? prior.broaderRolloutBlocked
        : null;
  const batch1ReviewBeforeExpansion =
    parsed.batch1ReviewBeforeExpansion != null
      ? parsed.batch1ReviewBeforeExpansion
      : prior.batch1ReviewBeforeExpansion != null
        ? prior.batch1ReviewBeforeExpansion
        : null;
  const confirmed = Boolean(
    replyMonitoringOwner &&
      responseReviewProcess &&
      positiveReplyHandling &&
      negativeReplyHandling &&
      broaderRolloutBlocked !== null &&
      batch1ReviewBeforeExpansion !== null
  );
  return {
    replyMonitoringOwner,
    responseReviewProcess,
    positiveReplyHandling,
    negativeReplyHandling,
    broaderRolloutBlocked,
    batch1ReviewBeforeExpansion,
    replyMonitoringBatch1Confirmed: confirmed,
    batch1ResultsReviewed: Boolean(
      confirmed && broaderRolloutBlocked === true && batch1ReviewBeforeExpansion
    ),
    missing: [
      !replyMonitoringOwner ? 'reply monitoring owner' : null,
      !responseReviewProcess ? 'response review process' : null,
      !positiveReplyHandling ? 'positive reply handling' : null,
      !negativeReplyHandling ? 'negative/not-now reply handling' : null,
      broaderRolloutBlocked === null
        ? 'broader rollout blocked status'
        : null,
      batch1ReviewBeforeExpansion === null
        ? 'Batch 1 review requirement before expansion'
        : null,
    ].filter(Boolean),
  };
}

/**
 * True when active readiness id is the reply-monitoring / Batch 1 substep.
 */
function isReplyMonitoringBatchReviewItemId(id) {
  const n = normalizeReadinessItemId(id) || id;
  return (
    n === 'reply_monitoring_batch1' ||
    n === 'reply_monitoring_batch_review' ||
    n === 'reply_monitoring' ||
    n === 'reply_monitoring_owner' ||
    n === 'broader_rollout_batch1'
  );
}

function resolveActiveReadinessItemId(opts = {}) {
  const raw =
    opts.activeReadinessItemId ||
    (opts.slots && opts.slots.activeReadinessItemId) ||
    opts.readinessItemId ||
    (opts.selectedReadinessItem && opts.selectedReadinessItem.id) ||
    null;
  if (raw) return normalizeReadinessItemId(raw);

  const priorIntent = String(opts.priorIntent || opts.intent || '');
  if (
    priorIntent === 'readiness_substep' ||
    priorIntent === 'readiness_field_correction' ||
    priorIntent === 'readiness_substep_update'
  ) {
    return normalizeReadinessItemId(
      (opts.slots && opts.slots.activeReadinessItemId) ||
        opts.readinessItemId ||
        null
    );
  }
  return null;
}

/**
 * True when the operator is correcting or filling a readiness field —
 * including while already inside a readiness substep.
 */
function looksLikeReadinessFieldCorrection(text, opts = {}) {
  const s = String(text || '').trim();
  if (!s) return false;

  // Final full readiness summaries are never field corrections.
  if (looksLikeCampaignReadySummary(s)) return false;

  const activeId = resolveActiveReadinessItemId(opts);
  const allowBarePath =
    activeId === 'operational_path' ||
    activeId === 'operational_path_selection';

  // Concrete operational path choice confirms readiness — never execute.
  // Check before substep-selection short-circuit so
  // "Select operational path: manual send export…" is treated as confirmation.
  const pathParsed = parseOperationalPathSelection(s, { allowBarePath });
  if (pathParsed.hasAny) {
    return true;
  }

  // Follow-up tracking answers inside the active substep (or a full answer block).
  const followUpParsed = parseFollowUpTrackingFields(s);
  if (
    followUpParsed.hasAny &&
    (activeId === 'follow_up_tracking' ||
      activeId === 'follow_up_tracking_process' ||
      followUpParsed.updatedFields.length >= 3)
  ) {
    return true;
  }

  // Reply monitoring / Batch 1 review answers inside the active substep.
  const monitoringParsed = parseReplyMonitoringBatchReviewFields(s);
  if (
    monitoringParsed.hasAny &&
    (isReplyMonitoringBatchReviewItemId(activeId) ||
      monitoringParsed.updatedFields.length >= 3)
  ) {
    return true;
  }

  // Selecting a checklist item is not a field correction.
  if (detectSelectedReadinessItem(s) && !parseSenderIdentityFields(s).hasAny) {
    // Allow "resolve sender identity" without fields to stay as substep selection.
    if (!/\b(?:update|change|correct|set)\b/i.test(s)) return false;
  }

  // Checklist / summarize asks are not field corrections.
  if (looksLikeOperatorReadinessCheck(s) && !/\b(?:update|change|correct)\b/i.test(s)) {
    return false;
  }

  // Explicit correction / update of a readiness field.
  if (
    /\b(?:update|change|correct|set|fix)\b/i.test(s) &&
    /\b(?:sender\s+)?(?:email(?:\s+address)?|name|signature|reply(?:\s*[-/]?\s*to)?(?:\s+(?:inbox|address|email))?|inbox|follow[- ]?up)\b/i.test(
      s
    )
  ) {
    return true;
  }

  if (activeId === 'sender_identity' || activeId === 'sender') {
    if (parseSenderIdentityFields(s).hasAny) return true;
  }
  if (activeId === 'reply_handling' || activeId === 'reply_inbox_handling') {
    if (parseReplyHandlingFields(s).hasAny) return true;
  }
  if (
    activeId === 'follow_up_tracking' ||
    activeId === 'follow_up_tracking_process'
  ) {
    if (followUpParsed.hasAny) return true;
  }
  if (isReplyMonitoringBatchReviewItemId(activeId)) {
    if (monitoringParsed.hasAny) return true;
  }

  // Labeled sender block even without an active substep id (post-approval).
  // Support bullet prefixes ("- Sender name:") via parseSenderIdentityFields.
  const senderFields = parseSenderIdentityFields(s);
  if (
    senderFields.updatedFields.length >= 2 ||
    (senderFields.name && senderFields.email && senderFields.signature)
  ) {
    return true;
  }

  // Labeled reply-handling block (inbox / match / monitoring owner).
  // Do not treat a reply-monitoring Batch 1 answer as reply-inbox handling.
  const replyFields = parseReplyHandlingFields(s);
  const monitoringOnlyOwner =
    replyFields.updatedFields.length === 1 &&
    replyFields.updatedFields[0] === 'monitoring_owner' &&
    monitoringParsed.hasAny;
  if (
    !monitoringOnlyOwner &&
    (replyFields.updatedFields.length >= 2 ||
      (replyFields.replyInbox &&
        replyFields.sameAsSender !== null &&
        replyFields.monitoringOwner))
  ) {
    return true;
  }

  // Labeled reply-monitoring / Batch 1 review block.
  if (monitoringParsed.updatedFields.length >= 3) {
    return true;
  }

  return false;
}

/**
 * Next unresolved readiness item after confirming `itemId`.
 */
function nextReadinessItemAfter(itemId, context = {}) {
  const order = [
    'sender_identity',
    'reply_handling',
    'operational_path',
    'follow_up_tracking',
    'reply_monitoring_batch1',
  ];
  const normalizedId = normalizeReadinessItemId(itemId) || itemId;
  const start = Math.max(0, order.indexOf(normalizedId));
  const state = knownReadinessState(context);
  const confirmed = {
    sender_identity: state.senderIdentityConfirmed,
    reply_handling: state.replyInboxConfirmed,
    operational_path: state.operationalPathChosen,
    follow_up_tracking: state.followUpTrackingConfirmed,
    reply_monitoring_batch1: Boolean(
      state.replyMonitoringBatch1Confirmed ||
        (state.replyMonitoringConfirmed && state.batch1ResultsReviewed)
    ),
  };

  for (let i = start + 1; i < order.length; i += 1) {
    const id = order[i];
    if (!confirmed[id]) {
      return (
        READINESS_NEXT_ITEM_PROMPTS[id] ||
        (READINESS_SUBSTEPS[id]
          ? {
              id,
              label: READINESS_SUBSTEPS[id].label,
              ask: READINESS_SUBSTEPS[id].questions[0],
              questions: READINESS_SUBSTEPS[id].questions,
            }
          : { id, label: id, ask: `Let's resolve ${id}.` })
      );
    }
  }
  return null;
}

/**
 * Compose acknowledgment for a readiness field correction / substep update.
 * Never includes Launch Gate operational options or execute-approval asks.
 */
function composeReadinessFieldCorrection(context = {}) {
  const text =
    context.operatorMessage || context.text || context.userMessage || '';
  const slots = { ...(context.slots || {}) };
  const priorFields = {
    senderName: slots.senderName || context.senderName || null,
    senderEmail: slots.senderEmail || context.senderEmail || null,
    senderSignature: slots.senderSignature || context.senderSignature || null,
    ...(context.priorFields || {}),
  };

  let activeId =
    resolveActiveReadinessItemId({
      ...context,
      slots,
      activeReadinessItemId: context.activeReadinessItemId || slots.activeReadinessItemId,
    }) || 'sender_identity';

  // Parse ALL explicit fields in the latest message, then merge into prior
  // state. Never drop previously provided fields unless replaced here.
  const senderParsed = parseSenderIdentityFields(text);
  const replyParsed = parseReplyHandlingFields(text);
  const pathParsed = parseOperationalPathSelection(text, {
    allowBarePath:
      activeId === 'operational_path' ||
      activeId === 'operational_path_selection',
  });
  const followUpParsed = parseFollowUpTrackingFields(text);
  const monitoringParsed = parseReplyMonitoringBatchReviewFields(text);
  const priorActiveId = normalizeReadinessItemId(activeId) || activeId;

  // Prefer active-substep context. Only infer from fields when the active
  // id is missing / clearly mismatched — never steal a monitoring answer
  // into reply_handling just because "monitoring owner" is present.
  if (isReplyMonitoringBatchReviewItemId(priorActiveId) && monitoringParsed.hasAny) {
    activeId = 'reply_monitoring_batch1';
  } else if (
    monitoringParsed.hasAny &&
    monitoringParsed.updatedFields.length >= 3 &&
    !senderParsed.hasAny &&
    !pathParsed.hasAny &&
    !followUpParsed.hasAny
  ) {
    activeId = 'reply_monitoring_batch1';
  } else if (
    followUpParsed.hasAny &&
    !senderParsed.hasAny &&
    !pathParsed.hasAny &&
    !monitoringParsed.hasAny &&
    !(
      replyParsed.hasAny &&
      (replyParsed.replyInbox || replyParsed.sameAsSender !== null)
    )
  ) {
    activeId = 'follow_up_tracking';
  } else if (
    pathParsed.hasAny &&
    !senderParsed.hasAny &&
    !replyParsed.hasAny &&
    !followUpParsed.hasAny &&
    !monitoringParsed.hasAny
  ) {
    activeId = 'operational_path';
  } else if (
    senderParsed.hasAny &&
    !replyParsed.hasAny &&
    !pathParsed.hasAny &&
    !followUpParsed.hasAny &&
    !monitoringParsed.hasAny
  ) {
    activeId = 'sender_identity';
  } else if (
    replyParsed.hasAny &&
    !senderParsed.hasAny &&
    !pathParsed.hasAny &&
    !followUpParsed.hasAny &&
    !monitoringParsed.hasAny &&
    !isReplyMonitoringBatchReviewItemId(priorActiveId)
  ) {
    activeId = 'reply_handling';
  }

  activeId = normalizeReadinessItemId(activeId) || activeId;

  const lines = [];
  let senderState = mergeSenderIdentityState(priorFields, {});
  let replyHandlingState = null;
  let operationalPathState = null;
  let followUpTrackingState = null;
  let replyMonitoringBatchReviewState = null;
  let nextItem = null;
  let itemConfirmed = false;
  const corrected = senderParsed.updatedFields.slice();

  if (activeId === 'sender_identity') {
    senderState = mergeSenderIdentityState(priorFields, senderParsed);
    const multiFieldUpdate = senderParsed.updatedFields.length >= 2;
    const fullIdentityBlock =
      Boolean(senderParsed.name) &&
      Boolean(senderParsed.email) &&
      Boolean(senderParsed.signature);
    const narrowUpdateOnly =
      senderParsed.updatedFields.length === 1 &&
      /\b(?:update|change|correct)\b/i.test(String(text));

    if (senderState.senderIdentityConfirmed) {
      itemConfirmed = true;
      // Narrow single-field correction that completes identity still acks the field.
      if (narrowUpdateOnly && senderParsed.email) {
        lines.push(`Updated sender email to ${senderState.senderEmail}.`);
        lines.push('');
      } else if (narrowUpdateOnly && senderParsed.name) {
        lines.push(`Updated sender name to ${senderState.senderName}.`);
        lines.push('');
      } else if (narrowUpdateOnly && senderParsed.signature) {
        lines.push(`Updated signature to ${senderState.senderSignature}.`);
        lines.push('');
      } else if (
        multiFieldUpdate &&
        /\b(?:update|change|correct)\b/i.test(String(text))
      ) {
        lines.push('Updated sender identity.');
        lines.push('');
      }

      lines.push('Sender identity is confirmed:');
      lines.push(`- Sender name: ${senderState.senderName}`);
      lines.push(`- Sender email address: ${senderState.senderEmail}`);
      lines.push(`- Signature: ${senderState.senderSignature}`);
      nextItem = nextReadinessItemAfter('sender_identity', {
        ...context,
        slots: {
          ...slots,
          senderIdentityConfirmed: true,
          senderName: senderState.senderName,
          senderEmail: senderState.senderEmail,
          senderSignature: senderState.senderSignature,
        },
        confirmedReadiness: {
          ...(context.confirmedReadiness || {}),
          sender_identity: true,
        },
      });
      if (nextItem) {
        const questions =
          Array.isArray(nextItem.questions) && nextItem.questions.length
            ? nextItem.questions
            : nextItem.ask
              ? [nextItem.ask]
              : [];
        lines.push('');
        lines.push(`Next readiness item: ${nextItem.label}.`);
        if (questions.length) {
          lines.push('');
          for (const q of questions) lines.push(q);
        }
        lines.push('');
        lines.push(READINESS_SUBSTEP_SAFETY_LINE);
      }
    } else {
      if (multiFieldUpdate || fullIdentityBlock) {
        lines.push('Updated sender identity.');
      } else if (senderParsed.email) {
        lines.push(`Updated sender email to ${senderState.senderEmail}.`);
      } else if (senderParsed.name) {
        lines.push(`Updated sender name to ${senderState.senderName}.`);
      } else if (senderParsed.signature) {
        lines.push(`Updated signature to ${senderState.senderSignature}.`);
      } else if (senderParsed.hasAny) {
        lines.push('Updated sender identity fields.');
      } else {
        lines.push('Recorded sender identity details.');
      }

      if (senderState.missing.length) {
        lines.push('');
        lines.push('Still needed for sender identity:');
        for (const m of senderState.missing) lines.push(`- ${m}`);
        lines.push('');
        lines.push(READINESS_SUBSTEP_SAFETY_LINE);
      }
    }
  } else if (activeId === 'reply_handling') {
    const priorReply = {
      replyInbox: slots.replyInbox || context.replyInbox || null,
      replyToMatchesSender:
        slots.replyToMatchesSender != null
          ? slots.replyToMatchesSender
          : context.replyToMatchesSender != null
            ? context.replyToMatchesSender
            : null,
      replyMonitoringOwner:
        slots.replyMonitoringOwner ||
        context.replyMonitoringOwner ||
        null,
    };
    const replyState = mergeReplyHandlingState(priorReply, replyParsed);
    const multiFieldUpdate = replyParsed.updatedFields.length >= 2;
    const fullReplyBlock =
      Boolean(replyParsed.replyInbox) &&
      replyParsed.sameAsSender !== null &&
      Boolean(replyParsed.monitoringOwner);
    const narrowUpdateOnly =
      replyParsed.updatedFields.length === 1 &&
      /\b(?:update|change|correct)\b/i.test(String(text));

    if (replyState.replyHandlingConfirmed) {
      itemConfirmed = true;
      if (narrowUpdateOnly && replyParsed.replyInbox) {
        lines.push(`Updated reply inbox to ${replyState.replyInbox}.`);
        lines.push('');
      } else if (
        multiFieldUpdate &&
        /\b(?:update|change|correct)\b/i.test(String(text))
      ) {
        lines.push('Updated reply handling.');
        lines.push('');
      }

      lines.push('Reply inbox / reply-to handling is confirmed:');
      lines.push(`- Reply inbox: ${replyState.replyInbox}`);
      lines.push(
        `- Reply-to matches sender address: ${
          replyState.replyToMatchesSender ? 'yes' : 'no'
        }`
      );
      lines.push(
        `- Reply monitoring owner: ${replyState.replyMonitoringOwner}`
      );

      nextItem = nextReadinessItemAfter('reply_handling', {
        ...context,
        slots: {
          ...slots,
          replyInboxConfirmed: true,
          replyHandlingConfirmed: true,
          replyInbox: replyState.replyInbox,
          replyToMatchesSender: replyState.replyToMatchesSender,
          replyMonitoringOwner: replyState.replyMonitoringOwner,
        },
        confirmedReadiness: {
          ...(context.confirmedReadiness || {}),
          reply_inbox_handling: true,
          reply_monitoring_owner: true,
        },
      });
      if (nextItem) {
        const questions =
          Array.isArray(nextItem.questions) && nextItem.questions.length
            ? nextItem.questions
            : nextItem.ask
              ? [nextItem.ask]
              : [];
        lines.push('');
        lines.push(`Next readiness item: ${nextItem.label}.`);
        if (questions.length) {
          lines.push('');
          for (const q of questions) lines.push(q);
        }
        // Path selection is readiness-only — never execution confirmation.
        lines.push('');
        lines.push(READINESS_SUBSTEP_SAFETY_LINE);
      }
    } else {
      if (multiFieldUpdate || fullReplyBlock) {
        lines.push('Updated reply handling.');
      } else if (replyParsed.replyInbox) {
        lines.push(`Updated reply inbox to ${replyState.replyInbox}.`);
      } else if (replyParsed.hasAny) {
        lines.push('Updated reply-handling details.');
      } else {
        lines.push('Recorded reply-handling details.');
      }
      if (replyState.missing.length) {
        lines.push('');
        lines.push('Still needed for reply handling:');
        for (const m of replyState.missing) lines.push(`- ${m}`);
        lines.push('');
        lines.push(READINESS_SUBSTEP_SAFETY_LINE);
      }
    }

    // Expose reply state on the composed result via closure vars below.
    replyHandlingState = replyState;
  } else if (
    activeId === 'operational_path' ||
    activeId === 'operational_path_selection'
  ) {
    const priorPath = {
      operationalPathId:
        slots.operationalPathId || context.operationalPathId || null,
      operationalPathLabel:
        slots.operationalPathLabel || context.operationalPathLabel || null,
      pathId: slots.operationalPathId || context.operationalPathId || null,
      pathLabel:
        slots.operationalPathLabel || context.operationalPathLabel || null,
    };
    const pathState = mergeOperationalPathState(priorPath, pathParsed);
    operationalPathState = pathState;

    if (pathState.operationalPathChosen) {
      itemConfirmed = true;
      lines.push('Operational path is selected:');
      lines.push(`- ${pathState.operationalPathLabel}`);
      lines.push('');
      lines.push(
        pathState.selectionNote ||
          'This is path selection only. Nothing has been prepared or executed.'
      );

      nextItem = nextReadinessItemAfter('operational_path', {
        ...context,
        slots: {
          ...slots,
          operationalPathChosen: true,
          operationalPathId: pathState.operationalPathId,
          operationalPathLabel: pathState.operationalPathLabel,
        },
        confirmedReadiness: {
          ...(context.confirmedReadiness || {}),
          operational_path: true,
          operational_path_selection: true,
        },
      });
      if (nextItem) {
        const questions =
          Array.isArray(nextItem.questions) && nextItem.questions.length
            ? nextItem.questions
            : nextItem.ask
              ? [nextItem.ask]
              : [];
        lines.push('');
        lines.push(`Next readiness item: ${nextItem.label}.`);
        if (questions.length) {
          lines.push('');
          for (const q of questions) lines.push(q);
        }
        lines.push('');
        lines.push(READINESS_SUBSTEP_SAFETY_LINE);
      }
    } else {
      lines.push('Still needed for operational path selection:');
      lines.push(
        '- Choose manual send export, CRM drafts, queued sends later, or hold with no action'
      );
      lines.push('');
      lines.push(
        'This is path selection only — not execute approval.'
      );
      lines.push('');
      lines.push(READINESS_SUBSTEP_SAFETY_LINE);
    }
  } else if (
    activeId === 'follow_up_tracking' ||
    activeId === 'follow_up_tracking_process'
  ) {
    const priorFollowUp = {
      followUpTrackingLocation:
        slots.followUpTrackingLocation ||
        context.followUpTrackingLocation ||
        null,
      trackingLocation:
        slots.followUpTrackingLocation ||
        context.followUpTrackingLocation ||
        null,
      followUp1Timing: slots.followUp1Timing || context.followUp1Timing || null,
      followUp2Timing: slots.followUp2Timing || context.followUp2Timing || null,
      followUpReviewFirstManual:
        slots.followUpReviewFirstManual != null
          ? slots.followUpReviewFirstManual
          : context.followUpReviewFirstManual != null
            ? context.followUpReviewFirstManual
            : null,
      reviewFirstManual:
        slots.followUpReviewFirstManual != null
          ? slots.followUpReviewFirstManual
          : context.followUpReviewFirstManual != null
            ? context.followUpReviewFirstManual
            : null,
      automaticFollowUpSendsApproved:
        slots.automaticFollowUpSendsApproved != null
          ? slots.automaticFollowUpSendsApproved
          : context.automaticFollowUpSendsApproved != null
            ? context.automaticFollowUpSendsApproved
            : null,
    };
    const followUpState = mergeFollowUpTrackingState(
      priorFollowUp,
      followUpParsed
    );
    followUpTrackingState = followUpState;
    activeId = 'follow_up_tracking';

    if (followUpState.followUpTrackingConfirmed) {
      itemConfirmed = true;
      const reviewFirstLine = followUpState.followUpReviewFirstManual
        ? 'Follow-ups remain review-first/manual unless explicitly enabled later'
        : 'Follow-ups are not held to review-first/manual';
      const autoSendLine = followUpState.automaticFollowUpSendsApproved
        ? 'Automatic follow-up sends are approved'
        : 'No automatic follow-up sends are approved';

      lines.push('Follow-up tracking process is confirmed:');
      lines.push(`- Status tracked in ${followUpState.followUpTrackingLocation}`);
      lines.push(
        `- Follow-up 1 planned for ${followUpState.followUp1Timing}`
      );
      lines.push(
        `- Follow-up 2 planned for ${followUpState.followUp2Timing}`
      );
      lines.push(`- ${reviewFirstLine}`);
      lines.push(`- ${autoSendLine}`);

      nextItem = nextReadinessItemAfter('follow_up_tracking', {
        ...context,
        slots: {
          ...slots,
          followUpTrackingConfirmed: true,
          followUpTrackingLocation: followUpState.followUpTrackingLocation,
          followUp1Timing: followUpState.followUp1Timing,
          followUp2Timing: followUpState.followUp2Timing,
          followUpReviewFirstManual: followUpState.followUpReviewFirstManual,
          automaticFollowUpSendsApproved:
            followUpState.automaticFollowUpSendsApproved,
        },
        confirmedReadiness: {
          ...(context.confirmedReadiness || {}),
          follow_up_tracking_process: true,
          follow_up_tracking: true,
        },
      });
      if (nextItem) {
        const questions =
          Array.isArray(nextItem.questions) && nextItem.questions.length
            ? nextItem.questions
            : nextItem.ask
              ? [nextItem.ask]
              : [];
        lines.push('');
        lines.push(`Next readiness item: ${nextItem.label}.`);
        if (questions.length) {
          lines.push('');
          for (const q of questions) lines.push(q);
        }
        lines.push('');
        lines.push(READINESS_SUBSTEP_SAFETY_LINE);
      }
    } else {
      if (followUpParsed.hasAny) {
        lines.push('Updated follow-up tracking details.');
      } else {
        lines.push('Recorded follow-up tracking details.');
      }
      if (followUpState.missing.length) {
        lines.push('');
        lines.push('Still needed for follow-up tracking process:');
        for (const m of followUpState.missing) lines.push(`- ${m}`);
        lines.push('');
        lines.push(READINESS_SUBSTEP_SAFETY_LINE);
      }
    }
  } else if (isReplyMonitoringBatchReviewItemId(activeId)) {
    const priorMonitoring = {
      replyMonitoringOwner:
        slots.replyMonitoringOwner || context.replyMonitoringOwner || null,
      monitoringOwner:
        slots.replyMonitoringOwner || context.replyMonitoringOwner || null,
      responseReviewProcess:
        slots.responseReviewProcess || context.responseReviewProcess || null,
      positiveReplyHandling:
        slots.positiveReplyHandling || context.positiveReplyHandling || null,
      negativeReplyHandling:
        slots.negativeReplyHandling || context.negativeReplyHandling || null,
      broaderRolloutBlocked:
        slots.broaderRolloutBlocked != null
          ? slots.broaderRolloutBlocked
          : context.broaderRolloutBlocked != null
            ? context.broaderRolloutBlocked
            : null,
      batch1ReviewBeforeExpansion:
        slots.batch1ReviewBeforeExpansion != null
          ? slots.batch1ReviewBeforeExpansion
          : context.batch1ReviewBeforeExpansion != null
            ? context.batch1ReviewBeforeExpansion
            : null,
    };
    const monitoringState = mergeReplyMonitoringBatchReviewState(
      priorMonitoring,
      monitoringParsed
    );
    replyMonitoringBatchReviewState = monitoringState;
    activeId = 'reply_monitoring_batch1';

    if (monitoringState.replyMonitoringBatch1Confirmed) {
      itemConfirmed = true;
      const broaderLine = monitoringState.broaderRolloutBlocked
        ? 'Broader rollout remains blocked until Batch 1 results are reviewed'
        : 'Broader rollout is not blocked pending Batch 1 review';
      const expansionLine = monitoringState.batch1ReviewBeforeExpansion
        ? 'Batch 1 results reviewed before expanding to Cedar, optional Manchester candidates, or any new prospect segment'
        : 'Batch 1 review is not required before expansion';

      lines.push('Reply monitoring / Batch 1 review process is confirmed:');
      lines.push(
        `- Reply monitoring owner: ${monitoringState.replyMonitoringOwner}`
      );
      lines.push(`- ${monitoringState.responseReviewProcess}`);
      lines.push(`- ${monitoringState.positiveReplyHandling}`);
      lines.push(`- ${monitoringState.negativeReplyHandling}`);
      lines.push(`- ${broaderLine}`);
      lines.push(`- ${expansionLine}`);

      nextItem = nextReadinessItemAfter('reply_monitoring_batch1', {
        ...context,
        slots: {
          ...slots,
          replyMonitoringOwner: monitoringState.replyMonitoringOwner,
          responseReviewProcess: monitoringState.responseReviewProcess,
          positiveReplyHandling: monitoringState.positiveReplyHandling,
          negativeReplyHandling: monitoringState.negativeReplyHandling,
          broaderRolloutBlocked: monitoringState.broaderRolloutBlocked,
          batch1ReviewBeforeExpansion:
            monitoringState.batch1ReviewBeforeExpansion,
          replyMonitoringBatch1Confirmed: true,
          batch1ResultsReviewed: monitoringState.batch1ResultsReviewed,
          followUpTrackingConfirmed:
            slots.followUpTrackingConfirmed === true ||
            context.followUpTrackingConfirmed === true,
          operationalPathChosen:
            slots.operationalPathChosen === true ||
            context.operationalPathChosen === true,
          senderIdentityConfirmed:
            slots.senderIdentityConfirmed === true ||
            context.senderIdentityConfirmed === true,
          replyInboxConfirmed:
            slots.replyInboxConfirmed === true ||
            slots.replyHandlingConfirmed === true ||
            context.replyInboxConfirmed === true,
        },
        confirmedReadiness: {
          ...(context.confirmedReadiness || {}),
          reply_monitoring_batch1: true,
          reply_monitoring_batch_review: true,
          reply_monitoring_owner: true,
          broader_rollout_batch1: monitoringState.batch1ResultsReviewed,
        },
      });

      if (nextItem) {
        const questions =
          Array.isArray(nextItem.questions) && nextItem.questions.length
            ? nextItem.questions
            : nextItem.ask
              ? [nextItem.ask]
              : [];
        lines.push('');
        lines.push(`Next readiness item: ${nextItem.label}.`);
        if (questions.length) {
          lines.push('');
          for (const q of questions) lines.push(q);
        }
        lines.push('');
        lines.push(READINESS_SUBSTEP_SAFETY_LINE);
      } else {
        const pathLabel =
          slots.operationalPathLabel ||
          context.operationalPathLabel ||
          'manual send export for operator review';
        lines.push('');
        lines.push('Readiness summary:');
        lines.push('- Sender identity: confirmed');
        lines.push('- Reply inbox / reply-to handling: confirmed');
        lines.push(`- Operational path: ${pathLabel} selected`);
        lines.push('- Follow-up tracking: confirmed');
        lines.push('- Reply monitoring / Batch 1 review: confirmed');
        lines.push('- Execution lock: active');
        lines.push('');
        lines.push(
          'Next step remains explicit execute approval if the operator wants to prepare the manual-send export. Do not execute anything automatically.'
        );
      }
    } else {
      if (monitoringParsed.hasAny) {
        lines.push('Updated reply monitoring / Batch 1 review details.');
      } else {
        lines.push('Recorded reply monitoring / Batch 1 review details.');
      }
      if (monitoringState.missing.length) {
        lines.push('');
        lines.push('Still needed for reply monitoring / Batch 1 review:');
        for (const m of monitoringState.missing) lines.push(`- ${m}`);
        lines.push('');
        lines.push(READINESS_SUBSTEP_SAFETY_LINE);
      }
    }
  } else {
    lines.push('Updated the active readiness fields.');
  }

  const message = lines.join('\n').trim();
  const nextActiveId = itemConfirmed && nextItem ? nextItem.id : activeId;

  return {
    mode: CONVERSATION_MODES.READINESS_FIELD_CORRECTION,
    responseMode: toResponseMode(CONVERSATION_MODES.READINESS_FIELD_CORRECTION),
    message,
    includeRendererSections: false,
    includeExpandedSafety: false,
    readinessItemId: activeId,
    activeReadinessItemId: nextActiveId,
    nextReadinessItem: nextItem,
    senderIdentity: senderState,
    replyHandling: replyHandlingState,
    operationalPath: operationalPathState,
    followUpTracking: followUpTrackingState,
    replyMonitoringBatchReview: replyMonitoringBatchReviewState,
    itemConfirmed,
    correctedFields:
      activeId === 'reply_handling'
        ? replyParsed.updatedFields.slice()
        : activeId === 'operational_path' ||
            activeId === 'operational_path_selection'
          ? pathParsed.updatedFields.slice()
          : activeId === 'follow_up_tracking'
            ? followUpParsed.updatedFields.slice()
            : activeId === 'reply_monitoring_batch1'
              ? monitoringParsed.updatedFields.slice()
              : corrected,
    requiresExplicitApproval: false,
    executionPending: false,
    sendsMade: false,
    crmWritesMade: false,
    exportMade: false,
    accountChangesMade: false,
  };
}

/**
 * True only when the operator clearly asks to take / approve a concrete
 * external or actionable step. Mentions of available next paths alone are not
 * enough — especially under "before choosing", "not yet", questions, etc.
 */
function looksLikeExecutionRequest(text) {
  const s = String(text || '').trim();
  if (!s) return false;

  // Planning / readiness / deferral always wins over path-name mentions.
  if (looksLikeNonExecutionIntent(s) || looksLikeOperatorReadinessCheck(s)) {
    return false;
  }
  // Path selection confirms readiness only — never execute confirmation.
  if (parseOperationalPathSelection(s).hasAny) {
    return false;
  }
  // Follow-up tracking answers confirm readiness only — never execute.
  if (parseFollowUpTrackingFields(s).hasAny) {
    return false;
  }
  // Reply monitoring / Batch 1 review answers confirm readiness only.
  if (parseReplyMonitoringBatchReviewFields(s).hasAny) {
    return false;
  }

  if (
    /\b(?:send|sends|sending)\b[\s\S]{0,40}\b(?:now|batch|emails?|campaign)\b/i.test(
      s
    )
  ) {
    return true;
  }
  if (
    /\b(?:prepare|create|generate|build|make)\s+(?:(?:a|an|the)\s+)?(?:manual-?send\s+)?export\b/i.test(
      s
    )
  ) {
    return true;
  }
  if (
    /\b(?:create|prepare|generate)\s+(?:(?:the|some)\s+)?crm\s+drafts?\b/i.test(
      s
    )
  ) {
    return true;
  }
  if (
    /\b(?:queue|schedule)\s+(?:(?:the|those|a)\s+)?(?:sends?|launch)\b/i.test(
      s
    )
  ) {
    return true;
  }
  if (
    /\b(?:approve|execute)\s+(?:(?:the|this|a)\s+)?(?:send|export|crm|execute|manual-?send|launch)\b/i.test(
      s
    )
  ) {
    return true;
  }
  if (
    /\bgo\s+ahead\s+and\s+(?:create|prepare|export|queue|send|execute)\b/i.test(
      s
    )
  ) {
    return true;
  }
  if (
    /\byes[,.]?\s+(?:execute|approve|prepare|create|queue|export|send)\b/i.test(
      s
    )
  ) {
    return true;
  }
  if (/\bwrite\s+to\s+crm\b|\bcrm\s+write\b/i.test(s)) {
    return true;
  }
  return false;
}

/**
 * Select conversation mode AFTER workflow/state decisions.
 */
function selectConversationMode(input = {}) {
  const assessment =
    input.assessment || assessConversationContext(input);

  if (assessment.isDiagnostic) {
    return CONVERSATION_MODES.OPERATOR_DIAGNOSTIC;
  }
  // Accidental / low-signal beats state summary, readiness, and execution.
  if (assessment.isClarificationNeeded) {
    return CONVERSATION_MODES.CLARIFICATION_NEEDED;
  }
  // Final campaign-ready summary beats substep / field correction collapse.
  if (assessment.isCampaignReadySummary) {
    return CONVERSATION_MODES.CAMPAIGN_READY_SUMMARY;
  }
  // Field correction / substep update beats Launch Gate operational options.
  if (assessment.isReadinessFieldCorrection) {
    return CONVERSATION_MODES.READINESS_FIELD_CORRECTION;
  }
  // Selected readiness item beats checklist / "which item first?" ask.
  if (assessment.isReadinessSubstep) {
    return CONVERSATION_MODES.READINESS_SUBSTEP;
  }
  // Readiness / planning before execution — never ask to approve execute
  // when the operator is still resolving gaps or discussing options.
  if (assessment.isReadinessCheck) {
    return CONVERSATION_MODES.OPERATOR_READINESS_CHECK;
  }
  if (assessment.isExecutionRequest) {
    return CONVERSATION_MODES.EXECUTION_CONFIRMATION;
  }
  if (assessment.isRevision) {
    return CONVERSATION_MODES.OPERATOR_REVISION_RESPONSE;
  }
  if (assessment.stateChanged || assessment.gateAlreadyApproved) {
    // Already-approved gates must never fall into formal review.
    return CONVERSATION_MODES.OPERATOR_STATE_UPDATE;
  }
  if (assessment.isFirstTimeReview && !assessment.gateAlreadyApproved) {
    return CONVERSATION_MODES.FORMAL_REVIEW_GATE;
  }

  // Prefer explicit mapping from legacy selectResponseMode when provided.
  if (input.responseMode) {
    const mapped = toConversationMode(input.responseMode);
    if (mapped) return mapped;
  }

  return CONVERSATION_MODES.OPERATOR_STATE_UPDATE;
}

/**
 * Compose the operator-facing message for the selected mode.
 * Pass `body` / `artifactMessage` when workflow already produced content
 * that should be kept (revision artifact, formal gate card).
 */
function composeConversationResponse(mode, context = {}) {
  const resolved =
    mode ||
    selectConversationMode(context) ||
    CONVERSATION_MODES.OPERATOR_STATE_UPDATE;

  switch (resolved) {
    case CONVERSATION_MODES.OPERATOR_STATE_UPDATE:
      return composeOperatorStateUpdate(context);
    case CONVERSATION_MODES.OPERATOR_REVISION_RESPONSE:
      return composeOperatorRevisionResponse(context);
    case CONVERSATION_MODES.OPERATOR_DIAGNOSTIC:
      return composeOperatorDiagnostic(context);
    case CONVERSATION_MODES.FORMAL_REVIEW_GATE:
      return composeFormalReviewGate(context);
    case CONVERSATION_MODES.CAMPAIGN_READY_SUMMARY:
      return composeCampaignReadySummary(context);
    case CONVERSATION_MODES.OPERATOR_READINESS_CHECK:
      return composeOperatorReadinessCheck(context);
    case CONVERSATION_MODES.READINESS_SUBSTEP:
      return composeReadinessSubstep(context);
    case CONVERSATION_MODES.READINESS_FIELD_CORRECTION:
    case CONVERSATION_MODES.READINESS_SUBSTEP_UPDATE:
      return composeReadinessFieldCorrection(context);
    case CONVERSATION_MODES.EXECUTION_CONFIRMATION:
      return composeExecutionConfirmation(context);
    case CONVERSATION_MODES.CLARIFICATION_NEEDED:
      return composeClarificationNeeded(context);
    default:
      return composeOperatorStateUpdate(context);
  }
}

function gateDisplayName(context = {}) {
  return (
    context.gateName ||
    context.title ||
    (context.gate && (context.gate.title || context.gate.kind)) ||
    'Launch Gate'
  );
}

function composeOperatorStateUpdate(context = {}) {
  // Prefer the single canonical Launch Gate approved-state formatter when
  // the gate is already approved / just approved — avoids stacked headers.
  if (
    context.gate ||
    context.justApproved ||
    context.gateAlreadyApproved ||
    /launch\s*gate/i.test(String(context.gateName || context.title || ''))
  ) {
    return formatApprovedLaunchGateConversational(context.gate || {}, {
      justApproved: context.justApproved === true,
      gateAlreadyApproved: context.gateAlreadyApproved === true,
      nextPaths: context.nextPaths,
      operatorGuidance: context.operatorGuidance,
      closingAsk: context.closingAsk || context.currentAsk,
      // Never stack a leadIn that restates approval / safety.
      leadIn: null,
    });
  }

  const gate = gateDisplayName(context);
  const lines = [
    `Here's where we are with ${gate}.`,
    '',
    compactSafetyLockLine({ short: context.safetyLine || context.compactSafety }),
  ];
  if (context.body || context.artifactMessage) {
    lines.push('');
    lines.push(String(context.body || context.artifactMessage).trim());
  }
  if (context.closingAsk) {
    lines.push('');
    lines.push(context.closingAsk);
  }
  return {
    mode: CONVERSATION_MODES.OPERATOR_STATE_UPDATE,
    responseMode: toResponseMode(CONVERSATION_MODES.OPERATOR_STATE_UPDATE),
    message: dedupeOperatorStateUpdateMessage(lines.join('\n').trim()),
    includeRendererSections: false,
    includeExpandedSafety: false,
  };
}

function composeOperatorRevisionResponse(context = {}) {
  const changes = Array.isArray(context.changes) ? context.changes : [];
  const lines = [];

  lines.push(
    context.acknowledgment ||
      'Got it — I updated the active artifact from your instructions.'
  );

  if (context.whatWasWrong) {
    lines.push('');
    lines.push(`What was off: ${context.whatWasWrong}`);
  }

  if (changes.length) {
    lines.push('');
    lines.push('What changed:');
    for (const c of changes) lines.push(`- ${c}`);
  }

  if (context.artifactMessage) {
    lines.push('');
    lines.push(String(context.artifactMessage).trim());
  } else if (context.body) {
    lines.push('');
    lines.push(String(context.body).trim());
  }

  lines.push('');
  lines.push(
    context.safetyLine || compactSafetyLockLine({ short: context.compactSafety })
  );

  if (context.closingQuestion) {
    lines.push('');
    lines.push(context.closingQuestion);
  }

  const message = lines.join('\n').trim();
  return {
    mode: CONVERSATION_MODES.OPERATOR_REVISION_RESPONSE,
    responseMode: toResponseMode(
      CONVERSATION_MODES.OPERATOR_REVISION_RESPONSE
    ),
    message,
    includeRendererSections: false,
    includeExpandedSafety: false,
    // Fail closed if a renderer somehow leaked into revision mode.
    containsForbiddenRendererCopy: containsRendererBoilerplate(message),
  };
}

function composeOperatorDiagnostic(context = {}) {
  const plain =
    context.plainLanguage ||
    context.headline ||
    'I found a mismatch between campaign memory and the stored draft renderer.';
  const detail = context.technicalDetail || context.detail || null;
  const stopLine =
    context.stopLine ||
    "I'm stopping before showing another stale draft.";

  const lines = [plain, '', stopLine];

  if (detail) {
    lines.push('');
    lines.push('Technical detail:');
    lines.push(String(detail).trim());
  }

  if (context.nextAction) {
    lines.push('');
    lines.push(String(context.nextAction).trim());
  }

  return {
    mode: CONVERSATION_MODES.OPERATOR_DIAGNOSTIC,
    responseMode: toResponseMode(CONVERSATION_MODES.OPERATOR_DIAGNOSTIC),
    message: lines.join('\n').trim(),
    includeRendererSections: false,
    includeExpandedSafety: false,
  };
}

function composeFormalReviewGate(context = {}) {
  // Formal gates may keep the workflow digest card when provided.
  const body = context.body || context.artifactMessage || context.message || '';
  const gate = gateDisplayName(context);
  const lang = approvalLanguageForGate({
    gateName: gate,
    approved: false,
  });

  const lines = [];
  if (context.leadIn) {
    lines.push(String(context.leadIn).trim());
    lines.push('');
  }
  if (body) {
    lines.push(String(body).trim());
  } else {
    lines.push(`${gate} is ready for first-time review.`);
    lines.push('');
    lines.push(expandedSafetyBlock());
  }

  if (context.closingQuestion) {
    if (!String(body).includes(String(context.closingQuestion))) {
      lines.push('');
      lines.push(context.closingQuestion);
    }
  } else if (!containsRendererBoilerplate(body)) {
    lines.push('');
    lines.push(lang.ask);
  }

  return {
    mode: CONVERSATION_MODES.FORMAL_REVIEW_GATE,
    responseMode: toResponseMode(CONVERSATION_MODES.FORMAL_REVIEW_GATE),
    message: lines.join('\n').trim(),
    includeRendererSections: true,
    includeExpandedSafety: true,
  };
}

/**
 * Extract operator-specified readiness checklist items from free text.
 * Prefers bullet / numbered list lines; never invents items.
 * Sender field VALUE lines ("Sender name: Jacob") are not gap items.
 */
function extractOperatorReadinessChecklist(text) {
  const s = String(text || '');
  if (!s.trim()) return [];

  const items = [];
  const seen = new Set();

  const pushItem = (raw) => {
    let item = String(raw || '')
      .replace(/^[\s•*\-–—]+/, '')
      .replace(/^\d+[.)]\s*/, '')
      .replace(/\s+/g, ' ')
      .replace(/[.;,\s]+$/g, '')
      .trim();
    if (!item || item.length < 8) return;
    // Skip instructional / selection lines that are not checklist items.
    if (
      /^(?:before\s+choosing|please\s+summarize|help\s+me|still\s+unresolved|which\s+readiness|nothing\s+external|resolve\s+|i\s+already\s+selected|do\s+not\s+repeat|select(?:ed)?\s+operational\s+path|this\s+is\s+path\s+selection)\b/i.test(
        item
      )
    ) {
      return;
    }
    // Confirmed/provided field values are not unresolved readiness gaps.
    if (isSenderFieldValueLine(item)) return;
    if (isReplyFieldValueLine(item)) return;
    if (isOperationalPathValueLine(item)) return;
    const key = item.toLowerCase();
    if (seen.has(key)) return;
    seen.add(key);
    items.push(item);
  };

  // Bullet / numbered lines anywhere in the message.
  for (const line of s.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    if (/^(?:[-*•–—]|\d+[.)])\s+\S/.test(trimmed)) {
      pushItem(trimmed);
    }
  }

  if (items.length) return items;

  // Fallback: "include: a; b; c" / "checklist: a, b, and c"
  // Do not treat "readiness item: sender identity" selection language as a list.
  if (
    /\b(?:already\s+selected|i\s+(?:already\s+)?(?:selected|chose|picked)|resolve\s+)/i.test(
      s
    ) &&
    detectSelectedReadinessItem(s)
  ) {
    return [];
  }

  const includeMatch = s.match(
    /(?:include|checklist|unresolved(?:\s+items?)?|readiness(?:\s+items?)?)\s*[:\-]\s*([\s\S]+)$/i
  );
  if (includeMatch) {
    const chunk = includeMatch[1]
      .split(/\n|(?:;|\s+and\s+|,\s*(?=[a-z]))/i)
      .map((part) => part.trim())
      .filter(Boolean);
    for (const part of chunk) pushItem(part);
  }

  return items;
}

/**
 * True when a checklist line is a provided sender field value, not a gap.
 * e.g. "Sender name: Jacob Maynard"
 */
function isSenderFieldValueLine(text) {
  return /^(?:sender\s+)?(?:name|email(?:\s+address)?|signature)\s*[:\-–—]\s*\S+/i.test(
    String(text || '').trim()
  );
}

/**
 * True when a checklist line is a provided reply-handling field value.
 * e.g. "Reply inbox: …", "Reply-to should match the sender address: yes"
 */
function isReplyFieldValueLine(text) {
  const t = String(text || '').trim();
  if (
    /^reply\s+inbox\s*[:\-–—]\s*\S+/i.test(t) ||
    /^reply(?:\s*[-/]?\s*to)?\s+should\s+match\b[\s\S]*[:\-–—]\s*\S+/i.test(t) ||
    /^(?:reply\s+)?monitoring\s+owner(?:\s*\/\s*process)?\s*[:\-–—]\s*\S+/i.test(
      t
    ) ||
    /^reply-?to\s+matches\s+sender(?:\s+address)?\s*[:\-–—]\s*\S+/i.test(t)
  ) {
    return true;
  }
  return false;
}

/**
 * True when a checklist line is a selected operational path value, not a gap.
 * e.g. "manual send export for operator review"
 */
function isOperationalPathValueLine(text) {
  const t = String(text || '').trim();
  if (!t) return false;
  if (/^operational\s+path(?:\s+selection)?\s*[:\-–—]\s*\S+/i.test(t)) {
    return true;
  }
  if (
    /^operational\s+path(?:\s+selection)?\s+is\s+(?:selected|chosen)\b/i.test(t)
  ) {
    return true;
  }
  // Bare known path labels (often extracted from a selection message).
  for (const choice of Object.values(OPERATIONAL_PATH_CHOICES)) {
    if (
      new RegExp(
        `^${choice.label.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`,
        'i'
      ).test(t)
    ) {
      return true;
    }
    if (choice.match.test(t) && t.length <= choice.label.length + 24) {
      return true;
    }
  }
  return false;
}

/**
 * True when a checklist line is a provided follow-up tracking field value.
 */
function isFollowUpTrackingValueLine(text) {
  const t = String(text || '').trim();
  if (!t) return false;
  if (/^status\s+tracked\s+in\b/i.test(t)) return true;
  if (/^follow[- ]?up\s*[12]\s+planned\s+for\b/i.test(t)) return true;
  if (/^follow[- ]?ups?\s+remain\s+review[- ]?first\b/i.test(t)) return true;
  if (/^no\s+automatic\s+follow[- ]?up\s+sends?\b/i.test(t)) return true;
  if (
    /^(?:follow[- ]?up\s+)?(?:tracking\s+location|status)\s*[:\-–—]\s*\S+/i.test(
      t
    )
  ) {
    return true;
  }
  if (
    /^follow[- ]?up\s*[12]\s*(?:timing|planned)?\s*[:\-–—]\s*\S+/i.test(t)
  ) {
    return true;
  }
  return false;
}

/**
 * True when a checklist line is a provided reply-monitoring / Batch 1 field.
 * Does NOT match unresolved gap phrasing such as
 * "broader rollout remains blocked until Batch 1 results are reviewed"
 * or "reply monitoring owner/process is not confirmed".
 */
function isReplyMonitoringBatchReviewValueLine(text) {
  const t = String(text || '').trim();
  if (!t) return false;
  if (/\bis\s+not\s+confirmed\b/i.test(t)) return false;
  if (/^reply\s+monitoring\s+owner\s*[:\-–—]\s*\S+/i.test(t)) return true;
  if (/^responses?\s+reviewed\s+manually\b/i.test(t)) return true;
  if (/^positive\s+replies?\s+handled\b/i.test(t)) return true;
  if (/^(?:negative|not[- ]?now)\s+replies?\s+respected\b/i.test(t)) {
    return true;
  }
  if (/^Batch\s*1\s+results?\s+reviewed\s+before\s+expanding\b/i.test(t)) {
    return true;
  }
  if (
    /^(?:response(?:s)?\s+review(?:\s+process)?|positive\s+replies?(?:\s+handling)?|(?:negative(?:\s+or\s+not[- ]?now)?|not[- ]?now)\s+replies?(?:\s+handling)?)\s*[:\-–—]\s*\S+/i.test(
      t
    )
  ) {
    return true;
  }
  return false;
}

/**
 * True when a line is an open readiness question (still unresolved).
 */
function isOpenReadinessQuestionLine(text) {
  const t = String(text || '').trim();
  if (!t) return false;
  if (
    isSenderFieldValueLine(t) ||
    isReplyFieldValueLine(t) ||
    isOperationalPathValueLine(t) ||
    isFollowUpTrackingValueLine(t) ||
    isReplyMonitoringBatchReviewValueLine(t)
  ) {
    return false;
  }
  if (/\?$/.test(t)) return true;
  return /^(?:what|who|should|where|how|do\s+you\s+want)\b/i.test(t);
}

function readinessConceptId(item) {
  const s = String(item || '');
  if (isSenderFieldValueLine(s) || /\bsender\s+identity\b/i.test(s)) {
    return 'sender_identity';
  }
  if (isReplyFieldValueLine(s)) {
    if (/monitoring\s+owner/i.test(s)) return 'reply_monitoring_owner';
    return 'reply_inbox_handling';
  }
  if (isOperationalPathValueLine(s)) {
    return 'operational_path';
  }
  if (isFollowUpTrackingValueLine(s)) {
    return 'follow_up_tracking_process';
  }
  if (isReplyMonitoringBatchReviewValueLine(s)) {
    if (/broader\s+rollout|Batch\s*1/i.test(s)) return 'broader_rollout_batch1';
    return 'reply_monitoring_owner';
  }
  for (const concept of READINESS_CONCEPT_PATTERNS) {
    if (concept.match.test(s)) return concept.id;
  }
  // Open reply questions belong to reply handling, not custom unresolved noise.
  if (
    isOpenReadinessQuestionLine(s) &&
    /\b(?:inbox|reply-?to|monitor(?:ing)?\s+replies|replies)\b/i.test(s)
  ) {
    return 'reply_inbox_handling';
  }
  if (
    isOpenReadinessQuestionLine(s) &&
    /\b(?:manual\s+send|crm\s+drafts|queued\s+sends|hold\s+with\s+no\s+action|operational\s+path)\b/i.test(
      s
    )
  ) {
    return 'operational_path';
  }
  if (
    isOpenReadinessQuestionLine(s) &&
    /\bfollow[- ]?up\b/i.test(s)
  ) {
    return 'follow_up_tracking_process';
  }
  if (
    isOpenReadinessQuestionLine(s) &&
    /\b(?:monitor(?:ing)?\s+replies|broader\s+rollout|Batch\s*1\s+results?)\b/i.test(
      s
    )
  ) {
    return 'reply_monitoring_owner';
  }
  return `custom:${s.toLowerCase().slice(0, 80)}`;
}

/**
 * Confirmed readiness field records (durable across substeps / summary asks).
 * Prefer these over the latest prompt fragment alone.
 */
function getConfirmedReadinessRecords(context = {}) {
  const slots = context.slots || {};
  const gate = context.gate || context.outreachLaunchGate || {};
  const summary = gate.operatorStateSummary || {};
  const memory =
    (context.campaignMemory && context.campaignMemory.operatorLearnings) ||
    context.operatorLearnings ||
    {};
  return {
    ...(memory.confirmed_readiness_records || {}),
    ...(summary.confirmedReadinessRecords || {}),
    ...(context.confirmedReadinessRecords || {}),
    ...(slots.confirmedReadinessRecords || {}),
  };
}

function senderRecordFromConfirmed(records = {}) {
  return (
    records.sender_identity ||
    records.senderIdentity ||
    records.sender_identity_fields ||
    null
  );
}

function replyRecordFromConfirmed(records = {}) {
  return (
    records.reply_handling ||
    records.reply_inbox_handling ||
    records.replyHandling ||
    records.replyInboxHandling ||
    null
  );
}

function operationalPathRecordFromConfirmed(records = {}) {
  return (
    records.operational_path ||
    records.operational_path_selection ||
    records.operationalPath ||
    null
  );
}

function followUpRecordFromConfirmed(records = {}) {
  return (
    records.follow_up_tracking ||
    records.follow_up_tracking_process ||
    records.followUpTracking ||
    null
  );
}

function monitoringRecordFromConfirmed(records = {}) {
  return (
    records.reply_monitoring_batch1 ||
    records.reply_monitoring_batch_review ||
    records.replyMonitoringBatch1 ||
    records.replyMonitoringBatchReview ||
    null
  );
}

/**
 * Build / merge structured confirmed readiness records from known field values.
 * Used when an item is confirmed so later summary asks can retrieve them.
 */
function buildConfirmedReadinessRecords(context = {}, patch = {}) {
  const prior = getConfirmedReadinessRecords(context);
  const slots = context.slots || {};
  const next = { ...prior };

  const senderName =
    patch.senderName ||
    slots.senderName ||
    context.senderName ||
    (prior.sender_identity && prior.sender_identity.senderName) ||
    null;
  const senderEmail =
    patch.senderEmail ||
    slots.senderEmail ||
    context.senderEmail ||
    (prior.sender_identity && prior.sender_identity.senderEmail) ||
    null;
  const senderSignature =
    patch.senderSignature ||
    slots.senderSignature ||
    context.senderSignature ||
    (prior.sender_identity && prior.sender_identity.senderSignature) ||
    null;
  if (
    patch.senderIdentityConfirmed === true ||
    slots.senderIdentityConfirmed === true ||
    context.senderIdentityConfirmed === true ||
    (senderName && senderEmail && senderSignature)
  ) {
    if (senderName && senderEmail && senderSignature) {
      next.sender_identity = {
        confirmed: true,
        senderName,
        senderEmail,
        senderSignature,
      };
    }
  }

  const replyInbox =
    patch.replyInbox ||
    slots.replyInbox ||
    context.replyInbox ||
    (prior.reply_handling && prior.reply_handling.replyInbox) ||
    null;
  const replyToMatchesSender =
    patch.replyToMatchesSender != null
      ? patch.replyToMatchesSender
      : slots.replyToMatchesSender != null
        ? slots.replyToMatchesSender
        : context.replyToMatchesSender != null
          ? context.replyToMatchesSender
          : prior.reply_handling &&
              prior.reply_handling.replyToMatchesSender != null
            ? prior.reply_handling.replyToMatchesSender
            : null;
  const replyMonitoringOwner =
    patch.replyMonitoringOwner ||
    slots.replyMonitoringOwner ||
    context.replyMonitoringOwner ||
    (prior.reply_handling && prior.reply_handling.replyMonitoringOwner) ||
    null;
  if (
    patch.replyInboxConfirmed === true ||
    patch.replyHandlingConfirmed === true ||
    slots.replyInboxConfirmed === true ||
    slots.replyHandlingConfirmed === true ||
    (replyInbox && replyToMatchesSender !== null && replyMonitoringOwner)
  ) {
    if (replyInbox && replyToMatchesSender !== null && replyMonitoringOwner) {
      next.reply_handling = {
        confirmed: true,
        replyInbox,
        replyToMatchesSender,
        replyMonitoringOwner,
      };
      next.reply_inbox_handling = next.reply_handling;
    }
  }

  const operationalPathId =
    patch.operationalPathId ||
    slots.operationalPathId ||
    context.operationalPathId ||
    (prior.operational_path && prior.operational_path.operationalPathId) ||
    null;
  const operationalPathLabel =
    patch.operationalPathLabel ||
    slots.operationalPathLabel ||
    context.operationalPathLabel ||
    (prior.operational_path && prior.operational_path.operationalPathLabel) ||
    null;
  if (
    patch.operationalPathChosen === true ||
    slots.operationalPathChosen === true ||
    operationalPathLabel
  ) {
    if (operationalPathLabel) {
      next.operational_path = {
        confirmed: true,
        operationalPathId,
        operationalPathLabel,
      };
    }
  }

  const followUpTrackingLocation =
    patch.followUpTrackingLocation ||
    slots.followUpTrackingLocation ||
    context.followUpTrackingLocation ||
    (prior.follow_up_tracking &&
      prior.follow_up_tracking.followUpTrackingLocation) ||
    null;
  const followUp1Timing =
    patch.followUp1Timing ||
    slots.followUp1Timing ||
    context.followUp1Timing ||
    (prior.follow_up_tracking && prior.follow_up_tracking.followUp1Timing) ||
    null;
  const followUp2Timing =
    patch.followUp2Timing ||
    slots.followUp2Timing ||
    context.followUp2Timing ||
    (prior.follow_up_tracking && prior.follow_up_tracking.followUp2Timing) ||
    null;
  const followUpReviewFirstManual =
    patch.followUpReviewFirstManual != null
      ? patch.followUpReviewFirstManual
      : slots.followUpReviewFirstManual != null
        ? slots.followUpReviewFirstManual
        : context.followUpReviewFirstManual != null
          ? context.followUpReviewFirstManual
          : prior.follow_up_tracking &&
              prior.follow_up_tracking.followUpReviewFirstManual != null
            ? prior.follow_up_tracking.followUpReviewFirstManual
            : null;
  const automaticFollowUpSendsApproved =
    patch.automaticFollowUpSendsApproved != null
      ? patch.automaticFollowUpSendsApproved
      : slots.automaticFollowUpSendsApproved != null
        ? slots.automaticFollowUpSendsApproved
        : context.automaticFollowUpSendsApproved != null
          ? context.automaticFollowUpSendsApproved
          : prior.follow_up_tracking &&
              prior.follow_up_tracking.automaticFollowUpSendsApproved != null
            ? prior.follow_up_tracking.automaticFollowUpSendsApproved
            : null;
  if (
    patch.followUpTrackingConfirmed === true ||
    slots.followUpTrackingConfirmed === true ||
    followUpTrackingLocation
  ) {
    if (followUpTrackingLocation) {
      next.follow_up_tracking = {
        confirmed: true,
        followUpTrackingLocation,
        followUp1Timing,
        followUp2Timing,
        followUpReviewFirstManual,
        automaticFollowUpSendsApproved,
      };
      next.follow_up_tracking_process = next.follow_up_tracking;
    }
  }

  const responseReviewProcess =
    patch.responseReviewProcess ||
    slots.responseReviewProcess ||
    context.responseReviewProcess ||
    (prior.reply_monitoring_batch1 &&
      prior.reply_monitoring_batch1.responseReviewProcess) ||
    null;
  const positiveReplyHandling =
    patch.positiveReplyHandling ||
    slots.positiveReplyHandling ||
    context.positiveReplyHandling ||
    (prior.reply_monitoring_batch1 &&
      prior.reply_monitoring_batch1.positiveReplyHandling) ||
    null;
  const negativeReplyHandling =
    patch.negativeReplyHandling ||
    slots.negativeReplyHandling ||
    context.negativeReplyHandling ||
    (prior.reply_monitoring_batch1 &&
      prior.reply_monitoring_batch1.negativeReplyHandling) ||
    null;
  const broaderRolloutBlocked =
    patch.broaderRolloutBlocked != null
      ? patch.broaderRolloutBlocked
      : slots.broaderRolloutBlocked != null
        ? slots.broaderRolloutBlocked
        : context.broaderRolloutBlocked != null
          ? context.broaderRolloutBlocked
          : prior.reply_monitoring_batch1 &&
              prior.reply_monitoring_batch1.broaderRolloutBlocked != null
            ? prior.reply_monitoring_batch1.broaderRolloutBlocked
            : null;
  const batch1ReviewBeforeExpansion =
    patch.batch1ReviewBeforeExpansion != null
      ? patch.batch1ReviewBeforeExpansion
      : slots.batch1ReviewBeforeExpansion != null
        ? slots.batch1ReviewBeforeExpansion
        : context.batch1ReviewBeforeExpansion != null
          ? context.batch1ReviewBeforeExpansion
          : prior.reply_monitoring_batch1 &&
              prior.reply_monitoring_batch1.batch1ReviewBeforeExpansion != null
            ? prior.reply_monitoring_batch1.batch1ReviewBeforeExpansion
            : null;
  const monitoringOwner =
    patch.replyMonitoringOwner ||
    slots.replyMonitoringOwner ||
    context.replyMonitoringOwner ||
    replyMonitoringOwner ||
    (prior.reply_monitoring_batch1 &&
      prior.reply_monitoring_batch1.replyMonitoringOwner) ||
    null;
  if (
    patch.replyMonitoringBatch1Confirmed === true ||
    slots.replyMonitoringBatch1Confirmed === true ||
    responseReviewProcess ||
    broaderRolloutBlocked === true
  ) {
    next.reply_monitoring_batch1 = {
      confirmed: true,
      replyMonitoringOwner: monitoringOwner,
      responseReviewProcess,
      positiveReplyHandling,
      negativeReplyHandling,
      broaderRolloutBlocked,
      batch1ReviewBeforeExpansion,
    };
    next.reply_monitoring_batch_review = next.reply_monitoring_batch1;
  }

  return { ...next, ...patch.records };
}

/**
 * Resolve sender identity fields from confirmed records + slots + latest message.
 */
function resolveSenderIdentityFromContext(context = {}) {
  const slots = context.slots || {};
  const record = senderRecordFromConfirmed(getConfirmedReadinessRecords(context)) || {};
  const prior = {
    senderName:
      slots.senderName ||
      context.senderName ||
      record.senderName ||
      record.name ||
      (context.priorFields && context.priorFields.senderName) ||
      null,
    senderEmail:
      slots.senderEmail ||
      context.senderEmail ||
      record.senderEmail ||
      record.email ||
      (context.priorFields && context.priorFields.senderEmail) ||
      null,
    senderSignature:
      slots.senderSignature ||
      context.senderSignature ||
      record.senderSignature ||
      record.signature ||
      (context.priorFields && context.priorFields.senderSignature) ||
      null,
  };
  const parsed = parseSenderIdentityFields(
    context.operatorMessage || context.text || context.userMessage || ''
  );
  return mergeSenderIdentityState(prior, parsed);
}

/**
 * Resolve reply-handling fields from confirmed records + slots + latest message.
 */
function resolveReplyHandlingFromContext(context = {}) {
  const slots = context.slots || {};
  const record = replyRecordFromConfirmed(getConfirmedReadinessRecords(context)) || {};
  const prior = {
    replyInbox:
      slots.replyInbox || context.replyInbox || record.replyInbox || null,
    replyToMatchesSender:
      slots.replyToMatchesSender != null
        ? slots.replyToMatchesSender
        : context.replyToMatchesSender != null
          ? context.replyToMatchesSender
          : record.replyToMatchesSender != null
            ? record.replyToMatchesSender
            : null,
    replyMonitoringOwner:
      slots.replyMonitoringOwner ||
      context.replyMonitoringOwner ||
      record.replyMonitoringOwner ||
      null,
  };
  const parsed = parseReplyHandlingFields(
    context.operatorMessage || context.text || context.userMessage || ''
  );
  return mergeReplyHandlingState(prior, parsed);
}

/**
 * Resolve operational path selection from confirmed records + slots + message.
 */
function resolveOperationalPathFromContext(context = {}) {
  const slots = context.slots || {};
  const record =
    operationalPathRecordFromConfirmed(getConfirmedReadinessRecords(context)) ||
    {};
  const prior = {
    operationalPathId:
      slots.operationalPathId ||
      context.operationalPathId ||
      record.operationalPathId ||
      null,
    operationalPathLabel:
      slots.operationalPathLabel ||
      context.operationalPathLabel ||
      record.operationalPathLabel ||
      null,
    pathId:
      slots.operationalPathId ||
      context.operationalPathId ||
      record.operationalPathId ||
      null,
    pathLabel:
      slots.operationalPathLabel ||
      context.operationalPathLabel ||
      record.operationalPathLabel ||
      null,
  };
  const activeId =
    normalizeReadinessItemId(
      context.activeReadinessItemId || slots.activeReadinessItemId || null
    ) || null;
  const parsed = parseOperationalPathSelection(
    context.operatorMessage || context.text || context.userMessage || '',
    {
      allowBarePath: activeId === 'operational_path',
    }
  );
  return mergeOperationalPathState(prior, parsed);
}

/**
 * Resolve follow-up tracking fields from confirmed records + slots + message.
 */
function resolveFollowUpTrackingFromContext(context = {}) {
  const slots = context.slots || {};
  const record =
    followUpRecordFromConfirmed(getConfirmedReadinessRecords(context)) || {};
  const prior = {
    followUpTrackingLocation:
      slots.followUpTrackingLocation ||
      context.followUpTrackingLocation ||
      record.followUpTrackingLocation ||
      null,
    trackingLocation:
      slots.followUpTrackingLocation ||
      context.followUpTrackingLocation ||
      record.followUpTrackingLocation ||
      null,
    followUp1Timing:
      slots.followUp1Timing ||
      context.followUp1Timing ||
      record.followUp1Timing ||
      null,
    followUp2Timing:
      slots.followUp2Timing ||
      context.followUp2Timing ||
      record.followUp2Timing ||
      null,
    followUpReviewFirstManual:
      slots.followUpReviewFirstManual != null
        ? slots.followUpReviewFirstManual
        : context.followUpReviewFirstManual != null
          ? context.followUpReviewFirstManual
          : record.followUpReviewFirstManual != null
            ? record.followUpReviewFirstManual
            : null,
    reviewFirstManual:
      slots.followUpReviewFirstManual != null
        ? slots.followUpReviewFirstManual
        : context.followUpReviewFirstManual != null
          ? context.followUpReviewFirstManual
          : record.followUpReviewFirstManual != null
            ? record.followUpReviewFirstManual
            : null,
    automaticFollowUpSendsApproved:
      slots.automaticFollowUpSendsApproved != null
        ? slots.automaticFollowUpSendsApproved
        : context.automaticFollowUpSendsApproved != null
          ? context.automaticFollowUpSendsApproved
          : record.automaticFollowUpSendsApproved != null
            ? record.automaticFollowUpSendsApproved
            : null,
  };
  const parsed = parseFollowUpTrackingFields(
    context.operatorMessage || context.text || context.userMessage || ''
  );
  return mergeFollowUpTrackingState(prior, parsed);
}

/**
 * Resolve reply monitoring / Batch 1 review from confirmed records + slots.
 */
function resolveReplyMonitoringBatchReviewFromContext(context = {}) {
  const slots = context.slots || {};
  const record =
    monitoringRecordFromConfirmed(getConfirmedReadinessRecords(context)) || {};
  const replyRecord =
    replyRecordFromConfirmed(getConfirmedReadinessRecords(context)) || {};
  const prior = {
    replyMonitoringOwner:
      slots.replyMonitoringOwner ||
      context.replyMonitoringOwner ||
      record.replyMonitoringOwner ||
      replyRecord.replyMonitoringOwner ||
      null,
    monitoringOwner:
      slots.replyMonitoringOwner ||
      context.replyMonitoringOwner ||
      record.replyMonitoringOwner ||
      replyRecord.replyMonitoringOwner ||
      null,
    responseReviewProcess:
      slots.responseReviewProcess ||
      context.responseReviewProcess ||
      record.responseReviewProcess ||
      null,
    positiveReplyHandling:
      slots.positiveReplyHandling ||
      context.positiveReplyHandling ||
      record.positiveReplyHandling ||
      null,
    negativeReplyHandling:
      slots.negativeReplyHandling ||
      context.negativeReplyHandling ||
      record.negativeReplyHandling ||
      null,
    broaderRolloutBlocked:
      slots.broaderRolloutBlocked != null
        ? slots.broaderRolloutBlocked
        : context.broaderRolloutBlocked != null
          ? context.broaderRolloutBlocked
          : record.broaderRolloutBlocked != null
            ? record.broaderRolloutBlocked
            : null,
    batch1ReviewBeforeExpansion:
      slots.batch1ReviewBeforeExpansion != null
        ? slots.batch1ReviewBeforeExpansion
        : context.batch1ReviewBeforeExpansion != null
          ? context.batch1ReviewBeforeExpansion
          : record.batch1ReviewBeforeExpansion != null
            ? record.batch1ReviewBeforeExpansion
            : null,
  };
  const parsed = parseReplyMonitoringBatchReviewFields(
    context.operatorMessage || context.text || context.userMessage || ''
  );
  return mergeReplyMonitoringBatchReviewState(prior, parsed);
}

/**
 * Known readiness facts from gate / slots / campaign memory / latest message.
 * Missing facts stay unknown — never treat unknown as resolved.
 */
function knownReadinessState(context = {}) {
  const gate = context.gate || context.outreachLaunchGate || {};
  const slots = context.slots || {};
  const summary = gate.operatorStateSummary || {};
  const memory =
    (context.campaignMemory && context.campaignMemory.operatorLearnings) ||
    context.operatorLearnings ||
    {};
  const records = getConfirmedReadinessRecords(context);
  const confirmed = {
    ...(summary.confirmedReadiness || {}),
    ...(context.confirmedReadiness || {}),
    ...(slots.confirmedReadiness || {}),
    ...(memory.confirmed_readiness || {}),
  };
  const senderRecord = senderRecordFromConfirmed(records);
  const replyRecord = replyRecordFromConfirmed(records);
  const pathRecord = operationalPathRecordFromConfirmed(records);
  const followUpRecord = followUpRecordFromConfirmed(records);
  const monitoringRecord = monitoringRecordFromConfirmed(records);
  const senderResolved = resolveSenderIdentityFromContext(context);
  const replyResolved = resolveReplyHandlingFromContext(context);
  const pathResolved = resolveOperationalPathFromContext(context);
  const followUpResolved = resolveFollowUpTrackingFromContext(context);
  const monitoringResolved =
    resolveReplyMonitoringBatchReviewFromContext(context);
  const replyConfirmed = Boolean(
    confirmed.reply_inbox_handling ||
      confirmed.replyInboxHandling ||
      (replyRecord && replyRecord.confirmed) ||
      slots.replyInboxConfirmed ||
      slots.replyHandlingConfirmed ||
      context.replyInboxConfirmed ||
      context.replyHandlingConfirmed ||
      replyResolved.replyHandlingConfirmed
  );

  return {
    senderIdentityConfirmed: Boolean(
      confirmed.sender_identity ||
        confirmed.senderIdentity ||
        (senderRecord && senderRecord.confirmed) ||
        slots.senderIdentityConfirmed ||
        context.senderIdentityConfirmed ||
        senderResolved.senderIdentityConfirmed
    ),
    senderIdentity: senderResolved,
    replyHandling: replyResolved,
    replyInboxConfirmed: replyConfirmed,
    replyMonitoringConfirmed: Boolean(
      replyConfirmed ||
        confirmed.reply_monitoring_owner ||
        confirmed.replyMonitoringOwner ||
        slots.replyMonitoringConfirmed ||
        context.replyMonitoringConfirmed ||
        Boolean(replyResolved.replyMonitoringOwner) ||
        Boolean(monitoringResolved.replyMonitoringOwner)
    ),
    operationalPath: pathResolved,
    operationalPathChosen: Boolean(
      confirmed.operational_path ||
        confirmed.operational_path_selection ||
        confirmed.operationalPath ||
        confirmed.operationalPathSelection ||
        (pathRecord && pathRecord.confirmed) ||
        slots.operationalPathChosen ||
        context.operationalPathChosen ||
        pathResolved.operationalPathChosen
    ),
    followUpTracking: followUpResolved,
    followUpTrackingConfirmed: Boolean(
      confirmed.follow_up_tracking_process ||
        confirmed.follow_up_tracking ||
        confirmed.followUpTrackingProcess ||
        confirmed.followUpTracking ||
        (followUpRecord && followUpRecord.confirmed) ||
        slots.followUpTrackingConfirmed ||
        context.followUpTrackingConfirmed ||
        followUpResolved.followUpTrackingConfirmed
    ),
    replyMonitoringBatchReview: monitoringResolved,
    replyMonitoringBatch1Confirmed: Boolean(
      confirmed.reply_monitoring_batch1 ||
        confirmed.reply_monitoring_batch_review ||
        confirmed.replyMonitoringBatch1 ||
        confirmed.replyMonitoringBatchReview ||
        (monitoringRecord && monitoringRecord.confirmed) ||
        slots.replyMonitoringBatch1Confirmed ||
        context.replyMonitoringBatch1Confirmed ||
        monitoringResolved.replyMonitoringBatch1Confirmed
    ),
    confirmedReadinessRecords: records,
    trackingAccountApproved: Boolean(
      confirmed.tracking_account_settings ||
        confirmed.trackingAccountSettings ||
        slots.trackingAccountApproved ||
        context.trackingAccountApproved
    ),
    batch1ResultsReviewed: Boolean(
      confirmed.broader_rollout_batch1 ||
        confirmed.batch1ResultsReviewed ||
        slots.batch1ResultsReviewed ||
        context.batch1ResultsReviewed ||
        monitoringResolved.batch1ResultsReviewed
    ),
    inapplicable: {
      ...(summary.inapplicableReadiness || {}),
      ...(context.inapplicableReadiness || {}),
    },
  };
}

function evaluateReadinessItemAgainstState(item, context = {}) {
  const text = String(item || '').trim();
  const concept = readinessConceptId(text);
  const state = knownReadinessState(context);
  const inapplicableReason =
    state.inapplicable[concept] ||
    state.inapplicable[text.toLowerCase()] ||
    null;

  if (inapplicableReason) {
    return {
      text,
      status: 'inapplicable',
      reason: String(inapplicableReason),
      concept,
      display: `${text} — inapplicable: ${inapplicableReason}`,
    };
  }

  // Provided sender field values are confirmed facts, never unresolved gaps.
  if (isSenderFieldValueLine(text)) {
    return {
      text,
      status: 'confirmed',
      reason: 'sender identity field present',
      concept: 'sender_identity',
      display: text,
    };
  }

  // Provided reply-handling field values are confirmed facts.
  if (isReplyFieldValueLine(text)) {
    return {
      text,
      status: 'confirmed',
      reason: 'reply handling field present',
      concept: readinessConceptId(text),
      display: text,
    };
  }

  // Selected operational path values are confirmed facts, never unresolved gaps.
  if (isOperationalPathValueLine(text)) {
    return {
      text,
      status: 'confirmed',
      reason: 'operational path selected',
      concept: 'operational_path',
      display: text,
    };
  }

  // Provided follow-up tracking field values are confirmed facts.
  if (isFollowUpTrackingValueLine(text)) {
    return {
      text,
      status: 'confirmed',
      reason: 'follow-up tracking field present',
      concept: 'follow_up_tracking_process',
      display: text,
    };
  }

  // Provided reply-monitoring / Batch 1 review field values are confirmed.
  if (isReplyMonitoringBatchReviewValueLine(text)) {
    return {
      text,
      status: 'confirmed',
      reason: 'reply monitoring / Batch 1 review field present',
      concept: readinessConceptId(text),
      display: text,
    };
  }

  const confirmedByConcept = {
    sender_identity: state.senderIdentityConfirmed,
    reply_inbox_handling: state.replyInboxConfirmed,
    reply_handling_generic: state.replyInboxConfirmed,
    reply_monitoring_owner:
      state.replyMonitoringBatch1Confirmed || state.replyMonitoringConfirmed,
    operational_path: state.operationalPathChosen,
    follow_up_tracking_process: state.followUpTrackingConfirmed,
    tracking_account_settings: state.trackingAccountApproved,
    broader_rollout_batch1:
      state.batch1ResultsReviewed || state.replyMonitoringBatch1Confirmed,
  };

  if (confirmedByConcept[concept] === true) {
    return {
      text,
      status: 'confirmed',
      reason: 'already confirmed in campaign state',
      concept,
      display: text,
    };
  }

  // Unknown or explicitly unresolved — keep operator wording; do not drop.
  return {
    text,
    status: 'unresolved',
    reason: null,
    concept,
    display: text,
  };
}

/**
 * Merge operator checklist with known readiness state.
 * Separates confirmed vs unresolved. Operator wording preserved for gaps.
 * Distinct concepts stay distinct.
 */
function mergeOperatorReadinessChecklist(context = {}) {
  const explicit =
    (Array.isArray(context.unresolvedItems) && context.unresolvedItems) ||
    (Array.isArray(context.readinessGaps) && context.readinessGaps) ||
    null;

  const fromMessage = extractOperatorReadinessChecklist(
    context.operatorMessage || context.text || context.userMessage || ''
  );

  const gate = context.gate || context.outreachLaunchGate || {};
  const summary = gate.operatorStateSummary || {};
  const fromSummary =
    (Array.isArray(summary.unresolvedItems) && summary.unresolvedItems) ||
    (Array.isArray(summary.readinessGaps) && summary.readinessGaps) ||
    [];

  const operatorItems = (explicit && explicit.length
    ? explicit
    : fromMessage.length
      ? fromMessage
      : []
  )
    .map((item) => String(item || '').trim())
    .filter(Boolean);

  const sourceItems = operatorItems.length
    ? operatorItems
    : fromSummary.length
      ? fromSummary.map((item) => String(item).trim()).filter(Boolean)
      : DEFAULT_UNRESOLVED_READINESS_ITEMS.slice();

  const state = knownReadinessState(context);
  const usedConcepts = new Set();
  const evaluations = [];
  const unresolved = [];
  const confirmed = [];

  // Always surface confirmed sender identity from known state when present.
  if (
    state.senderIdentityConfirmed &&
    state.senderIdentity &&
    state.senderIdentity.senderName &&
    state.senderIdentity.senderEmail &&
    state.senderIdentity.senderSignature
  ) {
    confirmed.push({
      concept: 'sender_identity',
      status: 'confirmed',
      label: 'Sender identity is confirmed',
      fields: [
        `Sender name: ${state.senderIdentity.senderName}`,
        `Sender email address: ${state.senderIdentity.senderEmail}`,
        `Signature: ${state.senderIdentity.senderSignature}`,
      ],
    });
    usedConcepts.add('sender_identity');
  }

  // Confirmed reply handling block.
  if (
    state.replyInboxConfirmed &&
    state.replyHandling &&
    state.replyHandling.replyInbox &&
    state.replyHandling.replyToMatchesSender !== null &&
    state.replyHandling.replyMonitoringOwner
  ) {
    confirmed.push({
      concept: 'reply_inbox_handling',
      status: 'confirmed',
      label: 'Reply inbox / reply-to handling is confirmed',
      fields: [
        `Reply inbox: ${state.replyHandling.replyInbox}`,
        `Reply-to matches sender address: ${
          state.replyHandling.replyToMatchesSender ? 'yes' : 'no'
        }`,
        `Reply monitoring owner: ${state.replyHandling.replyMonitoringOwner}`,
      ],
    });
    usedConcepts.add('reply_inbox_handling');
    usedConcepts.add('reply_handling_generic');
    usedConcepts.add('reply_monitoring_owner');
  }

  // Confirmed operational path selection.
  if (
    state.operationalPathChosen &&
    state.operationalPath &&
    state.operationalPath.operationalPathLabel
  ) {
    confirmed.push({
      concept: 'operational_path',
      status: 'confirmed',
      label: 'Operational path is selected',
      fields: [state.operationalPath.operationalPathLabel],
    });
    usedConcepts.add('operational_path');
  }

  for (const item of sourceItems) {
    const evaluated = evaluateReadinessItemAgainstState(item, context);
    evaluations.push(evaluated);

    if (evaluated.status === 'confirmed' || evaluated.status === 'resolved') {
      // Field values already represented in confirmed blocks above.
      if (
        evaluated.concept === 'sender_identity' ||
        evaluated.concept === 'reply_inbox_handling' ||
        evaluated.concept === 'reply_handling_generic' ||
        evaluated.concept === 'reply_monitoring_owner' ||
        evaluated.concept === 'operational_path'
      ) {
        continue;
      }
      if (usedConcepts.has(evaluated.concept) && !evaluated.concept.startsWith('custom:')) {
        continue;
      }
      usedConcepts.add(evaluated.concept);
      confirmed.push(evaluated);
      continue;
    }

    if (evaluated.status === 'inapplicable') {
      if (usedConcepts.has(evaluated.concept) && !evaluated.concept.startsWith('custom:')) {
        continue;
      }
      usedConcepts.add(evaluated.concept);
      unresolved.push(evaluated.display);
      continue;
    }

    // unresolved
    if (usedConcepts.has(evaluated.concept) && !evaluated.concept.startsWith('custom:')) {
      continue;
    }
    usedConcepts.add(evaluated.concept);
    unresolved.push(evaluated.display);
  }

  // Drop confirmed field-value lines and default "not confirmed" wording.
  const unresolvedFiltered = unresolved.filter((item) => {
    if (
      isSenderFieldValueLine(item) ||
      isReplyFieldValueLine(item) ||
      isOperationalPathValueLine(item)
    ) {
      return false;
    }
    if (
      state.senderIdentityConfirmed &&
      /\bsender\s+identity\s+is\s+not\s+confirmed\b/i.test(item)
    ) {
      return false;
    }
    if (
      state.replyInboxConfirmed &&
      /\breply\s+handling\s+is\s+not\s+confirmed\b/i.test(item)
    ) {
      return false;
    }
    if (
      state.operationalPathChosen &&
      /\boperational\s+path\b/i.test(item) &&
      /\b(?:not\s+chosen|is\s+not\s+confirmed|not\s+selected)\b/i.test(item)
    ) {
      return false;
    }
    return true;
  });

  return {
    items: unresolvedFiltered,
    unresolvedItems: unresolvedFiltered,
    confirmedItems: confirmed,
    operatorSpecified: operatorItems.length > 0,
    evaluations,
    senderIdentityConfirmed: state.senderIdentityConfirmed,
    senderIdentity: state.senderIdentity,
    replyInboxConfirmed: state.replyInboxConfirmed,
    replyHandling: state.replyHandling,
    operationalPathChosen: state.operationalPathChosen,
    operationalPath: state.operationalPath,
    followUpTrackingConfirmed: state.followUpTrackingConfirmed,
    followUpTracking: state.followUpTracking,
  };
}

/**
 * Resolve unresolved readiness items from context / gate / operator message.
 */
function unresolvedReadinessItems(context = {}) {
  return mergeOperatorReadinessChecklist(context).unresolvedItems;
}

/**
 * Resolve approved Batch 1 scope details (approved names + exclusions).
 */
function resolveBatch1ScopeDetailsFromContext(context = {}) {
  const slots = context.slots || {};
  const strategy =
    context.outreachStrategyPreview ||
    context.priorOutreachStrategyPreview ||
    null;
  const review =
    context.prospectBatchReview ||
    context.priorProspectBatchReview ||
    null;
  const batch =
    (review && review.approvedBatch) ||
    context.approvedBatch ||
    null;

  let approved = [];
  if (Array.isArray(context.batch1ApprovedNames)) {
    approved = context.batch1ApprovedNames.slice();
  } else if (Array.isArray(slots.batch1ApprovedNames)) {
    approved = slots.batch1ApprovedNames.slice();
  } else if (batch) {
    const candidates = Array.isArray(batch.candidates) ? batch.candidates : [];
    approved = candidates
      .map((c) => (typeof c === 'string' ? c : c && (c.companyName || c.company)))
      .filter(Boolean);
  }

  const shortName = (name) => {
    const s = String(name || '').trim();
    if (!s) return s;
    if (/^Cedar\b/i.test(s)) return 'Cedar';
    if (/^Keyrenter\b/i.test(s)) return 'Keyrenter';
    if (/Cushman/i.test(s) && /Wakefield/i.test(s)) return 'Cushman & Wakefield';
    return s;
  };

  const excluded = [];
  const pushExcluded = (items, reason) => {
    for (const item of items || []) {
      const name =
        typeof item === 'string'
          ? item
          : item && (item.companyName || item.company || item.name);
      if (!name) continue;
      excluded.push(`${shortName(name)}: ${reason}`);
    }
  };

  if (batch) {
    pushExcluded(
      batch.excludedSourceVerification,
      'source verification required'
    );
    pushExcluded(
      batch.excludedExistingRelationship,
      'existing relationship / nurture'
    );
    if (
      Array.isArray(batch.excludedOptionalExpansion) &&
      batch.excludedOptionalExpansion.length
    ) {
      excluded.push('optional Manchester candidates: excluded');
    }
    pushExcluded(batch.excludedRejected, 'rejected as too institutional');
  } else if (review) {
    pushExcluded(
      review.sourceVerificationRequired,
      'source verification required'
    );
    pushExcluded(
      review.existingRelationship,
      'existing relationship / nurture'
    );
    if (
      Array.isArray(review.optionalExpansion) &&
      review.optionalExpansion.length
    ) {
      excluded.push('optional Manchester candidates: excluded');
    }
    for (const item of review.rejected || []) {
      const name = item && (item.companyName || item.company);
      const reason =
        (item && (item.statusReason || item.reason)) ||
        'rejected as too institutional';
      if (!name) continue;
      if (/institutional/i.test(reason)) {
        excluded.push(`${shortName(name)}: rejected as too institutional`);
      } else {
        excluded.push(`${shortName(name)}: ${reason}`);
      }
    }
  }

  if (!excluded.length && Array.isArray(context.batch1ExcludedLines)) {
    excluded.push(...context.batch1ExcludedLines);
  }
  if (!excluded.length && Array.isArray(slots.batch1ExcludedLines)) {
    excluded.push(...slots.batch1ExcludedLines);
  }

  if (!approved.length && strategy && strategy.approvedBatchPhrase) {
    // Fall back later via text scope.
  }

  return { approved, excluded };
}

/**
 * Resolve approved Batch 1 scope text from campaign artifacts / slots.
 */
function resolveBatch1ScopeFromContext(context = {}) {
  if (context.batch1Scope) return String(context.batch1Scope).trim();
  const slots = context.slots || {};
  if (slots.batch1Scope) return String(slots.batch1Scope).trim();

  const details = resolveBatch1ScopeDetailsFromContext(context);
  if (details.approved.length) {
    return details.approved.join('; ');
  }

  const strategy =
    context.outreachStrategyPreview ||
    context.priorOutreachStrategyPreview ||
    null;
  if (strategy && strategy.batch1Scope) {
    return String(strategy.batch1Scope).trim();
  }

  const review =
    context.prospectBatchReview ||
    context.priorProspectBatchReview ||
    null;
  const batch =
    (review && review.approvedBatch) ||
    context.approvedBatch ||
    null;
  if (batch) {
    const candidates = Array.isArray(batch.candidates) ? batch.candidates : [];
    const names = candidates
      .map((c) => c && (c.companyName || c.company))
      .filter(Boolean);
    const count =
      batch.candidateCount != null ? batch.candidateCount : candidates.length;
    if (count || names.length) {
      const parts = [
        `${count || names.length} approved cold first-pass prospects in Batch 1.`,
      ];
      if (names.length) {
        parts.push(
          `Accounts: ${names.slice(0, 12).join('; ')}${
            names.length > 12 ? `; +${names.length - 12} more` : ''
          }.`
        );
      }
      return parts.join(' ');
    }
  }

  const gate = context.gate || context.outreachLaunchGate || {};
  if (gate.batch1Scope) return String(gate.batch1Scope).trim();
  if (
    gate.approvedCandidateCount != null ||
    (Array.isArray(gate.batchProspects) && gate.batchProspects.length)
  ) {
    const count =
      gate.approvedCandidateCount != null
        ? gate.approvedCandidateCount
        : gate.batchProspects.length;
    const names = Array.isArray(gate.batchProspects) ? gate.batchProspects : [];
    return [
      `${count} approved cold first-pass prospects in Batch 1.`,
      names.length
        ? `Accounts: ${names.slice(0, 12).join('; ')}.`
        : null,
    ]
      .filter(Boolean)
      .join(' ');
  }

  return 'Approved Batch 1 cold first-pass prospects (scope already approved).';
}

/**
 * Next explicit execute action for the selected operational path.
 * Never auto-executes — language only.
 */
function nextExecuteActionForOperationalPath(pathId, pathLabel) {
  const id = String(pathId || '').trim();
  const label = String(pathLabel || '').trim();
  if (
    id === 'manual_send_export' ||
    /manual[-\s]?send\s+export/i.test(label)
  ) {
    return 'explicitly approve preparing the manual-send export for operator review';
  }
  if (id === 'crm_drafts' || /\bcrm\s+drafts?\b/i.test(label)) {
    return 'explicitly approve creating CRM drafts';
  }
  if (id === 'queued_sends_later' || /\bqueue(?:d)?\s+sends?/i.test(label)) {
    return 'explicitly approve queuing sends later';
  }
  if (id === 'hold' || /\bhold(?:\s+with\s+no\s+action)?\b/i.test(label)) {
    return 'hold with no action (no execute action required)';
  }
  return 'explicitly approve the next prepare/export/CRM action if desired';
}

function normalizeTrackingLocationLabel(raw) {
  let s = String(raw || '').trim();
  if (!s) return s;
  s = s.replace(/^the\s+/i, '');
  return s;
}

function normalizeMonitoringProcessLine(raw, fallback) {
  const s = String(raw || '').trim();
  if (!s) return fallback;
  // Prefer concise expected phrasing when the stored process is verbose.
  if (/manual(?:ly)?/i.test(s) && /review/i.test(s)) {
    return 'replies reviewed manually before follow-up or broader rollout';
  }
  return s.replace(/^Responses?\s+/i, 'replies ').replace(/\.$/, '');
}

function normalizeCampaignReadyBullet(raw, fallback) {
  let s = String(raw || fallback || '').trim().replace(/\.$/, '');
  if (!s) return fallback;
  if (/positive\s+replies/i.test(s)) {
    return 'positive replies handled as conversation/walkthrough opportunities';
  }
  if (/negative|not-?now/i.test(s)) {
    return 'negative or not-now replies respected and noted';
  }
  return s.charAt(0).toLowerCase() + s.slice(1);
}

/**
 * Final campaign-ready summary — compose the WHOLE readiness state.
 * Retrieves confirmed readiness records (not only the latest prompt / substep).
 * Never collapses to the latest readiness item. Never executes.
 */
function composeCampaignReadySummary(context = {}) {
  const state = knownReadinessState(context);
  const slots = context.slots || {};
  const businessName =
    context.businessName ||
    slots.businessName ||
    (context.outreachStrategyPreview &&
      context.outreachStrategyPreview.businessName) ||
    (context.prospectBatchReview &&
      context.prospectBatchReview.businessName) ||
    'Anchor';
  const batchLabel =
    context.batchName ||
    slots.batchName ||
    (context.outreachStrategyPreview &&
      context.outreachStrategyPreview.batchName) ||
    'Batch 1';
  const batch1Scope = resolveBatch1ScopeFromContext(context);
  const scopeDetails = resolveBatch1ScopeDetailsFromContext(context);

  const pathLabel =
    (state.operationalPath && state.operationalPath.operationalPathLabel) ||
    slots.operationalPathLabel ||
    context.operationalPathLabel ||
    'manual send export for operator review';
  const pathId =
    (state.operationalPath && state.operationalPath.operationalPathId) ||
    slots.operationalPathId ||
    context.operationalPathId ||
    null;
  const nextExecute = nextExecuteActionForOperationalPath(pathId, pathLabel);

  const sender = state.senderIdentity || {};
  const reply = state.replyHandling || {};
  const followUp = state.followUpTracking || {};
  const monitoring = state.replyMonitoringBatchReview || {};

  const senderFullyPresent = Boolean(
    sender.senderName && sender.senderEmail && sender.senderSignature
  );
  const replyFullyPresent = Boolean(
    reply.replyInbox &&
      reply.replyToMatchesSender !== null &&
      reply.replyMonitoringOwner
  );

  const lines = [];
  lines.push(`${businessName} ${batchLabel} campaign-ready summary`);
  lines.push('');

  lines.push('Batch 1 approved scope:');
  if (scopeDetails.approved.length) {
    for (const name of scopeDetails.approved) {
      lines.push(`- ${name}`);
    }
  } else {
    lines.push(`- ${batch1Scope}`);
  }
  if (scopeDetails.excluded.length) {
    lines.push('');
    lines.push('Excluded:');
    for (const row of scopeDetails.excluded) {
      lines.push(`- ${row}`);
    }
  }
  lines.push('');

  if (state.senderIdentityConfirmed && senderFullyPresent) {
    lines.push('Sender identity confirmed:');
    lines.push(`- ${sender.senderName}`);
    lines.push(`- ${sender.senderEmail}`);
    lines.push(`- signature: ${sender.senderSignature}`);
  } else {
    lines.push('Sender identity: not fully confirmed');
  }
  lines.push('');

  if (state.replyInboxConfirmed && replyFullyPresent) {
    lines.push('Reply handling confirmed:');
    lines.push(`- reply inbox: ${reply.replyInbox}`);
    lines.push(
      `- reply-to matches sender: ${
        reply.replyToMatchesSender ? 'yes' : 'no'
      }`
    );
    lines.push(`- reply monitoring owner: ${reply.replyMonitoringOwner}`);
  } else {
    lines.push('Reply handling: not fully confirmed');
  }
  lines.push('');

  if (state.operationalPathChosen && pathLabel) {
    lines.push('Operational path selected:');
    lines.push(`- ${pathLabel}`);
  } else {
    lines.push('Operational path: not selected');
  }
  lines.push('');

  if (state.followUpTrackingConfirmed) {
    lines.push('Follow-up tracking confirmed:');
    const location = normalizeTrackingLocationLabel(
      followUp.followUpTrackingLocation
    );
    if (location) {
      lines.push(`- status tracked in ${location}`);
    }
    if (followUp.followUp1Timing) {
      lines.push(`- Follow-up 1: ${followUp.followUp1Timing}`);
    }
    if (followUp.followUp2Timing) {
      lines.push(`- Follow-up 2: ${followUp.followUp2Timing}`);
    }
    if (followUp.followUpReviewFirstManual !== false) {
      lines.push('- follow-ups remain review-first/manual');
    }
    if (followUp.automaticFollowUpSendsApproved !== true) {
      lines.push('- no automatic follow-up sends approved');
    }
  } else {
    lines.push('Follow-up tracking: not confirmed');
  }
  lines.push('');

  if (state.replyMonitoringBatch1Confirmed) {
    lines.push('Reply monitoring / Batch 1 review confirmed:');
    lines.push(
      `- ${normalizeMonitoringProcessLine(
        monitoring.responseReviewProcess,
        'replies reviewed manually before follow-up or broader rollout'
      )}`
    );
    lines.push(
      `- ${normalizeCampaignReadyBullet(
        monitoring.positiveReplyHandling,
        'positive replies handled as conversation/walkthrough opportunities'
      )}`
    );
    lines.push(
      `- ${normalizeCampaignReadyBullet(
        monitoring.negativeReplyHandling,
        'negative or not-now replies respected and noted'
      )}`
    );
    if (monitoring.broaderRolloutBlocked !== false) {
      lines.push(
        '- broader rollout blocked until Batch 1 results are reviewed'
      );
    }
  } else {
    lines.push('Reply monitoring / Batch 1 review: not confirmed');
  }
  lines.push('');

  lines.push('Execution lock:');
  lines.push('- active');
  lines.push(
    '- nothing has been sent, exported, written to CRM, queued, or changed in accounts/settings'
  );
  lines.push('');
  lines.push('Next possible execute action:');
  lines.push(`- ${nextExecute}`);
  lines.push('');
  lines.push(
    context.safetyLine ||
      context.compactSafety ||
      CAMPAIGN_READY_SUMMARY_SAFETY_LINE
  );
  lines.push('');
  lines.push(
    context.closingAsk ||
      context.closingQuestion ||
      CAMPAIGN_READY_SUMMARY_CLOSING
  );

  const message = dedupeOperatorStateUpdateMessage(lines.join('\n').trim());
  return {
    mode: CONVERSATION_MODES.CAMPAIGN_READY_SUMMARY,
    responseMode: toResponseMode(CONVERSATION_MODES.CAMPAIGN_READY_SUMMARY),
    message,
    includeRendererSections: false,
    includeExpandedSafety: false,
    batch1Scope,
    senderIdentityConfirmed: state.senderIdentityConfirmed === true,
    replyInboxConfirmed: state.replyInboxConfirmed === true,
    operationalPathChosen: state.operationalPathChosen === true,
    operationalPathLabel: pathLabel,
    followUpTrackingConfirmed: state.followUpTrackingConfirmed === true,
    replyMonitoringBatch1Confirmed:
      state.replyMonitoringBatch1Confirmed === true,
    executionLockActive: true,
    nextExecuteAction: nextExecute,
    confirmedReadinessRecords: state.confirmedReadinessRecords || {},
    requiresExplicitApproval: false,
    executionPending: false,
    sendsMade: false,
    crmWritesMade: false,
    exportMade: false,
    accountChangesMade: false,
  };
}

/**
 * Readiness check — list unresolved items only; never ask for execute approval.
 * Confirmed readiness is shown separately and never under "still unresolved".
 * After an item is completed, advance to the next unresolved item — do not
 * re-ask "Which readiness item should we resolve first?"
 */
function composeOperatorReadinessCheck(context = {}) {
  const merged = mergeOperatorReadinessChecklist(context);
  const unresolved = merged.unresolvedItems || merged.items || [];
  const closingAsk =
    context.closingAsk ||
    context.closingQuestion ||
    context.currentAsk ||
    READINESS_CHECKLIST_CLOSING_ASK;
  const safetyLine =
    context.safetyLine ||
    context.compactSafety ||
    READINESS_CHECKLIST_SAFETY_LINE;

  const lines = [];

  if (
    merged.senderIdentityConfirmed &&
    merged.senderIdentity &&
    merged.senderIdentity.senderName &&
    merged.senderIdentity.senderEmail &&
    merged.senderIdentity.senderSignature
  ) {
    lines.push('Sender identity is confirmed:');
    lines.push(`- Sender name: ${merged.senderIdentity.senderName}`);
    lines.push(
      `- Sender email address: ${merged.senderIdentity.senderEmail}`
    );
    lines.push(`- Signature: ${merged.senderIdentity.senderSignature}`);
    lines.push('');
  }

  if (
    merged.replyInboxConfirmed &&
    merged.replyHandling &&
    merged.replyHandling.replyInbox &&
    merged.replyHandling.replyToMatchesSender !== null &&
    merged.replyHandling.replyMonitoringOwner
  ) {
    lines.push('Reply inbox / reply-to handling is confirmed:');
    lines.push(`- Reply inbox: ${merged.replyHandling.replyInbox}`);
    lines.push(
      `- Reply-to matches sender address: ${
        merged.replyHandling.replyToMatchesSender ? 'yes' : 'no'
      }`
    );
    lines.push(
      `- Reply monitoring owner: ${merged.replyHandling.replyMonitoringOwner}`
    );
    lines.push('');
  }

  if (
    merged.operationalPathChosen &&
    merged.operationalPath &&
    merged.operationalPath.operationalPathLabel
  ) {
    lines.push('Operational path is selected:');
    lines.push(`- ${merged.operationalPath.operationalPathLabel}`);
    lines.push('');
    lines.push(
      merged.operationalPath.selectionNote ||
        'This is path selection only. Nothing has been prepared or executed.'
    );
    lines.push('');
  }

  const readinessState = knownReadinessState(context);
  if (
    readinessState.followUpTrackingConfirmed &&
    readinessState.followUpTracking &&
    readinessState.followUpTracking.followUpTrackingLocation
  ) {
    const fu = readinessState.followUpTracking;
    lines.push('Follow-up tracking process is confirmed:');
    lines.push(`- Status tracked in ${fu.followUpTrackingLocation}`);
    lines.push(`- Follow-up 1 planned for ${fu.followUp1Timing}`);
    lines.push(`- Follow-up 2 planned for ${fu.followUp2Timing}`);
    lines.push(
      `- ${
        fu.followUpReviewFirstManual
          ? 'Follow-ups remain review-first/manual unless explicitly enabled later'
          : 'Follow-ups are not held to review-first/manual'
      }`
    );
    lines.push(
      `- ${
        fu.automaticFollowUpSendsApproved
          ? 'Automatic follow-up sends are approved'
          : 'No automatic follow-up sends are approved'
      }`
    );
    lines.push('');
  }

  let nextItem = null;
  if (merged.senderIdentityConfirmed && !merged.replyInboxConfirmed) {
    nextItem = READINESS_NEXT_ITEM_PROMPTS.reply_handling;
  } else if (merged.replyInboxConfirmed && !merged.operationalPathChosen) {
    nextItem = READINESS_NEXT_ITEM_PROMPTS.operational_path;
  } else if (
    merged.operationalPathChosen &&
    !readinessState.followUpTrackingConfirmed
  ) {
    nextItem = READINESS_NEXT_ITEM_PROMPTS.follow_up_tracking;
  } else if (
    readinessState.followUpTrackingConfirmed &&
    !readinessState.replyMonitoringBatch1Confirmed
  ) {
    nextItem = READINESS_NEXT_ITEM_PROMPTS.reply_monitoring_batch1;
  }

  if (nextItem) {
    const questions = nextItem.questions || [nextItem.ask];
    lines.push(`Next readiness item: ${nextItem.label}.`);
    lines.push('');
    for (const q of questions) lines.push(q);
    lines.push('');
    lines.push(safetyLine);
  } else if (unresolved.length) {
    lines.push(
      context.leadIn ||
        'Still unresolved before any export, CRM drafts, or queued sends:'
    );
    lines.push('');
    for (const item of unresolved) lines.push(`- ${item}`);
    lines.push('');
    lines.push(safetyLine);
    lines.push('');
    lines.push(closingAsk);
  } else {
    lines.push('Readiness items are confirmed for now.');
    lines.push('');
    lines.push(safetyLine);
  }

  const message = lines.join('\n').trim();
  return {
    mode: CONVERSATION_MODES.OPERATOR_READINESS_CHECK,
    responseMode: toResponseMode(CONVERSATION_MODES.OPERATOR_READINESS_CHECK),
    message,
    includeRendererSections: false,
    includeExpandedSafety: false,
    unresolvedItems: unresolved,
    confirmedItems: merged.confirmedItems,
    operatorSpecifiedChecklist: merged.operatorSpecified,
    senderIdentityConfirmed: merged.senderIdentityConfirmed === true,
    replyInboxConfirmed: merged.replyInboxConfirmed === true,
    operationalPathChosen: merged.operationalPathChosen === true,
    operationalPath: merged.operationalPath || null,
    followUpTrackingConfirmed: readinessState.followUpTrackingConfirmed === true,
    followUpTracking: readinessState.followUpTracking || null,
    nextReadinessItem: nextItem,
    requiresExplicitApproval: false,
    executionPending: false,
    sendsMade: false,
    crmWritesMade: false,
    exportMade: false,
    accountChangesMade: false,
  };
}

/**
 * Readiness substep — operator already selected an item.
 * Ask only that item's detail questions; never re-ask which item to resolve.
 */
function composeReadinessSubstep(context = {}) {
  const readinessItemId =
    normalizeReadinessItemId(context.readinessItemId) ||
    context.readinessItemId ||
    null;
  const selected =
    context.selectedReadinessItem ||
    detectSelectedReadinessItem(
      context.operatorMessage || context.text || context.userMessage || ''
    ) ||
    (readinessItemId && READINESS_SUBSTEPS[readinessItemId]) ||
    READINESS_SUBSTEPS.sender_identity;

  const questions = Array.isArray(context.questions) && context.questions.length
    ? context.questions
    : selected.questions || [];
  const closingAsk =
    context.closingAsk ||
    context.closingQuestion ||
    selected.closingAsk ||
    `Once you answer those, I'll mark ${selected.label} as confirmed or note what still needs review.`;
  const safetyLine =
    context.safetyLine ||
    context.compactSafety ||
    READINESS_SUBSTEP_SAFETY_LINE;
  const leadIn =
    context.leadIn ||
    `Let's resolve ${selected.label}. I won't repeat the full readiness checklist.`;

  const lines = [leadIn, ''];
  for (let i = 0; i < questions.length; i += 1) {
    lines.push(`${i + 1}. ${questions[i]}`);
  }
  lines.push('', safetyLine, '', closingAsk);

  const message = lines.join('\n').trim();
  return {
    mode: CONVERSATION_MODES.READINESS_SUBSTEP,
    responseMode: toResponseMode(CONVERSATION_MODES.READINESS_SUBSTEP),
    message,
    includeRendererSections: false,
    includeExpandedSafety: false,
    selectedReadinessItem: selected,
    readinessItemId: selected.id,
    questions: questions.slice(),
    closingAsk,
    requiresExplicitApproval: false,
    sendsMade: false,
    crmWritesMade: false,
    exportMade: false,
    accountChangesMade: false,
  };
}

function composeExecutionConfirmation(context = {}) {
  const action = context.action || context.executeAction || 'execute action';
  const records =
    context.recordsAffected ||
    context.recordSummary ||
    'the selected campaign records';
  const sender = context.sender || context.account || 'the configured sender/account';
  const effects =
    context.externalEffects ||
    'This can send, export, or write externally and may be irreversible.';

  const lines = [
    'Execution confirmation — nothing has run yet.',
    '',
    `Exact action: ${action}`,
    `Records affected: ${records}`,
    `Sender / account: ${sender}`,
    `External effects: ${effects}`,
    '',
    expandedSafetyBlock({
      header: 'Still locked until you explicitly approve:',
    }),
    '',
    'Do you explicitly approve this execute action?',
  ];

  if (context.holdGuidance) {
    lines.push('');
    lines.push(String(context.holdGuidance).trim());
  }

  return {
    mode: CONVERSATION_MODES.EXECUTION_CONFIRMATION,
    responseMode: toResponseMode(CONVERSATION_MODES.EXECUTION_CONFIRMATION),
    message: lines.join('\n').trim(),
    includeRendererSections: false,
    includeExpandedSafety: true,
    requiresExplicitApproval: true,
    sendsMade: false,
    crmWritesMade: false,
    exportMade: false,
    accountChangesMade: false,
  };
}

/**
 * Light clarification for accidental / low-signal input.
 * Preserves workflow state — never re-renders full Launch Gate summary,
 * operational options block, or execute-approval ask.
 */
function composeClarificationNeeded(context = {}) {
  const raw = String(
    context.operatorMessage || context.text || context.userMessage || ''
  ).trim();
  const quoted = raw || '(empty message)';
  const stepHint =
    context.stepHint ||
    (context.gateAlreadyApproved ||
    context.launchGateApproved ||
    context.approvedReadinessOnly
      ? 'the readiness-check step'
      : context.currentState ||
        context.step ||
        context.planningState ||
        'the current step');
  const ask =
    context.clarificationAsk ||
    context.closingAsk ||
    context.closingQuestion ||
    CLARIFICATION_NEEDED_ASK;

  const message =
    context.message ||
    `Not sure what you meant by \`${quoted}\`. We're still at ${stepHint}. ${ask}`;

  return {
    mode: CONVERSATION_MODES.CLARIFICATION_NEEDED,
    responseMode: toResponseMode(CONVERSATION_MODES.CLARIFICATION_NEEDED),
    message: String(message).trim(),
    includeRendererSections: false,
    includeExpandedSafety: false,
    requiresExplicitApproval: false,
    sendsMade: false,
    crmWritesMade: false,
    exportMade: false,
    accountChangesMade: false,
  };
}

/**
 * Canonical approved Launch Gate Operator State Update.
 * One acknowledgment, one safety/lock line, one options list, one ask.
 */
function formatApprovedLaunchGateConversational(gate, opts = {}) {
  const g = gate || {};
  const summary = g.operatorStateSummary || {};

  const nextPaths =
    opts.nextPaths ||
    (Array.isArray(summary.nextOptions) && summary.nextOptions.length
      ? summary.nextOptions.map(normalizeNextPathPhrase)
      : DEFAULT_NEXT_PATHS.slice());

  const guidance =
    opts.operatorGuidance ||
    "I'd keep this held until sender identity and reply handling are confirmed.";
  const closingAsk =
    opts.closingAsk ||
    opts.currentAsk ||
    approvalLanguageForGate({ gateName: 'Launch Gate', approved: true }).ask;

  // Single canonical opening paragraph — never stack headline + leadIn + ack.
  const opening =
    opts.openingParagraph ||
    'Outreach Launch Gate is approved for readiness only. Nothing external happened: no send, no export, no CRM write, and no account changes. Execution is still locked.';

  const lines = [
    opening,
    '',
    'The next choice is operational:',
    ...nextPaths.map((p, i) => `${i + 1}. ${p}`),
    '',
    guidance,
    '',
    closingAsk,
  ];

  // Ignore duplicative leadIns (approval/safety restatements). Personality
  // asides that do not restate status may still prepend once.
  const leadIn = sanitizeApprovedStateLeadIn(opts.leadIn, opening);
  const body = lines.join('\n').trim();
  const message = dedupeOperatorStateUpdateMessage(
    leadIn ? `${leadIn}\n\n${body}` : body
  );

  return {
    mode: CONVERSATION_MODES.OPERATOR_STATE_UPDATE,
    responseMode: toResponseMode(CONVERSATION_MODES.OPERATOR_STATE_UPDATE),
    message,
    includeRendererSections: false,
    includeExpandedSafety: false,
  };
}

/**
 * Drop leadIns that restate approval / safety / lock — those belong only in
 * the canonical opening paragraph.
 */
function sanitizeApprovedStateLeadIn(leadIn, openingParagraph) {
  const raw = String(leadIn || '').trim();
  if (!raw) return null;
  if (/approved for readiness only/i.test(raw)) return null;
  if (/nothing external happened/i.test(raw)) return null;
  if (/execution is still locked|execution lock/i.test(raw)) return null;
  if (/campaign-ready/i.test(raw)) return null;
  if (/showing the (?:current state|approved-state)|not the review card/i.test(raw)) {
    return null;
  }
  // Exact duplicate of opening (or contained by it) — skip.
  const open = String(openingParagraph || '');
  if (open && (open.includes(raw) || raw.includes(open.slice(0, 40)))) {
    return null;
  }
  return raw;
}

/**
 * Dedupe pass for operator_state_update and campaign_ready_summary messages.
 * Keeps first occurrence of status / safety / lock statements, collapses
 * repeated blank lines, and drops a duplicated full campaign-ready summary.
 */
function dedupeOperatorStateUpdateMessage(text) {
  let raw = String(text || '').trim();
  if (!raw) return raw;

  // Collapse an entire duplicated campaign-ready summary (title appears twice).
  const titleMatch = raw.match(/^(.+\bcampaign-ready summary)\s*$/im);
  if (titleMatch) {
    const title = titleMatch[1];
    const firstIdx = raw.indexOf(title);
    const secondIdx = raw.indexOf(title, firstIdx + title.length);
    if (secondIdx > firstIdx) {
      raw = raw.slice(firstIdx, secondIdx).trim();
    }
  }

  const seen = {
    approved: false,
    nothingExternal: false,
    executionLocked: false,
    nextChoice: false,
    guidance: false,
    nextPathAsk: false,
    campaignReadySummary: false,
  };

  const out = [];
  const paragraphs = raw.split(/\n{2,}/);

  for (const para of paragraphs) {
    const p = String(para || '').trim();
    if (!p) continue;

    const isCampaignReadyTitle = /\bcampaign-ready summary\b/i.test(p);
    if (isCampaignReadyTitle) {
      if (seen.campaignReadySummary) {
        // Drop the rest of a duplicated campaign-ready summary.
        break;
      }
      seen.campaignReadySummary = true;
      out.push(p);
      continue;
    }

    const isApprovedAck = /approved for readiness only/i.test(p);
    const isNothingExternal = /nothing external happened/i.test(p);
    const isExecutionLocked =
      /execution is still locked|execution lock (?:still )?active/i.test(p);
    const isNextChoice = /the next choice is operational/i.test(p);
    const isGuidance =
      /i'?d keep this held until sender identity/i.test(p);
    const isNextPathAsk =
      /which next path do you want to prepare/i.test(p);

    // Merge status+safety+lock into one opening when they arrive as fragments.
    if (
      (isApprovedAck || isNothingExternal || isExecutionLocked) &&
      !isNextChoice
    ) {
      if (seen.approved || seen.nothingExternal || seen.executionLocked) {
        // Already emitted a combined/fragment opening — skip duplicates.
        // Prefer keeping the richer first paragraph; skip thinner repeats.
        continue;
      }
      seen.approved = true;
      seen.nothingExternal = true;
      seen.executionLocked = true;
      out.push(
        'Outreach Launch Gate is approved for readiness only. Nothing external happened: no send, no export, no CRM write, and no account changes. Execution is still locked.'
      );
      continue;
    }

    if (isNextChoice) {
      if (seen.nextChoice) continue;
      seen.nextChoice = true;
      out.push(p);
      continue;
    }

    if (isGuidance) {
      if (seen.guidance) continue;
      seen.guidance = true;
      out.push(p);
      continue;
    }

    if (isNextPathAsk) {
      if (seen.nextPathAsk) continue;
      seen.nextPathAsk = true;
      out.push(p);
      continue;
    }

    out.push(p);
  }

  return out.join('\n\n').trim();
}

function normalizeNextPathPhrase(item) {
  const s = String(item || '').trim();
  if (!s) return s;
  const lower = s.toLowerCase();
  if (/manual-?send\s+export/i.test(lower)) {
    return 'prepare a manual-send export for review';
  }
  if (/crm\s+drafts/i.test(lower)) {
    return 'create CRM drafts, if explicitly approved';
  }
  if (/queue\s+sends/i.test(lower)) {
    return 'queue sends later, if execution is intentionally enabled';
  }
  if (/hold/i.test(lower)) {
    return 'hold with no action';
  }
  return lower.charAt(0).toLowerCase() + s.slice(1);
}

/**
 * Soften a stale-source diagnostic into Operator Diagnostic style
 * (plain language first, technical details second).
 */
function formatOperatorDiagnosticMessage(opts = {}) {
  return composeOperatorDiagnostic({
    plainLanguage:
      opts.plainLanguage ||
      'I found the problem: campaign memory was correct, but the stored draft renderer was still winning.',
    stopLine:
      opts.stopLine ||
      "I'm stopping before showing another stale draft.",
    technicalDetail: opts.technicalDetail || opts.detail || null,
    nextAction:
      opts.nextAction ||
      'Confirm whether to force-rebuild from operator instructions only (bypass the stored artifact / workflow card renderer).',
  });
}

/**
 * Post-decision wrapper: take a workflow reply and apply conversation policy.
 * Does not invent new workflow state — only adjusts voice / mode / framing.
 */
function applyConversationalPolicy(reply, context = {}) {
  if (!reply || typeof reply !== 'object') return reply;

  const assessment = assessConversationContext({
    ...context,
    operatorMessage: context.operatorMessage || context.text,
    stateChanged:
      context.stateChanged ||
      reply.intent === 'outreach_launch_gate_approved' ||
      reply.launchGateApproved === true,
    gateAlreadyApproved:
      context.gateAlreadyApproved ||
      reply.launchGateApproved === true ||
      (reply.outreachLaunchGate &&
        (reply.outreachLaunchGate.launchGateApproved === true ||
          reply.outreachLaunchGate.status === 'approved_readiness_only')),
    isRevision:
      context.isRevision ||
      reply.responseMode === RESPONSE_MODES.OPERATOR_CHAT_RESPONSE,
    isDiagnostic:
      context.isDiagnostic ||
      reply.responseMode === RESPONSE_MODES.STALE_SOURCE_DIAGNOSTIC ||
      (reply.kind || reply.intent) === 'stale_source_diagnostic',
    isInitialReviewGate:
      context.isInitialReviewGate ||
      reply.responseMode === RESPONSE_MODES.WORKFLOW_REVIEW_CARD,
    isReadinessCheck:
      context.isReadinessCheck ||
      reply.responseMode === 'operator_readiness_check' ||
      reply.conversationMode === 'operator_readiness_check' ||
      reply.intent === 'operator_readiness_check',
    isCampaignReadySummary:
      context.isCampaignReadySummary ||
      reply.responseMode === 'campaign_ready_summary' ||
      reply.conversationMode === 'campaign_ready_summary' ||
      reply.intent === 'campaign_ready_summary',
    isReadinessSubstep:
      context.isReadinessSubstep ||
      reply.responseMode === 'readiness_substep' ||
      reply.conversationMode === 'readiness_substep' ||
      reply.intent === 'readiness_substep',
    isReadinessFieldCorrection:
      context.isReadinessFieldCorrection ||
      reply.responseMode === 'readiness_field_correction' ||
      reply.responseMode === 'readiness_substep_update' ||
      reply.conversationMode === 'readiness_field_correction' ||
      reply.conversationMode === 'readiness_substep_update' ||
      reply.intent === 'readiness_field_correction' ||
      reply.intent === 'readiness_substep_update',
    selectedReadinessItem:
      context.selectedReadinessItem || reply.selectedReadinessItem || null,
    activeReadinessItemId:
      context.activeReadinessItemId ||
      reply.activeReadinessItemId ||
      reply.readinessItemId ||
      (reply.slots && reply.slots.activeReadinessItemId) ||
      null,
    slots: context.slots || reply.slots || null,
    isClarificationNeeded:
      context.isClarificationNeeded ||
      reply.responseMode === 'clarification_needed' ||
      reply.conversationMode === 'clarification_needed' ||
      reply.intent === 'clarification_needed',
    isExecutionRequest:
      context.isExecutionRequest ||
      reply.responseMode === 'execution_confirmation',
    step: reply.step || reply.planningState,
    intent: reply.intent,
  });

  const mode =
    context.forceMode ||
    selectConversationMode({
      ...context,
      assessment,
      responseMode: reply.responseMode,
    });

  const next = {
    ...reply,
    conversationMode: mode,
    responseMode: toResponseMode(mode) || reply.responseMode,
  };

  // Formal review gates keep their card body unless already approved.
  if (
    mode === CONVERSATION_MODES.FORMAL_REVIEW_GATE &&
    !assessment.gateAlreadyApproved
  ) {
    return next;
  }

  // Operator state updates + campaign-ready summaries: enforce dedupe.
  if (
    (mode === CONVERSATION_MODES.OPERATOR_STATE_UPDATE ||
      mode === CONVERSATION_MODES.CAMPAIGN_READY_SUMMARY) &&
    typeof next.message === 'string'
  ) {
    next.message = dedupeOperatorStateUpdateMessage(next.message);
  }

  // Revision / diagnostic / execution: strip accidental renderer headings
  // when the mode forbids them and body was provided by workflow.
  if (
    mode !== CONVERSATION_MODES.FORMAL_REVIEW_GATE &&
    containsRendererBoilerplate(next.message) &&
    context.allowRewrite !== false
  ) {
    // Do not silently delete the whole message — flag for callers.
    next.containsForbiddenRendererCopy = true;
  }

  return next;
}

/**
 * Bridge: enhance legacy selectResponseMode result with conversation mode.
 */
function selectResponseModeWithPolicy(opts = {}) {
  const assessment = assessConversationContext(opts);
  const conversationMode = selectConversationMode({
    ...opts,
    assessment,
  });
  return {
    conversationMode,
    responseMode: toResponseMode(conversationMode),
    assessment,
  };
}

module.exports = {
  CONVERSATION_MODES,
  CONVERSATION_MODE_TO_RESPONSE_MODE,
  RESPONSE_MODE_TO_CONVERSATION_MODE,
  RENDERER_SECTION_PATTERNS,
  FULL_SAFETY_LINES,
  DEFAULT_NEXT_PATHS,
  DEFAULT_UNRESOLVED_READINESS_ITEMS,
  READINESS_CHECKLIST_CLOSING_ASK,
  READINESS_CHECKLIST_SAFETY_LINE,
  CLARIFICATION_NEEDED_ASK,
  KNOWN_SHORT_OPERATOR_INTENTS,
  READINESS_SUBSTEPS,
  READINESS_SUBSTEP_SAFETY_LINE,
  READINESS_NEXT_ITEM_PROMPTS,
  OPERATIONAL_PATH_CHOICES,
  toConversationMode,
  toResponseMode,
  containsRendererBoilerplate,
  compactSafetyLockLine,
  expandedSafetyBlock,
  approvalLanguageForGate,
  assessConversationContext,
  looksLikeLowSignalAmbiguousInput,
  looksLikeNonExecutionIntent,
  looksLikeCampaignReadySummary,
  looksLikeOperatorReadinessCheck,
  looksLikeReadinessSubstepSelection,
  detectSelectedReadinessItem,
  looksLikeReadinessFieldCorrection,
  parseSenderIdentityFields,
  parseReplyHandlingFields,
  parseOperationalPathSelection,
  parseFollowUpTrackingFields,
  parseReplyMonitoringBatchReviewFields,
  mergeSenderIdentityState,
  mergeReplyHandlingState,
  mergeOperationalPathState,
  mergeFollowUpTrackingState,
  mergeReplyMonitoringBatchReviewState,
  normalizeOperationalPathChoice,
  normalizeReadinessItemId,
  resolveActiveReadinessItemId,
  isReplyMonitoringBatchReviewItemId,
  nextReadinessItemAfter,
  looksLikeExecutionRequest,
  selectConversationMode,
  composeConversationResponse,
  composeOperatorStateUpdate,
  composeOperatorRevisionResponse,
  composeOperatorDiagnostic,
  composeFormalReviewGate,
  composeCampaignReadySummary,
  composeOperatorReadinessCheck,
  composeReadinessSubstep,
  composeReadinessFieldCorrection,
  composeExecutionConfirmation,
  composeClarificationNeeded,
  extractOperatorReadinessChecklist,
  mergeOperatorReadinessChecklist,
  evaluateReadinessItemAgainstState,
  unresolvedReadinessItems,
  isSenderFieldValueLine,
  isReplyFieldValueLine,
  isOperationalPathValueLine,
  isFollowUpTrackingValueLine,
  isReplyMonitoringBatchReviewValueLine,
  resolveSenderIdentityFromContext,
  resolveReplyHandlingFromContext,
  resolveOperationalPathFromContext,
  resolveFollowUpTrackingFromContext,
  resolveReplyMonitoringBatchReviewFromContext,
  formatApprovedLaunchGateConversational,
  formatOperatorDiagnosticMessage,
  applyConversationalPolicy,
  selectResponseModeWithPolicy,
  CAMPAIGN_READY_SUMMARY_SAFETY_LINE,
  CAMPAIGN_READY_SUMMARY_CLOSING,
  resolveBatch1ScopeFromContext,
  resolveBatch1ScopeDetailsFromContext,
  nextExecuteActionForOperationalPath,
  normalizeNextPathPhrase,
  sanitizeApprovedStateLeadIn,
  dedupeOperatorStateUpdateMessage,
  getConfirmedReadinessRecords,
  buildConfirmedReadinessRecords,
};
