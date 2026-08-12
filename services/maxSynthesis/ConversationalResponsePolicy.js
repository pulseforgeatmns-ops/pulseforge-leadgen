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
 *   5. execution_confirmation     — about to send/export/CRM/account action
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
  EXECUTION_CONFIRMATION: 'execution_confirmation',
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
  [CONVERSATION_MODES.EXECUTION_CONFIRMATION]:
    RESPONSE_MODES.EXECUTION_CONFIRMATION || 'execution_confirmation',
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
  execution_confirmation: CONVERSATION_MODES.EXECUTION_CONFIRMATION,
});

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
  const isExecutionRequest = Boolean(
    input.isExecutionRequest ||
      input.executionPending ||
      looksLikeExecutionRequest(operatorMessage)
  );

  const shortestUseful =
    isDiagnostic
      ? 'plain_language_problem_then_detail'
      : stateChanged || gateAlreadyApproved
        ? 'state_then_next_options'
        : isRevision
          ? 'ack_change_artifact'
          : isExecutionRequest
            ? 'explicit_execute_confirm'
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
    isExecutionRequest,
    shortestUseful,
    mustBlock,
    nextAsk: input.nextAsk || null,
    priorityOrder: PRIORITY_ORDER,
  };
}

function looksLikeExecutionRequest(text) {
  const s = String(text || '').trim();
  if (!s) return false;
  if (
    /\b(?:send|sends|sending)\b[\s\S]{0,40}\b(?:now|batch|emails?|campaign)\b/i.test(
      s
    )
  ) {
    return true;
  }
  if (
    /\b(?:export|crm\s+write|write\s+to\s+crm|create\s+crm\s+drafts?|queue\s+sends?|schedule\s+(?:the\s+)?(?:send|launch)|execute\s+(?:send|export|crm|launch))\b/i.test(
      s
    )
  ) {
    return true;
  }
  if (
    /\bapprove\s+(?:the\s+)?(?:send|export|crm|execute|launch\s+execute)\b/i.test(
      s
    )
  ) {
    return true;
  }
  if (/\bprepare\s+(?:a\s+)?manual-?send\s+export\b/i.test(s)) {
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
    case CONVERSATION_MODES.EXECUTION_CONFIRMATION:
      return composeExecutionConfirmation(context);
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
  const gate = gateDisplayName(context);
  const shortGate = /launch\s*gate/i.test(gate) ? 'Launch Gate' : gate;
  const already = Boolean(
    context.gateAlreadyApproved && !context.stateChanged && !context.justApproved
  );
  const nextPaths = context.nextPaths || DEFAULT_NEXT_PATHS;
  const guidance =
    context.operatorGuidance ||
    "I'd keep this held until sender identity and reply handling are confirmed.";

  const lines = [];

  if (already && context.leadIn) {
    lines.push(String(context.leadIn).trim());
    lines.push('');
  }

  if (already) {
    lines.push(`${shortGate} is approved for readiness only.`);
  } else {
    lines.push(`${shortGate} is now approved for readiness only.`);
  }

  lines.push('');
  lines.push(
    context.safetyLine ||
      'Nothing external happened: no send, no export, no CRM write, and no account changes. The campaign is now campaign-ready, but execution is still locked.'
  );
  lines.push('');
  lines.push('The next choice is operational:');
  nextPaths.forEach((p, i) => {
    lines.push(`${i + 1}. ${p}`);
  });
  lines.push('');
  lines.push(guidance);

  if (context.closingAsk) {
    lines.push('');
    lines.push(context.closingAsk);
  } else {
    const lang = approvalLanguageForGate({
      gateName: shortGate,
      approved: true,
    });
    lines.push('');
    lines.push(lang.ask);
  }

  return {
    mode: CONVERSATION_MODES.OPERATOR_STATE_UPDATE,
    responseMode: toResponseMode(CONVERSATION_MODES.OPERATOR_STATE_UPDATE),
    message: lines.join('\n').trim(),
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
 * Format an approved Launch Gate as an Operator State Update.
 * Replaces rigid “Confirmed not executed / Next options” renderer copy.
 */
function formatApprovedLaunchGateConversational(gate, opts = {}) {
  const g = gate || {};
  const summary = g.operatorStateSummary || {};
  const justApproved = opts.justApproved === true;
  const alreadyApproved =
    opts.gateAlreadyApproved === true ||
    (!justApproved &&
      (g.launchGateApproved === true ||
        g.status === 'approved_readiness_only' ||
        g.approved === true));

  const nextPaths =
    opts.nextPaths ||
    (Array.isArray(summary.nextOptions) && summary.nextOptions.length
      ? summary.nextOptions.map(normalizeNextPathPhrase)
      : DEFAULT_NEXT_PATHS.slice());

  const shortGate = 'Launch Gate';
  const lines = [];

  // Stable headline for clients/tests.
  lines.push('Outreach Launch Gate: approved for readiness only.');
  lines.push('');

  if (opts.leadIn) {
    lines.push(String(opts.leadIn).trim());
    lines.push('');
  }

  if (justApproved) {
    lines.push(`${shortGate} is now approved for readiness only.`);
  } else {
    lines.push(`${shortGate} is approved for readiness only.`);
  }

  lines.push('');
  lines.push(
    opts.safetyLine ||
      'Nothing external happened: no send, no export, no CRM write, and no account changes. The campaign is now campaign-ready, but execution is still locked.'
  );
  lines.push('');
  lines.push('The next choice is operational:');
  nextPaths.forEach((p, i) => {
    lines.push(`${i + 1}. ${p}`);
  });
  lines.push('');
  lines.push(
    opts.operatorGuidance ||
      "I'd keep this held until sender identity and reply handling are confirmed."
  );
  lines.push('');
  lines.push(
    opts.closingAsk ||
      opts.currentAsk ||
      approvalLanguageForGate({ gateName: shortGate, approved: true }).ask
  );

  void alreadyApproved;

  return {
    mode: CONVERSATION_MODES.OPERATOR_STATE_UPDATE,
    responseMode: toResponseMode(CONVERSATION_MODES.OPERATOR_STATE_UPDATE),
    message: lines.join('\n').trim(),
    includeRendererSections: false,
    includeExpandedSafety: false,
  };
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
  toConversationMode,
  toResponseMode,
  containsRendererBoilerplate,
  compactSafetyLockLine,
  expandedSafetyBlock,
  approvalLanguageForGate,
  assessConversationContext,
  looksLikeExecutionRequest,
  selectConversationMode,
  composeConversationResponse,
  composeOperatorStateUpdate,
  composeOperatorRevisionResponse,
  composeOperatorDiagnostic,
  composeFormalReviewGate,
  composeExecutionConfirmation,
  formatApprovedLaunchGateConversational,
  formatOperatorDiagnosticMessage,
  applyConversationalPolicy,
  selectResponseModeWithPolicy,
  normalizeNextPathPhrase,
};
