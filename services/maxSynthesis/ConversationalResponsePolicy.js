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
 *   5. operator_readiness_check   — summarize / resolve remaining readiness gaps
 *   6. execution_confirmation     — about to send/export/CRM/account action
 *
 * Classification priority for post-approval turns:
 * discussing options / resolving readiness / selecting a path later
 * ALWAYS beat bare mentions of export/CRM/queue. Execution confirmation
 * only when the operator clearly asks to take or approve a concrete action.
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
  OPERATOR_READINESS_CHECK: 'operator_readiness_check',
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
  [CONVERSATION_MODES.OPERATOR_READINESS_CHECK]:
    RESPONSE_MODES.OPERATOR_READINESS_CHECK || 'operator_readiness_check',
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
  operator_readiness_check: CONVERSATION_MODES.OPERATOR_READINESS_CHECK,
  execution_confirmation: CONVERSATION_MODES.EXECUTION_CONFIRMATION,
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
  const readinessFromText = looksLikeOperatorReadinessCheck(operatorMessage);
  const nonExecutionFromText = looksLikeNonExecutionIntent(operatorMessage);
  const executionFromText = looksLikeExecutionRequest(operatorMessage);

  // Explicit flags still lose to clear readiness / planning language in the
  // operator message — bare option names must never force execute confirm.
  const isReadinessCheck = Boolean(
    input.isReadinessCheck ||
      input.forceReadinessCheck ||
      readinessFromText
  );
  const isNonExecutionIntent = Boolean(
    isReadinessCheck ||
      input.isNonExecutionIntent ||
      nonExecutionFromText
  );
  const isExecutionRequest = Boolean(
    !isNonExecutionIntent &&
      (input.isExecutionRequest ||
        input.executionPending ||
        executionFromText)
  );

  const shortestUseful =
    isDiagnostic
      ? 'plain_language_problem_then_detail'
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
  if (/\b(?:summarize|summarise)\b/i.test(s)) return true;
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
 * Operator wants unresolved readiness gaps summarized / resolved — not execute.
 */
function looksLikeOperatorReadinessCheck(text) {
  const s = String(text || '').trim();
  if (!s) return false;

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
    /\b(?:sender|reply|readiness|identity)\b/i.test(s)
  ) {
    return true;
  }

  return false;
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
    case CONVERSATION_MODES.OPERATOR_READINESS_CHECK:
      return composeOperatorReadinessCheck(context);
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
    // Skip instructional lines that are not checklist items.
    if (
      /^(?:before\s+choosing|please\s+summarize|help\s+me|still\s+unresolved|which\s+readiness|nothing\s+external)\b/i.test(
        item
      )
    ) {
      return;
    }
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

function readinessConceptId(item) {
  const s = String(item || '');
  for (const concept of READINESS_CONCEPT_PATTERNS) {
    if (concept.match.test(s)) return concept.id;
  }
  return `custom:${s.toLowerCase().slice(0, 80)}`;
}

/**
 * Known readiness facts from gate / slots / campaign memory.
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
  const confirmed = {
    ...(summary.confirmedReadiness || {}),
    ...(context.confirmedReadiness || {}),
    ...(memory.confirmed_readiness || {}),
  };

  return {
    senderIdentityConfirmed: Boolean(
      confirmed.sender_identity ||
        confirmed.senderIdentity ||
        slots.senderIdentityConfirmed ||
        context.senderIdentityConfirmed
    ),
    replyInboxConfirmed: Boolean(
      confirmed.reply_inbox_handling ||
        confirmed.replyInboxHandling ||
        slots.replyInboxConfirmed ||
        context.replyInboxConfirmed
    ),
    replyMonitoringConfirmed: Boolean(
      confirmed.reply_monitoring_owner ||
        confirmed.replyMonitoringOwner ||
        slots.replyMonitoringConfirmed ||
        context.replyMonitoringConfirmed
    ),
    operationalPathChosen: Boolean(
      confirmed.operational_path ||
        confirmed.operationalPath ||
        slots.operationalPathChosen ||
        context.operationalPathChosen
    ),
    followUpTrackingConfirmed: Boolean(
      confirmed.follow_up_tracking_process ||
        confirmed.followUpTrackingProcess ||
        slots.followUpTrackingConfirmed ||
        context.followUpTrackingConfirmed
    ),
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
        context.batch1ResultsReviewed
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

  const confirmedByConcept = {
    sender_identity: state.senderIdentityConfirmed,
    reply_inbox_handling: state.replyInboxConfirmed,
    reply_handling_generic: state.replyInboxConfirmed,
    reply_monitoring_owner: state.replyMonitoringConfirmed,
    operational_path: state.operationalPathChosen,
    follow_up_tracking_process: state.followUpTrackingConfirmed,
    tracking_account_settings: state.trackingAccountApproved,
    broader_rollout_batch1: state.batch1ResultsReviewed,
  };

  if (confirmedByConcept[concept] === true) {
    return {
      text,
      status: 'resolved',
      reason: 'already confirmed in campaign state',
      concept,
      display: `${text} — already confirmed in campaign state`,
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
 * Operator-specified items win for wording and must not be collapsed into
 * the default template. Distinct concepts stay distinct.
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

  const usedConcepts = new Set();
  const merged = [];

  for (const item of sourceItems) {
    const evaluated = evaluateReadinessItemAgainstState(item, context);
    // Preserve distinct concepts — never collapse reply-to vs monitoring, etc.
    if (usedConcepts.has(evaluated.concept) && !evaluated.concept.startsWith('custom:')) {
      // Same concept repeated with different wording: keep first (operator order).
      continue;
    }
    usedConcepts.add(evaluated.concept);
    merged.push(evaluated.display);
  }

  return {
    items: merged,
    operatorSpecified: operatorItems.length > 0,
    evaluations: sourceItems.map((item) =>
      evaluateReadinessItemAgainstState(item, context)
    ),
  };
}

/**
 * Resolve unresolved readiness items from context / gate / operator message.
 */
function unresolvedReadinessItems(context = {}) {
  return mergeOperatorReadinessChecklist(context).items;
}

/**
 * Readiness check — list unresolved items only; never ask for execute approval.
 * Preserves operator-specified checklist items unless they conflict with known
 * state or safety rules.
 */
function composeOperatorReadinessCheck(context = {}) {
  const merged = mergeOperatorReadinessChecklist(context);
  const items = merged.items;
  const closingAsk =
    context.closingAsk ||
    context.closingQuestion ||
    context.currentAsk ||
    READINESS_CHECKLIST_CLOSING_ASK;
  const safetyLine =
    context.safetyLine ||
    context.compactSafety ||
    READINESS_CHECKLIST_SAFETY_LINE;

  const lines = [
    context.leadIn ||
      'Still unresolved before any export, CRM drafts, or queued sends:',
    '',
    ...items.map((item) => `- ${item}`),
    '',
    safetyLine,
    '',
    closingAsk,
  ];

  const message = lines.join('\n').trim();
  return {
    mode: CONVERSATION_MODES.OPERATOR_READINESS_CHECK,
    responseMode: toResponseMode(CONVERSATION_MODES.OPERATOR_READINESS_CHECK),
    message,
    includeRendererSections: false,
    includeExpandedSafety: false,
    unresolvedItems: items,
    operatorSpecifiedChecklist: merged.operatorSpecified,
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
 * Dedupe pass for operator_state_update messages.
 * Keeps first occurrence of status / safety / lock statements and collapses
 * repeated blank lines.
 */
function dedupeOperatorStateUpdateMessage(text) {
  const raw = String(text || '').trim();
  if (!raw) return raw;

  const seen = {
    approved: false,
    nothingExternal: false,
    executionLocked: false,
    nextChoice: false,
    guidance: false,
    nextPathAsk: false,
  };

  const out = [];
  const paragraphs = raw.split(/\n{2,}/);

  for (const para of paragraphs) {
    const p = String(para || '').trim();
    if (!p) continue;

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

  // Operator state updates: enforce single acknowledgment / safety / lock.
  if (
    mode === CONVERSATION_MODES.OPERATOR_STATE_UPDATE &&
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
  toConversationMode,
  toResponseMode,
  containsRendererBoilerplate,
  compactSafetyLockLine,
  expandedSafetyBlock,
  approvalLanguageForGate,
  assessConversationContext,
  looksLikeNonExecutionIntent,
  looksLikeOperatorReadinessCheck,
  looksLikeExecutionRequest,
  selectConversationMode,
  composeConversationResponse,
  composeOperatorStateUpdate,
  composeOperatorRevisionResponse,
  composeOperatorDiagnostic,
  composeFormalReviewGate,
  composeOperatorReadinessCheck,
  composeExecutionConfirmation,
  extractOperatorReadinessChecklist,
  mergeOperatorReadinessChecklist,
  evaluateReadinessItemAgainstState,
  unresolvedReadinessItems,
  formatApprovedLaunchGateConversational,
  formatOperatorDiagnosticMessage,
  applyConversationalPolicy,
  selectResponseModeWithPolicy,
  normalizeNextPathPhrase,
  sanitizeApprovedStateLeadIn,
  dedupeOperatorStateUpdateMessage,
};
