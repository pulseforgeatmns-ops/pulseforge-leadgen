'use strict';

/**
 * Max Chat Responsiveness — operator instruction precedence for campaign chat.
 *
 * Priority (highest → lowest):
 *   1. System and safety rules
 *   2. Latest operator instruction
 *   3. Active workflow state
 *   4. Shared Campaign Memory
 *   5. Approved campaign artifacts
 *   6. Evidence / source records
 *   7. Default templates / renderers
 *
 * Lower-priority sources must not overwrite higher-priority instructions.
 */

const RESPONSE_MODES = Object.freeze({
  WORKFLOW_REVIEW_CARD: 'workflow_review_card',
  OPERATOR_CHAT_RESPONSE: 'operator_chat_response',
  OPERATOR_STATE_SUMMARY: 'operator_state_summary',
  STALE_SOURCE_DIAGNOSTIC: 'stale_source_diagnostic',
  /** Explicit send/export/CRM/account confirmation — never auto-execute. */
  EXECUTION_CONFIRMATION: 'execution_confirmation',
});

const PRIORITY_ORDER = Object.freeze([
  'system_safety',
  'latest_operator_instruction',
  'active_workflow_state',
  'shared_campaign_memory',
  'approved_campaign_artifacts',
  'evidence_source_records',
  'default_templates_renderers',
]);

const SUBJECT_MERGE_TOKEN_PATTERN =
  /\{\{\s*business_name\s*\}\}\s*-\s*commercial\s+cleaning/i;

const STREET_ADDRESS_RE =
  /\b(?:\d{1,6}\s+[A-Za-z0-9.'-]+(?:\s+(?:St|Street|Ave|Avenue|Rd|Road|Dr|Drive|Ln|Lane|Blvd|Boulevard|Way|Ct|Court|Cir|Circle|Hwy|Highway|Pkwy|Parkway)\.?)|(?:(?:use|reference|mention|include|personalize\s+(?:with|using|from)|recommend)\s+(?:the\s+)?(?:street|mailing|physical)\s+address))\b/i;

function nowIso() {
  return new Date().toISOString();
}

function emptyCampaignWorkingState() {
  return {
    activeArtifactKind: null,
    pendingInstructions: [],
    appliedDirectives: [],
    latestOperatorInstruction: null,
    latestOperatorInstructionAt: null,
    rejectedOutputFingerprints: [],
    lastResponseMode: null,
    lastStaleDiagnostic: null,
    awaitingForceRebuildConfirmation: false,
    bypassStoredOutreachDraftPreview: false,
    updatedAt: null,
  };
}

function ensureCampaignWorkingState(state = {}) {
  const existing =
    (state && state.campaignWorkingState) ||
    (state &&
      state.campaignPlanning &&
      state.campaignPlanning.campaignWorkingState) ||
    null;
  if (existing && typeof existing === 'object') {
    return {
      ...emptyCampaignWorkingState(),
      ...existing,
      pendingInstructions: [...(existing.pendingInstructions || [])],
      appliedDirectives: [...(existing.appliedDirectives || [])],
      rejectedOutputFingerprints: [
        ...(existing.rejectedOutputFingerprints || []),
      ],
    };
  }
  return emptyCampaignWorkingState();
}

/**
 * Detect revision / correction / diagnostic asks against an active workflow
 * artifact (especially Outreach Draft Preview).
 */
function looksLikeOperatorWorkflowRevision(text, opts = {}) {
  const s = String(text || '').trim();
  if (!s) return false;

  // Force-rebuild confirmation is its own path — do not treat as a soft revise.
  if (looksLikeForceRebuildConfirmation(s, opts)) return false;

  const step = String(opts.step || opts.currentStep || '').toLowerCase();
  const onDraft =
    /outreach_draft_preview/.test(step) ||
    Boolean(opts.priorOutreachDraftPreview || opts.outreachDraftPreview);

  if (
    /\brevise\b[\s\S]{0,80}\boutreach\s+draft(?:\s+preview)?\b/i.test(s) ||
    /\boutreach\s+draft(?:\s+preview)?\b[\s\S]{0,80}\brevise\b/i.test(s)
  ) {
    return true;
  }

  if (
    onDraft &&
    (/\b(?:no street addresses?|draft actual follow-?ups?|answer like an (?:llm|operator)|not a workflow renderer|use\s+\{\{\s*business_name\s*\}\})\b/i.test(
      s
    ) ||
      SUBJECT_MERGE_TOKEN_PATTERN.test(s))
  ) {
    return true;
  }

  if (
    onDraft &&
    (opts.messageClass === 'correction' ||
      opts.messageClass === 'refinement_feedback' ||
      opts.messageClass === 'add_on') &&
    /\b(?:subject|follow-?up|street address|personalization|tested winner|view evidence|primary actions|draft)\b/i.test(
      s
    )
  ) {
    return true;
  }

  if (
    onDraft &&
    /\b(?:fix|correct|change|update|rewrite|redo|rework)\b/i.test(s) &&
    /\b(?:subject|follow-?up|street|draft|preview|copy|wording|phrasing)\b/i.test(
      s
    )
  ) {
    return true;
  }

  return false;
}

/**
 * True when the operator confirms force-rebuild from operator instructions
 * only — after a stale-source diagnostic. Must mutate the execution path,
 * never re-ask for confirmation.
 */
function looksLikeForceRebuildConfirmation(text, opts = {}) {
  const s = String(text || '').trim();
  if (!s) return false;

  const working = ensureCampaignWorkingState({
    campaignWorkingState: opts.campaignWorkingState || null,
  });
  const awaiting =
    working.awaitingForceRebuildConfirmation === true ||
    working.lastResponseMode === RESPONSE_MODES.STALE_SOURCE_DIAGNOSTIC ||
    opts.awaitingForceRebuildConfirmation === true ||
    opts.lastResponseMode === RESPONSE_MODES.STALE_SOURCE_DIAGNOSTIC;

  const explicitForce =
    /\bforce[-\s]?rebuild\b/i.test(s) ||
    /\bbypass(?:ing)?\s+(?:the\s+)?(?:stored\s+)?(?:artifact|draft|outreach_draft_preview)\b/i.test(
      s
    ) ||
    /\bfrom\s+operator\s+instructions?\s+only\b/i.test(s) ||
    /\boperator\s+instructions?\s+only\b/i.test(s);

  if (explicitForce) return true;

  // Short affirmations only count while awaiting force-rebuild confirmation.
  if (
    awaiting &&
    /^(?:yes|y|yeah|yep|ok|okay|sure|do\s+it|proceed|confirm(?:ed)?|go\s+ahead|force\s+it)(?:[.!]|\s|$)/i.test(
      s
    )
  ) {
    return true;
  }

  return false;
}

/**
 * Parse free-form operator chat into durable learnings + working directives.
 */
function parseOperatorChatDirectives(text) {
  const s = String(text || '').trim();
  const learnings = {};
  const directives = [];
  const changes = [];

  if (!s) {
    return { learnings, directives, changes, rawText: s };
  }

  // Subject pattern — prefer literal merge-token form when present.
  const subjectMatch = s.match(
    /(?:use|subject(?:\s+line)?(?:\s+is|\s+should\s+be)?|pattern)\s*[:=]?\s*[`'"]?(\{\{\s*business_name\s*\}\}\s*-\s*commercial\s+cleaning)[`'"]?/i
  );
  const bareSubject = s.match(SUBJECT_MERGE_TOKEN_PATTERN);
  if (subjectMatch || bareSubject) {
    const pattern = (subjectMatch && subjectMatch[1]) || bareSubject[0];
    const normalized = pattern.replace(/\s+/g, ' ').replace(/\{\{\s+/g, '{{').replace(/\s+\}\}/g, '}}');
    const canonical = '{{business_name}} - commercial cleaning';
    learnings.tested_subject_line_pattern = canonical;
    learnings.subject_keep_merge_tokens = true;
    learnings.claim_tested_winner = false;
    directives.push({
      type: 'subject_pattern',
      value: canonical,
      source: 'operator_chat',
    });
    changes.push(`subject → ${canonical}`);
  }

  if (/\bno street addresses?\b/i.test(s)) {
    learnings.personalization_rule = 'do not use street addresses by default';
    learnings.personalization_preference =
      'use town, company, property type, portfolio cue, or public role signal';
    directives.push({
      type: 'no_street_addresses',
      value: true,
      source: 'operator_chat',
    });
    changes.push('no street addresses in personalization');
  }

  if (
    /\bdraft actual follow-?ups?\b/i.test(s) ||
    /\bfollow-?ups?\s+(?:as\s+)?(?:drafted\s+)?emails?\b/i.test(s) ||
    /\b(?:draft|write|include)\s+(?:actual\s+)?follow-?ups?\b/i.test(s)
  ) {
    learnings.draft_follow_ups = true;
    learnings.follow_up_mode = 'drafted_emails';
    directives.push({
      type: 'draft_follow_ups',
      value: true,
      source: 'operator_chat',
    });
    changes.push('draft actual Follow-up 1 and Follow-up 2 emails');
  }

  if (
    /\banswer like an (?:llm|operator)\b/i.test(s) ||
    /\bnot a workflow renderer\b/i.test(s) ||
    /\bconversational\b/i.test(s)
  ) {
    learnings.response_mode_preference = RESPONSE_MODES.OPERATOR_CHAT_RESPONSE;
    directives.push({
      type: 'response_mode',
      value: RESPONSE_MODES.OPERATOR_CHAT_RESPONSE,
      source: 'operator_chat',
    });
    changes.push('respond conversationally (not as a workflow card)');
  }

  if (
    /\bno\s+(?:unsupported\s+)?(?:["']?tested winner["']?|tested-winner)\b/i.test(
      s
    ) ||
    /\bdon'?t\s+(?:claim|say|use)\b[\s\S]{0,40}\btested winner\b/i.test(s)
  ) {
    learnings.claim_tested_winner = false;
    directives.push({
      type: 'no_tested_winner_claim',
      value: true,
      source: 'operator_chat',
    });
    changes.push('drop unsupported “tested winner” language');
  }

  if (/\bAnchor helps\b/i.test(s) || /\bsay\s+["']?Anchor helps/i.test(s)) {
    learnings.copy_voice_opener = 'company_helps';
    directives.push({
      type: 'company_helps_voice',
      value: true,
      source: 'operator_chat',
    });
    changes.push('use “Anchor helps…” company voice');
  }

  if (/\buse\s+\{\{\s*town\s*\}\}/i.test(s) || /\b\{\{\s*town\s*\}\}/i.test(s)) {
    learnings.use_town_token = true;
    directives.push({
      type: 'use_town_token',
      value: true,
      source: 'operator_chat',
    });
    changes.push('keep {{town}} token in body');
  }

  if (
    /\bno\s+(?:sends?|exports?|crm\s+writes?|account\s+changes?)\b/i.test(s)
  ) {
    directives.push({
      type: 'no_side_effects',
      value: true,
      source: 'operator_chat',
    });
  }

  return {
    learnings,
    directives,
    changes,
    rawText: s,
    hasDirectives: directives.length > 0,
  };
}

function applyOperatorDirectivesToWorkingState(workingState, parsed, opts = {}) {
  const next = ensureCampaignWorkingState({
    campaignWorkingState: workingState,
  });
  const at = nowIso();
  next.latestOperatorInstruction = parsed.rawText || null;
  next.latestOperatorInstructionAt = at;
  next.activeArtifactKind =
    opts.activeArtifactKind || next.activeArtifactKind || null;
  if (parsed.directives && parsed.directives.length) {
    next.pendingInstructions = [
      ...next.pendingInstructions,
      {
        at,
        text: parsed.rawText,
        directives: parsed.directives,
        changes: parsed.changes || [],
      },
    ].slice(-20);
    next.appliedDirectives = [
      ...next.appliedDirectives,
      ...parsed.directives.map((d) => ({ ...d, at })),
    ].slice(-50);
  }
  next.updatedAt = at;
  return next;
}

function markDirectivesApplied(workingState, fingerprint) {
  const next = ensureCampaignWorkingState({
    campaignWorkingState: workingState,
  });
  next.pendingInstructions = [];
  if (fingerprint) {
    next.rejectedOutputFingerprints = [
      ...next.rejectedOutputFingerprints,
      fingerprint,
    ].slice(-10);
  }
  next.updatedAt = nowIso();
  return next;
}

function recordRejectedOutput(workingState, fingerprint) {
  const next = ensureCampaignWorkingState({
    campaignWorkingState: workingState,
  });
  if (!fingerprint) return next;
  next.rejectedOutputFingerprints = [
    ...next.rejectedOutputFingerprints,
    fingerprint,
  ].slice(-10);
  next.updatedAt = nowIso();
  return next;
}

function countRejectedFingerprint(workingState, fingerprint) {
  const fps = (workingState && workingState.rejectedOutputFingerprints) || [];
  return fps.filter((f) => f === fingerprint).length;
}

/**
 * Select Workflow Review Card vs Operator Chat Response.
 * Prefer conversation-policy selection when available (post workflow/state).
 */
function selectResponseMode(opts = {}) {
  // Lazy require avoids circular init with ConversationalResponsePolicy.
  let policySelect = null;
  try {
    policySelect =
      require('./ConversationalResponsePolicy').selectResponseModeWithPolicy;
  } catch (_err) {
    policySelect = null;
  }

  const parsed = opts.parsedDirectives || parseOperatorChatDirectives(opts.text);
  const learnings = opts.learnings || {};
  const messageClass = opts.messageClass || null;
  const isRevision =
    opts.forceOperatorChat ||
    looksLikeOperatorWorkflowRevision(opts.text, opts) ||
    parsed.hasDirectives ||
    learnings.response_mode_preference ===
      RESPONSE_MODES.OPERATOR_CHAT_RESPONSE;

  if (opts.staleDiagnosticRequired) {
    return RESPONSE_MODES.STALE_SOURCE_DIAGNOSTIC;
  }

  if (opts.executionPending === true || opts.isExecutionRequest === true) {
    return RESPONSE_MODES.EXECUTION_CONFIRMATION;
  }

  if (
    isRevision ||
    messageClass === 'correction' ||
    messageClass === 'refinement_feedback' ||
    messageClass === 'clarification_request'
  ) {
    return RESPONSE_MODES.OPERATOR_CHAT_RESPONSE;
  }

  // Already-approved readiness must never re-open a formal review card.
  if (
    opts.intent === 'outreach_launch_gate_approved' ||
    opts.launchGateApproved === true ||
    opts.approvedReadinessOnly === true
  ) {
    return RESPONSE_MODES.OPERATOR_STATE_SUMMARY;
  }

  // Initial structured review gates keep the digest card.
  if (
    opts.isInitialReviewGate ||
    opts.intent === 'produce_outreach_draft_preview' ||
    opts.intent === 'outreach_copy_plan_approved' ||
    opts.intent === 'produce_outreach_launch_gate' ||
    opts.intent === 'show_outreach_launch_gate' ||
    opts.intent === 'outreach_draft_preview_approved'
  ) {
    return RESPONSE_MODES.WORKFLOW_REVIEW_CARD;
  }

  if (typeof policySelect === 'function') {
    const selected = policySelect({
      ...opts,
      parsedDirectives: parsed,
      isRevision,
      messageClass,
    });
    if (selected && selected.responseMode) return selected.responseMode;
  }

  return RESPONSE_MODES.WORKFLOW_REVIEW_CARD;
}

function draftOutputFingerprint(preview) {
  if (!preview || typeof preview !== 'object') return 'empty';
  const subjects = (preview.subjectOptions || []).join('|');
  const body = String(preview.firstTouchBody || '').slice(0, 240);
  const follow =
    (preview.followUpDrafts &&
      preview.followUpDrafts.map((f) => f.subject || f.body || '').join('|')) ||
    (preview.followUpSketch || []).join('|');
  const hasDigest = Boolean(preview.operatorDigest);
  const hasStreet = STREET_ADDRESS_RE.test(
    JSON.stringify(preview.personalizationByProspect || [])
  );
  return [
    subjects,
    body.includes('{{town}}') ? 'town' : 'no-town',
    follow.slice(0, 120),
    hasDigest ? 'digest' : 'nodigest',
    hasStreet ? 'street' : 'nostreet',
  ].join('::');
}

/**
 * Pre-response validation for Anchor Outreach Draft Preview.
 * Returns { ok, failures[] }.
 */
function validateOutreachDraftAgainstInstructions(preview, opts = {}) {
  const failures = [];
  const learnings = opts.learnings || {};
  const p = preview || {};
  const subjects = Array.isArray(p.subjectOptions) ? p.subjectOptions : [];
  const body = String(p.firstTouchBody || (p.firstTouchDraft && p.firstTouchDraft.body) || '');
  const personalization = Array.isArray(p.personalizationByProspect)
    ? p.personalizationByProspect
    : [];
  const message = String(opts.message || '');

  const expectedSubject =
    learnings.tested_subject_line_pattern ||
    '{{business_name}} - commercial cleaning';
  const keepTokens =
    learnings.subject_keep_merge_tokens === true ||
    /\{\{/.test(expectedSubject);

  if (keepTokens) {
    const hasExact = subjects.some(
      (s) =>
        String(s).trim().toLowerCase() ===
        expectedSubject.trim().toLowerCase()
    );
    if (!hasExact) {
      failures.push({
        code: 'subject_not_merge_token',
        detail: `expected exact subject "${expectedSubject}"`,
      });
    }
    if (subjects.some((s) => /^Anchor\s*-\s*commercial cleaning$/i.test(String(s).trim()))) {
      failures.push({
        code: 'stale_expanded_subject',
        detail: 'stale expanded “Anchor - commercial cleaning” subject',
      });
    }
  }

  for (const row of personalization) {
    if (STREET_ADDRESS_RE.test(String(row.personalizationNote || ''))) {
      failures.push({
        code: 'street_address_personalization',
        detail: row.companyName || 'prospect',
      });
      break;
    }
  }
  if (STREET_ADDRESS_RE.test(body) || STREET_ADDRESS_RE.test(message)) {
    failures.push({ code: 'street_address_in_body', detail: 'body/message' });
  }

  if (!/\{\{\s*town\s*\}\}/i.test(body)) {
    failures.push({ code: 'missing_town_token', detail: 'first-touch body' });
  }

  if (!/\bhelps\b/i.test(body) && !/\bI work with\b/i.test(body)) {
    failures.push({ code: 'missing_company_helps_voice', detail: 'opener' });
  }
  if (/\bI work with\b/i.test(body) && learnings.copy_voice_opener !== 'first_person') {
    failures.push({ code: 'first_person_work_with', detail: 'opener' });
  }

  const diff = String(learnings.copy_differentiator || '');
  if (diff) {
    const needed = ['reliab', 'respons', 'accountab', 'vendor'];
    const lower = body.toLowerCase();
    // Soft check — body or follow-ups should lean on differentiators somewhere.
    const blob = `${body} ${JSON.stringify(p.followUpDrafts || p.followUpSketch || [])}`.toLowerCase();
    if (!needed.some((n) => blob.includes(n))) {
      failures.push({
        code: 'missing_differentiator_language',
        detail: diff,
      });
    }
  }

  if (!body || body.length < 40) {
    failures.push({ code: 'missing_first_touch_draft', detail: 'first-touch' });
  }

  const wantFollowUps =
    learnings.draft_follow_ups === true ||
    learnings.follow_up_mode === 'drafted_emails';
  if (wantFollowUps) {
    const drafts = Array.isArray(p.followUpDrafts) ? p.followUpDrafts : [];
    const hasFu1 = drafts.some(
      (d) => d && (d.step === 1 || /follow-?up\s*1/i.test(d.label || ''))
    );
    const hasFu2 = drafts.some(
      (d) => d && (d.step === 2 || /follow-?up\s*2/i.test(d.label || ''))
    );
    if (!hasFu1) {
      failures.push({ code: 'missing_follow_up_1_draft', detail: 'follow-up 1' });
    }
    if (!hasFu2) {
      failures.push({ code: 'missing_follow_up_2_draft', detail: 'follow-up 2' });
    }
    // Sketches alone are not enough when drafts were requested.
    if (
      (!drafts.length || drafts.length < 2) &&
      Array.isArray(p.followUpSketch) &&
      p.followUpSketch.some((line) => /restate the same CTA|close-the-loop/i.test(line))
    ) {
      failures.push({
        code: 'follow_ups_still_sketches',
        detail: 'follow-up sketch present instead of drafts',
      });
    }
  }

  if (learnings.claim_tested_winner === false) {
    if (/tested winner/i.test(message) || /tested winner/i.test(JSON.stringify(p.sectionTitles || {}))) {
      failures.push({
        code: 'unsupported_tested_winner_claim',
        detail: 'message/section titles',
      });
    }
  }

  if (
    opts.responseMode === RESPONSE_MODES.OPERATOR_CHAT_RESPONSE ||
    learnings.response_mode_preference === RESPONSE_MODES.OPERATOR_CHAT_RESPONSE
  ) {
    if (/\bView evidence\b/i.test(message) || /\bPrimary actions\b/i.test(message)) {
      failures.push({
        code: 'workflow_card_boilerplate',
        detail: 'View evidence / Primary actions',
      });
    }
    if (/\bRecommended decision\b/i.test(message)) {
      failures.push({
        code: 'workflow_card_recommended_decision',
        detail: 'Recommended decision',
      });
    }
  }

  if (
    p.sendsMade ||
    p.crmWritesMade ||
    p.exportMade ||
    p.accountChangesMade ||
    /\b(sent|exported|crm write|updating dns|changing gbp)\b/i.test(message)
  ) {
    failures.push({
      code: 'implied_side_effects',
      detail: 'send/export/CRM/account change',
    });
  }

  return { ok: failures.length === 0, failures };
}

function buildStaleSourceDiagnostic(opts = {}) {
  const working = ensureCampaignWorkingState({
    campaignWorkingState: opts.campaignWorkingState,
  });
  const learnings = opts.learnings || {};
  const parsed = opts.parsedDirectives || {};
  const responseMode = opts.responseMode || null;
  const failures = opts.validationFailures || [];
  const winningSource = opts.winningSource || 'unknown';
  const injectionSources = opts.injectionSources || [];

  const technicalLines = [
    'Stale source diagnostic:',
    `- Campaign memory retrieved: subject_pattern=${JSON.stringify(
      learnings.tested_subject_line_pattern || null
    )}; personalization_rule=${JSON.stringify(
      learnings.personalization_rule || null
    )}; draft_follow_ups=${JSON.stringify(
      learnings.draft_follow_ups || learnings.follow_up_mode || null
    )}; claim_tested_winner=${JSON.stringify(
      learnings.claim_tested_winner
    )}`,
    `- Active workflow artifact retrieved: ${
      opts.activeArtifactKind || working.activeArtifactKind || 'outreach_draft_preview'
    }${opts.storedArtifactPresent ? ' (stored draft present)' : ' (no stored draft)'}`,
    `- Latest operator instruction included in prompt context: ${
      working.latestOperatorInstruction
        ? 'yes — ' + JSON.stringify(working.latestOperatorInstruction).slice(0, 160)
        : 'no'
    }`,
    `- Response mode selected: ${responseMode || working.lastResponseMode || 'unset'}`,
    `- Source that won in final generation: ${winningSource}`,
    `- Why stale fields survived: ${(failures || [])
      .map((f) => f.code)
      .join(', ') || opts.staleReason || 'unknown'}`,
  ];

  if (injectionSources.length) {
    technicalLines.push(
      `- Exact stale injection sources: ${injectionSources
        .map((s) => `${s.source}.${s.field}=${JSON.stringify(s.value).slice(0, 80)}`)
        .join('; ')}`
    );
  }

  // Conversational diagnostic: plain language first, technical detail second.
  let message;
  try {
    const {
      formatOperatorDiagnosticMessage,
      CONVERSATION_MODES,
    } = require('./ConversationalResponsePolicy');
    const composed = formatOperatorDiagnosticMessage({
      plainLanguage:
        opts.plainLanguage ||
        'I found the problem: campaign memory was correct, but the stored draft renderer was still winning.',
      stopLine:
        opts.stopLine ||
        "I'm stopping before showing another stale draft.",
      technicalDetail: technicalLines.join('\n'),
      nextAction:
        opts.nextAction ||
        'Confirm whether to force-rebuild from operator instructions only (bypassing the stored artifact / workflow card renderer).',
    });
    message = composed.message;
    void CONVERSATION_MODES;
  } catch (_err) {
    message = [
      'I found the problem: campaign memory was correct, but the stored draft renderer was still winning.',
      '',
      "I'm stopping before showing another stale draft.",
      '',
      ...technicalLines,
      '',
      'Confirm whether to force-rebuild from operator instructions only (bypassing the stored artifact / workflow card renderer).',
    ].join('\n');
  }

  return {
    kind: 'stale_source_diagnostic',
    message,
    responseMode: RESPONSE_MODES.STALE_SOURCE_DIAGNOSTIC,
    conversationMode: 'operator_diagnostic',
    diagnostic: {
      campaignMemory: {
        tested_subject_line_pattern: learnings.tested_subject_line_pattern || null,
        personalization_rule: learnings.personalization_rule || null,
        draft_follow_ups: learnings.draft_follow_ups || null,
        claim_tested_winner: learnings.claim_tested_winner,
      },
      activeWorkflowArtifact: opts.activeArtifactKind || 'outreach_draft_preview',
      storedArtifactPresent: Boolean(opts.storedArtifactPresent),
      latestOperatorInstructionIncluded: Boolean(working.latestOperatorInstruction),
      latestOperatorInstruction: working.latestOperatorInstruction,
      responseMode,
      winningSource,
      validationFailures: failures,
      injectionSources,
      directivesParsed: parsed.directives || [],
      priorityOrder: PRIORITY_ORDER,
      awaitingForceRebuildConfirmation: true,
    },
    sendsMade: false,
    crmWritesMade: false,
    exportMade: false,
    accountChangesMade: false,
  };
}

/**
 * Name exact stale fields/sources in a draft or rendered message.
 * Used for fail-closed diagnostics — never re-show the stale draft.
 */
function identifyStaleInjectionSources(preview, message, learnings = {}) {
  const hits = [];
  const p = preview || {};
  const msg = String(message || '');
  const subjects = Array.isArray(p.subjectOptions) ? p.subjectOptions : [];
  const expected =
    learnings.tested_subject_line_pattern ||
    '{{business_name}} - commercial cleaning';

  for (const s of subjects) {
    if (/^Anchor\s*-\s*commercial cleaning$/i.test(String(s).trim())) {
      hits.push({
        source: 'stored_outreach_draft_preview',
        field: 'subjectOptions',
        value: s,
        reason: 'stale_expanded_subject',
      });
    }
    if (
      String(s).trim().toLowerCase() !== expected.trim().toLowerCase() &&
      learnings.subject_keep_merge_tokens !== false
    ) {
      hits.push({
        source: 'stored_outreach_draft_preview',
        field: 'subjectOptions',
        value: s,
        reason: 'subject_mismatch',
      });
    }
  }

  if (/Anchor\s*-\s*commercial cleaning/i.test(msg)) {
    hits.push({
      source: 'rendered_message',
      field: 'message',
      value: 'Anchor - commercial cleaning',
      reason: 'stale_expanded_subject_in_message',
    });
  }

  for (const row of p.personalizationByProspect || []) {
    if (STREET_ADDRESS_RE.test(String(row.personalizationNote || ''))) {
      hits.push({
        source: 'stored_outreach_draft_preview',
        field: `personalizationByProspect[${row.companyName || '?'}]`,
        value: row.personalizationNote,
        reason: 'street_address_personalization',
      });
    }
  }
  if (STREET_ADDRESS_RE.test(msg)) {
    hits.push({
      source: 'rendered_message',
      field: 'message',
      value: 'street_address_pattern',
      reason: 'street_address_in_message',
    });
  }

  if (
    (!Array.isArray(p.followUpDrafts) || p.followUpDrafts.length < 2) &&
    Array.isArray(p.followUpSketch) &&
    p.followUpSketch.some((line) =>
      /restate the same CTA|close-the-loop|sketch/i.test(String(line))
    )
  ) {
    hits.push({
      source: 'stored_outreach_draft_preview',
      field: 'followUpSketch',
      value: p.followUpSketch[0],
      reason: 'follow_ups_still_sketches',
    });
  }

  if (p.operatorDigest) {
    hits.push({
      source: 'workflow_card_renderer',
      field: 'operatorDigest',
      value: p.operatorDigest.kind || 'digest',
      reason: 'workflow_card_present',
    });
  }
  if (/\bView evidence\b/i.test(msg)) {
    hits.push({
      source: 'workflow_card_renderer',
      field: 'message',
      value: 'View evidence',
      reason: 'evidence_boilerplate',
    });
  }
  if (/\bPrimary actions\b/i.test(msg)) {
    hits.push({
      source: 'workflow_card_renderer',
      field: 'message',
      value: 'Primary actions',
      reason: 'primary_actions_boilerplate',
    });
  }
  if (
    learnings.claim_tested_winner === false &&
    /tested winner/i.test(msg)
  ) {
    hits.push({
      source: 'template_or_section_title',
      field: 'message',
      value: 'tested winner',
      reason: 'unsupported_tested_winner_claim',
    });
  }

  return hits;
}

/**
 * Mark working state as awaiting an explicit force-rebuild confirmation.
 */
function markAwaitingForceRebuild(workingState, diagnostic = null) {
  const next = ensureCampaignWorkingState({
    campaignWorkingState: workingState,
  });
  next.awaitingForceRebuildConfirmation = true;
  next.bypassStoredOutreachDraftPreview = false;
  next.lastResponseMode = RESPONSE_MODES.STALE_SOURCE_DIAGNOSTIC;
  if (diagnostic) next.lastStaleDiagnostic = diagnostic;
  next.updatedAt = nowIso();
  return next;
}

/**
 * Apply confirmed force-rebuild: bypass stored draft on the next generation.
 */
function markForceRebuildBypass(workingState) {
  const next = ensureCampaignWorkingState({
    campaignWorkingState: workingState,
  });
  next.awaitingForceRebuildConfirmation = false;
  next.bypassStoredOutreachDraftPreview = true;
  next.rejectedOutputFingerprints = [];
  next.lastResponseMode = RESPONSE_MODES.OPERATOR_CHAT_RESPONSE;
  next.updatedAt = nowIso();
  return next;
}

/**
 * Clear bypass flag after a successful operator-chat rebuild.
 */
function clearForceRebuildBypass(workingState) {
  const next = ensureCampaignWorkingState({
    campaignWorkingState: workingState,
  });
  next.bypassStoredOutreachDraftPreview = false;
  next.awaitingForceRebuildConfirmation = false;
  next.updatedAt = nowIso();
  return next;
}

function buildFollowUpEmailDrafts(opts = {}) {
  const name = opts.businessName || 'the business';
  const audience = opts.audience || 'property managers';
  const cta =
    opts.cta ||
    'a short discovery conversation about recurring commercial cleaning reliability';
  const subject =
    opts.subject || '{{business_name}} - commercial cleaning';
  const differentiator =
    opts.differentiator ||
    'reliability, responsiveness, accountability, and fewer vendor-chasing headaches';

  const followUp1 = {
    step: 1,
    label: 'Follow-up 1',
    timing: '~3 business days',
    subject,
    body: [
      `Hi {{first_name}},`,
      '',
      `Following up in case my note about ${name} got buried.`,
      '',
      `${name} helps ${audience} across {{town}} stay covered with commercial cleaning they can count on — ${differentiator}.`,
      '',
      `Would you still be open to ${cta}?`,
      '',
      `Best,`,
      `{{sender_name}}`,
      `${name}`,
    ].join('\n'),
  };

  const followUp2 = {
    step: 2,
    label: 'Follow-up 2',
    timing: '~7 business days',
    subject,
    body: [
      `Hi {{first_name}},`,
      '',
      `Last note from me on this — happy to close the loop either way.`,
      '',
      `If recurring commercial cleaning reliability is on your radar for {{town}}, ${name} can walk through a simple checklist and estimate process without the vendor-chasing headaches.`,
      '',
      `Reply with a time that works, book a quick chat, or tell me “not now” and I’ll close this out.`,
      '',
      `Best,`,
      `{{sender_name}}`,
      `${name}`,
    ].join('\n'),
  };

  return [followUp1, followUp2];
}

/**
 * Conversational operator-facing draft message (not a workflow digest card).
 */
function formatOperatorChatDraftResponse(preview, opts = {}) {
  const p = preview || {};
  const changes = opts.changes || [];
  const lines = [];

  lines.push(
    opts.acknowledgment ||
      'Got it — I updated the active Outreach Draft Preview from your instructions.'
  );
  if (changes.length) {
    lines.push('');
    lines.push('What changed:');
    for (const c of changes) lines.push(`- ${c}`);
  }

  lines.push('');
  lines.push('## Subject');
  for (const s of p.subjectOptions || []) lines.push(`- ${s}`);
  if (!(p.subjectOptions || []).length) lines.push('- —');

  lines.push('');
  lines.push('## First-touch email');
  lines.push(p.firstTouchBody || (p.firstTouchDraft && p.firstTouchDraft.body) || '—');

  const drafts = Array.isArray(p.followUpDrafts) ? p.followUpDrafts : [];
  if (drafts.length) {
    for (const d of drafts) {
      lines.push('');
      lines.push(`## ${d.label || `Follow-up ${d.step}`}${d.timing ? ` (${d.timing})` : ''}`);
      if (d.subject) lines.push(`Subject: ${d.subject}`);
      lines.push(d.body || '—');
    }
  } else if (Array.isArray(p.followUpSketch) && p.followUpSketch.length) {
    lines.push('');
    lines.push('## Follow-ups');
    for (const item of p.followUpSketch) lines.push(`- ${item}`);
  }

  if (Array.isArray(p.personalizationByProspect) && p.personalizationByProspect.length) {
    lines.push('');
    lines.push('## Personalization notes (no street addresses)');
    for (const row of p.personalizationByProspect) {
      lines.push(
        `- ${row.companyName}${row.town ? ` (${row.town})` : ''}: ${
          row.personalizationNote || '—'
        }`
      );
    }
  }

  lines.push('');
  lines.push(
    p.disclaimer ||
      'Nothing external has happened. Sends, export, and CRM writes remain locked.'
  );
  lines.push('');
  lines.push(
    opts.closingQuestion ||
      'Do you approve this Outreach Draft Preview, or do you want revisions?'
  );

  return lines.join('\n').trim();
}

module.exports = {
  RESPONSE_MODES,
  PRIORITY_ORDER,
  SUBJECT_MERGE_TOKEN_PATTERN,
  STREET_ADDRESS_RE,
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
};
