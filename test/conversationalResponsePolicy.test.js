'use strict';

/**
 * Max Conversational Response Policy unit tests.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  CONVERSATION_MODES,
  RESPONSE_MODES,
  selectConversationMode,
  composeConversationResponse,
  formatApprovedLaunchGateConversational,
  formatOperatorDiagnosticMessage,
  containsRendererBoilerplate,
  approvalLanguageForGate,
  looksLikeExecutionRequest,
  looksLikeNonExecutionIntent,
  looksLikeCampaignReadySummary,
  looksLikeOperatorReadinessCheck,
  looksLikeLowSignalAmbiguousInput,
  looksLikeReadinessSubstepSelection,
  detectSelectedReadinessItem,
  compactSafetyLockLine,
  assessConversationContext,
  applyConversationalPolicy,
  selectResponseMode,
  composeCampaignReadySummary,
  composeOperatorReadinessCheck,
  composeReadinessSubstep,
  composeReadinessFieldCorrection,
  composeClarificationNeeded,
  extractOperatorReadinessChecklist,
  mergeOperatorReadinessChecklist,
  looksLikeReadinessFieldCorrection,
  parseSenderIdentityFields,
  parseReplyHandlingFields,
  parseOperationalPathSelection,
  parseFollowUpTrackingFields,
  parseReplyMonitoringBatchReviewFields,
  mergeSenderIdentityState,
  mergeReplyHandlingState,
  isSenderFieldValueLine,
  isReplyFieldValueLine,
  isOperationalPathValueLine,
  READINESS_CHECKLIST_SAFETY_LINE,
  CLARIFICATION_NEEDED_ASK,
  READINESS_SUBSTEPS,
} = require('../services/maxSynthesis');

const SEVEN_ITEM_READINESS_CHECKLIST = [
  'sender identity is not confirmed',
  'reply inbox / reply-to handling is not confirmed',
  'operational path is not chosen yet: manual send vs CRM draft vs queued send',
  'follow-up tracking process is not confirmed',
  'reply monitoring owner/process is not confirmed',
  'broader rollout remains blocked until Batch 1 results are reviewed',
  'tracking / account settings remain unchanged unless explicitly approved later',
];

const SEVEN_ITEM_READINESS_MESSAGE = [
  'Before choosing export, CRM drafts, or queued sends, help me resolve the remaining readiness items. Please summarize only what is still unresolved:',
  '',
  ...SEVEN_ITEM_READINESS_CHECKLIST.map((item) => `- ${item}`),
].join('\n');

describe('ConversationalResponsePolicy', () => {
  it('selects formal review gate for first-time launch gate', () => {
    const mode = selectConversationMode({
      isInitialReviewGate: true,
      intent: 'produce_outreach_launch_gate',
    });
    assert.equal(mode, CONVERSATION_MODES.FORMAL_REVIEW_GATE);
  });

  it('selects operator state update when gate already approved', () => {
    const mode = selectConversationMode({
      launchGateApproved: true,
      intent: 'outreach_launch_gate_approved',
      isInitialReviewGate: true,
    });
    assert.equal(mode, CONVERSATION_MODES.OPERATOR_STATE_UPDATE);
  });

  it('selects revision response for corrections', () => {
    const mode = selectConversationMode({
      messageClass: 'refinement_feedback',
      isRevision: true,
      text: 'Revise the Outreach Draft Preview',
    });
    assert.equal(mode, CONVERSATION_MODES.OPERATOR_REVISION_RESPONSE);
  });

  it('selects diagnostic when stale source is required', () => {
    const mode = selectConversationMode({
      staleDiagnosticRequired: true,
    });
    assert.equal(mode, CONVERSATION_MODES.OPERATOR_DIAGNOSTIC);
  });

  it('selects execution confirmation for export/send asks', () => {
    assert.equal(looksLikeExecutionRequest('Prepare a manual-send export'), true);
    const mode = selectConversationMode({
      text: 'Prepare a manual-send export for review',
      isExecutionRequest: true,
    });
    assert.equal(mode, CONVERSATION_MODES.EXECUTION_CONFIRMATION);
  });

  it('does not treat readiness / planning language as execution', () => {
    const negatives = [
      "What's still unresolved?",
      'Help me decide between export and CRM drafts.',
      'Before choosing, summarize readiness gaps.',
      'What would manual-send export involve?',
      "Let's talk through sender identity first.",
      "I'd probably do manual export, but not yet.",
      "What's the safest next move?",
      'Hold for now.',
      'Before choosing export, CRM drafts, or queued sends, help me resolve the remaining readiness items. Please summarize only what is still unresolved.',
    ];
    for (const text of negatives) {
      assert.equal(
        looksLikeExecutionRequest(text),
        false,
        `expected non-execution for: ${text}`
      );
      assert.equal(
        looksLikeNonExecutionIntent(text) ||
          looksLikeOperatorReadinessCheck(text),
        true,
        `expected planning/readiness for: ${text}`
      );
    }

    const positives = [
      'Prepare the manual-send export.',
      'Create the CRM drafts.',
      'Queue the sends.',
      'Approve export.',
      'Yes, execute the manual-send export.',
      'Go ahead and create the export file.',
      'Prepare a manual-send export for review',
    ];
    for (const text of positives) {
      assert.equal(
        looksLikeExecutionRequest(text),
        true,
        `expected execution for: ${text}`
      );
    }
  });

  it('selects operator_readiness_check for unresolved readiness asks', () => {
    const text =
      'Before choosing export, CRM drafts, or queued sends, help me resolve the remaining readiness items. Please summarize only what is still unresolved.';
    assert.equal(looksLikeOperatorReadinessCheck(text), true);
    assert.equal(looksLikeReadinessSubstepSelection(text), false);
    assert.equal(looksLikeExecutionRequest(text), false);
    const mode = selectConversationMode({
      text,
      operatorMessage: text,
      launchGateApproved: true,
    });
    assert.equal(mode, CONVERSATION_MODES.OPERATOR_READINESS_CHECK);
  });

  it('selects readiness_substep when operator picks sender identity', () => {
    const text =
      'Resolve sender identity now. Do not repeat the full readiness checklist. I already selected the first readiness item: sender identity.';
    assert.equal(looksLikeReadinessSubstepSelection(text), true);
    assert.equal(looksLikeOperatorReadinessCheck(text), false);
    assert.equal(looksLikeExecutionRequest(text), false);
    assert.equal(detectSelectedReadinessItem(text).id, 'sender_identity');
    const mode = selectConversationMode({
      text,
      operatorMessage: text,
      launchGateApproved: true,
    });
    assert.equal(mode, CONVERSATION_MODES.READINESS_SUBSTEP);
    assert.notEqual(mode, CONVERSATION_MODES.OPERATOR_READINESS_CHECK);
    assert.notEqual(mode, CONVERSATION_MODES.EXECUTION_CONFIRMATION);
    assert.equal(
      selectResponseMode({ text, launchGateApproved: true }),
      RESPONSE_MODES.READINESS_SUBSTEP
    );
  });

  it('composes sender-identity substep without which-item ask', () => {
    const text = 'Resolve sender identity now.';
    const composed = composeReadinessSubstep({
      operatorMessage: text,
      launchGateApproved: true,
    });
    assert.equal(composed.mode, CONVERSATION_MODES.READINESS_SUBSTEP);
    assert.equal(composed.responseMode, 'readiness_substep');
    assert.equal(composed.readinessItemId, 'sender_identity');
    assert.equal(composed.requiresExplicitApproval, false);
    assert.match(composed.message, /What sender name should appear on the email/i);
    assert.match(
      composed.message,
      /What sender email address should be used or reviewed/i
    );
    assert.match(
      composed.message,
      /Should the signature be from a person, the company, or both/i
    );
    assert.match(
      composed.message,
      /Once you answer those, I'll mark sender identity as confirmed or note what still needs review/i
    );
    assert.match(
      composed.message,
      /Nothing external has happened\. Sends, export, and CRM writes remain locked\./i
    );
    assert.doesNotMatch(
      composed.message,
      /Which readiness item should we resolve first/i
    );
    assert.doesNotMatch(composed.message, /Still unresolved before any export/i);
    assert.doesNotMatch(composed.message, /Sender identity is not confirmed/i);
    assert.doesNotMatch(composed.message, /Reply handling is not confirmed/i);
    assert.doesNotMatch(composed.message, /Exact action/i);
    assert.doesNotMatch(
      composed.message,
      /Do you explicitly approve this execute action/i
    );
    assert.doesNotMatch(composed.message, /The next choice is operational/i);
  });

  it('selects readiness_substep when operator picks reply handling', () => {
    const text =
      'Resolve reply handling now. Do not repeat the full readiness checklist.';
    assert.equal(looksLikeReadinessSubstepSelection(text), true);
    assert.equal(looksLikeOperatorReadinessCheck(text), false);
    assert.equal(looksLikeExecutionRequest(text), false);
    assert.equal(detectSelectedReadinessItem(text).id, 'reply_handling');
    const mode = selectConversationMode({
      text,
      operatorMessage: text,
      launchGateApproved: true,
    });
    assert.equal(mode, CONVERSATION_MODES.READINESS_SUBSTEP);
    assert.notEqual(mode, CONVERSATION_MODES.OPERATOR_READINESS_CHECK);
    assert.notEqual(mode, CONVERSATION_MODES.EXECUTION_CONFIRMATION);
    assert.equal(
      selectResponseMode({ text, launchGateApproved: true }),
      RESPONSE_MODES.READINESS_SUBSTEP
    );
  });

  it('composes reply-handling substep without which-item or execute ask', () => {
    const text = 'Resolve reply handling now.';
    const composed = composeReadinessSubstep({
      operatorMessage: text,
      launchGateApproved: true,
    });
    assert.equal(composed.mode, CONVERSATION_MODES.READINESS_SUBSTEP);
    assert.equal(composed.responseMode, 'readiness_substep');
    assert.equal(composed.readinessItemId, 'reply_handling');
    assert.equal(composed.requiresExplicitApproval, false);
    assert.match(
      composed.message,
      /Which reply inbox \/ reply-to address should be used/i
    );
    assert.match(composed.message, /Who monitors replies/i);
    assert.match(
      composed.message,
      /How should replies be handled before broader rollout/i
    );
    assert.match(
      composed.message,
      /Once you answer those, I'll mark reply handling as confirmed or note what still needs review/i
    );
    assert.match(
      composed.message,
      /Nothing external has happened\. Sends, export, and CRM writes remain locked\./i
    );
    assert.doesNotMatch(
      composed.message,
      /Which readiness item should we resolve first/i
    );
    assert.doesNotMatch(composed.message, /Still unresolved before any export/i);
    assert.doesNotMatch(composed.message, /What sender name should appear/i);
    assert.doesNotMatch(composed.message, /Exact action/i);
    assert.doesNotMatch(
      composed.message,
      /Do you explicitly approve this execute action/i
    );
    assert.doesNotMatch(composed.message, /prepare a manual-send export/i);
    assert.doesNotMatch(composed.message, /create CRM drafts/i);
    assert.doesNotMatch(composed.message, /queue sends/i);
  });

  it('classifies sender email correction as readiness_field_correction', () => {
    const text = 'update sender email address to jacob@goanchorcleaning.com';
    assert.equal(
      looksLikeReadinessFieldCorrection(text, {
        activeReadinessItemId: 'sender_identity',
      }),
      true
    );
    assert.equal(looksLikeReadinessSubstepSelection(text), false);
    assert.equal(looksLikeOperatorReadinessCheck(text), false);
    assert.equal(looksLikeExecutionRequest(text), false);
    assert.equal(looksLikeLowSignalAmbiguousInput(text), false);

    const parsed = parseSenderIdentityFields(text);
    assert.equal(parsed.email, 'jacob@goanchorcleaning.com');
    assert.ok(parsed.updatedFields.includes('email'));

    const mode = selectConversationMode({
      text,
      operatorMessage: text,
      launchGateApproved: true,
      activeReadinessItemId: 'sender_identity',
      slots: {
        activeReadinessItemId: 'sender_identity',
        senderName: 'Jacob Maynard',
        senderEmail: 'jacob@goanchorcleaning.com',
        senderSignature: 'Jacob Maynard, Anchor Cleaning',
      },
    });
    assert.equal(mode, CONVERSATION_MODES.READINESS_FIELD_CORRECTION);
    assert.notEqual(mode, CONVERSATION_MODES.OPERATOR_STATE_UPDATE);
    assert.notEqual(mode, CONVERSATION_MODES.EXECUTION_CONFIRMATION);
    assert.equal(
      selectResponseMode({
        text,
        launchGateApproved: true,
        activeReadinessItemId: 'sender_identity',
        slots: { activeReadinessItemId: 'sender_identity' },
      }),
      RESPONSE_MODES.READINESS_FIELD_CORRECTION
    );
  });

  it('composes sender email correction without Launch Gate options', () => {
    const text = 'update sender email address to jacob@goanchorcleaning.com';
    const composed = composeReadinessFieldCorrection({
      operatorMessage: text,
      activeReadinessItemId: 'sender_identity',
      slots: {
        activeReadinessItemId: 'sender_identity',
        senderName: 'Jacob Maynard',
        senderEmail: 'jacob\\@goanchorcleaning.com',
        senderSignature: 'Jacob Maynard, Anchor Cleaning',
      },
    });

    assert.equal(composed.mode, CONVERSATION_MODES.READINESS_FIELD_CORRECTION);
    assert.equal(composed.responseMode, 'readiness_field_correction');
    assert.equal(composed.requiresExplicitApproval, false);
    assert.equal(composed.itemConfirmed, true);
    assert.match(
      composed.message,
      /Updated sender email to jacob@goanchorcleaning\.com/i
    );
    assert.match(composed.message, /Sender identity is confirmed/i);
    assert.match(composed.message, /Sender name:\s*Jacob Maynard/i);
    assert.match(
      composed.message,
      /Sender email address:\s*jacob@goanchorcleaning\.com/i
    );
    assert.match(
      composed.message,
      /Signature:\s*Jacob Maynard, Anchor Cleaning/i
    );
    assert.match(
      composed.message,
      /Next readiness item:\s*reply inbox \/ reply-to handling/i
    );
    assert.match(composed.message, /^What inbox should receive replies\?/m);
    assert.match(
      composed.message,
      /^Should reply-to match the sender address\?/m
    );
    assert.match(composed.message, /^Who will monitor replies\?/m);
    assert.match(
      composed.message,
      /Nothing external has happened\. Sends, export, and CRM writes remain locked\./i
    );
    assert.doesNotMatch(composed.message, /The next choice is operational/i);
    assert.doesNotMatch(composed.message, /prepare a manual-send export/i);
    assert.doesNotMatch(composed.message, /Which next path do you want to prepare/i);
    assert.doesNotMatch(
      composed.message,
      /Do you explicitly approve this execute action/i
    );
    assert.doesNotMatch(composed.message, /Exact action/i);
    assert.doesNotMatch(composed.message, /create CRM drafts/i);
    assert.doesNotMatch(composed.message, /queue sends/i);
  });

  it('merges all sender identity fields from a correction-plus-full-state message', () => {
    const text = [
      'Update sender email address to:',
      'jacob@goanchorcleaning.com',
      '',
      'Sender identity should now be:',
      '- Sender name: Jacob Maynard',
      '- Sender email address: jacob@goanchorcleaning.com',
      '- Signature: Jacob Maynard, Anchor Cleaning',
    ].join('\n');

    const parsed = parseSenderIdentityFields(text);
    assert.equal(parsed.name, 'Jacob Maynard');
    assert.equal(parsed.email, 'jacob@goanchorcleaning.com');
    assert.equal(parsed.signature, 'Jacob Maynard, Anchor Cleaning');
    assert.deepEqual(parsed.updatedFields.sort(), ['email', 'name', 'signature']);

    const composed = composeReadinessFieldCorrection({
      operatorMessage: text,
      activeReadinessItemId: 'sender_identity',
      slots: { activeReadinessItemId: 'sender_identity' },
    });

    assert.equal(composed.responseMode, 'readiness_field_correction');
    assert.equal(composed.itemConfirmed, true);
    assert.equal(composed.senderIdentity.senderName, 'Jacob Maynard');
    assert.equal(
      composed.senderIdentity.senderEmail,
      'jacob@goanchorcleaning.com'
    );
    assert.equal(
      composed.senderIdentity.senderSignature,
      'Jacob Maynard, Anchor Cleaning'
    );
    assert.deepEqual(composed.senderIdentity.missing, []);
    assert.match(composed.message, /Updated sender identity\./);
    assert.match(composed.message, /Sender identity is confirmed/i);
    assert.match(composed.message, /Sender name:\s*Jacob Maynard/i);
    assert.match(
      composed.message,
      /Sender email address:\s*jacob@goanchorcleaning\.com/i
    );
    assert.match(
      composed.message,
      /Signature:\s*Jacob Maynard, Anchor Cleaning/i
    );
    assert.match(
      composed.message,
      /Next readiness item:\s*reply inbox \/ reply-to handling/i
    );
    assert.match(composed.message, /^What inbox should receive replies\?/m);
    assert.match(
      composed.message,
      /^Should reply-to match the sender address\?/m
    );
    assert.match(composed.message, /^Who will monitor replies\?/m);
    assert.doesNotMatch(composed.message, /Still needed for sender identity/i);
    assert.doesNotMatch(
      composed.message,
      /Still needed for sender identity:[\s\S]*sender name/i
    );
    assert.doesNotMatch(
      composed.message,
      /Still needed for sender identity:[\s\S]*signature/i
    );
    assert.doesNotMatch(composed.message, /prepare a manual-send export/i);
    assert.doesNotMatch(composed.message, /Which next path do you want to prepare/i);
  });

  it('confirms sender identity and does not list confirmed fields as unresolved', () => {
    const text = [
      '- Sender name: Jacob Maynard',
      '- Sender email address: jacob@goanchorcleaning.com',
      '- Signature: Jacob Maynard, Anchor Cleaning',
    ].join('\n');

    assert.equal(looksLikeReadinessFieldCorrection(text), true);
    assert.equal(looksLikeOperatorReadinessCheck(text), false);
    assert.deepEqual(extractOperatorReadinessChecklist(text), []);

    const composed = composeReadinessFieldCorrection({
      operatorMessage: text,
      activeReadinessItemId: 'sender_identity',
    });
    assert.equal(composed.itemConfirmed, true);
    assert.match(composed.message, /^Sender identity is confirmed:/m);
    assert.match(composed.message, /Sender name:\s*Jacob Maynard/i);
    assert.match(
      composed.message,
      /Sender email address:\s*jacob@goanchorcleaning\.com/i
    );
    assert.match(
      composed.message,
      /Signature:\s*Jacob Maynard, Anchor Cleaning/i
    );
    assert.match(
      composed.message,
      /Next readiness item:\s*reply inbox \/ reply-to handling/i
    );
    assert.match(composed.message, /^What inbox should receive replies\?/m);
    assert.match(
      composed.message,
      /^Should reply-to match the sender address\?/m
    );
    assert.match(composed.message, /^Who will monitor replies\?/m);
    assert.match(
      composed.message,
      /Nothing external has happened\. Sends, export, and CRM writes remain locked\./i
    );
    assert.doesNotMatch(composed.message, /Still unresolved before any export/i);
    assert.doesNotMatch(composed.message, /Still needed for sender identity/i);

    // Even if routed through readiness-check compose, confirmed fields stay out
    // of the unresolved list.
    const check = composeOperatorReadinessCheck({
      operatorMessage: text,
      unresolvedItems: [
        'Sender name: Jacob Maynard',
        'Sender email address: jacob@goanchorcleaning.com',
        'Signature: Jacob Maynard, Anchor Cleaning',
        'what inbox should receive replies?',
        'should reply-to match the sender address?',
        'who will monitor replies?',
      ],
    });
    assert.match(check.message, /Sender identity is confirmed/i);
    assert.doesNotMatch(check.message, /Still unresolved before any export/i);
    for (const item of check.unresolvedItems || []) {
      assert.equal(isSenderFieldValueLine(item), false);
    }
    assert.ok(
      !(check.unresolvedItems || []).some((i) =>
        /Sender name:\s*Jacob Maynard/i.test(i)
      )
    );
  });

  it('confirms reply handling and advances to operational path without execute', () => {
    const text = [
      '- Reply inbox: jacob@goanchorcleaning.com',
      '- Reply-to should match the sender address: yes',
      '- Reply monitoring owner: Jacob Maynard',
    ].join('\n');

    assert.equal(looksLikeReadinessFieldCorrection(text), true);
    assert.equal(looksLikeOperatorReadinessCheck(text), false);
    assert.equal(looksLikeExecutionRequest(text), false);
    assert.deepEqual(extractOperatorReadinessChecklist(text), []);

    const parsed = parseReplyHandlingFields(text);
    assert.equal(parsed.replyInbox, 'jacob@goanchorcleaning.com');
    assert.equal(parsed.sameAsSender, true);
    assert.equal(parsed.monitoringOwner, 'Jacob Maynard');
    assert.ok(parsed.updatedFields.includes('same_as_sender'));

    const composed = composeReadinessFieldCorrection({
      operatorMessage: text,
      activeReadinessItemId: 'reply_handling',
      slots: {
        activeReadinessItemId: 'reply_handling',
        senderIdentityConfirmed: true,
        senderName: 'Jacob Maynard',
        senderEmail: 'jacob@goanchorcleaning.com',
        senderSignature: 'Jacob Maynard, Anchor Cleaning',
      },
    });

    assert.equal(composed.responseMode, 'readiness_field_correction');
    assert.equal(composed.itemConfirmed, true);
    assert.equal(composed.requiresExplicitApproval, false);
    assert.equal(composed.executionPending, false);
    assert.equal(composed.nextReadinessItem.id, 'operational_path');
    assert.match(
      composed.message,
      /^Reply inbox \/ reply-to handling is confirmed:/m
    );
    assert.match(
      composed.message,
      /Reply inbox:\s*jacob@goanchorcleaning\.com/i
    );
    assert.match(
      composed.message,
      /Reply-to matches sender address:\s*yes/i
    );
    assert.match(
      composed.message,
      /Reply monitoring owner:\s*Jacob Maynard/i
    );
    assert.match(
      composed.message,
      /Next readiness item:\s*operational path selection/i
    );
    assert.match(
      composed.message,
      /Do you want to prepare for manual send, CRM drafts, queued sends later, or hold with no action\?/i
    );
    assert.match(
      composed.message,
      /Nothing external has happened\. Sends, export, and CRM writes remain locked\./i
    );
    assert.doesNotMatch(composed.message, /Still unresolved before any export/i);
    assert.doesNotMatch(
      composed.message,
      /Which readiness item should we resolve first/i
    );
    assert.doesNotMatch(
      composed.message,
      /Do you explicitly approve this execute action/i
    );
    assert.doesNotMatch(composed.message, /Exact action/i);
    assert.equal(looksLikeExecutionRequest(composed.message), false);

    // Misrouted readiness-check path must also keep reply values confirmed.
    const check = composeOperatorReadinessCheck({
      operatorMessage: text,
      unresolvedItems: [
        'Reply inbox: jacob@goanchorcleaning.com',
        'Reply-to should match the sender address: yes',
        'Reply monitoring owner: Jacob Maynard',
      ],
      slots: {
        senderIdentityConfirmed: true,
        senderName: 'Jacob Maynard',
        senderEmail: 'jacob@goanchorcleaning.com',
        senderSignature: 'Jacob Maynard, Anchor Cleaning',
      },
    });
    assert.match(
      check.message,
      /Reply inbox \/ reply-to handling is confirmed/i
    );
    assert.match(check.message, /Reply-to matches sender address:\s*yes/i);
    assert.match(
      check.message,
      /Next readiness item:\s*operational path selection/i
    );
    assert.doesNotMatch(check.message, /Still unresolved before any export/i);
    assert.doesNotMatch(
      check.message,
      /Which readiness item should we resolve first/i
    );
    for (const item of check.unresolvedItems || []) {
      assert.equal(isReplyFieldValueLine(item), false);
    }
  });

  it('selects readiness_field_correction for operational path selection', () => {
    const text =
      'Select operational path: manual send export for operator review. This is path selection only, not execute approval.';
    assert.equal(looksLikeReadinessFieldCorrection(text), true);
    assert.equal(looksLikeExecutionRequest(text), false);
    assert.equal(looksLikeOperatorReadinessCheck(text), false);
    assert.equal(looksLikeNonExecutionIntent(text), true);

    const parsed = parseOperationalPathSelection(text);
    assert.equal(parsed.hasAny, true);
    assert.equal(parsed.pathId, 'manual_send_export');
    assert.equal(parsed.pathLabel, 'manual send export for operator review');
    assert.equal(parsed.isPathSelectionOnly, true);

    const mode = selectConversationMode({
      text,
      operatorMessage: text,
      launchGateApproved: true,
      activeReadinessItemId: 'operational_path',
      slots: {
        activeReadinessItemId: 'operational_path',
        senderIdentityConfirmed: true,
        replyInboxConfirmed: true,
      },
    });
    assert.equal(mode, CONVERSATION_MODES.READINESS_FIELD_CORRECTION);
    assert.notEqual(mode, CONVERSATION_MODES.EXECUTION_CONFIRMATION);
    assert.notEqual(mode, CONVERSATION_MODES.OPERATOR_READINESS_CHECK);
  });

  it('confirms operational path and advances to follow-up tracking without execute', () => {
    const text =
      'Select operational path: manual send export for operator review. This is path selection only, not execute approval.';

    const composed = composeReadinessFieldCorrection({
      operatorMessage: text,
      activeReadinessItemId: 'operational_path',
      slots: {
        activeReadinessItemId: 'operational_path',
        senderIdentityConfirmed: true,
        senderName: 'Jacob Maynard',
        senderEmail: 'jacob@goanchorcleaning.com',
        senderSignature: 'Jacob Maynard, Anchor Cleaning',
        replyInboxConfirmed: true,
        replyHandlingConfirmed: true,
        replyInbox: 'jacob@goanchorcleaning.com',
        replyToMatchesSender: true,
        replyMonitoringOwner: 'Jacob Maynard',
      },
    });

    assert.equal(composed.responseMode, 'readiness_field_correction');
    assert.equal(composed.itemConfirmed, true);
    assert.equal(composed.requiresExplicitApproval, false);
    assert.equal(composed.executionPending, false);
    assert.equal(composed.exportMade, false);
    assert.equal(composed.readinessItemId, 'operational_path');
    assert.equal(composed.nextReadinessItem.id, 'follow_up_tracking');
    assert.equal(
      composed.operationalPath.operationalPathLabel,
      'manual send export for operator review'
    );
    assert.match(composed.message, /^Operational path is selected:/m);
    assert.match(
      composed.message,
      /- manual send export for operator review/i
    );
    assert.match(
      composed.message,
      /This is path selection only\. No export has been prepared\./i
    );
    assert.match(
      composed.message,
      /Next readiness item:\s*follow-up tracking process/i
    );
    assert.match(composed.message, /^Where should follow-up status be tracked\?/m);
    assert.match(
      composed.message,
      /^Should Follow-up 1 be planned for about 3 business days after first touch\?/m
    );
    assert.match(
      composed.message,
      /^Should Follow-up 2 be planned for about 7 business days after first touch\?/m
    );
    assert.match(
      composed.message,
      /^Should all follow-ups remain review-first\/manual unless explicitly enabled later\?/m
    );
    assert.match(
      composed.message,
      /Nothing external has happened\. Sends, export, and CRM writes remain locked\./i
    );
    assert.doesNotMatch(composed.message, /Still unresolved before any export/i);
    assert.doesNotMatch(
      composed.message,
      /Which readiness item should we resolve first/i
    );
    assert.doesNotMatch(
      composed.message,
      /Do you explicitly approve this execute action/i
    );
    assert.doesNotMatch(composed.message, /Exact action/i);
    assert.equal(looksLikeExecutionRequest(composed.message), false);

    // Misrouted readiness-check path must keep selected path confirmed.
    const check = composeOperatorReadinessCheck({
      operatorMessage: text,
      unresolvedItems: [
        'manual send export for operator review',
        'where should follow-up status be tracked?',
        'Should Follow-up 1 be planned for about 3 business days after first touch?',
      ],
      slots: {
        senderIdentityConfirmed: true,
        senderName: 'Jacob Maynard',
        senderEmail: 'jacob@goanchorcleaning.com',
        senderSignature: 'Jacob Maynard, Anchor Cleaning',
        replyInboxConfirmed: true,
        replyInbox: 'jacob@goanchorcleaning.com',
        replyToMatchesSender: true,
        replyMonitoringOwner: 'Jacob Maynard',
      },
    });
    assert.match(check.message, /Operational path is selected/i);
    assert.match(
      check.message,
      /manual send export for operator review/i
    );
    assert.match(
      check.message,
      /Next readiness item:\s*follow-up tracking process/i
    );
    assert.doesNotMatch(check.message, /Still unresolved before any export/i);
    assert.doesNotMatch(
      check.message,
      /Which readiness item should we resolve first/i
    );
    for (const item of check.unresolvedItems || []) {
      assert.equal(isOperationalPathValueLine(item), false);
      assert.doesNotMatch(item, /manual send export for operator review/i);
    }
  });

  it('confirms follow-up tracking from active-substep answers and advances to reply monitoring', () => {
    const text = [
      'Follow-up status should be tracked in the manual-send export/review sheet for now.',
      'Follow-up 1 should be planned for about 3 business days after first touch.',
      'Follow-up 2 should be planned for about 7 business days after first touch.',
      'All follow-ups remain review-first/manual unless explicitly enabled later.',
      'No automatic follow-up sends are approved.',
    ].join(' ');

    assert.equal(
      looksLikeReadinessFieldCorrection(text, {
        activeReadinessItemId: 'follow_up_tracking_process',
      }),
      true
    );
    assert.equal(looksLikeExecutionRequest(text), false);
    assert.equal(looksLikeOperatorReadinessCheck(text), false);

    const parsed = parseFollowUpTrackingFields(text);
    assert.equal(parsed.hasAny, true);
    assert.equal(
      parsed.trackingLocation,
      'the manual-send export/review sheet'
    );
    assert.equal(
      parsed.followUp1Timing,
      'about 3 business days after first touch'
    );
    assert.equal(
      parsed.followUp2Timing,
      'about 7 business days after first touch'
    );
    assert.equal(parsed.reviewFirstManual, true);
    assert.equal(parsed.automaticFollowUpSendsApproved, false);

    const mode = selectConversationMode({
      text,
      operatorMessage: text,
      launchGateApproved: true,
      activeReadinessItemId: 'follow_up_tracking_process',
      slots: {
        activeReadinessItemId: 'follow_up_tracking_process',
        senderIdentityConfirmed: true,
        replyInboxConfirmed: true,
        operationalPathChosen: true,
      },
    });
    assert.equal(mode, CONVERSATION_MODES.READINESS_FIELD_CORRECTION);
    assert.notEqual(mode, CONVERSATION_MODES.EXECUTION_CONFIRMATION);
    assert.notEqual(mode, CONVERSATION_MODES.OPERATOR_READINESS_CHECK);
    assert.notEqual(mode, CONVERSATION_MODES.READINESS_SUBSTEP);

    const composed = composeReadinessFieldCorrection({
      operatorMessage: text,
      activeReadinessItemId: 'follow_up_tracking_process',
      slots: {
        activeReadinessItemId: 'follow_up_tracking_process',
        senderIdentityConfirmed: true,
        senderName: 'Jacob Maynard',
        senderEmail: 'jacob@goanchorcleaning.com',
        senderSignature: 'Jacob Maynard, Anchor Cleaning',
        replyInboxConfirmed: true,
        replyHandlingConfirmed: true,
        replyInbox: 'jacob@goanchorcleaning.com',
        replyToMatchesSender: true,
        replyMonitoringOwner: 'Jacob Maynard',
        operationalPathChosen: true,
        operationalPathId: 'manual_send_export',
        operationalPathLabel: 'manual send export for operator review',
      },
    });

    assert.equal(composed.responseMode, 'readiness_field_correction');
    assert.equal(composed.itemConfirmed, true);
    assert.equal(composed.requiresExplicitApproval, false);
    assert.equal(composed.executionPending, false);
    assert.equal(composed.exportMade, false);
    assert.equal(composed.sendsMade, false);
    assert.equal(composed.crmWritesMade, false);
    assert.equal(composed.readinessItemId, 'follow_up_tracking');
    assert.equal(
      composed.nextReadinessItem.id,
      'reply_monitoring_batch1'
    );
    assert.equal(
      composed.activeReadinessItemId,
      'reply_monitoring_batch1'
    );
    assert.match(
      composed.message,
      /^Follow-up tracking process is confirmed:/m
    );
    assert.match(
      composed.message,
      /- Status tracked in the manual-send export\/review sheet/i
    );
    assert.match(
      composed.message,
      /- Follow-up 1 planned for about 3 business days after first touch/i
    );
    assert.match(
      composed.message,
      /- Follow-up 2 planned for about 7 business days after first touch/i
    );
    assert.match(
      composed.message,
      /- Follow-ups remain review-first\/manual unless explicitly enabled later/i
    );
    assert.match(
      composed.message,
      /- No automatic follow-up sends are approved/i
    );
    assert.match(
      composed.message,
      /Next readiness item:\s*reply monitoring \/ Batch 1 review process/i
    );
    assert.match(
      composed.message,
      /Who will monitor replies, how should responses be reviewed, and should broader rollout remain blocked until Batch 1 results are reviewed\?/i
    );
    assert.doesNotMatch(
      composed.message,
      /Where should follow-up status be tracked\?/i
    );
    assert.doesNotMatch(
      composed.message,
      /Should Follow-up 1 be planned/i
    );
    assert.doesNotMatch(
      composed.message,
      /Do you explicitly approve this execute action/i
    );
    assert.doesNotMatch(composed.message, /Exact action/i);
    assert.doesNotMatch(composed.message, /prepare a manual-send export/i);
    assert.equal(looksLikeExecutionRequest(composed.message), false);
  });

  it('confirms reply monitoring / Batch 1 review from active-substep answers and summarizes readiness', () => {
    const text = [
      'Reply monitoring owner: Jacob Maynard',
      'Responses should be reviewed manually before any follow-up or broader rollout decision',
      'Positive replies should be handled as conversation/walkthrough opportunities',
      'Negative or not-now replies should be respected and noted',
      'Broader rollout remains blocked until Batch 1 results are reviewed',
      'Batch 1 results should be reviewed before expanding to Cedar, optional Manchester candidates, or any new prospect segment',
    ].join('\n');

    assert.equal(
      looksLikeReadinessFieldCorrection(text, {
        activeReadinessItemId: 'reply_monitoring_batch_review',
      }),
      true
    );
    assert.equal(looksLikeExecutionRequest(text), false);
    assert.equal(looksLikeOperatorReadinessCheck(text), false);

    const parsed = parseReplyMonitoringBatchReviewFields(text);
    assert.equal(parsed.hasAny, true);
    assert.equal(parsed.replyMonitoringOwner, 'Jacob Maynard');
    assert.equal(
      parsed.responseReviewProcess,
      'Responses reviewed manually before any follow-up or broader rollout decision'
    );
    assert.equal(
      parsed.positiveReplyHandling,
      'Positive replies handled as conversation/walkthrough opportunities'
    );
    assert.equal(
      parsed.negativeReplyHandling,
      'Negative or not-now replies respected and noted'
    );
    assert.equal(parsed.broaderRolloutBlocked, true);
    assert.equal(parsed.batch1ReviewBeforeExpansion, true);

    const mode = selectConversationMode({
      text,
      operatorMessage: text,
      launchGateApproved: true,
      activeReadinessItemId: 'reply_monitoring_batch_review',
      slots: {
        activeReadinessItemId: 'reply_monitoring_batch1',
        senderIdentityConfirmed: true,
        replyInboxConfirmed: true,
        operationalPathChosen: true,
        followUpTrackingConfirmed: true,
      },
    });
    assert.equal(mode, CONVERSATION_MODES.READINESS_FIELD_CORRECTION);
    assert.notEqual(mode, CONVERSATION_MODES.EXECUTION_CONFIRMATION);
    assert.notEqual(mode, CONVERSATION_MODES.OPERATOR_READINESS_CHECK);
    assert.notEqual(mode, CONVERSATION_MODES.READINESS_SUBSTEP);

    const composed = composeReadinessFieldCorrection({
      operatorMessage: text,
      activeReadinessItemId: 'reply_monitoring_batch_review',
      slots: {
        activeReadinessItemId: 'reply_monitoring_batch_review',
        senderIdentityConfirmed: true,
        senderName: 'Jacob Maynard',
        senderEmail: 'jacob@goanchorcleaning.com',
        senderSignature: 'Jacob Maynard, Anchor Cleaning',
        replyInboxConfirmed: true,
        replyHandlingConfirmed: true,
        replyInbox: 'jacob@goanchorcleaning.com',
        replyToMatchesSender: true,
        replyMonitoringOwner: 'Jacob Maynard',
        operationalPathChosen: true,
        operationalPathId: 'manual_send_export',
        operationalPathLabel: 'manual send export for operator review',
        followUpTrackingConfirmed: true,
        followUpTrackingLocation: 'the manual-send export/review sheet',
        followUp1Timing: 'about 3 business days after first touch',
        followUp2Timing: 'about 7 business days after first touch',
        followUpReviewFirstManual: true,
        automaticFollowUpSendsApproved: false,
      },
    });

    assert.equal(composed.responseMode, 'readiness_field_correction');
    assert.equal(composed.itemConfirmed, true);
    assert.equal(composed.requiresExplicitApproval, false);
    assert.equal(composed.executionPending, false);
    assert.equal(composed.exportMade, false);
    assert.equal(composed.sendsMade, false);
    assert.equal(composed.crmWritesMade, false);
    assert.equal(composed.readinessItemId, 'reply_monitoring_batch1');
    assert.equal(composed.nextReadinessItem, null);
    assert.equal(composed.activeReadinessItemId, 'reply_monitoring_batch1');
    assert.match(
      composed.message,
      /^Reply monitoring \/ Batch 1 review process is confirmed:/m
    );
    assert.match(
      composed.message,
      /- Reply monitoring owner: Jacob Maynard/i
    );
    assert.match(
      composed.message,
      /- Responses reviewed manually before any follow-up or broader rollout decision/i
    );
    assert.match(
      composed.message,
      /- Positive replies handled as conversation\/walkthrough opportunities/i
    );
    assert.match(
      composed.message,
      /- Negative or not-now replies respected and noted/i
    );
    assert.match(
      composed.message,
      /- Broader rollout remains blocked until Batch 1 results are reviewed/i
    );
    assert.match(
      composed.message,
      /- Batch 1 results reviewed before expanding to Cedar, optional Manchester candidates, or any new prospect segment/i
    );
    assert.match(composed.message, /Readiness summary:/i);
    assert.match(composed.message, /- Sender identity: confirmed/i);
    assert.match(
      composed.message,
      /- Reply inbox \/ reply-to handling: confirmed/i
    );
    assert.match(
      composed.message,
      /- Operational path: manual send export for operator review selected/i
    );
    assert.match(composed.message, /- Follow-up tracking: confirmed/i);
    assert.match(
      composed.message,
      /- Reply monitoring \/ Batch 1 review: confirmed/i
    );
    assert.match(composed.message, /- Execution lock: active/i);
    assert.match(
      composed.message,
      /Next step remains explicit execute approval if the operator wants to prepare the manual-send export/i
    );
    assert.match(
      composed.message,
      /Do not execute anything automatically/i
    );
    assert.doesNotMatch(
      composed.message,
      /Who will monitor replies, how should responses be reviewed/i
    );
    assert.doesNotMatch(
      composed.message,
      /Do you explicitly approve this execute action/i
    );
    assert.doesNotMatch(composed.message, /Exact action/i);
    assert.equal(looksLikeExecutionRequest(composed.message), false);
  });

  it('classifies final campaign-ready summary without collapsing to a substep', () => {
    const text =
      'Please provide a final Anchor Batch 1 campaign-ready summary for operator review. Include approved Batch 1 scope, confirmed sender identity, confirmed reply handling, selected operational path, confirmed follow-up tracking, confirmed reply monitoring / Batch 1 review, execution lock status, and what explicit execute action would be needed next.';

    assert.equal(looksLikeCampaignReadySummary(text), true);
    assert.equal(detectSelectedReadinessItem(text), null);
    assert.equal(looksLikeReadinessSubstepSelection(text), false);
    assert.equal(
      looksLikeReadinessFieldCorrection(text, {
        activeReadinessItemId: 'operational_path',
      }),
      false
    );
    assert.equal(looksLikeOperatorReadinessCheck(text), false);
    assert.equal(looksLikeExecutionRequest(text), false);
    assert.equal(
      selectConversationMode({
        operatorMessage: text,
        gateAlreadyApproved: true,
        slots: { activeReadinessItemId: 'operational_path' },
      }),
      CONVERSATION_MODES.CAMPAIGN_READY_SUMMARY
    );
    assert.equal(
      selectResponseMode({
        text,
        launchGateApproved: true,
        activeReadinessItemId: 'operational_path',
      }),
      RESPONSE_MODES.CAMPAIGN_READY_SUMMARY
    );
  });

  it('composes full campaign-ready summary from confirmed readiness state', () => {
    const composed = composeCampaignReadySummary({
      businessName: 'Anchor Cleaning',
      batch1Scope: '6 approved cold first-pass prospects in Batch 1.',
      slots: {
        senderIdentityConfirmed: true,
        senderName: 'Jacob Maynard',
        senderEmail: 'jacob@goanchorcleaning.com',
        senderSignature: 'Jacob Maynard, Anchor Cleaning',
        replyInboxConfirmed: true,
        replyHandlingConfirmed: true,
        replyInbox: 'jacob@goanchorcleaning.com',
        replyToMatchesSender: true,
        replyMonitoringOwner: 'Jacob Maynard',
        operationalPathChosen: true,
        operationalPathId: 'manual_send_export',
        operationalPathLabel: 'manual send export for operator review',
        followUpTrackingConfirmed: true,
        followUpTrackingLocation: 'the manual-send export/review sheet',
        replyMonitoringBatch1Confirmed: true,
        batch1ResultsReviewed: true,
        broaderRolloutBlocked: true,
      },
    });

    assert.equal(composed.mode, CONVERSATION_MODES.CAMPAIGN_READY_SUMMARY);
    assert.equal(composed.responseMode, 'campaign_ready_summary');
    assert.equal(composed.executionPending, false);
    assert.equal(composed.exportMade, false);
    assert.equal(composed.sendsMade, false);
    assert.equal(composed.executionLockActive, true);
    assert.match(composed.message, /Anchor Cleaning Batch 1 campaign-ready summary/i);
    assert.match(composed.message, /Batch 1 approved scope/i);
    assert.match(composed.message, /Sender identity confirmed/i);
    assert.match(composed.message, /Jacob Maynard/);
    assert.match(composed.message, /jacob@goanchorcleaning\.com/);
    assert.match(composed.message, /Jacob Maynard, Anchor Cleaning/);
    assert.match(composed.message, /Reply handling confirmed/i);
    assert.match(composed.message, /reply-to matches sender/i);
    assert.match(composed.message, /Jacob Maynard monitors/i);
    assert.match(composed.message, /Operational path selected/i);
    assert.match(
      composed.message,
      /manual send export for operator review/i
    );
    assert.match(composed.message, /Follow-up tracking confirmed/i);
    assert.match(
      composed.message,
      /Reply monitoring \/ Batch 1 review confirmed/i
    );
    assert.match(composed.message, /Execution lock:\s*active/i);
    assert.match(
      composed.message,
      /Next possible execute action:\s*explicitly approve preparing the manual-send export for operator review/i
    );
    assert.match(composed.message, /Nothing has been executed/i);
    assert.doesNotMatch(
      composed.message,
      /Do you explicitly approve this execute action/i
    );
    assert.doesNotMatch(composed.message, /Exact action/i);
    assert.doesNotMatch(
      composed.message,
      /Which readiness item should we resolve first/i
    );
    // Must not collapse to only the latest operational-path confirmation.
    assert.ok(
      composed.message.indexOf('Sender identity confirmed') >= 0 &&
        composed.message.indexOf('Operational path selected') >= 0 &&
        composed.message.indexOf('Reply monitoring / Batch 1 review confirmed') >=
          0
    );
  });

  it('classifies low-signal accidental input as clarification_needed', () => {
    const samples = ['v', 'k', '.', '?', 'x', 'zz'];
    for (const text of samples) {
      assert.equal(
        looksLikeLowSignalAmbiguousInput(text),
        true,
        `expected low-signal for: ${text}`
      );
      const mode = selectConversationMode({
        text,
        operatorMessage: text,
        launchGateApproved: true,
      });
      assert.equal(
        mode,
        CONVERSATION_MODES.CLARIFICATION_NEEDED,
        `expected clarification_needed for: ${text}`
      );
      assert.notEqual(mode, CONVERSATION_MODES.OPERATOR_STATE_UPDATE);
      assert.notEqual(mode, CONVERSATION_MODES.OPERATOR_READINESS_CHECK);
      assert.notEqual(mode, CONVERSATION_MODES.EXECUTION_CONFIRMATION);
    }

    // Known short intents stay intentional.
    assert.equal(looksLikeLowSignalAmbiguousInput('hold'), false);
    assert.equal(looksLikeLowSignalAmbiguousInput('yes'), false);
    assert.equal(looksLikeLowSignalAmbiguousInput('ok'), false);
  });

  it('composes clarification without full state summary or options block', () => {
    const composed = composeClarificationNeeded({
      operatorMessage: 'v',
      launchGateApproved: true,
    });
    assert.equal(composed.mode, CONVERSATION_MODES.CLARIFICATION_NEEDED);
    assert.equal(composed.responseMode, 'clarification_needed');
    assert.match(composed.message, /Not sure what you meant by `v`/i);
    assert.match(composed.message, /readiness-check step/i);
    assert.match(
      composed.message,
      /sender identity, reply handling, follow-up tracking, or hold/i
    );
    assert.doesNotMatch(composed.message, /The next choice is operational/i);
    assert.doesNotMatch(
      composed.message,
      /Which next path do you want to prepare/i
    );
    assert.doesNotMatch(composed.message, /Exact action/i);
    assert.doesNotMatch(
      composed.message,
      /Do you explicitly approve this execute action/i
    );
    assert.equal(composed.requiresExplicitApproval, false);
  });

  it('composes readiness check without execution confirmation structure', () => {
    const composed = composeOperatorReadinessCheck({});
    assert.equal(composed.mode, CONVERSATION_MODES.OPERATOR_READINESS_CHECK);
    assert.match(composed.message, /Still unresolved/i);
    assert.match(composed.message, /Sender identity/i);
    assert.match(composed.message, /Reply handling/i);
    assert.match(composed.message, /Which readiness item should we resolve first/i);
    assert.doesNotMatch(composed.message, /Exact action/i);
    assert.doesNotMatch(composed.message, /Records affected/i);
    assert.doesNotMatch(
      composed.message,
      /Do you explicitly approve this execute action/i
    );
    assert.equal(composed.requiresExplicitApproval, false);
  });

  it('preserves operator-specified seven-item readiness checklist', () => {
    const extracted = extractOperatorReadinessChecklist(
      SEVEN_ITEM_READINESS_MESSAGE
    );
    assert.equal(extracted.length, 7);
    for (const item of SEVEN_ITEM_READINESS_CHECKLIST) {
      assert.ok(
        extracted.some((e) => e.toLowerCase() === item.toLowerCase()),
        `missing extracted item: ${item}`
      );
    }

    const composed = composeOperatorReadinessCheck({
      operatorMessage: SEVEN_ITEM_READINESS_MESSAGE,
    });
    assert.equal(composed.operatorSpecifiedChecklist, true);
    assert.equal(composed.unresolvedItems.length, 7);
    for (const item of SEVEN_ITEM_READINESS_CHECKLIST) {
      assert.match(
        composed.message,
        new RegExp(item.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i')
      );
    }
    // Distinct concepts must not collapse.
    assert.match(composed.message, /reply inbox \/ reply-to handling/i);
    assert.match(composed.message, /reply monitoring owner\/process/i);
    assert.match(composed.message, /operational path is not chosen yet/i);
    assert.match(composed.message, /follow-up tracking process/i);
    assert.match(composed.message, /broader rollout remains blocked/i);
    assert.match(
      composed.message,
      /tracking \/ account settings remain unchanged unless explicitly approved later/i
    );
    assert.ok(composed.message.includes(READINESS_CHECKLIST_SAFETY_LINE));
    assert.match(
      composed.message,
      /Which readiness item should we resolve first\?/
    );
    assert.doesNotMatch(composed.message, /Exact action/i);
  });

  it('keeps confirmed readiness out of the unresolved list', () => {
    const merged = mergeOperatorReadinessChecklist({
      unresolvedItems: [
        'sender identity is not confirmed',
        'reply monitoring owner/process is not confirmed',
      ],
      confirmedReadiness: { sender_identity: true },
      slots: {
        senderName: 'Jacob Maynard',
        senderEmail: 'jacob@goanchorcleaning.com',
        senderSignature: 'Jacob Maynard, Anchor Cleaning',
        senderIdentityConfirmed: true,
      },
    });
    assert.equal(merged.unresolvedItems.length, 1);
    assert.match(merged.unresolvedItems[0], /reply monitoring/i);
    assert.equal(merged.senderIdentityConfirmed, true);
    assert.ok(
      merged.confirmedItems.some((c) => c.concept === 'sender_identity')
    );
    assert.ok(
      !merged.unresolvedItems.some((i) => /sender identity|Sender name:/i.test(i))
    );
  });

  it('states why an inapplicable readiness item does not apply', () => {
    const merged = mergeOperatorReadinessChecklist({
      unresolvedItems: [
        'broader rollout remains blocked until Batch 1 results are reviewed',
      ],
      inapplicableReadiness: {
        broader_rollout_batch1: 'Batch 1 is intentionally paused by operator',
      },
    });
    assert.equal(merged.items.length, 1);
    assert.match(merged.items[0], /inapplicable/i);
    assert.match(merged.items[0], /intentionally paused/i);
  });

  it('composes approved launch gate without renderer sections', () => {
    const composed = formatApprovedLaunchGateConversational(
      {
        title: 'Outreach Launch Gate',
        status: 'approved_readiness_only',
        launchGateApproved: true,
      },
      { justApproved: true }
    );
    assert.equal(composed.mode, CONVERSATION_MODES.OPERATOR_STATE_UPDATE);
    assert.equal(
      composed.message,
      [
        'Outreach Launch Gate is approved for readiness only. Nothing external happened: no send, no export, no CRM write, and no account changes. Execution is still locked.',
        '',
        'The next choice is operational:',
        '1. prepare a manual-send export for review',
        '2. create CRM drafts, if explicitly approved',
        '3. queue sends later, if execution is intentionally enabled',
        '4. hold with no action',
        '',
        "I'd keep this held until sender identity and reply handling are confirmed.",
        '',
        'Which next path do you want to prepare, if any?',
      ].join('\n')
    );
    assert.equal(containsRendererBoilerplate(composed.message), false);
    assert.doesNotMatch(composed.message, /Recommended decision/i);
    assert.doesNotMatch(composed.message, /Primary actions/i);
    assert.doesNotMatch(composed.message, /Does this look right to approve/i);
  });

  it('dedupes stacked approved-state acknowledgments', () => {
    const {
      dedupeOperatorStateUpdateMessage,
    } = require('../services/maxSynthesis');
    const stacked = [
      'Outreach Launch Gate: approved for readiness only.',
      '',
      'Launch Gate is already approved for readiness only. Nothing external happened.',
      '',
      'Launch Gate is approved for readiness only.',
      '',
      'Nothing external happened: no send, no export, no CRM write, and no account changes. The campaign is now campaign-ready, but execution is still locked.',
      '',
      'The next choice is operational:',
      '1. prepare a manual-send export for review',
      '2. create CRM drafts, if explicitly approved',
      '3. queue sends later, if execution is intentionally enabled',
      '4. hold with no action',
      '',
      "I'd keep this held until sender identity and reply handling are confirmed.",
      '',
      'Which next path do you want to prepare, if any?',
    ].join('\n');
    const cleaned = dedupeOperatorStateUpdateMessage(stacked);
    assert.equal(
      (cleaned.match(/approved for readiness only/gi) || []).length,
      1
    );
    assert.equal(
      (cleaned.match(/nothing external happened/gi) || []).length,
      1
    );
    assert.equal(
      (cleaned.match(/execution is still locked/gi) || []).length,
      1
    );
    assert.equal(
      (cleaned.match(/The next choice is operational/g) || []).length,
      1
    );
    assert.equal(
      (cleaned.match(/Which next path do you want to prepare/g) || []).length,
      1
    );
    assert.match(
      cleaned,
      /^Outreach Launch Gate is approved for readiness only\. Nothing external happened:/
    );
  });

  it('drops duplicative approved-state leadIns', () => {
    const composed = formatApprovedLaunchGateConversational(
      { status: 'approved_readiness_only', launchGateApproved: true },
      {
        leadIn:
          'Launch Gate is already approved for readiness only. Nothing external happened.',
      }
    );
    assert.equal(
      (composed.message.match(/approved for readiness only/gi) || []).length,
      1
    );
    assert.equal(
      (composed.message.match(/nothing external happened/gi) || []).length,
      1
    );
    assert.doesNotMatch(composed.message, /already approved/i);
  });

  it('uses state-aware approval language', () => {
    const pending = approvalLanguageForGate({
      gateName: 'Outreach Launch Gate',
      approved: false,
    });
    assert.match(pending.ask, /approve this|revisions/i);

    const approved = approvalLanguageForGate({
      gateName: 'Launch Gate',
      approved: true,
    });
    assert.match(approved.statement, /approved for readiness only/i);
    assert.match(approved.ask, /next path/i);

    const exec = approvalLanguageForGate({ executionPending: true });
    assert.match(exec.ask, /explicitly approve this execute action/i);
  });

  it('diagnostic leads with plain language', () => {
    const composed = formatOperatorDiagnosticMessage({});
    assert.match(composed.message, /I found the problem/i);
    assert.match(composed.message, /stopping before showing another stale draft/i);
    assert.ok(
      composed.message.indexOf('I found the problem') <
        composed.message.indexOf('Technical detail') ||
        !composed.message.includes('Technical detail')
    );
  });

  it('execution confirmation is explicit and locked', () => {
    const composed = composeConversationResponse(
      CONVERSATION_MODES.EXECUTION_CONFIRMATION,
      {
        action: 'prepare a manual-send export',
        recordsAffected: '7 Batch 1 prospects',
        sender: 'Anchor sender',
      }
    );
    assert.match(composed.message, /Exact action/);
    assert.match(composed.message, /Records affected/);
    assert.match(composed.message, /Sender \/ account/);
    assert.match(composed.message, /explicitly approve this execute action/i);
    assert.equal(composed.requiresExplicitApproval, true);
    assert.equal(composed.sendsMade, false);
  });

  it('compact safety avoids full boilerplate list', () => {
    const line = compactSafetyLockLine();
    assert.match(line, /remain locked/i);
    assert.doesNotMatch(line, /No DNS changes/);
  });

  it('assessment answers composition questions', () => {
    const a = assessConversationContext({
      operatorMessage: 'Approve the Outreach Launch Gate',
      justApproved: true,
      step: 'outreach_launch_gate',
    });
    assert.equal(a.stateChanged, true);
    assert.equal(a.shortestUseful, 'state_then_next_options');
  });

  it('applyConversationalPolicy tags replies with conversationMode', () => {
    const next = applyConversationalPolicy(
      {
        message:
          'Outreach Launch Gate is approved for readiness only. Nothing external happened: no send, no export, no CRM write, and no account changes. Execution is still locked.',
        responseMode: RESPONSE_MODES.OPERATOR_STATE_SUMMARY,
        launchGateApproved: true,
        intent: 'outreach_launch_gate_approved',
      },
      { gateAlreadyApproved: true }
    );
    assert.equal(next.conversationMode, CONVERSATION_MODES.OPERATOR_STATE_UPDATE);
    assert.equal(next.responseMode, RESPONSE_MODES.OPERATOR_STATE_SUMMARY);
  });

  it('legacy selectResponseMode still returns wire values', () => {
    assert.equal(
      selectResponseMode({
        intent: 'outreach_launch_gate_approved',
        launchGateApproved: true,
      }),
      RESPONSE_MODES.OPERATOR_STATE_SUMMARY
    );
    assert.equal(
      selectResponseMode({
        isInitialReviewGate: true,
        intent: 'produce_outreach_draft_preview',
      }),
      RESPONSE_MODES.WORKFLOW_REVIEW_CARD
    );
    assert.equal(
      selectResponseMode({ executionPending: true }),
      RESPONSE_MODES.EXECUTION_CONFIRMATION
    );
    assert.equal(
      selectResponseMode({
        text: 'v',
        launchGateApproved: true,
      }),
      RESPONSE_MODES.CLARIFICATION_NEEDED
    );
  });
});
