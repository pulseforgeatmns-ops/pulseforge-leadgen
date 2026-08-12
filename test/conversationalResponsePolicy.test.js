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
  compactSafetyLockLine,
  assessConversationContext,
  applyConversationalPolicy,
  selectResponseMode,
} = require('../services/maxSynthesis');

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
  });
});
