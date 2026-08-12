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
    assert.match(
      composed.message,
      /Outreach Launch Gate: approved for readiness only\./
    );
    assert.match(composed.message, /Launch Gate is now approved for readiness only/);
    assert.match(composed.message, /campaign-ready/);
    assert.match(composed.message, /next choice is operational/);
    assert.match(composed.message, /1\. prepare a manual-send export/);
    assert.match(composed.message, /I'd keep this held/);
    assert.equal(containsRendererBoilerplate(composed.message), false);
    assert.doesNotMatch(composed.message, /Recommended decision/i);
    assert.doesNotMatch(composed.message, /Primary actions/i);
    assert.doesNotMatch(composed.message, /Does this look right to approve/i);
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
        message: 'Outreach Launch Gate: approved for readiness only.',
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
