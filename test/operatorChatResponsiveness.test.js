'use strict';

/**
 * Max Chat Responsiveness unit tests.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  RESPONSE_MODES,
  PRIORITY_ORDER,
  parseOperatorChatDirectives,
  looksLikeOperatorWorkflowRevision,
  selectResponseMode,
  validateOutreachDraftAgainstInstructions,
  buildFollowUpEmailDrafts,
  formatOperatorChatDraftResponse,
  ensureCampaignWorkingState,
  applyOperatorDirectivesToWorkingState,
} = require('../services/maxSynthesis/OperatorChatResponsiveness');

describe('OperatorChatResponsiveness', () => {
  it('parses Anchor draft correction directives', () => {
    const parsed = parseOperatorChatDirectives(
      'Revise the Outreach Draft Preview. Use `{{business_name}} - commercial cleaning`; no street addresses; draft actual follow-ups; answer like an LLM/operator, not a workflow renderer.'
    );
    assert.equal(parsed.hasDirectives, true);
    assert.equal(
      parsed.learnings.tested_subject_line_pattern,
      '{{business_name}} - commercial cleaning'
    );
    assert.equal(parsed.learnings.subject_keep_merge_tokens, true);
    assert.equal(parsed.learnings.claim_tested_winner, false);
    assert.equal(parsed.learnings.draft_follow_ups, true);
    assert.equal(
      parsed.learnings.response_mode_preference,
      RESPONSE_MODES.OPERATOR_CHAT_RESPONSE
    );
    assert.ok(parsed.directives.some((d) => d.type === 'no_street_addresses'));
  });

  it('selects operator chat response for revision turns', () => {
    const mode = selectResponseMode({
      text: 'Revise the Outreach Draft Preview. Use {{business_name}} - commercial cleaning',
      step: 'outreach_draft_preview',
      messageClass: 'refinement_feedback',
      priorOutreachDraftPreview: { kind: 'outreach_draft_preview' },
    });
    assert.equal(mode, RESPONSE_MODES.OPERATOR_CHAT_RESPONSE);
  });

  it('keeps workflow review card for initial draft production', () => {
    const mode = selectResponseMode({
      text: 'Create the Outreach Draft Preview',
      isInitialReviewGate: true,
      intent: 'produce_outreach_draft_preview',
    });
    assert.equal(mode, RESPONSE_MODES.WORKFLOW_REVIEW_CARD);
  });

  it('validates Anchor draft acceptance criteria', () => {
    const drafts = buildFollowUpEmailDrafts({
      businessName: 'Anchor',
      subject: '{{business_name}} - commercial cleaning',
    });
    const preview = {
      subjectOptions: ['{{business_name}} - commercial cleaning'],
      firstTouchBody: [
        'Hi {{first_name}},',
        '',
        'Anchor helps property managers across {{town}} who want reliable commercial cleaning without chasing vendors.',
        '',
        'Anchor focuses on reliability, responsiveness, accountability, and fewer vendor-chasing headaches.',
      ].join('\n'),
      personalizationByProspect: [
        {
          companyName: 'Acme',
          personalizationNote: 'Reference {{town}} and portfolio cues only.',
        },
      ],
      followUpDrafts: drafts,
      sendsMade: false,
    };
    const ok = validateOutreachDraftAgainstInstructions(preview, {
      learnings: {
        tested_subject_line_pattern: '{{business_name}} - commercial cleaning',
        subject_keep_merge_tokens: true,
        claim_tested_winner: false,
        draft_follow_ups: true,
        copy_differentiator:
          'reliability, responsiveness, accountability, fewer vendor-chasing headaches',
      },
      responseMode: RESPONSE_MODES.OPERATOR_CHAT_RESPONSE,
      message: formatOperatorChatDraftResponse(preview, {
        changes: ['subject updated'],
      }),
    });
    assert.equal(ok.ok, true, JSON.stringify(ok.failures));
  });

  it('flags workflow card boilerplate in operator chat mode', () => {
    const result = validateOutreachDraftAgainstInstructions(
      {
        subjectOptions: ['{{business_name}} - commercial cleaning'],
        firstTouchBody:
          'Anchor helps property managers across {{town}} with reliability and responsiveness.',
        followUpDrafts: buildFollowUpEmailDrafts({ businessName: 'Anchor' }),
        personalizationByProspect: [],
      },
      {
        learnings: {
          tested_subject_line_pattern:
            '{{business_name}} - commercial cleaning',
          subject_keep_merge_tokens: true,
          claim_tested_winner: false,
          draft_follow_ups: true,
        },
        responseMode: RESPONSE_MODES.OPERATOR_CHAT_RESPONSE,
        message: '## Primary actions\n## View evidence\ntested winner',
      }
    );
    assert.equal(result.ok, false);
    assert.ok(
      result.failures.some((f) => f.code === 'workflow_card_boilerplate')
    );
  });

  it('writes operator directives into working state', () => {
    const parsed = parseOperatorChatDirectives(
      'no street addresses; draft actual follow-ups'
    );
    const working = applyOperatorDirectivesToWorkingState(
      ensureCampaignWorkingState({}),
      parsed,
      { activeArtifactKind: 'outreach_draft_preview' }
    );
    assert.equal(working.activeArtifactKind, 'outreach_draft_preview');
    assert.ok(working.latestOperatorInstruction);
    assert.ok(working.appliedDirectives.length >= 2);
  });

  it('priority order puts operator instructions above templates', () => {
    assert.equal(PRIORITY_ORDER[0], 'system_safety');
    assert.equal(PRIORITY_ORDER[1], 'latest_operator_instruction');
    assert.ok(
      PRIORITY_ORDER.indexOf('latest_operator_instruction') <
        PRIORITY_ORDER.indexOf('default_templates_renderers')
    );
    assert.ok(
      PRIORITY_ORDER.indexOf('active_workflow_state') <
        PRIORITY_ORDER.indexOf('evidence_source_records')
    );
  });

  it('detects draft revision asks on active draft step', () => {
    assert.equal(
      looksLikeOperatorWorkflowRevision(
        'Revise the Outreach Draft Preview. Use {{business_name}} - commercial cleaning',
        { step: 'outreach_draft_preview' }
      ),
      true
    );
    assert.equal(
      looksLikeOperatorWorkflowRevision('Approve the Outreach Draft Preview', {
        step: 'outreach_draft_preview',
      }),
      false
    );
  });
});
