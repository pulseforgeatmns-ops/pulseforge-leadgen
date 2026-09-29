'use strict';

/**
 * SPEC-211 — Informed Execution-Approval Clarification
 * When operator gives ambiguous response to execution_approval decision,
 * clarification message must include canonical executionReview.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const { STAGES, OPERATOR_DECISION_KINDS } = amo;
const {
  maybeHandlePendingDecisionTurn,
  buildClarifyProse,
} = require('../PendingDecisionTurn');
const {
  RESOLUTION_OUTCOMES,
  resolvePendingOperatorDecision,
} = require('../PendingDecisionResolver');

const OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester NH for law firms.';

describe('SPEC-211 — Informed Execution-Approval Clarification', () => {
  it('buildClarifyProse includes executionReview for execution_approval + AMBIGUOUS', () => {
    // Setup: Create a resolution with execution_approval + AMBIGUOUS outcome
    const resolution = {
      outcome: RESOLUTION_OUTCOMES.AMBIGUOUS,
      decisionKind: OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL,
      prompt: 'Authorize external execution of prepared outreach?',
      missionId: 'test-mission-id',
    };

    // Setup: Create a mock snapshot with executionReview
    const snapshot = {
      mission: {
        id: 'test-mission-id',
        objective: OBJECTIVE,
      },
      executionReview: {
        spec: 'execution_review',
        missionId: 'test-mission-id',
        stage: STAGES.READY,
        targets: [
          {
            rank: 1,
            company: 'ABC Law Firm',
            priorityReason: 'High ICP score',
            buyingSignals: ['recent hiring'],
            scoutEvidence: ['located in service area'],
          },
        ],
        communication: {
          subject: 'Your commercial cleaning needs',
          body: 'We provide eco-friendly cleaning solutions...',
          cta: 'Schedule a call',
          selectedVariant: 'Primary',
          variantCount: 1,
        },
        infrastructure: {
          queue: [
            { prospect_id: 1, prospect_name: 'ABC Law Firm', email: 'contact@abclaw.com' },
          ],
          safeCapacity: 1,
          timingRecommendation: [],
          deliverabilityStatus: 'healthy',
          governorOutcome: 'ready',
          governorReason: null,
          reputationWarnings: [],
        },
        decision: {
          summary: 'Authorize external execution of the prepared outreach queue.',
          plannedSendCount: 1,
          onApproval: 'Up to 1 prepared send(s) become eligible for provider execution.',
          blockers: [],
          unknowns: [],
        },
      },
    };

    // Execute: Build clarify prose with ambiguous execution_approval
    const prose = buildClarifyProse('continuee', resolution, snapshot);

    // Verify: Prose includes the executionReview sections
    assert.ok(prose.includes('Execution Ready'), 'Should include "Execution Ready" header');
    assert.ok(prose.includes('Targets'), 'Should include Targets section');
    assert.ok(prose.includes('ABC Law Firm'), 'Should include target company name');
    assert.ok(prose.includes('Channel'), 'Should include Channel section');
    assert.ok(prose.includes('Message'), 'Should include Message section');
    assert.ok(prose.includes('Your commercial cleaning needs'), 'Should include email subject');
    assert.ok(prose.includes('Outbound Plan'), 'Should include Outbound Plan section');
    assert.ok(prose.includes('Safety / Delivery'), 'Should include Safety/Delivery section');
    assert.ok(
      prose.includes('Authorize external execution'),
      'Should include approval prompt'
    );
    assert.ok(
      prose.includes(
        "I didn't catch a clear yes or no for the pending decision"
      ),
      'Should include clarification prefix'
    );
  });

  it('buildClarifyProse handles missing executionReview gracefully', () => {
    const resolution = {
      outcome: RESOLUTION_OUTCOMES.AMBIGUOUS,
      decisionKind: OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL,
      prompt: 'Authorize external execution of prepared outreach?',
      missionId: 'test-mission-id',
    };

    // Snapshot without executionReview
    const snapshot = {
      mission: {
        id: 'test-mission-id',
        objective: OBJECTIVE,
      },
    };

    const prose = buildClarifyProse('continuee', resolution, snapshot);

    // Should still show the prompt even without executionReview
    assert.ok(
      prose.includes("I didn't catch a clear yes or no"),
      'Should include clarification message'
    );
    assert.ok(
      prose.includes('Authorize external execution'),
      'Should include approval prompt'
    );
  });

  it('buildClarifyProse does not include executionReview for other decision kinds', () => {
    const resolution = {
      outcome: RESOLUTION_OUTCOMES.AMBIGUOUS,
      decisionKind: OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL,
      prompt: 'Approve discovery?',
      missionId: 'test-mission-id',
    };

    const snapshot = {
      mission: {
        id: 'test-mission-id',
        objective: OBJECTIVE,
      },
      executionReview: {
        // This should be ignored for non-execution_approval decisions
        spec: 'execution_review',
        targets: [{ company: 'Test Company' }],
      },
    };

    const prose = buildClarifyProse('continuee', resolution, snapshot);

    // Should NOT include execution review details for discovery_approval
    assert.ok(
      !prose.includes('Execution Ready'),
      'Should not include Execution Ready for discovery_approval'
    );
    assert.ok(
      !prose.includes('Targets'),
      'Should not include Targets section for discovery_approval'
    );
    assert.ok(
      prose.includes("I didn't catch a clear yes or no"),
      'Should include clarification message'
    );
    assert.ok(
      prose.includes('Approve discovery'),
      'Should include discovery prompt'
    );
  });

  it('buildClarifyProse for ambiguous non-execution_approval keeps original behavior', () => {
    const resolution = {
      outcome: RESOLUTION_OUTCOMES.AMBIGUOUS,
      decisionKind: OPERATOR_DECISION_KINDS.PLAN_APPROVAL,
      prompt: 'Approve the plan?',
      missionId: 'test-mission-id',
    };

    const snapshot = {
      mission: {
        id: 'test-mission-id',
        objective: OBJECTIVE,
      },
    };

    const prose = buildClarifyProse('maybe', resolution, snapshot);

    // Should use original clarification message
    assert.ok(
      prose.includes("I didn't catch a clear yes or no"),
      'Should use original clarification message'
    );
    assert.ok(
      prose.includes('Approve the plan?'),
      'Should include plan approval prompt'
    );
    assert.ok(
      !prose.includes('Execution Ready'),
      'Should not include Execution Ready for plan_approval'
    );
  });
});
