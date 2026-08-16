'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const {
  deriveOperationalState,
  deriveCampaignOutcome,
  recommendCrmPromotion,
  compareOperationalPriority,
  buildRelationshipIntel,
  mapEscalationUrgency,
  recommendEscalationAction,
} = require('../utils/aoOperationalState');

function baseLead(overrides = {}) {
  return {
    status: 'new_visit',
    interest_level: 'medium',
    original_visit_note: null,
    probe_answers: null,
    contact_name: null,
    is_decision_maker: false,
    waiting_on_jake: false,
    open_escalation_id: null,
    open_escalation_status: null,
    open_task_status: 'open',
    attribution_source: 'direct_mail_campaign',
    open_next_action: 'in_person_revisit',
    ...overrides,
  };
}

test('deriveOperationalState maps walkthrough_requested', () => {
  assert.equal(deriveOperationalState(baseLead({ status: 'walkthrough_requested' })), 'walkthrough_requested');
});

test('deriveOperationalState maps open escalation to jake_action_needed', () => {
  assert.equal(
    deriveOperationalState(baseLead({
      status: 'needs_follow_up',
      open_escalation_id: 'esc-1',
      open_escalation_status: 'new',
    })),
    'jake_action_needed'
  );
});

test('deriveOperationalState maps decision maker contact', () => {
  assert.equal(
    deriveOperationalState(baseLead({
      status: 'needs_follow_up',
      contact_name: 'Jerry',
      is_decision_maker: true,
    })),
    'decision_maker_reached'
  );
});

test('deriveOperationalState maps direct mail seed to not_started', () => {
  assert.equal(
    deriveOperationalState(baseLead({
      status: 'needs_follow_up',
      original_visit_note: 'Received direct mail before AO visit',
      open_task_status: 'open',
    })),
    'not_started'
  );
});

test('deriveOperationalState maps disqualified statuses', () => {
  assert.equal(deriveOperationalState(baseLead({ status: 'not_a_fit' })), 'disqualified');
  assert.equal(deriveOperationalState(baseLead({ crm_prospect_id: 42, status: 'converted_to_crm' })), 'converted_to_crm');
});

test('recommendCrmPromotion requires buying signals', () => {
  const warm = recommendCrmPromotion(baseLead({
    status: 'walkthrough_requested',
    is_decision_maker: true,
    interest_level: 'high',
  }));
  assert.equal(warm.eligible, true);
  assert.ok(warm.reasons.includes('Walkthrough requested'));

  const cold = recommendCrmPromotion(baseLead({ status: 'new_visit', original_visit_note: 'No answer' }));
  assert.equal(cold.eligible, false);
});

test('compareOperationalPriority ranks walkthrough above visited', () => {
  assert.ok(compareOperationalPriority(
    baseLead({ status: 'walkthrough_requested' }),
    baseLead({ status: 'new_visit' })
  ) < 0);
});

test('buildRelationshipIntel extracts vendor complaints from probe answers', () => {
  const intel = buildRelationshipIntel(baseLead({
    probe_answers: { outside_cleaner: 'ABC Cleaning', cleaner_issues: 'Missed Fridays' },
    original_visit_note: 'They want another quote',
  }));
  assert.equal(intel.current_vendor, 'ABC Cleaning');
  assert.equal(intel.current_pain, 'Missed Fridays');
  assert.equal(intel.price_shopping_risk, 'likely');
});

test('mapEscalationUrgency elevates walkthrough requests', () => {
  assert.equal(mapEscalationUrgency({ reason: 'walkthrough_request', status: 'new' }), 'high');
});

test('recommendEscalationAction is action-oriented', () => {
  const action = recommendEscalationAction({ reason: 'walkthrough_request' });
  assert.match(action, /walkthrough/i);
});

test('deriveCampaignOutcome maps operational states', () => {
  assert.equal(deriveCampaignOutcome(baseLead({ status: 'walkthrough_requested' })), 'walkthrough_requested');
  assert.equal(
    deriveCampaignOutcome(baseLead({
      status: 'needs_follow_up',
      original_visit_note: 'Received direct mail before AO visit',
      open_task_status: 'open',
    })),
    'not_started'
  );
});
