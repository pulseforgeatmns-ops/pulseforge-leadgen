'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const { classifyAoMaxIntent, extractBriefingTarget } = require('../utils/aoMaxIntent');
const {
  mapAccountRow,
  computeRankScore,
  comparePrioritizedAccounts,
  formatPrioritizationResponse,
  buildCoachingReply,
} = require('../utils/aoAccountPrioritization');

function taskRow(overrides = {}) {
  return {
    task_id: 'task-1',
    lead_id: 'lead-1',
    ao_owner_id: 42,
    client_id: 10,
    business_name: 'Acme Law Group',
    address: '1 Main St',
    lead_status: 'needs_follow_up',
    interest_level: 'medium',
    priority: 'normal',
    due_date: '2026-09-16',
    next_action: 'in_person_revisit',
    suggested_message: null,
    waiting_on_jake: false,
    task_status: 'open',
    last_interaction_summary: 'Spoke with office manager',
    attribution_source: 'direct_mail_campaign',
    campaign_name: 'Campaign 001',
    original_visit_note: 'Interested but wants to think',
    probe_answers: null,
    contact_name: 'Pat',
    contact_title: 'Office Manager',
    contact_phone: '603-555-0100',
    is_decision_maker: false,
    open_escalation_id: null,
    open_escalation_status: null,
    ...overrides,
  };
}

test('classifyAoMaxIntent routes account prioritization questions', () => {
  const samples = [
    'What accounts should I focus on today?',
    'What are my top accounts?',
    'Who should I work first?',
    'What should I prioritize?',
    'Where should I go today?',
    'Who needs attention?',
    'What follow-ups are most important?',
    'What should I do next?',
  ];
  for (const question of samples) {
    const result = classifyAoMaxIntent(question);
    assert.equal(result.intent, 'account_prioritization', question);
  }
});

test('classifyAoMaxIntent routes coaching questions', () => {
  assert.equal(classifyAoMaxIntent('How should I handle a gatekeeper?').intent, 'coaching');
  assert.equal(classifyAoMaxIntent('What should I say when they already have a cleaner?').intent, 'coaching');
  assert.equal(classifyAoMaxIntent('Help me with this conversation').intent, 'coaching');
});

test('classifyAoMaxIntent routes account briefing questions', () => {
  const result = classifyAoMaxIntent('Brief me on Blue Door Property Management');
  assert.equal(result.intent, 'account_briefing');
  assert.equal(result.briefingTarget, 'Blue Door Property Management');
  assert.equal(extractBriefingTarget('What do I know about Acme CPA?'), 'Acme CPA');
});

test('formatPrioritizationResponse includes account fields and start recommendation', () => {
  const reply = formatPrioritizationResponse([
    {
      business_name: 'ABC Property Management',
      why_now: 'Follow-up is due today.',
      status_label: 'Follow-up needed · high interest · direct mail',
      next_step: 'Reach the property manager and ask for a 10-minute walkthrough.',
    },
  ], { hasOverdue: true });

  assert.match(reply, /ABC Property Management/);
  assert.match(reply, /Why: Follow-up is due today\./);
  assert.match(reply, /Status:/);
  assert.match(reply, /Next:/);
  assert.match(reply, /Start with ABC Property Management because follow-up is due today/i);
});

test('formatPrioritizationResponse returns explicit empty state', () => {
  const reply = formatPrioritizationResponse([]);
  assert.match(reply, /don't currently have any assigned accounts/i);
  assert.doesNotMatch(reply, /Acme|Example|ABC Property/i);
});

test('overdue follow-up outranks untouched low-priority account', () => {
  const today = '2026-09-16';
  const overdue = mapAccountRow(taskRow({
    business_name: 'Overdue Firm',
    due_date: '2026-09-14',
    priority: 'normal',
    interest_level: 'low',
  }), today);
  const untouched = mapAccountRow(taskRow({
    business_name: 'Untouched Firm',
    due_date: '2026-09-20',
    priority: 'normal',
    interest_level: null,
    lead_status: 'new_visit',
    last_interaction_summary: null,
    original_visit_note: null,
    attribution_source: 'ao_field_visit',
  }), today);

  assert.ok(overdue.rank_score > untouched.rank_score);
});

test('warm walkthrough candidate ranks above normal untouched account', () => {
  const today = '2026-09-16';
  const warmWalkthrough = mapAccountRow(taskRow({
    business_name: 'Warm Walkthrough Co',
    lead_status: 'walkthrough_requested',
    priority: 'warm',
    interest_level: 'high',
    due_date: today,
    next_action: 'Book walkthrough',
  }), today);
  const normal = mapAccountRow(taskRow({
    business_name: 'Normal Co',
    priority: 'normal',
    interest_level: 'medium',
    due_date: '2026-09-20',
  }), today);

  assert.ok(warmWalkthrough.rank_score > normal.rank_score);
});

test('computeRankScore deprioritizes waiting-on-jake tasks', () => {
  const today = '2026-09-16';
  const state = 'follow_up_needed';
  const actionable = computeRankScore(taskRow({ waiting_on_jake: false }), state, today);
  const waiting = computeRankScore(taskRow({ waiting_on_jake: true }), state, today);
  assert.ok(actionable > waiting);
});

test('buildCoachingReply still returns gatekeeper coaching', () => {
  const result = buildCoachingReply('How should I handle a gatekeeper?');
  assert.equal(result.intent, 'coaching');
  assert.match(result.reply, /decision-maker/i);
  assert.doesNotMatch(result.reply, /Start with/i);
});

test('assigned account queries are scoped to AO owner and tenant', () => {
  const fs = require('node:fs');
  const path = require('node:path');
  const src = fs.readFileSync(path.join(__dirname, '..', 'services', 'aoAccountIntelligence.js'), 'utf8');
  assert.match(src, /t\.ao_owner_id = \$1/);
  assert.match(src, /l\.client_id = \$2/);
  assert.match(src, /l\.ao_owner_id = \$1/);
});

test('conversation path uses account intelligence instead of generic safeGuidance fallback', () => {
  const fs = require('node:fs');
  const path = require('node:path');
  const aoMaxFlowSrc = fs.readFileSync(path.join(__dirname, '..', 'services', 'aoMaxFlow.js'), 'utf8');
  const aoMaxConversationSrc = fs.readFileSync(path.join(__dirname, '..', 'services', 'aoMaxConversation.js'), 'utf8');
  const { classifyAoMaxIntent } = require('../utils/aoMaxIntent');
  assert.match(aoMaxFlowSrc, /handleConversationTurn/);
  assert.match(aoMaxConversationSrc, /buildAccountPrioritizationReply/);
  assert.match(aoMaxConversationSrc, /buildAccountBriefingReply/);
  assert.equal(classifyAoMaxIntent('What accounts should I focus on today?').intent, 'account_prioritization');
});
