'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const {
  formatPrioritizationResponse,
  mapAccountRow,
} = require('../utils/aoAccountPrioritization');
const {
  formatAccountBriefing,
  formatAccountContactsReply,
  formatFollowUpTiming,
  sanitizeAoFacingText,
} = require('../utils/aoAccountBriefing');
const { resolveConversationIntent } = require('../utils/aoConversationContext');
const { extractBriefingTarget } = require('../utils/aoMaxIntent');

const TODAY = '2026-09-16';
const CRM_PROSPECT_ID = '11111111-1111-1111-1111-111111111111';
const CRM_COMPANY_ID = '22222222-2222-2222-2222-222222222222';
const SOURCE_CONVERSATION_ID = '33333333-3333-3333-3333-333333333333';
const BATCH_ID = '44444444-4444-4444-4444-444444444444';

function assignmentNote({
  company,
  owner = 'Zack',
  lane = 'Property Management',
  priority = 'High',
  pipelineStage = 'research',
  nextAction = 'research',
  dueDate = '2026-09-14',
} = {}) {
  const metadata = {
    batch_id: BATCH_ID,
    source_conversation_id: SOURCE_CONVERSATION_ID,
    company,
    owner,
    ao_owner_id: 26,
    status: 'needs_follow_up',
    pipeline_stage: pipelineStage,
    next_action: nextAction,
    source: 'AO Assignment',
    mission_type: 'recurring_commercial_acquisition',
    lane,
    priority,
    initial_next_action: 'Research decision-maker',
    due_date: dueDate,
    crm_prospect_id: CRM_PROSPECT_ID,
    crm_company_id: CRM_COMPANY_ID,
  };

  return [
    '[AO Assignment | ao-assignment-2026-09-14-canonical-45]',
    `${company} — ${owner}`,
    'Research decision-maker',
    'Research → identify decision-maker → identify likely cleaning situation/gap → choose call/email/in-person approach → execute.',
    '',
    'Metadata:',
    JSON.stringify(metadata, null, 2),
  ].join('\n');
}

function leadFixture(overrides = {}) {
  return {
    business_name: 'Brady Sullivan Properties',
    address: '1000 Elm St, Manchester NH',
    business_type: 'property_management',
    status: 'needs_follow_up',
    interest_level: null,
    priority: 'high',
    contact_name: null,
    contact_title: null,
    contact_phone: null,
    is_decision_maker: false,
    open_next_action: 'research',
    open_task_due: new Date('2026-09-14T00:00:00.000Z'),
    waiting_on_jake: false,
    last_interaction_summary: assignmentNote({ company: 'Brady Sullivan Properties' }),
    original_visit_note: assignmentNote({ company: 'Brady Sullivan Properties' }),
    probe_answers: null,
    open_escalation_id: null,
    open_escalation_status: null,
    crm_prospect_id: CRM_PROSPECT_ID,
    ao_owner_id: 26,
    ...overrides,
  };
}

function taskRow(overrides = {}) {
  return {
    task_id: 'task-1',
    lead_id: 'lead-1',
    ao_owner_id: 26,
    client_id: 10,
    business_name: 'Anagnost Companies',
    address: '1 Main St',
    business_type: null,
    lead_status: 'needs_follow_up',
    interest_level: null,
    priority: 'high',
    due_date: '2026-09-14',
    next_action: 'research',
    suggested_message: null,
    waiting_on_jake: false,
    task_status: 'open',
    last_interaction_summary: assignmentNote({
      company: 'Anagnost Companies',
      lane: 'Development',
    }),
    original_visit_note: null,
    probe_answers: null,
    contact_name: null,
    contact_title: null,
    contact_phone: null,
    is_decision_maker: false,
    open_escalation_id: null,
    open_escalation_status: null,
    ...overrides,
  };
}

function assertNoInternalLeak(text) {
  assert.doesNotMatch(text, /Metadata:/);
  assert.doesNotMatch(text, /crm_company_id/);
  assert.doesNotMatch(text, /crm_prospect_id/);
  assert.doesNotMatch(text, /ao_owner_id/);
  assert.doesNotMatch(text, /source_conversation_id/);
  assert.doesNotMatch(text, /batch_id/);
  assert.doesNotMatch(text, /ao-assignment-2026-09-14-canonical-45/);
  assert.doesNotMatch(text, new RegExp(CRM_PROSPECT_ID));
  assert.doesNotMatch(text, new RegExp(CRM_COMPANY_ID));
  assert.doesNotMatch(text, new RegExp(SOURCE_CONVERSATION_ID));
  assert.doesNotMatch(text, /GMT\+0000/);
  assert.doesNotMatch(text, /Coordinated Universal Time/);
  assert.doesNotMatch(text, /\{[\s\S]*"pipeline_stage"[\s\S]*\}/);
}

test('briefing does not include raw metadata JSON or internal identifiers', () => {
  const reply = formatAccountBriefing(leadFixture(), { today: TODAY });
  assertNoInternalLeak(reply);
  assert.match(reply, /Briefing — Brady Sullivan Properties/);
  assert.doesNotMatch(reply, /Next:\s*Research\s*$/m);
});

test('research-stage account with no contact gets a specific decision-maker action', () => {
  const reply = formatAccountBriefing(leadFixture(), { today: TODAY });
  assert.match(reply, /Research stage/i);
  assert.match(reply, /Identify the facilities\/property-management decision-maker/i);
  assert.match(reply, /property manager/i);
  assert.match(reply, /No contact identified yet/i);
  assert.doesNotMatch(reply, /\bZack\b/);
  assert.match(reply, /Overdue since Sep 14/);
  assert.match(reply, /Assigned through Anchor AO batch/);
});

test('contact identified with no conversation gets a specific outreach action', () => {
  const reply = formatAccountBriefing(leadFixture({
    contact_name: 'Pat Hale',
    contact_title: 'Office Manager',
    contact_phone: '603-555-0100',
    last_interaction_summary: assignmentNote({ company: 'Brady Sullivan Properties' }),
    original_visit_note: assignmentNote({ company: 'Brady Sullivan Properties' }),
    open_next_action: 'research',
  }), { today: TODAY });

  assert.match(reply, /Call Pat Hale at 603-555-0100/i);
  assert.match(reply, /backup\/overflow cleaning resource/i);
  assert.doesNotMatch(reply, /Next:\s*Research\b/);
  assertNoInternalLeak(reply);
});

test('follow-up due with prior conversation references real prior context', () => {
  const reply = formatAccountBriefing(leadFixture({
    contact_name: 'Sarah Chen',
    contact_title: 'Property Manager',
    is_decision_maker: true,
    last_interaction_summary: 'Sarah asked about overflow coverage on the Elm Street portfolio and wanted a callback after Monday.',
    original_visit_note: 'Met Sarah in the lobby.',
    open_next_action: 'phone_follow_up',
    open_task_due: '2026-09-16',
    business_type: 'property_management',
  }), { today: TODAY });

  assert.match(reply, /Follow up with Sarah Chen about overflow coverage/i);
  assert.match(reply, /next concrete step/i);
  assert.doesNotMatch(reply, /walkthrough she requested/i);
  assertNoInternalLeak(reply);
});

test('walkthrough state gets a walkthrough-specific next step', () => {
  const reply = formatAccountBriefing(leadFixture({
    status: 'walkthrough_requested',
    contact_name: 'Sarah Chen',
    last_interaction_summary: 'Sarah requested a walkthrough of the common areas.',
    original_visit_note: 'Sarah requested a walkthrough of the common areas.',
    open_next_action: 'Book walkthrough',
    interest_level: 'high',
  }), { today: TODAY });

  assert.match(reply, /Confirm walkthrough date\/time/i);
  assert.match(reply, /access\/scope details/i);
  assertNoInternalLeak(reply);
});

test('proposal state gets a proposal-specific next step', () => {
  const reply = formatAccountBriefing(leadFixture({
    status: 'proposal_needed',
    contact_name: 'Dana Ruiz',
    last_interaction_summary: 'Sent a scope outline after the site visit.',
    original_visit_note: 'Walked the building with Dana.',
    waiting_on_jake: false,
    open_next_action: 'proposal_follow_up',
  }), { today: TODAY });

  assert.match(reply, /Follow up on the proposal/i);
  assert.match(reply, /scope or timing questions/i);
  assert.doesNotMatch(reply, /No AO action required/i);
});

test('waiting-on-operator state asks for no AO action', () => {
  const reply = formatAccountBriefing(leadFixture({
    waiting_on_jake: true,
    open_escalation_reason: 'quote_request',
    open_escalation_summary: 'pricing the Elm Street walkthrough',
    open_escalation_id: 'esc-1',
    open_escalation_status: 'new',
    contact_name: 'Dana Ruiz',
    last_interaction_summary: 'Dana wants a quote after the walkthrough.',
    original_visit_note: 'Dana wants a quote after the walkthrough.',
  }), { today: TODAY });

  assert.match(reply, /Waiting on Jake for pricing the Elm Street walkthrough/i);
  assert.match(reply, /No AO action required until then/i);
});

test('property-management account with no contact uses property-management role guidance', () => {
  const reply = formatAccountContactsReply(leadFixture({
    business_name: 'Brady Sullivan Properties',
    business_type: 'property_management',
    contact_name: null,
  }), { today: TODAY });
  assert.match(reply, /property manager/i);
  assert.match(reply, /facilities manager/i);
  assert.doesNotMatch(reply, /ask for the office manager or owner who handles cleaning decisions/i);
});

test('commercial-office account uses office/facilities role guidance', () => {
  const reply = formatAccountBriefing(leadFixture({
    business_name: 'Backus Meyer & Branch LLP',
    business_type: 'law_firm',
    last_interaction_summary: null,
    original_visit_note: null,
    contact_name: null,
    open_next_action: 'research',
    crm_prospect_id: null,
  }), { today: TODAY });

  assert.match(reply, /office manager/i);
  assert.match(reply, /practice manager/i);
  assert.match(reply, /Identify the office or facilities manager/i);
});

test('due dates format as overdue, today, and future month-day', () => {
  assert.equal(formatFollowUpTiming('2026-09-14', { today: TODAY }).label, 'Overdue since Sep 14');
  assert.equal(formatFollowUpTiming('2026-09-16', { today: TODAY }).label, 'Due today');
  assert.equal(formatFollowUpTiming('2026-09-18', { today: TODAY }).label, 'Due Sep 18');
  assert.doesNotMatch(formatFollowUpTiming(new Date('2026-09-14T00:00:00.000Z'), { today: TODAY }).label, /GMT/);
});

test('missing data does not fabricate contacts, phones, or buying signals', () => {
  const reply = formatAccountBriefing({
    business_name: 'Unknown Storefront',
    status: 'needs_follow_up',
    interest_level: null,
    contact_name: null,
    contact_phone: null,
    last_interaction_summary: null,
    original_visit_note: null,
    probe_answers: null,
    open_next_action: null,
    open_task_due: null,
  }, { today: TODAY });

  assert.doesNotMatch(reply, /603-|555-/);
  assert.doesNotMatch(reply, /walkthrough she requested/i);
  assert.doesNotMatch(reply, /high-interest conversation/i);
  assert.doesNotMatch(reply, /Sarah/);
  assert.match(reply, /No contact identified yet/i);
  assert.match(reply, /No recent conversation logged/i);
});

test('prioritization list uses improved actionable next steps', () => {
  const accounts = [
    mapAccountRow(taskRow(), TODAY),
    mapAccountRow(taskRow({
      business_name: 'Brady Sullivan Properties',
      lead_id: 'lead-2',
      last_interaction_summary: assignmentNote({ company: 'Brady Sullivan Properties' }),
      due_date: '2026-09-14',
    }), TODAY),
  ];
  const reply = formatPrioritizationResponse(accounts, { hasOverdue: true });
  assert.match(reply, /Anagnost Companies/);
  assert.match(reply, /High-priority development account/i);
  assert.match(reply, /Research stage/i);
  assert.match(reply, /Identify the facilities\/property-management decision-maker/i);
  assert.doesNotMatch(reply, /\bNext: Research\b/);
  assertNoInternalLeak(reply);
});

test('tied accounts do not receive fabricated differentiation', () => {
  const reply = formatPrioritizationResponse([
    {
      business_name: 'Anagnost Companies',
      why_now: 'Follow-up is overdue.',
      status_label: 'Research stage · follow-up overdue',
      next_step: 'Identify the facilities/property-management decision-maker and confirm the best contact route.',
      rank_score: 10820,
    },
    {
      business_name: 'Brady Sullivan Properties',
      why_now: 'Follow-up is overdue.',
      status_label: 'Research stage · follow-up overdue',
      next_step: 'Identify the facilities/property-management decision-maker and confirm the best contact route.',
      rank_score: 10820,
    },
  ], { hasOverdue: true });

  assert.match(reply, /tied on current operational priority/i);
  assert.doesNotMatch(reply, /stronger opportunity/i);
  assert.doesNotMatch(reply, /Start with Anagnost Companies because/i);
});

test('persistent conversation and typo resolution still work', () => {
  const context = {
    prioritized_accounts: [
      { business_name: 'Anagnost Companies', lead_id: 'lead-1', why_now: 'Follow-up is overdue.' },
      { business_name: 'Brady Sullivan Properties', lead_id: 'lead-2', why_now: 'Follow-up is overdue.' },
    ],
    selected_account: { business_name: 'Anagnost Companies', lead_id: 'lead-1' },
  };
  const typo = resolveConversationIntent('brief me on aganost', context);
  assert.equal(typo.intent, 'account_briefing');
  assert.equal(typo.briefingTarget, 'Anagnost Companies');

  const followUp = resolveConversationIntent('Who should I ask for?', context);
  assert.equal(followUp.intent, 'account_contacts');
  assert.equal(followUp.briefingTarget, 'Anagnost Companies');
  assert.equal(extractBriefingTarget('brief me on aganost'), 'aganost');
});

test('briefing helpers have no outbound send side effects', () => {
  const files = [
    path.join(__dirname, '..', 'utils', 'aoAccountBriefing.js'),
    path.join(__dirname, '..', 'utils', 'aoAccountPrioritization.js'),
  ];
  for (const file of files) {
    const src = fs.readFileSync(file, 'utf8');
    assert.doesNotMatch(src, /brevo|twilio|sendEmail|sendSms|autosend|nodemailer/i);
  }
});

test('sanitizeAoFacingText strips assignment JSON while keeping persistence callers unchanged', () => {
  const raw = assignmentNote({ company: 'Brady Sullivan Properties' });
  const cleaned = sanitizeAoFacingText(raw);
  assert.doesNotMatch(cleaned, /batch_id/);
  assert.doesNotMatch(cleaned, /crm_company_id/);
  assert.match(raw, /crm_prospect_id/);
});
