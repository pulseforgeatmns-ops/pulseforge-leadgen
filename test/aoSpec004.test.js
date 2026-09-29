'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const { formatProspectBrief } = require('../utils/aoProspectBrief');
const {
  AO_ROUTING_ISSUE_TYPES,
  isValidRoutingIssueType,
  isActiveConversationStatus,
} = require('../utils/aoRoutingIssueTypes');
const { parseArgs: parseRoutingArgs } = require('../scripts/reviewAoRoutingIssues');
const { parseArgs: parseConversationArgs } = require('../scripts/reviewAoConversations');

const richProspect = {
  prospect: {
    name: 'Jane Doe',
    job_title: 'Office Manager',
    email: 'jane@beaconlaw.com',
    phone: '603-555-0100',
    vertical: 'law_firm',
    status: 'cold',
    prospect_motion: 'AO_LED',
    ao_fit_score: 82,
    ao_fit_reason: 'Strong Anchor fit — single-tenant law office in Manchester pilot.',
    ao_assignment_category: 'ROUTE_CLUSTER',
    recommended_angle: 'Position Anchor as a reliable backup when turnover hits.',
    recommended_first_action: 'In-person follow-up on direct mail piece.',
    advisory_stage: 'routed',
    last_debrief_status: null,
    next_action: 'AO_FOLLOW_UP',
    next_action_owner: 'ao',
    next_action_due_at: '2026-09-25T00:00:00.000Z',
  },
  company: {
    name: 'Beacon Law',
    industry: 'law firm',
    location: 'Manchester NH',
    website: 'https://beaconlaw.example',
  },
  touchpoints: [
    {
      created_at: '2026-09-20T12:00:00.000Z',
      channel: 'email',
      action_type: 'send',
      outcome: 'delivered',
    },
  ],
  task: {
    segment: 'commercial-office',
    why_account_matters: 'Due today on Campaign 001.',
    recommended_angle: 'Backup cleaner angle',
    first_action: 'Stop by after direct mail.',
    suggested_opener: 'We sent info about commercial cleaning…',
    desired_next_outcome: 'Book walkthrough',
    assignment_category: 'WARM_SIGNAL',
    deadline: '2026-09-24',
  },
};

test('formatProspectBrief includes AO routing fields', () => {
  const brief = formatProspectBrief(richProspect);
  assert.match(brief, /Prospect Brief/);
  assert.match(brief, /Beacon Law/);
  assert.match(brief, /backup cleaner angle|reliable backup/i);
  assert.match(brief, /In-person follow-up|AO_FOLLOW_UP/);
  assert.match(brief, /ROUTE_CLUSTER|WARM_SIGNAL/);
  assert.match(brief, /Jane Doe/);
  assert.match(brief, /2026-09-20/);
});

test('formatProspectBrief returns minimal brief for sparse prospect', () => {
  const brief = formatProspectBrief({
    prospect: { name: 'Sparse Co' },
    company: { name: 'Sparse Co' },
    touchpoints: [],
  });
  assert.match(brief, /don't have enough data/i);
  assert.match(brief, /Known:/);
  assert.match(brief, /Unknown:/);
  assert.match(brief, /Recommended next research step:/);
});

test('routing issue type enum matches spec', () => {
  assert.deepEqual(AO_ROUTING_ISSUE_TYPES, [
    'wrong_route',
    'wrong_prospect',
    'wrong_mission',
    'lost_context',
    'should_have_opened_brief',
    'should_have_opened_conversation',
    'treated_as_done_incorrectly',
    'other',
  ]);
  assert.equal(isValidRoutingIssueType('wrong_route'), true);
  assert.equal(isValidRoutingIssueType('invalid'), false);
});

test('active conversation statuses include reopened', () => {
  assert.equal(isActiveConversationStatus('active'), true);
  assert.equal(isActiveConversationStatus('reopened'), true);
  assert.equal(isActiveConversationStatus('done'), false);
});

test('ao routes expose SPEC-AO-004 endpoints', () => {
  const src = fs.readFileSync(path.join(__dirname, '..', 'routes', 'ao.js'), 'utf8');
  assert.match(src, /\/api\/max\/prospect-brief/);
  assert.match(src, /\/api\/max\/flag-routing/);
  assert.match(src, /\/api\/max\/conversations\/active/);
  assert.match(src, /\/api\/max\/conversations\/:id\/reopen/);
  assert.match(src, /\/api\/max\/conversations\/:id\/done/);
});

test('ao dashboard exposes Brief, routing flag, history, and reopen UI', () => {
  const src = fs.readFileSync(path.join(__dirname, '..', 'public', 'ao-dashboard.html'), 'utf8');
  assert.match(src, /data-brief-lead/);
  assert.match(src, /Flag routing issue/);
  assert.match(src, /panel-history/);
  assert.match(src, /\/api\/max\/prospect-brief/);
  assert.match(src, /\/api\/max\/flag-routing/);
  assert.match(src, /\/api\/max\/conversations/);
  assert.match(src, /hydrateActiveConversation/);
  assert.match(src, /reopenConversation/);
});

test('aoMaxConversation exports prospect brief and recovery helpers', () => {
  const src = fs.readFileSync(path.join(__dirname, '..', 'services', 'aoMaxConversation.js'), 'utf8');
  assert.match(src, /handleProspectBriefAction/);
  assert.match(src, /reopenConversation/);
  assert.match(src, /markConversationDone/);
  assert.match(src, /listConversations/);
  assert.match(src, /getActiveOrRestorableConversation/);
});

test('review scripts parse tenant and limit flags', () => {
  assert.deepEqual(parseRoutingArgs(['--tenant', '10', '--limit', '25']), {
    tenantId: '10',
    limit: 25,
    json: false,
  });
  assert.deepEqual(parseConversationArgs(['--tenant', '10', '--status', 'done', '--limit', '50']), {
    tenantId: '10',
    status: 'done',
    limit: 50,
    json: false,
  });
});

test('schema bootstrap includes ao_routing_issue_flags and conversation status', () => {
  const src = fs.readFileSync(path.join(__dirname, '..', 'utils', 'aoFieldSchema.js'), 'utf8');
  assert.match(src, /ao_routing_issue_flags/);
  assert.match(src, /status TEXT NOT NULL DEFAULT 'active'/);
});

test('prospect brief service logs AO_PROSPECT_BRIEF_REQUESTED audit event', () => {
  const src = fs.readFileSync(path.join(__dirname, '..', 'services', 'aoProspectBriefService.js'), 'utf8');
  assert.match(src, /AO_PROSPECT_BRIEF_REQUESTED/);
  assert.match(src, /action: 'prospect_brief'/);
});

test('reopen conversation logs AO_CONVERSATION_REOPENED audit event', () => {
  const src = fs.readFileSync(path.join(__dirname, '..', 'services', 'aoMaxConversation.js'), 'utf8');
  assert.match(src, /AO_CONVERSATION_REOPENED/);
  assert.match(src, /manual_reopen/);
});

test('routing issue flags attempt Jev decision_id linkage', () => {
  const src = fs.readFileSync(path.join(__dirname, '..', 'services', 'aoRoutingIssueFlags.js'), 'utf8');
  assert.match(src, /decision_shadow_events/);
  assert.match(src, /decision_id/);
});
