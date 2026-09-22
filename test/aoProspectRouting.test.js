'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const {
  routeProspect,
  classifyMotion,
  computeAoFitScore,
  buildWeeklyListMix,
} = require('../services/aoProspectRoutingService');
const { formatAoTask } = require('../utils/aoProspectTaskFormat');

function prospect(overrides = {}) {
  return {
    id: '00000000-0000-0000-0000-000000000001',
    client_id: 10,
    first_name: 'Pat',
    last_name: 'Manager',
    email: 'pat@example.com',
    phone: '6035550100',
    vertical: 'commercial_office',
    icp_score: 72,
    status: 'cold',
    service_area_match: 'Manchester, NH',
    do_not_contact: false,
    ...overrides,
  };
}

function company(overrides = {}) {
  return {
    name: 'Example Office LLC',
    location: 'Manchester, NH',
    ...overrides,
  };
}

test('strategic cold account routes to AO_LED', () => {
  const routing = routeProspect({
    prospect: prospect({
      vertical: 'cleaning_company_overflow',
      email: 'info@overflowclean.example',
      phone: '6035550101',
      icp_score: 78,
    }),
    company: company({ name: 'Manchester Overflow Cleaning Co' }),
    availableAos: [{ id: 7, name: 'Rory', active: true, open_task_count: 2 }],
  });
  assert.equal(routing.recommended_motion, 'AO_LED');
  assert.ok(routing.ao_fit_score >= 60);
  assert.match(routing.ao_fit_reason, /Anchor/i);
});

test('property manager with generic email routes to HYBRID or AO_LED', () => {
  const routing = routeProspect({
    prospect: prospect({
      vertical: 'property_manager',
      email: 'info@granitepm.example',
      phone: '6035550101',
      icp_score: 75,
    }),
    company: company({ name: 'Granite State Property Management' }),
    availableAos: [{ id: 7, name: 'Rory', active: true }],
  });
  assert.ok(['HYBRID', 'AO_LED'].includes(routing.recommended_motion));
  assert.match(routing.recommended_angle, /backup|overflow|turnover/i);
});

test('ordinary low-priority account with usable email routes to EMAIL_LED', () => {
  const routing = routeProspect({
    prospect: prospect({
      vertical: 'commercial_office',
      email: 'sarah.jones@smalloffice.example',
      phone: null,
      icp_score: 55,
      status: 'cold',
    }),
    company: company({ name: 'Small Office LLC' }),
    availableAos: [{ id: 7, name: 'Rory', active: true }],
  });
  assert.equal(routing.recommended_motion, 'EMAIL_LED');
});

test('warm reply routes to AO assignment category WARM_SIGNAL', () => {
  const routing = routeProspect({
    prospect: prospect({
      status: 'warm',
      vertical: 'commercial_office',
      email: 'frontdesk@office.example',
    }),
    company: company(),
    touchpoints: [{ action_type: 'reply', channel: 'email' }],
    availableAos: [{ id: 8, name: 'Zack', active: true }],
  });
  assert.ok(['AO_LED', 'HYBRID'].includes(routing.recommended_motion));
  assert.equal(routing.assignment_category, 'WARM_SIGNAL');
  assert.equal(routing.recommended_ao_id, 8);
});

test('bad fit account routes to SUPPRESS', () => {
  const routing = routeProspect({
    prospect: prospect({
      icp_score: 20,
      service_area_match: 'Boston, MA',
    }),
    company: company({ location: 'Boston, MA' }),
    availableAos: [{ id: 7, name: 'Rory', active: true }],
  });
  assert.equal(routing.recommended_motion, 'SUPPRESS');
});

test('do_not_contact forces SUPPRESS regardless of score', () => {
  const routing = routeProspect({
    prospect: prospect({ do_not_contact: true, icp_score: 90 }),
    company: company(),
    availableAos: [{ id: 7, name: 'Rory', active: true }],
  });
  assert.equal(routing.recommended_motion, 'SUPPRESS');
});

test('AO task format includes required advisory fields', () => {
  const routing = routeProspect({
    prospect: prospect({ vertical: 'property_manager', email: 'info@granitepm.example' }),
    company: company({ name: 'Granite State Property Management' }),
    availableAos: [{ id: 7, name: 'Rory', active: true }],
  });
  const task = formatAoTask(routing, { assignedAoName: 'Rory' });
  assert.equal(task.account, 'Granite State Property Management');
  assert.match(task.segment, /Property management/i);
  assert.equal(task.assigned_ao, 'Rory');
  assert.ok(task.why_this_account_matters);
  assert.ok(task.suggested_opener);
  assert.deepEqual(task.what_to_log, [
    'decision_maker',
    'current_vendor_status',
    'pain_or_timing',
    'next_step',
    'follow_up_date',
  ]);
});

test('weekly list mix respects category buckets', () => {
  const items = [];
  for (let i = 0; i < 6; i += 1) {
    items.push({
      prospectId: `id-${i}`,
      routing: routeProspect({
        prospect: prospect({ vertical: 'property_manager', icp_score: 80 + i }),
        company: company({ name: `PM ${i}` }),
        availableAos: [{ id: 7, name: 'Rory', active: true }],
      }),
    });
  }
  const weekly = buildWeeklyListMix(items, { today: '2026-09-22' });
  assert.ok(weekly.length >= 5);
  assert.equal(weekly.filter(entry => entry.assignment_category === 'UNFAIR_ADVANTAGE').length, 5);
});

test('classifyMotion returns NURTURE for real but inactive account', () => {
  const { recommended_motion } = classifyMotion({
    prospect: prospect({ icp_score: 45, status: 'cold', email: null, phone: null }),
    company: company(),
    aoFitScore: 42,
    touchpoints: [],
  });
  assert.equal(recommended_motion, 'NURTURE');
});

test('computeAoFitScore penalizes outside service area', () => {
  const { ao_fit_score, score_breakdown } = computeAoFitScore({
    prospect: prospect({ service_area_match: 'Boston, MA', icp_score: 80 }),
    company: company({ location: 'Boston, MA' }),
  });
  assert.ok(score_breakdown.some(item => item.factor === 'Local accessibility' && item.points < 0));
  assert.ok(ao_fit_score < 80);
});
