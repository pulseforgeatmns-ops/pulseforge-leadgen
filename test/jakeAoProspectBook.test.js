'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const {
  buildAssignmentNote,
  distributeDueDates,
  isJakeAssignmentBatchNote,
  laneInitialNextAction,
  normalizeAoBusinessKey,
} = require('../utils/aoAssignment');
const { PROSPECTS, BATCH_SLUG } = require('../scripts/data/jakeAoProspectBook');

test('laneInitialNextAction returns lane-specific guidance', () => {
  assert.match(
    laneInitialNextAction('property_management'),
    /property\/facilities decision-maker/i,
  );
  assert.match(
    laneInitialNextAction('commercial_office'),
    /office\/facilities manager/i,
  );
  assert.match(
    laneInitialNextAction('medical_dental'),
    /practice\/office manager/i,
  );
  assert.match(
    laneInitialNextAction('development'),
    /facilities\/property operations contact/i,
  );
});

test('buildAssignmentNote matches canonical AO assignment format', () => {
  const note = buildAssignmentNote({
    batchSlug: BATCH_SLUG,
    company: 'LNH Property Management',
    ownerName: 'Jake',
    lane: 'property_management',
    dueDate: '2026-09-16',
    aoOwnerId: 7,
  });

  assert.match(note, /\[AO Assignment \| ao-assignment-2026-09-16-jake-dogfood\]/);
  assert.match(note, /Metadata:/);
  assert.match(note, /"source": "AO Assignment"/);
  assert.match(note, /property\/facilities decision-maker/i);
  assert.equal(isJakeAssignmentBatchNote(note, BATCH_SLUG), true);
});

test('distributeDueDates staggers first 5 today, next 5 tomorrow, rest later', () => {
  const dates = distributeDueDates(18, { today: '2026-09-16' });
  assert.equal(dates.length, 18);
  assert.equal(dates.filter(d => d === '2026-09-16').length, 5);
  assert.equal(dates[5], '2026-09-17');
  assert.ok(dates[10] >= '2026-09-18');
});

test('jake prospect book has 18 accounts with required variety', () => {
  assert.equal(PROSPECTS.length, 18);
  const segments = new Set(PROSPECTS.map(p => p.segment));
  assert.ok(segments.has('property_management'));
  assert.ok(segments.has('professional_office'));
  assert.ok(segments.has('medical_dental'));
  assert.ok(segments.has('stretch'));
});

test('jake prospect book avoids Mike direct-mail target names', () => {
  const mikeTargets = require('../scripts/data/anchorDirectMailTargets').DIRECT_MAIL_TARGETS
    .map(t => normalizeAoBusinessKey(t.business_name));
  for (const prospect of PROSPECTS) {
    assert.equal(
      mikeTargets.includes(normalizeAoBusinessKey(prospect.business_name)),
      false,
      prospect.business_name,
    );
  }
});
