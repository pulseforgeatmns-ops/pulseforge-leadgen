'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const { buildDayZeroOperatorBrief } = require('../utils/aoCommandDeckBrief');

test('buildDayZeroOperatorBrief provides Campaign 001 day-zero narrative', () => {
  const brief = buildDayZeroOperatorBrief();
  assert.match(brief.narrative, /Campaign 001 targets queued/);
  assert.match(brief.narrative, /Manchester direct-mail route/);
  assert.equal(brief.jakeActions.length, 1);
  assert.equal(brief.mikeActions.length, 1);
  assert.equal(brief.actionCards.length, 5);
  assert.equal(brief.actionCards[0].label, "Open Mike's Route");
  assert.match(brief.actionCards[1].href, /escalations/);
});

test('buildDayZeroOperatorBrief action cards exclude primary AO Briefing nav', () => {
  const brief = buildDayZeroOperatorBrief();
  const labels = brief.actionCards.map((c) => c.label);
  assert.ok(!labels.some((l) => /open ao briefing/i.test(l)));
  assert.ok(labels.some((l) => /Campaign 001/.test(l)));
  assert.ok(labels.some((l) => /Promote CRM/.test(l)));
});
