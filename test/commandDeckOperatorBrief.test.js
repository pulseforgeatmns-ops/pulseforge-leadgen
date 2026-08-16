'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const { buildDayZeroOperatorBrief } = require('../utils/aoCommandDeckBrief');

test('buildDayZeroOperatorBrief provides Campaign 001 day-zero narrative', () => {
  const brief = buildDayZeroOperatorBrief();
  assert.match(brief.narrative, /Campaign 001 targets queued/);
  assert.match(brief.narrative, /Manchester direct-mail route/);
  assert.ok(brief.commandRail);
  assert.equal(brief.commandRail.needsJake.length, 0);
  assert.ok(brief.commandRail.mikeAo);
  assert.equal(brief.commandRail.campaign001.total, 20);
});

test('buildDayZeroOperatorBrief command rail has quick actions', () => {
  const brief = buildDayZeroOperatorBrief();
  const labels = brief.commandRail.quickActions.map((c) => c.label);
  assert.ok(labels.some((l) => /Copy Mike Instructions/.test(l)));
  assert.ok(labels.some((l) => /View Field Visits/.test(l)));
});
