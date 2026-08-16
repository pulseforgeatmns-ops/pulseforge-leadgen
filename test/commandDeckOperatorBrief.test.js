'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const {
  buildDayZeroOperatorBrief,
  buildCommandRailQuickActions,
  buildTodayChanges,
} = require('../utils/aoCommandDeckBrief');

test('buildDayZeroOperatorBrief provides Campaign 001 day-zero narrative', () => {
  const brief = buildDayZeroOperatorBrief();
  assert.match(
    brief.narrative,
    /Mike has 20 Campaign 001 targets queued\. No visits logged today yet\. Highest-leverage action: have Mike start the Manchester direct-mail route and review after 3 stops\./
  );
  assert.ok(brief.commandRail);
  assert.equal(brief.commandRail.needsJake.length, 0);
  assert.ok(brief.commandRail.mikeAo);
  assert.equal(brief.commandRail.campaign001.total, 20);
  assert.ok(Array.isArray(brief.todayChanges));
  assert.ok(brief.jakeActions.length);
  assert.ok(brief.mikeActions.length);
});

test('buildDayZeroOperatorBrief command rail quick actions match operator spec', () => {
  const brief = buildDayZeroOperatorBrief();
  const labels = brief.commandRail.quickActions.map((c) => c.label);
  assert.ok(labels.some((l) => /Open Mike Route/.test(l)));
  assert.ok(labels.some((l) => /View Escalations/.test(l)));
  assert.ok(labels.some((l) => /View Field Visits/.test(l)));
  assert.ok(labels.some((l) => /View Campaign 001/.test(l)));
  assert.equal(labels.some((l) => /Promote CRM Candidates/.test(l)), false);
});

test('buildCommandRailQuickActions includes promo only when candidates exist', () => {
  const without = buildCommandRailQuickActions({ openEscalationCount: 1, promoCount: 0 });
  const withPromo = buildCommandRailQuickActions({ openEscalationCount: 1, promoCount: 2 });
  assert.equal(without.some((a) => a.id === 'promo'), false);
  assert.equal(withPromo.some((a) => a.id === 'promo'), true);
  assert.equal(withPromo.find((a) => a.id === 'escalations').href, '#cd-escalations');
});

test('buildTodayChanges prefers digest paragraphs when provided', () => {
  const lines = buildTodayChanges({
    paragraphs: ['Mike logged 2 visits today.', '1 escalation needs Jake.'],
  });
  assert.deepEqual(lines, ['Mike logged 2 visits today.', '1 escalation needs Jake.']);
});
