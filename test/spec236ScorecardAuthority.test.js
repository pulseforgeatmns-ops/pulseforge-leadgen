'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  generateDraftScorecard,
  buildBriefScorecardSections,
  getRuntimeScorecard,
} = require('../packages/operator-scorecard');

const BUSINESS_GOAL =
  'Prove Babrun can reliably acquire the right founder customers for the 12-week program, beginning with enrolling the first qualified founder and learning which segments and problems drive action.';

function babrunInput() {
  return {
    tenantId: 'babrun-spec-236',
    businessName: 'Babrun',
    businessGoal: BUSINESS_GOAL,
    objectives: [BUSINESS_GOAL],
    operatorMetrics: [
      'qualified founder conversations',
      'ICP-qualified conversations',
      'serious program conversations',
      'paid enrollments',
      'discovery → enrollment conversion',
    ],
  };
}

describe('SPEC-236 scorecard authority separation', () => {
  it('preserves exact operator metrics and Max recommendations separately', () => {
    const scorecard = generateDraftScorecard(babrunInput());
    const operatorNames = [
      'Qualified Founder Conversations',
      'ICP-Qualified Conversations',
      'Serious Program Conversations',
      'Paid Enrollments',
      'Discovery → Enrollment Conversion',
    ];
    const operatorMetrics = scorecard.metrics.filter((metric) => metric.source === 'operator');

    assert.deepEqual(
      operatorMetrics.map((metric) => metric.name).sort(),
      operatorNames.sort()
    );
    assert.ok(scorecard.metrics.some((metric) => metric.source === 'max'));
    assert.equal(scorecard.metrics.some((metric) => metric.source === 'max' && operatorNames.includes(metric.name)), false);
  });

  it('does not render the full objective as Supports and preserves concise mappings', () => {
    const scorecard = generateDraftScorecard(babrunInput());
    const sections = buildBriefScorecardSections(scorecard);
    const rendered = JSON.stringify(sections);
    const recommended = sections.find((section) => section.id === 'recommendedScorecard');
    const underReview = sections.find((section) => section.id === 'metricsUnderReview');

    assert.doesNotMatch(rendered, new RegExp(`Supports ${BUSINESS_GOAL.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}`));
    assert.match(JSON.stringify(recommended), /Supports Operator-defined success/);
    assert.match(JSON.stringify(recommended), /Supports Repeatable Acquisition/);

    const recommendedKeys = new Set(recommended.items.map((metric) => metric.key));
    assert.equal(underReview.items.some((metric) => recommendedKeys.has(metric.key)), false);
  });

  it('keeps draft recommendations advisory until operator approval', () => {
    const scorecard = generateDraftScorecard(babrunInput());
    const runtime = getRuntimeScorecard(scorecard);

    assert.equal(scorecard.status, 'draft');
    assert.equal(runtime.status, 'absent');
    assert.equal(runtime.definitionOfSuccess, null);
  });
});