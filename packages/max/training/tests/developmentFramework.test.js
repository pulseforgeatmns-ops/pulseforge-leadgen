'use strict';

const { describe, test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');

const training = require('../index');

describe('SPEC-102F Max Development Framework', () => {
  test('registry validates all competencies and exercises', () => {
    for (const competency of training.listCompetencies()) {
      const result = training.validateCompetency(competency);
      assert.equal(result.valid, true, `${competency.id}: ${result.errors.join('; ')}`);
    }
  });

  test('training record includes graduated competencies from the spec example', () => {
    const record = training.buildTrainingRecord();
    assert.ok(record.summary.graduated >= 8);
    const ids = record.trainingRecord.map(c => c.id);
    assert.ok(ids.includes('retrieve_before_delegation'));
    assert.ok(ids.includes('specialist_trace_interrogation'));
    assert.ok(ids.includes('investigation_coverage_reasoning'));
    assert.ok(ids.includes('durable_business_understanding'));
  });

  test('formatTrainingRecordText matches operator-facing layout', () => {
    const record = training.buildTrainingRecord();
    const text = training.formatTrainingRecordText(record);
    assert.match(text, /Training Record/);
    assert.match(text, /Retrieve Before Delegation/);
    assert.match(text, /Graduated/);
    assert.match(text, /Multi-specialist Arbitration/);
    assert.match(text, /Training/);
    assert.match(text, /Economic Tradeoff Reasoning/);
    assert.match(text, /Not Started/);
  });

  test('performance review dimensions cover delegation through reflection', () => {
    const dims = training.listReviewDimensions().map(d => d.id);
    assert.deepEqual(dims, [
      'delegation',
      'retrieval',
      'judgment',
      'evidence',
      'communication',
      'uncertainty',
      'operator_trust',
      'reflection',
    ]);
  });

  test('real work priority lists Anchor Cleaning first', () => {
    const priority = training.listRealWorkPriority();
    assert.equal(priority[0].id, 'anchor_cleaning');
    assert.equal(priority[0].clientId, 10);
  });

  test('regression suite maps every graduated competency to existing tests', () => {
    const suite = training.assertRegressionSuite();
    assert.equal(suite.ok, true);
    assert.ok(suite.mappedTests >= 8);
    for (const entry of suite.entries) {
      assert.equal(entry.exists, true, `${entry.testPath} missing for ${entry.competencyId}`);
    }
  });

  test('claim_grounding is a graduated Pilot 0 competency', () => {
    const competency = training.getCompetency('claim_grounding');
    assert.equal(competency.stage, training.STAGES.GRADUATED);
    assert.ok(competency.specRefs.includes('SPEC-108'));
    assert.match(competency.exercises[0].generalLesson, /supported operating-state claims/i);
    assert.match(competency.exercises[0].transferTest, /has outreach begun/i);
  });

  test('retrieve_before_delegation exercise includes transfer test', () => {
    const competency = training.getCompetency('retrieve_before_delegation');
    const exercise = competency.exercises[0];
    assert.match(exercise.transferTest, /Kumho Tire/);
    assert.match(exercise.generalLesson, /delegate/i);
  });

  test('training record file is valid JSON', () => {
    const raw = fs.readFileSync(training.RECORD_PATH, 'utf8');
    const parsed = JSON.parse(raw);
    assert.equal(parsed.spec, 'SPEC-102F');
  });

  test('spec document exists', () => {
    const specPath = path.join(training.REPO_ROOT, 'docs/specs/SPEC-102F_Max_Development_Framework.md');
    assert.equal(fs.existsSync(specPath), true);
  });
});
