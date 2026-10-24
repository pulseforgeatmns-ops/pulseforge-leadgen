'use strict';

const fs = require('fs');
const path = require('path');

const { STAGES } = require('./CompetencyLifecycle');
const { listCompetencies } = require('./CompetencyRegistry');

const REPO_ROOT = path.resolve(__dirname, '../../..');

function resolveTestPath(relativePath) {
  return path.join(REPO_ROOT, relativePath);
}

function testPathExists(relativePath) {
  return fs.existsSync(resolveTestPath(relativePath));
}

function buildRegressionSuite() {
  const competencies = listCompetencies();
  const graduated = competencies.filter(c => c.stage === STAGES.GRADUATED);
  const entries = [];
  const missing = [];

  for (const competency of graduated) {
    if (!competency.regressionTests?.length) {
      missing.push({
        competencyId: competency.id,
        label: competency.label,
        reason: 'graduated competency has no regressionTests mapping',
      });
      continue;
    }
    for (const testPath of competency.regressionTests) {
      const exists = testPathExists(testPath);
      entries.push({
        competencyId: competency.id,
        label: competency.label,
        testPath,
        exists,
      });
      if (!exists) {
        missing.push({
          competencyId: competency.id,
          label: competency.label,
          testPath,
          reason: 'regression test file not found',
        });
      }
    }
  }

  return {
    spec: 'SPEC-102F',
    graduatedCount: graduated.length,
    mappedTests: entries.length,
    entries,
    missing,
    ok: missing.length === 0,
  };
}

function assertRegressionSuite() {
  const suite = buildRegressionSuite();
  if (suite.ok) return suite;
  const detail = suite.missing
    .map(m => `${m.competencyId}: ${m.reason}${m.testPath ? ` (${m.testPath})` : ''}`)
    .join('\n');
  throw new Error(`Max competency regression suite failed:\n${detail}`);
}

module.exports = {
  REPO_ROOT,
  resolveTestPath,
  testPathExists,
  buildRegressionSuite,
  assertRegressionSuite,
};
