'use strict';

/**
 * SPEC-120 — classify CI outcomes and emit deployment gate summary.
 *
 * Reads stage timings and upstream job results. Writes GitHub step summary
 * and exits non-zero when merge should remain blocked.
 */

const fs = require('fs');
const path = require('path');

const OUTCOMES = Object.freeze({
  SUCCESS: 'SUCCESS',
  APPLICATION_FAILURE: 'APPLICATION_FAILURE',
  TEST_FAILURE: 'TEST_FAILURE',
  INFRASTRUCTURE_FAILURE: 'INFRASTRUCTURE_FAILURE',
  DEPENDENCY_FAILURE: 'DEPENDENCY_FAILURE',
  RUNNER_FAILURE: 'RUNNER_FAILURE',
});

const stageFile =
  process.env.CI_STAGE_FILE ||
  path.join(process.cwd(), 'ci-stage-timings.jsonl');

function readTimings() {
  if (!fs.existsSync(stageFile)) return [];
  return fs
    .readFileSync(stageFile, 'utf8')
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      try {
        return JSON.parse(line);
      } catch {
        return null;
      }
    })
    .filter(Boolean);
}

function sumDuration(timings, stage) {
  return timings
    .filter((row) => row.stage === stage)
    .reduce((total, row) => total + (row.durationSeconds || 0), 0);
}

function classify(outcome) {
  const {
    workflowConclusion = '',
    cancelled = false,
    failedStage = '',
    failedStep = '',
    environmentHealthy = true,
    testFailed = false,
    buildFailed = false,
  } = outcome;

  if (workflowConclusion === 'success') return OUTCOMES.SUCCESS;

  if (cancelled) {
    if (
      failedStage === 'environment' ||
      /Install PostgreSQL|apt-get|cache/i.test(failedStep)
    ) {
      return OUTCOMES.INFRASTRUCTURE_FAILURE;
    }
    if (/npm ci|install dependencies/i.test(failedStep)) {
      return OUTCOMES.DEPENDENCY_FAILURE;
    }
    return OUTCOMES.RUNNER_FAILURE;
  }

  if (!environmentHealthy) return OUTCOMES.INFRASTRUCTURE_FAILURE;
  if (buildFailed || /npm ci|install dependencies/i.test(failedStep)) {
    return OUTCOMES.DEPENDENCY_FAILURE;
  }
  if (testFailed || failedStage === 'test') return OUTCOMES.TEST_FAILURE;
  if (failedStage === 'build') return OUTCOMES.DEPENDENCY_FAILURE;
  if (failedStage === 'environment') return OUTCOMES.INFRASTRUCTURE_FAILURE;

  return OUTCOMES.APPLICATION_FAILURE;
}

function actionRequired(outcome) {
  switch (outcome) {
    case OUTCOMES.INFRASTRUCTURE_FAILURE:
      return 'Retry the workflow or inspect PostgreSQL environment setup. This is not an application regression.';
    case OUTCOMES.DEPENDENCY_FAILURE:
      return 'Inspect npm lockfile/install logs. Dependency installation failed before tests ran.';
    case OUTCOMES.RUNNER_FAILURE:
      return 'GitHub runner cancelled or timed out. Retry before treating this as a product failure.';
    case OUTCOMES.TEST_FAILURE:
      return 'Inspect failing application/integration tests. Merge remains blocked until tests pass.';
    case OUTCOMES.APPLICATION_FAILURE:
      return 'Inspect application logs for the failing stage.';
    default:
      return 'All required stages passed.';
  }
}

function main() {
  const timings = readTimings();
  const environmentDuration = sumDuration(timings, 'environment');
  const buildDuration = sumDuration(timings, 'build');
  const testDuration = sumDuration(timings, 'test');
  const gateDuration = sumDuration(timings, 'deployment-gate');
  const totalDuration =
    environmentDuration + buildDuration + testDuration + gateDuration;

  const envRow = timings.find((row) => row.stage === 'environment') || {};
  const outcome = classify({
    workflowConclusion: process.env.CI_WORKFLOW_CONCLUSION || '',
    cancelled: process.env.CI_CANCELLED === 'true',
    failedStage: process.env.CI_FAILED_STAGE || '',
    failedStep: process.env.CI_FAILED_STEP || '',
    environmentHealthy: process.env.CI_ENVIRONMENT_HEALTHY !== 'false',
    testFailed: process.env.CI_TEST_FAILED === 'true',
    buildFailed: process.env.CI_BUILD_FAILED === 'true',
  });

  const summary = [
    '## SPEC-120 Deployment Gate',
    '',
    `**Outcome:** \`${outcome}\``,
    '',
    '### Stage Durations',
    '',
    '| Stage | Seconds |',
    '| --- | ---: |',
    `| Environment | ${environmentDuration} |`,
    `| Build | ${buildDuration} |`,
    `| Test | ${testDuration} |`,
    `| Deployment Gate | ${gateDuration} |`,
    `| **Total** | **${totalDuration}** |`,
    '',
    '### Infrastructure Health',
    '',
    `- Cache hit: ${envRow.cacheHit === true ? 'yes' : envRow.cacheHit === false ? 'no' : 'unknown'}`,
    `- PostgreSQL source: ${envRow.postgresSource || 'unknown'}`,
    `- Runner image: ${envRow.runnerImage || process.env.ImageOS || 'unknown'}`,
    `- Environment healthy: ${process.env.CI_ENVIRONMENT_HEALTHY !== 'false' ? 'yes' : 'no'}`,
    '',
    '### Operator Action',
    '',
    actionRequired(outcome),
    '',
  ].join('\n');

  const summaryPath = process.env.GITHUB_STEP_SUMMARY;
  if (summaryPath) fs.appendFileSync(summaryPath, `${summary}\n`);

  console.log(summary);
  console.log(`CI_CLASSIFICATION=${outcome}`);

  if (outcome !== OUTCOMES.SUCCESS) {
    process.exit(1);
  }
}

main();
