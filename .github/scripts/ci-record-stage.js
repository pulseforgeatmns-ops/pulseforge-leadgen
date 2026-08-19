'use strict';

/**
 * SPEC-120 — append a stage timing record for CI observability.
 *
 * Usage:
 *   node .github/scripts/ci-record-stage.js environment 45
 *   node .github/scripts/ci-record-stage.js build 82 '{"cacheHit":true}'
 */

const fs = require('fs');
const path = require('path');

const stageFile =
  process.env.CI_STAGE_FILE ||
  path.join(process.cwd(), 'ci-stage-timings.jsonl');

const stage = process.argv[2];
const durationSeconds = Number(process.argv[3]);
const extras = process.argv[4] ? JSON.parse(process.argv[4]) : {};

if (!stage || !Number.isFinite(durationSeconds)) {
  console.error(
    'Usage: node ci-record-stage.js <stage> <durationSeconds> [jsonExtras]'
  );
  process.exit(2);
}

const row = {
  stage,
  durationSeconds,
  timestamp: new Date().toISOString(),
  ...extras,
};

fs.appendFileSync(stageFile, `${JSON.stringify(row)}\n`);
console.log(`[ci-timing] ${stage}: ${durationSeconds}s`);
