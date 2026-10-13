'use strict';

// Keep noisy fixture output out of the job log without dropping any tests or
// changing their exit status. The complete test output is retained separately.
const MAX_FAILURES = 60;
const MAX_DETAILS = 8;
const MAX_LINE = 240;

function summarizeNodeTestLog(input) {
  const lines = String(input).replace(/\u001b\[[0-9;]*m/g, '').split(/\r?\n/);
  const clip = (line) => {
    const clean = line.replace(/[\x00-\x08\x0b-\x1f\x7f]/g, '');
    return clean.length > MAX_LINE ? `${clean.slice(0, MAX_LINE)}…` : clean;
  };
  const totals = new Map();
  const failures = [];
  let count = 0;
  let current = null;
  let specFailures = false;
  let stack = false;
  const begin = (title) => {
    count += 1;
    current = count <= MAX_FAILURES ? { title: clip(title.trim()), details: [] } : null;
    if (current) failures.push(current);
    stack = false;
  };
  for (const line of lines) {
    const total = line.match(/^(?:#|ℹ) (tests|suites|pass|fail|cancelled|skipped|todo)\s+(\d+)/);
    if (total) totals.set(total[1], total[2]);
    if (/^\s*not ok \d+\s*-/.test(line)) { begin(line); continue; }
    if (/^\s*✖ failing tests:/.test(line)) { specFailures = true; current = null; continue; }
    if (specFailures && /^test at /.test(line)) { begin(line); continue; }
    if (/^\s*(?:ok \d+\s*-|# Subtest:)/.test(line)) { current = null; continue; }
    if (!current || current.details.length >= MAX_DETAILS) continue;
    if (/^\s*stack:\s*\|/.test(line)) { stack = true; continue; }
    if (/^\s*(?:location|failureType|error|code|name|expected|actual|operator):/.test(line)
      || (stack && /^\s+\S.*\([^)]*:\d+(?::\d+)?\)/.test(line))
      || (specFailures && /^\s*(?:✖ |(?:AssertionError|TypeError|Error)\b|at |[+-] )/.test(line))) {
      current.details.push(clip(line.trim()));
    }
  }
  const output = ['Node test summary', ...[...totals].map(([key, value]) => `  ${key}: ${value}`)];
  if (count) {
    output.push(`Failed entries: ${count} (TAP suite failures may also be listed)`);
    for (const failure of failures) {
      output.push(`  ${failure.title}`, ...failure.details.map((line) => `    ${line}`));
    }
    if (count > failures.length) output.push(`  ${count - failures.length} more entries in the full test-output artifact.`);
  } else if (!totals.size) {
    output.push('No test totals found. Bounded output tail:');
    output.push(...lines.filter((line) => line.trim()).slice(-30).map((line) => `  ${clip(line)}`));
  }
  return `${output.join('\n')}\n`;
}

if (require.main === module) {
  const fs = require('node:fs');
  process.stdout.write(summarizeNodeTestLog(fs.readFileSync(process.argv[2], 'utf8')));
}

module.exports = { summarizeNodeTestLog, MAX_FAILURES, MAX_DETAILS, MAX_LINE };
