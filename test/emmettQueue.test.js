'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const forbiddenSymbols = [
  { name: 'async function getProspectsForEmail', pattern: /async\s+function\s+getProspectsForEmail\s*\(/ },
  { name: 'getSequenceForProspect', pattern: /getSequenceForProspect\s*\(/ },
  { name: 'advanceEmailSequenceState', pattern: /advanceEmailSequenceState\s*\(/ },
  { name: 'const SEQUENCES', pattern: /const\s+SEQUENCES\b/ },
  { name: 'await sendEmail(', pattern: /await\s+sendEmail\s*\(/ },
  { name: 'createEmailSendLog', pattern: /createEmailSendLog\b/ },
  { name: 'completeEmailSendLog', pattern: /completeEmailSendLog\b/ },
];

test('SPEC-189: Emmett infrastructure scheduler contains no acquisition machinery', () => {
  const scheduler = fs.readFileSync(path.join(__dirname, '../utils/emmettScheduler.js'), 'utf8');

  for (const { name, pattern } of forbiddenSymbols) {
    assert.doesNotMatch(scheduler, pattern, `${name} must be absent from emmettScheduler.js`);
  }

  // Confirm the scheduler only assesses infrastructure
  assert.match(scheduler, /assessInfrastructure/);
  assert.match(scheduler, /checkSendingDomainHealth/);
  assert.match(scheduler, /assessOutboundCapacity/);
});

test('SPEC-189: Emmett cron adapter wraps infrastructure scheduler only', () => {
  const adapter = fs.readFileSync(path.join(__dirname, '../emmettSchedulerCron.js'), 'utf8');

  for (const { name, pattern } of forbiddenSymbols) {
    assert.doesNotMatch(adapter, pattern, `${name} must be absent from emmettSchedulerCron.js`);
  }

  assert.match(adapter, /assessInfrastructure/);
  assert.match(adapter, /run\(/);
});
