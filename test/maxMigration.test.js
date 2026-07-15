const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const { mapLegacyStatus } = require('../utils/maxOrchestration');
const { migrationDecisionId, parseArgs } = require('../scripts/backfillMaxOrchestration');

test('backfill is dry-run by default and apply is explicit', () => {
  assert.equal(parseArgs([]).apply, false);
  assert.equal(parseArgs(['--apply']).apply, true);
});

test('legacy statuses map to canonical lifecycle states', () => {
  assert.equal(mapLegacyStatus('contacted'), 'heating');
  assert.equal(mapLegacyStatus('closed'), 'engaged');
  assert.equal(mapLegacyStatus('auto_responder'), 'nurture');
  assert.equal(mapLegacyStatus('bounced'), 'null');
  assert.equal(mapLegacyStatus('dead'), 'recycle');
  assert.equal(mapLegacyStatus('warm', true), 'disqualified');
});

test('migration decision identity is deterministic for resumability', () => {
  assert.equal(migrationDecisionId('abc'), migrationDecisionId('abc'));
  assert.notEqual(migrationDecisionId('abc'), migrationDecisionId('def'));
});

test('backfill source contains no prospect-facing integration calls', () => {
  const source = fs.readFileSync(path.join(__dirname, '../scripts/backfillMaxOrchestration.js'), 'utf8');
  for (const forbidden of ['sendEmail(', 'createTodoistTask(', 'enrollSequence(', 'retryEnrichment(', 'INSERT INTO agent_actions']) {
    assert.equal(source.includes(forbidden), false, `unexpected side effect token: ${forbidden}`);
  }
});
