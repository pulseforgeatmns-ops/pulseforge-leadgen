const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const read = file => fs.readFileSync(path.join(__dirname, '..', file), 'utf8');

test('live signal sources use failure-isolated orchestration adapters', () => {
  assert.match(read('routes/webhooks.js'), /safeIngestBrevoSignal\(result, payload\)/);
  assert.match(read('rileyAgent.js'), /safeIngestRileyReplySignal/);
  assert.match(read('utils/icpScoring.js'), /safeIngestIcpScoreChange/);
  assert.match(read('enrichProspects.js'), /safeIngestEnrichmentOutcome/);
  assert.match(read('tieredEnrichmentAgent.js'), /safeIngestEnrichmentOutcome/);
});

test('new orchestration services never mutate legacy operational status', () => {
  const files = [
    'utils/maxOrchestration.js', 'utils/maxSignalIngestion.js', 'utils/maxManualOverride.js',
    'maxDecayAgent.js', 'scripts/recalculateMaxOrchestration.js',
  ];
  for (const file of files) {
    assert.doesNotMatch(read(file), /SET\s+status\s*=/i, file);
  }
});

test('decay is registered behind the existing cron dispatcher', () => {
  const cron = read('routes/cron.js');
  assert.match(cron, /max_decay:\s*'\.\.\/maxDecayAgent'/);
  assert.match(cron, /agent === 'max_decay'/);
});
