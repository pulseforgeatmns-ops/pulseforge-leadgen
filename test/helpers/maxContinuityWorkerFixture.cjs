'use strict';
// Local CI entry only; the staging CLI never selects this file or accepts a
// localhost URL. Exercise the identical worker and parent isolation on real PG.
(async () => {
  const assert = require('node:assert/strict');
  const config = JSON.parse(process.env.MAX_CONTINUITY_WORKER_CONFIG);
  assert.equal(config.host, '127.0.0.1');
  assert.equal(new URL(config.connectionString).pathname, '/max_continuity_rehearsal');
  for (const key of ['ANTHROPIC_API_KEY','BREVO_API_KEY','NODE_OPTIONS','MAX_CONVERSATION_CONTEXT','MAX_DECISION_ACTIVE_ROUTING']) {
    assert.equal(process.env[key], undefined);
  }
  assert.equal(require('node:fs').existsSync('.env'), false);
  const report = await require('../../scripts/maxContinuity/worker.cjs').runWorker(config);
  if (process.send) process.send(report);
  process.exitCode = report.result === 'pass' ? 0 : 1;
})().catch(() => { process.exitCode = 1; });
