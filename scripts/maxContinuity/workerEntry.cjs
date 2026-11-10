'use strict';
// Private child entry. Repeat URL/scope validation before loading any app code.
const { stagingConfig } = require('./policy.cjs');
(async () => {
  const raw = JSON.parse(process.env.MAX_CONTINUITY_WORKER_CONFIG || '{}');
  const config = stagingConfig({ ...process.env, MAX_CONTINUITY_DATABASE_URL: raw.connectionString });
  const report = await require('./worker.cjs').runWorker(config);
  if (process.send) process.send(report);
  process.exitCode = report.result === 'pass' ? 0 : 1;
})().catch(() => { process.exitCode = 1; });
