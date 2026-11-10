'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const path = require('node:path');
const { TARGET, stagingConfig, cleanEnvironment } = require('../scripts/maxContinuity/policy.cjs');
const valid = () => ({ RAILWAY_PROJECT_ID: TARGET.project, RAILWAY_ENVIRONMENT_ID: TARGET.environment,
  RAILWAY_SERVICE_ID: TARGET.service, MAX_CONTINUITY_DATABASE_SERVICE_ID: TARGET.postgres,
  MAX_CONTINUITY_REHEARSAL: 'staging-only',
  MAX_CONTINUITY_DATABASE_URL: 'postgresql://max_continuity_rehearsal:fixture-secret@postgres.railway.internal:5432/max_continuity_rehearsal' });

test('staging guard requires every exact target and restricted private database', () => {
  assert.equal(stagingConfig(valid()).requireRestrictedRole, true);
  for (const key of Object.keys(valid())) {
    assert.throws(() => stagingConfig({ ...valid(), [key]: undefined }));
  }
  for (const [key, value] of [
    ['RAILWAY_PROJECT_ID', 'other'], ['RAILWAY_ENVIRONMENT_ID', '7046b237-d3a6-4884-8a1e-bd75d809fa3b'],
    ['RAILWAY_SERVICE_ID', TARGET.postgres], ['MAX_CONTINUITY_DATABASE_SERVICE_ID', TARGET.service],
    ['MAX_CONTINUITY_REHEARSAL', 'true'],
  ]) assert.throws(() => stagingConfig({ ...valid(), [key]: value }));
  for (const url of [
    'postgresql://postgres:secret@postgres.railway.internal/postgres',
    'postgresql://max_continuity_rehearsal:secret@production.invalid/max_continuity_rehearsal',
    'postgresql://max_continuity_rehearsal:secret@127.0.0.1/max_continuity_rehearsal',
    valid().MAX_CONTINUITY_DATABASE_URL + '?host=production.invalid',
    valid().MAX_CONTINUITY_DATABASE_URL.replace(':5432', ':9999'),
    valid().MAX_CONTINUITY_DATABASE_URL.replace(':fixture-secret', ''),
  ]) assert.throws(() => stagingConfig({ ...valid(), MAX_CONTINUITY_DATABASE_URL: url }));
  assert.equal(cleanEnvironment({}).ANTHROPIC_API_KEY, undefined);
});

test('CLI rejects production scope before loading app code and logs no input values', () => {
  const result = spawnSync(process.execPath, ['scripts/maxContinuity/run.cjs'], {
    cwd: path.join(__dirname, '..'), encoding: 'utf8', timeout: 5000,
    env: { ...valid(), RAILWAY_ENVIRONMENT_ID: '7046b237-d3a6-4884-8a1e-bd75d809fa3b',
      DATABASE_URL: 'PRODUCTION_SECRET', ANTHROPIC_API_KEY: 'PROVIDER_SECRET' },
  });
  assert.equal(result.status, 1);
  assert.deepEqual(JSON.parse(result.stdout), { version: 1, result: 'fail', reason: 'staging_guard_rejected', cleanup: 'not-needed' });
  assert.equal(result.stderr, '');
  assert.doesNotMatch(result.stdout, /secret|postgresql|password/i);
});

test('egress guard blocks every transport and records swallowed attempts', () => {
  const { installGuard } = require('../scripts/maxContinuity/egress.cjs');
  const guard = installGuard({ host: '127.0.0.1', port: 1 }, { blockSubprocess: true });
  try {
    guard.selfTest(); guard.assertClean();
    for (const attempt of [
      () => new (require('node:net').Socket)().connect({ host: 'production.invalid', port: 5432 }),
      () => require('node:tls').connect({ host: 'production.invalid', port: 443 }),
      () => fetch('https://production.invalid'),
      () => require('node:dgram').createSocket('udp4'),
      () => require('node:dns').lookup('production.invalid', () => {}),
      () => require('node:child_process').execFileSync('curl', ['https://production.invalid']),
      () => guard.deny(),
    ]) assert.throws(attempt, { code: 'EGRESS_DENIED' });
    assert.throws(() => guard.assertClean(), /unexpected_egress_attempt/);
  } finally { guard.restore(); }
});
