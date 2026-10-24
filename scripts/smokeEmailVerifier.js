require('dotenv').config();
const assert = require('assert');
const {
  verifyEmail,
  clearVerifierCache,
  _test,
} = require('../utils/emailVerifier');

const cases = [
  {
    name: 'deliverable maps valid',
    raw: { status: 'deliverable', reason: 'accepted_email' },
    expected: { status: 'valid', valid: true, reason: 'accepted_email' },
  },
  {
    name: 'undeliverable maps invalid',
    raw: { status: 'undeliverable', reason: 'rejected_email' },
    expected: { status: 'invalid', valid: false, reason: 'rejected_email' },
  },
  {
    name: 'risky accept_all maps catchall',
    raw: { status: 'risky', reason: 'accept_all' },
    expected: { status: 'catchall', valid: false, reason: 'accept_all' },
  },
  {
    name: 'risky role_based maps risky',
    raw: { status: 'risky', reason: 'role_based' },
    expected: { status: 'risky', valid: false, reason: 'role_based' },
  },
  {
    name: 'unknown maps unknown',
    raw: { status: 'unknown', reason: 'timeout' },
    expected: { status: 'unknown', valid: false, reason: 'timeout' },
  },
];

async function run() {
  process.env.BOUNCER_API_KEY = process.env.BOUNCER_API_KEY || 'smoke-test-key';
  process.env.BOUNCER_ENABLED = 'true';
  const logged = [];
  const requests = [];

  _test.setTestHooks({
    logImpl: entry => {
      logged.push(entry);
    },
  });

  try {
    for (let i = 0; i < cases.length; i++) {
      const testCase = cases[i];
      clearVerifierCache();
      _test.setTestHooks({
        logImpl: entry => {
          logged.push(entry);
        },
        fetchImpl: async (url, options) => {
          requests.push({ url, options });
          return {
            ok: true,
            status: 200,
            json: async () => testCase.raw,
          };
        },
      });

      const result = await verifyEmail(`smoke-${i}@example.test`);
      assert.strictEqual(result.status, testCase.expected.status, testCase.name);
      assert.strictEqual(result.valid, testCase.expected.valid, testCase.name);
      assert.strictEqual(result.reason, testCase.expected.reason, testCase.name);
      assert.strictEqual(result.vendor, 'bouncer', testCase.name);
    }

    assert.strictEqual(logged.length, cases.length, 'logs one verifier call per test case');
    assert.strictEqual(requests.length, cases.length, 'makes one verifier request per test case');
    requests.forEach((request, index) => {
      const url = new URL(request.url);
      assert.strictEqual(request.options.method, 'GET', cases[index].name);
      assert.strictEqual(url.searchParams.get('email'), `smoke-${index}@example.test`, cases[index].name);
      assert.strictEqual(request.options.headers['x-api-key'], process.env.BOUNCER_API_KEY, cases[index].name);
      assert.strictEqual(logged[index].status, cases[index].expected.status, cases[index].name);
    });
    console.log('[smokeEmailVerifier] All verifier mapping smoke tests passed');
  } finally {
    _test.setTestHooks();
    clearVerifierCache();
  }
}

run().catch(err => {
  console.error(`[smokeEmailVerifier] Failed: ${err.message}`);
  process.exit(1);
});
