'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const {
  classifyUnverifiedReason,
  isRetryableUnverifiedReason,
  retryUnverifiedEmails,
  emptyVerificationRetryTelemetry,
} = require('./helpers/loadEmailVerificationRecovery')();

test('email verification classifier separates retryable from unsafe states', () => {
  assert.equal(classifyUnverifiedReason({ email: 'ops@pm.example' }), 'verification_never_attempted');
  assert.equal(classifyUnverifiedReason({
    email: 'ops@pm.example',
    verifier_response: { reason: 'verifier_timeout' },
  }), 'verifier_timeout');
  assert.equal(classifyUnverifiedReason({
    email: 'ops@pm.example',
    verifier_response: { reason: 'unavailable' },
  }), 'verifier_unavailable');
  assert.equal(classifyUnverifiedReason({
    email: 'ops@pm.example',
    email_status: 'risky',
  }), 'risky_catchall');
  assert.equal(classifyUnverifiedReason({
    email: 'ops@pm.example',
    email_status: 'invalid',
  }), 'invalid');
  assert.equal(classifyUnverifiedReason({ email: 'not-an-email' }), 'malformed_email');
  assert.equal(classifyUnverifiedReason({
    email: 'ops@pm.example',
    verifier_checked_at: new Date(Date.now() - 48 * 60 * 60 * 1000).toISOString(),
    email_status: 'unknown',
  }), 'stale_verification');
  assert.equal(isRetryableUnverifiedReason('verification_never_attempted'), true);
  assert.equal(isRetryableUnverifiedReason('risky_catchall'), false);
  assert.equal(isRetryableUnverifiedReason('invalid'), false);
});

test('verification retry promotes only valid results and never risky/invalid', async () => {
  const updates = [];
  const pool = {
    query: async (sql, params) => {
      if (/FROM prospects/i.test(sql) && /email_verified/i.test(sql)) {
        return {
          rows: [
            { id: 1, client_id: 10, email: 'never@pm.example', email_verified: false },
            { id: 2, client_id: 10, email: 'risky@pm.example', email_verified: false, email_status: 'risky' },
            { id: 3, client_id: 10, email: 'stale@pm.example', email_verified: false, email_status: 'unknown',
              verifier_checked_at: new Date(Date.now() - 48 * 60 * 60 * 1000).toISOString() },
          ],
        };
      }
      if (/UPDATE prospects/i.test(sql)) {
        updates.push({ id: params[0], verified: params[1], status: params[4] });
        return { rowCount: 1, rows: [] };
      }
      return { rows: [], rowCount: 0 };
    },
  };

  const result = await retryUnverifiedEmails(pool, {
    verify: async (email) => {
      if (email.startsWith('never')) {
        return {
          emailVerified: true,
          emailStatus: 'valid',
          emailVerificationMethod: 'bouncer',
          verifiedAt: new Date(),
          verifierCheckedAt: new Date(),
          doNotContact: false,
          reject: false,
        };
      }
      if (email.startsWith('stale')) {
        return {
          emailVerified: false,
          emailStatus: 'catchall',
          doNotContact: true,
          reject: false,
        };
      }
      throw new Error('should not retry risky');
    },
  });

  assert.equal(result.emailNotVerified, 3);
  assert.equal(result.verificationRetryAttempted, 2);
  assert.equal(result.verificationRetryValid, 1);
  assert.equal(result.verificationRetryRisky, 1);
  assert.equal(result.skipped.risky_catchall, 1);
  assert.equal(updates.length, 2);
  assert.equal(updates.find(row => row.id === 1).verified, true);
  assert.equal(updates.find(row => row.id === 3).verified, false);
});

test('empty verification retry telemetry starts at zero', () => {
  const empty = emptyVerificationRetryTelemetry();
  assert.equal(empty.verificationRetryValid, 0);
  assert.equal(empty.verificationRetryAttempted, 0);
});

for (const rowCount of [0, 2, undefined]) {
  test(`does not claim a valid verification when persistence rowCount is ${rowCount}`, async () => {
    const pool = { query: async sql => /UPDATE prospects/i.test(sql)
      ? { rowCount, rows: [] }
      : { rows: [{ id: 13, client_id: 13, email: 'owner@customer.example', email_verified: false }] } };
    const result = await retryUnverifiedEmails(pool, { clientId: 13, verify: async () => ({
      emailVerified: true, emailStatus: 'valid', doNotContact: false,
    }) });
    assert.equal(result.verificationRetryValid, 0);
    assert.equal(result.verificationRetryFailed, 1);
  });
}

const CHECKED_AT = '2026-10-13T19:00:00.123456Z';
function candidate(overrides = {}) {
  return { id: 'prospect-fixture', client_id: 13, email: 'Owner@Customer.Example ',
    email_verified: false, email_status: 'unknown', email_verification_method: 'fixture_before',
    verifier_response: { reason: 'verifier_timeout' },
    verifier_response_snapshot: '{"reason":"verifier_timeout"}',
    verifier_checked_at: CHECKED_AT, verified_at: CHECKED_AT,
    do_not_contact: false, company_id: 'company-fixture', ...overrides };
}
function verification(outcome = 'valid') {
  return { emailVerified: outcome === 'valid', emailStatus: outcome,
    emailVerificationMethod: 'fixture_after', verifierResponse: { status: outcome },
    verifiedAt: CHECKED_AT, verifierCheckedAt: CHECKED_AT, doNotContact: false, reject: false };
}
function tracePool(row, result = { rowCount: 1, rows: [] }) {
  const reads = [];
  const writes = [];
  return { reads, writes, query: async (sql, args) => {
    if (/UPDATE prospects/i.test(sql)) { writes.push({ sql, args }); return result; }
    reads.push({ sql, args });
    return { rows: [row] };
  } };
}

for (const clientId of [1, 10, 13, 17, '13']) {
  test(`verification read and guarded write stay on requested tenant ${JSON.stringify(clientId)}`, async () => {
    const row = candidate({ client_id: Number(clientId) });
    const pool = tracePool(row);
    const result = await retryUnverifiedEmails(pool, { clientId, verify: async (email, input) => {
      assert.equal(email, 'owner@customer.example');
      assert.equal(input.email, email);
      return verification();
    } });
    assert.equal(result.verificationRetryValid, 1);
    assert.equal(result.verificationRetryFailed, 0);
    assert.equal(pool.reads[0].args[0], Number(clientId));
    const { sql, args } = pool.writes[0];
    assert.match(sql, /WHERE id = \$1 AND client_id = \$10/);
    assert.doesNotMatch(sql, /client_id\s*=\s*10\b/);
    assert.equal(args[9], Number(clientId));
    assert.equal(args[10], 'owner@customer.example');
    assert.deepEqual(Array.from(args.slice(11)), [false, 'fixture_before', CHECKED_AT, 'unknown',
      '{"reason":"verifier_timeout"}', CHECKED_AT, 'company-fixture', false]);
    assert.match(pool.reads[0].sql, /verifier_response::text AS verifier_response_snapshot/);
    assert.match(pool.reads[0].sql, /verifier_checked_at::text AS verifier_checked_at/);
    assert.match(pool.reads[0].sql, /verified_at::text AS verified_at/);
    assert.match(pool.reads[0].sql, /ORDER BY prospects\.verifier_checked_at/);
    for (const clause of [
      'LOWER(BTRIM(email)) = $11',
      'COALESCE(do_not_contact, false) = false',
      'COALESCE(email_verified, false) = false',
      'email_verified IS NOT DISTINCT FROM $12',
      'email_verification_method IS NOT DISTINCT FROM $13',
      'verified_at IS NOT DISTINCT FROM $14::timestamptz',
      'email_status IS NOT DISTINCT FROM $15',
      'verifier_response IS NOT DISTINCT FROM $16::jsonb',
      'verifier_checked_at IS NOT DISTINCT FROM $17::timestamptz',
      'company_id IS NOT DISTINCT FROM $18',
      'do_not_contact IS NOT DISTINCT FROM $19',
    ]) assert.ok(sql.includes(clause), `missing write guard: ${clause}`);
    assert.doesNotMatch(sql, /SET[\s\S]*(?:acquisition_metadata|enrichment_provenance|policy_hash|owner_id)\s*=/);
  });
}

test('omitting clientId preserves the Anchor default', async () => {
  const pool = tracePool(candidate({ client_id: 10 }));
  assert.equal((await retryUnverifiedEmails(pool, { verify: async () => verification() })).verificationRetryValid, 1);
  assert.equal(pool.reads[0].args[0], 10);
  assert.equal(pool.writes[0].args[9], 10);
});

test('invalid tenant IDs fail before reads, verifier calls or updates', async () => {
  for (const clientId of [null, '', '13suffix', '1e1', '13.5', true, [], {}, 0, -1, 1.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1]) {
    const pool = { query: async () => assert.fail('invalid tenant must not query') };
    await assert.rejects(retryUnverifiedEmails(pool, { clientId,
      verify: async () => assert.fail('invalid tenant must not verify') }), /positive integer clientId/);
  }
});

test('wrong tenant and no-longer-eligible loaded candidates cannot invoke the verifier', async () => {
  for (const change of [{ client_id: 10 }, { client_id: undefined }, { do_not_contact: true }, { email_verified: true }]) {
    const pool = tracePool(candidate(change));
    const result = await retryUnverifiedEmails(pool, { clientId: 13,
      verify: async () => assert.fail('out-of-scope candidate must not verify') });
    assert.equal(result.verificationRetryValid, 0);
    assert.equal(result.verificationRetryFailed, 1);
    assert.equal(pool.writes.length, 0);
  }
});

for (const outcome of ['valid', 'risky', 'catchall', 'invalid']) {
  for (const rowCount of [0, 1, 2, undefined]) {
    test(`${outcome} counts only one persisted row (rowCount ${rowCount})`, async () => {
      const pool = tracePool(candidate(), { rowCount });
      const result = await retryUnverifiedEmails(pool, { clientId: 13, verify: async () => verification(outcome) });
      const expected = outcome === 'catchall' ? 'Risky' : outcome[0].toUpperCase() + outcome.slice(1);
      for (const name of ['Valid', 'Risky', 'Invalid']) {
        assert.equal(result[`verificationRetry${name}`], rowCount === 1 && expected === name ? 1 : 0);
      }
      assert.equal(result.verificationRetryFailed, rowCount === 1 ? 0 : 1);
      assert.equal(pool.writes[0].args[1], outcome === 'valid');
      assert.equal(pool.writes[0].args[7], outcome !== 'valid');
    });
  }
}

test('provider timeout, unavailable, unknown and thrown errors preserve stored verification metadata', async () => {
  for (const verify of [
    async () => ({ ...verification('unknown'), verifierResponse: { reason: 'verifier_timeout' } }),
    async () => ({ ...verification('unknown'), verifierResponse: { reason: 'verifier_unavailable' } }),
    async () => verification('unknown'),
    async () => { throw new Error('fixture provider failed'); },
    async () => null,
  ]) {
    const row = candidate();
    const original = structuredClone(row);
    const pool = tracePool(row);
    const result = await retryUnverifiedEmails(pool, { clientId: 13, verify });
    assert.equal(result.verificationRetryAttempted, 1);
    assert.equal(result.verificationRetryValid, 0);
    assert.equal(result.verificationRetryFailed, 1);
    assert.equal(pool.writes.length, 0);
    assert.deepEqual(row, original);
  }
});

test('persistence exception counts failure once and later candidates can still succeed', async () => {
  let calls = 0;
  const pool = { query: async sql => {
    if (!/UPDATE prospects/i.test(sql)) return { rows: [candidate(), candidate({ id: 'next' })] };
    if (++calls === 1) throw new Error('fixture write error');
    return { rowCount: 1 };
  } };
  const result = await retryUnverifiedEmails(pool, { clientId: 13, verify: async () => verification() });
  assert.equal(result.verificationRetryAttempted, 2);
  assert.equal(result.verificationRetryValid, 1);
  assert.equal(result.verificationRetryFailed, 1);
});

test('retrying a previously verified fixture counts no duplicate success', async () => {
  let saved = false;
  const pool = { query: async sql => {
    if (!/UPDATE prospects/i.test(sql)) return { rows: saved ? [] : [candidate()] };
    saved = true;
    return { rowCount: 1 };
  } };
  const options = { clientId: 13, verify: async () => verification() };
  assert.equal((await retryUnverifiedEmails(pool, options)).verificationRetryValid, 1);
  const repeated = await retryUnverifiedEmails(pool, options);
  assert.equal(repeated.verificationRetryAttempted, 0);
  assert.equal(repeated.verificationRetryValid, 0);
});
