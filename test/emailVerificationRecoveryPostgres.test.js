'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { retryUnverifiedEmails } = require('./helpers/loadEmailVerificationRecovery')();
const { governedContactReason } = require('../utils/governedContactEligibility');

const prospectId = id => `10000000-0000-4000-8000-${String(id).padStart(12, '0')}`;
const CHECKED_AT = '2026-10-13T19:00:00.123456Z';
const verification = (status = 'valid') => ({
  emailVerified: status === 'valid', emailStatus: status,
  emailVerificationMethod: 'fixture_after', verifierResponse: { status },
  verifiedAt: CHECKED_AT, verifierCheckedAt: CHECKED_AT,
  doNotContact: false, reject: false,
});

// This test never falls back to DATABASE_URL and never skips a missing local
// PostgreSQL binary. No real verification module or API is loaded.
test('verification retry persistence and concurrency on disposable PostgreSQL', { timeout: 90000 }, async t => {
  const server = await startDisposablePostgres('verification-retry-pg-');
  const pool = new Pool({ connectionString: server.connectionString });
  t.after(async () => { await pool.end(); await server.stop(); });
  await pool.query(`CREATE TABLE prospects (
    id UUID PRIMARY KEY, client_id INTEGER NOT NULL, email TEXT,
    email_verified BOOLEAN, email_status TEXT, email_verification_method TEXT,
    verifier_response JSONB, verifier_checked_at TIMESTAMPTZ, verified_at TIMESTAMPTZ,
    do_not_contact BOOLEAN, company_id UUID, notes TEXT, updated_at TIMESTAMPTZ DEFAULT NOW(),
    acquisition_metadata JSONB, enrichment_provenance JSONB,
    domain TEXT DEFAULT 'customer.example'
  )`);

  async function seed(clientId = 13, id = 1, response = '{"reason":"verifier_timeout"}') {
    await pool.query(`INSERT INTO prospects (
      id, client_id, email, email_verified, email_status, email_verification_method,
      verifier_response, verifier_checked_at, verified_at, do_not_contact, company_id, notes,
      acquisition_metadata, enrichment_provenance
    ) VALUES ($1, $2, 'owner@customer.example', false, 'unknown', 'fixture_before',
      $3::jsonb, $4, $4, false, '20000000-0000-4000-8000-000000000101', 'Original note',
      '{"contactResolution":{"finalState":"VERIFIED_FOUNDER_EMAIL","bestEmail":"owner@customer.example"}}',
      '{"email":{"source":"website_email"}}')`, [prospectId(id), clientId, response, CHECKED_AT]);
  }
  async function snapshot(id = 1) {
    return (await pool.query('SELECT to_jsonb(p) AS row FROM prospects p WHERE id = $1', [prospectId(id)])).rows[0]?.row;
  }
  async function reset() { await pool.query('TRUNCATE prospects'); }

  for (const clientId of [1, 10, 13, 17]) {
    await t.test(`tenant ${clientId} writes only its own row and repeated runs do not overcount`, async () => {
      await reset();
      for (const id of [1, 10, 13, 17]) await seed(id, id);
      const others = await Promise.all([1, 10, 13, 17].filter(id => id !== clientId).map(snapshot));
      const result = await retryUnverifiedEmails(pool, { clientId, verify: async () => verification() });
      assert.equal(result.verificationRetryValid, 1);
      assert.equal(result.verificationRetryFailed, 0);
      const row = await snapshot(clientId);
      assert.equal(row.email_verified, true);
      assert.equal(row.email_status, 'valid');
      assert.equal(row.do_not_contact, false);
      assert.equal(row.email_verification_method, 'fixture_after');
      assert.equal(row.acquisition_metadata.contactResolution.finalState, 'VERIFIED_FOUNDER_EMAIL');
      assert.deepEqual(await Promise.all([1, 10, 13, 17].filter(id => id !== clientId).map(snapshot)), others);
      const repeated = await retryUnverifiedEmails(pool, { clientId, verify: async () => assert.fail('already verified') });
      assert.equal(repeated.verificationRetryAttempted, 0);
      assert.equal(repeated.verificationRetryValid, 0);
    });
  }

  await t.test('equivalent normalized address is verified without rewriting stored email', async () => {
    await reset(); await seed();
    await pool.query("UPDATE prospects SET email = ' Owner@Customer.Example ' WHERE id = $1", [prospectId(1)]);
    const result = await retryUnverifiedEmails(pool, { clientId: 13, verify: async email => {
      assert.equal(email, 'owner@customer.example');
      await pool.query("UPDATE prospects SET email = 'OWNER@customer.example' WHERE id = $1", [prospectId(1)]);
      return verification();
    } });
    assert.equal(result.verificationRetryValid, 1);
    assert.equal((await snapshot()).email, 'OWNER@customer.example');
  });

  for (const response of [null, 'null', '{"reason":"verifier_timeout"}']) {
    await t.test(`SQL NULL / JSON null and timestamp precision survive baseline CAS: ${response}`, async () => {
      await reset(); await seed(13, 1, response);
      const result = await retryUnverifiedEmails(pool, { clientId: 13, verify: async () => verification() });
      assert.equal(result.verificationRetryValid, 1);
      assert.equal(result.verificationRetryFailed, 0);
    });
  }

  for (const [name, change] of [
    ['tenant reassignment', 'client_id = 10'],
    ['changed email', "email = 'different@customer.example'"],
    ['changed company', "company_id = '20000000-0000-4000-8000-000000000999'"],
    ['concurrent opt-out', 'do_not_contact = true'],
    ['new verified result', 'email_verified = true'],
    ['new risky verdict', "email_status = 'risky'"],
    ['new verification method', "email_verification_method = 'other_verifier'"],
    ['new verifier response', "verifier_response = '{\"reason\":\"new_result\"}'::jsonb"],
    ['microsecond checked timestamp change', "verifier_checked_at = '2026-10-13T19:00:00.123457Z'"],
    ['microsecond verified timestamp change', "verified_at = '2026-10-13T19:00:00.123457Z'"],
    ['NULL verification baseline change', 'email_verified = NULL'],
    ['NULL DNC baseline change', 'do_not_contact = NULL'],
  ]) {
    await t.test(`${name} cannot be overwritten or counted as a success`, async () => {
      await reset(); await seed();
      let afterChange;
      const result = await retryUnverifiedEmails(pool, { clientId: 13, verify: async () => {
        await pool.query(`UPDATE prospects SET ${change} WHERE id = $1`, [prospectId(1)]);
        afterChange = await snapshot();
        return verification();
      } });
      assert.equal(result.verificationRetryValid, 0);
      assert.equal(result.verificationRetryFailed, 1);
      assert.deepEqual(await snapshot(), afterChange);
    });
  }

  await t.test('deleted candidates are failures without claiming inventory growth', async () => {
    await reset(); await seed();
    const result = await retryUnverifiedEmails(pool, { clientId: 13, verify: async () => {
      await pool.query('DELETE FROM prospects WHERE id = $1', [prospectId(1)]);
      return verification();
    } });
    assert.equal(result.verificationRetryValid, 0);
    assert.equal(result.verificationRetryFailed, 1);
    assert.equal(await snapshot(), undefined);
  });

  await t.test('two concurrent retries of one snapshot count a single persisted result', async () => {
    await reset(); await seed();
    let started = 0;
    let release;
    const bothStarted = new Promise(resolve => { release = resolve; });
    const verify = async () => { if (++started === 2) release(); await bothStarted; return verification(); };
    const results = await Promise.all([
      retryUnverifiedEmails(pool, { clientId: 13, verify }),
      retryUnverifiedEmails(pool, { clientId: 13, verify }),
    ]);
    assert.equal(results.reduce((sum, r) => sum + r.verificationRetryValid, 0), 1);
    assert.equal(results.reduce((sum, r) => sum + r.verificationRetryFailed, 0), 1);
    assert.equal((await snapshot()).notes.split('email verification retried').length - 1, 1);
  });

  await t.test('a row-locked concurrent opt-out wins over an in-flight valid response', async () => {
    await reset(); await seed();
    const other = await pool.connect();
    try {
      await other.query('BEGIN');
      await other.query('SELECT id FROM prospects WHERE id = $1 FOR UPDATE', [prospectId(1)]);
      let ready;
      const verifying = new Promise(resolve => { ready = resolve; });
      const retry = retryUnverifiedEmails(pool, { clientId: 13, verify: async () => { ready(); return verification(); } });
      await verifying;
      await other.query('UPDATE prospects SET do_not_contact = true WHERE id = $1', [prospectId(1)]);
      await other.query('COMMIT');
      const result = await retry;
      assert.equal(result.verificationRetryValid, 0);
      assert.equal(result.verificationRetryFailed, 1);
      const row = await snapshot();
      assert.equal(row.do_not_contact, true);
      assert.equal(row.email_verified, false);
      assert.equal(row.notes, 'Original note');
    } finally { await other.query('ROLLBACK'); other.release(); }
  });

  for (const status of ['risky', 'catchall', 'invalid']) {
    await t.test(`${status} is stored without promotion and remains DNC`, async () => {
      await reset(); await seed();
      const result = await retryUnverifiedEmails(pool, { clientId: 13, verify: async () => verification(status) });
      const row = await snapshot();
      assert.equal(row.email_verified, false);
      assert.equal(row.do_not_contact, true);
      assert.equal(row.email_status, status);
      assert.equal(result.verificationRetryValid, 0);
      assert.equal(result[status === 'invalid' ? 'verificationRetryInvalid' : 'verificationRetryRisky'], 1);
    });
  }

  await t.test('provider failure never changes stored metadata or claims success', async () => {
    for (const verify of [async () => verification('unknown'), async () => { throw new Error('fixture timeout'); }]) {
      await reset(); await seed();
      const before = await snapshot();
      const result = await retryUnverifiedEmails(pool, { clientId: 13, verify });
      assert.equal(result.verificationRetryFailed, 1);
      assert.equal(result.verificationRetryValid, 0);
      assert.deepEqual(await snapshot(), before);
    }
  });

  for (const [classification, reason] of [
    ['VERIFIED_FOUNDER_EMAIL', null],
    ['VERIFIED_ROLE_EMAIL', 'contact_classification_not_authorized'],
    ['REVIEW_REQUIRED', 'contact_classification_not_sendable'],
  ]) {
    await t.test(`deliverability does not rewrite reviewed classification ${classification}`, async () => {
      await reset(); await seed();
      await pool.query(`UPDATE prospects SET acquisition_metadata = jsonb_set(
        acquisition_metadata, '{contactResolution,finalState}', $1::jsonb) WHERE id = $2`, [JSON.stringify(classification), prospectId(1)]);
      const before = await snapshot();
      const result = await retryUnverifiedEmails(pool, { clientId: 13, verify: async () => verification() });
      const row = await snapshot();
      assert.equal(result.verificationRetryValid, 1);
      assert.deepEqual(row.acquisition_metadata, before.acquisition_metadata);
      assert.deepEqual(row.enrichment_provenance, before.enrichment_provenance);
      assert.equal(governedContactReason(row, { tenantId: '13', allowedContactClassifications: ['VERIFIED_FOUNDER_EMAIL'] }), reason);
    });
  }
});
