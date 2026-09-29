'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const {
  classifyUnverifiedReason,
  isRetryableUnverifiedReason,
  retryUnverifiedEmails,
  emptyVerificationRetryTelemetry,
} = require('../services/emailVerificationRecovery');

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
            { id: 1, email: 'never@pm.example', email_verified: false },
            { id: 2, email: 'risky@pm.example', email_verified: false, email_status: 'risky' },
            { id: 3, email: 'stale@pm.example', email_verified: false, email_status: 'unknown',
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
