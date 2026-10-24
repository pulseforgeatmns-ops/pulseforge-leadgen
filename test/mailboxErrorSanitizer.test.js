'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  sanitizeMailboxError,
  sanitizeMailboxErrorText,
} = require('../utils/mailboxErrorSanitizer');

const SECRET = 'supersecret';

function assertSecretAbsent(output) {
  assert.doesNotMatch(output, new RegExp(SECRET, 'i'));
}

describe('Mailbox Error Redaction Contract', () => {
  it('1. password=supersecret redacts credential value', () => {
    const out = sanitizeMailboxErrorText(`connection failed password=${SECRET}`);
    assert.match(out, /password=\[redacted\]/);
    assertSecretAbsent(out);
  });

  it('2. auth=supersecret becomes auth=[redacted]', () => {
    const out = sanitizeMailboxErrorText(`auth failed auth=${SECRET}`);
    assert.match(out, /auth=\[redacted\]/);
    assertSecretAbsent(out);
  });

  it('3. refresh_token=supersecret redacts credential value', () => {
    const out = sanitizeMailboxErrorText(`oauth refresh_token=${SECRET} expired`);
    assert.match(out, /refresh_token=\[redacted\]/);
    assertSecretAbsent(out);
  });

  it('4. client_secret=supersecret redacts credential value', () => {
    const out = sanitizeMailboxErrorText(`token client_secret=${SECRET}`);
    assert.match(out, /client_secret=\[redacted\]/);
    assertSecretAbsent(out);
  });

  it('5. object formatting failures do not leak [object Object]', () => {
    const out = sanitizeMailboxError(new Error(`auth failed password=${{ token: SECRET }}`));
    assert.doesNotMatch(out, /\[object Object\]/i);
    assert.match(out, /\[redacted\]/);
    assertSecretAbsent(out);
  });

  it('6. google_oauth_refresh_failed diagnostic preserved', () => {
    const out = sanitizeMailboxErrorText('google_oauth_refresh_failed during refresh');
    assert.match(out, /google_oauth_refresh_failed/);
  });

  it('7. invalid_grant diagnostic preserved', () => {
    const out = sanitizeMailboxErrorText('error=invalid_grant');
    assert.match(out, /invalid_grant/);
  });

  it('8. invalid_client diagnostic preserved', () => {
    const out = sanitizeMailboxErrorText('error=invalid_client');
    assert.match(out, /invalid_client/);
  });

  it('9. Google safe error_description preserved unless it embeds a credential', () => {
    const safe = sanitizeMailboxError(
      Object.assign(new Error('Google mailbox OAuth token refresh failed: Token has been revoked.'), {
        code: 'google_oauth_refresh_failed',
        httpStatus: 400,
        error: 'invalid_grant',
        error_description: 'Token has been revoked.',
      })
    );
    assert.match(safe, /error_description=Token has been revoked\./);
    assert.match(safe, /invalid_grant/);
    assert.match(safe, /httpStatus=400/);

    const leaky = sanitizeMailboxErrorText(
      `error_description=Rejected client_secret=${SECRET}`
    );
    assert.match(leaky, /client_secret=\[redacted\]/);
    assertSecretAbsent(leaky);
  });

  it('10. original secret literal never appears in sanitized output', () => {
    const sample = sanitizeMailboxErrorText(
      `password=${SECRET} auth=${SECRET} refresh_token=${SECRET} client_secret=${SECRET} passwd=${SECRET}`
    );
    assertSecretAbsent(sample);
    assert.match(sample, /password=\[redacted\]/);
    assert.match(sample, /auth=\[redacted\]/);
  });

  it('preserves OAuth and authentication_failed terminology without auth= assignment', () => {
    const out = sanitizeMailboxErrorText(
      'OAuth google_oauth_refresh_failed authentication_failed invalid_grant invalid_client'
    );
    assert.match(out, /OAuth/);
    assert.match(out, /google_oauth_refresh_failed/);
    assert.match(out, /authentication_failed/);
    assert.match(out, /invalid_grant/);
    assert.match(out, /invalid_client/);
  });
});
