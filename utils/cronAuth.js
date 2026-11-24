'use strict';

/**
 * MAX-P0-002 — fail-closed shared-secret check for legacy cron routes.
 *
 * The legacy guard `if (process.env.CRON_SECRET && secret !== process.env.CRON_SECRET)`
 * skipped authentication entirely when CRON_SECRET was unset or empty. On Render
 * production (2026-11-24) CRON_SECRET was unset, so those routes accepted any caller.
 *
 * This check refuses every request when the configured secret is missing or empty,
 * refuses an empty or non-string supplied secret, and compares in constant time.
 * Callers keep their existing transport (query/body/header); only the decision changes.
 */

const crypto = require('crypto');

function legacyCronSecretValid(supplied, env = process.env) {
  const configured = typeof env?.CRON_SECRET === 'string' ? env.CRON_SECRET : '';
  if (!configured) return false;
  if (typeof supplied !== 'string' || !supplied) return false;
  const expected = Buffer.from(configured);
  const actual = Buffer.from(supplied);
  if (expected.length !== actual.length) return false;
  return crypto.timingSafeEqual(expected, actual);
}

module.exports = { legacyCronSecretValid };
