const pool = require('../db');

const BOUNCER_VERIFY_URL = 'https://api.usebouncer.com/v1.1/email/verify';
const CACHE_TTL_MS = 24 * 60 * 60 * 1000;
const TIMEOUT_MS = 5000;
const cache = new Map();

let fetchOverride = null;
let logOverride = null;

function normalizeEmail(email) {
  return String(email || '').trim().toLowerCase();
}

function getRawStatus(raw) {
  return String(raw?.status || '').trim().toLowerCase();
}

function getRawReason(raw) {
  return String(raw?.reason || raw?.reason_code || raw?.verdict_reason || '').trim().toLowerCase();
}

function mapBouncerResponse(raw) {
  const rawStatus = getRawStatus(raw);
  const reason = getRawReason(raw);

  if (rawStatus === 'deliverable') {
    return { status: 'valid', valid: true, reason: reason || 'deliverable', raw, vendor: 'bouncer' };
  }

  if (rawStatus === 'undeliverable') {
    return { status: 'invalid', valid: false, reason: reason || 'undeliverable', raw, vendor: 'bouncer' };
  }

  if (rawStatus === 'risky') {
    if (reason === 'accept_all' || reason === 'catch_all') {
      return { status: 'catchall', valid: false, reason: reason || 'accept_all', raw, vendor: 'bouncer' };
    }
    return { status: 'risky', valid: false, reason: reason || 'risky', raw, vendor: 'bouncer' };
  }

  return { status: 'unknown', valid: false, reason: reason || rawStatus || 'unknown', raw, vendor: 'bouncer' };
}

async function logVerifierCall({ email, raw, durationMs }) {
  if (logOverride) return logOverride({ email, raw, durationMs });

  try {
    await pool.query(`
      INSERT INTO verifier_call_log (vendor, email, response, duration_ms)
      VALUES ($1, $2, $3::jsonb, $4)
    `, ['bouncer', email, JSON.stringify(raw || null), durationMs]);
  } catch (err) {
    console.error(`[EmailVerifier] verifier_call_log insert failed: ${err.message}`);
  }
}

function getCached(email) {
  const cached = cache.get(email);
  if (!cached) return null;
  if (Date.now() - cached.cachedAt > CACHE_TTL_MS) {
    cache.delete(email);
    return null;
  }
  return { ...cached.result, cached: true };
}

function setCached(email, result) {
  cache.set(email, { cachedAt: Date.now(), result });
}

async function verifyEmail(email) {
  const normalizedEmail = normalizeEmail(email);
  if (!normalizedEmail || !normalizedEmail.includes('@')) {
    return { status: 'invalid', valid: false, reason: 'invalid_format', raw: null, vendor: 'bouncer' };
  }

  const cached = getCached(normalizedEmail);
  if (cached) return cached;

  const apiKey = process.env.BOUNCER_API_KEY;
  if (!apiKey) {
    const result = { status: 'unknown', valid: false, reason: 'verifier_not_configured', raw: null, vendor: 'bouncer' };
    setCached(normalizedEmail, result);
    return result;
  }

  const startedAt = Date.now();
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), TIMEOUT_MS);
  let raw = null;

  try {
    const fetchImpl = fetchOverride || globalThis.fetch;
    if (typeof fetchImpl !== 'function') throw new Error('fetch_unavailable');

    const response = await fetchImpl(BOUNCER_VERIFY_URL, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-api-key': apiKey,
      },
      body: JSON.stringify({ email: normalizedEmail }),
      signal: controller.signal,
    });

    raw = await response.json().catch(() => ({
      status: 'unknown',
      reason: 'invalid_json_response',
      http_status: response.status,
    }));

    if (!response.ok) {
      raw = {
        ...raw,
        status: raw.status || 'unknown',
        reason: raw.reason || 'http_error',
        http_status: response.status,
      };
    }

    const result = response.ok
      ? mapBouncerResponse(raw)
      : { status: 'unknown', valid: false, reason: raw.reason || 'http_error', raw, vendor: 'bouncer' };

    await logVerifierCall({ email: normalizedEmail, raw, durationMs: Date.now() - startedAt });
    setCached(normalizedEmail, result);
    return result;
  } catch (err) {
    const reason = err?.name === 'AbortError' ? 'verifier_timeout' : 'verifier_timeout';
    raw = { status: 'unknown', reason, error: err?.message || reason };
    const result = { status: 'unknown', valid: false, reason, raw, vendor: 'bouncer' };
    await logVerifierCall({ email: normalizedEmail, raw, durationMs: Date.now() - startedAt });
    setCached(normalizedEmail, result);
    return result;
  } finally {
    clearTimeout(timeout);
  }
}

function clearVerifierCache() {
  cache.clear();
}

function setTestHooks({ fetchImpl, logImpl } = {}) {
  fetchOverride = fetchImpl || null;
  logOverride = logImpl || null;
}

module.exports = {
  verifyEmail,
  clearVerifierCache,
  mapBouncerResponse,
  _test: {
    setTestHooks,
  },
};
