'use strict';

const USER_AGENT = 'pulseforge-levi-scout-research-only/1.0';
const TIMEOUT_MS = 8000;
const MAX_ATTEMPTS = 3;
const BACKOFF_MS = Object.freeze([250, 750]);
const MAX_REQUESTS_PER_LOOKUP = 4;

function fail(code) { throw Object.assign(new Error(code), { code }); }

function isPaidEnrichmentUrl(url) {
  return /maps\.googleapis\.com|places\.googleapis\.com|serpapi\.com|hunter\.io|prospeo\.io|apollo\.io|clearbit\.com|lusha\.com/i
    .test(String(url || ''));
}

function isTransientFetchError(error) {
  if (!error) return false;
  const code = error.code;
  const name = error.name;
  const message = String(error.message || '');
  return code === 23
    || code === 'ABORT_ERR'
    || code === 'ETIMEDOUT'
    || code === 'ECONNRESET'
    || code === 'EAI_AGAIN'
    || code === 'UND_ERR_CONNECT_TIMEOUT'
    || name === 'TimeoutError'
    || name === 'AbortError'
    || /aborted due to timeout|network timeout|fetch failed|UND_ERR_CONNECT_TIMEOUT/i.test(message);
}

function requestHeaders() {
  return { Accept: 'application/json', 'User-Agent': USER_AGENT };
}

function timeoutSignal(ms) {
  if (typeof AbortSignal === 'function' && typeof AbortSignal.timeout === 'function') {
    return AbortSignal.timeout(ms);
  }
  return undefined;
}

async function waitMs(ms, wait) {
  if (!ms) return;
  if (typeof wait === 'function') {
    await wait(ms);
    return;
  }
  await new Promise(resolve => setTimeout(resolve, ms));
}

function datasetIdOf(url) {
  const match = /resource\/([a-z0-9]{4}-[a-z0-9]{4})/i.exec(String(url || ''));
  return match ? match[1].toLowerCase() : null;
}

function createLookupBudget(maxRequests = MAX_REQUESTS_PER_LOOKUP) {
  let used = 0;
  return {
    max: maxRequests,
    used() { return used; },
    remaining() { return Math.max(0, maxRequests - used); },
    consume() {
      if (used >= maxRequests) return false;
      used += 1;
      return true;
    },
  };
}

async function readJson({
  fetchImpl,
  url,
  timeoutMs = TIMEOUT_MS,
  maxAttempts = MAX_ATTEMPTS,
  wait,
  telemetry,
} = {}) {
  if (isPaidEnrichmentUrl(url)) fail('levi_research_paid_provider_approval_required');
  const events = Array.isArray(telemetry) ? telemetry : [];
  let lastStatus = 'empty';
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    const started = Date.now();
    try {
      const options = { headers: requestHeaders() };
      const signal = timeoutSignal(timeoutMs);
      if (signal) options.signal = signal;
      const response = await fetchImpl(url, options);
      if (!response) {
        lastStatus = 'empty_response';
        events.push({ url, attempt, status: lastStatus, datasetId: datasetIdOf(url) });
        return { rows: [], status: lastStatus, attempts: attempt, telemetry: events };
      }
      if (response.ok === false) {
        const httpStatus = Number(response.status) || 0;
        const retryable = httpStatus === 429 || httpStatus >= 500;
        lastStatus = 'http_error';
        events.push({
          url,
          attempt,
          status: lastStatus,
          httpStatus,
          datasetId: datasetIdOf(url),
          ms: Date.now() - started,
        });
        if (retryable && attempt < maxAttempts) {
          await waitMs(BACKOFF_MS[attempt - 1] || BACKOFF_MS.at(-1), wait);
          continue;
        }
        return { rows: [], status: lastStatus, attempts: attempt, httpStatus, telemetry: events };
      }
      const body = typeof response.json === 'function' ? await response.json() : [];
      const rows = Array.isArray(body) ? body : [];
      lastStatus = rows.length ? 'ok' : 'empty';
      events.push({
        url,
        attempt,
        status: lastStatus,
        rows: rows.length,
        datasetId: datasetIdOf(url),
        ms: Date.now() - started,
      });
      return { rows, status: lastStatus, attempts: attempt, telemetry: events };
    } catch (error) {
      if (error && error.code === 'levi_research_paid_provider_approval_required') throw error;
      const transient = isTransientFetchError(error);
      lastStatus = transient ? 'timeout' : 'error';
      events.push({
        url,
        attempt,
        status: lastStatus,
        code: error && error.code,
        name: error && error.name,
        message: String(error && error.message || '').slice(0, 160),
        datasetId: datasetIdOf(url),
        ms: Date.now() - started,
      });
      if (transient && attempt < maxAttempts) {
        await waitMs(BACKOFF_MS[attempt - 1] || BACKOFF_MS.at(-1), wait);
        continue;
      }
      return { rows: [], status: lastStatus, attempts: attempt, telemetry: events };
    }
  }
  return { rows: [], status: lastStatus, attempts: maxAttempts, telemetry: events };
}

async function boundedReadJson({
  fetchImpl,
  url,
  budget,
  timeoutMs,
  wait,
  telemetry,
} = {}) {
  if (!budget || !budget.consume()) {
    const event = { url, status: 'skipped_budget', datasetId: datasetIdOf(url) };
    if (Array.isArray(telemetry)) telemetry.push(event);
    return { rows: [], status: 'skipped_budget', attempts: 0, telemetry: Array.isArray(telemetry) ? telemetry : [event] };
  }
  return readJson({ fetchImpl, url, timeoutMs, wait, telemetry });
}

module.exports = {
  USER_AGENT,
  TIMEOUT_MS,
  MAX_ATTEMPTS,
  BACKOFF_MS,
  MAX_REQUESTS_PER_LOOKUP,
  isPaidEnrichmentUrl,
  isTransientFetchError,
  createLookupBudget,
  readJson,
  boundedReadJson,
  datasetIdOf,
};
