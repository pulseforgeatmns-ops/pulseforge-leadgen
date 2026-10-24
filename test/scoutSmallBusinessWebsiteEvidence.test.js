'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { acquireBusinessEvidence } = require('../services/scoutWebsiteBusinessEvidence');
const { evaluateReplenishmentAdmission } = require('../utils/replenishmentVertical');
const previousEnabled = process.env.SCOUT_DURABLE_CANDIDATES_ENABLED;
process.env.SCOUT_DURABLE_CANDIDATES_ENABLED = 'true';
test.after(() => {
  if (previousEnabled == null) delete process.env.SCOUT_DURABLE_CANDIDATES_ENABLED;
  else process.env.SCOUT_DURABLE_CANDIDATES_ENABLED = previousEnabled;
});

const candidate = { name: 'Blue Door', website: 'https://bluedoor.example', location: 'Manchester, NH' };
const context = { tenantId: '13', missionSegment: 'small_business_owners', allowedCities: ['Manchester'] };
async function investigate(html, overrides = {}, candidateOverrides = {}) {
  const calls = [];
  const result = await acquireBusinessEvidence({ ...candidate, ...candidateOverrides }, { ...context, ...overrides }, {
    fetchPage: async url => {
      calls.push(url);
      return { ok: true, url, text: `<html><body>${html}</body></html>` };
    },
  });
  return { result, calls };
}

test('existing bounded crawl recognizes actual small-business operators in the mission taxonomy', async t => {
  for (const [vertical, statement] of [
    ['cleaning', 'We provide residential cleaning services in Manchester.'],
    ['landscaping', 'We are a landscaping company serving Manchester.'],
    ['painting', 'We provide house painting services in Manchester.'],
    ['hvac', 'We are an HVAC contractor based in Manchester.'],
    ['electrician', 'We are an electrical contractor serving Manchester.'],
    ['home_services', 'We provide home repair services in Manchester.'],
    ['restaurant', 'We are a family-owned restaurant in Manchester.'],
    ['salon', 'We are an independent hair salon in Manchester.'],
    ['fitness', 'We are an independent gym in Manchester.'],
    ['auto', 'We are an auto repair shop in Manchester.'],
  ]) await t.test(vertical, async () => {
    assert.equal(evaluateReplenishmentAdmission(candidate, context).reason, 'unclassifiable_vertical');
    const { result, calls } = await investigate(`<h1>Blue Door</h1><p>${statement}</p>`);
    assert.equal(result?.admission.vertical, vertical);
    assert.equal(result?.evidence.quote, statement);
    assert.equal(result?.evidence.source_url, 'https://bluedoor.example/');
    assert.equal(result?.candidate.description, statement);
    assert.equal(calls.length, 3, 'same existing three-successful-page crawl');
    assert.equal(result?.candidate.email, undefined, 'business fit cannot create a contact');
    assert.equal(result?.candidate.acquisition_metadata, undefined, 'business fit cannot assert founder qualification');
  });
});

test('operator headings and inline formatting remain source-bound business evidence', async () => {
  for (const html of ['<h1>Residential Cleaning Services</h1>', '<p>We provide <strong>residential cleaning services</strong> in Manchester.</p>']) {
    const { result } = await investigate(html);
    assert.equal(result?.admission.vertical, 'cleaning');
  }
});

test('query labels and non-operator, hidden, quoted, or unrelated prose never supply business evidence', async t => {
  for (const html of [
    '<h1>Blue Door</h1><p>Cleaning Company Manchester NH</p>',
    '<h1>Software for Cleaning Companies</h1><p>We provide software for cleaning companies.</p>',
    '<h1>Cleaning Company Insurance</h1><p>We provide insurance to cleaning companies.</p>',
    '<p>We are not a cleaning company.</p>', '<p>We were a cleaning company.</p>',
    '<p>Our customer is a cleaning company.</p>', '<p>They provide cleaning services.</p>',
    '<blockquote>We are a cleaning company.</blockquote>',
    '<div class="testimonials"><h1>Cleaning Company</h1><p>We are a cleaning company.</p></div>',
    '<article><h1>Cleaning Company</h1><p>We are a cleaning company.</p></article>',
    '<div hidden><h1>Cleaning Company</h1></div>',
    '<div aria-hidden="true"><p>We are a cleaning company.</p></div>',
    '<div style="display: none"><p>We are a cleaning company.</p></div>',
    '<script>We are a cleaning company.</script>',
    '<nav><h1>Cleaning Company</h1></nav>',
    '<footer><p>We are a cleaning company.</p></footer>',
    '<p>We provide</p><p>cleaning services.</p>',
    '<p>We provide cleaning services training for other businesses.</p>',
    '<h1>Cleaning Company Web Design</h1>', '<h1>Cleaning Company Jobs</h1>',
    '<h1>Cleaning Company Growth Secrets</h1>',
    '<div itemtype="https://schema.org/Review"><p>We are a cleaning company.</p></div>',
    '<h2>Our Clients</h2><div><p>We are a cleaning company.</p></div>',
    '<h2>Portfolio</h2><h3>Cleaning Company</h3><p>We provide cleaning services.</p>',
    '<p>We provide <span hidden>insurance for</span> cleaning services.</p>',
    '<h1>Cleaning Company</h1><p>We are a software company.</p>',
  ]) await t.test(html, async () => {
    const { result, calls } = await investigate(html, { discoveryQuery: 'Cleaning Company Manchester NH', discoveryConcept: 'Cleaning Company' });
    assert.equal(result, null);
    assert.equal(calls.length, 3);
  });
});

test('original observations and flattened descriptive facts are retained and cannot be overwritten by new evidence', async () => {
  const existing = { description: 'Located in Manchester.', discoverySource: 'public_business_data',
    classificationObservations: [{ name: 'Blue Door', description: 'Independent local business.', discoveryQuery: 'Small Business' }] };
  const { result } = await investigate('<p>We are a cleaning company.</p>', {}, existing);
  assert.ok(result);
  assert.equal(result.candidate.classificationObservations[0], existing.classificationObservations[0]);
  assert.ok(result.candidate.classificationObservations.some(row => row.description === existing.description));
  for (const conflicting of [
    { description: 'We provide software for cleaning companies.' },
    { classificationObservations: [{ description: 'A marketing agency for local cleaning companies.' }] },
  ]) assert.equal((await investigate('<h1>Cleaning Company</h1>', {}, conflicting)).result, null);
});

test('a new sibling first-party section can follow a testimonial section without borrowing its content', async () => {
  const { result } = await investigate('<h2>Our Clients</h2><p>We are an HVAC contractor.</p>'
    + '<h2>Our Services</h2><p>We provide cleaning services.</p>');
  assert.equal(result?.admission.vertical, 'cleaning');
  assert.equal(result?.evidence.observations.length, 1);
});

test('mission scope and original geography boundaries remain unchanged', async () => {
  const html = '<p>We are a cleaning company in Manchester.</p>';
  assert.equal((await investigate(html, { missionSegment: 'short_term_rental' })).result, null);
  assert.equal((await investigate(html, { missionSegment: 'small_business_owners', missionSegments: ['painting'] })).result, null);
  assert.equal((await investigate('<p>We are a painting company.</p>', {
    missionSegment: 'small_business_owners', missionSegments: ['painting'],
  })).result?.admission.vertical, 'painting', 'narrowed SMB subsegments retain website evidence without broadening the audience');
  assert.equal((await investigate(html, { missionSegment: 'unknown_segment' })).result, null);
  assert.equal((await investigate('<p>We are a plumbing contractor.</p>')).result, null, 'Babrun taxonomy is not broadened');
  for (const [scope, business] of [
    [{}, { location: 'Concord, NH' }],
    [{ allowedCities: [], region: 'United States' }, {}],
    [{}, { website: 'https://facebook.com/bluedoor' }],
  ]) {
    const { result, calls } = await investigate(html, scope, business);
    assert.equal(result, null);
    assert.equal(calls.length, 0, 'no new fetch eligibility');
  }
});

test('foreign redirects and failed fetches do not supply evidence or raise existing request cap', async () => {
  for (const response of [
    { ok: true, url: 'https://foreign.example/', text: '<html><p>We are a cleaning company.</p></html>' },
    { ok: false, status: 503, text: '<html><p>We are a cleaning company.</p></html>' },
  ]) {
    let calls = 0;
    const result = await acquireBusinessEvidence(candidate, context, { fetchPage: async () => { calls++; return response; } });
    assert.equal(result, null);
    assert.equal(calls, 6, 'six logical fetch attempts including failures');
  }
});

test('a contradictory later page cannot be discarded after a favorable first page', async () => {
  for (const contradiction of ['<p>We provide software for cleaning companies.</p>',
    '<h1>Software for cleaning businesses</h1>', '<h1>Property management</h1>']) {
    let calls = 0;
    const result = await acquireBusinessEvidence(candidate, context, { fetchPage: async url => ({
      ok: true, url, text: ++calls === 1 ? '<html><h1>Cleaning Services</h1></html>' : `<html>${contradiction}</html>`,
    }) });
    assert.equal(result, null);
    assert.equal(calls, 3);
  }
});

test('oversized and excessive-depth page batches fail closed', async () => {
  for (const html of [
    '<p>We are a cleaning company.</p>' + ' '.repeat(1_000_001),
    '<div>'.repeat(50) + '<p>We are a cleaning company.</p>' + '</div>'.repeat(50),
  ]) assert.equal((await investigate(html)).result, null);
});

test('new business interpretation requires explicit enablement and an allowed tenant', async () => {
  const html = '<p>We provide cleaning services.</p>';
  for (const tenantId of [undefined, null, '', '1', '17', ['13'], true, {}]) {
    assert.equal((await investigate(html, { tenantId })).result, null);
  }
  for (const value of [undefined, 'false', 'TRUE', '1']) {
    if (value == null) delete process.env.SCOUT_DURABLE_CANDIDATES_ENABLED;
    else process.env.SCOUT_DURABLE_CANDIDATES_ENABLED = value;
    assert.equal((await investigate(html)).result, null);
  }
  process.env.SCOUT_DURABLE_CANDIDATES_ENABLED = 'true';
  assert.equal((await investigate(html)).result?.admission.vertical, 'cleaning');
});

test('opt-in outcomes distinguish a completed bounded crawl with facts from one without admissible facts', async () => {
  for (const [html, expected] of [
    ['<html><p>We provide cleaning services.</p></html>', 'evidence'],
    ['<html><h1>Blue Door</h1><p>Contact us in Manchester.</p></html>', 'no_evidence'],
    ['<html><h1>Cleaning Services</h1><h1>Software for cleaning businesses</h1></html>', 'no_evidence'],
  ]) {
    let calls = 0;
    const fetchPage = async url => { calls++; return { ok: true, status: 200, url, text: html }; };
    const result = await acquireBusinessEvidence(candidate, context, { reportOutcome: true, fetchPage });
    assert.equal(result.outcome, expected);
    assert.deepEqual(result.crawl, { attempts: 3, successfulPages: 3, partial: false });
    assert.equal(calls, 3);
    assert.equal(Object.hasOwn(result, 'retryable'), false, 'completed negative evidence must not grant a timed retry');
    if (expected === 'evidence') assert.equal(result.result.admission.vertical, 'cleaning');
    else { assert.equal(result.result, null); assert.equal(result.reason, 'no_supported_business_evidence'); }
    const legacy = await acquireBusinessEvidence(candidate, context, { fetchPage });
    assert.equal(legacy?.admission?.vertical || null, expected === 'evidence' ? 'cleaning' : null);
    assert.equal(legacy?.outcome, undefined, 'legacy callers keep the success object/null contract');
  }
});

test('opt-in ineligible outcomes never acquire pages', async () => {
  for (const [business, mission, reason] of [
    [{ ...candidate, website: 'https://facebook.com/bluedoor' }, context, 'no_official_domain'],
    [candidate, { ...context, allowedCities: [], region: 'United States' }, 'missing_allowed_cities'],
    [{ ...candidate, location: 'Concord, NH' }, context, 'outside_mission_geography'],
  ]) {
    const result = await acquireBusinessEvidence(business, mission, { reportOutcome: true,
      fetchPage: async () => { throw Error('Ineligible candidate must not fetch'); } });
    assert.equal(result.outcome, 'ineligible'); assert.equal(result.reason, reason); assert.equal(result.result, null);
    assert.deepEqual(result.crawl, { attempts: 0, successfulPages: 0, partial: false });
  }
});

test('only allowlisted transient failures request a bounded retry, without leaking exception text', async t => {
  for (const [kind, failure, reason, retryable] of [
    ['abort timeout', Object.assign(Error('private timeout detail'), { name: 'AbortError' }), 'fetch_timeout', true],
    ['axios timeout', Object.assign(Error('private timeout detail'), { code: 'ECONNABORTED' }), 'fetch_timeout', true],
    ['temporary DNS', Object.assign(Error('private network detail'), { code: 'EAI_AGAIN' }), 'transient_network_failure', true],
    ['unknown exception', Error('private unknown detail'), 'unknown_fetch_failure', false],
    ['not found DNS', Object.assign(Error('private network detail'), { code: 'ENOTFOUND' }), 'unknown_fetch_failure', false],
    ['rate limit', 429, 'transient_http_failure', true],
    ['service unavailable', 503, 'transient_http_failure', true],
    ['forbidden', 403, 'http_access_denied', false],
    ['not found', 404, 'http_not_found', false],
    ['gone', 410, 'http_not_found', false],
    ['other client failure', 422, 'http_failure', false],
  ]) await t.test(kind, async () => {
    let calls = 0;
    const result = await acquireBusinessEvidence(candidate, context, { reportOutcome: true, fetchPage: async url => {
      calls++;
      if (failure instanceof Error) throw failure;
      return { ok: false, status: failure, url, text: '<html>private response detail</html>' };
    } });
    assert.equal(result.outcome, 'fetch_failed'); assert.equal(result.result, null);
    assert.equal(result.reason, reason); assert.equal(result.retryable, retryable);
    assert.deepEqual(result.crawl, { attempts: 6, successfulPages: 0, partial: false });
    assert.equal(calls, 6, 'failure reporting never raises the original logical request cap');
    assert.doesNotMatch(JSON.stringify(result), /private|bluedoor|https:/);
  });
});

test('partial positive or contradictory batches stay unknown; permanent failure wins over transient retry', async () => {
  for (const contradictory of [false, true]) {
    let calls = 0;
    const result = await acquireBusinessEvidence(candidate, context, { reportOutcome: true, fetchPage: async url => {
      calls++;
      if (calls === 1) return { ok: true, url, text: '<html><h1>Cleaning Services</h1></html>' };
      if (calls === 2 && contradictory) return { ok: true, url, text: '<html><h1>Software for cleaning businesses</h1></html>' };
      throw Object.assign(Error('private timeout'), { name: 'AbortError' });
    } });
    assert.equal(result.outcome, 'fetch_failed'); assert.equal(result.result, null); assert.equal(result.retryable, true);
    assert.deepEqual(result.crawl, { attempts: 6, successfulPages: contradictory ? 2 : 1, partial: true });
    assert.equal(calls, 6);
  }
  let calls = 0;
  const mixed = await acquireBusinessEvidence(candidate, context, { reportOutcome: true, fetchPage: async url => ({
    ok: false, status: ++calls === 1 ? 503 : 403, url, text: '<html>unavailable</html>',
  }) });
  assert.equal(mixed.outcome, 'fetch_failed'); assert.equal(mixed.retryable, false);
  assert.equal(mixed.reason, 'http_access_denied'); assert.equal(calls, 6);
});

test('offsite or malformed successful responses are held without automatic retry', async () => {
  for (const response of [
    { ok: true, status: 200, url: 'https://foreign.example', text: '<html><h1>Cleaning Services</h1></html>' },
    { ok: true, status: 200, text: '{"business":"cleaning"}' },
  ]) {
    const result = await acquireBusinessEvidence(candidate, context, { reportOutcome: true, fetchPage: async () => response });
    assert.equal(result.outcome, 'fetch_failed'); assert.equal(result.retryable, false); assert.equal(result.result, null);
    assert.equal(result.crawl.attempts, 6);
  }
});

test('structured completeness gating is opt-in and does not change legacy partial-success shape', async () => {
  for (const reportOutcome of [undefined, false, 'true']) {
    let calls = 0;
    const result = await acquireBusinessEvidence(candidate, context, { reportOutcome, fetchPage: async url => {
      if (++calls === 1) return { ok: true, url, text: '<html><h1>Cleaning Services</h1></html>' };
      return { ok: false, status: 503, url, text: '<html>unavailable</html>' };
    } });
    assert.equal(result?.admission.vertical, 'cleaning'); assert.equal(result.outcome, undefined);
    assert.equal(calls, 6);
  }
});
