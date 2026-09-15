const { describe, it, before, after } = require('node:test');
const assert = require('node:assert/strict');
const express = require('express');
const fs = require('node:fs');
const path = require('node:path');
const pool = require('../db');
const { validateWalkthroughPayload, SPACE_TYPES } = require('../lib/walkthroughValidate');
const { captureWalkthroughLead, ANCHOR_CLIENT_ID, ACTION_TYPE } = require('../lib/walkthroughCapture');
const { SOURCE_KIND } = require('../lib/walkthroughAttribution');
const walkthroughRouter = require('../routes/walkthrough');
const { createWalkthroughCaptureMockPool } = require('./helpers/walkthroughCaptureMockPool');

const SITE = path.join(__dirname, '..', 'sites', 'anchor-cleaning', 'index.html');

function basePayload(overrides = {}) {
  return {
    name: 'Alex Owner',
    business_name: 'Riverside Law',
    phone: '(603) 555-0142',
    email: 'alex@riverside.example',
    city: 'Manchester',
    space_type: 'law_office',
    company_website: '',
    ...overrides,
  };
}

function listen(app) {
  const server = app.listen(0, '127.0.0.1');
  return new Promise((resolve) => {
    server.on('listening', () => {
      const { port } = server.address();
      resolve({
        server,
        base: `http://127.0.0.1:${port}`,
        async close() {
          await new Promise((r) => server.close(r));
        },
      });
    });
  });
}

async function request(base, method, urlPath, body) {
  const url = new URL(urlPath, base);
  const res = await fetch(url, {
    method,
    headers: body == null ? undefined : { 'Content-Type': 'application/json', Accept: 'application/json' },
    body: body == null ? undefined : JSON.stringify(body),
  });
  const text = await res.text();
  let json = null;
  try {
    json = text ? JSON.parse(text) : null;
  } catch (_) {
    json = null;
  }
  return { status: res.status, headers: res.headers, text, json };
}

describe('walkthrough validation', () => {
  it('accepts a complete commercial-office request', () => {
    const result = validateWalkthroughPayload(basePayload());
    assert.equal(result.ok, true);
    assert.equal(result.values.space_type_label, 'Law office');
    assert.equal(result.values.email, 'alex@riverside.example');
    assert.equal(result.values.phone_digits, '6035550142');
  });

  it('rejects missing and invalid fields', () => {
    const result = validateWalkthroughPayload({
      name: 'A',
      email: 'not-an-email',
      phone: '123',
      space_type: 'warehouse',
    });
    assert.equal(result.ok, false);
    assert.ok(result.errors.name);
    assert.ok(result.errors.business_name);
    assert.ok(result.errors.email);
    assert.ok(result.errors.phone);
    assert.ok(result.errors.city);
    assert.ok(result.errors.space_type);
  });

  it('covers the advertised space-type options', () => {
    assert.deepEqual(SPACE_TYPES, [
      'law_office',
      'accounting',
      'medical_office',
      'general_office',
      'retail',
      'other',
    ]);
  });
});

describe('walkthrough capture', () => {
  it('writes a pending agent_actions row for Anchor', async () => {
    const original = pool.query;
    const mock = createWalkthroughCaptureMockPool({ nextActionId: 77 });
    pool.query = mock.query.bind(mock);
    try {
      const validated = validateWalkthroughPayload(basePayload());
      const stored = await captureWalkthroughLead(validated.values);
      assert.equal(stored.id, 77);
      assert.equal(stored.client_id, ANCHOR_CLIENT_ID);
      assert.ok(stored.prospect_id);
      const insert = mock.state.agentActions[0];
      assert.equal(insert.params[0], 'website');
      assert.equal(insert.params[1], ACTION_TYPE);
      assert.equal(insert.params[2], 'Facilities assessment request — Riverside Law');
      assert.equal(insert.params[5], 10);
      const payload = insert.payload;
      assert.equal(payload.source, 'website_walkthrough');
      assert.equal(payload.contact.business_name, 'Riverside Law');
      assert.equal(payload.contact.space_type, 'law_office');
      assert.equal(payload.prospect_id, stored.prospect_id);
    } finally {
      pool.query = original;
    }
  });
});

describe('walkthrough public route', () => {
  let harness;
  let originalQuery;

  before(async () => {
    originalQuery = pool.query;
    const mock = createWalkthroughCaptureMockPool({ nextActionId: 8801 });
    pool.query = mock.query.bind(mock);
    const app = express();
    app.use(express.json());
    app.use('/', walkthroughRouter);
    harness = await listen(app);
    walkthroughRouter._rateBuckets.clear();
  });

  after(async () => {
    pool.query = originalQuery;
    if (harness) await harness.close();
  });

  it('creates a walkthrough request', async () => {
    const res = await request(harness.base, 'POST', '/api/public/walkthrough', basePayload());
    assert.equal(res.status, 201);
    assert.equal(res.json.ok, true);
    assert.equal(res.json.submission_id, 8801);
    assert.match(res.json.message, /facilities assessment/i);
  });

  it('returns field errors without internals', async () => {
    const res = await request(harness.base, 'POST', '/api/public/walkthrough', { name: 'Alex' });
    assert.equal(res.status, 400);
    assert.equal(res.json.error, 'Validation failed');
    assert.ok(res.json.details.email);
    assert.equal(res.text.includes('pool'), false);
  });

  it('swallows honeypot submissions', async () => {
    const res = await request(harness.base, 'POST', '/api/public/walkthrough', basePayload({
      company_website: 'https://spam.example',
    }));
    assert.equal(res.status, 204);
    assert.equal(res.json, null);
  });

  it('accepts optional first-party attribution on walkthrough POST', async () => {
    const mock = createWalkthroughCaptureMockPool({ nextActionId: 8802 });
    pool.query = mock.query.bind(mock);
    const res = await request(harness.base, 'POST', '/api/public/walkthrough', basePayload({
      attribution: {
        oppref: 'paid-token',
        landing_page_url: 'https://goanchorcleaning.com/?oppref=paid-token',
        referrer: 'https://chatgpt.com/',
        evil: 'ignored',
      },
    }));
    assert.equal(res.status, 201);
    assert.equal(res.json.submission_id, 8802);
    const insertPayload = mock.state.agentActions[0].payload;
    assert.equal(insertPayload.source, 'website_walkthrough');
    assert.equal(insertPayload.attribution.raw.oppref, 'paid-token');
    assert.equal(insertPayload.attribution.normalized.lead_source, 'chatgpt_ads');
    assert.equal(insertPayload.attribution.provenance.sourceKind, SOURCE_KIND);
    assert.notEqual(insertPayload.attribution.provenance.sourceKind, 'PLATFORM_API');
    assert.equal(insertPayload.attribution.raw.evil, undefined);
    assert.ok(insertPayload.prospect_id);
  });
});

describe('Anchor homepage ads contract', () => {
  const html = fs.readFileSync(SITE, 'utf8');

  it('uses the Search-ready title, description, and headline', () => {
    assert.match(html, /<title>Commercial Office Cleaning in Manchester, NH \| Anchor Cleaning<\/title>/);
    assert.match(html, /content="Anchor Cleaning provides recurring commercial office cleaning and janitorial service for professional offices in Greater Manchester, NH\. Request a facilities assessment\."/);
    assert.match(html, /<h1[^>]*>A standing service for offices that/);
    assert.match(html, /Commercial office cleaning in Greater Manchester, NH/);
    assert.match(html, /Commercial office cleaning and janitorial service for professional offices in Greater Manchester, NH\./);
  });

  it('exposes clickable phone and email contact paths', () => {
    assert.match(html, /tel:\+16034202430/);
    assert.match(html, /Call\/Text: \(603\) 420-2430/);
    assert.match(html, /mailto:jacob@goanchorcleaning\.com/);
    assert.match(html, /Greater Manchester, New Hampshire .*commercial office cleaning &amp; janitorial service/);
  });

  it('includes the facilities assessment form, trust line, and conversion events', () => {
    assert.match(html, /Schedule a facilities assessment\. Ten minutes\./);
    assert.match(html, /Tell me a little about your facility and we'll set a quick assessment\./);
    assert.match(html, /Request a facilities assessment/);
    assert.doesNotMatch(html, />\s*Request a walkthrough\s*</);
    assert.doesNotMatch(html, /Walk me through your space/);
    assert.doesNotMatch(html, /quick walkthrough/i);
    assert.match(html, /name="name"/);
    assert.match(html, /name="business_name"/);
    assert.match(html, /name="phone"/);
    assert.match(html, /name="email"/);
    assert.match(html, /name="city"/);
    assert.match(html, /name="space_type"/);
    assert.match(html, />Law office</);
    assert.match(html, />Accounting \/ professional office</);
    assert.match(html, /Insured service/);
    assert.match(html, /Recurring office cleaning/);
    assert.match(html, /Clear monthly quotes/);
    assert.match(html, /Same standard every visit/);
    assert.match(html, /walkthrough_form_submit/);
    assert.match(html, /phone_click/);
    assert.match(html, /email_click/);
    assert.match(html, /if\(w\.oaiq\)return/);
    assert.match(html, /QtVasj1GCLfpLWsTwpYTBC/);
    assert.match(html, /trackOpenAiLeadCreated/);
    assert.match(html, /lead_created/);
    assert.match(html, /json\.submission_id/);
    assert.match(html, /\/api\/public\/walkthrough/);
    assert.match(html, /Thanks\. I'll reach out to set up a quick facilities assessment and give you a clear monthly quote\./);
  });
});
