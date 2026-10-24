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
const { normalizeSubmissionId } = require('../lib/walkthroughSubmissionId');
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

function routePayload(overrides = {}) {
  return basePayload({ email: 'alex@office-mail.com', phone: '6034202430', ...overrides });
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

describe('walkthrough submission_id normalization', () => {
  it('coerces string database ids to numbers', () => {
    assert.equal(normalizeSubmissionId({ id: '8802' }), 8802);
    assert.equal(normalizeSubmissionId({ submission_id: '9910' }), 9910);
  });

  it('accepts UUID agent_actions ids from production schema', () => {
    const uuid = '67042048-c4b6-4b84-a671-f892c49a3eff';
    assert.equal(normalizeSubmissionId({ id: uuid }), uuid);
    assert.equal(typeof normalizeSubmissionId({ id: uuid }), 'string');
  });

  it('rejects malformed stored ids', () => {
    assert.throws(() => normalizeSubmissionId({ id: '8801-2' }), /walkthrough_submission_id_invalid/);
    assert.throws(() => normalizeSubmissionId({ id: '' }), /walkthrough_submission_id_invalid/);
  });
});

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

  it('covers the advertised commercial space-type options', () => {
    for (const spaceType of [
      'law_office',
      'accounting',
      'medical_office',
      'general_office',
      'retail',
      'other',
    ]) {
      assert.ok(SPACE_TYPES.includes(spaceType));
    }
  });

  it('accepts residential home cleaning space types from the residential quote form', () => {
    const result = validateWalkthroughPayload({
      name: 'Jake',
      business_name: 'Residential home',
      phone: '6032935816',
      email: 'homeowner@example.com',
      city: 'Manchester',
      space_type: 'residential_monthly',
    });
    assert.equal(result.ok, true);
    assert.equal(result.values.space_type_label, 'Monthly recurring cleaning');
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
      assert.equal(insert.params[2], 'Facility Assessment request — Riverside Law');
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
    const res = await request(harness.base, 'POST', '/api/public/walkthrough', routePayload());
    assert.equal(res.status, 201);
    assert.equal(res.json.ok, true);
    assert.equal(res.json.submission_id, 8801);
    assert.equal(typeof res.json.submission_id, 'number');
    assert.match(res.json.message, /Facility Assessment/i);
  });

  it('creates a residential home cleaning request', async () => {
    const res = await request(harness.base, 'POST', '/api/public/walkthrough', {
      name: 'Jake',
      business_name: 'Residential home',
      phone: '6032935816',
      email: 'jake.residential@office-mail.com',
      city: 'Manchester',
      space_type: 'residential_monthly',
      company_website: '',
    });
    assert.equal(res.status, 201);
    assert.equal(res.json.ok, true);
    assert.equal(typeof res.json.submission_id, 'number');
    assert.equal(res.json.submission_id, 8802);
    assert.match(res.json.message, /home cleaning quote/i);
    assert.doesNotMatch(res.json.message, /Facility Assessment/i);
  });

  it('returns 201 when agent_actions ids are UUIDs (production schema)', async () => {
    const uuid = 'fab48ecd-bc29-4612-a220-8aec7069236b';
    const mock = createWalkthroughCaptureMockPool({ nextActionId: uuid });
    pool.query = mock.query.bind(mock);
    walkthroughRouter._rateBuckets.clear();
    const res = await request(harness.base, 'POST', '/api/public/walkthrough', routePayload({
      email: `uuid-route-${Date.now()}@office-mail.com`,
    }));
    assert.equal(res.status, 201);
    assert.equal(res.json.ok, true);
    assert.equal(res.json.submission_id, uuid);
    assert.equal(typeof res.json.submission_id, 'string');
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
    const res = await request(harness.base, 'POST', '/api/public/walkthrough', routePayload({
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

  it('rejects synthetic and marked demo submissions before any CRM write', async () => {
    const query = pool.query;
    pool.query = async () => { throw new Error('Synthetic intake must not write to the CRM'); };
    try {
      for (const body of [basePayload(), routePayload({ is_demo: true }), routePayload({ submission_mode: 'test' })]) {
        walkthroughRouter._rateBuckets.clear();
        const res = await request(harness.base, 'POST', '/api/public/walkthrough', body);
        assert.equal(res.status, 422);
        assert.equal(res.json.submission_id, undefined);
      }
    } finally {
      pool.query = query;
      walkthroughRouter._rateBuckets.clear();
    }
  });
});

describe('Anchor homepage ads contract', () => {
  const html = fs.readFileSync(SITE, 'utf8');

  it('uses the Search-ready title, description, and headline', () => {
    assert.match(html, /<title>Commercial Cleaning in Manchester, NH \| Anchor Cleaning<\/title>/);
    assert.match(html, /content="Premium recurring commercial cleaning for offices, professional facilities and property managers throughout Greater Manchester, NH\. Request a facility assessment with Anchor Cleaning\."/);
    assert.match(html, /<h1[^>]*>Commercial cleaning in Manchester, NH\./);
    assert.match(html, /Recurring commercial cleaning, office cleaning, and janitorial service/);
  });

  it('exposes clickable phone and email contact paths', () => {
    assert.match(html, /tel:\+16034202430/);
    assert.match(html, /Call\/Text: \(603\) 420-2430/);
    assert.match(html, /mailto:jacob@goanchorcleaning\.com/);
    assert.match(html, /Greater Manchester, New Hampshire .*commercial office cleaning &amp; janitorial service/);
  });

  it('includes the Facility Assessment form, trust line, and conversion events', () => {
    assert.match(html, /Request Your Facility Assessment/);
    assert.match(html, /arrange a facility assessment to understand your space/);
    assert.match(html, /Request a Facility Assessment/);
    assert.match(html, /Request Facility Assessment/);
    assert.doesNotMatch(html, />\s*Request a walkthrough\s*</i);
    assert.doesNotMatch(html, /Walk me through your space/);
    assert.doesNotMatch(html, /quick walkthrough/i);
    assert.doesNotMatch(html, /facilities assessment/i);
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
    assert.match(html, /Documented service standards/);
    assert.match(html, /Same standard every visit/);
    assert.match(html, /id="assessment-form"/);
    assert.match(html, /social-preview-v20260916\.jpg\?v=20260916/);
    assert.match(html, /anchor-logo-canonical\.png\?v=20260916/);
    assert.match(html, /"logo": "https:\/\/goanchorcleaning.com\/assets\/brand\/anchor-logo-canonical.png\?v=20260916"/);
    assert.match(html, /walkthrough_form_submit/);
    assert.match(html, /phone_click/);
    assert.match(html, /email_click/);
    assert.match(html, /https:\/\/www\.clarity\.ms\/tag\/"\+i/);
    assert.match(html, /"yhnafqbr5k"/);
    assert.equal((html.match(/yhnafqbr5k/g) || []).length, 1);
    assert.match(html, /if\(w\.oaiq\)return/);
    assert.match(html, /QtVasj1GCLfpLWsTwpYTBC/);
    assert.match(html, /bzrcdn\.openai\.com\/sdk\/oaiq\.min\.js/);
    assert.match(html, /trackOpenAiLeadCreated/);
    assert.match(html, /lead_created/);
    assert.match(html, /json\.submission_id/);
    assert.match(html, /\/api\/public\/walkthrough/);
    assert.match(html, /isWalkthroughAccepted/);
    assert.match(html, /window\.location\.replace\(THANK_YOU\)/);
    assert.doesNotMatch(html, /statusEl\.className = 'form-status ok'/);
  });
});
