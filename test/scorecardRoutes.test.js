const { describe, it, before, after } = require('node:test');
const assert = require('node:assert/strict');
const express = require('express');
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const pool = require('../db');
const { captureScorecardLead, SCORECARD_CLIENT_ID, ACTION_TYPE } = require('../lib/scorecardCapture');
const { resolveResult } = require('../lib/scorecardScoring');

const ROOT = path.join(__dirname, '..');

function basePayload(overrides = {}) {
  return {
    business_type: 'residential',
    monthly_inquiries: '11-30',
    after_hours_process: 'owner_answers',
    missed_call_text: 'yes',
    quote_follow_up_speed: 'same_day',
    quote_follow_up_count: '2',
    automatic_review_request: 'yes',
    current_system: 'spreadsheet',
    typical_job_value: '250-500',
    name: 'Alex Owner',
    business_name: 'Sparkle Clean Co',
    email: 'alex@example.com',
    mobile: '6035550100',
    marketing_consent: false,
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

describe('scorecard capture payload', () => {
  it('writes complete answers, result_category, and high_intent to agent_actions', async () => {
    const original = pool.query;
    let insert = null;
    pool.query = async (sql, params) => {
      insert = { sql, params };
      return { rows: [{ id: 901 }] };
    };
    try {
      const answers = {
        ...basePayload({
          monthly_inquiries: '31-75',
          missed_call_text: 'no',
          marketing_consent: true,
        }),
      };
      delete answers.company_website;
      const result = resolveResult(answers);
      const stored = await captureScorecardLead(answers, result);

      assert.equal(stored.id, 901);
      assert.equal(stored.client_id, SCORECARD_CLIENT_ID);
      assert.match(insert.sql, /INSERT INTO agent_actions/i);
      assert.equal(insert.params[0], 'scorecard');
      assert.equal(insert.params[1], ACTION_TYPE);
      assert.equal(insert.params[5], 1);

      const payload = JSON.parse(insert.params[4]);
      assert.equal(payload.result_category, 'call_recovery_gap');
      assert.equal(payload.high_intent, true);
      assert.equal(payload.marketing_consent, true);
      assert.equal(payload.contact.email, 'alex@example.com');
      assert.equal(payload.answers.monthly_inquiries, '31-75');
      assert.equal(payload.answers.missed_call_text, 'no');
      assert.equal(payload.answers.current_system, 'spreadsheet');
      assert.equal(payload.answers.typical_job_value, '250-500');
      assert.ok(payload.answers.business_type);
      assert.ok(payload.answers.after_hours_process);
      assert.ok(payload.answers.quote_follow_up_speed);
      assert.ok(payload.answers.quote_follow_up_count);
      assert.ok(payload.answers.automatic_review_request);
    } finally {
      pool.query = original;
    }
  });
});

describe('scorecard public routes', () => {
  let harness;
  let originalQuery;

  before(async () => {
    originalQuery = pool.query;
    pool.query = async (sql, params) => {
      if (/INSERT INTO agent_actions/i.test(sql)) {
        return { rows: [{ id: 4242 }], params };
      }
      throw new Error(`Unexpected query in scorecard route test: ${sql}`);
    };

    const app = express();
    app.use(express.json());
    app.use('/shared', express.static(path.join(ROOT, 'public', 'shared')));
    app.use('/', require('../routes/scorecard'));
    harness = await listen(app);
  });

  after(async () => {
    pool.query = originalQuery;
    if (harness) await harness.close();
  });

  it('serves landing, form, results, and static assets', async () => {
    const landing = await request(harness.base, 'GET', '/scorecard');
    assert.equal(landing.status, 200);
    assert.match(landing.text, /Find the jobs your cleaning business is quietly losing/);
    assert.match(landing.text, /Get my score/);
    assert.match(landing.text, /viewport/);

    const form = await request(harness.base, 'GET', '/scorecard/form');
    assert.equal(form.status, 200);
    assert.match(form.text, /sc-progress/);
    assert.match(form.text, /name="marketing_consent"/);
    assert.doesNotMatch(form.text, /id="marketing_consent"[^>]*checked/);

    const results = await request(harness.base, 'GET', '/scorecard/results');
    assert.equal(results.status, 200);
    assert.match(results.text, /calendly\.com\/jacob-gopulseforge\/pulsforge-revenue-recovery-assessment/);
    assert.match(results.text, /Book a 20-minute Revenue Recovery Assessment/);
    assert.match(results.text, /Follow-Up Recovery Kit — \$29/);
    assert.match(results.text, /What this means for your business/);
    assert.match(results.text, /Want this running automatically/);

    const css = await request(harness.base, 'GET', '/scorecard/scorecard.css');
    assert.equal(css.status, 200);
    assert.match(css.text, /--pf-touch-target/);

    const js = await request(harness.base, 'GET', '/scorecard/scorecard.js');
    assert.equal(js.status, 200);
    assert.match(js.text, /\/api\/public\/scorecard/);
  });

  it('POST captures and returns public result fields without gaps or internals', async () => {
    const res = await request(harness.base, 'POST', '/api/public/scorecard', basePayload({
      missed_call_text: 'no',
      monthly_inquiries: '0-10',
    }));
    assert.equal(res.status, 201);
    assert.equal(res.json.ok, true);
    assert.equal(res.json.submission_id, 4242);
    assert.equal(res.json.result.category, 'call_recovery_gap');
    assert.equal(res.json.result.high_intent, false);
    assert.equal(res.json.result.primary_cta, 'kit');
    assert.equal(
      res.json.ctas.assessment_url,
      'https://calendly.com/jacob-gopulseforge/pulsforge-revenue-recovery-assessment'
    );
    assert.ok(res.json.result.payoff);
    assert.match(res.json.result.payoff.meaning, /live buying opportunity/i);
    assert.match(res.json.result.payoff.first_move, /missed-call text-back/i);
    assert.match(res.json.result.payoff.job_value_illustration, /\$250–\$500/);
    assert.equal(res.json.result.payoff.job_value_key, '250-500');
    assert.deepEqual(res.json.result.payoff.recovery_plan, [
      'Respond quickly',
      'Follow up consistently',
      'Track the outcome',
    ]);
    assert.equal(res.json.result.gaps, undefined);
    assert.equal(res.json.stack, undefined);
    assert.equal(res.json.message, undefined);
  });

  it('routes quote and review categories and high-intent override via API', async () => {
    const quote = await request(harness.base, 'POST', '/api/public/scorecard', basePayload({
      quote_follow_up_count: '0',
      automatic_review_request: 'no',
    }));
    assert.equal(quote.json.result.category, 'quote_follow_up_gap');

    const review = await request(harness.base, 'POST', '/api/public/scorecard', basePayload({
      automatic_review_request: 'no',
    }));
    assert.equal(review.json.result.category, 'review_growth_gap');

    const highInquiries = await request(harness.base, 'POST', '/api/public/scorecard', basePayload({
      monthly_inquiries: '76+',
      automatic_review_request: 'no',
    }));
    assert.equal(highInquiries.json.result.high_intent, true);
    assert.equal(highInquiries.json.result.primary_cta, 'assessment');

    const highSystem = await request(harness.base, 'POST', '/api/public/scorecard', basePayload({
      current_system: 'jobber_servicem8',
      automatic_review_request: 'no',
    }));
    assert.equal(highSystem.json.result.high_intent, true);
  });

  it('honeypot returns 204 without writing', async () => {
    let wrote = false;
    const prev = pool.query;
    pool.query = async (...args) => {
      wrote = true;
      return prev(...args);
    };
    try {
      const res = await request(harness.base, 'POST', '/api/public/scorecard', basePayload({
        company_website: ' https://spam.example ',
      }));
      assert.equal(res.status, 204);
      assert.equal(res.text, '');
      assert.equal(wrote, false);
    } finally {
      pool.query = prev;
    }
  });

  it('rejects invalid payloads with field errors only', async () => {
    const res = await request(harness.base, 'POST', '/api/public/scorecard', {
      email: 'nope',
    });
    assert.equal(res.status, 400);
    assert.equal(res.json.error, 'Validation failed');
    assert.ok(Array.isArray(res.json.details));
    assert.equal(res.json.stack, undefined);
    assert.doesNotMatch(JSON.stringify(res.json), /agent_actions|postgres|DATABASE/i);
  });

  it('hides internal errors from visitors on capture failure', async () => {
    const prev = pool.query;
    pool.query = async () => {
      throw new Error('relation "agent_actions" does not exist — password=supersecret');
    };
    try {
      const res = await request(harness.base, 'POST', '/api/public/scorecard', basePayload({
        missed_call_text: 'no',
      }));
      assert.equal(res.status, 500);
      assert.equal(res.json.error, 'Could not save your scorecard. Please try again.');
      assert.doesNotMatch(JSON.stringify(res.json), /agent_actions|supersecret|relation/i);
    } finally {
      pool.query = prev;
    }
  });
});

describe('scorecard production-style mount order', () => {
  it('still serves /scorecard when root static is mounted before the router', async () => {
    const original = pool.query;
    pool.query = async () => ({ rows: [{ id: 1 }] });
    const app = express();
    app.use(express.json());
    app.use('/shared', express.static(path.join(ROOT, 'public', 'shared')));
    // Mirrors server.js: static root before route mounts
    app.use(express.static(ROOT));
    app.use('/', require('../routes/scorecard'));
    const h = await listen(app);
    try {
      const landing = await request(h.base, 'GET', '/scorecard');
      assert.equal(landing.status, 200);
      assert.match(landing.text, /Get my score/);

      const tokens = await request(h.base, 'GET', '/shared/tokens.css');
      assert.equal(tokens.status, 200);

      const post = await request(h.base, 'POST', '/api/public/scorecard', basePayload({
        current_system: 'crm',
        automatic_review_request: 'no',
      }));
      assert.equal(post.status, 201);
      assert.equal(post.json.result.high_intent, true);
      assert.equal(post.json.result.category, 'review_growth_gap');
    } finally {
      pool.query = original;
      await h.close();
    }
  });
});

describe('scorecard UI wiring', () => {
  it('keeps Calendly, viewport, and progress affordances in the public HTML/CSS', () => {
    const formHtml = fs.readFileSync(path.join(ROOT, 'public/scorecard/form.html'), 'utf8');
    const resultsHtml = fs.readFileSync(path.join(ROOT, 'public/scorecard/results.html'), 'utf8');
    const css = fs.readFileSync(path.join(ROOT, 'public/scorecard/scorecard.css'), 'utf8');
    const tokens = fs.readFileSync(path.join(ROOT, 'public/shared/tokens.css'), 'utf8');
    const server = fs.readFileSync(path.join(ROOT, 'server.js'), 'utf8');

    assert.match(formHtml, /name="viewport"/);
    assert.match(formHtml, /id="sc-progress-fill"/);
    assert.match(formHtml, /id="marketing_consent"/);
    assert.doesNotMatch(formHtml, /id="marketing_consent"[^>]*checked/);

    assert.match(resultsHtml, /calendly\.com\/jacob-gopulseforge\/pulsforge-revenue-recovery-assessment/);
    assert.match(resultsHtml, /sc-cta-high/);
    assert.match(resultsHtml, /sc-cta-standard/);
    assert.match(resultsHtml, /What this means for your business/);
    assert.match(resultsHtml, /Your first move today/);
    assert.match(resultsHtml, /What one recovered job could be worth/);
    assert.match(resultsHtml, /Your 3-step recovery plan/);
    assert.match(resultsHtml, /Want this running automatically instead of relying on someone to remember the next step\?/);

    assert.match(tokens, /--pf-touch-target:\s*44px/);
    assert.match(css, /min-height:\s*var\(--pf-touch-target\)/);
    assert.match(css, /@media \(min-width: 640px\)/);

    assert.match(server, /require\('\.\/routes\/scorecard'\)/);
  });
});
