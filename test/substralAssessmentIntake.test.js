'use strict';

/**
 * Studio Substral assessment intake.
 *
 * The intake's whole job is to accept a request without asserting anything
 * about the domain. These tests pin the admission rules and, more importantly,
 * pin the boundary: an intake row is a request, never a finding.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const {
  ACTION_TYPE,
  CREATED_BY,
  SOURCE,
  DEFAULT_CLIENT_ID,
  normalizeAssessmentDomain,
  validateAssessmentPayload,
  buildAssessmentActionPayload,
  captureAssessmentRequest,
  resolveClientId,
} = require('../lib/substralAssessmentIntake');

const { REASON_MESSAGES } = require('../routes/substralAssessment');
const {
  PROHIBITED_CLAIM_PATTERNS,
} = require('../packages/capabilities/websiteOpportunityIntelligence/types');

/** Minimal pg pool stand-in. */
function fakePool(rows = [{ id: 4242 }]) {
  const calls = [];
  return {
    calls,
    async query(sql, params) {
      calls.push({ sql, params });
      return { rows };
    },
  };
}

describe('domain normalization', () => {
  it('reduces anything paste-shaped to a bare registrable host', () => {
    const cases = {
      'example.com': 'example.com',
      'EXAMPLE.COM': 'example.com',
      '  example.com  ': 'example.com',
      'www.example.com': 'example.com',
      'https://example.com': 'example.com',
      'http://www.example.com/': 'example.com',
      'https://www.example.com/services/?utm_source=x#top': 'example.com',
      'https://example.com:8443/path': 'example.com',
      'example.com.': 'example.com',
      'https://user@example.com/page': 'example.com',
      'shop.example.co.uk': 'shop.example.co.uk',
      'my-business.example': 'my-business.example',
    };
    for (const [input, expected] of Object.entries(cases)) {
      const result = normalizeAssessmentDomain(input);
      assert.equal(result.ok, true, `${input} should be admitted`);
      assert.equal(result.domain, expected, `${input} normalized wrong`);
    }
  });

  it('rejects input that is not a domain', () => {
    const cases = {
      '': 'empty',
      '   ': 'empty',
      example: 'no_tld',
      'example.': 'no_tld',
      'example.c0m': 'no_tld',
      'exa mple': 'no_tld',
      'exam_ple.com': 'malformed',
      '-example.com': 'malformed',
      'example-.com': 'malformed',
    };
    for (const [input, reason] of Object.entries(cases)) {
      const result = normalizeAssessmentDomain(input);
      assert.equal(result.ok, false, `${JSON.stringify(input)} should be rejected`);
      assert.equal(result.reason, reason, `${JSON.stringify(input)} wrong reason`);
    }
  });

  it('rejects hosts that are not reachable from the public internet', () => {
    for (const input of ['localhost', 'http://localhost:3000', 'app.localhost', '127.0.0.1', '192.168.1.10', '[::1]']) {
      const result = normalizeAssessmentDomain(input);
      assert.equal(result.ok, false, `${input} should be rejected`);
      assert.equal(result.reason, 'not_public', `${input} wrong reason`);
    }
  });

  it('rejects search engines, directories and social profiles', () => {
    for (const input of [
      'google.com',
      'https://www.google.com/search?q=cleaners',
      'maps.google.com',
      'bing.com',
      'facebook.com/somebusiness',
      'linkedin.com/company/x',
      'yelp.com/biz/x',
      'findlaw.com',
      'bit.ly/abc',
    ]) {
      const result = normalizeAssessmentDomain(input);
      assert.equal(result.ok, false, `${input} should be rejected`);
      assert.equal(result.reason, 'not_a_subject', `${input} wrong reason`);
    }
  });

  it('rejects the operator’s own properties', () => {
    for (const input of ['studiosubstral.com', 'https://www.gopulseforge.com', 'goanchorcleaning.com']) {
      const result = normalizeAssessmentDomain(input);
      assert.equal(result.ok, false, `${input} should be rejected`);
      assert.equal(result.reason, 'own_domain');
    }
  });

  it('stays in step with the discovery admission rules it borrows', () => {
    const {
      isSearchOrMapsDomain,
      isDirectoryDomain,
    } = require('../packages/capabilities/websiteOpportunityIntelligence/discoveryAdmission');
    for (const domain of ['google.com', 'maps.google.com', 'bing.com']) {
      assert.equal(isSearchOrMapsDomain(domain), true);
    }
    assert.equal(isDirectoryDomain('findlaw.com'), true);
  });
});

describe('payload validation', () => {
  it('accepts a complete request and normalizes the fields', () => {
    const result = validateAssessmentPayload({
      domain: 'HTTPS://WWW.Example.com/contact',
      email: '  Owner@Example.com ',
      context: '  Considering   a rebuild  ',
      referer: 'https://news.example/post',
    });
    assert.equal(result.ok, true);
    assert.deepEqual(result.values, {
      domain: 'example.com',
      email: 'owner@example.com',
      context: 'Considering a rebuild',
      referer: 'https://news.example/post',
    });
  });

  it('treats context and referer as optional', () => {
    const result = validateAssessmentPayload({ domain: 'example.com', email: 'a@b.co' });
    assert.equal(result.ok, true);
    assert.equal(result.values.context, null);
    assert.equal(result.values.referer, null);
  });

  it('requires a deliverable address, because the report is sent not shown', () => {
    for (const email of ['', 'not-an-email', 'a@b', 'a@@b.co', 'a b@c.co']) {
      const result = validateAssessmentPayload({ domain: 'example.com', email });
      assert.equal(result.ok, false, `${JSON.stringify(email)} should be rejected`);
      assert.equal(result.errors.email, 'email');
    }
  });

  it('reports the domain and the email problem together', () => {
    const result = validateAssessmentPayload({ domain: 'nope', email: 'bad' });
    assert.equal(result.ok, false);
    assert.equal(result.errors.domain, 'no_tld');
    assert.equal(result.errors.email, 'email');
  });

  it('truncates long free text rather than rejecting the request', () => {
    const result = validateAssessmentPayload({
      domain: 'example.com',
      email: 'a@b.co',
      context: 'x'.repeat(900),
    });
    assert.equal(result.ok, true);
    assert.equal(result.values.context.length, 300);
  });

  it('has operator-facing copy for every rejection reason it can emit', () => {
    const reasons = [
      'empty',
      'no_tld',
      'malformed',
      'not_public',
      'not_a_subject',
      'own_domain',
      'email',
    ];
    for (const reason of reasons) {
      assert.ok(REASON_MESSAGES[reason], `no message for reason: ${reason}`);
    }
  });
});

describe('capture', () => {
  it('writes one pending agent_actions row for the operator', async () => {
    const pool = fakePool();
    const result = await captureAssessmentRequest(pool, {
      domain: 'example.com',
      email: 'owner@example.com',
      context: 'Traffic changed',
      referer: null,
    });

    assert.equal(pool.calls.length, 1);
    const { sql, params } = pool.calls[0];
    assert.match(sql, /INSERT INTO agent_actions/);
    assert.match(sql, /'pending'/);
    assert.equal(params[0], CREATED_BY);
    assert.equal(params[1], ACTION_TYPE);
    assert.equal(params[2], 'Assessment request — example.com');
    assert.equal(params[5], DEFAULT_CLIENT_ID);

    assert.deepEqual(result, {
      id: 4242,
      stored: true,
      domain: 'example.com',
      client_id: DEFAULT_CLIENT_ID,
    });
  });

  it('records the request as requested, never as assessed', () => {
    const payload = buildAssessmentActionPayload({
      domain: 'example.com',
      email: 'owner@example.com',
      context: null,
      referer: null,
    });
    assert.equal(payload.stage, 'requested');
    assert.equal(payload.source, SOURCE);
    assert.equal(payload.brand, 'Studio Substral');
    assert.equal(payload.subject_domain, 'example.com');

    // No evidence, no score, no diagnosis may ride along on an intake row.
    const keys = Object.keys(payload).join(' ');
    for (const forbidden of [
      'score',
      'opportunity',
      'diagnosis',
      'findings',
      'evidence',
      'measured',
      'recommendation',
    ]) {
      assert.ok(!keys.includes(forbidden), `intake payload must not carry ${forbidden}`);
    }
  });

  it('never emits language the assessment engine prohibits', () => {
    const { successMessage } = require('../routes/substralAssessment');
    const text = [
      successMessage('example.com', 'owner@example.com'),
      ...Object.values(REASON_MESSAGES),
    ].join(' ');
    for (const pattern of PROHIBITED_CLAIM_PATTERNS) {
      assert.doesNotMatch(text, pattern, `intake copy trips ${pattern}`);
    }
    assert.doesNotMatch(text, /\b\d{1,3}\s*\/\s*100\b/);
  });

  it('confirms only that the request was queued', () => {
    const { successMessage } = require('../routes/substralAssessment');
    const message = successMessage('example.com', 'owner@example.com');
    assert.match(message, /example\.com is queued/);
    assert.match(message, /a person reviews the findings/);
    assert.match(message, /the report will say so/);
    assert.doesNotMatch(message, /\b(slow|broken|failing|poor|bad)\b/i);
  });
});

describe('tenant scoping', () => {
  const original = process.env.STUDIO_SUBSTRAL_CLIENT_ID;

  beforeEach(() => {
    if (original == null) delete process.env.STUDIO_SUBSTRAL_CLIENT_ID;
    else process.env.STUDIO_SUBSTRAL_CLIENT_ID = original;
  });

  it('defaults to the Pulseforge operator queue', () => {
    delete process.env.STUDIO_SUBSTRAL_CLIENT_ID;
    assert.equal(resolveClientId(), DEFAULT_CLIENT_ID);
  });

  it('honours an explicit tenant when one is configured', () => {
    process.env.STUDIO_SUBSTRAL_CLIENT_ID = '7';
    assert.equal(resolveClientId(), 7);
  });

  it('ignores an unusable value rather than writing a broken row', () => {
    for (const value of ['0', '-3', 'abc', '']) {
      process.env.STUDIO_SUBSTRAL_CLIENT_ID = value;
      assert.equal(resolveClientId(), DEFAULT_CLIENT_ID, `value ${value}`);
    }
  });
});
