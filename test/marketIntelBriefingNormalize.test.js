'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  aggregateNormalizedCtas,
  normalizeBriefingCta,
  sanitizeTimelineForBriefing,
  sanitizeTimelineCta,
} = require('../utils/marketIntelBriefingNormalize');
const {
  getRecentMessagingChanges,
  getTopCtas,
} = require('../services/marketIntelligenceBriefing');

describe('marketIntelBriefingNormalize', () => {
  it('excludes image, social, tracking, unsubscribe, privacy, and footer URLs', () => {
    const cases = [
      ['https://cdn.example.com/logo.png', 'image_url'],
      ['https://example.com/pixel.gif?uid=1', 'image_url'],
      ['https://linkedin.com/company/acme', 'social_profile_link'],
      ['https://www.facebook.com/acme', 'social_profile_link'],
      ['https://x.com/acme', 'social_profile_link'],
      ['https://instagram.com/acme', 'social_profile_link'],
      ['https://click.example.com/trk/open?id=1', 'tracking_pixel_or_click'],
      ['https://example.com/wf/click?upn=abc', 'tracking_pixel_or_click'],
      ['https://example.com/unsubscribe?e=1', 'unsubscribe_or_preferences'],
      ['https://example.com/manage-preferences', 'unsubscribe_or_preferences'],
      ['https://example.com/privacy-policy', 'privacy_or_legal'],
      ['https://example.com/terms-of-service', 'privacy_or_legal'],
      ['https://example.com/about-us', 'footer_or_navigation'],
      ['https://example.com/blog/post', 'footer_or_navigation'],
      ['https://example.com/careers', 'footer_or_navigation'],
    ];

    for (const [url, reason] of cases) {
      const result = normalizeBriefingCta(url);
      assert.equal(result.included, false, url);
      assert.equal(result.excludedReason, reason, url);
      assert.equal(result.ctaQuality, 'excluded', url);
    }
  });

  it('normalizes useful CTA paths into labels', () => {
    assert.equal(normalizeBriefingCta('https://acme.com/signup').label, 'sign up');
    assert.equal(normalizeBriefingCta('https://acme.com/quote').label, 'get quote');
    assert.equal(normalizeBriefingCta('https://acme.com/demo').label, 'book demo');
    assert.equal(normalizeBriefingCta('https://acme.com/free-trial').label, 'start free trial');
    assert.equal(normalizeBriefingCta('https://acme.com/get-started').label, 'get started');
    assert.equal(normalizeBriefingCta('book a demo').label, 'book a demo');
    assert.equal(normalizeBriefingCta('book a demo').ctaQuality, 'high');
  });

  it('prefers button/link text over href when available', () => {
    const result = normalizeBriefingCta('https://acme.com/r/abc123', {
      valueJson: { buttonText: 'Book a demo' },
    });
    assert.equal(result.included, true);
    assert.equal(result.label, 'book a demo');
    assert.equal(result.source, 'anchor_text');
    assert.equal(result.ctaQuality, 'high');
  });

  it('keeps excluded social URLs out of aggregated top CTAs', () => {
    const { items, excluded } = aggregateNormalizedCtas(
      [
        { value_text: 'https://linkedin.com/company/acme', company_name: 'Acme', id: 'o1' },
        { value_text: 'https://cdn.acme.com/banner.png', company_name: 'Acme', id: 'o2' },
        { value_text: 'https://acme.com/unsubscribe', company_name: 'Acme', id: 'o3' },
        { value_text: 'https://acme.com/signup', company_name: 'Acme', id: 'o4' },
        { value_text: 'book a demo', company_name: 'Apollo', id: 'o5' },
      ],
      { limit: 10 }
    );

    const labels = items.map((i) => i.cta);
    assert.deepEqual(labels.sort(), ['book a demo', 'sign up']);
    assert.equal(labels.some((l) => /linkedin|png|unsubscribe/i.test(l)), false);
    assert.ok(excluded.length >= 3);
    assert.ok(excluded.every((e) => e.excludedReason));
  });

  it('sanitizeTimelineForBriefing nulls excluded CTAs so changes ignore them', () => {
    const timeline = sanitizeTimelineForBriefing([
      { id: 'e1', touch: 1, cta: 'https://linkedin.com/company/acme', offer: null },
      { id: 'e2', touch: 2, cta: 'https://cdn.example.com/pixel.gif', offer: null },
      { id: 'e3', touch: 3, cta: 'book a demo', offer: null },
    ]);

    assert.equal(timeline[0].cta, null);
    assert.equal(timeline[0]._ctaMeta.excludedReason, 'social_profile_link');
    assert.equal(timeline[1].cta, null);
    assert.equal(timeline[1]._ctaMeta.excludedReason, 'image_url');
    assert.equal(timeline[2].cta, 'book a demo');

    const excluded = sanitizeTimelineCta('https://facebook.com/acme');
    assert.equal(excluded.cta, null);
    assert.equal(excluded.excludedReason, 'social_profile_link');
  });
});

describe('briefing CTA filtering integration', () => {
  const COMPANY = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa';
  const EMAILS = [
    '11111111-1111-1111-1111-111111111111',
    '22222222-2222-2222-2222-222222222222',
    '33333333-3333-3333-3333-333333333333',
    '44444444-4444-4444-4444-444444444444',
  ];

  function poolWithNoisyCtas() {
    const emails = [
      {
        id: EMAILS[0],
        company_id: COMPANY,
        company_name: 'Acme',
        import_intent: 'general_market_messaging',
        received_at: new Date('2026-07-10T10:00:00.000Z'),
      },
      {
        id: EMAILS[1],
        company_id: COMPANY,
        company_name: 'Acme',
        import_intent: 'general_market_messaging',
        received_at: new Date('2026-07-17T10:00:00.000Z'),
      },
      {
        id: EMAILS[2],
        company_id: COMPANY,
        company_name: 'Acme',
        import_intent: 'general_market_messaging',
        received_at: new Date('2026-07-24T10:00:00.000Z'),
      },
      {
        id: EMAILS[3],
        company_id: COMPANY,
        company_name: 'Acme',
        import_intent: 'general_market_messaging',
        received_at: new Date('2026-07-31T10:00:00.000Z'),
      },
    ];

    const observations = [
      {
        id: 'o1',
        email_id: EMAILS[0],
        company_id: COMPANY,
        company_name: 'Acme',
        field: 'cta',
        category: 'campaign',
        value_text: 'https://linkedin.com/company/acme',
        value_json: {},
        evidence_quote: 'https://linkedin.com/company/acme',
        received_at: emails[0].received_at,
        extracted_at: emails[0].received_at,
        import_intent: 'general_market_messaging',
      },
      {
        id: 'o2',
        email_id: EMAILS[1],
        company_id: COMPANY,
        company_name: 'Acme',
        field: 'cta',
        category: 'campaign',
        value_text: 'https://cdn.acme.com/hero.png',
        value_json: {},
        evidence_quote: 'https://cdn.acme.com/hero.png',
        received_at: emails[1].received_at,
        extracted_at: emails[1].received_at,
        import_intent: 'general_market_messaging',
      },
      {
        id: 'o3',
        email_id: EMAILS[2],
        company_id: COMPANY,
        company_name: 'Acme',
        field: 'cta',
        category: 'campaign',
        value_text: 'https://acme.com/about-us',
        value_json: {},
        evidence_quote: 'https://acme.com/about-us',
        received_at: emails[2].received_at,
        extracted_at: emails[2].received_at,
        import_intent: 'general_market_messaging',
      },
      {
        id: 'o4',
        email_id: EMAILS[3],
        company_id: COMPANY,
        company_name: 'Acme',
        field: 'cta',
        category: 'campaign',
        value_text: 'book a demo',
        value_json: {},
        evidence_quote: 'Book a demo',
        received_at: emails[3].received_at,
        extracted_at: emails[3].received_at,
        import_intent: 'general_market_messaging',
      },
    ];

    return {
      async query(sql, params = []) {
        const text = String(sql);

        if (text.includes('to_regclass')) return { rows: [{ name: 'public.stub' }] };
        if (text.includes('AS total_emails')) {
          return {
            rows: [{
              total_emails: emails.length,
              total_observations: observations.length,
              emails_with_observations: emails.length,
              companies_observed: 1,
              companies_with_observations: 1,
              companies_with_profiles: 1,
              unknown_company_present: false,
              emails_assigned_to_unknown: 0,
            }],
          };
        }
        if (text.includes('FROM market_intel_sync_state')) {
          return {
            rows: [{
              id: 'default',
              label: 'MARKET_INTEL',
              days: 365,
              import_intent: 'general_market_messaging',
              last_synced_at: new Date('2026-07-31T00:00:00.000Z'),
              last_run_stats: {},
              updated_at: new Date('2026-07-31T00:00:00.000Z'),
            }],
          };
        }

        if (
          text.includes("o.field = 'cta'") &&
          !text.includes('GROUP BY') &&
          text.includes('o.value_json')
        ) {
          return {
            rows: observations.map((o) => ({
              id: o.id,
              value_text: o.value_text,
              value_json: o.value_json,
              evidence_quote: o.evidence_quote,
              email_id: o.email_id,
              received_at: o.received_at,
              company_name: o.company_name,
            })),
          };
        }

        if (text.includes('AS company_name') && text.includes('AS email_count') && text.includes('GROUP BY c.id')) {
          return {
            rows: [{
              company_id: COMPANY,
              company_name: 'Acme',
              email_count: emails.length,
              observation_count: observations.length,
              first_observed_at: emails[0].received_at,
              last_observed_at: emails[3].received_at,
              import_intent: 'general_market_messaging',
            }],
          };
        }

        if (text.includes('FROM market_emails e') && text.includes('WHERE e.company_id = $1')) {
          return {
            rows: emails.map((e) => ({
              id: e.id,
              gmail_id: `g-${e.id.slice(0, 4)}`,
              thread_id: 't1',
              subject: 'Hello',
              from_email: 'hello@acme.com',
              from_name: 'Acme',
              received_at: e.received_at,
              sent_at: null,
              links: [],
              company_name: 'Acme',
              company_domain: 'acme.com',
              imported_at: e.received_at,
            })),
          };
        }

        if (text.includes('FROM market_observations') && text.includes('ANY($1::uuid[])')) {
          const ids = new Set(params[0] || []);
          return {
            rows: observations
              .filter((o) => ids.has(o.email_id))
              .map((o) => ({
                id: o.id,
                email_id: o.email_id,
                category: o.category,
                field: o.field,
                value_text: o.value_text,
                value_json: o.value_json,
                evidence_quote: o.evidence_quote,
                evidence_path: 'links',
                extractor: 'deterministic_v1',
                extracted_at: o.extracted_at,
              })),
          };
        }

        return { rows: [] };
      },
    };
  }

  it('top CTAs exclude social/image/footer URLs', async () => {
    const ctas = await getTopCtas({
      pool: poolWithNoisyCtas(),
      days: 60,
      until: '2026-08-01T12:00:00.000Z',
    });

    assert.equal(ctas.length, 1);
    assert.equal(ctas[0].cta, 'book a demo');
    assert.equal(ctas.some((c) => /linkedin|png|about-us|http/i.test(c.cta)), false);
  });

  it('recent CTA changes ignore excluded social/image/footer CTA values', async () => {
    const result = await getRecentMessagingChanges({
      pool: poolWithNoisyCtas(),
      days: 60,
      limit: 10,
      until: '2026-08-01T12:00:00.000Z',
    });

    const ctaChanges = (result.items || []).filter((c) => c.changeType === 'cta_changed');
    for (const change of ctaChanges) {
      assert.equal(/linkedin|png|about-us|facebook|http/i.test(change.summary), false);
      assert.equal(/linkedin|png|about-us|facebook|http/i.test(String(change.previousWindow?.value || '')), false);
      assert.equal(/linkedin|png|about-us|facebook|http/i.test(String(change.recentWindow?.value || '')), false);
    }

    // With only one usable CTA across the window, there should be no CTA change claim.
    assert.equal(ctaChanges.length, 0);
  });
});
