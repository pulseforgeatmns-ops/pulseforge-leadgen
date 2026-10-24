'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  diffTimelineField,
  median,
  modeValue,
} = require('../services/marketIntelligenceQuery');

describe('marketIntelligenceQuery helpers', () => {
  it('diffTimelineField reports descriptive CTA and pricing changes only', () => {
    const timeline = [
      {
        touch: 1,
        id: 'e1',
        cta: 'book a demo',
        pricingMention: null,
        positioning: 'outbound_automation',
        guarantee: null,
        subjectLength: 20,
        cadenceDaysFromPrevious: null,
        receivedAt: '2026-01-01T00:00:00.000Z',
      },
      {
        touch: 2,
        id: 'e2',
        cta: 'start free trial',
        pricingMention: '$99/mo',
        positioning: 'outbound_automation',
        guarantee: 'money-back guarantee',
        subjectLength: 12,
        cadenceDaysFromPrevious: 7,
        receivedAt: '2026-01-08T00:00:00.000Z',
      },
    ];

    const ctaDiffs = diffTimelineField(timeline, 'cta');
    assert.equal(ctaDiffs.length, 1);
    assert.equal(ctaDiffs[0].before, 'book a demo');
    assert.equal(ctaDiffs[0].after, 'start free trial');
    assert.equal(ctaDiffs[0].fromTouch, 1);
    assert.equal(ctaDiffs[0].toTouch, 2);

    const pricingDiffs = diffTimelineField(timeline, 'pricing_mention');
    assert.equal(pricingDiffs.length, 1);
    assert.equal(pricingDiffs[0].after, '$99/mo');

    const positioningDiffs = diffTimelineField(timeline, 'positioning');
    assert.equal(positioningDiffs.length, 0);

    const guaranteePresence = diffTimelineField(timeline, 'guarantee_presence');
    assert.equal(guaranteePresence.length, 1);
    assert.equal(guaranteePresence[0].before, false);
    assert.equal(guaranteePresence[0].after, true);

    const subjectLen = diffTimelineField(timeline, 'subject_length');
    assert.equal(subjectLen[0].before, 20);
    assert.equal(subjectLen[0].after, 12);
  });

  it('modeValue and median are deterministic', () => {
    assert.equal(modeValue(['a', 'b', 'a', 'c']), 'a');
    assert.equal(median([1, 3, 2]), 2);
    assert.equal(median([1, 2, 3, 4]), 2.5);
    assert.equal(median([]), null);
  });
});

describe('marketIntelligenceQuery with mock pool', () => {
  it('rebuilds a profile with evidenceRefs from timeline observations', async () => {
    const companyId = '11111111-1111-1111-1111-111111111111';
    const email1 = '22222222-2222-2222-2222-222222222222';
    const email2 = '33333333-3333-3333-3333-333333333333';
    let profileInsert = null;

    const pool = {
      async query(sql, params = []) {
        if (sql.includes('FROM market_companies WHERE id')) {
          return {
            rows: [{ id: companyId, name: 'Apollo', domain: 'apollo.io', is_unknown: false }],
          };
        }
        if (sql.includes('FROM market_emails e') && sql.includes('JOIN market_companies')) {
          return {
            rows: [
              {
                id: email1,
                gmail_id: 'g1',
                thread_id: 't1',
                subject: 'Book a demo',
                from_email: 'hello@apollo.io',
                from_name: 'Apollo',
                received_at: new Date('2026-01-01T00:00:00Z'),
                sent_at: null,
                links: [],
                company_name: 'Apollo',
                company_domain: 'apollo.io',
                imported_at: new Date('2026-01-01T00:00:00Z'),
              },
              {
                id: email2,
                gmail_id: 'g2',
                thread_id: 't1',
                subject: 'AI SDR messaging',
                from_email: 'hello@apollo.io',
                from_name: 'Apollo',
                received_at: new Date('2026-02-01T00:00:00Z'),
                sent_at: null,
                links: [],
                company_name: 'Apollo',
                company_domain: 'apollo.io',
                imported_at: new Date('2026-02-01T00:00:00Z'),
              },
            ],
          };
        }
        if (sql.includes('FROM market_observations') && sql.includes('ANY')) {
          return {
            rows: [
              {
                id: 'o1',
                email_id: email1,
                category: 'campaign',
                field: 'cta',
                value_text: 'book a demo',
                value_json: {},
                evidence_quote: 'Book a demo',
                evidence_path: 'subject',
                extractor: 'deterministic_v1',
                extracted_at: new Date(),
              },
              {
                id: 'o2',
                email_id: email1,
                category: 'messaging',
                field: 'positioning',
                value_text: 'outbound_automation',
                value_json: {},
                evidence_quote: 'outbound automation',
                evidence_path: 'body_text',
                extractor: 'deterministic_v1',
                extracted_at: new Date(),
              },
              {
                id: 'o3',
                email_id: email2,
                category: 'campaign',
                field: 'cta',
                value_text: 'book a demo',
                value_json: {},
                evidence_quote: 'Book a demo',
                evidence_path: 'subject',
                extractor: 'deterministic_v1',
                extracted_at: new Date(),
              },
              {
                id: 'o4',
                email_id: email2,
                category: 'messaging',
                field: 'positioning',
                value_text: 'ai_sdr',
                value_json: {},
                evidence_quote: 'AI SDR',
                evidence_path: 'body_text',
                extractor: 'deterministic_v1',
                extracted_at: new Date(),
              },
              {
                id: 'o5',
                email_id: email2,
                category: 'messaging',
                field: 'offer',
                value_text: 'free trial',
                value_json: {},
                evidence_quote: 'free trial',
                evidence_path: 'body_text',
                extractor: 'deterministic_v1',
                extracted_at: new Date(),
              },
            ],
          };
        }
        if (sql.includes('INSERT INTO market_company_profiles')) {
          profileInsert = params;
          return { rows: [] };
        }
        throw new Error(`unexpected sql: ${sql.slice(0, 80)}`);
      },
    };

    const { rebuildCompanyProfile } = require('../services/marketIntelligenceQuery');
    const profile = await rebuildCompanyProfile(companyId, { pool });

    assert.equal(profile.companyName, 'Apollo');
    assert.equal(profile.emailsObserved, 2);
    assert.equal(profile.distinctOffers, 1);
    assert.equal(profile.currentCta, 'book a demo');
    assert.equal(profile.latestDirection, 'ai_sdr');
    assert.deepEqual(profile.evidenceRefs, [email1, email2]);
    assert.ok(profileInsert);
    assert.equal(profileInsert[0], companyId);
  });

  it('aggregates cross-market patterns with sample evidenceRefs', async () => {
    const pool = {
      async query(sql, params) {
        assert.equal(params[0], 'cta');
        return {
          rows: [
            {
              value_text: 'book a demo',
              count: 12,
              evidence_refs: ['e1', 'e2', 'e3', 'e4', 'e5', 'e6'],
            },
            {
              value_text: 'learn more',
              count: 4,
              evidence_refs: ['e9'],
            },
          ],
        };
      },
    };
    const { crossMarketPatterns } = require('../services/marketIntelligenceQuery');
    const result = await crossMarketPatterns({ pool, field: 'cta', limit: 10 });
    assert.equal(result.field, 'cta');
    assert.equal(result.patterns[0].count, 12);
    assert.equal(result.patterns[0].evidenceRefs.length, 5);
  });

  it('computes cross-market sequence stats from lengths and gaps', async () => {
    const pool = {
      async query(sql) {
        if (sql.includes('GROUP BY company_id')) {
          return { rows: [{ seq_len: 3 }, { seq_len: 5 }, { seq_len: 7 }] };
        }
        if (sql.includes('cadence_days')) {
          return {
            rows: [
              { cadence_days: 3 },
              { cadence_days: 5 },
              { cadence_days: 7 },
            ],
          };
        }
        throw new Error(sql.slice(0, 60));
      },
    };
    const { crossMarketSequenceStats } = require('../services/marketIntelligenceQuery');
    const stats = await crossMarketSequenceStats({ pool });
    assert.equal(stats.companies, 3);
    assert.equal(stats.averageSequenceLength, 5);
    assert.equal(stats.medianSequenceLength, 5);
    assert.equal(stats.averageFollowUpSpacingDays, 5);
  });
});
