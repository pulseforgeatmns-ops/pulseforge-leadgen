'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  clampDays,
  clampLimit,
  formatBriefingReport,
  getCompanyCadence,
  getMarketIntelligenceBriefing,
  getMessagingThemes,
  getObservationsByIntent,
  getRecentMessagingChanges,
  getTopCtas,
  getTopOffers,
  resolveWindow,
} = require('../services/marketIntelligenceBriefing');

const COMPANY_A = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa';
const COMPANY_B = 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb';
const EMAIL_1 = '11111111-1111-1111-1111-111111111111';
const EMAIL_2 = '22222222-2222-2222-2222-222222222222';
const EMAIL_3 = '33333333-3333-3333-3333-333333333333';
const OBS_1 = 'o1111111-1111-1111-1111-111111111111';
const OBS_2 = 'o2222222-2222-2222-2222-222222222222';
const OBS_3 = 'o3333333-3333-3333-3333-333333333333';
const OBS_4 = 'o4444444-4444-4444-4444-444444444444';

function fixedNow() {
  return new Date('2026-08-01T12:00:00.000Z');
}

function buildMockPool({ empty = false } = {}) {
  const emails = empty
    ? []
    : [
        {
          id: EMAIL_1,
          company_id: COMPANY_A,
          company_name: 'Apollo',
          import_intent: 'general_market_messaging',
          received_at: new Date('2026-07-20T10:00:00.000Z'),
          is_unknown: false,
        },
        {
          id: EMAIL_2,
          company_id: COMPANY_A,
          company_name: 'Apollo',
          import_intent: 'general_market_messaging',
          received_at: new Date('2026-07-25T10:00:00.000Z'),
          is_unknown: false,
        },
        {
          id: EMAIL_3,
          company_id: COMPANY_B,
          company_name: 'Clay',
          import_intent: 'competitive_watch',
          received_at: new Date('2026-07-22T10:00:00.000Z'),
          is_unknown: false,
        },
      ];

  const observations = empty
    ? []
    : [
        {
          id: OBS_1,
          email_id: EMAIL_1,
          company_id: COMPANY_A,
          company_name: 'Apollo',
          field: 'offer',
          category: 'messaging',
          value_text: '20% off annual',
          received_at: emails[0].received_at,
          extracted_at: emails[0].received_at,
          import_intent: 'general_market_messaging',
          is_unknown: false,
        },
        {
          id: OBS_2,
          email_id: EMAIL_2,
          company_id: COMPANY_A,
          company_name: 'Apollo',
          field: 'offer',
          category: 'messaging',
          value_text: '20% off annual',
          received_at: emails[1].received_at,
          extracted_at: emails[1].received_at,
          import_intent: 'general_market_messaging',
          is_unknown: false,
        },
        {
          id: OBS_3,
          email_id: EMAIL_1,
          company_id: COMPANY_A,
          company_name: 'Apollo',
          field: 'cta',
          category: 'campaign',
          value_text: 'book a demo',
          received_at: emails[0].received_at,
          extracted_at: emails[0].received_at,
          import_intent: 'general_market_messaging',
          is_unknown: false,
        },
        {
          id: OBS_4,
          email_id: EMAIL_3,
          company_id: COMPANY_B,
          company_name: 'Clay',
          field: 'cta',
          category: 'campaign',
          value_text: 'start free trial',
          received_at: emails[2].received_at,
          extracted_at: emails[2].received_at,
          import_intent: 'competitive_watch',
          is_unknown: false,
        },
        {
          id: 'o5555555-5555-5555-5555-555555555555',
          email_id: EMAIL_2,
          company_id: COMPANY_A,
          company_name: 'Apollo',
          field: 'positioning',
          category: 'messaging',
          value_text: 'outbound_automation',
          received_at: emails[1].received_at,
          extracted_at: emails[1].received_at,
          import_intent: 'general_market_messaging',
          is_unknown: false,
        },
      ];

  function inWindow(row, since, until) {
    const at = new Date(row.received_at);
    return at >= since && at <= until;
  }

  function matchesFilters(row, params, hasCompany, hasIntent) {
    const since = params[0];
    const until = params[1];
    if (!inWindow(row, since, until)) return false;
    let idx = 2;
    if (hasCompany) {
      if (String(row.company_id) !== String(params[idx])) return false;
      idx += 1;
    }
    if (hasIntent) {
      if (String(row.import_intent) !== String(params[idx])) return false;
    }
    return true;
  }

  return {
    async query(sql, params = []) {
      const text = String(sql);

      // readiness table checks / corpus (best-effort stub)
      if (text.includes('to_regclass')) {
        return { rows: [{ name: 'public.stub' }] };
      }
      if (text.includes('FROM market_emails') && text.includes('total_emails')) {
        return {
          rows: [
            {
              total_emails: emails.length,
              total_observations: observations.length,
              emails_with_observations: new Set(observations.map((o) => o.email_id)).size,
              companies_observed: new Set(emails.map((e) => e.company_id)).size,
              companies_with_observations: new Set(observations.map((o) => o.company_id)).size,
              companies_with_profiles: 1,
              unknown_company_present: false,
              emails_assigned_to_unknown: 0,
            },
          ],
        };
      }
      if (text.includes('FROM market_intel_sync_state')) {
        return {
          rows: [
            {
              id: 'default',
              label: 'MARKET_INTEL',
              days: 365,
              import_intent: 'general_market_messaging',
              last_synced_at: new Date('2026-07-30T00:00:00.000Z'),
              last_run_stats: {},
              updated_at: new Date('2026-07-30T00:00:00.000Z'),
            },
          ],
        };
      }

      // Timeline for recent changes
      if (text.includes('FROM market_emails e') && text.includes('WHERE e.company_id = $1')) {
        const companyId = params[0];
        return {
          rows: emails
            .filter((e) => e.company_id === companyId)
            .map((e) => ({
              id: e.id,
              gmail_id: `g-${e.id.slice(0, 4)}`,
              thread_id: 't1',
              subject: 'Subject',
              from_email: 'hello@example.com',
              from_name: e.company_name,
              received_at: e.received_at,
              sent_at: null,
              links: [],
              company_name: e.company_name,
              company_domain: `${e.company_name.toLowerCase()}.io`,
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
              value_json: {},
              evidence_quote: o.value_text,
              evidence_path: 'body_text',
              extractor: 'deterministic_v1',
              extracted_at: o.extracted_at,
            })),
        };
      }

      // Raw CTA rows for briefing normalize/re-aggregate
      if (
        text.includes("o.field = 'cta'") &&
        !text.includes('GROUP BY') &&
        text.includes('o.value_json')
      ) {
        const hasCompany = text.includes('e.company_id = $');
        const hasIntent = text.includes('e.import_intent = $');
        const filtered = observations.filter((o) => {
          if (o.field !== 'cta') return false;
          return matchesFilters(o, params, hasCompany, hasIntent);
        });
        return {
          rows: filtered.map((o) => ({
            id: o.id,
            value_text: o.value_text,
            value_json: {},
            evidence_quote: o.value_text,
            email_id: o.email_id,
            received_at: o.received_at,
            company_name: o.company_name,
          })),
        };
      }

      // company cadence
      if (
        text.includes('AS company_name') &&
        text.includes('AS email_count') &&
        text.includes('GROUP BY c.id')
      ) {
        const hasCompany = text.includes('e.company_id = $');
        const hasIntent = text.includes('e.import_intent = $');
        const filtered = emails.filter((e) => matchesFilters(e, params, hasCompany, hasIntent));
        const byCompany = new Map();
        for (const e of filtered) {
          if (!byCompany.has(e.company_id)) {
            byCompany.set(e.company_id, {
              company_id: e.company_id,
              company_name: e.company_name,
              email_count: 0,
              observation_count: 0,
              first_observed_at: e.received_at,
              last_observed_at: e.received_at,
              import_intent: e.import_intent,
            });
          }
          const row = byCompany.get(e.company_id);
          row.email_count += 1;
          row.observation_count += observations.filter((o) => o.email_id === e.id).length;
          if (e.received_at < row.first_observed_at) row.first_observed_at = e.received_at;
          if (e.received_at > row.last_observed_at) row.last_observed_at = e.received_at;
        }
        const limit = Number(params[params.length - 1]) || 10;
        return {
          rows: [...byCompany.values()]
            .sort((a, b) => b.email_count - a.email_count || a.company_name.localeCompare(b.company_name))
            .slice(0, limit),
        };
      }

      // corpus summary
      if (
        text.includes('AS email_count') &&
        text.includes('AS company_count') &&
        text.includes('AS observation_count') &&
        text.includes('AS import_intents')
      ) {
        const hasCompany = text.includes('e.company_id = $') || text.includes('e2.company_id = $');
        const hasIntent = text.includes('e.import_intent = $') || text.includes('e2.import_intent = $');
        const filteredEmails = emails.filter((e) => matchesFilters(e, params, hasCompany, hasIntent));
        const filteredObs = observations.filter((o) => matchesFilters(o, params, hasCompany, hasIntent));
        return {
          rows: [
            {
              email_count: filteredEmails.length,
              company_count: new Set(filteredEmails.map((e) => e.company_id)).size,
              observation_count: filteredObs.length,
              import_intents: [...new Set(filteredEmails.map((e) => e.import_intent))],
            },
          ],
        };
      }

      // observations by intent
      if (text.includes('AS import_intent') && text.includes('GROUP BY e.import_intent')) {
        const hasCompany = text.includes('e.company_id = $');
        const hasIntent = text.includes('e.import_intent = $');
        const filtered = emails.filter((e) => matchesFilters(e, params, hasCompany, hasIntent));
        const byIntent = new Map();
        for (const e of filtered) {
          const key = e.import_intent || 'unknown';
          if (!byIntent.has(key)) {
            byIntent.set(key, {
              import_intent: key,
              email_count: 0,
              observation_count: 0,
              company_ids: new Set(),
              latest_observed_at: e.received_at,
            });
          }
          const row = byIntent.get(key);
          row.email_count += 1;
          row.observation_count += observations.filter((o) => o.email_id === e.id).length;
          row.company_ids.add(e.company_id);
          if (e.received_at > row.latest_observed_at) row.latest_observed_at = e.received_at;
        }
        return {
          rows: [...byIntent.values()].map((r) => ({
            import_intent: r.import_intent,
            email_count: r.email_count,
            observation_count: r.observation_count,
            company_count: r.company_ids.size,
            latest_observed_at: r.latest_observed_at,
          })),
        };
      }

      // pattern aggregates (offers/themes)
      if (
        text.includes('FROM market_observations o') &&
        text.includes('GROUP BY') &&
        (text.includes('AS label') || text.includes('AS theme'))
      ) {
        const hasCompany = text.includes('e.company_id = $');
        const hasIntent = text.includes('e.import_intent = $');
        let idx = 2;
        if (hasCompany) idx += 1;
        if (hasIntent) idx += 1;

        let fieldFilter = null;
        let fieldsAny = null;
        let category = null;
        if (text.includes('o.field = $')) {
          fieldFilter = params[idx];
          idx += 1;
        } else if (text.includes('o.field = ANY')) {
          fieldsAny = params[idx];
          idx += 1;
        }
        if (text.includes('o.category = $')) {
          category = params[idx];
          idx += 1;
        }
        const limit = Number(params[params.length - 1]) || 10;

        const filtered = observations.filter((o) => {
          if (!matchesFilters(o, params, hasCompany, hasIntent)) return false;
          if (fieldFilter && o.field !== fieldFilter) return false;
          if (fieldsAny && !fieldsAny.includes(o.field)) return false;
          if (category && o.category !== category) return false;
          return true;
        });

        const isTheme = text.includes('AS theme');
        const groups = new Map();
        for (const o of filtered) {
          const key = isTheme ? `${o.field}:${o.value_text}` : o.value_text;
          if (!groups.has(key)) {
            groups.set(key, {
              label: o.value_text,
              theme: key,
              field: o.field,
              count: 0,
              companies: new Set(),
              latest_observed_at: o.received_at,
              example_observation_ids: [],
              example_email_ids: [],
            });
          }
          const g = groups.get(key);
          g.count += 1;
          g.companies.add(o.company_name);
          if (o.received_at > g.latest_observed_at) g.latest_observed_at = o.received_at;
          if (g.example_observation_ids.length < 5) g.example_observation_ids.push(o.id);
          if (g.example_email_ids.length < 5) g.example_email_ids.push(o.email_id);
        }

        const rows = [...groups.values()]
          .sort((a, b) => b.count - a.count || String(a.label || a.theme).localeCompare(String(b.label || b.theme)))
          .slice(0, limit)
          .map((g) => ({
            label: g.label,
            theme: g.theme,
            field: g.field,
            count: g.count,
            companies: [...g.companies],
            latest_observed_at: g.latest_observed_at,
            example_observation_ids: g.example_observation_ids,
            example_email_ids: g.example_email_ids,
          }));
        return { rows };
      }

      return { rows: [] };
    },
  };
}

describe('marketIntelligenceBriefing helpers', () => {
  it('clamps days/limit and resolves windows', () => {
    assert.equal(clampDays(30), 30);
    assert.equal(clampDays(0), 30);
    assert.equal(clampLimit(10), 10);
    assert.equal(clampLimit(999), 100);

    const window = resolveWindow({
      since: '2026-07-01T00:00:00.000Z',
      until: '2026-07-31T00:00:00.000Z',
    });
    assert.equal(window.days, 30);
    assert.equal(window.since, '2026-07-01T00:00:00.000Z');
  });
});

describe('marketIntelligenceBriefing with mock pool', () => {
  it('returns expected briefing shape with isEvidence false', async () => {
    const pool = buildMockPool();
    const briefing = await getMarketIntelligenceBriefing({
      pool,
      days: 30,
      limit: 10,
      until: fixedNow().toISOString(),
    });

    assert.equal(briefing.ok, true);
    assert.equal(briefing.kind, 'market_intelligence_briefing');
    assert.equal(briefing.isEvidence, false);
    assert.ok(briefing.generatedAt);
    assert.ok(briefing.window.since);
    assert.ok(briefing.window.until);
    assert.equal(typeof briefing.corpus.emailCount, 'number');
    assert.ok(Array.isArray(briefing.sections.topOffers));
    assert.ok(Array.isArray(briefing.sections.topCtas));
    assert.ok(Array.isArray(briefing.sections.messagingThemes));
    assert.ok(Array.isArray(briefing.sections.companyCadence));
    assert.ok(Array.isArray(briefing.sections.recentChanges));
    assert.ok(Array.isArray(briefing.sections.observationsByIntent));
    assert.ok(Array.isArray(briefing.caveats));
    assert.ok(briefing.caveats.some((c) => c.includes('synthesis_not_evidence')));
  });

  it('empty corpus returns ok response with caveats, not fake populated sections', async () => {
    const pool = buildMockPool({ empty: true });
    const briefing = await getMarketIntelligenceBriefing({
      pool,
      days: 30,
      until: fixedNow().toISOString(),
    });

    assert.equal(briefing.ok, true);
    assert.equal(briefing.isEvidence, false);
    assert.equal(briefing.corpus.emailCount, 0);
    assert.equal(briefing.sections.topOffers.length, 0);
    assert.equal(briefing.sections.topCtas.length, 0);
    assert.equal(briefing.sections.companyCadence.length, 0);
    assert.ok(briefing.caveats.some((c) => c.startsWith('empty_corpus')));
  });

  it('aggregates top offers correctly', async () => {
    const pool = buildMockPool();
    const offers = await getTopOffers({
      pool,
      days: 30,
      until: fixedNow().toISOString(),
    });

    assert.ok(offers.length >= 1);
    assert.equal(offers[0].label, '20% off annual');
    assert.equal(offers[0].count, 2);
    assert.ok(offers[0].companies.includes('Apollo'));
    assert.ok(offers[0].exampleObservationIds.length >= 1);
    assert.ok(offers[0].exampleEmailIds.length >= 1);
  });

  it('aggregates top CTAs correctly', async () => {
    const pool = buildMockPool();
    const ctas = await getTopCtas({
      pool,
      days: 30,
      until: fixedNow().toISOString(),
    });

    assert.equal(ctas.length, 2);
    const labels = ctas.map((c) => c.cta).sort();
    assert.deepEqual(labels, ['book a demo', 'start free trial']);
    assert.equal(ctas.every((c) => c.count === 1), true);
  });

  it('company cadence sorts by activity', async () => {
    const pool = buildMockPool();
    const cadence = await getCompanyCadence({
      pool,
      days: 30,
      until: fixedNow().toISOString(),
    });

    assert.ok(cadence.length >= 2);
    assert.equal(cadence[0].companyName, 'Apollo');
    assert.equal(cadence[0].emailCount, 2);
    assert.ok(cadence[0].emailCount >= cadence[1].emailCount);
  });

  it('import intent filter works for offers and intent breakdown', async () => {
    const pool = buildMockPool();
    const offers = await getTopOffers({
      pool,
      days: 30,
      until: fixedNow().toISOString(),
      importIntent: 'competitive_watch',
    });
    assert.equal(offers.length, 0);

    const ctas = await getTopCtas({
      pool,
      days: 30,
      until: fixedNow().toISOString(),
      importIntent: 'competitive_watch',
    });
    assert.equal(ctas.length, 1);
    assert.equal(ctas[0].cta, 'start free trial');

    const intents = await getObservationsByIntent({
      pool,
      days: 30,
      until: fixedNow().toISOString(),
    });
    const names = intents.map((i) => i.importIntent).sort();
    assert.deepEqual(names, ['competitive_watch', 'general_market_messaging']);
  });

  it('messaging themes prefer structured fields and keep headlines optional', async () => {
    const pool = buildMockPool();
    const themes = await getMessagingThemes({
      pool,
      days: 30,
      until: fixedNow().toISOString(),
    });
    assert.ok(themes.items.some((t) => t.theme === 'positioning:outbound_automation'));
    assert.equal(themes.items.some((t) => String(t.theme).startsWith('headline:')), false);
    assert.equal(themes.includeHeadlines, false);

    const withHeadlines = await getMessagingThemes({
      pool,
      days: 30,
      until: fixedNow().toISOString(),
      includeHeadlines: true,
    });
    assert.equal(withHeadlines.includeHeadlines, true);
    // Fixture has no headline field; section should still be present/empty.
    assert.ok(Array.isArray(withHeadlines.headlinePatterns));
  });

  it('recent changes returns caveats when contrast is thin', async () => {
    const pool = buildMockPool();
    const result = await getRecentMessagingChanges({
      pool,
      days: 30,
      limit: 5,
      until: fixedNow().toISOString(),
    });
    assert.ok(Array.isArray(result.items));
    assert.ok(Array.isArray(result.caveats));
    if (!result.items.length) {
      assert.ok(result.caveats.some((c) => c.includes('recent_changes_unavailable')));
    }
  });

  it('formatBriefingReport renders human output', async () => {
    const pool = buildMockPool();
    const briefing = await getMarketIntelligenceBriefing({
      pool,
      days: 30,
      until: fixedNow().toISOString(),
    });
    const text = formatBriefingReport(briefing);
    assert.match(text, /Market Intelligence Briefing/);
    assert.match(text, /Top Offers:/);
    assert.match(text, /Top CTAs:/);
    assert.match(text, /Most Active Companies:/);
    assert.match(text, /Caveats:/);
    assert.match(text, /isEvidence: false/);
  });
});
