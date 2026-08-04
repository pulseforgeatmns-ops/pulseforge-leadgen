'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  REQUIRED_TABLES,
  buildMarketIntelReadinessReport,
  deriveReadinessStatus,
  emptyMetrics,
  formatReadinessReport,
  roundPct,
} = require('../services/marketIntelligenceReadiness');
const { parseArgs } = require('../scripts/marketIntelReadiness');

function allTablesPresent() {
  return {
    tables: Object.fromEntries(REQUIRED_TABLES.map((t) => [t, true])),
    missing: [],
    allPresent: true,
  };
}

describe('marketIntelligenceReadiness helpers', () => {
  it('roundPct handles zero denominator', () => {
    assert.equal(roundPct(0, 0), 0);
    assert.equal(roundPct(1, 2), 50);
    assert.equal(roundPct(1, 3), 33.3);
  });

  it('marks missing tables as blocked', () => {
    const derived = deriveReadinessStatus({
      tableReadiness: {
        tables: { market_emails: false },
        missing: ['market_emails'],
        allPresent: false,
      },
      metrics: emptyMetrics(),
    });
    assert.equal(derived.status, 'blocked');
    assert.ok(derived.blockers.some((b) => b.includes('missing_table:market_emails')));
    assert.ok(derived.nextActions.some((a) => a.includes('migrations/')));
  });

  it('marks empty corpus as blocked even when tables exist', () => {
    const derived = deriveReadinessStatus({
      tableReadiness: allTablesPresent(),
      metrics: emptyMetrics(),
    });
    assert.equal(derived.status, 'blocked');
    assert.ok(derived.blockers.some((b) => b.startsWith('empty_corpus')));
    assert.ok(derived.nextActions.some((a) => a.includes('market:intel:import')));
  });

  it('marks low extraction/profile coverage as partial', () => {
    const metrics = emptyMetrics();
    metrics.totalEmails = 100;
    metrics.emailsWithObservations = 20;
    metrics.emailExtractionCoveragePct = 20;
    metrics.companiesObserved = 10;
    metrics.companiesWithProfiles = 2;
    metrics.profileRebuildCoveragePct = 20;
    metrics.lastSyncState = {
      id: 'default',
      lastSyncedAt: '2026-08-01T00:00:00.000Z',
    };

    const derived = deriveReadinessStatus({
      tableReadiness: allTablesPresent(),
      metrics,
    });
    assert.equal(derived.status, 'partial');
    assert.ok(derived.blockers.some((b) => b.includes('email_extraction_coverage_below_floor')));
    assert.ok(derived.blockers.some((b) => b.includes('profile_rebuild_coverage_below_floor')));
  });

  it('marks ready when floors and sync timestamp are met', () => {
    const metrics = emptyMetrics();
    metrics.totalEmails = 40;
    metrics.totalObservations = 120;
    metrics.emailsWithObservations = 30;
    metrics.emailExtractionCoveragePct = 75;
    metrics.companiesObserved = 8;
    metrics.companiesWithObservations = 7;
    metrics.companyExtractionCoveragePct = 87.5;
    metrics.companiesWithProfiles = 6;
    metrics.profileRebuildCoveragePct = 75;
    metrics.unknownCompanyPresent = true;
    metrics.emailsAssignedToUnknown = 2;
    metrics.lastSyncState = {
      id: 'default',
      label: 'MARKET_INTEL',
      days: 365,
      lastSyncedAt: '2026-08-03T12:00:00.000Z',
      lastRunStats: { imported: 40 },
      updatedAt: '2026-08-03T12:00:00.000Z',
    };

    const derived = deriveReadinessStatus({
      tableReadiness: allTablesPresent(),
      metrics,
    });
    assert.equal(derived.status, 'ready');
    assert.equal(derived.blockers.length, 0);
    assert.ok(derived.nextActions.some((a) => /trustworthy|observational/i.test(a)));
  });

  it('formatReadinessReport includes status and blockers', () => {
    const text = formatReadinessReport({
      status: 'blocked',
      generatedAt: '2026-08-03T00:00:00.000Z',
      tableReadiness: allTablesPresent(),
      metrics: emptyMetrics(),
      blockers: ['empty_corpus: no market_emails imported'],
      nextActions: ['Run npm run market:intel:import to ingest labeled marketing emails'],
    });
    assert.match(text, /Status: blocked/);
    assert.match(text, /empty_corpus/);
    assert.match(text, /market:intel:import/);
  });
});

describe('marketIntelligenceReadiness with mock pool', () => {
  it('builds a blocked report when tables are missing', async () => {
    const pool = {
      async query(sql, params = []) {
        if (sql.includes('to_regclass')) {
          const name = String(params[0] || '');
          if (name.includes('market_emails')) {
            return { rows: [{ name: null }] };
          }
          return { rows: [{ name: name.replace('public.', '') }] };
        }
        throw new Error(`unexpected sql: ${sql}`);
      },
    };

    const report = await buildMarketIntelReadinessReport({ pool });
    assert.equal(report.status, 'blocked');
    assert.equal(report.observationalOnly, true);
    assert.equal(report.tableReadiness.tables.market_emails, false);
    assert.ok(report.blockers.some((b) => b.includes('missing_table:market_emails')));
  });

  it('builds a blocked empty-corpus report when counts are zero', async () => {
    const pool = {
      async query(sql) {
        if (sql.includes('to_regclass')) {
          return { rows: [{ name: 'present' }] };
        }
        if (sql.includes('AS total_emails')) {
          return {
            rows: [{
              total_emails: 0,
              total_observations: 0,
              emails_with_observations: 0,
              companies_observed: 0,
              companies_with_observations: 0,
              companies_with_profiles: 0,
              unknown_company_present: true,
              emails_assigned_to_unknown: 0,
            }],
          };
        }
        if (sql.includes('FROM market_intel_sync_state')) {
          return { rows: [] };
        }
        throw new Error(`unexpected sql: ${sql}`);
      },
    };

    const report = await buildMarketIntelReadinessReport({ pool });
    assert.equal(report.status, 'blocked');
    assert.equal(report.metrics.totalEmails, 0);
    assert.ok(report.blockers.some((b) => b.startsWith('empty_corpus')));
  });

  it('builds a ready report from corpus metrics', async () => {
    const pool = {
      async query(sql) {
        if (sql.includes('to_regclass')) {
          return { rows: [{ name: 'present' }] };
        }
        if (sql.includes('AS total_emails')) {
          return {
            rows: [{
              total_emails: 20,
              total_observations: 80,
              emails_with_observations: 18,
              companies_observed: 5,
              companies_with_observations: 5,
              companies_with_profiles: 4,
              unknown_company_present: true,
              emails_assigned_to_unknown: 1,
            }],
          };
        }
        if (sql.includes('FROM market_intel_sync_state')) {
          return {
            rows: [{
              id: 'default',
              label: 'MARKET_INTEL',
              days: 365,
              last_synced_at: new Date('2026-08-03T10:00:00.000Z'),
              last_run_stats: { imported: 20 },
              updated_at: new Date('2026-08-03T10:00:00.000Z'),
            }],
          };
        }
        throw new Error(`unexpected sql: ${sql}`);
      },
    };

    const report = await buildMarketIntelReadinessReport({ pool });
    assert.equal(report.status, 'ready');
    assert.equal(report.metrics.totalEmails, 20);
    assert.equal(report.metrics.emailExtractionCoveragePct, 90);
    assert.equal(report.metrics.profileRebuildCoveragePct, 80);
    assert.equal(report.metrics.emailsAssignedToUnknown, 1);
    assert.equal(report.metrics.unknownCompanyPresent, true);
    assert.equal(report.metrics.lastSyncState.lastSyncedAt, '2026-08-03T10:00:00.000Z');
    assert.equal(report.blockers.length, 0);
  });
});

describe('marketIntelReadiness CLI args', () => {
  it('parses --json and --check', () => {
    const options = parseArgs(['--json', '--check']);
    assert.equal(options.json, true);
    assert.equal(options.check, true);
  });

  it('rejects unknown arguments', () => {
    assert.throws(() => parseArgs(['--score']), /Unknown argument/);
  });
});
