'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  formatPreflightReport,
  preflightMarketIntelIngestion,
} = require('../services/marketIntelligencePreflight');
const { parseArgs } = require('../scripts/preflightMarketIntelligence');

describe('marketIntelligencePreflight', () => {
  it('fails closed when credentials are missing', async () => {
    const report = await preflightMarketIntelIngestion({
      deps: {
        loadOAuthCredentials() {
          throw new Error('Missing Gmail OAuth credentials');
        },
      },
    });
    assert.equal(report.ok, false);
    assert.ok(report.blockers.some((b) => b.startsWith('gmail_credentials_unavailable')));
  });

  it('fails when label is missing', async () => {
    const report = await preflightMarketIntelIngestion({
      label: 'MARKET_INTEL',
      deps: {
        loadOAuthCredentials() { return { client_id: 'x', client_secret: 'y' }; },
        async createGmailClient() { return {}; },
        async listGmailLabels() {
          return [{ id: '1', name: 'INBOX' }, { id: '2', name: 'Other' }];
        },
      },
    });
    assert.equal(report.ok, false);
    assert.ok(report.blockers.some((b) => b.startsWith('gmail_label_missing')));
    assert.deepEqual(report.checks.label.availableLabelsSample, ['INBOX', 'Other']);
  });

  it('passes when auth, label, and discovery succeed', async () => {
    const report = await preflightMarketIntelIngestion({
      label: 'MARKET_INTEL',
      days: 30,
      limit: 10,
      requireMessages: true,
      deps: {
        loadOAuthCredentials() { return { client_id: 'x', client_secret: 'y' }; },
        async createGmailClient() { return { marker: true }; },
        async listGmailLabels() {
          return [{ id: 'Label_9', name: 'MARKET_INTEL', type: 'user' }];
        },
        async countMatchingMessages({ query, limit }) {
          assert.match(query, /label:MARKET_INTEL/);
          assert.equal(limit, 10);
          return {
            query,
            discoveredCount: 3,
            cappedByLimit: false,
            sampleIds: ['a', 'b', 'c'],
          };
        },
      },
    });
    assert.equal(report.ok, true);
    assert.equal(report.checks.discovery.discoveredCount, 3);
    assert.equal(report.blockers.length, 0);
  });

  it('requires messages when requireMessages is true', async () => {
    const report = await preflightMarketIntelIngestion({
      requireMessages: true,
      deps: {
        loadOAuthCredentials() { return {}; },
        async createGmailClient() { return {}; },
        async listGmailLabels() {
          return [{ id: 'Label_9', name: 'MARKET_INTEL' }];
        },
        async countMatchingMessages() {
          return { discoveredCount: 0, cappedByLimit: false, sampleIds: [] };
        },
      },
    });
    assert.equal(report.ok, false);
    assert.ok(report.blockers.some((b) => b.startsWith('gmail_label_empty')));
  });

  it('formats a human report', () => {
    const text = formatPreflightReport({
      ok: true,
      generatedAt: '2026-08-03T00:00:00.000Z',
      label: 'MARKET_INTEL',
      days: 365,
      query: 'label:MARKET_INTEL newer_than:365d',
      checks: {
        credentials: { ok: true },
        auth: { ok: true },
        label: { ok: true },
        discovery: { ok: true, discoveredCount: 12 },
      },
      blockers: [],
      warnings: [],
      nextActions: ['Run dry-run'],
    });
    assert.match(text, /Status: pass/);
    assert.match(text, /12 messages/);
  });
});

describe('preflightMarketIntelligence CLI args', () => {
  it('parses defaults and flags', () => {
    assert.deepEqual(parseArgs([]), {
      days: 365,
      label: 'MARKET_INTEL',
      limit: 1000,
      requireMessages: false,
      json: false,
      help: false,
    });
    assert.equal(parseArgs(['--require-messages', '--json']).requireMessages, true);
  });
});
