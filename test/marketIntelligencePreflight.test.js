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
      tokenSource: 'gmail',
      deps: {
        loadOAuthCredentials() { return { client_id: 'x', client_secret: 'y' }; },
        async createGmailClient(opts) {
          assert.equal(opts.tokenSource, 'gmail');
          return { marker: true };
        },
        async getGmailProfile() {
          return { emailAddress: 'jake@example.com' };
        },
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
    assert.equal(report.authenticatedEmail, 'jake@example.com');
    assert.equal(report.tokenSource, 'gmail');
  });

  it('resolves authenticatedEmail before label discovery', async () => {
    const order = [];
    const seen = [];
    const report = await preflightMarketIntelIngestion({
      showAccount: true,
      tokenSource: 'gmail',
      deps: {
        loadOAuthCredentials() { return { client_id: 'x', client_secret: 'y' }; },
        async createGmailClient() { return {}; },
        async getGmailProfile() {
          order.push('profile');
          return { emailAddress: 'ops@example.com' };
        },
        async listGmailLabels() {
          order.push('labels');
          return [{ id: 'Label_9', name: 'MARKET_INTEL' }];
        },
        async countMatchingMessages() {
          order.push('discovery');
          return { discoveredCount: 1, cappedByLimit: false, sampleIds: ['m1'] };
        },
      },
      onAuthenticatedAccount(email) {
        seen.push(email);
        order.push('callback');
      },
    });
    assert.equal(report.ok, true);
    assert.equal(report.authenticatedEmail, 'ops@example.com');
    assert.deepEqual(seen, ['ops@example.com']);
    assert.deepEqual(order, ['profile', 'callback', 'labels', 'discovery']);
  });

  it('keeps profile failures diagnostic-only', async () => {
    const report = await preflightMarketIntelIngestion({
      showAccount: true,
      deps: {
        loadOAuthCredentials() { return {}; },
        async createGmailClient() { return {}; },
        async getGmailProfile() {
          throw new Error('profile denied');
        },
        async listGmailLabels() {
          return [{ id: 'Label_9', name: 'MARKET_INTEL' }];
        },
        async countMatchingMessages() {
          return { discoveredCount: 2, cappedByLimit: false, sampleIds: ['a'] };
        },
      },
    });
    assert.equal(report.ok, true);
    assert.equal(report.authenticatedEmail, null);
    assert.ok(report.warnings.some((w) => w.startsWith('gmail_profile_unavailable')));
  });

  it('requires messages when requireMessages is true', async () => {
    const report = await preflightMarketIntelIngestion({
      requireMessages: true,
      deps: {
        loadOAuthCredentials() { return {}; },
        async createGmailClient() { return {}; },
        async getGmailProfile() { return { emailAddress: 'x@y.com' }; },
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
      tokenSource: 'gmail',
      authenticatedEmail: 'ops@example.com',
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
    assert.match(text, /Token source: gmail/);
    assert.match(text, /Authenticated account: ops@example.com/);
    assert.match(text, /12 messages/);
  });
});

describe('preflightMarketIntelligence CLI args', () => {
  it('parses defaults and flags including token-source', () => {
    const defaults = parseArgs([]);
    assert.equal(defaults.days, 365);
    assert.equal(defaults.label, 'MARKET_INTEL');
    assert.equal(defaults.limit, 1000);
    assert.equal(defaults.requireMessages, false);
    assert.equal(defaults.showAccount, false);
    assert.equal(defaults.tokenSource, null);
    assert.ok(['gmail', 'auto'].includes(defaults.resolvedTokenSource));
    assert.equal(defaults.json, false);
    assert.equal(defaults.help, false);

    assert.equal(parseArgs(['--require-messages', '--json']).requireMessages, true);
    assert.equal(parseArgs(['--show-account']).showAccount, true);
    assert.equal(parseArgs(['--token-source=gmail']).resolvedTokenSource, 'gmail');
    assert.equal(parseArgs(['--token-source=riley']).resolvedTokenSource, 'riley');
    assert.equal(parseArgs(['--token-source=auto']).resolvedTokenSource, 'auto');
    assert.throws(() => parseArgs(['--token-source=personal']), /Invalid tokenSource/);
  });
});
