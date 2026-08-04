'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  companyNameFromDomain,
  resolveMarketCompany,
} = require('../utils/marketCompanyResolve');
const {
  buildLabelQuery,
  extractLinks,
  parseFrom,
  parseGmailMessage,
} = require('../utils/marketEmailParse');
const { parseArgs } = require('../scripts/importMarketIntelligence');
const {
  DEFAULT_IMPORT_INTENT,
  IMPORT_INTENTS,
  formatImportReport,
  resolveImportIntent,
} = require('../services/marketIntelligenceIngestion');

describe('marketCompanyResolve', () => {
  it('maps apollo.io → Apollo', () => {
    assert.equal(companyNameFromDomain('apollo.io'), 'Apollo');
    assert.deepEqual(resolveMarketCompany({ fromEmail: 'hello@apollo.io' }), {
      domain: 'apollo.io',
      name: 'Apollo',
      isUnknown: false,
    });
  });

  it('strips marketing subdomains', () => {
    assert.equal(companyNameFromDomain('mail.hubspot.com'), 'Hubspot');
    assert.equal(companyNameFromDomain('news.stripe.com'), 'Stripe');
  });

  it('returns Unknown Company when domain is missing', () => {
    assert.deepEqual(resolveMarketCompany({ fromEmail: 'not-an-email' }), {
      domain: null,
      name: 'Unknown Company',
      isUnknown: true,
    });
  });
});

describe('marketEmailParse', () => {
  it('builds a label + lookback Gmail query', () => {
    assert.equal(buildLabelQuery({ label: 'MARKET_INTEL', days: 365 }), 'label:MARKET_INTEL newer_than:365d');
    assert.equal(buildLabelQuery({ label: 'OTHER', days: 30 }), 'label:OTHER newer_than:30d');
  });

  it('parses From into name + email', () => {
    assert.deepEqual(parseFrom('Apollo <hello@apollo.io>'), {
      fromName: 'Apollo',
      fromEmail: 'hello@apollo.io',
    });
  });

  it('extracts links from text', () => {
    const links = extractLinks('See https://apollo.io/pricing and <https://apollo.io/demo>.');
    assert.ok(links.includes('https://apollo.io/pricing'));
    assert.ok(links.includes('https://apollo.io/demo'));
  });

  it('preserves raw evidence from a Gmail full message', () => {
    const plain = Buffer.from('Touch one body https://apollo.io/x', 'utf8')
      .toString('base64')
      .replace(/\+/g, '-')
      .replace(/\//g, '_');
    const parsed = parseGmailMessage({
      id: 'gmail-1',
      threadId: 'thread-1',
      internalDate: '1714809600000',
      payload: {
        headers: [
          { name: 'From', value: 'Apollo <hello@apollo.io>' },
          { name: 'Subject', value: 'Week 1 sequence' },
          { name: 'Date', value: 'Sat, 4 May 2024 12:00:00 +0000' },
          { name: 'Message-ID', value: '<msg-1@apollo.io>' },
        ],
        mimeType: 'text/plain',
        body: { data: plain },
      },
    }, { importedAt: new Date('2026-08-01T00:00:00.000Z') });

    assert.equal(parsed.gmailId, 'gmail-1');
    assert.equal(parsed.threadId, 'thread-1');
    assert.equal(parsed.messageId, '<msg-1@apollo.io>');
    assert.equal(parsed.subject, 'Week 1 sequence');
    assert.equal(parsed.fromEmail, 'hello@apollo.io');
    assert.match(parsed.bodyText, /Touch one body/);
    assert.ok(parsed.links.includes('https://apollo.io/x'));
    assert.equal(parsed.headers.Subject, 'Week 1 sequence');
    assert.equal(parsed.receivedAt.toISOString(), '2024-05-04T08:00:00.000Z');
  });
});

describe('import intent helpers', () => {
  it('defaults to general_market_messaging and treats sourceIntent as alias', () => {
    assert.equal(resolveImportIntent({}), DEFAULT_IMPORT_INTENT);
    assert.equal(
      resolveImportIntent({ sourceIntent: 'competitive_watch' }),
      IMPORT_INTENTS.COMPETITIVE_WATCH
    );
    assert.equal(
      resolveImportIntent({ importIntent: 'general_market_messaging', sourceIntent: 'general_market_messaging' }),
      IMPORT_INTENTS.GENERAL_MARKET_MESSAGING
    );
    assert.equal(resolveImportIntent({ importIntent: 'vendor_newsletter' }), IMPORT_INTENTS.VENDOR_NEWSLETTER);
    assert.equal(resolveImportIntent({ importIntent: 'direct_competitor' }), IMPORT_INTENTS.DIRECT_COMPETITOR);
    assert.equal(resolveImportIntent({ importIntent: 'indirect_competitor' }), IMPORT_INTENTS.INDIRECT_COMPETITOR);
    assert.equal(resolveImportIntent({ importIntent: 'unknown' }), IMPORT_INTENTS.UNKNOWN);
  });

  it('rejects conflicting import/source intents', () => {
    assert.throws(
      () => resolveImportIntent({
        importIntent: 'general_market_messaging',
        sourceIntent: 'competitive_watch',
      }),
      /Conflicting intents/
    );
  });

  it('rejects intents outside the SPEC-068 allowlist', () => {
    assert.throws(
      () => resolveImportIntent({ importIntent: 'rival_confirmed' }),
      /Allowed: general_market_messaging, competitive_watch, vendor_newsletter, direct_competitor, indirect_competitor, unknown/
    );
    assert.throws(
      () => resolveImportIntent({ importIntent: 'rival_confirmed' }),
      /acquisition context only/
    );
  });
});

describe('importMarketIntelligence CLI', () => {
  it('parses options with defaults', () => {
    const defaults = parseArgs([]);
    assert.equal(defaults.days, 365);
    assert.equal(defaults.label, 'MARKET_INTEL');
    assert.equal(defaults.limit, 1000);
    assert.equal(defaults.dryRun, false);
    assert.equal(defaults.json, false);
    assert.equal(defaults.preflight, false);
    assert.equal(defaults.skipPreflight, false);
    assert.equal(defaults.help, false);
    assert.equal(defaults.importIntent, null);
    assert.equal(defaults.sourceIntent, null);
    assert.equal(defaults.tokenSource, null);
    assert.equal(defaults.resolvedIntent, DEFAULT_IMPORT_INTENT);
    assert.ok(['gmail', 'auto'].includes(defaults.resolvedTokenSource));

    assert.equal(parseArgs(['--days=30', '--label=OTHER', '--limit=5', '--dry-run']).dryRun, true);
    assert.equal(parseArgs(['--preflight']).preflight, true);
    assert.equal(parseArgs(['--skip-preflight']).skipPreflight, true);
    assert.equal(
      parseArgs(['--intent=competitive_watch']).resolvedIntent,
      IMPORT_INTENTS.COMPETITIVE_WATCH
    );
    assert.equal(
      parseArgs(['--source-intent=general_market_messaging']).resolvedIntent,
      IMPORT_INTENTS.GENERAL_MARKET_MESSAGING
    );
    assert.equal(parseArgs(['--token-source=gmail']).resolvedTokenSource, 'gmail');
    assert.equal(parseArgs(['--token-source=riley']).resolvedTokenSource, 'riley');
    assert.equal(parseArgs(['--token-source=auto']).resolvedTokenSource, 'auto');
  });

  it('formats the operator report including intent and unknown-company rate', () => {
    const report = formatImportReport({
      imported: 1482,
      skipped: 53,
      duplicates: 12,
      unknownCompany: 97,
      unknownCompanyRatePct: 6.5,
      importIntent: 'general_market_messaging',
      durationSeconds: 48,
    });
    assert.match(report, /Import intent: general_market_messaging/);
    assert.match(report, /Imported: 1,482/);
    assert.match(report, /Skipped: 53/);
    assert.match(report, /Duplicates: 12/);
    assert.match(report, /Unknown Company: 97 \(6\.5%\)/);
    assert.match(report, /Duration: 48s/);
  });
});

describe('gmailClient label helpers', () => {
  const { findLabelByName } = require('../utils/gmailClient');

  it('matches exact Gmail label names', () => {
    const labels = [
      { id: '1', name: 'INBOX' },
      { id: '2', name: 'MARKET_INTEL' },
    ];
    assert.equal(findLabelByName(labels, 'MARKET_INTEL').id, '2');
    assert.equal(findLabelByName(labels, 'market_intel'), null);
  });
});