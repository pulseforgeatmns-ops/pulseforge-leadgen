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
const { formatImportReport } = require('../services/marketIntelligenceIngestion');

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

describe('importMarketIntelligence CLI', () => {
  it('parses options with defaults', () => {
    assert.deepEqual(parseArgs([]), {
      days: 365,
      label: 'MARKET_INTEL',
      limit: 1000,
      dryRun: false,
      json: false,
    });
    assert.equal(parseArgs(['--days=30', '--label=OTHER', '--limit=5', '--dry-run']).dryRun, true);
  });

  it('formats the operator report', () => {
    const report = formatImportReport({
      imported: 1482,
      skipped: 53,
      duplicates: 12,
      unknownCompany: 97,
      durationSeconds: 48,
    });
    assert.match(report, /Imported: 1,482/);
    assert.match(report, /Skipped: 53/);
    assert.match(report, /Duplicates: 12/);
    assert.match(report, /Unknown Company: 97/);
    assert.match(report, /Duration: 48s/);
  });
});
