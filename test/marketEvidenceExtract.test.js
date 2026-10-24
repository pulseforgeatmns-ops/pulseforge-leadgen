'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  extractMarketEvidence,
  clipQuote,
  countImgTags,
} = require('../utils/marketEvidenceExtract');
const { parseArgs } = require('../scripts/extractMarketIntelligence');
const { formatExtractReport } = require('../services/marketIntelligenceExtraction');

describe('marketEvidenceExtract', () => {
  it('extracts quote-backed CTA, offer, pricing, and positioning', () => {
    const rows = extractMarketEvidence(
      {
        subject: 'Book a demo this week',
        bodyText:
          'Our outbound automation helps teams. Start a free trial from $99/mo. Trusted by 10,000+ companies. Money-back guarantee.',
        fromName: 'Sam Founder',
        fromEmail: 'sam@apollo.io',
        links: ['https://apollo.io/demo'],
        receivedAt: '2026-01-15T12:00:00.000Z',
        headers: {},
      },
      {
        companyName: 'Apollo',
        companyDomain: 'apollo.io',
        sequencePosition: 2,
      }
    );

    const byField = Object.fromEntries(rows.map((r) => [r.field, r]));
    assert.equal(byField.company.valueText, 'Apollo');
    assert.equal(byField.sender.valueText, 'sam@apollo.io');
    assert.equal(byField.role.valueText, 'founder');
    assert.equal(byField.sequence_position.valueText, '2');
    assert.match(byField.cta.evidenceQuote, /book a demo/i);
    assert.ok(byField.cta.evidenceQuote.length > 0);
    assert.ok(byField.offer);
    assert.ok(byField.pricing_mention);
    assert.ok(byField.guarantee);
    assert.ok(byField.social_proof);
    assert.equal(byField.positioning.valueText, 'outbound_automation');
    assert.equal(byField.headline.evidencePath, 'subject');
  });

  it('omits messaging fields when no verbatim quote exists', () => {
    const rows = extractMarketEvidence({
      subject: 'Hello',
      bodyText: 'Just checking in about your operations.',
      fromEmail: 'hi@example.com',
      links: [],
      headers: {},
    });
    const fields = new Set(rows.map((r) => r.field));
    assert.equal(fields.has('offer'), false);
    assert.equal(fields.has('pricing_mention'), false);
    assert.equal(fields.has('guarantee'), false);
    assert.equal(fields.has('urgency'), false);
    assert.equal(fields.has('positioning'), false);
  });

  it('flags reply indicators and personalization signals', () => {
    const rows = extractMarketEvidence({
      subject: 'Re: following up',
      bodyText: 'Saw your LinkedIn post and that you are hiring SDRs. {{first_name}}',
      fromEmail: 'rep@vendor.com',
      headers: { 'In-Reply-To': '<abc@vendor.com>' },
      links: [],
    });
    const signals = rows.filter((r) => r.field === 'signal').map((r) => r.valueText);
    assert.ok(rows.some((r) => r.field === 'reply_indicator'));
    assert.ok(signals.includes('linkedin_mention'));
    assert.ok(signals.includes('hiring_mention'));
    assert.ok(signals.includes('generic_merge_field'));
    assert.equal(signals.includes('none'), false);
  });

  it('emits personalization none when no markers', () => {
    const rows = extractMarketEvidence({
      subject: 'Quick note',
      bodyText: 'Hope you are well.',
      fromEmail: 'a@b.com',
      links: [],
      headers: {},
    });
    const signals = rows.filter((r) => r.category === 'personalization');
    assert.equal(signals.length, 1);
    assert.equal(signals[0].valueText, 'none');
  });

  it('detects image-heavy HTML format', () => {
    const html = '<img/><img/><img/><p>Hi</p>';
    assert.equal(countImgTags(html), 3);
    const rows = extractMarketEvidence({
      subject: 'Visual',
      bodyText: 'Hi',
      bodyHtml: html,
      fromEmail: 'a@b.com',
      links: [],
      headers: {},
    });
    const format = rows.find((r) => r.field === 'format_style');
    assert.equal(format.valueText, 'image_heavy');
    assert.equal(format.evidencePath, 'body_html');
  });

  it('clips long quotes', () => {
    const long = 'x'.repeat(300);
    assert.ok(clipQuote(long, 50).endsWith('…'));
    assert.ok(clipQuote(long, 50).length <= 50);
  });
});

describe('extractMarketIntelligence CLI', () => {
  it('parses flags', () => {
    const options = parseArgs([
      '--limit=25',
      '--dry-run',
      '--company-id=abc',
      '--email-id=def',
      '--no-rebuild-profiles',
    ]);
    assert.equal(options.limit, 25);
    assert.equal(options.dryRun, true);
    assert.equal(options.companyId, 'abc');
    assert.equal(options.emailId, 'def');
    assert.equal(options.rebuildProfiles, false);
  });

  it('formats extract report', () => {
    const report = formatExtractReport({
      dryRun: true,
      scanned: 10,
      extracted: 9,
      observations: 40,
      failed: 1,
      profilesRebuilt: 0,
      durationSeconds: 2,
    });
    assert.match(report, /Mode: dry-run/);
    assert.match(report, /Observations: 40/);
  });
});
