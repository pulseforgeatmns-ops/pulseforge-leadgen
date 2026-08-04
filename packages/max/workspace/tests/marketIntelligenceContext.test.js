'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  buildMarketIntelligenceContext,
  extractMarketSearchTerm,
  requestedPatternFields,
} = require('../MarketIntelligenceContext');
const {
  createWorkspaceEngine,
  EXECUTION_DOMAINS,
} = require('../../index');

function fakeMarketService() {
  return {
    async listMarketCompanies({ q }) {
      assert.equal(q, 'Apollo');
      return [
        {
          id: '11111111-1111-1111-1111-111111111111',
          name: 'Apollo',
          domain: 'apollo.io',
          emailsObserved: 2,
        },
      ];
    },
    async getCompanyProfile(id) {
      assert.equal(id, '11111111-1111-1111-1111-111111111111');
      return {
        companyId: id,
        companyName: 'Apollo',
        emailsObserved: 2,
        currentCta: 'book a demo',
        primaryPositioning: 'outbound_automation',
        latestDirection: 'AI SDR',
        evidenceRefs: ['email-1', 'email-2'],
      };
    },
    async getCompanyCampaignTimeline(id) {
      assert.equal(id, '11111111-1111-1111-1111-111111111111');
      return [
        {
          id: 'email-1',
          touch: 1,
          subject: 'Book a demo',
          receivedAt: '2026-01-01T00:00:00.000Z',
        },
      ];
    },
    async crossMarketPatterns({ field }) {
      return {
        field,
        patterns: [
          {
            value: field === 'positioning' ? 'outbound_automation' : 'book a demo',
            count: 3,
            evidenceRefs: ['email-1'],
          },
        ],
      };
    },
    async crossMarketSequenceStats() {
      return {
        companies: 4,
        medianSequenceLength: 2,
        medianFollowUpSpacingDays: 7,
      };
    },
  };
}

describe('SPEC-066 MarketIntelligenceContext', () => {
  it('extracts a market company search term and requested fields', () => {
    assert.equal(extractMarketSearchTerm('Monitor Apollo campaigns.'), 'Apollo');
    assert.deepEqual(
      requestedPatternFields('Show pricing and guarantee patterns'),
      ['pricing_mention', 'guarantee']
    );
  });

  it('hydrates Max with SPEC-065 evidence without recommendations', async () => {
    const context = await buildMarketIntelligenceContext(
      'Monitor Apollo positioning and CTAs.',
      { service: fakeMarketService() }
    );

    assert.equal(context.status, 'available');
    assert.equal(context.selectedCompany.name, 'Apollo');
    assert.equal(context.profile.currentCta, 'book a demo');
    assert.equal(context.patterns.length, 2);
    assert.deepEqual(context.unavailable, []);
  });

  it('workspace market turn consumes corpus and stays out of Mission', async () => {
    const workspace = createWorkspaceEngine({
      disableLlm: true,
      missionsEnabled: true,
      marketIntelligenceService: fakeMarketService(),
    });

    const result = await workspace.ask({
      question: 'Monitor Apollo positioning and CTAs.',
      context: { tenantId: '10', page: 'market' },
    });

    assert.equal(result.executionDomain, EXECUTION_DOMAINS.MARKET_INTELLIGENCE);
    assert.equal(result.route, 'intelligence');
    assert.equal(result.mission, null);
    assert.match(result.structured.answer, /Apollo has 2 observed market email/);
    assert.match(result.structured.answer, /Current CTA observed: book a demo/);
    assert.equal(result.structured.recommendedActions.length, 0);
    assert.ok(result.structured.supportingEvidence.some((e) => e.sourceType === 'market_intelligence'));
    assert.equal(result.structured.metadata.sourcesUsed.market, true);
  });
});
