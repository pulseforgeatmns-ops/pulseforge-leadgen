'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  buildInitialGrowthDirection,
  buildGrowthConversationOpening,
  DIRECTIONAL_LABEL,
} = require('../services/clientIntelligenceGrowthDirection');
const {
  createMemoryStore,
  startClientInterview,
  postInterviewMessage,
  approveBlueprint,
  startGrowthConversation,
  postGrowthMessage,
  getInterview,
} = require('../services/clientIntelligenceInterview');

const ANCHOR_BLUEPRINT = {
  id: 'bp-anchor',
  version: '1.0',
  status: 'approved',
  sections: {
    identity: {
      summary:
        'Anchor Cleaning is a commercial-focused cleaning company. This identity framing is how the operator describes the business today.',
      confidence: 0.9,
      evidenceIds: [],
      unknowns: [],
    },
    services: {
      summary:
        'Today the business delivers commercial cleaning for professional offices. The strongest growth focus is recurring commercial cleaning for customers who need weekly or multiple-times-per-week service.',
      confidence: 0.9,
      evidenceIds: [],
      unknowns: [],
    },
    idealCustomers: {
      summary:
        'Ideal customers are property managers, short-term rental companies, facility managers, professional offices, rec centers, and high-traffic buildings.',
      confidence: 0.88,
      evidenceIds: [],
      unknowns: [],
    },
    avoidCustomers: {
      summary: 'Customers who prioritize the lowest price over reliability.',
      confidence: 0.8,
      evidenceIds: [],
      unknowns: [],
    },
    targetMarkets: {
      summary:
        'Priority markets center on Greater Manchester, including Bedford, Londonderry, Auburn, Goffstown, and Hooksett.',
      confidence: 0.9,
      evidenceIds: [],
      unknowns: [],
    },
    competitiveAdvantages: {
      summary:
        'Customers choose Anchor because they trust the work will be done right without needing to chase the team.',
      confidence: 0.85,
      evidenceIds: [],
      unknowns: [],
    },
    brandVoice: {
      summary: 'Calm, professional, reliable, and easy to work with.',
      confidence: 0.8,
      evidenceIds: [],
      unknowns: [],
    },
    campaignGoals: {
      summary:
        'Near-term growth goals focus on commercial cleaning growth in Greater Manchester.',
      confidence: 0.85,
      evidenceIds: [],
      unknowns: [],
    },
    successMetrics: {
      summary: 'A clearer path to commercial opportunities over the next 90 days.',
      confidence: 0.8,
      evidenceIds: [],
      unknowns: [],
    },
  },
};

const ANSWERS = [
  'Aji Home Services',
  'Residential cleaning',
  'Homeowners',
  'Warehouses',
  'Myrtle Beach',
  'Reliable crews',
  'Friendly',
  'More appointments',
  'Booked jobs',
];

describe('Initial Growth Direction artifact', () => {
  it('builds a directional Anchor-style preview from Blueprint facts only', () => {
    const gd = buildInitialGrowthDirection(ANCHOR_BLUEPRINT, {
      normalizedFacts: {
        business_name: 'Anchor Cleaning',
        growth_focus: 'recurring commercial cleaning',
        ideal_customers: [
          'property managers',
          'facility managers',
          'professional offices',
        ],
        geography: [
          'Greater Manchester',
          'Bedford',
          'Hooksett',
          'Londonderry',
          'Auburn',
          'Goffstown',
        ],
      },
    });

    assert.equal(gd.kind, 'initial_growth_direction');
    assert.equal(gd.title, 'Initial Growth Direction');
    assert.equal(gd.directional, true);
    assert.equal(gd.disclaimer, DIRECTIONAL_LABEL);
    assert.match(gd.heading, /Anchor/i);
    assert.match(gd.firstFocus, /recurring commercial cleaning/i);
    assert.ok(gd.paragraphs.length >= 3 && gd.paragraphs.length <= 5);
    assert.match(gd.paragraphs[0], /Based on this Blueprint/i);
    assert.match(gd.paragraphs[0], /Greater Manchester/i);
    assert.ok(gd.segmentsToInspect.some((s) => /property managers/i.test(s)));
    assert.ok(gd.marketsToInspect.some((m) => /Greater Manchester|Bedford/i.test(m)));
    assert.match(gd.nextConversationPreview, /growth conversation/i);
    const blob = gd.paragraphs.join(' ');
    assert.doesNotMatch(blob, /campaign sequence|here is your prospect list|I validated|market is validated/i);
    assert.match(blob, /not from market validation|has not done yet/i);
  });

  it('growth conversation opening stays pre-strategy', () => {
    const gd = buildInitialGrowthDirection(ANCHOR_BLUEPRINT);
    const opening = buildGrowthConversationOpening(gd);
    assert.match(opening, /approved Blueprint/i);
    assert.match(opening, /Directional first focus/i);
    assert.doesNotMatch(opening, /here is your prospect list|launching campaign/i);
  });
});

describe('approve → Initial Growth Direction → Growth Conversation', () => {
  it('approve returns initialGrowthDirection and getInterview exposes it', async () => {
    const store = createMemoryStore();
    const opts = { store, useMemoryPlaybookStore: true };
    const started = await startClientInterview({ clientId: 210 }, opts);
    let turn = started;
    for (const a of ANSWERS) {
      turn = await postInterviewMessage(started.interviewId, a, opts);
    }
    const result = await approveBlueprint(turn.blueprint.id, opts);
    assert.equal(result.alreadyApproved, false);
    assert.ok(result.initialGrowthDirection);
    assert.equal(result.initialGrowthDirection.kind, 'initial_growth_direction');
    assert.ok(result.initialGrowthDirection.paragraphs.length >= 3);

    const detail = await getInterview(started.interviewId, opts);
    assert.ok(detail.initialGrowthDirection);
    assert.equal(
      detail.initialGrowthDirection.firstFocus,
      result.initialGrowthDirection.firstFocus
    );
  });

  it('Start Growth Conversation uses approved Blueprint as context', async () => {
    const store = createMemoryStore();
    const opts = { store, useMemoryPlaybookStore: true };
    const started = await startClientInterview({ clientId: 211 }, opts);
    let turn = started;
    for (const a of ANSWERS) {
      turn = await postInterviewMessage(started.interviewId, a, opts);
    }
    await approveBlueprint(turn.blueprint.id, opts);

    const growth = await startGrowthConversation(started.interviewId, opts);
    assert.equal(growth.status, 'GROWTH_CONVERSATION');
    assert.match(growth.message, /approved Blueprint/i);
    assert.ok(growth.initialGrowthDirection);
    assert.equal(growth.blueprint.status, 'approved');

    const reply = await postGrowthMessage(
      started.interviewId,
      'Let us dig into the segments first.',
      opts
    );
    assert.equal(reply.status, 'GROWTH_CONVERSATION');
    assert.match(reply.message, /segment/i);
    assert.doesNotMatch(reply.message, /I built a prospect list|campaign is live/i);
  });

  it('idempotent approve still returns growth direction', async () => {
    const store = createMemoryStore();
    const opts = { store, useMemoryPlaybookStore: true };
    const started = await startClientInterview({ clientId: 212 }, opts);
    let turn = started;
    for (const a of ANSWERS) {
      turn = await postInterviewMessage(started.interviewId, a, opts);
    }
    const first = await approveBlueprint(turn.blueprint.id, opts);
    const again = await approveBlueprint(turn.blueprint.id, opts);
    assert.equal(again.alreadyApproved, true);
    assert.ok(again.initialGrowthDirection);
    assert.equal(
      again.initialGrowthDirection.firstFocus,
      first.initialGrowthDirection.firstFocus
    );
  });
});
