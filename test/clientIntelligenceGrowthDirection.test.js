'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  buildInitialGrowthDirection,
  buildGrowthConversationOpening,
  buildGrowthConversationReply,
  detectGrowthConversationIntent,
  buildSegmentRanking,
  DIRECTIONAL_LABEL,
  SEGMENT_RANKING_KIND,
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
        growth_focus:
          'recurring commercial cleaning; customers who need weekly or multiple-times-per-week service',
        ideal_customers: [
          'property managers',
          'short-term rental companies',
          'facility managers',
          'professional offices',
          'daycares',
          'rec centers',
          'high-traffic buildings',
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
    assert.match(gd.heading, /Anchor'?s first growth focus/i);
    assert.match(gd.firstFocus, /recurring commercial cleaning/i);
    assert.ok(gd.paragraphs.length >= 3 && gd.paragraphs.length <= 5);

    const lead = gd.paragraphs[0];
    assert.match(
      lead,
      /Based on this Blueprint, Anchor'?s first growth focus should be recurring commercial cleaning in Greater Manchester/i
    );
    assert.match(
      lead,
      /especially for customers who need consistent service weekly or multiple times per week\./i
    );
    assert.doesNotMatch(
      lead,
      /weekly or multiple times per week in Greater Manchester/i
    );

    assert.match(
      gd.paragraphs[1],
      /follows directly from the approved Blueprint:/i
    );
    assert.match(gd.paragraphs[1], /directional read, not market validation/i);

    const segmentsPara = gd.paragraphs[2];
    assert.match(segmentsPara, /first segments worth comparing are/i);
    assert.match(
      segmentsPara,
      /across Greater Manchester, especially Bedford, Hooksett, Londonderry, Auburn, and Goffstown/i
    );
    assert.equal(
      (segmentsPara.match(/Greater Manchester/gi) || []).length,
      1,
      'segments paragraph should not repeat Greater Manchester'
    );

    assert.match(gd.paragraphs.join('\n'), /who Anchor should avoid/i);
    assert.match(
      gd.paragraphs[gd.paragraphs.length - 1],
      /next conversation should turn this directional read into a focused growth plan/i
    );

    assert.ok(gd.segmentsToInspect.some((s) => /property managers/i.test(s)));
    assert.ok(gd.marketsToInspect.some((m) => /Greater Manchester/i.test(m)));
    const blob = gd.paragraphs.join(' ');
    assert.doesNotMatch(
      blob,
      /campaign sequence|here is your prospect list|I validated|market is validated/i
    );
    assert.doesNotMatch(blob, /First, Max would inspect/i);
    assert.doesNotMatch(blob, /Greater Manchester \(/i);
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
    assert.equal(reply.segmentRanking, null);
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

describe('Growth Conversation segment ranking', () => {
  function anchorGrowthDirection() {
    return buildInitialGrowthDirection(ANCHOR_BLUEPRINT, {
      normalizedFacts: {
        business_name: 'Anchor Cleaning',
        growth_focus:
          'recurring commercial cleaning; customers who need weekly or multiple-times-per-week service',
        ideal_customers: [
          'property managers',
          'short-term rental companies',
          'facility managers',
          'professional offices',
          'daycares',
          'rec centers',
          'high-traffic buildings',
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
  }

  it('detects rank / compare / prioritize / directional-call intents', () => {
    assert.equal(
      detectGrowthConversationIntent('Please rank the segments'),
      'rank_segments'
    );
    assert.equal(
      detectGrowthConversationIntent('Please compare the segments and give me the best first'),
      'compare_segments'
    );
    assert.equal(
      detectGrowthConversationIntent('prioritize these segments for me'),
      'prioritize_segments'
    );
    assert.equal(
      detectGrowthConversationIntent('Make a directional call on where we should start'),
      'make_directional_call'
    );
    assert.equal(
      detectGrowthConversationIntent('which segment should we test first'),
      'choose_first_segment'
    );
    assert.equal(
      detectGrowthConversationIntent('Let us dig into the segments first.'),
      'dig_segments'
    );
  });

  it('user request containing "rank" produces Segment Ranking, not the generic mix prompt', () => {
    const gd = anchorGrowthDirection();
    const reply = buildGrowthConversationReply(
      'Please compare the segments and rank them. Give me:\n1. Best first segment to test\n2. Second-best segment\n3. Segment to keep warm but not prioritize yet\n4. Segment to avoid for now',
      gd,
      ANCHOR_BLUEPRINT
    );
    assert.equal(reply.intent, 'rank_segments');

    assert.ok(RANKING_INTENTS_INCLUDES(reply.intent));
    assert.equal(reply.segmentRanking.kind, SEGMENT_RANKING_KIND);
    assert.match(reply.message, /^Segment Ranking/m);
    assert.doesNotMatch(
      reply.message,
      /From the Blueprint, the segments worth comparing first are/i
    );
    assert.doesNotMatch(
      reply.message,
      /Still directional — next we can bound the market/i
    );

    assert.match(reply.message, /Best first segment to test:\s*Property managers/i);
    assert.match(reply.message, /Second-best segment:\s*Professional offices/i);
    assert.match(reply.message, /Keep warm:\s*Short-term rental companies/i);
    assert.match(reply.message, /Avoid for now:\s*Rec centers or broad high-traffic buildings/i);
    assert.match(reply.message, /Directional recommendation:/i);
    assert.match(
      reply.message,
      /Start with property managers, while testing professional offices as a secondary path/i
    );
    assert.match(reply.message, /Confidence:/i);
    assert.match(
      reply.message,
      /Directional, not market-validated|not market-validated/i
    );
    assert.doesNotMatch(
      reply.message,
      /prospect list|campaign copy|email sequence|here is your campaign/i
    );
  });

  it('buildSegmentRanking returns structured slots with why and cautions', () => {
    const gd = anchorGrowthDirection();
    const ranking = buildSegmentRanking(gd, ANCHOR_BLUEPRINT, {
      intent: 'rank_segments',
    });
    assert.equal(ranking.kind, SEGMENT_RANKING_KIND);
    assert.equal(ranking.marketValidated, false);
    const byRole = Object.fromEntries(
      ranking.rankings.map((r) => [r.role, r])
    );
    assert.equal(byRole.best_first.segment, 'property managers');
    assert.ok(byRole.best_first.why.length >= 2);
    assert.ok(byRole.best_first.cautions.length >= 1);
    assert.equal(byRole.second_best.segment, 'professional offices');
    assert.equal(byRole.keep_warm.segment, 'short-term rental companies');
    assert.match(byRole.avoid_for_now.displaySegment, /rec centers/i);
    assert.match(ranking.directionalRecommendation, /property managers/i);
    assert.match(ranking.confidence, /directional|not market-validated/i);
  });

  it('postGrowthMessage rank request returns ranking artifact end-to-end', async () => {
    const store = createMemoryStore();
    const opts = { store, useMemoryPlaybookStore: true };
    const started = await startClientInterview({ clientId: 213 }, opts);
    let turn = started;
    for (const a of ANSWERS) {
      turn = await postInterviewMessage(started.interviewId, a, opts);
    }
    await approveBlueprint(turn.blueprint.id, opts);
    await startGrowthConversation(started.interviewId, opts);

    const reply = await postGrowthMessage(
      started.interviewId,
      'Please rank the segments and make a directional call.',
      opts
    );
    assert.match(reply.message, /Segment Ranking/i);
    assert.ok(reply.segmentRanking);
    assert.equal(reply.segmentRanking.kind, SEGMENT_RANKING_KIND);
    assert.match(reply.message, /Best first segment to test/i);
    assert.match(reply.message, /Directional recommendation/i);
    assert.match(reply.message, /not market-validated|directional/i);
    assert.doesNotMatch(
      reply.message,
      /From the Blueprint, the segments worth comparing first are/i
    );
    assert.doesNotMatch(
      reply.message,
      /Still directional — next we can bound the market/i
    );
    assert.doesNotMatch(
      reply.message,
      /prospect list|campaign is live|here is your campaign copy/i
    );
  });
});

function RANKING_INTENTS_INCLUDES(intent) {
  return [
    'rank_segments',
    'compare_segments',
    'choose_first_segment',
    'make_directional_call',
    'prioritize_segments',
  ].includes(intent);
}
