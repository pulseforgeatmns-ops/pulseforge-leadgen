'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  buildInitialGrowthDirection,
  buildGrowthConversationOpening,
  buildGrowthConversationReply,
  detectGrowthConversationIntent,
  buildSegmentRanking,
  buildValidationTarget,
  DIRECTIONAL_LABEL,
  SEGMENT_RANKING_KIND,
  VALIDATION_TARGET_KIND,
  VALIDATION_TARGET_INTENT,
  SELECT_PRIMARY_INTENT,
  FIRST_SEGMENT_DECISION_KIND,
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

const GOOD_FIRST_WIN_MESSAGE = `Let's define what a good first win looks like before building any campaign or prospect list.

For the property manager segment, I want to know:
- What type of property manager is the best first test?
- What size or type of property should Anchor pursue first?
- What pain point should the first message focus on?
- What proof would make Anchor credible enough to consider?
- What early signals would tell us this segment is worth continuing?
- What would count as a successful first 30 days of validation?

Keep this directional and practical. I'm not asking for outreach copy or a prospect list yet. I want the validation target first.`;

describe('Growth Conversation define_validation_target', () => {
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

  it('detects good-first-win / validation-target phrases as define_validation_target', () => {
    assert.equal(
      detectGrowthConversationIntent(GOOD_FIRST_WIN_MESSAGE),
      VALIDATION_TARGET_INTENT
    );
    assert.equal(
      detectGrowthConversationIntent(
        'Define the validation target for property managers'
      ),
      VALIDATION_TARGET_INTENT
    );
    assert.equal(
      detectGrowthConversationIntent(
        'What early signals would tell us this is worth continuing?'
      ),
      VALIDATION_TARGET_INTENT
    );
    assert.equal(
      detectGrowthConversationIntent(
        'What would count as a successful first 30 days?'
      ),
      VALIDATION_TARGET_INTENT
    );
    assert.equal(
      detectGrowthConversationIntent(
        'I am not asking for outreach copy — give me the first win criteria'
      ),
      VALIDATION_TARGET_INTENT
    );
    // Ranking still wins when the user explicitly asks to rank, without
    // validation-target phrasing.
    assert.equal(
      detectGrowthConversationIntent('Please rank the segments'),
      'rank_segments'
    );
  });

  it('after Segment Ranking, a good-first-win request does not repeat Segment Ranking', () => {
    const gd = anchorGrowthDirection();
    const rankingReply = buildGrowthConversationReply(
      'Please rank the segments and make a directional call.',
      gd,
      ANCHOR_BLUEPRINT
    );
    assert.equal(rankingReply.segmentRanking.kind, SEGMENT_RANKING_KIND);

    const reply = buildGrowthConversationReply(
      GOOD_FIRST_WIN_MESSAGE,
      gd,
      ANCHOR_BLUEPRINT,
      { priorSegmentRanking: rankingReply.segmentRanking }
    );

    assert.equal(reply.intent, VALIDATION_TARGET_INTENT);
    assert.equal(reply.segmentRanking, null);
    assert.ok(reply.validationTarget);
    assert.equal(reply.validationTarget.kind, VALIDATION_TARGET_KIND);
    assert.match(reply.message, /^Property Manager Validation Target/m);
    assert.doesNotMatch(reply.message, /^Segment Ranking/m);
    assert.doesNotMatch(reply.message, /Best first segment to test/i);
    assert.doesNotMatch(reply.message, /Directional recommendation:/i);
  });

  it('validation target includes type, size, pain point, proof, signals, 30-day criteria, cautions', () => {
    const gd = anchorGrowthDirection();
    const prior = buildSegmentRanking(gd, ANCHOR_BLUEPRINT, {
      intent: 'rank_segments',
    });
    const target = buildValidationTarget(gd, ANCHOR_BLUEPRINT, {
      intent: VALIDATION_TARGET_INTENT,
      userMessage: GOOD_FIRST_WIN_MESSAGE,
      priorSegmentRanking: prior,
    });
    const reply = buildGrowthConversationReply(
      GOOD_FIRST_WIN_MESSAGE,
      gd,
      ANCHOR_BLUEPRINT,
      { priorSegmentRanking: prior }
    );

    assert.equal(target.focusSegment, 'property managers');
    assert.match(reply.message, /Best first property manager type/i);
    assert.match(
      reply.message,
      /Small to mid-sized local property managers/i
    );
    assert.match(reply.message, /Property size\/type to pursue first/i);
    assert.match(reply.message, /weekly or multiple times per week/i);
    assert.match(reply.message, /First pain point to test/i);
    assert.match(reply.message, /Reliability and responsiveness/i);
    assert.match(reply.message, /Credibility proof/i);
    assert.match(reply.message, /commercial cleaning checklist/i);
    assert.match(reply.message, /Early signals worth continuing/i);
    assert.match(reply.message, /agrees to a walkthrough/i);
    assert.match(reply.message, /Successful first 30 days of validation/i);
    assert.match(reply.message, /walkthrough or estimate request/i);
    assert.match(reply.message, /Cautions/i);
    assert.match(reply.message, /Do not chase every property manager/i);
    assert.match(reply.message, /Directional, not market-validated/i);
    assert.equal(target.marketValidated, false);

    assert.doesNotMatch(
      reply.message,
      /outreach copy|email sequence|here is your campaign|prospect list|here are \d+ prospects/i
    );
  });

  it('postGrowthMessage progresses from ranking to validation target end-to-end', async () => {
    const store = createMemoryStore();
    const opts = { store, useMemoryPlaybookStore: true };
    const started = await startClientInterview({ clientId: 214 }, opts);
    let turn = started;
    for (const a of ANSWERS) {
      turn = await postInterviewMessage(started.interviewId, a, opts);
    }
    await approveBlueprint(turn.blueprint.id, opts);
    await startGrowthConversation(started.interviewId, opts);

    const ranked = await postGrowthMessage(
      started.interviewId,
      'Please rank the segments and make a directional call.',
      opts
    );
    assert.match(ranked.message, /Segment Ranking/i);
    assert.equal(ranked.segmentRanking.kind, SEGMENT_RANKING_KIND);
    assert.equal(ranked.growthConversation.status, 'ranking_ready');

    const reply = await postGrowthMessage(
      started.interviewId,
      GOOD_FIRST_WIN_MESSAGE,
      opts
    );
    assert.equal(reply.intent, VALIDATION_TARGET_INTENT);
    assert.match(reply.message, /Property Manager Validation Target/i);
    assert.doesNotMatch(reply.message, /^Segment Ranking/m);
    assert.doesNotMatch(reply.message, /Best first segment to test/i);
    assert.ok(reply.validationTarget);
    assert.equal(reply.validationTarget.kind, VALIDATION_TARGET_KIND);
    assert.match(reply.message, /Best first property manager type/i);
    assert.match(reply.message, /First pain point to test/i);
    assert.match(reply.message, /Early signals worth continuing/i);
    assert.match(reply.message, /Successful first 30 days of validation/i);
    assert.match(reply.message, /Cautions/i);
    assert.doesNotMatch(
      reply.message,
      /outreach copy|here is your campaign copy|prospect list/i
    );
    assert.equal(
      reply.growthConversation.status,
      'validation_target_ready'
    );
    // Prior ranking is retained; not regenerated as the reply body.
    assert.ok(reply.growthConversation.segmentRanking);
    assert.equal(
      reply.growthConversation.segmentRanking.kind,
      SEGMENT_RANKING_KIND
    );
  });
});

describe('Growth Conversation state progression (segment mix → ranking → primary → validation)', () => {
  const SEGMENT_MIX_MESSAGE =
    "Let's dig into the segment mix first — that feels like the right place to start.";
  const RANK_MESSAGE =
    'Please compare the segments and rank them. Give me the best first segment to test.';
  const PRIMARY_SELECT_MESSAGE =
    'I agree — property managers feel like the most attractive first segment, with professional offices as secondary.';

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

  it('detects primary-segment selection after ranking (not dig_segments)', () => {
    const rankingReply = buildGrowthConversationReply(
      RANK_MESSAGE,
      anchorGrowthDirection(),
      ANCHOR_BLUEPRINT
    );
    assert.equal(
      detectGrowthConversationIntent(PRIMARY_SELECT_MESSAGE, {
        growthState: rankingReply.growthState,
      }),
      SELECT_PRIMARY_INTENT
    );
    assert.equal(
      detectGrowthConversationIntent("Let's start with property managers", {
        growthState: rankingReply.growthState,
      }),
      SELECT_PRIMARY_INTENT
    );
    assert.equal(
      detectGrowthConversationIntent('Let’s start with property managers', {
        growthState: rankingReply.growthState,
      }),
      SELECT_PRIMARY_INTENT
    );
  });

  it('regression: Anchor sequence advances ranking → primary → validation without segment-mix loop', () => {
    const gd = anchorGrowthDirection();

    // 1) Choose segment mix.
    const mix = buildGrowthConversationReply(
      SEGMENT_MIX_MESSAGE,
      gd,
      ANCHOR_BLUEPRINT
    );
    assert.equal(mix.intent, 'dig_segments');
    assert.equal(mix.growthState.selected_focus_area, 'segment_mix');
    assert.match(
      mix.message,
      /From the Blueprint, the segments worth comparing first are/i
    );

    // 2) Rank segments → store ranking, advance step.
    const ranked = buildGrowthConversationReply(
      RANK_MESSAGE,
      gd,
      ANCHOR_BLUEPRINT,
      { growthState: mix.growthState }
    );
    assert.equal(ranked.intent, 'rank_segments');
    assert.ok(ranked.segmentRanking);
    assert.equal(ranked.segmentRanking.kind, SEGMENT_RANKING_KIND);
    assert.ok(ranked.growthState.segment_ranking);
    assert.ok(ranked.growthState.completed_steps.includes('rank_segments'));
    assert.equal(
      ranked.growthState.current_growth_step,
      'select_primary_segment'
    );

    // 3) Select property managers + professional offices secondary.
    const selected = buildGrowthConversationReply(
      PRIMARY_SELECT_MESSAGE,
      gd,
      ANCHOR_BLUEPRINT,
      { growthState: ranked.growthState }
    );

    assert.equal(selected.intent, SELECT_PRIMARY_INTENT);
    assert.equal(selected.growthState.primary_segment, 'property_managers');
    assert.equal(selected.growthState.secondary_segment, 'professional_offices');
    assert.ok(
      selected.growthState.held_segments.includes('short_term_rental_companies')
    );
    assert.ok(
      selected.growthState.deprioritized_segments.includes('rec_centers')
    );
    assert.ok(
      selected.growthState.deprioritized_segments.includes(
        'broad_high_traffic_buildings'
      )
    );
    assert.equal(
      selected.growthState.current_growth_step,
      'define_validation_target'
    );
    assert.ok(
      selected.growthState.completed_steps.includes('select_primary_segment')
    );

    assert.doesNotMatch(
      selected.message,
      /From the Blueprint, the segments worth comparing first are/i
    );
    assert.doesNotMatch(
      selected.message,
      /Still directional — next we can bound the market or define what a good first win looks like/i
    );
    assert.match(
      selected.message,
      /I'll treat property managers as the first segment to validate and professional offices as the secondary path/i
    );
    assert.match(selected.message, /First Segment Decision/i);
    assert.ok(selected.firstSegmentDecision);
    assert.equal(
      selected.firstSegmentDecision.kind,
      FIRST_SEGMENT_DECISION_KIND
    );
    assert.equal(
      selected.firstSegmentDecision.primary_segment,
      'property_managers'
    );
    assert.equal(
      selected.firstSegmentDecision.secondary_segment,
      'professional_offices'
    );
    assert.equal(
      selected.firstSegmentDecision.next_step,
      'define_validation_target'
    );
    assert.ok(selected.validationTarget);
    assert.equal(selected.validationTarget.kind, VALIDATION_TARGET_KIND);
    assert.equal(selected.validationTarget.target_segment, 'property_managers');
    assert.match(selected.message, /Property Manager Validation Target/i);
    assert.match(selected.message, /Best first property manager type/i);
    assert.match(selected.message, /Successful first 30 days of validation/i);

    // 4) Later "segment" talk stays state-aware.
    const followUp = buildGrowthConversationReply(
      'Can we talk more about the segment focus?',
      gd,
      ANCHOR_BLUEPRINT,
      { growthState: selected.growthState }
    );
    assert.doesNotMatch(
      followUp.message,
      /From the Blueprint, the segments worth comparing first are/i
    );
    assert.doesNotMatch(
      followUp.message,
      /Still directional — next we can bound the market or define what a good first win looks like/i
    );
    assert.equal(followUp.growthState.primary_segment, 'property_managers');
  });

  it('regression e2e: postGrowthMessage persists primary selection and does not re-loop', async () => {
    const store = createMemoryStore();
    const opts = { store, useMemoryPlaybookStore: true };
    const started = await startClientInterview({ clientId: 215 }, opts);
    let turn = started;
    for (const a of ANSWERS) {
      turn = await postInterviewMessage(started.interviewId, a, opts);
    }
    await approveBlueprint(turn.blueprint.id, opts);
    await startGrowthConversation(started.interviewId, opts);

    const mix = await postGrowthMessage(
      started.interviewId,
      SEGMENT_MIX_MESSAGE,
      opts
    );
    assert.equal(mix.growthConversation.selected_focus_area, 'segment_mix');

    const ranked = await postGrowthMessage(
      started.interviewId,
      RANK_MESSAGE,
      opts
    );
    assert.match(ranked.message, /Segment Ranking/i);
    assert.equal(ranked.growthConversation.status, 'ranking_ready');
    assert.ok(
      ranked.growthConversation.completed_steps.includes('rank_segments')
    );

    const selected = await postGrowthMessage(
      started.interviewId,
      PRIMARY_SELECT_MESSAGE,
      opts
    );
    assert.equal(selected.intent, SELECT_PRIMARY_INTENT);
    assert.equal(selected.growthConversation.primary_segment, 'property_managers');
    assert.equal(
      selected.growthConversation.secondary_segment,
      'professional_offices'
    );
    assert.equal(
      selected.growthConversation.current_growth_step,
      'define_validation_target'
    );
    assert.doesNotMatch(
      selected.message,
      /From the Blueprint, the segments worth comparing first are/i
    );
    assert.doesNotMatch(
      selected.message,
      /Still directional — next we can bound the market or define what a good first win looks like/i
    );
    assert.match(selected.message, /First Segment Decision/i);
    assert.ok(selected.validationTarget);
    assert.equal(selected.validationTarget.kind, VALIDATION_TARGET_KIND);
    assert.equal(
      selected.growthConversation.status,
      'validation_target_ready'
    );

    const followUp = await postGrowthMessage(
      started.interviewId,
      'Can we talk more about the segment focus?',
      opts
    );
    assert.doesNotMatch(
      followUp.message,
      /From the Blueprint, the segments worth comparing first are/i
    );
    assert.equal(followUp.growthConversation.primary_segment, 'property_managers');
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
