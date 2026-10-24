'use strict';

/**
 * Max Synthesis Layer — shared normalize + Build Proposal snapshot tests.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  MESSAGE_INTENTS,
  classifyMessageIntent,
  applyConversationMemoryUpdate,
  normalizeBusinessFacts,
  stripInstructionFraming,
  buildArtifactSynthesisContext,
  containsRawPromptFragment,
  findRawPromptFragments,
  resolveCampaignArtifactAction,
} = require('../services/maxSynthesis');

const {
  buildProspectListBuildProposal,
  formatProspectListBuildProposalMessage,
  buildFirstCampaignPlanPreview,
  buildProspectListCriteriaPreview,
  BUILD_PROPOSAL_TITLE,
} = require('../services/clientIntelligenceCampaignPlanning');

const {
  buildInitialGrowthDirection,
} = require('../services/clientIntelligenceGrowthDirection');

const {
  createMemoryStore,
  startClientInterview,
  postInterviewMessage,
  MESSAGE_TYPES,
  QUESTION_BANK,
} = require('../services/clientIntelligenceInterview');

const ANCHOR_CTX = Object.freeze({
  businessName: 'Anchor Cleaning',
  primarySegment: 'property managers',
  targetMarket: 'Greater Manchester',
  towns: ['Bedford', 'Hooksett', 'Londonderry', 'Auburn', 'Goffstown'],
});

const RAW_CRITERIA_FIELDS = Object.freeze({
  targetSegment:
    'Small to mid-sized local property managers in Greater Manchester who oversee offices, mixed-use buildings, small commercial properties, or multi-tenant spaces that likely need recurring cleaning weekly or multiple times per week.',
  targetSubtype:
    'property managers overseeing offices, mixed-use buildings, small commercial properties, or multi-tenant spaces that likely need recurring cleaning weekly or multiple times per week',
  marketBound:
    'Start with Bedford, Hooksett, Londonderry, Auburn, and Goffstown. Keep Greater Manchester in scope, but keep the first test tight enough to learn quickly.',
  campaignObjective:
    'Prove that small to mid-sized property managers in Greater Manchester will engage in qualified conversations about recurring cleaning.',
  coreValidationQuestion:
    'Can Anchor create qualified property-manager conversations that turn into walkthroughs or estimates?',
  inclusionCriteria: [
    'Manage offices, mixed-use buildings, small commercial properties, or multi-tenant spaces',
  ],
  exclusionCriteria: ['Large institutional property managers'],
  status: 'approved',
});

function assertNoRawPromptFragments(text, label = 'text') {
  const hits = findRawPromptFragments(text);
  assert.equal(
    hits.length,
    0,
    `${label} contains banned fragments: ${hits.join(', ')}\n---\n${text}`
  );
  assert.equal(containsRawPromptFragment(text), false, label);
  assert.doesNotMatch(String(text), /inside Start with/);
  assert.doesNotMatch(String(text), /that match Small to/);
  assert.doesNotMatch(String(text), /Market focus:\s*Start with/i);
  assert.doesNotMatch(String(text), /I forgot to mention/i);
  assert.doesNotMatch(String(text), /This revision introduced/i);
  assert.doesNotMatch(String(text), /Keep Greater Manchester in scope/i);
  assert.doesNotMatch(String(text), /(?<!\.)\.\.(?!\.)/);
}

describe('Max Synthesis Layer — BusinessFactNormalizer', () => {
  it('strips instruction verbs into phrase-safe values', () => {
    assert.equal(
      stripInstructionFraming('Start with Bedford, Hooksett'),
      'Bedford, Hooksett'
    );
    assert.equal(
      stripInstructionFraming('Prove that property managers will book walkthroughs'),
      'property managers will book walkthroughs'
    );
    assert.equal(
      stripInstructionFraming('The first target segment is property managers'),
      'property managers'
    );
    assert.equal(
      stripInstructionFraming('I forgot to mention property managers'),
      'property managers'
    );
    assert.equal(
      stripInstructionFraming('This revision introduced a tighter market bound'),
      'a tighter market bound'
    );
  });

  it('normalizes required phrase fields from prior criteria paragraphs', () => {
    const facts = normalizeBusinessFacts({
      context: ANCHOR_CTX,
      priorCriteriaPreview: RAW_CRITERIA_FIELDS,
    });
    assert.equal(
      facts.phrases.targetSegmentPhrase,
      'small to mid-sized local property managers'
    );
    assert.equal(
      facts.phrases.targetSubtypePhrase,
      'property managers overseeing offices, mixed-use buildings, small commercial properties, or multi-tenant spaces'
    );
    assert.equal(
      facts.phrases.marketBoundPhrase,
      'Bedford, Hooksett, Londonderry, Auburn, and Goffstown, with Greater Manchester kept in scope'
    );
    assert.match(
      facts.phrases.objectivePhrase,
      /^test whether qualified property-manager conversations can turn into walkthroughs or estimate requests$/i
    );
    assertNoRawPromptFragments(JSON.stringify(facts.phrases), 'phrases');
  });

  it('exposes evidence separately from display phrases', () => {
    const ctx = buildArtifactSynthesisContext({
      context: ANCHOR_CTX,
      priorCriteriaPreview: RAW_CRITERIA_FIELDS,
    });
    assert.equal(ctx.rawDisplayAllowed, false);
    assert.match(ctx.evidence.rawMarketBound, /^Start with Bedford/);
    assert.equal(ctx.phrase('marketBoundPhrase').startsWith('Start with'), false);
  });
});

describe('Max Synthesis Layer — MessageIntentClassifier + memory', () => {
  it('classifies approval_plus_next_request', () => {
    const intent = classifyMessageIntent(
      'Approved. Before we build anything, tell me how you would approach building the first prospect list for this test...'
    );
    assert.equal(intent, MESSAGE_INTENTS.APPROVAL_PLUS_NEXT_REQUEST);
  });

  it('classifies add_on for forgotten ICP detail', () => {
    const intent = classifyMessageIntent('I also forgot property managers', {
      looksLikeAddOn: () => true,
    });
    assert.equal(intent, MESSAGE_INTENTS.ADD_ON);
  });

  it('records add-on facts on the targeted section in memory', () => {
    const { memory } = applyConversationMemoryUpdate(
      {},
      {
        messageClass: MESSAGE_INTENTS.ADD_ON,
        text: 'I also forgot property managers',
        section: 'idealCustomers',
        substance: 'property managers',
      }
    );
    assert.ok(
      (memory.acceptedFacts || []).some(
        (f) =>
          f.section === 'idealCustomers' &&
          /property managers/i.test(f.substance) &&
          f.source === 'add_on'
      )
    );
  });
});

describe('Max Synthesis Layer — Prospect List Build Proposal snapshot', () => {
  it('renders natural approach summary from normalized phrases', () => {
    const proposal = buildProspectListBuildProposal(ANCHOR_CTX, {}, {
      priorCriteriaPreview: RAW_CRITERIA_FIELDS,
    });
    const message = formatProspectListBuildProposalMessage(proposal);

    assert.equal(proposal.title, BUILD_PROPOSAL_TITLE);
    assert.match(
      proposal.approachSummary,
      /^For Anchor's first test, I would build a small, reviewable batch of 15–25 property managers in Bedford, Hooksett, Londonderry, Auburn, and Goffstown\./
    );
    assert.match(
      proposal.approachSummary,
      /The list should focus on small to mid-sized local firms that manage offices, mixed-use buildings, small commercial properties, or multi-tenant spaces\./
    );

    assert.equal(
      proposal.firstBatchPlan.marketFocus,
      'Bedford, Hooksett, Londonderry, Auburn, and Goffstown, with Greater Manchester kept in scope'
    );
    assert.equal(
      proposal.synthesisPhrases.targetSegmentPhrase,
      'small to mid-sized local property managers'
    );

    assertNoRawPromptFragments(message, 'build proposal message');
    assertNoRawPromptFragments(proposal.approachSummary, 'approachSummary');
    assert.doesNotMatch(message, /prospect list of Small to mid-sized/i);
    assert.doesNotMatch(message, /inside Start with/i);
    assert.doesNotMatch(message, /sources that match Small to/i);
    assert.doesNotMatch(message, /Market focus:\s*Start with/i);

    // No duplicated full subtype sentence.
    const subtype =
      proposal.synthesisPhrases.targetSubtypePhrase;
    assert.ok(subtype);
    assert.equal(
      message.split(subtype).length <= 2,
      true,
      'subtype phrase should not be duplicated as full sentences'
    );
  });

  it('approval_plus_next advances to Build Proposal via shared memory action', () => {
    const action = resolveCampaignArtifactAction({
      userMessage:
        'Approved. Before we build anything, tell me how you would approach building the first prospect list for this test...',
      priorCriteriaPreview: {
        kind: 'prospect_list_criteria_preview',
        status: 'draft',
        ...RAW_CRITERIA_FIELDS,
      },
      step: 'prospect_list_criteria_preview',
    });
    assert.equal(action.messageClass, MESSAGE_INTENTS.APPROVAL_PLUS_NEXT_REQUEST);
    assert.equal(action.action, 'emit_build_proposal');
  });
});

describe('Max Synthesis Layer — shared path on Growth/Campaign artifacts', () => {
  it('Campaign Preview and Criteria Preview expose synthesisPhrases', () => {
    const preview = buildFirstCampaignPlanPreview(ANCHOR_CTX, {
      campaign_objective: {
        raw: 'Prove that property managers will book walkthroughs. I forgot to mention this revision introduced nothing.',
      },
      target_segment: { raw: 'Keep property managers as defined.' },
      market_bounds: {
        raw: 'Start with Bedford, Hooksett, Londonderry, Auburn, and Goffstown. Keep Greater Manchester in scope.',
      },
    });
    assert.ok(preview.synthesisPhrases);
    assert.equal(
      preview.synthesisPhrases.targetSegmentPhrase.startsWith('Start with'),
      false
    );
    assertNoRawPromptFragments(
      JSON.stringify(preview.synthesisPhrases),
      'campaign preview phrases'
    );

    const criteria = buildProspectListCriteriaPreview(
      ANCHOR_CTX,
      {
        campaignObjective: preview.campaignObjective,
        targetSegment: 'property managers',
        marketBound: preview.marketBound,
        inclusionCriteria: ['local managers'],
        exclusionCriteria: ['national firms'],
        previewGenerated: true,
      },
      { priorPreview: preview }
    );
    assert.ok(criteria.synthesisPhrases);
    assertNoRawPromptFragments(
      JSON.stringify(criteria.synthesisPhrases),
      'criteria phrases'
    );
  });

  it('Outreach Strategy Preview section 5 embeds phrase-safe fields only', () => {
    const {
      buildOutreachStrategyPreview,
      formatOutreachStrategyPreviewMessage,
    } = require('../services/clientIntelligenceCampaignPlanning');

    const strategy = buildOutreachStrategyPreview(
      {
        approvedBatch: {
          name: 'Batch 1',
          candidateCount: 6,
          candidates: [
            { companyName: 'Elm Grove Companies' },
            { companyName: 'Avise Properties' },
          ],
        },
      },
      {
        ...ANCHOR_CTX,
        competitiveAdvantages:
          'Customers choose this business for reliability and responsiveness.',
      },
      { priorCriteriaPreview: RAW_CRITERIA_FIELDS }
    );

    assert.equal(
      strategy.outreachApproach[0],
      "Lead with Anchor's reliability and responsiveness for small to mid-sized property managers in Bedford, Hooksett, Londonderry, Auburn, and Goffstown."
    );
    assert.equal(
      strategy.outreachAudiencePhrase,
      'small to mid-sized property managers'
    );
    assert.equal(
      strategy.outreachMarketPhrase,
      'Bedford, Hooksett, Londonderry, Auburn, and Goffstown'
    );
    assert.equal(strategy.outreachAnglePhrase, 'reliability and responsiveness');
    assert.match(strategy.approvedBatchPhrase, /approved Batch 1 record/);

    const message = formatOutreachStrategyPreviewMessage(strategy);
    assertNoRawPromptFragments(message, 'outreach strategy preview message');
    assert.doesNotMatch(
      message,
      /Small to mid-sized local property managers in Greater Manchester who oversee/i
    );
    assert.doesNotMatch(message, /differentiators for /i);
  });

  it('Growth Direction attaches synthesisPhrases and strips meta fragments from avoid copy', () => {
    const blueprint = {
      id: 'bp-1',
      version: '1',
      sections: {
        identity: { summary: 'Anchor Cleaning is a commercial cleaning company.' },
        services: { summary: 'Recurring commercial cleaning for offices.' },
        idealCustomers: { summary: 'Property managers and professional offices.' },
        avoidCustomers: {
          summary:
            'I forgot to mention Anchor should avoid buyers focused only on the lowest price. This revision introduced nothing useful.',
        },
        targetMarkets: {
          summary:
            'Greater Manchester including Bedford, Hooksett, Londonderry, Auburn, Goffstown',
        },
        campaignGoals: { summary: 'Qualified conversations and walkthroughs.' },
      },
    };
    const gd = buildInitialGrowthDirection(blueprint, {
      normalizedFacts: {
        business_name: 'Anchor Cleaning',
        ideal_customers: ['property managers'],
      },
    });
    assert.ok(gd.synthesisPhrases);
    const body = (gd.paragraphs || []).join('\n');
    assert.doesNotMatch(body, /I forgot to mention/i);
    assert.doesNotMatch(body, /This revision introduced/i);
    assertNoRawPromptFragments(
      JSON.stringify(gd.synthesisPhrases),
      'growth direction phrases'
    );
  });
});

describe('Max Synthesis Layer — add-on updates ideal customers, not current question', () => {
  function withStore() {
    const store = createMemoryStore();
    return { store, opts: { store } };
  }

  it('I also forgot property managers updates ICP while on avoid question', async () => {
    const { opts, store } = withStore();
    const started = await startClientInterview({ clientId: 9401 }, opts);
    await postInterviewMessage(
      started.interviewId,
      'Anchor Cleaning — commercial cleaning for professional offices.',
      opts
    );
    await postInterviewMessage(
      started.interviewId,
      'Office cleaning, recurring commercial cleans, and deep cleans.',
      opts
    );
    await postInterviewMessage(
      started.interviewId,
      'facility managers, professional offices, daycares',
      opts
    );

    const before = await store.getSession(started.interviewId);
    assert.equal(QUESTION_BANK[before.interview_state.stepIndex].id, 'avoid_customers');

    const supplement = await postInterviewMessage(
      started.interviewId,
      'I also forgot property managers',
      opts
    );
    assert.equal(supplement.messageType, MESSAGE_TYPES.SUPPLEMENTAL_CONTEXT);
    assert.equal(supplement.question.id, 'avoid_customers');

    const after = await store.getSession(started.interviewId);
    assert.equal(after.interview_state.stepIndex, before.interview_state.stepIndex);
    assert.ok(
      (after.interview_state.normalizedFacts.ideal_customers || []).some((s) =>
        /property managers/i.test(s)
      )
    );
    assert.equal(after.interview_state.answers.avoid_customers, undefined);
    assert.equal(
      /forgot to mention|I also forgot/i.test(
        JSON.stringify(after.interview_state.normalizedFacts)
      ),
      false
    );
  });
});
