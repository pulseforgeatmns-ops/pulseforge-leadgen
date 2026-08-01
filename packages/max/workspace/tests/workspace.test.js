'use strict';

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const {
  normalizeContext,
  contextFingerprint,
  buildOpeningState,
  buildSuggestions,
  composeResponse,
  formatDeterministicProse,
  PresentationEngine,
  createWorkspaceEngine,
  PAGE_TYPES,
} = require('..');

function sampleDeckContext(overrides = {}) {
  return normalizeContext({
    page: PAGE_TYPES.COMMAND_DECK,
    tenantId: '1',
    briefing: {
      headline: 'Your market shifted overnight.',
      summary: '3 companies moved. 1 watch alert.',
      marketChanges: 3,
      watchAlertCount: 1,
      priorityCount: 2,
      generatedAt: '2026-07-26T12:00:00.000Z',
    },
    visibleCards: [
      {
        id: 'card:highest_leverage:rec-1',
        type: 'highest_leverage',
        title: 'Review Marlowe Properties',
        summary: 'Opportunity 91 · Confidence 94',
        confidence: 94,
        reasoningId: 'rec-1',
        policyId: 'pol-1',
        briefingId: 'brief-1',
        sources: [
          { kind: 'briefing', id: 'brief-1' },
          { kind: 'reasoning', id: 'rec-1' },
          { kind: 'policy', id: 'pol-1' },
        ],
        payload: {
          recommendation: {
            id: 'rec-1',
            companyId: 'co-marlowe',
            companyName: 'Marlowe Properties',
            recommendedAction: 'review',
          },
          opportunity: 91,
          confidence: 94,
          supportingSignals: [
            { id: 'sig-1', summary: 'Engagement rising', kind: 'signal' },
            { id: 'sig-2', summary: 'Decision-maker identified', kind: 'signal' },
          ],
          contradictingSignals: [
            { id: 'opp-1', summary: 'Recent bounce risk', kind: 'signal' },
          ],
          policy: {
            allowed: true,
            requiresApproval: false,
            blocked: false,
            outcome: 'allow',
            reason: 'Confidence above threshold',
          },
        },
        updatedAt: '2026-07-26T12:00:00.000Z',
      },
      {
        id: 'card:watch:1',
        type: 'watch_alert',
        title: 'Watch: Marlowe score drop risk',
        summary: 'Warmth decaying',
        sources: [{ kind: 'memory', id: 'watch-1' }],
      },
    ],
    deck: {
      morningBrief: {
        headline: 'Your market shifted overnight.',
        summary: '3 companies moved. 1 watch alert.',
        marketChanges: 3,
        watchAlertCount: 1,
      },
      highestLeverageAction: {
        recommendation: {
          id: 'rec-1',
          companyId: 'co-marlowe',
          companyName: 'Marlowe Properties',
          recommendedAction: 'review',
        },
        opportunity: 91,
        confidence: 94,
        supportingSignals: [
          { id: 'sig-1', summary: 'Engagement rising' },
        ],
        contradictingSignals: [
          { id: 'opp-1', summary: 'Recent bounce risk' },
        ],
        policy: {
          allowed: true,
          outcome: 'allow',
          reason: 'Confidence above threshold',
        },
      },
      watchAlerts: [
        {
          id: 'card:watch:1',
          title: 'Watch: Marlowe score drop risk',
          summary: 'Warmth decaying',
        },
      ],
      priorityQueue: {
        items: [
          {
            id: 'rec-1',
            companyId: 'co-marlowe',
            companyName: 'Marlowe Properties',
            opportunity: 91,
            confidence: 94,
            movement: '↑4',
          },
          {
            id: 'rec-2',
            companyId: 'co-lumen',
            companyName: 'Lumen',
            opportunity: 84,
            confidence: 89,
            movement: '—',
          },
        ],
      },
      meta: { tenantId: '1', generatedAt: '2026-07-26T12:00:00.000Z' },
    },
    ...overrides,
  });
}

describe('MaxContext envelope', () => {
  it('requires tenantId and validates page', () => {
    assert.throws(() => normalizeContext({}), /tenantId/);
    assert.throws(
      () => normalizeContext({ tenantId: '1', page: 'crm' }),
      /page must be one of/
    );
  });

  it('accepts all supported page types', () => {
    for (const page of Object.values(PAGE_TYPES)) {
      const ctx = normalizeContext({ tenantId: '1', page });
      assert.equal(ctx.page, page);
    }
  });
});

describe('OpeningStateBuilder', () => {
  it('opens command deck with briefing counts', () => {
    const opening = buildOpeningState(sampleDeckContext(), { hour: 9 });
    assert.match(opening.greeting, /Good morning/);
    assert.ok(opening.body.some((l) => /3 opportunities/i.test(l)));
    assert.ok(opening.body.some((l) => /watch alert/i.test(l)));
    assert.match(opening.fullText, /investigate/i);
  });

  it('opens company context without inventing deltas', () => {
    const ctx = sampleDeckContext({
      page: PAGE_TYPES.COMPANY,
      companyId: 'co-marlowe',
      selectedEntity: {
        id: 'co-marlowe',
        type: 'company',
        name: 'Marlowe Properties',
      },
    });
    const opening = buildOpeningState(ctx, { hour: 14 });
    assert.match(opening.fullText, /Marlowe Properties/);
    assert.doesNotMatch(opening.fullText, /increased 12 points/);
  });

  it('opens recommendation context', () => {
    const ctx = sampleDeckContext({
      page: PAGE_TYPES.RECOMMENDATION,
      recommendationId: 'rec-1',
      selectedEntity: {
        id: 'rec-1',
        type: 'recommendation',
        name: 'Marlowe Properties',
      },
    });
    const opening = buildOpeningState(ctx, { hour: 10 });
    assert.match(opening.fullText, /highest leverage recommendation/i);
  });
});

describe('SuggestionEngine', () => {
  it('varies suggestions by page and uses envelope names', () => {
    const deck = buildSuggestions(sampleDeckContext());
    assert.ok(deck.some((s) => /Marlowe/.test(s)));
    assert.ok(deck.some((s) => /overnight/i.test(s)));

    const company = buildSuggestions(
      sampleDeckContext({
        page: PAGE_TYPES.COMPANY,
        companyId: 'co-marlowe',
        selectedEntity: {
          id: 'co-marlowe',
          type: 'company',
          name: 'Marlowe Properties',
        },
      })
    );
    assert.ok(company.some((s) => /confidence/i.test(s)));
    assert.ok(!company.some((s) => /#1/.test(s)));

    const rec = buildSuggestions(
      sampleDeckContext({
        page: PAGE_TYPES.RECOMMENDATION,
        recommendationId: 'rec-1',
      })
    );
    assert.ok(rec.some((s) => /policy/i.test(s)));
  });
});

describe('ResponseComposer', () => {
  it('answers rank questions from HLA without inventing evidence ids', () => {
    const structured = composeResponse({
      context: sampleDeckContext(),
      question: 'Why is Marlowe #1?',
    });
    assert.match(structured.answer, /Marlowe/);
    assert.ok(structured.reasoning.length >= 1);
    assert.equal(structured.confidence, 94);
    for (const e of structured.supportingEvidence) {
      assert.ok(e.id);
      assert.ok(e.summary);
    }
    assert.ok(structured.metadata.sourcesUsed.briefing);
    assert.ok(structured.metadata.sourcesUsed.reasoning);
  });

  it('does not fabricate confidence when absent', () => {
    const ctx = normalizeContext({
      tenantId: '1',
      page: PAGE_TYPES.COMMAND_DECK,
      briefing: { headline: 'Quiet morning.', marketChanges: 0, watchAlertCount: 0 },
      visibleCards: [],
    });
    const structured = composeResponse({
      context: ctx,
      question: 'Explain confidence',
    });
    assert.equal(structured.confidence, null);
    assert.ok(Array.isArray(structured.metadata.unavailable));
  });

  it('surfaces contradicting evidence when present', () => {
    const structured = composeResponse({
      context: sampleDeckContext(),
      question: 'Show contradicting evidence',
    });
    assert.ok(structured.contradictingEvidence.length >= 1);
    assert.match(structured.answer, /contradict/i);
  });

  it('answers highest-moves questions from the CommandDeck array queue', () => {
    const structured = composeResponse({
      context: sampleDeckContext(),
      question: 'What are the highest calls or moves to make today?',
    });
    assert.match(structured.answer, /Marlowe Properties/);
    assert.match(structured.answer, /↑4/);
    assert.ok(structured.timelineReferences.some((ref) => /Marlowe Properties/.test(ref.summary)));
    assert.ok(structured.metadata.unavailable.includes('call_activity'));
    assert.ok(!structured.metadata.unavailable.includes('contradicting_evidence'));
  });

  it('states the missing historical context when activity data is absent', () => {
    const context = normalizeContext({
      tenantId: '1',
      page: PAGE_TYPES.COMMAND_DECK,
      briefing: {
        headline: 'Market context is still building.',
        summary: 'No historical snapshots are available yet.',
      },
      deck: { priorityQueue: [] },
    });
    const structured = composeResponse({
      context,
      question: 'What are the highest calls or moves to make today?',
    });
    assert.match(structured.answer, /can’t rank today’s calls or moves/i);
    assert.ok(structured.metadata.unavailable.includes('call_activity'));
    assert.ok(structured.metadata.unavailable.includes('market_movement'));
  });

  it('reports policy from envelope', () => {
    const structured = composeResponse({
      context: sampleDeckContext({
        page: PAGE_TYPES.RECOMMENDATION,
        recommendationId: 'rec-1',
      }),
      question: 'Walk through policy evaluation',
    });
    assert.match(structured.answer, /allow/i);
    assert.ok(structured.metadata.sourcesUsed.policy);
  });
});

describe('PresentationEngine', () => {
  it('fallback preserves SRO meaning without LLM', async () => {
    const engine = new PresentationEngine({ disableLlm: true });
    const structured = composeResponse({
      context: sampleDeckContext(),
      question: 'What changed overnight?',
    });
    const presented = await engine.present(structured);
    assert.equal(presented.presentation, 'fallback');
    assert.match(presented.prose, /3/);
    assert.equal(presented.structured.confidence, structured.confidence);
    assert.equal(
      presented.structured.supportingEvidence.length,
      structured.supportingEvidence.length
    );
  });

  it('formatDeterministicProse lists unavailable gaps', () => {
    const prose = formatDeterministicProse({
      answer: 'Limited detail.',
      reasoning: ['No signals.'],
      metadata: { unavailable: ['supporting_evidence'] },
      nextInvestigations: ['Show risks.'],
    });
    assert.match(prose, /Unavailable/);
    assert.match(prose, /supporting_evidence/);
  });

  it('formatDeterministicProse honors strictOutputShape', () => {
    const prose = formatDeterministicProse({
      answer: '| a | b |\n| --- | --- |\n| 1 | 2 |\n\nPreparation-only: no mission created; no launch, approval, print, or mail.',
      reasoning: ['Should be suppressed.'],
      metadata: {
        strictOutputShape: true,
        unavailable: ['mailing_address'],
      },
      nextInvestigations: ['Should not appear.'],
    });
    assert.match(prose, /\| a \| b \|/);
    assert.match(prose, /Preparation-only/);
    assert.doesNotMatch(prose, /Reasoning/);
    assert.doesNotMatch(prose, /Unavailable/);
    assert.doesNotMatch(prose, /Next:/);
    assert.doesNotMatch(prose, /Should be suppressed/);
  });
});

describe('WorkspaceEngine', () => {
  /** @type {import('../WorkspaceEngine').WorkspaceEngine} */
  let engine;

  beforeEach(() => {
    engine = createWorkspaceEngine({ disableLlm: true });
  });

  it('open never returns an empty conversation', () => {
    const opened = engine.open(sampleDeckContext(), { hour: 9 });
    assert.ok(opened.sessionId);
    assert.ok(opened.opening.fullText.length > 20);
    assert.ok(opened.suggestions.length >= 3);
  });

  it('ask returns structured + prose + metadata', async () => {
    const opened = engine.open(sampleDeckContext(), { hour: 9 });
    const result = await engine.ask({
      sessionId: opened.sessionId,
      question: 'Why is Marlowe #1?',
    });
    assert.ok(result.prose);
    assert.ok(result.structured.answer);
    assert.ok(result.metadata);
    assert.ok(result.metadata.sourcesUsed);
    assert.equal(result.presentation, 'fallback');
  });

  it('acknowledges context switches', async () => {
    const opened = engine.open(sampleDeckContext(), { hour: 9 });
    const switched = engine.switchContext(opened.sessionId, {
      page: PAGE_TYPES.COMPANY,
      tenantId: '1',
      companyId: 'co-marlowe',
      selectedEntity: {
        id: 'co-marlowe',
        type: 'company',
        name: 'Marlowe Properties',
      },
      visibleCards: sampleDeckContext().visibleCards,
    });
    assert.match(switched.contextSwitch, /Marlowe Properties/);
    assert.notEqual(
      contextFingerprint(opened.context),
      contextFingerprint(switched.context)
    );

    const asked = await engine.ask({
      sessionId: opened.sessionId,
      question: 'Explain supporting signals.',
      context: {
        page: PAGE_TYPES.RECOMMENDATION,
        tenantId: '1',
        recommendationId: 'rec-1',
        selectedEntity: {
          id: 'rec-1',
          type: 'recommendation',
          name: 'Marlowe Properties',
        },
        visibleCards: sampleDeckContext().visibleCards,
        deck: sampleDeckContext().deck,
      },
    });
    assert.ok(asked.contextSwitch);
    assert.match(asked.prose, /Marlowe|recommendation|looking at/i);
  });

  it('remembers session context across turns', async () => {
    const opened = engine.open(sampleDeckContext(), { hour: 9 });
    await engine.ask({
      sessionId: opened.sessionId,
      question: 'Why is Marlowe #1?',
    });
    const second = await engine.ask({
      sessionId: opened.sessionId,
      question: 'Show contradicting evidence.',
    });
    assert.equal(second.context.page, PAGE_TYPES.COMMAND_DECK);
    assert.ok(second.structured.contradictingEvidence.length >= 1);
    const session = engine.sessions.get(opened.sessionId);
    assert.ok(session.messages.length >= 4);
  });
});
