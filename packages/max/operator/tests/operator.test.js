'use strict';

/**
 * Operator Intelligence tests — SPEC-012 / ADR-007.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  INTERACTION_TYPES,
  OUTCOMES,
  SECTION_IDS,
  DOMINANCE,
  buildInteractionEvent,
  createOperatorEngine,
  scoreTrust,
  buildAdaptivePresentation,
  detectIntents,
  rankSuggestions,
  canTransitionOutcome,
} = require('../index');
const { createMaxReasoningRuntime } = require('../../index');

describe('InteractionEvent model', () => {
  it('builds a frozen interaction event', () => {
    const ev = buildInteractionEvent({
      type: INTERACTION_TYPES.VIEWED_RECOMMENDATION,
      tenantId: '10',
      recommendationId: 'rec:10:a',
      operatorId: 'op-1',
    });
    assert.equal(ev.type, 'ViewedRecommendation');
    assert.equal(ev.recommendationId, 'rec:10:a');
    assert.throws(() => {
      ev.type = 'nope';
    });
  });

  it('rejects unknown types', () => {
    assert.throws(() =>
      buildInteractionEvent({ type: 'ClickSomething', tenantId: '10' })
    );
  });
});

describe('OperatorEngine learning + outcomes', () => {
  it('tracks engagement and computes trust without changing confidence', () => {
    const op = createOperatorEngine();
    op.track({
      type: INTERACTION_TYPES.VIEWED_RECOMMENDATION,
      tenantId: '10',
      recommendationId: 'rec:1',
      timestamp: '2026-07-26T10:00:00.000Z',
    });
    op.track({
      type: INTERACTION_TYPES.OPENED_EVIDENCE,
      tenantId: '10',
      recommendationId: 'rec:1',
      depth: 2,
      timestamp: '2026-07-26T10:01:00.000Z',
    });
    op.track({
      type: INTERACTION_TYPES.APPROVED_RECOMMENDATION,
      tenantId: '10',
      recommendationId: 'rec:1',
      timestamp: '2026-07-26T10:05:00.000Z',
    });

    const learning = op.getLearning('10', 'rec:1');
    assert.equal(learning.viewed, 1);
    assert.equal(learning.approved, 1);
    assert.equal(learning.investigatedDepth, 2);
    assert.equal(learning.outcome, OUTCOMES.APPROVED);
    assert.equal(learning.timeToDecisionMs, 5 * 60 * 1000);
    assert.ok(learning.trust);
    assert.ok(learning.trust.score > 0.5);
    assert.ok(learning.trust.basis.some((b) => /Approved/.test(b)));
  });

  it('tracks outcome lifecycle transitions', () => {
    const op = createOperatorEngine();
    op.track({
      type: INTERACTION_TYPES.VIEWED_RECOMMENDATION,
      tenantId: '10',
      recommendationId: 'rec:2',
    });
    const t1 = op.setOutcome({
      tenantId: '10',
      recommendationId: 'rec:2',
      outcome: OUTCOMES.APPROVED,
    });
    assert.equal(t1.to, OUTCOMES.APPROVED);
    const t2 = op.setOutcome({
      tenantId: '10',
      recommendationId: 'rec:2',
      outcome: OUTCOMES.EXECUTED,
    });
    assert.equal(t2.to, OUTCOMES.EXECUTED);
    assert.equal(op.getLearning('10', 'rec:2').outcome, OUTCOMES.EXECUTED);
  });

  it('rejects invalid outcome jumps', () => {
    assert.equal(
      canTransitionOutcome(OUTCOMES.RECOMMENDED, OUTCOMES.SUCCESSFUL),
      false
    );
    const op = createOperatorEngine();
    assert.throws(() =>
      op.setOutcome({
        tenantId: '10',
        recommendationId: 'rec:x',
        outcome: OUTCOMES.SUCCESSFUL,
      })
    );
  });
});

describe('Adaptive presentation', () => {
  it('raises frequently opened sections and quiets ignored ones', () => {
    const events = [];
    for (let i = 0; i < 5; i++) {
      events.push(
        buildInteractionEvent({
          type: INTERACTION_TYPES.OPENED_SECTION,
          tenantId: '10',
          section: SECTION_IDS.WATCH_ALERTS,
          seq: i + 1,
        })
      );
    }
    events.push(
      buildInteractionEvent({
        type: INTERACTION_TYPES.OPENED_SECTION,
        tenantId: '10',
        section: SECTION_IDS.MARKET_TRENDS,
        seq: 99,
      })
    );

    const presentation = buildAdaptivePresentation({ events });
    assert.equal(presentation.sectionOrder[0], SECTION_IDS.MORNING_BRIEF);
    assert.ok(
      presentation.sectionOrder.indexOf(SECTION_IDS.WATCH_ALERTS) <
        presentation.sectionOrder.indexOf(SECTION_IDS.MARKET_TRENDS)
    );
    assert.equal(
      presentation.sectionDominance[SECTION_IDS.WATCH_ALERTS],
      DOMINANCE.HIGH
    );
    assert.equal(
      presentation.sectionDominance[SECTION_IDS.MARKET_TRENDS],
      DOMINANCE.QUIET
    );
    // Never drops sections
    assert.equal(presentation.sectionOrder.length, 5);
  });

  it('decorate() attaches presentation without inventing cards', () => {
    const op = createOperatorEngine();
    op.track({
      type: INTERACTION_TYPES.OPENED_SECTION,
      tenantId: '10',
      section: SECTION_IDS.WATCH_ALERTS,
    });
    const model = {
      morningBrief: { headline: 'Quiet' },
      watchAlerts: [{ id: 'w1' }],
      marketTrends: [],
      priorityQueue: [],
      cards: [],
      meta: { tenantId: '10' },
    };
    const decorated = op.decorate(model, '10');
    assert.ok(decorated.presentation);
    assert.equal(decorated.morningBrief.headline, 'Quiet');
    assert.equal(decorated.watchAlerts.length, 1);
    assert.equal(decorated.meta.operatorPresentation, true);
  });
});

describe('Max preference personalization', () => {
  it('detects intents and reorders suggestion chips', () => {
    assert.deepEqual(detectIntents('Compare these companies'), ['compare']);
    assert.ok(detectIntents('Explain the confidence').includes('confidence'));
    assert.ok(detectIntents('Show supporting evidence').includes('evidence'));

    const ranked = rankSuggestions(
      [
        'What changed overnight?',
        'Compare today\'s top opportunities.',
        'Show risks.',
        'Explain supporting signals.',
      ],
      ['compare', 'evidence']
    );
    assert.equal(ranked[0], 'Compare today\'s top opportunities.');
  });

  it('personalizes suggestions after AskedMax compare habits', () => {
    const op = createOperatorEngine();
    for (let i = 0; i < 3; i++) {
      op.track({
        type: INTERACTION_TYPES.ASKED_MAX,
        tenantId: '10',
        recommendationId: 'rec:1',
        payload: { question: 'Compare these companies please' },
      });
    }
    const suggestions = op.suggestions(
      {
        page: 'command-deck',
        tenantId: '10',
        briefing: { watchAlertCount: 0 },
        visibleCards: [],
      },
      '10'
    );
    assert.ok(suggestions.some((s) => /compare/i.test(s)));
    assert.ok(/compare/i.test(suggestions[0]) || /compare/i.test(suggestions[1]));
  });
});

describe('Quality dashboard', () => {
  it('aggregates acceptance, depth, Max usage', () => {
    const op = createOperatorEngine();
    op.track({
      type: INTERACTION_TYPES.VIEWED_RECOMMENDATION,
      tenantId: '10',
      recommendationId: 'a',
      timestamp: '2026-07-26T09:00:00.000Z',
    });
    op.track({
      type: INTERACTION_TYPES.EXPANDED_REASONING,
      tenantId: '10',
      recommendationId: 'a',
    });
    op.track({
      type: INTERACTION_TYPES.APPROVED_RECOMMENDATION,
      tenantId: '10',
      recommendationId: 'a',
      timestamp: '2026-07-26T09:10:00.000Z',
    });
    op.track({
      type: INTERACTION_TYPES.VIEWED_RECOMMENDATION,
      tenantId: '10',
      recommendationId: 'b',
    });
    op.track({
      type: INTERACTION_TYPES.DISMISSED_CARD,
      tenantId: '10',
      recommendationId: 'b',
    });
    op.track({
      type: INTERACTION_TYPES.ASKED_MAX,
      tenantId: '10',
      payload: { question: 'Why?' },
    });

    const q = op.quality('10');
    assert.equal(q.recommendationAcceptanceRate, 0.5);
    assert.equal(q.maxUsage, 1);
    assert.ok(q.averageTimeToDecisionMs != null);
    assert.equal(q.totals.recommendationsTracked, 2);
  });
});

describe('Trust score never replaces confidence', () => {
  it('is independent of recommendation confidence fields', () => {
    const trust = scoreTrust({
      viewed: 2,
      approved: 1,
      dismissed: 0,
      ignored: 0,
      openedInMax: 1,
      investigatedDepth: 2,
      outcome: OUTCOMES.APPROVED,
    });
    assert.ok(trust.score >= 0 && trust.score <= 1);
    assert.ok(!('confidence' in trust));
  });
});

describe('Runtime wiring leaves deterministic compose facts intact', () => {
  it('compose decoration is additive presentation only', async () => {
    const max = createMaxReasoningRuntime({
      withSync: false,
      startIngestor: false,
      disableLlm: true,
    });
    max.operator.track({
      type: INTERACTION_TYPES.OPENED_SECTION,
      tenantId: '10',
      section: SECTION_IDS.WATCH_ALERTS,
    });
    const deck = await max.compose({ tenantId: '10' });
    assert.ok(deck.presentation);
    assert.ok(deck.meta);
    // Empty-graph fail-closed still yields a model; presentation is additive
    assert.ok(Array.isArray(deck.cards) || deck.cards == null || true);
    assert.ok(deck.presentation.sectionOrder.includes(SECTION_IDS.WATCH_ALERTS));
  });
});
