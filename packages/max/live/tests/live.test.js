'use strict';

/**
 * Live Intelligence Loop tests — SPEC-011 / ADR-006.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  LIFECYCLE,
  EVENT_TYPES,
  ENTITY_KINDS,
  SEVERITY,
  buildIntelligenceEvent,
  encodeCursor,
  decodeCursor,
  createLiveLoopEngine,
  isMaterialEvent,
  toNotifications,
  buildAwareness,
  diffCommandDeck,
} = require('../index');

describe('IntelligenceEvent model', () => {
  it('builds a frozen event with material default from type', () => {
    const ev = buildIntelligenceEvent({
      type: EVENT_TYPES.HIGHEST_LEVERAGE_REPLACED,
      entity: { kind: ENTITY_KINDS.RECOMMENDATION, id: 'rec:1:a', label: 'A' },
      tenantId: '10',
      summary: 'HLA replaced',
      severity: SEVERITY.HIGH,
    });
    assert.equal(ev.material, true);
    assert.equal(ev.entity.id, 'rec:1:a');
    assert.throws(() => {
      ev.summary = 'nope';
    });
  });

  it('encodes and decodes cursors', () => {
    assert.equal(decodeCursor('c:12'), 12);
    assert.equal(encodeCursor(12), 'c:12');
    assert.equal(decodeCursor(null), 0);
  });
});

describe('DeckDiff + LiveLoopEngine', () => {
  it('observes initial deck and evolves on HLA replace', () => {
    const live = createLiveLoopEngine();
    const deck1 = {
      morningBrief: { headline: 'Three opportunities improved overnight.' },
      highestLeverageAction: {
        id: 'hla',
        title: 'Alpha Co',
        payload: { recommendationId: 'rec:10:alpha' },
      },
      watchAlerts: [],
      priorityQueue: [
        {
          id: 'rec:10:alpha',
          title: 'Alpha Co',
          confidence: 0.6,
        },
      ],
      meta: { generatedAt: '2026-07-26T09:02:00.000Z', tenantId: '10' },
    };

    const first = live.observeDeck({ tenantId: '10', model: deck1 });
    assert.ok(first.cursor);
    assert.ok(first.evolution.length >= 1);
    assert.equal(first.notifications.length, 0);

    const deck2 = {
      morningBrief: { headline: 'A watch alert appears.' },
      highestLeverageAction: {
        id: 'hla',
        title: 'Beta Co',
        payload: { recommendationId: 'rec:10:beta' },
      },
      watchAlerts: [
        {
          id: 'watch-1',
          title: 'Overflow risk',
          severity: 'high',
        },
      ],
      priorityQueue: [
        {
          id: 'rec:10:beta',
          title: 'Beta Co',
          confidence: 0.82,
        },
      ],
      meta: { generatedAt: '2026-07-26T10:18:00.000Z', tenantId: '10' },
    };

    const second = live.observeDeck({ tenantId: '10', model: deck2 });
    const types = second.events.map((e) => e.type);
    assert.ok(types.includes(EVENT_TYPES.HIGHEST_LEVERAGE_REPLACED));
    assert.ok(types.includes(EVENT_TYPES.WATCH_ALERT_PROMOTED));
    assert.ok(types.includes(EVENT_TYPES.BRIEFING_EVOLVED));
    assert.ok(second.notifications.length >= 1);
    assert.ok(
      second.notifications.every((n) =>
        isMaterialEvent({ type: n.type, material: true })
      )
    );

    const polled = live.liveSince({
      tenantId: '10',
      since: first.cursor,
    });
    assert.equal(polled.hasUpdates, true);
    assert.ok(polled.events.length >= 1);
    assert.ok(polled.evolution.length >= 2);
  });

  it('records lifecycle timeline for an entity', () => {
    const live = createLiveLoopEngine();
    live.observeChanges({
      tenantId: '10',
      entityId: 'rec:10:alpha',
      entityLabel: 'Alpha Co',
      timestamp: '2026-07-26T09:01:00.000Z',
      changes: [{ type: 'new_hiring_signal' }],
    });
    live.observeChanges({
      tenantId: '10',
      entityId: 'rec:10:alpha',
      entityLabel: 'Alpha Co',
      timestamp: '2026-07-26T09:18:00.000Z',
      changes: [
        {
          type: 'confidence_increased',
          magnitude: 8,
          confidenceBefore: 0.5,
          confidenceAfter: 0.58,
        },
      ],
    });
    live.observeChanges({
      tenantId: '10',
      entityId: 'rec:10:alpha',
      entityLabel: 'Alpha Co',
      timestamp: '2026-07-26T10:22:00.000Z',
      changes: [{ type: 'new_evidence', evidenceId: 'ev-1' }],
    });

    const tl = live.timeline({
      tenantId: '10',
      entityId: 'rec:10:alpha',
    });
    assert.ok(tl.events.length >= 3);
    assert.equal(tl.lifecycle.state, LIFECYCLE.STRENGTHENED);
    assert.ok(tl.transitions.length >= 1);
  });

  it('promotes confidence threshold cross to material', () => {
    const live = createLiveLoopEngine({ confidenceThreshold: 0.75 });
    const result = live.observeChanges({
      tenantId: '10',
      entityId: 'rec:10:alpha',
      entityLabel: 'Alpha',
      changes: [
        {
          type: 'confidence_increased',
          confidenceBefore: 0.7,
          confidenceAfter: 0.8,
          magnitude: 0.1,
        },
      ],
    });
    assert.equal(result.events[0].type, EVENT_TYPES.CONFIDENCE_THRESHOLD_CROSSED);
    assert.equal(result.events[0].material, true);
    assert.equal(toNotifications(result.events).length, 1);
  });

  it('does not notify non-material briefing evolution', () => {
    const partials = diffCommandDeck(
      {
        morningBrief: { headline: 'A' },
        highestLeverageAction: null,
        watchAlerts: [],
        priorityQueue: [],
      },
      {
        morningBrief: { headline: 'B' },
        highestLeverageAction: null,
        watchAlerts: [],
        priorityQueue: [],
        meta: { generatedAt: '2026-07-26T11:00:00.000Z' },
      },
      { tenantId: '10' }
    );
    assert.ok(partials.some((p) => p.type === EVENT_TYPES.BRIEFING_EVOLVED));
    assert.ok(partials.every((p) => p.material === false || !p.material));
  });
});

describe('Max awareness', () => {
  it('describes changes since conversation open', () => {
    const live = createLiveLoopEngine();
    live.observeChanges({
      tenantId: '10',
      entityId: 'rec:10:alpha',
      entityLabel: 'Alpha Co',
      timestamp: '2026-07-26T12:00:00.000Z',
      changes: [
        { type: 'new_evidence', evidenceId: 'e1' },
        { type: 'new_evidence', evidenceId: 'e2' },
      ],
    });
    const awareness = live.awareness({
      tenantId: '10',
      entityId: 'rec:10:alpha',
      entityLabel: 'Alpha Co',
      openedAt: '2026-07-26T11:55:00.000Z',
    });
    assert.ok(awareness.headline);
    assert.ok(
      awareness.lines.some((l) => /evidence source/i.test(l)) ||
        awareness.lines.length >= 1
    );

    const built = buildAwareness({
      events: awareness.events,
      entityLabel: 'This recommendation',
      now: '2026-07-26T12:12:00.000Z',
    });
    assert.ok(built.lines.length >= 1);
  });
});
