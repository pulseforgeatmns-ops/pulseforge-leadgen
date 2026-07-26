'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  seedTenant,
  registerScoreWatches,
  TENANT,
  AS_OF,
  createMaxReasoningRuntime,
  createKnowledgeRuntime,
} = require('./helpers');
const {
  CARD_TYPES,
  ACTION_TYPES,
  buildIntelligenceCard,
  EMPTY_CATALOG,
  composeWatchAlerts,
  dedupeAlerts,
  deriveSeverity,
  formatMovement,
} = require('..');

const BRIEF_WINDOW = {
  periodStart: '2026-07-19T00:00:00.000Z',
  periodEnd: AS_OF,
};

describe('CommandDeckComposer — single view model', () => {
  it('composes Morning Brief, HLA, watches, trends, and priority queue', async () => {
    const { max, companies } = await seedTenant({ companyCount: 3 });
    registerScoreWatches(max, companies);
    max.policy.configureTenant(TENANT, {
      minimumConfidence: 0.1,
      maximumRisk: 1,
      maximumContradictionSeverity: 1,
      approvalRequired: [],
      blockedDays: [],
      blockAutonomousOutreach: false,
      cooldownHours: 0,
      maxEvidenceAgeDays: 0,
    });

    const deck = await max.compose({
      tenantId: TENANT,
      asOf: AS_OF,
      period: 'daily',
      ...BRIEF_WINDOW,
    });

    assert.ok(deck.morningBrief);
    assert.ok(deck.morningBrief.headline);
    assert.ok(typeof deck.morningBrief.summary === 'string');
    assert.ok(Number.isFinite(deck.morningBrief.marketChanges));
    assert.ok(Number.isFinite(deck.morningBrief.watchAlertCount));
    assert.ok(Number.isFinite(deck.morningBrief.priorityCount));
    assert.equal(deck.morningBrief.generatedAt != null, true);

    assert.ok(deck.highestLeverageAction);
    assert.ok(deck.highestLeverageAction.recommendation);
    assert.ok(deck.highestLeverageAction.policy);
    assert.ok(Array.isArray(deck.highestLeverageAction.supportingSignals));
    assert.ok(Array.isArray(deck.highestLeverageAction.contradictingSignals));

    assert.ok(Array.isArray(deck.watchAlerts));
    assert.ok(Array.isArray(deck.marketTrends));
    assert.ok(Array.isArray(deck.priorityQueue));
    assert.ok(deck.priorityQueue.length >= 1);
    assert.equal(deck.priorityQueue[0].rank, 1);
    assert.ok(deck.priorityQueue[0].recommendationId);
    assert.ok(deck.priorityQueue[0].movement);

    assert.ok(Array.isArray(deck.cards));
    assert.ok(deck.cards.length >= 1);
    assert.equal(deck.meta.tenantId, TENANT);
    assert.ok(deck.meta.briefingId);
    assert.ok(deck.meta.buildTimeMs >= 0);
  });

  it('exposes compose() on createMaxReasoningRuntime', async () => {
    const max = createMaxReasoningRuntime({ startIngestor: false });
    assert.equal(typeof max.compose, 'function');
    assert.equal(typeof max.commandDeck.compose, 'function');
  });

  it('does not call ReasoningEngine.evaluate during compose', async () => {
    const { max, companies } = await seedTenant({ companyCount: 2 });
    registerScoreWatches(max, companies);
    max.policy.configureTenant(TENANT, {
      minimumConfidence: 0.1,
      maxEvidenceAgeDays: 0,
      cooldownHours: 0,
      blockedDays: [],
      approvalRequired: [],
    });

    let evaluateCalls = 0;
    const original = max.engine.evaluate.bind(max.engine);
    max.engine.evaluate = async (...args) => {
      evaluateCalls += 1;
      return original(...args);
    };

    await max.compose({
      tenantId: TENANT,
      asOf: AS_OF,
      period: 'daily',
      ...BRIEF_WINDOW,
    });

    assert.equal(evaluateCalls, 0);
  });
});

describe('CommandDeckComposer — IntelligenceCard contract', () => {
  it('every card implements the shared contract + explainability', async () => {
    const { max, companies } = await seedTenant({ companyCount: 3 });
    registerScoreWatches(max, companies);
    max.policy.configureTenant(TENANT, {
      minimumConfidence: 0.1,
      maxEvidenceAgeDays: 0,
      cooldownHours: 0,
      blockedDays: [],
      approvalRequired: [],
    });

    const deck = await max.compose({
      tenantId: TENANT,
      asOf: AS_OF,
      period: 'daily',
      ...BRIEF_WINDOW,
    });

    for (const card of deck.cards) {
      assert.ok(card.id, 'card.id');
      assert.ok(card.type, 'card.type');
      assert.ok(Number.isFinite(card.priority), 'card.priority');
      assert.equal(typeof card.title, 'string');
      assert.equal(typeof card.summary, 'string');
      assert.ok(Array.isArray(card.actions), 'card.actions');
      assert.ok(Array.isArray(card.sources), 'card.sources');
      assert.ok('reasoningId' in card);
      assert.ok('policyId' in card);
      assert.ok('briefingId' in card);
      assert.equal(card.briefingId, deck.meta.briefingId);
      for (const action of card.actions) {
        assert.ok(action.id);
        assert.ok(action.type);
        assert.ok(action.label);
      }
    }

    const hla = deck.cards.find((c) => c.type === CARD_TYPES.HIGHEST_LEVERAGE);
    if (hla) {
      const types = new Set(hla.actions.map((a) => a.type));
      assert.ok(types.has(ACTION_TYPES.REVIEW_RECOMMENDATION));
      assert.ok(types.has(ACTION_TYPES.ASK_MAX));
      assert.ok(types.has(ACTION_TYPES.OPEN_COMPANY));
      assert.ok(types.has(ACTION_TYPES.DISMISS));
      assert.ok(types.has(ACTION_TYPES.SNOOZE));
    }
  });

  it('buildIntelligenceCard rejects missing id/type', () => {
    assert.throws(() => buildIntelligenceCard({ type: 'x' }), /requires id/);
    assert.throws(() => buildIntelligenceCard({ id: 'x' }), /requires type/);
  });
});

describe('CommandDeckComposer — empty states', () => {
  it('composer owns empty states when tenant has no companies', async () => {
    const knowledge = createKnowledgeRuntime({
      withSync: false,
      startIngestor: false,
    }).knowledge;
    const max = createMaxReasoningRuntime({ knowledge, startIngestor: false });

    const deck = await max.compose({
      tenantId: TENANT,
      asOf: AS_OF,
      period: 'daily',
    });

    assert.equal(deck.highestLeverageAction, null);
    assert.equal(deck.priorityQueue.length, 0);
    assert.equal(deck.watchAlerts.length, 0);
    assert.ok(deck.emptyStates.priorities);
    assert.ok(deck.emptyStates.watchAlerts);
    assert.ok(deck.emptyStates.marketTrends);
    assert.ok(deck.emptyStates.highestLeverage);
    assert.equal(
      deck.emptyStates.priorities.title,
      EMPTY_CATALOG.priorities.title
    );
    assert.match(
      deck.emptyStates.priorities.summary,
      /Continue market discovery/
    );

    const emptyCards = deck.cards.filter((c) => c.type === CARD_TYPES.EMPTY);
    assert.ok(emptyCards.length >= 3);
  });
});

describe('CommandDeckComposer — watch alerts ordering', () => {
  it('dedupes and severity-ranks newest first', () => {
    const alerts = [
      {
        watchId: 'w1',
        companyId: 'c1',
        message: 'a',
        scoreDelta: 5,
        at: '2026-07-25T10:00:00.000Z',
      },
      {
        watchId: 'w1',
        companyId: 'c1',
        message: 'a',
        scoreDelta: 5,
        at: '2026-07-25T10:00:00.000Z',
      },
      {
        watchId: 'w2',
        companyId: 'c2',
        message: 'b',
        scoreDelta: 30,
        at: '2026-07-24T10:00:00.000Z',
      },
      {
        watchId: 'w3',
        companyId: 'c3',
        message: 'c',
        scoreDelta: 30,
        at: '2026-07-26T10:00:00.000Z',
      },
    ];
    assert.equal(dedupeAlerts(alerts).length, 3);
    assert.equal(deriveSeverity({ scoreDelta: 30 }), 'critical');
    assert.equal(deriveSeverity({ scoreDelta: 5 }), 'low');

    const { watchAlerts } = composeWatchAlerts({
      briefing: { watchAlerts: { items: alerts } },
      briefingId: 'briefing:test',
      generatedAt: AS_OF,
    });
    assert.equal(watchAlerts.length, 3);
    assert.equal(watchAlerts[0].payload.severity, 'critical');
    assert.equal(watchAlerts[0].payload.watchId, 'w3');
  });
});

describe('CommandDeckComposer — priority movement', () => {
  it('formats movement from score delta without UI math', () => {
    assert.equal(formatMovement(4), '↑4');
    assert.equal(formatMovement(-1), '↓1');
    assert.equal(formatMovement(0), '—');
  });
});

describe('CommandDeckComposer — determinism', () => {
  it('identical inputs produce identical models (stable asOf)', async () => {
    const { max, companies } = await seedTenant({ companyCount: 2 });
    registerScoreWatches(max, companies);
    max.policy.configureTenant(TENANT, {
      minimumConfidence: 0.1,
      maxEvidenceAgeDays: 0,
      cooldownHours: 0,
      blockedDays: [],
      approvalRequired: [],
    });

    const input = {
      tenantId: TENANT,
      asOf: AS_OF,
      period: 'daily',
      ...BRIEF_WINDOW,
    };
    const a = await max.compose(input);
    const b = await max.compose({
      ...input,
      briefing: JSON.parse(JSON.stringify(await max.brief(input))),
      evaluatePolicy: false,
      policyDecisions: Object.fromEntries(
        a.priorityQueue
          .filter((p) => p.recommendationId)
          .map((p) => [
            p.recommendationId,
            {
              allowed: true,
              requiresApproval: false,
              blocked: false,
              outcome: 'allow',
              severity: 'none',
              reason: 'fixture',
              audit: { id: `policy-audit:fixture:${p.recommendationId}` },
            },
          ])
      ),
    });

    // Recompose from same briefing with fixed policy fixtures
    const briefing = await max.brief(input);
    const c = await max.compose({
      ...input,
      briefing,
      evaluatePolicy: false,
      policyDecisions: Object.fromEntries(
        (briefing.priorities || []).map((p) => [
          p.id,
          {
            allowed: true,
            requiresApproval: false,
            blocked: false,
            outcome: 'allow',
            severity: 'none',
            reason: 'fixture',
            audit: { id: `policy-audit:fixture:${p.id}` },
          },
        ])
      ),
    });
    const d = await max.compose({
      ...input,
      briefing,
      evaluatePolicy: false,
      policyDecisions: Object.fromEntries(
        (briefing.priorities || []).map((p) => [
          p.id,
          {
            allowed: true,
            requiresApproval: false,
            blocked: false,
            outcome: 'allow',
            severity: 'none',
            reason: 'fixture',
            audit: { id: `policy-audit:fixture:${p.id}` },
          },
        ])
      ),
    });

    assert.deepEqual(
      stripVolatile(c),
      stripVolatile(d)
    );
    assert.ok(b.morningBrief);
  });
});

function stripVolatile(model) {
  const clone = JSON.parse(JSON.stringify(model));
  if (clone.meta) {
    delete clone.meta.buildTimeMs;
    delete clone.meta.withinTarget;
  }
  return clone;
}
