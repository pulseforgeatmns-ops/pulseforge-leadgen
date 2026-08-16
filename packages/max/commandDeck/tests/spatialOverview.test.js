'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  collectDomainSignals,
  summarizeMissions,
  buildElevationReason,
  inferHlaDomain,
  buildDomainSummary,
  DISTANCE_BY_PRIORITY,
  PRIORITY_STATES,
  DOMAIN_IDS,
} = require('../spatial/DomainPriority');
const { composeSpatialOverview } = require('../sections/SpatialOverview');

describe('Living Command Deck — domain priority', () => {
  it('keeps stable distance mapping for discrete priority bands', () => {
    assert.ok(DISTANCE_BY_PRIORITY.elevated < DISTANCE_BY_PRIORITY.normal);
    assert.ok(DISTANCE_BY_PRIORITY.urgent < DISTANCE_BY_PRIORITY.elevated);
    assert.ok(DISTANCE_BY_PRIORITY.monitored > DISTANCE_BY_PRIORITY.normal);
  });

  it('compresses historical blockers without elevating campaigns', () => {
    const summary = summarizeMissions([
      { id: '1', status: 'failed', title: 'Old failure' },
      { id: '2', status: 'archived', title: 'Old mission' },
      { id: '3', status: 'waiting', title: 'Blocked historical' },
    ]);
    assert.equal(summary.active, 0);
    assert.equal(summary.needsAttention, 1);
    assert.ok(summary.historicalContained >= 2);
  });

  it('elevates content when a pending Paige recommendation exists', () => {
    const signals = collectDomainSignals({
      missions: [],
      priorityQueue: [],
      watchAlerts: [],
      pendingRecommendations: [
        {
          id: 'rec-1',
          status: 'pending',
          title: 'LinkedIn launch post',
          requiresOperatorDecision: true,
        },
      ],
      activeObjectives: [],
    });
    assert.equal(signals.content.priority, PRIORITY_STATES.URGENT);
    const reason = buildElevationReason(DOMAIN_IDS.CONTENT, signals.content);
    assert.ok(reason);
    assert.ok(reason.reason);
    assert.equal(reason.evidenceRefs[0].kind, 'content_recommendation');
  });

  it('does not fake elevation when state is unchanged', async () => {
    const model = {
      meta: { generatedAt: '2026-08-16T12:00:00.000Z' },
      morningBrief: { headline: 'Quiet day', summary: 'Monitoring.' },
      priorityQueue: [],
      watchAlerts: [],
      marketTrends: [],
      operations: { missions: [] },
    };

    const overview = await composeSpatialOverview({
      model,
      pendingRecommendations: [],
      activeObjectives: [],
      reconcilePriority: async ({ computed }) => ({
        priority: computed,
        previousPriority: computed,
        transition: null,
      }),
    });

    assert.equal(overview.domains.length, 4);
    for (const domain of overview.domains) {
      assert.equal(domain.transition, null);
    }
  });

  it('records explainable Normal → Elevated transition', async () => {
    const model = {
      meta: { generatedAt: '2026-08-16T12:00:00.000Z' },
      morningBrief: { headline: 'Update', summary: 'Content needs review.' },
      priorityQueue: [],
      watchAlerts: [],
      operations: { missions: [] },
    };

    const overview = await composeSpatialOverview({
      model,
      pendingRecommendations: [
        {
          id: 'rec-9',
          status: 'pending',
          title: 'Paige recommendation',
          summary: 'LinkedIn experiment review',
        },
      ],
      reconcilePriority: async ({ domainId, computed }) => {
        if (domainId === DOMAIN_IDS.CONTENT && computed === PRIORITY_STATES.ELEVATED) {
          return {
            priority: computed,
            previousPriority: PRIORITY_STATES.NORMAL,
            transition: {
              domain: domainId,
              previousState: PRIORITY_STATES.NORMAL,
              newState: PRIORITY_STATES.ELEVATED,
              reason: 'Paige completed analysis of recent LinkedIn outcomes.',
              evidenceRefs: [{ kind: 'content_recommendation', id: 'rec-9' }],
              changedAt: '2026-08-16T16:27:00.000Z',
            },
          };
        }
        return { priority: computed, previousPriority: null, transition: null };
      },
    });

    const content = overview.domains.find((d) => d.id === DOMAIN_IDS.CONTENT);
    assert.equal(content.priority, PRIORITY_STATES.ELEVATED);
    assert.ok(content.transition);
    assert.equal(content.transition.previousState, PRIORITY_STATES.NORMAL);
    assert.match(content.transition.reason, /LinkedIn/i);
    assert.ok(content.intelligence.active);
  });

  it('supports multiple simultaneously elevated domains', async () => {
    const model = {
      meta: { generatedAt: '2026-08-16T12:00:00.000Z' },
      priorityQueue: [{ id: 'p1', companyName: 'Acme' }],
      watchAlerts: [{ id: 'w1', headline: 'Signal' }],
      operations: {
        missions: [{ id: 'm1', status: 'review_required', title: 'Campaign' }],
      },
    };

    const overview = await composeSpatialOverview({ model });
    const elevated = overview.domains.filter(
      (d) => d.priority === PRIORITY_STATES.ELEVATED || d.priority === PRIORITY_STATES.URGENT
    );
    assert.ok(elevated.length >= 2);
  });

  it('builds unseen transitions since last visit without replaying all animations', async () => {
    const model = {
      meta: { generatedAt: '2026-08-16T18:00:00.000Z' },
      operations: { missions: [] },
    };

    const overview = await composeSpatialOverview({
      model,
      pendingRecommendations: [{ id: 'r1', status: 'pending', title: 'Rec' }],
      lastVisitAt: '2026-08-16T10:00:00.000Z',
      reconcilePriority: async ({ domainId, computed }) => {
        if (domainId === DOMAIN_IDS.CONTENT) {
          return {
            priority: PRIORITY_STATES.ELEVATED,
            previousPriority: PRIORITY_STATES.NORMAL,
            transition: {
              domain: domainId,
              previousState: PRIORITY_STATES.NORMAL,
              newState: PRIORITY_STATES.ELEVATED,
              reason: 'Paige recommendation ready',
              changedAt: '2026-08-16T16:00:00.000Z',
            },
          };
        }
        return { priority: computed, previousPriority: null, transition: null };
      },
    });

    assert.equal(overview.unseenChanges.length, 1);
    assert.equal(overview.unseenChanges[0].domainId, DOMAIN_IDS.CONTENT);
  });

  it('infers HLA domain for contextual routing', () => {
    assert.equal(
      inferHlaDomain({
        highestLeverageAction: {
          recommendation: { recommendedAction: 'Review LinkedIn post draft' },
        },
      }),
      DOMAIN_IDS.CONTENT
    );
  });

  it('summarizes campaigns with contained historical label', () => {
    const summary = buildDomainSummary(DOMAIN_IDS.CAMPAIGNS, {
      summary: summarizeMissions(
        Array.from({ length: 13 }, (_, i) => ({
          id: String(i),
          status: 'failed',
          title: `Historical ${i}`,
        }))
      ),
    });
    assert.match(summary.compressed, /historical|contained|13/);
  });
});
