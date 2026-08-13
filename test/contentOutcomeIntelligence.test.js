'use strict';

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');

const {
  ContentOutcomeError,
  createMemoryStore,
  createContentPublication,
  addPerformanceSnapshot,
  addBusinessOutcome,
  addQualitativeSignal,
  getPublicationOutcome,
  getContentOutcomeTimeline,
  listContentOutcomes,
  getRecentContentOutcomes,
  compareContentOutcomes,
  getContentOutcomesForIntelligence,
} = require('../services/contentOutcomeIntelligence');

const { createOutcomeEngine } = require('../packages/max/outcome');

function opts(store, tenantId = '1') {
  return { store, tenantId };
}

describe('contentOutcomeIntelligence', () => {
  let store;

  beforeEach(() => {
    store = createMemoryStore();
  });

  it('creates a publication from a Paige content artifact', async () => {
    const pub = await createContentPublication(
      {
        tenant_id: 1,
        content_artifact_id: '42',
        channel: 'linkedin',
        published_at: '2026-08-10T12:00:00.000Z',
        objective: 'launch_runway',
        topic: 'software should learn you',
        thesis: 'Software should learn the operator',
        format: 'text',
        intended_audience: ['builders', 'SMB operators'],
      },
      opts(store)
    );

    assert.ok(pub.id);
    assert.equal(pub.tenant_id, '1');
    assert.equal(pub.content_artifact_id, '42');
    assert.equal(pub.channel, 'linkedin');
    assert.equal(pub.objective, 'launch_runway');
    assert.deepEqual(pub.intended_audience, ['builders', 'SMB operators']);
  });

  it('enforces tenant isolation on read/write', async () => {
    const pub = await createContentPublication(
      {
        tenant_id: 1,
        content_artifact_id: 'a1',
        published_at: '2026-08-01T00:00:00.000Z',
      },
      opts(store)
    );

    await assert.rejects(
      () =>
        getPublicationOutcome(pub.id, { store, tenantId: '2' }),
      (err) =>
        err instanceof ContentOutcomeError && err.code === 'publication_not_found'
    );

    await assert.rejects(
      () =>
        addPerformanceSnapshot(
          pub.id,
          { tenant_id: 2, impressions: 100 },
          opts(store, '2')
        ),
      (err) =>
        err instanceof ContentOutcomeError && err.code === 'publication_not_found'
    );
  });

  it('accepts partial performance metrics', async () => {
    const pub = await createContentPublication(
      {
        tenant_id: 1,
        content_artifact_id: 'p1',
        published_at: '2026-08-01T00:00:00.000Z',
      },
      opts(store)
    );

    const snap = await addPerformanceSnapshot(
      pub.id,
      {
        tenant_id: 1,
        observed_at: '2026-08-01T04:00:00.000Z',
        impressions: 8400,
      },
      opts(store)
    );

    assert.equal(snap.impressions, 8400);
    assert.equal(snap.reactions, null);
    assert.equal(snap.comments, null);
  });

  it('keeps performance snapshots immutable (no overwrite)', async () => {
    const pub = await createContentPublication(
      {
        tenant_id: 1,
        content_artifact_id: 'p2',
        published_at: '2026-08-01T00:00:00.000Z',
      },
      opts(store)
    );

    await addPerformanceSnapshot(
      pub.id,
      { tenant_id: 1, observed_at: '2026-08-01T04:00:00.000Z', impressions: 8400 },
      opts(store)
    );
    await addPerformanceSnapshot(
      pub.id,
      { tenant_id: 1, observed_at: '2026-08-02T00:00:00.000Z', impressions: 21300 },
      opts(store)
    );
    await addPerformanceSnapshot(
      pub.id,
      { tenant_id: 1, observed_at: '2026-08-08T00:00:00.000Z', impressions: 31900 },
      opts(store)
    );

    const full = await getPublicationOutcome(pub.id, opts(store));
    assert.equal(full.performanceSnapshots.length, 3);
    assert.deepEqual(
      full.performanceSnapshots.map((s) => s.impressions),
      [8400, 21300, 31900]
    );
  });

  it('supports multiple business outcomes with attribution', async () => {
    const pub = await createContentPublication(
      {
        tenant_id: 1,
        content_artifact_id: 'p3',
        published_at: '2026-08-01T00:00:00.000Z',
      },
      opts(store)
    );

    await addBusinessOutcome(
      pub.id,
      {
        tenant_id: 1,
        outcome_type: 'partner_conversation',
        attribution: 'direct',
        person_id: 'person:muhammad',
        interaction_id: 'interaction:dm-1',
        evidence_id: 'evidence:dm-screenshot',
        description: 'Explicit DM after seeing the post',
      },
      opts(store)
    );
    await addBusinessOutcome(
      pub.id,
      {
        tenant_id: 1,
        outcome_type: 'prospect_conversation',
        attribution: 'possible',
        company_id: 'company:acme',
        description: 'Contacted three days later',
      },
      opts(store)
    );

    const full = await getPublicationOutcome(pub.id, opts(store));
    assert.equal(full.businessOutcomes.length, 2);
    assert.equal(full.businessOutcomes[0].attribution, 'direct');
    assert.equal(full.businessOutcomes[1].attribution, 'possible');
    assert.equal(full.evidenceReferences.length, 1);
    assert.equal(full.evidenceReferences[0].evidence_id, 'evidence:dm-screenshot');
  });

  it('persists qualitative signals', async () => {
    const pub = await createContentPublication(
      {
        tenant_id: 1,
        content_artifact_id: 'p4',
        published_at: '2026-08-01T00:00:00.000Z',
      },
      opts(store)
    );

    const signal = await addQualitativeSignal(
      pub.id,
      {
        tenant_id: 1,
        signal_type: 'language_adoption',
        description: "Several commenters repeated 'software should learn you.'",
        audience_type: 'ai_engineers',
        evidence_id: 'evidence:comment-thread',
      },
      opts(store)
    );

    assert.equal(signal.signal_type, 'language_adoption');
    const full = await getPublicationOutcome(pub.id, opts(store));
    assert.equal(full.qualitativeSignals.length, 1);
    assert.match(full.qualitativeSignals[0].description, /software should learn you/);
  });

  it('returns a complete outcome timeline', async () => {
    const pub = await createContentPublication(
      {
        tenant_id: 1,
        content_artifact_id: 'p5',
        published_at: '2026-08-01T00:00:00.000Z',
        objective: 'thought_leadership',
      },
      opts(store)
    );
    await addPerformanceSnapshot(
      pub.id,
      { tenant_id: 1, observed_at: '2026-08-02T00:00:00.000Z', impressions: 1000 },
      opts(store)
    );
    await addBusinessOutcome(
      pub.id,
      {
        tenant_id: 1,
        outcome_type: 'meeting_booked',
        attribution: 'likely',
        occurred_at: '2026-08-03T00:00:00.000Z',
      },
      opts(store)
    );
    await addQualitativeSignal(
      pub.id,
      {
        tenant_id: 1,
        signal_type: 'buyer_signal',
        description: 'SMB operators responded to operator story',
        observed_at: '2026-08-02T12:00:00.000Z',
      },
      opts(store)
    );

    const timeline = await getContentOutcomeTimeline(pub.id, opts(store));
    assert.equal(timeline.timeline[0].kind, 'publication');
    assert.ok(timeline.timeline.some((e) => e.kind === 'performance_snapshot'));
    assert.ok(timeline.timeline.some((e) => e.kind === 'business_outcome'));
    assert.ok(timeline.timeline.some((e) => e.kind === 'qualitative_signal'));
  });

  it('computes deterministic comparison without vanity scores or strategy mutation', async () => {
    for (const [artifact, impressions, outcomeType] of [
      ['c1', 3000, 'prospect_conversation'],
      ['c2', 50000, null],
      ['c3', 8000, 'partner_conversation'],
      ['c4', 12000, 'meeting_booked'],
    ]) {
      const pub = await createContentPublication(
        {
          tenant_id: 1,
          content_artifact_id: artifact,
          published_at: '2026-08-01T00:00:00.000Z',
          objective: 'launch_runway',
          topic: 'max launch',
          format: 'text',
          intended_audience: ['builders'],
        },
        opts(store)
      );
      await addPerformanceSnapshot(
        pub.id,
        {
          tenant_id: 1,
          impressions,
          comments: impressions > 10000 ? 40 : 10,
        },
        opts(store)
      );
      if (outcomeType) {
        await addBusinessOutcome(
          pub.id,
          { tenant_id: 1, outcome_type: outcomeType, attribution: 'likely' },
          opts(store)
        );
      }
    }

    const comparison = await compareContentOutcomes(
      { tenantId: 1 },
      opts(store)
    );
    assert.equal(comparison.totalPublications, 4);
    assert.equal(comparison.medianImpressions, 10000);
    assert.equal(comparison.totalQualifiedConversations, 1);
    assert.equal(comparison.totalPartnerConversations, 1);
    assert.equal(comparison.totalMeetings, 1);
    assert.equal(comparison.recommendsStrategy, false);
    assert.equal(comparison.groupedBy.objective.launch_runway, 4);
    assert.ok(!('post_score' in comparison));
  });

  it('lists and returns recent outcomes for intelligence consumers', async () => {
    await createContentPublication(
      {
        tenant_id: 1,
        content_artifact_id: 'r1',
        published_at: '2026-08-10T00:00:00.000Z',
      },
      opts(store)
    );
    await createContentPublication(
      {
        tenant_id: 1,
        content_artifact_id: 'r2',
        published_at: '2026-08-11T00:00:00.000Z',
      },
      opts(store)
    );

    const recent = await getRecentContentOutcomes(1, 5, opts(store));
    assert.equal(recent.length, 2);
    assert.equal(recent[0].publication.content_artifact_id, 'r2');

    const intel = await getContentOutcomesForIntelligence(1, opts(store));
    assert.equal(intel.kind, 'content_outcome_intelligence');
    assert.equal(intel.isEvidence, true);
    assert.equal(intel.mutatesPaige, false);
    assert.equal(intel.recommendsStrategy, false);
    assert.equal(intel.recent.length, 2);
  });

  it('links to SPEC-013 Outcome Intelligence without mutating Paige', async () => {
    const outcomeEngine = createOutcomeEngine();
    const pub = await createContentPublication(
      {
        tenant_id: 1,
        content_artifact_id: 'spec013',
        published_at: '2026-08-01T00:00:00.000Z',
      },
      opts(store)
    );

    const outcome = await addBusinessOutcome(
      pub.id,
      {
        tenant_id: 1,
        outcome_type: 'builder_connection',
        attribution: 'direct',
        description: 'Builder reached out',
      },
      { store, outcomeEngine }
    );

    assert.ok(outcome.canonical_outcome_id);
    const recorded = outcomeEngine.get('1', `content:${pub.id}`);
    assert.ok(recorded);
    assert.equal(recorded.meta.kind, 'content_business_outcome');
    assert.equal(recorded.meta.publicationId, pub.id);

    // Recording does not invent Paige strategy fields.
    assert.equal(outcome.outcome_type, 'builder_connection');
    assert.ok(!outcome.paige_strategy);
    assert.ok(!outcome.strategy_mutation);
  });

  it('does not leak tenant A publications into tenant B list', async () => {
    await createContentPublication(
      {
        tenant_id: 1,
        content_artifact_id: 't1',
        published_at: '2026-08-01T00:00:00.000Z',
      },
      opts(store)
    );
    await createContentPublication(
      {
        tenant_id: 10,
        content_artifact_id: 't10',
        published_at: '2026-08-01T00:00:00.000Z',
      },
      opts(store)
    );

    const a = await listContentOutcomes({ tenantId: 1 }, opts(store));
    const b = await listContentOutcomes({ tenantId: 10 }, opts(store));
    assert.equal(a.length, 1);
    assert.equal(b.length, 1);
    assert.equal(a[0].publication.tenant_id, '1');
    assert.equal(b[0].publication.tenant_id, '10');
  });
});

describe('contentOutcomeIntelligence routes + migration', () => {
  it('registers expected API paths in the route module', () => {
    const src = fs.readFileSync(
      path.join(__dirname, '..', 'routes', 'contentOutcomeIntelligence.js'),
      'utf8'
    );
    for (const needle of [
      '/api/content-publications',
      '/api/content-publications/:id',
      '/api/content-publications/:id/performance',
      '/api/content-publications/:id/outcomes',
      '/api/content-publications/:id/signals',
      '/api/content-outcomes',
      '/api/content-outcomes/compare',
      '/api/v1/content-outcomes/intelligence',
      '/content-outcomes',
    ]) {
      assert.ok(src.includes(`'${needle}'`), `missing route path ${needle}`);
    }
    assert.match(src, /router\.post/);
    assert.match(src, /router\.get/);
  });

  it('ships additive migration with required tables', () => {
    const sql = fs.readFileSync(
      path.join(
        __dirname,
        '..',
        'migrations',
        '2026-08-13-content-outcome-intelligence.sql'
      ),
      'utf8'
    );
    assert.match(sql, /CREATE TABLE IF NOT EXISTS content_publications/);
    assert.match(sql, /CREATE TABLE IF NOT EXISTS content_performance_snapshots/);
    assert.match(sql, /CREATE TABLE IF NOT EXISTS content_business_outcomes/);
    assert.match(sql, /CREATE TABLE IF NOT EXISTS content_qualitative_signals/);
    assert.match(sql, /tenant_id TEXT NOT NULL/);
    assert.doesNotMatch(sql, /post_score/);
  });

  it('mounts the route from server.js', () => {
    const src = fs.readFileSync(
      path.join(__dirname, '..', 'server.js'),
      'utf8'
    );
    assert.match(src, /contentOutcomeIntelligence/);
  });
});
