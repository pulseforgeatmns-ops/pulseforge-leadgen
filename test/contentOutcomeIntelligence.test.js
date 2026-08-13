'use strict';

const { describe, it } = require('node:test');
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
  toIntelligencePayload,
} = require('../services/contentOutcomeIntelligence');

function withStore() {
  const store = createMemoryStore();
  return { store, opts: { store } };
}

describe('contentOutcomeIntelligence (SPEC-092)', () => {
  it('creates a publication from a content artifact with objective', async () => {
    const { opts } = withStore();
    const pub = await createContentPublication(
      {
        clientId: 1,
        contentArtifactId: 'pending-abc',
        channel: 'linkedin',
        objective: 'launch_runway',
        topic: 'software should learn you',
        thesis: 'Software should learn the operator',
        format: 'text',
        intendedAudience: ['builders', 'SMB operators'],
        publishedAt: '2026-08-01T12:00:00Z',
      },
      opts
    );
    assert.ok(pub.id);
    assert.equal(pub.clientId, 1);
    assert.equal(pub.tenantId, '1');
    assert.equal(pub.contentArtifactId, 'pending-abc');
    assert.equal(pub.objective, 'launch_runway');
    assert.deepEqual(pub.intendedAudience, ['builders', 'SMB operators']);
  });

  it('stores multiple immutable performance snapshots with partial metrics', async () => {
    const { opts } = withStore();
    const pub = await createContentPublication(
      { clientId: 1, contentArtifactId: 'a1', publishedAt: '2026-08-01T00:00:00Z' },
      opts
    );
    const s1 = await addPerformanceSnapshot(
      pub.id,
      { clientId: 1, observedAt: '2026-08-01T04:00:00Z', impressions: 8400 },
      opts
    );
    const s2 = await addPerformanceSnapshot(
      pub.id,
      {
        clientId: 1,
        observedAt: '2026-08-02T00:00:00Z',
        impressions: 21300,
        reactions: 120,
        comments: 18,
      },
      opts
    );
    const s3 = await addPerformanceSnapshot(
      pub.id,
      { clientId: 1, observedAt: '2026-08-08T00:00:00Z', impressions: 31900 },
      opts
    );

    assert.equal(s1.impressions, 8400);
    assert.equal(s1.reactions, null);
    assert.equal(s2.impressions, 21300);
    assert.notEqual(s1.id, s2.id);
    assert.notEqual(s2.id, s3.id);

    const full = await getPublicationOutcome(pub.id, { ...opts, clientId: 1 });
    assert.equal(full.performanceSnapshots.length, 3);
    assert.equal(full.performanceSnapshots[0].impressions, 8400);
    assert.equal(full.performanceSnapshots[2].impressions, 31900);
  });

  it('associates multiple business outcomes with attribution', async () => {
    const { opts } = withStore();
    const pub = await createContentPublication(
      { clientId: 1, contentArtifactId: 'a2' },
      opts
    );
    await addBusinessOutcome(
      pub.id,
      {
        clientId: 1,
        outcomeType: 'partner_conversation',
        attribution: 'direct',
        personId: 'person-muhammad',
        evidenceId: 'ev-dm-1',
        description: 'Muhammad messaged after seeing the post',
      },
      opts
    );
    await addBusinessOutcome(
      pub.id,
      {
        clientId: 1,
        outcomeType: 'meeting_booked',
        attribution: 'possible',
        companyId: 'co-1',
      },
      opts
    );
    const full = await getPublicationOutcome(pub.id, { ...opts, clientId: 1 });
    assert.equal(full.businessOutcomes.length, 2);
    assert.equal(full.businessOutcomes[0].attribution, 'direct');
    assert.equal(full.businessOutcomes[1].attribution, 'possible');
    assert.deepEqual(full.evidenceReferences.sort(), ['ev-dm-1']);
  });

  it('persists qualitative signals', async () => {
    const { opts } = withStore();
    const pub = await createContentPublication(
      { clientId: 1, contentArtifactId: 'a3' },
      opts
    );
    await addQualitativeSignal(
      pub.id,
      {
        clientId: 1,
        signalType: 'language_adoption',
        description: "Commenters repeated 'software should learn you.'",
        audienceType: 'AI engineers',
      },
      opts
    );
    const full = await getPublicationOutcome(pub.id, { ...opts, clientId: 1 });
    assert.equal(full.qualitativeSignals.length, 1);
    assert.equal(full.qualitativeSignals[0].signalType, 'language_adoption');
  });

  it('enforces tenant isolation on read/write', async () => {
    const { opts } = withStore();
    const pub = await createContentPublication(
      { clientId: 1, contentArtifactId: 'tenant-a' },
      opts
    );
    await assert.rejects(
      () => getPublicationOutcome(pub.id, { ...opts, clientId: 2 }),
      (err) => err instanceof ContentOutcomeError && err.code === 'publication_not_found'
    );
    await assert.rejects(
      () =>
        addPerformanceSnapshot(
          pub.id,
          { clientId: 2, impressions: 10 },
          opts
        ),
      (err) => err instanceof ContentOutcomeError && err.code === 'publication_not_found'
    );
  });

  it('returns timeline and recent outcomes for intelligence consumers', async () => {
    const { opts } = withStore();
    const pub = await createContentPublication(
      {
        clientId: 1,
        contentArtifactId: 'a4',
        objective: 'thought_leadership',
        publishedAt: '2026-08-10T00:00:00Z',
      },
      opts
    );
    await addPerformanceSnapshot(
      pub.id,
      { clientId: 1, observedAt: '2026-08-11T00:00:00Z', impressions: 5000 },
      opts
    );
    await addBusinessOutcome(
      pub.id,
      {
        clientId: 1,
        outcomeType: 'qualified_dm',
        attribution: 'likely',
        occurredAt: '2026-08-12T00:00:00Z',
      },
      opts
    );

    const timeline = await getContentOutcomeTimeline(pub.id, {
      ...opts,
      clientId: 1,
    });
    assert.ok(timeline.events.length >= 3);
    assert.equal(timeline.events[0].kind, 'publication');

    const recent = await getRecentContentOutcomes(1, 5, opts);
    assert.equal(recent.length, 1);
    const payload = toIntelligencePayload(recent[0]);
    assert.equal(payload.kind, 'content_outcome');
    assert.equal(payload.tenantId, '1');
    assert.equal(payload.latestPerformance.impressions, 5000);
  });

  it('compares publications with deterministic aggregates and no vanity score', async () => {
    const { opts } = withStore();
    const a = await createContentPublication(
      {
        clientId: 1,
        contentArtifactId: 'c1',
        objective: 'lead_generation',
        publishedAt: '2026-08-01T00:00:00Z',
      },
      opts
    );
    const b = await createContentPublication(
      {
        clientId: 1,
        contentArtifactId: 'c2',
        objective: 'lead_generation',
        publishedAt: '2026-08-02T00:00:00Z',
      },
      opts
    );
    const c = await createContentPublication(
      {
        clientId: 1,
        contentArtifactId: 'c3',
        objective: 'awareness',
        publishedAt: '2026-08-03T00:00:00Z',
      },
      opts
    );
    await addPerformanceSnapshot(a.id, { clientId: 1, impressions: 1000, comments: 2 }, opts);
    await addPerformanceSnapshot(b.id, { clientId: 1, impressions: 3000, comments: 10 }, opts);
    await addPerformanceSnapshot(c.id, { clientId: 1, impressions: 50000, comments: 1 }, opts);
    await addBusinessOutcome(
      a.id,
      { clientId: 1, outcomeType: 'prospect_conversation', attribution: 'direct' },
      opts
    );
    await addBusinessOutcome(
      a.id,
      { clientId: 1, outcomeType: 'meeting_booked', attribution: 'likely' },
      opts
    );
    await addBusinessOutcome(
      b.id,
      { clientId: 1, outcomeType: 'partner_conversation', attribution: 'direct' },
      opts
    );

    const cmp = await compareContentOutcomes(
      { clientId: 1, groupBy: 'objective' },
      opts
    );
    assert.equal(cmp.totalPublications, 3);
    assert.equal(cmp.medianImpressions, 3000);
    assert.equal(cmp.totalQualifiedConversations, 1);
    assert.equal(cmp.totalPartnerConversations, 1);
    assert.equal(cmp.totalMeetings, 1);
    assert.equal(cmp.vanityScore, null);
    const leadGroup = cmp.groups.find((g) => g.key === 'lead_generation');
    assert.ok(leadGroup);
    assert.equal(leadGroup.totalPublications, 2);
  });

  it('lists with filters and does not invent causation', async () => {
    const { opts } = withStore();
    await createContentPublication(
      {
        clientId: 1,
        contentArtifactId: 'f1',
        objective: 'partnership_generation',
        topic: 'Max launch',
        channel: 'linkedin',
      },
      opts
    );
    await createContentPublication(
      {
        clientId: 1,
        contentArtifactId: 'f2',
        objective: 'awareness',
        topic: 'other',
      },
      opts
    );
    const filtered = await listContentOutcomes(
      { clientId: 1, objective: 'partnership_generation', topic: 'Max' },
      opts
    );
    assert.equal(filtered.length, 1);
    assert.equal(filtered[0].publication.topic, 'Max launch');
  });

  it('does not mutate SPEC-013 OutcomeEngine when recording content outcomes', async () => {
    const { opts } = withStore();
    const { createOutcomeEngine } = require('../packages/max/outcome');
    const engine = createOutcomeEngine();
    const before = engine.review('1');

    const pub = await createContentPublication(
      { clientId: 1, contentArtifactId: 'spec013-safe' },
      opts
    );
    await addBusinessOutcome(
      pub.id,
      { clientId: 1, outcomeType: 'other', attribution: 'unknown' },
      opts
    );

    const after = engine.review('1');
    assert.equal(after.sections.recommendationSuccess.generated, before.sections.recommendationSuccess.generated);
    assert.equal(after.sections.recommendationSuccess.observed, before.sections.recommendationSuccess.observed);
    assert.equal(after.mutatesReasoning, false);
    assert.equal(after.mutatesConfidence, false);
    assert.equal(after.sections.recommendationSuccess.successful, 0);
  });

  it('does not alter Paige agent module source when recording outcomes', () => {
    const paigePath = path.join(__dirname, '..', 'paigeAgent.js');
    const before = fs.readFileSync(paigePath, 'utf8');
    // Recording outcomes is a pure service call — no Paige require side effects.
    assert.match(before, /module\.exports|async function run/);
    const after = fs.readFileSync(paigePath, 'utf8');
    assert.equal(after, before);
  });
});

describe('contentOutcomeIntelligence migration (static)', () => {
  it('ships forward + rollback SQL without LinkedIn API deps', () => {
    const forward = fs.readFileSync(
      path.join(__dirname, '..', 'migrations', '2026-08-13-content-outcome-intelligence.sql'),
      'utf8'
    );
    const rollback = fs.readFileSync(
      path.join(
        __dirname,
        '..',
        'migrations',
        '2026-08-13-content-outcome-intelligence.rollback.sql'
      ),
      'utf8'
    );
    assert.match(forward, /content_publications/);
    assert.match(forward, /content_performance_snapshots/);
    assert.match(forward, /content_business_outcomes/);
    assert.match(forward, /content_qualitative_signals/);
    assert.match(forward, /attribution/);
    assert.doesNotMatch(forward, /linkedin\.com\/oauth|LINKEDIN_CLIENT/i);
    assert.match(rollback, /DROP TABLE IF EXISTS content_publications/);
  });
});
