'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');

const {
  createContentPublication,
  addPerformanceSnapshot,
  addBusinessOutcome,
  addQualitativeSignal,
} = require('../services/contentOutcomeIntelligence');

const {
  ContentLearningError,
  createLinkedMemoryStores,
  evaluateContentPublication,
  listContentLearnings,
  getContentLearning,
  getRelevantContentLearnings,
  generateContentRecommendation,
  recomputeContentLearnings,
  assessPublicationAgainstObjective,
  computeConfidence,
  statusFromEvidence,
  CONFIG,
} = require('../services/contentLearning');

function harness() {
  const { outcomeStore, learningStore } = createLinkedMemoryStores();
  return {
    outcomeStore,
    learningStore,
    opts: { store: outcomeStore, outcomeStore, learningStore },
  };
}

async function seedBreakout(opts, overrides = {}) {
  const pub = await createContentPublication(
    {
      clientId: 1,
      contentArtifactId: overrides.contentArtifactId || 'breakout-1',
      channel: 'linkedin',
      objective: overrides.objective || 'category_creation',
      topic: overrides.topic || 'Software should learn you',
      thesis: 'Software should learn the operator',
      format: 'text',
      intendedAudience: ['SMB operators', 'AI builders'],
      campaignId: 'max-launch-runway',
      publishedAt: '2026-08-01T12:00:00Z',
    },
    opts
  );

  await addPerformanceSnapshot(
    pub.id,
    {
      clientId: 1,
      observedAt: '2026-08-08T00:00:00Z',
      impressions: 18750,
      membersReached: 12645,
      comments: 181,
      followersGained: 49,
      profileViewsAttributed: 108,
      metadata: { outOfNetworkPct: 97 },
    },
    opts
  );

  const outcomeSpecs = overrides.outcomes || [
    { outcomeType: 'partner_conversation', attribution: 'direct' },
    { outcomeType: 'partner_conversation', attribution: 'likely' },
    { outcomeType: 'builder_connection', attribution: 'direct' },
    { outcomeType: 'prospect_conversation', attribution: 'possible' },
    { outcomeType: 'qualified_dm', attribution: 'likely' },
  ];
  // Expand to ~20 outcomes if requested for smoke parity
  const expanded = overrides.expandOutcomes
    ? Array.from({ length: 20 }, (_, i) => outcomeSpecs[i % outcomeSpecs.length])
    : outcomeSpecs;

  for (const o of expanded) {
    await addBusinessOutcome(
      pub.id,
      {
        clientId: 1,
        outcomeType: o.outcomeType,
        attribution: o.attribution,
        description: o.description || 'Inbound after post',
      },
      opts
    );
  }

  await addQualitativeSignal(
    pub.id,
    {
      clientId: 1,
      signalType: 'language_adoption',
      description: "Several people repeated the phrase 'software should learn you.'",
      audienceType: 'AI builder',
    },
    opts
  );
  await addQualitativeSignal(
    pub.id,
    {
      clientId: 1,
      signalType: 'message_resonance',
      description: 'Technical respondents independently discussed confidence and correction.',
      audienceType: 'engineer',
    },
    opts
  );
  await addQualitativeSignal(
    pub.id,
    {
      clientId: 1,
      signalType: 'audience_signal',
      description: 'SMB operators focused on operational implications.',
      audienceType: 'SMB operator',
    },
    opts
  );
  await addQualitativeSignal(
    pub.id,
    {
      clientId: 1,
      signalType: 'technical_interest',
      description: 'Engineers initiated employment conversations.',
      audienceType: 'engineer',
    },
    opts
  );

  return pub;
}

describe('contentLearning (SPEC-093)', () => {
  it('evaluates breakout post relative to category_creation with high observation / low generalization', async () => {
    const { opts } = harness();
    const pub = await seedBreakout(opts, { expandOutcomes: true });
    const result = await evaluateContentPublication(pub.id, { ...opts, clientId: 1 });

    assert.equal(result.assessment.objective, 'category_creation');
    assert.ok(result.assessment.observationConfidence >= 0.7);
    assert.ok(result.assessment.generalizationConfidence < 0.4);
    assert.equal(result.assessment.confidenceLabel.observation, 'high');
    assert.equal(result.assessment.confidenceLabel.generalization, 'low');
    assert.match(result.assessment.assessment, /category-level discovery|broad category/i);
    assert.equal(result.guardrails.universalContentScore, null);
    assert.equal(result.guardrails.autonomousPublish, false);
  });

  it('same metrics under lead_generation produce a different assessment lens', async () => {
    const { opts } = harness();
    const pub = await seedBreakout(opts, { objective: 'lead_generation' });
    const result = await evaluateContentPublication(pub.id, { ...opts, clientId: 1 });
    assert.equal(result.assessment.objective, 'lead_generation');
    assert.match(result.assessment.assessment, /Lead-generation lens/i);
    assert.ok(result.assessment.lens.includes('qualified_dms'));
  });

  it('single breakout cannot create supported generalized learning', async () => {
    const { opts } = harness();
    const pub = await seedBreakout(opts);
    const result = await evaluateContentPublication(pub.id, { ...opts, clientId: 1 });
    const dist = result.learnings.find((l) => l.learningType === 'distribution_pattern');
    assert.ok(dist);
    assert.equal(dist.status, 'signal');
    assert.equal(dist.sampleSize, 1);
    assert.ok(dist.generalizationConfidence < CONFIG.supportedMinGeneralization);
    assert.ok(dist.supportingPublicationIds.includes(pub.id));
  });

  it('persists learnings and retrieves them with tenant isolation', async () => {
    const { opts } = harness();
    const pub = await seedBreakout(opts);
    await evaluateContentPublication(pub.id, { ...opts, clientId: 1 });
    const list = await listContentLearnings({ clientId: 1 }, opts);
    assert.ok(list.length >= 1);
    const one = await getContentLearning(list[0].id, { ...opts, clientId: 1 });
    assert.equal(one.id, list[0].id);

    await assert.rejects(
      () => getContentLearning(list[0].id, { ...opts, clientId: 2 }),
      (err) => err instanceof ContentLearningError && err.code === 'learning_not_found'
    );
    const other = await listContentLearnings({ clientId: 2 }, opts);
    assert.equal(other.length, 0);
  });

  it('weights attribution — direct/likely/possible/unknown remain distinct in evidence', async () => {
    const { opts } = harness();
    const pub = await seedBreakout(opts);
    const result = await evaluateContentPublication(pub.id, { ...opts, clientId: 1 });
    assert.match(result.assessment.attributionSummary, /direct/);
    assert.match(result.assessment.attributionSummary, /likely/);
    const partnership = result.learnings.find((l) => l.learningType === 'partnership_signal');
    assert.ok(partnership);
    assert.match(partnership.evidenceSummary, /direct|likely|possible|unknown/i);
    assert.match(partnership.uncertaintySummary, /attribution/i);
  });

  it('keeps audience classes distinguishable and business outcomes distinct from engagement', async () => {
    const { opts } = harness();
    const pub = await seedBreakout(opts);
    const result = await evaluateContentPublication(pub.id, { ...opts, clientId: 1 });
    const audience = result.learnings.find((l) => l.learningType === 'audience_response');
    assert.ok(audience);
    assert.match(audience.statement, /SMB_operator|AI_builder|engineer/);
    assert.ok(result.outcomeRef.businessOutcomes >= 1);
    assert.ok(result.assessment.observed.comments != null);
    assert.notEqual(
      result.assessment.observed.businessOutcomeCount,
      result.assessment.observed.comments
    );
  });

  it('deterministic confidence: identical evidence yields identical confidence', () => {
    const a = computeConfidence({
      observationStrength: 0.8,
      supportingCount: 1,
      contradictingCount: 0,
      attributionQuality: 0.7,
      daysSinceEvidence: 0,
      objectiveConsistent: true,
    });
    const b = computeConfidence({
      observationStrength: 0.8,
      supportingCount: 1,
      contradictingCount: 0,
      attributionQuality: 0.7,
      daysSinceEvidence: 0,
      objectiveConsistent: true,
    });
    assert.deepEqual(a, b);
  });

  it('contradicting publications reduce status/confidence', async () => {
    const { opts } = harness();
    const pub1 = await seedBreakout(opts, { contentArtifactId: 'a' });
    await evaluateContentPublication(pub1.id, { ...opts, clientId: 1 });

    const pub2 = await createContentPublication(
      {
        clientId: 1,
        contentArtifactId: 'weak',
        channel: 'linkedin',
        objective: 'category_creation',
        topic: 'Software should learn you',
        format: 'text',
        publishedAt: '2026-08-15T12:00:00Z',
      },
      opts
    );
    await addPerformanceSnapshot(
      pub2.id,
      {
        clientId: 1,
        impressions: 900,
        metadata: { outOfNetworkPct: 20 },
      },
      opts
    );
    await evaluateContentPublication(pub2.id, { ...opts, clientId: 1 });

    const list = await listContentLearnings(
      { clientId: 1, learningType: 'distribution_pattern' },
      opts
    );
    const dist = list.find((l) => l.fingerprint.includes('strong_discovery'));
    assert.ok(dist);
    assert.ok(dist.contradictingPublicationIds.includes(pub2.id));
    assert.ok(
      dist.status === 'contradicted' ||
        dist.generalizationConfidence < 0.55
    );
  });

  it('second supporting publication promotes signal → emerging', async () => {
    const { opts } = harness();
    const pub1 = await seedBreakout(opts, { contentArtifactId: 's1' });
    await evaluateContentPublication(pub1.id, { ...opts, clientId: 1 });

    const pub2 = await createContentPublication(
      {
        clientId: 1,
        contentArtifactId: 's2',
        channel: 'linkedin',
        objective: 'category_creation',
        topic: 'Operators deserve software that adapts',
        format: 'text',
        publishedAt: '2026-08-20T12:00:00Z',
      },
      opts
    );
    await addPerformanceSnapshot(
      pub2.id,
      {
        clientId: 1,
        impressions: 15000,
        membersReached: 9000,
        followersGained: 40,
        metadata: { outOfNetworkPct: 88 },
      },
      opts
    );
    // Match fingerprint: same objective/format/channel/pattern; topic differs but fingerprint uses topic —
    // For emerging on same patternKey we need same fingerprint. Use same topic for thin-slice match.
    // Re-seed with same topic:
    const pub3 = await createContentPublication(
      {
        clientId: 1,
        contentArtifactId: 's3',
        channel: 'linkedin',
        objective: 'category_creation',
        topic: 'Software should learn you',
        format: 'text',
        publishedAt: '2026-08-21T12:00:00Z',
      },
      opts
    );
    await addPerformanceSnapshot(
      pub3.id,
      {
        clientId: 1,
        impressions: 16000,
        membersReached: 10000,
        metadata: { outOfNetworkPct: 90 },
      },
      opts
    );
    await evaluateContentPublication(pub3.id, { ...opts, clientId: 1 });

    const list = await listContentLearnings(
      { clientId: 1, learningType: 'distribution_pattern' },
      opts
    );
    const dist = list.find((l) => l.supportingPublicationIds.includes(pub1.id));
    assert.ok(dist);
    assert.ok(dist.sampleSize >= 2);
    assert.equal(dist.status, 'emerging');
    assert.notEqual(dist.status, 'supported');
  });

  it('recommendation cites evidence, defines experiment, and does not clone', async () => {
    const { opts } = harness();
    const pub = await seedBreakout(opts);
    await evaluateContentPublication(pub.id, { ...opts, clientId: 1 });

    const rec = await generateContentRecommendation(
      {
        clientId: 1,
        objective:
          'Build qualified attention and category understanding before the public Max reveal.',
        learningObjective: 'category_creation',
        channel: 'linkedin',
        campaignId: 'max-launch-runway',
      },
      opts
    );

    assert.ok(rec.supporting_learning_ids.length >= 1);
    assert.ok(rec.supporting_publication_ids.includes(pub.id));
    assert.ok(rec.experiment);
    assert.ok(rec.experiment.hypothesis);
    assert.ok(rec.experiment.preserve.length >= 1);
    assert.ok(rec.experiment.vary.includes('specific argument'));
    assert.ok(rec.uncertainties.length >= 1);
    assert.equal(rec.autonomousPublish, false);
    assert.equal(rec.operatorAuthority, true);
    assert.doesNotMatch(
      rec.recommended_direction.toLowerCase(),
      /^software should learn you$/
    );
    assert.match(rec.reason, /learning|SPEC-092|evidence/i);
  });

  it('recompute re-evaluates as evidence changes without mutating publish state', async () => {
    const { opts } = harness();
    const pub = await seedBreakout(opts);
    const first = await recomputeContentLearnings({ clientId: 1 }, opts);
    assert.equal(first.publicationsEvaluated, 1);
    assert.ok(first.learningCount >= 1);
    assert.equal(first.evaluations[0].guardrails.autonomousStrategyMutation, false);

    await addBusinessOutcome(
      pub.id,
      {
        clientId: 1,
        outcomeType: 'meeting_booked',
        attribution: 'direct',
      },
      opts
    );
    const second = await recomputeContentLearnings({ clientId: 1 }, opts);
    assert.equal(second.publicationsEvaluated, 1);
    assert.ok(second.learningCount >= 1);
  });

  it('relevant retrieval prioritizes objective/channel match', async () => {
    const { opts } = harness();
    const pub = await seedBreakout(opts);
    await evaluateContentPublication(pub.id, { ...opts, clientId: 1 });
    const relevant = await getRelevantContentLearnings(
      {
        tenantId: 1,
        objective: 'category_creation',
        channel: 'linkedin',
        limit: 5,
      },
      opts
    );
    assert.ok(relevant.length >= 1);
    assert.ok(relevant[0].retrievalScore >= relevant[relevant.length - 1].retrievalScore);
  });

  it('statusFromEvidence never supports n=1', () => {
    assert.equal(
      statusFromEvidence({
        supportingCount: 1,
        contradictingCount: 0,
        generalizationConfidence: 0.9,
        daysSinceEvidence: 0,
      }),
      'signal'
    );
  });

  it('assessPublicationAgainstObjective remains pure/deterministic', async () => {
    const { opts } = harness();
    const pub = await seedBreakout(opts);
    const {
      getPublicationOutcome,
    } = require('../services/contentOutcomeIntelligence');
    const full = await getPublicationOutcome(pub.id, { ...opts, clientId: 1 });
    const a = assessPublicationAgainstObjective(full);
    const b = assessPublicationAgainstObjective(full);
    assert.deepEqual(a, b);
  });
});

describe('contentLearning docs/migration (static)', () => {
  it('ships spec, migration, and rollback', () => {
    const root = path.join(__dirname, '..');
    assert.ok(
      fs.existsSync(
        path.join(root, 'docs/specs/SPEC-093_Paige_Outcome_Learning_Loop.md')
      )
    );
    const forward = fs.readFileSync(
      path.join(root, 'migrations/2026-08-13-paige-outcome-learning.sql'),
      'utf8'
    );
    const rollback = fs.readFileSync(
      path.join(root, 'migrations/2026-08-13-paige-outcome-learning.rollback.sql'),
      'utf8'
    );
    assert.match(forward, /content_learnings/);
    assert.match(forward, /observation_confidence/);
    assert.match(rollback, /DROP TABLE IF EXISTS content_learnings/);
  });
});
