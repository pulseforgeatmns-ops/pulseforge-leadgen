'use strict';

/**
 * SPEC-113 — Acquisition Intelligence Compiler.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const aic = require('../index');
const {
  WORKSPACE_STATUS,
  CONCEPT_TYPES,
  RELATIONS,
  loadFixtureDocuments,
  createCompiler,
  createMemoryAicStore,
} = require('../index');
const { createMemoryAimStore, qualifyProspect, isRuntimeAim, AIM_STATUS } = require('../../aim');

function hiringProspect() {
  return {
    id: 'co-north-loop',
    name: 'North Loop Agency',
    industry: 'founder-led agency',
    jobTitle: 'Founder',
    description: 'Owner-operated. I do everything myself.',
    signals: [
      {
        type: 'hiring',
        source: 'careers',
        observedAt: '2026-08-01T00:00:00.000Z',
        label: 'Hiring repeatedly; job postings on the career page.',
      },
      {
        type: 'reviews',
        source: 'google_reviews',
        observedAt: '2026-08-04T00:00:00.000Z',
        label: 'Owner operational — founder replies to reviews.',
      },
    ],
  };
}

function compileFixtures(clientKey = 'fedir-aic') {
  const compiler = createCompiler({
    store: createMemoryAicStore(),
    aimStore: createMemoryAimStore({ seedFedir: false }),
  });
  const workspace = compiler.ingestAndCompile(
    { clientKey, clientName: 'Fedir' },
    loadFixtureDocuments()
  );
  return { compiler, workspace };
}

describe('SPEC-113 Stage 1 Ingestion', () => {
  it('uploads market documents into a draft workspace', () => {
    const compiler = createCompiler();
    const created = compiler.createWorkspace({ clientKey: 'fedir-aic', clientName: 'Fedir' });
    assert.equal(created.status, WORKSPACE_STATUS.NEW);
    assert.equal(created.executesOutreach, false);
    assert.equal(created.isOperatingFact, false);
    const ingested = compiler.addDocuments(created.id, loadFixtureDocuments());
    assert.equal(ingested.status, WORKSPACE_STATUS.INGESTING);
    assert.equal(ingested.documents.length, 3);
    assert.ok(ingested.documents.some((d) => /Pain Points v3/i.test(d.title)));
    assert.ok(ingested.documents.every((d) => d.body && d.kind));
  });

  it('rejects empty ingestion', () => {
    const compiler = createCompiler();
    const created = compiler.createWorkspace({ clientKey: 'empty' });
    assert.throws(
      () => compiler.addDocuments(created.id, [{ title: 'Blank', body: '' }]),
      (err) => err.code === 'aic_empty_document'
    );
  });
});

describe('SPEC-113 Stage 2 Extraction', () => {
  it('extracts concepts, not summaries, with source provenance', () => {
    const { workspace } = compileFixtures();
    const types = new Set(workspace.concepts.map((c) => c.type));
    assert.ok(types.has(CONCEPT_TYPES.MISSION));
    assert.ok(types.has(CONCEPT_TYPES.TRANSFORMATION));
    assert.ok(types.has(CONCEPT_TYPES.ICP));
    assert.ok(types.has(CONCEPT_TYPES.PAIN));
    assert.ok(types.has(CONCEPT_TYPES.OBSERVABLE_SIGNAL));
    const mission = workspace.concepts.find((c) => c.type === CONCEPT_TYPES.MISSION);
    assert.match(mission.statement, /founder-led businesses into business-machine/i);
    assert.equal(mission.provenance.documentTitle, 'Pain Points v3');
    assert.ok(mission.evidenceExcerpt);
    const pain = workspace.concepts.find((c) => /founder dependency/i.test(c.label));
    assert.ok(pain);
    assert.equal(pain.provenance.source, 'document');
    assert.match(pain.statement, /cannot operate without the founder/i);
  });

  it('keeps unknowns instead of inventing geography or proof', () => {
    const { workspace } = compileFixtures();
    assert.ok(workspace.unknowns.some((u) => /geograph|revenue/i.test(u)));
    assert.equal(
      workspace.concepts.some((c) => /nashville|\$10k/i.test(c.statement)),
      false
    );
  });
});

describe('SPEC-113 Stage 3 Ontology', () => {
  it('relates Founder Dependency → Hiring → Job Posts with confidence', () => {
    const { workspace } = compileFixtures();
    const founder = workspace.concepts.find((c) => /founder dependency/i.test(c.label));
    const hiring = workspace.concepts.find(
      (c) => c.type === CONCEPT_TYPES.PAIN && /^hiring$/i.test(c.label)
    );
    const jobs = workspace.concepts.find((c) => /job posts?/i.test(c.label));
    assert.ok(founder && hiring && jobs);
    const supported = workspace.edges.find(
      (e) => e.from === founder.id && e.to === hiring.id && e.relation === RELATIONS.SUPPORTED_BY
    );
    const observed = workspace.edges.find(
      (e) => e.from === founder.id && e.to === jobs.id && e.relation === RELATIONS.OBSERVED_THROUGH
    );
    assert.ok(supported, 'expected supported_by Hiring');
    assert.ok(observed, 'expected observed_through Job Posts');
    assert.ok(founder.confidence >= 0.8);
    const explained = aic.explainConcept(workspace, founder.id);
    assert.equal(explained.provenance.documentTitle, 'Pain Points v3');
    assert.ok(explained.relations.some((r) => r.relation === RELATIONS.SUPPORTED_BY));
  });
});

describe('SPEC-113 Stage 4 Human review', () => {
  it('accepts, edits, merges, and removes concepts', () => {
    const { compiler, workspace } = compileFixtures();
    const pain = workspace.concepts.find((c) => /founder dependency/i.test(c.label));
    const signal = workspace.concepts.find((c) => /burnout language/i.test(c.label));
    const hiring = workspace.concepts.find(
      (c) => c.type === CONCEPT_TYPES.PAIN && /^hiring$/i.test(c.label)
    );
    compiler.review(workspace.id, pain.id, { action: 'accept' });
    compiler.review(workspace.id, signal.id, {
      action: 'edit',
      statement: 'Burnout language in founder interviews.',
    });
    const extra = workspace.concepts.find((c) => /recruiting/i.test(c.label));
    compiler.review(workspace.id, hiring.id, {
      action: 'merge',
      absorbedIds: extra ? [extra.id] : [],
      statement: extra ? `${hiring.statement} Recruiting is visible.` : hiring.statement,
    });
    const jobs = workspace.concepts.find((c) => /job posts?/i.test(c.label));
    compiler.review(workspace.id, jobs.id, { action: 'remove' });
    const reviewed = compiler.getWorkspace(workspace.id);
    assert.equal(reviewed.concepts.find((c) => c.id === pain.id).status, 'accepted');
    assert.equal(reviewed.concepts.find((c) => c.id === signal.id).status, 'edited');
    assert.equal(reviewed.concepts.find((c) => c.id === jobs.id).status, 'removed');
    assert.ok(reviewed.reviews.length >= 3);
  });

  it('does not publish automatically after compile', () => {
    const { workspace } = compileFixtures();
    assert.equal(workspace.status, WORKSPACE_STATUS.IN_REVIEW);
    assert.equal(workspace.publishedAim, null);
    assert.ok(workspace.concepts.every((c) => c.status === 'proposed' || c.status === 'accepted' || c.status === 'edited'));
  });
});

describe('SPEC-113 Stage 5 Publication', () => {
  it('refuses to publish without operator approval', () => {
    const { compiler, workspace } = compileFixtures();
    assert.throws(
      () => compiler.publish(workspace.id),
      (err) => err.code === 'aic_not_approved'
    );
  });

  it('publishes an AIM Scout can qualify against after approval', () => {
    const { compiler, workspace } = compileFixtures();
    compiler.approve(workspace.id, { operator: 'jacob' });
    const result = compiler.publish(workspace.id);
    assert.equal(result.aim.status, AIM_STATUS.PUBLISHED);
    assert.equal(isRuntimeAim(result.aim), true);
    assert.equal(result.aim.isOperatingFact, false);
    assert.match(result.aim.mission.transformation, /business-machine/i);
    assert.ok(result.aim.painOntology.byId.founder_dependency);
    assert.ok(result.aim.provenance.some((p) => p.label && p.excerpt && p.operatorApproval));
    const qualification = qualifyProspect(result.aim, hiringProspect());
    assert.equal(qualification.topPain.id, 'founder_dependency');
    assert.ok(qualification.topPain.percent >= 70);
  });
});

describe('SPEC-113 compiler never executes outreach', () => {
  it('rejects outreach payloads', () => {
    const compiler = createCompiler();
    assert.throws(
      () => compiler.createWorkspace({ clientKey: 'x', notes: 'send email via brevo' }),
      (err) => err.code === 'aic_no_outreach'
    );
  });
});
