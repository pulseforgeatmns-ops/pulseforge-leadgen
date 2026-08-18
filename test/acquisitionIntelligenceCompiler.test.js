'use strict';

/**
 * SPEC-113 — Acquisition Intelligence Compiler (shared wiring).
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const training = require('../packages/max/training');
const {
  createCompiler,
  createMemoryAicStore,
  loadFixtureDocuments,
  WORKSPACE_STATUS,
} = require('../packages/aic');
const { createMemoryAimStore, isRuntimeAim, AIM_STATUS } = require('../packages/aim');
const {
  buildAcquisitionSearchDefinition,
  evaluateBasicFit,
  attachFitToClassified,
  resolveAim,
} = require('../services/scoutAcquisitionIntelligence');

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

function publishedCompilerAim() {
  const aimStore = createMemoryAimStore({ seedFedir: false });
  const compiler = createCompiler({
    store: createMemoryAicStore(),
    aimStore,
  });
  const workspace = compiler.ingestAndCompile(
    { clientKey: 'fedir-aic', clientName: 'Fedir' },
    loadFixtureDocuments()
  );
  compiler.approve(workspace.id, { operator: 'jacob' });
  const published = compiler.publish(workspace.id);
  return { compiler, workspace: compiler.getWorkspace(workspace.id), aim: published.aim, aimStore };
}

describe('SPEC-113 competency registry', () => {
  it('registers acquisition_intelligence_compiler as a graduated competency', () => {
    const competency = training.getCompetency('acquisition_intelligence_compiler');
    assert.equal(competency.stage, training.STAGES.GRADUATED);
    assert.ok(competency.specRefs.includes('SPEC-113'));
    assert.match(competency.exercises[0].generalLesson, /compiled, then approved, then published/i);
  });
});

describe('SPEC-113 Scout never reads documents or draft AIMs', () => {
  it('treats an unpublished AIM as absent and keeps commercial-cleaning fit', () => {
    const { aim } = publishedCompilerAim();
    const draft = { ...aim, status: AIM_STATUS.DRAFT };
    assert.equal(isRuntimeAim(draft), false);
    const definition = buildAcquisitionSearchDefinition({
      tenantId: '10',
      aim: draft,
      targetContext: {
        geography: 'Manchester, NH',
        segments: ['property_management'],
        businessType: 'commercial_cleaning',
      },
      businessContext: {
        serviceGeography: 'Manchester, NH',
        commercialCapability: 'commercial_cleaning',
      },
    });
    assert.equal(definition.aim, null);
    assert.match(definition.populationStatement, /Commercial organizations/i);
    const fit = evaluateBasicFit(
      {
        name: 'Granite State Property Management',
        industry: 'property_management',
        location: 'Manchester, NH',
        website: 'https://granitepm.example',
      },
      definition
    );
    assert.equal(fit.basicFit, true);
  });

  it('Scout reasons from the published AIM instead of raw documents', () => {
    const { aim, workspace } = publishedCompilerAim();
    assert.equal(aim.status, AIM_STATUS.PUBLISHED);
    assert.ok(aim.compiler.documentTitles.includes('Pain Points v3'));
    const definition = buildAcquisitionSearchDefinition({
      tenantId: 'fedir-aic',
      aim,
      businessContext: { aimClientKey: 'fedir-aic' },
    });
    assert.equal(definition.valid, true);
    assert.match(definition.populationStatement, /founder-led businesses into business-machine/i);
    const blob = JSON.stringify(definition);
    assert.equal(blob.includes(workspace.documents[0].body.slice(0, 80)), false);
    const prospect = hiringProspect();
    const fit = evaluateBasicFit(prospect, definition);
    assert.equal(fit.basicFit, true);
    const attached = attachFitToClassified(
      { observations: [], unknowns: [], signals: prospect.signals, evidenceRefs: [] },
      prospect,
      definition
    );
    assert.equal(attached.classified.aimQualification.topPain.id, 'founder_dependency');
    assert.ok(attached.classified.aimQualification.topPain.percent >= 70);
  });

  it('resolveAim ignores draft store rows and returns published AIMs', () => {
    const { aim, aimStore } = publishedCompilerAim();
    aimStore.putAim({ ...aim, clientKey: 'draft-client', status: AIM_STATUS.DRAFT });
    assert.equal(
      resolveAim({ aimStore, aimClientKey: 'draft-client' }),
      null
    );
    const loaded = resolveAim({ aimStore, aimClientKey: 'fedir-aic' });
    assert.equal(loaded.status, WORKSPACE_STATUS.PUBLISHED);
    assert.equal(loaded.clientKey, 'fedir-aic');
  });
});
