'use strict';

/**
 * SPEC-112 — Acquisition Intelligence Model (shared wiring).
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const training = require('../packages/max/training');
const {
  buildFedirAim,
  qualifyProspect,
  briefPaige,
  PAIN_IDS,
  RECOMMENDATIONS,
  FEDIR_CLIENT_KEY,
} = require('../packages/aim');
const {
  evaluateBasicFit,
  attachFitToClassified,
  buildAcquisitionSearchDefinition,
} = require('../services/scoutAcquisitionIntelligence');
const { generateContentRecommendation, createLinkedMemoryStores } = require('../services/contentLearning');
const { qualify, getAim, pilotStatus } = require('../services/acquisitionIntelligenceModel');

function fedirProspect() {
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
        label: 'Owner replying to reviews personally.',
      },
      {
        type: 'growth',
        source: 'linkedin',
        observedAt: '2026-07-20T00:00:00.000Z',
        label: 'Growth announcements on LinkedIn.',
      },
    ],
  };
}

describe('SPEC-112 competency registry', () => {
  it('registers acquisition_intelligence_model as a graduated competency', () => {
    const competency = training.getCompetency('acquisition_intelligence_model');
    assert.equal(competency.stage, training.STAGES.GRADUATED);
    assert.ok(competency.specRefs.includes('SPEC-112'));
    assert.match(competency.exercises[0].generalLesson, /understand a market before/i);
  });
});

describe('SPEC-112 Scout without AIM is unchanged', () => {
  it('still uses facility/segment fit when no AIM is attached', () => {
    const definition = buildAcquisitionSearchDefinition({
      tenantId: '10',
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
    assert.equal(definition.valid, true);
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
    assert.match(fit.reasons.join(' '), /property|Manchester/i);
  });
});

describe('SPEC-112 Scout with AIM reasons over the market', () => {
  it('qualifies a founder-led agency against Fedir instead of commercial facilities', () => {
    const aim = buildFedirAim();
    const definition = buildAcquisitionSearchDefinition({
      tenantId: 'fedir',
      aim,
      businessContext: { aimClientKey: FEDIR_CLIENT_KEY },
    });
    assert.equal(definition.valid, true);
    assert.match(definition.populationStatement, /founder-led businesses into business-machine/i);
    const prospect = fedirProspect();
    const fit = evaluateBasicFit(prospect, definition);
    assert.equal(fit.basicFit, true);
    const attached = attachFitToClassified(
      { observations: [], unknowns: [], signals: prospect.signals, evidenceRefs: [] },
      prospect,
      definition
    );
    assert.equal(attached.classified.aimQualification.topPain.id, PAIN_IDS.FOUNDER_DEPENDENCY);
    assert.ok(attached.classified.aimQualification.topPain.percent >= 90);
    assert.equal(
      attached.classified.aimQualification.overallRecommendation.id,
      RECOMMENDATIONS.PURSUE
    );
  });

  it('rejects PE-backed exclusions under the AIM', () => {
    const aim = buildFedirAim();
    const fit = evaluateBasicFit(
      {
        name: 'Atlas Enterprise HQ',
        description: 'PE-backed and already systemized.',
      },
      { aim }
    );
    assert.equal(fit.basicFit, false);
    assert.match(fit.reasons[0], /Excluded/i);
  });
});

describe('SPEC-112 Paige messaging intelligence', () => {
  it('attaches an AIM briefing when qualification is supplied', async () => {
    const { outcomeStore, learningStore } = createLinkedMemoryStores();
    const aim = buildFedirAim();
    const qualification = qualifyProspect(aim, fedirProspect());
    const rec = await generateContentRecommendation(
      {
        clientId: 1,
        objective: 'Open conversations with founder-led operators.',
        aim,
        aimQualification: qualification,
      },
      { store: outcomeStore, outcomeStore, learningStore }
    );
    assert.ok(rec.aim_briefing);
    assert.equal(rec.aim_briefing.likelyPain.id, PAIN_IDS.FOUNDER_DEPENDENCY);
    assert.equal(rec.aim_briefing.proof.available, false);
    assert.match(rec.aim_briefing.cta, /systems and managers|daily loop/i);
  });

  it('does not attach a briefing when AIM is absent', async () => {
    const { outcomeStore, learningStore } = createLinkedMemoryStores();
    const rec = await generateContentRecommendation(
      {
        clientId: 1,
        objective: 'Build qualified attention.',
      },
      { store: outcomeStore, outcomeStore, learningStore }
    );
    assert.equal(rec.aim_briefing, undefined);
  });
});

describe('SPEC-112 service facade', () => {
  it('qualifies through the Fedir seed and reports an honest pilot', () => {
    const model = getAim('fedir');
    assert.equal(model.clientKey, 'fedir');
    const result = qualify('fedir', fedirProspect());
    assert.equal(result.qualification.topPain.id, PAIN_IDS.FOUNDER_DEPENDENCY);
    assert.equal(result.briefing.likelyPain.id, PAIN_IDS.FOUNDER_DEPENDENCY);
    const status = pilotStatus('fedir');
    assert.equal(status.technical.aimCompleted.met, true);
    assert.equal(status.technical.qualifiedProspects.met, false);
    assert.ok(status.technical.qualifiedProspects.count >= 1);
  });
});

describe('SPEC-112 AIM is not operating fact', () => {
  it('stamps isOperatingFact false on model, qualification, and brief', () => {
    const aim = buildFedirAim();
    const qualification = qualifyProspect(aim, fedirProspect());
    const brief = briefPaige({ aim, qualification });
    assert.equal(aim.isOperatingFact, false);
    assert.equal(qualification.isOperatingFact, false);
    assert.equal(brief.kind, 'aim_messaging_brief');
  });
});
