'use strict';

/**
 * SPEC-112 — Acquisition Intelligence Model.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const aim = require('../index');
const {
  FEDIR_CLIENT_KEY,
  buildFedirAim,
  qualifyProspect,
  briefPaige,
  assessPilot,
  createMemoryAimStore,
  RECOMMENDATIONS,
  PAIN_IDS,
  AIM_STATUS,
  isQualified,
} = require('../index');

function fedirProspect(overrides = {}) {
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
    ...overrides,
  };
}

describe('SPEC-112 Phase 1 Market Understanding', () => {
  it('Fedir AIM answers the mission question', () => {
    const model = buildFedirAim();
    assert.equal(model.clientKey, FEDIR_CLIENT_KEY);
    assert.equal(model.status, AIM_STATUS.COMPLETE);
    assert.equal(model.isOperatingFact, false);
    assert.match(model.mission.transformation, /founder-led businesses into business-machine/i);
    assert.equal(model.mission.known, true);
  });

  it('ICP is reasoning, not demographics', () => {
    const model = buildFedirAim();
    assert.equal(model.icp.kind, 'reasoning');
    assert.ok(model.icp.company.reasoning);
    assert.ok(model.icp.founder.reasoning);
    assert.equal(model.icp.size.known, false);
    assert.equal(model.icp.geography.known, false);
    assert.match(model.icp.exclusions.reasoning, /PE-backed|systemized/i);
    assert.ok(model.icp.size.unknowns.length);
    assert.ok(model.icp.geography.unknowns.length);
  });

  it('desired transformation is current → future state', () => {
    const model = buildFedirAim();
    assert.equal(model.transformation.currentState, 'I do everything myself.');
    assert.equal(
      model.transformation.futureState,
      'My business operates through systems and managers.'
    );
    assert.equal(model.transformation.known, true);
  });
});

describe('SPEC-112 Phase 2 Acquisition Intelligence', () => {
  it('teaches Scout People / Growth / Finance pains with observable signals', () => {
    const model = buildFedirAim();
    const ids = model.painOntology.categories.map((c) => c.id).sort();
    assert.deepEqual(ids, ['customer_growth', 'finance', 'people_management']);
    const founder = model.painOntology.byId[PAIN_IDS.FOUNDER_DEPENDENCY];
    assert.ok(founder.signals.includes('owner replying to reviews'));
    assert.ok(founder.signals.includes('hiring repeatedly'));
    assert.ok(founder.signals.includes('job postings'));
    assert.ok(founder.signals.includes('growth announcements'));
    const growth = model.painOntology.byId[PAIN_IDS.POOR_LEAD_GENERATION];
    assert.ok(growth.signals.includes('irregular marketing'));
    const finance = model.painOntology.byId[PAIN_IDS.CASH_FLOW];
    assert.ok(finance.signals.includes('financing'));
  });
});

describe('SPEC-112 Phase 3 Qualification Model', () => {
  it('scores all six dimensions with explanations', () => {
    const model = buildFedirAim();
    const result = qualifyProspect(model, fedirProspect());
    assert.equal(result.kind, 'aim_qualification');
    assert.equal(result.isOperatingFact, false);
    for (const key of ['icpFit', 'painMatch', 'evidenceQuality', 'buyingReadiness', 'confidence']) {
      assert.equal(typeof result.dimensions[key].score, 'number', key);
      assert.ok(result.dimensions[key].reasons.length, key);
    }
    assert.ok(result.overallRecommendation.id);
    assert.ok(result.overallRecommendation.reason);
  });

  it('scores Founder Dependency in the ~92% band when Fedir signals are present', () => {
    const model = buildFedirAim();
    const result = qualifyProspect(model, fedirProspect());
    assert.equal(result.topPain.id, PAIN_IDS.FOUNDER_DEPENDENCY);
    assert.ok(result.topPain.percent >= 90, `expected ~92, got ${result.topPain.percent}`);
    assert.ok(result.topPain.percent <= 96, `expected ~92, got ${result.topPain.percent}`);
    assert.ok(result.dimensions.icpFit.score >= 70);
    assert.equal(result.overallRecommendation.id, RECOMMENDATIONS.PURSUE);
    assert.equal(isQualified(result), true);
  });

  it('rejects AIM exclusions instead of scoring them as a fit', () => {
    const model = buildFedirAim();
    const result = qualifyProspect(model, {
      id: 'co-pe',
      name: 'Atlas Enterprise HQ',
      description: 'PE-backed and already systemized with professional managers.',
    });
    assert.equal(result.excluded, true);
    assert.equal(result.overallRecommendation.id, RECOMMENDATIONS.REJECT);
    assert.match(result.dimensions.icpFit.reasons[0], /Excluded/i);
  });

  it('returns unknown rather than inventing pain when evidence is thin', () => {
    const model = buildFedirAim();
    const result = qualifyProspect(model, {
      id: 'co-thin',
      name: 'Untitled LLC',
    });
    assert.equal(result.overallRecommendation.id, RECOMMENDATIONS.UNKNOWN);
    assert.ok(result.dimensions.painMatch.unknowns.length);
    assert.equal(result.topPain, null);
  });
});

describe('SPEC-112 Phase 4 Knowledge Capture', () => {
  it('stores definition, evidence, objections, language, messaging, and questions', () => {
    const model = buildFedirAim();
    const record = model.knowledgeById[PAIN_IDS.FOUNDER_DEPENDENCY];
    assert.ok(record.definition);
    assert.ok(record.observableEvidence.length);
    assert.ok(record.commonObjections.length);
    assert.ok(record.typicalLanguage.includes('I do everything myself.'));
    assert.ok(record.recommendedMessaging.length);
    assert.ok(record.discoveryQuestions.length);
    assert.deepEqual(record.caseStudies, []);
    assert.deepEqual(record.successStories, []);
    assert.ok(record.unknowns.includes('caseStudies'));
  });
});

describe('SPEC-112 Phase 5 Messaging Intelligence', () => {
  it('gives Paige likely pain, language, proof unknown, and CTA from Founder Dependency', () => {
    const model = buildFedirAim();
    const result = qualifyProspect(model, fedirProspect());
    const brief = briefPaige({ aim: model, qualification: result });
    assert.equal(brief.available, true);
    assert.equal(brief.likelyPain.id, PAIN_IDS.FOUNDER_DEPENDENCY);
    assert.ok(brief.likelyPain.percent >= 90);
    assert.ok(brief.language.some((line) => /everything myself|too many hats|vacation/i.test(line)));
    assert.equal(brief.proof.available, false);
    assert.match(brief.proof.unknown, /do not invent/i);
    assert.match(brief.cta, /daily loop|systems and managers|generic/i);
    assert.ok(brief.discoveryQuestions.length);
  });

  it('does not invent a brief when qualification is missing', () => {
    const brief = briefPaige({ aim: buildFedirAim() });
    assert.equal(brief.available, false);
  });
});

describe('SPEC-112 Phase 6 Pilot Success', () => {
  it('reports technical vs business milestones without inventing 50 prospects', () => {
    const model = buildFedirAim();
    const store = createMemoryAimStore();
    const result = qualifyProspect(model, fedirProspect());
    store.putQualification(result);
    const status = assessPilot({
      aim: store.getAim(FEDIR_CLIENT_KEY),
      qualifications: store.listQualifications(FEDIR_CLIENT_KEY),
    });
    assert.equal(status.invented, false);
    assert.equal(status.technical.aimCompleted.met, true);
    assert.equal(status.technical.scoutUnderstandsMarket.met, true);
    assert.equal(status.technical.qualificationModelWorking.met, true);
    assert.equal(status.technical.qualifiedProspects.met, false);
    assert.equal(status.technical.qualifiedProspects.count, 1);
    assert.equal(status.technical.qualifiedProspects.target, 50);
    assert.equal(status.technical.outreachBegins.met, false);
    assert.equal(status.business.forms.met, false);
    assert.equal(status.business.sales.met, false);
  });
});

describe('SPEC-112 store and builder', () => {
  it('seeds Fedir into the memory store', () => {
    const store = createMemoryAimStore();
    const model = store.getAim('fedir');
    assert.equal(model.clientName, 'Fedir');
    assert.equal(model.mission.known, true);
  });

  it('qualifyAndBrief returns both artifacts', () => {
    const model = buildFedirAim();
    const { qualification, briefing } = aim.qualifyAndBrief(model, fedirProspect());
    assert.equal(qualification.topPain.id, PAIN_IDS.FOUNDER_DEPENDENCY);
    assert.equal(briefing.likelyPain.id, PAIN_IDS.FOUNDER_DEPENDENCY);
  });
});
