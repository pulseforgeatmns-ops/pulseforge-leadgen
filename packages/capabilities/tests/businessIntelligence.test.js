'use strict';

/**
 * SPEC-053 Business Intelligence Engine tests.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  deriveBusinessIntelligence,
  applyBusinessIntelligenceGates,
  answersQualityGates,
  createBusinessIntelligenceCapability,
} = require('../businessIntelligence');
const { deriveSalesIntelligence } = require('../salesIntelligence');
const { createBuiltinRegistry, BUILTIN_IDS } = require('../index');
const {
  ARTIFACT_TYPES,
  resolveArtifactType,
  draftsFromCapabilityOutputs,
  flattenArtifactsToOutputs,
} = require('../../mission-engine/ArtifactRegistry');
const { getStage, seedStagesForType } = require('../../mission-engine/StageLibrary');
const { MISSION_TYPES } = require('../../mission-engine/types');
const { evaluatePipelineGate } = require('../../mission-engine/PipelineGate');
const { CAPABILITY_RESULT_STATUS } = require('../types');

const SAMPLE_PLAYBOOK = {
  id: 'pb_anchor',
  name: 'Anchor Cleaning — Manchester',
  version: '1.0',
  brandVoice: 'relationship_first',
  targetMarkets: ['Law Firm'],
  valuePropositions: ['Owner-operated', 'Responsive communication'],
  offers: ['a walkthrough of one managed property'],
  idealCustomer: {
    primaryMarkets: ['Law Firm'],
    industriesToAvoid: ['residential cleaning'],
    buyingTriggers: ['headcount growth'],
    geographicCoverage: 'Greater Manchester NH',
  },
  preferredChannels: ['direct_mail'],
  outreachSequence: [{ day: 0, channel: 'direct_mail', action: 'mail' }],
  constraints: [],
  successMetrics: ['walkthroughs booked'],
};

function sampleProspect(overrides = {}) {
  return {
    id: 'p1',
    companyName: 'Maynard & Associates',
    industry: 'Law Firm',
    jobTitle: 'Office Manager',
    contactName: 'Jordan Lee',
    address: '200 Elm St, Manchester NH',
    website: 'https://maynard.example.com',
    email: 'jordan@maynard.example.com',
    ...overrides,
  };
}

describe('SPEC-053 Business Intelligence', () => {
  it('derives analytical reasoning for a law firm (not a directory)', () => {
    const profile = deriveBusinessIntelligence(sampleProspect(), {
      playbook: SAMPLE_PLAYBOOK,
    });
    assert.equal(profile.company, 'Maynard & Associates');
    assert.equal(profile.industry, 'Law Firm');
    assert.equal(profile.business_model, 'Professional services');
    assert.match(profile.revenue_model, /billable/i);
    assert.ok(profile.operational_constraints.length >= 1);
    assert.ok(profile.likely_kpis.length >= 1);
    assert.ok(profile.buying_triggers.length >= 1);
    assert.ok(profile.service_angle);
    assert.ok(profile.qualityAnswers.howTheyMakeMoney);
    assert.ok(profile.qualityAnswers.problemOwner);
    assert.ok(profile.reasoningLayers.level2_business_model);
    assert.ok(profile.confidence);
  });

  it('exposes explicit uncertainty when industry is missing', () => {
    const raw = deriveBusinessIntelligence(
      sampleProspect({ industry: null, website: null, jobTitle: null }),
      {}
    );
    const gated = applyBusinessIntelligenceGates(raw);
    assert.ok(gated.uncertainty.length >= 1);
    assert.ok(
      gated.uncertainty.some((u) => /industry|uncertain/i.test(u))
    );
    assert.ok(
      gated.confidence === 'Low' || gated.confidenceScore < 0.6
    );
  });

  it('quality gates require the five reasoning answers', () => {
    const profile = applyBusinessIntelligenceGates(
      deriveBusinessIntelligence(sampleProspect(), { playbook: SAMPLE_PLAYBOOK })
    );
    assert.equal(answersQualityGates(profile), true);
    assert.ok(profile.qualityAnswers.whyBuyNow);
    assert.ok(profile.qualityAnswers.growthConstraints);
  });

  it('Sales Intelligence consumes Business Intelligence', () => {
    const bi = applyBusinessIntelligenceGates(
      deriveBusinessIntelligence(sampleProspect(), { playbook: SAMPLE_PLAYBOOK })
    );
    const sales = deriveSalesIntelligence(sampleProspect(), {
      playbook: SAMPLE_PLAYBOOK,
      businessIntelligence: bi,
    });
    assert.ok(
      sales.evidenceRefs.some((r) => /business_intelligence/i.test(r)) ||
        sales.recommended_angle === bi.service_angle ||
        sales.messaging_strategy.positioning === bi.service_angle
    );
    assert.ok(sales.primary_pain);
    assert.ok(
      sales.personalization_claims.some(
        (c) => c.source === 'business_intelligence'
      )
    );
  });

  it('registers capability and produces Artifact Bus drafts', async () => {
    const registry = createBuiltinRegistry();
    const cap = registry.get(BUILTIN_IDS.BUSINESS_INTELLIGENCE);
    assert.ok(cap);
    assert.equal(cap.id, BUILTIN_IDS.BUSINESS_INTELLIGENCE);
    assert.deepEqual(cap.produces, ['business_intelligence_profile']);

    const result = await cap.execute({
      inputs: { prospects: [sampleProspect()] },
      constraints: { clientPlaybook: SAMPLE_PLAYBOOK },
    });
    assert.equal(result.status, CAPABILITY_RESULT_STATUS.COMPLETED);
    assert.ok(result.outputs.profileCount >= 1);
    assert.ok(result.outputs.businessIntelligenceProfiles[0].revenue_model);

    assert.equal(
      resolveArtifactType('business_intelligence_profile'),
      ARTIFACT_TYPES.BUSINESS_INTELLIGENCE_PROFILE
    );
    const drafts = draftsFromCapabilityOutputs(cap.produces, result.outputs);
    assert.ok(
      drafts.some(
        (d) => d.artifactType === ARTIFACT_TYPES.BUSINESS_INTELLIGENCE_PROFILE
      )
    );
    const prior = flattenArtifactsToOutputs(
      drafts.map((d) => ({
        artifactType: d.artifactType,
        payload: d.payload,
      }))
    );
    assert.ok(prior.businessIntelligenceProfiles.length >= 1);
  });

  it('seeds business_intelligence before sales_intelligence', () => {
    const seeds = seedStagesForType(MISSION_TYPES.CAMPAIGN_CREATION);
    const biIdx = seeds.indexOf('business_intelligence');
    const siIdx = seeds.indexOf('sales_intelligence');
    assert.ok(biIdx >= 0);
    assert.ok(siIdx > biIdx);
    const stage = getStage('business_intelligence');
    assert.equal(stage.name, 'Business Intelligence');
    assert.deepEqual(stage.produces, ['business_intelligence_profile']);
  });

  it('PipelineGate advances on BI profiles', async () => {
    const cap = createBusinessIntelligenceCapability();
    const result = await cap.execute({
      inputs: { prospects: [sampleProspect()] },
    });
    const gate = evaluatePipelineGate({
      capabilityId: BUILTIN_IDS.BUSINESS_INTELLIGENCE,
      runResult: { result },
      context: { inputs: { prospects: [sampleProspect()] } },
    });
    assert.equal(gate.advance, true);
    assert.ok(gate.publishOutputs !== false);
  });
});
