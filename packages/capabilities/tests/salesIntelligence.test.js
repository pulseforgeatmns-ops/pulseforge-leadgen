'use strict';

/**
 * SPEC-048 Sales Intelligence Engine tests.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const {
  deriveSalesIntelligence,
  applyProfileGates,
  gateOutreachCopy,
  evaluateHumanTest,
  openingFromProfile,
  createSalesIntelligenceCapability,
  recordApprovalEvent,
  computeOperatorApprovalRate,
  resetApprovalEvents,
  ACTIONS,
  GATE_REASONS,
} = require('../salesIntelligence');
const { composeMailPackage } = require('../mail/personalize');
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
  targetMarkets: ['Commercial Property Management', 'Law Firm'],
  valuePropositions: [
    'Owner-operated',
    'Responsive communication',
    'Consistent quality',
  ],
  offers: ['a walkthrough of one managed property'],
  idealCustomer: {
    primaryMarkets: ['Commercial Property Management'],
    industriesToAvoid: ['residential cleaning'],
    buyingTriggers: ['vendor transition'],
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
    companyName: 'Integrated Realty Resources',
    industry: 'Commercial Property Management',
    jobTitle: 'Property Manager',
    contactName: 'Alex Rivera',
    address: '100 Elm St, Manchester NH',
    website: 'https://irr.example.com',
    email: 'alex@irr.example.com',
    ...overrides,
  };
}

describe('SPEC-048 Sales Intelligence', () => {
  beforeEach(() => {
    resetApprovalEvents();
  });

  it('derives a structured profile with evidence-linked claims', () => {
    const profile = deriveSalesIntelligence(sampleProspect(), {
      playbook: SAMPLE_PLAYBOOK,
    });
    assert.equal(profile.company, 'Integrated Realty Resources');
    assert.equal(profile.industry, 'Commercial Property Management');
    assert.equal(profile.decision_maker, 'Property Manager');
    assert.ok(profile.primary_pain);
    assert.ok(profile.recommended_angle);
    assert.ok(profile.messaging_strategy.opening_focus);
    assert.ok(profile.personalization_claims.length >= 1);
    assert.ok(
      profile.personalization_claims.every(
        (c) => !c.verified || (c.evidenceRef && c.evidenceRef.length)
      )
    );
    assert.ok(profile.anchor_advantage.length >= 1);
  });

  it('rejects wrong-industry / missing evidence via gates', () => {
    const raw = deriveSalesIntelligence(
      sampleProspect({
        industry: 'residential cleaning franchise',
        jobTitle: null,
      }),
      { playbook: SAMPLE_PLAYBOOK }
    );
    const gated = applyProfileGates(raw, { playbook: SAMPLE_PLAYBOOK });
    assert.equal(gated.sendable, false);
    assert.ok(
      gated.gateRejections.some(
        (g) =>
          g.reason === GATE_REASONS.WRONG_INDUSTRY ||
          g.reason === GATE_REASONS.LOW_REASONING_CONFIDENCE
      )
    );
  });

  it('rejects prospect-after-Anchor openings', () => {
    const profile = applyProfileGates(
      deriveSalesIntelligence(sampleProspect(), { playbook: SAMPLE_PLAYBOOK }),
      { playbook: SAMPLE_PLAYBOOK }
    );
    const rejections = gateOutreachCopy(
      'Dear Alex,\n\nWe provide commercial cleaning for busy offices.\n\nSincerely,\nAnchor',
      profile,
      { clientNames: ['Anchor'] }
    );
    assert.ok(
      rejections.some((g) => g.reason === GATE_REASONS.PROSPECT_AFTER_ANCHOR)
    );
  });

  it('produces Operator Confidence Score via Human Test', () => {
    const profile = applyProfileGates(
      deriveSalesIntelligence(sampleProspect(), { playbook: SAMPLE_PLAYBOOK }),
      { playbook: SAMPLE_PLAYBOOK }
    );
    const opening = openingFromProfile(profile);
    const score = evaluateHumanTest({
      profile,
      letterBody: `Dear Alex,\n\n${opening}\n\nThat is where Anchor helps.\n`,
    });
    assert.ok(score.overall >= 0 && score.overall <= 100);
    assert.ok(Number.isFinite(score.industryAccuracy));
    assert.ok(Number.isFinite(score.buyerRelevance));
  });

  it('mail compose prefers Sales Intelligence and stays prospect-first', () => {
    const profile = applyProfileGates(
      deriveSalesIntelligence(sampleProspect(), { playbook: SAMPLE_PLAYBOOK }),
      { playbook: SAMPLE_PLAYBOOK }
    );
    const composed = composeMailPackage(sampleProspect(), {
      playbook: SAMPLE_PLAYBOOK,
      salesIntelligenceProfile: profile,
      clientName: 'Anchor Cleaning',
    });
    assert.ok(composed.letter.body.includes('Integrated Realty Resources'));
    const firstContent = composed.letter.body
      .split(/\n\s*\n/)
      .map((p) => p.trim())
      .find((p) => p && !/^dear\b/i.test(p));
    assert.ok(firstContent);
    assert.ok(!/^we\s+(provide|offer|specialize)\b/i.test(firstContent));
    assert.ok(!/^Anchor\b/i.test(firstContent));
    assert.ok(composed.salesIntelligence);
    assert.ok(composed.messagingStrategy);
    assert.ok(composed.operatorConfidence);
  });

  it('capability emits sales_intelligence_profile artifact outputs', async () => {
    const cap = createSalesIntelligenceCapability();
    assert.equal(cap.id, BUILTIN_IDS.SALES_INTELLIGENCE);
    const result = await cap.execute({
      missionId: 'm1',
      inputs: {
        prospects: [sampleProspect()],
        clientPlaybook: SAMPLE_PLAYBOOK,
      },
      constraints: { clientPlaybook: SAMPLE_PLAYBOOK },
    });
    assert.equal(result.status, CAPABILITY_RESULT_STATUS.COMPLETED);
    assert.ok(result.outputs.profiles.length === 1);
    assert.ok(result.outputs.profileCount === 1);
    assert.ok(
      result.artifacts.some((a) => a.type === 'sales_intelligence_profile')
    );
  });

  it('registers capability on builtin registry', () => {
    const registry = createBuiltinRegistry();
    assert.ok(registry.get(BUILTIN_IDS.SALES_INTELLIGENCE));
  });

  it('wires artifact type and stage library', () => {
    assert.equal(
      resolveArtifactType('sales_intelligence_profile'),
      ARTIFACT_TYPES.SALES_INTELLIGENCE_PROFILE
    );
    const stage = getStage('sales_intelligence');
    assert.ok(stage);
    assert.equal(stage.capabilityId, BUILTIN_IDS.SALES_INTELLIGENCE);
    assert.ok(stage.produces.includes('sales_intelligence_profile'));
    const seed = seedStagesForType(MISSION_TYPES.CAMPAIGN_CREATION);
    assert.ok(seed.includes('sales_intelligence'));
  });

  it('extracts and flattens SalesIntelligenceProfile on the bus', () => {
    const drafts = draftsFromCapabilityOutputs(['sales_intelligence_profile'], {
      profiles: [
        {
          prospectId: 'p1',
          company: 'Integrated Realty Resources',
          personalization_claims: [],
        },
      ],
      profileCount: 1,
      sendableCount: 1,
    });
    assert.equal(drafts.length, 1);
    assert.equal(
      drafts[0].artifactType,
      ARTIFACT_TYPES.SALES_INTELLIGENCE_PROFILE
    );
    const prior = flattenArtifactsToOutputs([
      {
        artifactType: ARTIFACT_TYPES.SALES_INTELLIGENCE_PROFILE,
        payload: drafts[0].payload,
      },
    ]);
    assert.ok(Array.isArray(prior.salesIntelligenceProfiles));
    assert.equal(prior.profileCount, 1);
  });

  it('pipeline gate validates sales intelligence outputs', () => {
    const gate = evaluatePipelineGate({
      capabilityId: BUILTIN_IDS.SALES_INTELLIGENCE,
      runResult: {
        result: {
          status: 'completed',
          outputs: {
            profiles: [{ company: 'X', sendable: true }],
            profileCount: 1,
            sendableCount: 1,
          },
          artifacts: [{ type: 'sales_intelligence_profile', count: 1 }],
          warnings: [],
        },
      },
      context: { inputs: { prospects: [sampleProspect()] } },
    });
    assert.equal(gate.advance, true);
  });

  it('tracks Operator Approval Rate stub', () => {
    recordApprovalEvent({
      action: ACTIONS.APPROVE_UNCHANGED,
      packageId: 'pkg1',
      channel: 'direct_mail',
    });
    recordApprovalEvent({
      action: ACTIONS.APPROVE_WITH_EDITS,
      packageId: 'pkg2',
      substantiveEdit: true,
      channel: 'direct_mail',
    });
    recordApprovalEvent({
      action: ACTIONS.REJECT,
      packageId: 'pkg3',
      channel: 'direct_mail',
    });
    const stats = computeOperatorApprovalRate({ channel: 'direct_mail' });
    assert.equal(stats.total, 3);
    assert.equal(stats.approvedUnchanged, 1);
    assert.equal(stats.operatorApprovalRate, Number((1 / 3).toFixed(4)));
  });

  it('openingFromProfile never leads with Anchor', () => {
    const profile = deriveSalesIntelligence(sampleProspect(), {
      playbook: SAMPLE_PLAYBOOK,
    });
    const opening = openingFromProfile(profile);
    assert.ok(!/^Anchor\b/i.test(opening));
    assert.ok(!/^We\s+provide\b/i.test(opening));
    assert.match(opening, /Integrated Realty|Commercial Property/i);
  });
});
