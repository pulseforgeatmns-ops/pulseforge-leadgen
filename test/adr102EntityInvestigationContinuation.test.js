'use strict';

/**
 * ADR-102 — Investigation Follows Uncertainty acceptance tests.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  INVESTIGATION_MODES,
  mapMissingEvidenceToGap,
  extractInvestigationCandidatesFromPayload,
  buildEntityGapTasksForCandidate,
  buildEntityInvestigationPlan,
  extractPreservedCandidatesFromPayload,
  resolveInvestigationMode,
  buildInvestigationContinuationContext,
} = require('../packages/scout/investigation/EntityInvestigationContinuation');
const { INVESTIGATIVE_EVIDENCE } = require('../packages/scout/coverage/EvidenceRequirements');
const {
  READINESS_STATES,
  QUALIFICATION_STATUSES,
} = require('../packages/max/scoutAcquisition/Types');
const { PROSPECT_BUCKETS } = require('../packages/max/scoutAcquisition/Types');
const { buildProspectEvaluation } = require('../packages/max/scoutAcquisition/ProspectEvaluation');
const { attachFitToClassified } = require('../packages/max/scoutAcquisition/FitEvaluation');
const { buildAcquisitionSearchDefinition } = require('../services/scoutAcquisitionIntelligence');
const { normalizeScoutDiscoveryPayload } = require('../packages/acquisition-mission/DiscoveryPayload');

function lot202Payload() {
  const classified = {
    name: 'Lot 202 Property Management',
    fit: 0.81,
    signals: [],
    unknowns: ['No identifiable operations decision-maker'],
    observations: [{ text: 'Property management operator in Manchester.' }],
    evidenceRefs: [{ id: 'ev-lot', label: 'Google Places listing', sourceKind: 'observed_fact' }],
  };
  const company = {
    id: 'co-lot-202',
    name: 'Lot 202 Property Management',
    industry: 'property_management',
    location: 'Manchester, NH',
    website: 'https://lot202.example',
    icpScore: 74,
  };
  const searchDefinition = buildAcquisitionSearchDefinition({
    tenantId: '10',
    targetContext: {
      geography: 'Manchester, NH',
      segments: ['property_management'],
      businessType: 'commercial_cleaning',
    },
    businessContext: {
      serviceGeography: 'Manchester, NH',
      commercialCapability: 'commercial_cleaning',
      preferredSegments: ['property_management'],
    },
  });
  const attached = attachFitToClassified(classified, company, searchDefinition, Date.now());
  const scoutResult = {
    status: 'completed',
    summary: '1 qualified prospect; readiness unknown.',
    payload: {
      opportunities: [],
      fitCandidates: [
        {
          companyId: company.id,
          name: company.name,
          fit: attached.fit,
          qualified: true,
          qualificationStatus: attached.evaluation.qualification.status,
          readinessState: attached.evaluation.readiness.status,
          evaluation: attached.evaluation,
          unknowns: classified.unknowns,
          evidenceRefs: classified.evidenceRefs,
        },
      ],
      qualifiedCount: 1,
      readinessReadyCount: 0,
      readinessUnknownCount: 1,
    },
  };
  return normalizeScoutDiscoveryPayload(scoutResult);
}

describe('ADR-102 — Investigation Follows Uncertainty', () => {
  it('maps decision-maker missing evidence to executable decision_makers task gap', () => {
    const mapped = mapMissingEvidenceToGap('Website / portfolio / review / decision-maker enrichment');
    assert.equal(mapped.evidenceType, INVESTIGATIVE_EVIDENCE.DECISION_MAKERS);
    assert.equal(mapped.gap, 'decision_maker');
  });

  it('Lot 202 produces entity investigation tasks instead of descriptive-only metadata', () => {
    const payload = lot202Payload();
    const candidates = extractInvestigationCandidatesFromPayload(payload);
    assert.equal(candidates.length, 1);
    assert.equal(candidates[0].name, 'Lot 202 Property Management');
    assert.equal(candidates[0].readinessState, READINESS_STATES.UNKNOWN);

    const tasks = buildEntityGapTasksForCandidate(candidates[0]);
    assert.ok(tasks.length > 0, 'expected executable entity tasks');
    assert.ok(
      tasks.every((task) => task.scope === 'entity' && task.entityId && task.gap && task.evidenceType),
      'investigation unit must be Candidate + Hypothesis + Evidence Gap'
    );
    assert.ok(
      tasks.some((task) => task.evidenceType === INVESTIGATIVE_EVIDENCE.DECISION_MAKERS),
      'decision-maker unknown should become decision_makers task'
    );
  });

  it('entity continuation mode preserves prior identities and builds entity plan', () => {
    const payload = lot202Payload();
    const mode = resolveInvestigationMode({
      priorPayload: payload,
      opts: { investigationContinuation: true },
    });
    assert.equal(mode, INVESTIGATION_MODES.ENTITY_CONTINUATION);

    const preserved = extractPreservedCandidatesFromPayload(payload);
    assert.equal(preserved.length, 1);
    assert.equal(preserved[0].id, 'co-lot-202');
    assert.equal(preserved[0]._preservedFromContinuation, true);

    const plan = buildEntityInvestigationPlan({
      mission: { id: 'mission-1', objective: 'Acquire PM customers in Manchester NH' },
      marketDefinition: { geography: 'Manchester, NH', segments: ['property_management'] },
      priorPayload: payload,
    });
    assert.equal(plan.investigationMode, INVESTIGATION_MODES.ENTITY_CONTINUATION);
    assert.ok(plan.tasks.length > 0);
    assert.match(plan.objective, /Continue entity investigation/);
    assert.ok(plan.tasks.every((task) => String(task.id).startsWith('task:co-lot-202:')));
  });

  it('blocked provider failure still routes to broad discovery continuation', () => {
    const mode = resolveInvestigationMode({
      priorPayload: {
        blocked: true,
        summary: 'Google Places REQUEST_DENIED.',
        qualifiedCount: 0,
        rankedProspects: [],
      },
      opts: { investigationContinuation: true },
    });
    assert.equal(mode, INVESTIGATION_MODES.BROAD_DISCOVERY);
  });

  it('empty universe with no candidates routes to broad discovery', () => {
    const mode = resolveInvestigationMode({
      priorPayload: {
        qualifiedCount: 0,
        rankedProspects: [],
        candidateUniverseCount: 0,
      },
      opts: { investigationContinuation: true },
    });
    assert.equal(mode, INVESTIGATION_MODES.BROAD_DISCOVERY);
  });

  it('buildInvestigationContinuationContext exposes preserved candidates and task count', () => {
    const payload = lot202Payload();
    const context = buildInvestigationContinuationContext({
      priorPayload: payload,
      opts: { investigationContinuation: true },
    });
    assert.equal(context.investigationContinuation, true);
    assert.equal(context.investigationMode, INVESTIGATION_MODES.ENTITY_CONTINUATION);
    assert.equal(context.preservedCandidates.length, 1);
    assert.ok(context.entityTaskCount > 0);
  });

  it('qualified prospect evaluation always yields investigation path', () => {
    const payload = lot202Payload();
    const candidate = payload.rankedProspects[0];
    assert.equal(candidate.qualificationStatus, QUALIFICATION_STATUSES.QUALIFIED);
    assert.equal(candidate.readinessState, READINESS_STATES.UNKNOWN);
    assert.equal(candidate.prospectBucket, PROSPECT_BUCKETS.INVESTIGATION_REQUIRED);

    const tasks = buildEntityGapTasksForCandidate(candidate);
    assert.ok(tasks.length > 0);
    assert.ok(tasks.some((task) => task.hypothesis || task.gap));
  });
});
