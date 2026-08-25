'use strict';

/**
 * SPEC-177 — Hypothesis-Driven Discovery Engine acceptance tests.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const { buildMarketDefinition } = require('../packages/scout/intelligence/MarketUnderstanding');
const { createInjectedDiscoverAdapter } = require('../packages/max/scoutAcquisition/DiscoveryAdapters');
const { buildAcquisitionSearchDefinition } = require('../packages/max/scoutAcquisition/SearchDefinition');
const {
  INVESTIGATIVE_EVIDENCE,
  INVESTIGATIVE_QUESTIONS,
  deriveQuestionsFromHypotheses,
  buildEvidenceRequirementsFromQuestions,
} = require('../packages/scout/coverage/EvidenceRequirements');
const {
  assignProvidersForEvidence,
  assignProvidersForRequirements,
  explainProviderForOperator,
  EVIDENCE_TO_PROVIDERS,
} = require('../packages/scout/coverage/EvidenceProviderAssignment');
const {
  createHypothesisInvestigationPlan,
  getNextInvestigationTasks,
  updatePlanAfterEvidence,
  markInvestigationComplete,
} = require('../packages/scout/coverage/HypothesisInvestigationPlanner');
const {
  runHypothesisDrivenDiscovery,
  mergeIdentities,
  explainProviderUsage,
} = require('../packages/scout/coverage/HypothesisDrivenDiscoveryEngine');
const { generateHypotheses } = require('../packages/scout/investigation/HypothesisGeneration');
const { normalizeProviderReport } = require('../packages/scout/coverage/ProviderEvidenceContract');
const { createDefaultProviderRegistry } = require('../packages/scout/intelligence/ProviderCapabilityRegistry');

function propertyManagerMission() {
  return {
    id: 'mission-pm-1',
    tenantId: '10',
    objectiveText: 'Find property managers who outsource cleaning in Greater Manchester.',
    constraints: { vertical: 'property_management', locationHint: 'Manchester NH' },
  };
}

function propertyManagerMarket(mission = propertyManagerMission()) {
  return buildMarketDefinition({ mission });
}

function makePlacesAdapter(candidatesByCall = () => []) {
  let callCount = 0;
  return createInjectedDiscoverAdapter(async () => {
    callCount += 1;
    return candidatesByCall(callCount);
  });
}

describe('SPEC-177 — Hypothesis-Driven Discovery Engine', () => {
  it('derives investigative questions from business hypotheses, not search terms', () => {
    const market = propertyManagerMarket();
    const hypotheses = generateHypotheses(market, propertyManagerMission());
    const questions = deriveQuestionsFromHypotheses(hypotheses, market);

    assert.ok(questions.length >= 3);
    assert.ok(questions.some((q) => q.question === INVESTIGATIVE_QUESTIONS.BUSINESS_EXISTS));
    assert.ok(questions.some((q) => q.text.includes('buying decisions') || q.question === INVESTIGATIVE_QUESTIONS.BUYING_DECISIONS));
    assert.ok(questions.every((q) => Array.isArray(q.requiredEvidence) && q.requiredEvidence.length > 0));
    assert.ok(questions.every((q) => !q.text.includes('Google Places')));
  });

  it('Scenario 1: Property Manager hypothesis assigns identity providers, not LinkedIn first', () => {
    const market = propertyManagerMarket();
    const hypotheses = generateHypotheses(market, propertyManagerMission());
    const questions = deriveQuestionsFromHypotheses(hypotheses, market);
    const requirements = buildEvidenceRequirementsFromQuestions(questions);
    const assignments = assignProvidersForRequirements(requirements);

    const identityProviders = assignments
      .filter((a) => a.evidenceType === INVESTIGATIVE_EVIDENCE.IDENTITY)
      .map((a) => a.providerId);

    assert.ok(identityProviders.includes('google_maps') || identityProviders.includes('county_records'));
    assert.ok(!identityProviders.includes('linkedin'));

    const plan = createHypothesisInvestigationPlan({
      mission: propertyManagerMission(),
      marketDefinition: market,
      hypotheses,
    });

    const firstTasks = getNextInvestigationTasks(plan);
    assert.ok(firstTasks.length > 0);
    assert.equal(firstTasks[0].evidenceType, INVESTIGATIVE_EVIDENCE.IDENTITY);
    assert.ok(!firstTasks.some((t) => t.evidenceType === INVESTIGATIVE_EVIDENCE.DECISION_MAKERS));
  });

  it('Scenario 2: after identity established, decision-maker investigation begins', () => {
    const market = propertyManagerMarket();
    const hypotheses = generateHypotheses(market, propertyManagerMission());
    let plan = createHypothesisInvestigationPlan({
      mission: propertyManagerMission(),
      marketDefinition: market,
      hypotheses,
    });

    plan = updatePlanAfterEvidence(plan, [
      { evidenceType: INVESTIGATIVE_EVIDENCE.IDENTITY, type: INVESTIGATIVE_EVIDENCE.IDENTITY },
    ]);

    const nextTasks = getNextInvestigationTasks(plan);
    const taskTypes = nextTasks.map((t) => t.evidenceType);
    assert.ok(taskTypes.includes(INVESTIGATIVE_EVIDENCE.DECISION_MAKERS) || taskTypes.includes(INVESTIGATIVE_EVIDENCE.PORTFOLIO));
  });

  it('Scenario 3: website unavailable — investigation continues with other providers', () => {
    const assignments = assignProvidersForEvidence(INVESTIGATIVE_EVIDENCE.DECISION_MAKERS, {
      unavailableProviders: ['website'],
    });
    const providerIds = assignments.map((a) => a.providerId);
    assert.ok(providerIds.includes('linkedin') || providerIds.includes('prospeo'));
    const websiteEntry = assignments.find((a) => a.providerId === 'website');
    if (websiteEntry) assert.equal(websiteEntry.status, 'unavailable');
  });

  it('Scenario 4: Google Places unavailable — substitutes registry providers', () => {
    const assignments = assignProvidersForEvidence(INVESTIGATIVE_EVIDENCE.IDENTITY, {
      unavailableProviders: ['google_maps'],
    });
    const providerIds = assignments.map((a) => a.providerId);
    assert.ok(providerIds.some((id) => ['county_records', 'existing_pf'].includes(id)));
    const googleEntry = assignments.find((a) => a.providerId === 'google_maps');
    if (googleEntry) assert.equal(googleEntry.status, 'unavailable');
  });

  it('Scenario 5: overlapping businesses merge into one canonical identity', () => {
    const merged = mergeIdentities([
      {
        id: 'a1',
        name: 'Granite Property Management',
        address: '100 Main St, Manchester NH',
        placeId: 'place-123',
        source: 'google_maps',
      },
      {
        id: 'a2',
        name: 'Granite Property Management',
        address: '100 Main St, Manchester NH',
        registry_id: 'REG-456',
        source: 'county_records',
      },
    ]);

    assert.equal(merged.length, 1);
    assert.ok(merged[0]._mergedFrom.length >= 2);
    assert.ok(merged[0].placeId || merged[0].registry_id);
  });

  it('Scenario 6: sufficient evidence marks hypothesis investigated and skips further work', () => {
    const market = propertyManagerMarket();
    const hypotheses = generateHypotheses(market, propertyManagerMission());
    let plan = createHypothesisInvestigationPlan({
      mission: propertyManagerMission(),
      marketDefinition: market,
      hypotheses,
    });

    const allEvidence = plan.evidenceRequirements.map((r) => ({
      evidenceType: r.evidenceType,
      type: r.evidenceType,
    }));
    plan = updatePlanAfterEvidence(plan, allEvidence);
    plan = markInvestigationComplete(plan);

    assert.equal(plan.sufficientlyInvestigated, true);
    assert.equal(getNextInvestigationTasks(plan).length, 0);
    assert.ok(plan.tasks.every((t) => t.status !== 'pending'));
  });

  it('Scenario 7: Anchor Cleaning STR mission — no provider-specific logic in planning layer', () => {
    const mission = {
      id: 'mission-anchor-10',
      tenantId: '10',
      objectiveText: 'Acquire commercial cleaning clients from property managers in Manchester NH.',
      constraints: { vertical: 'property_management', locationHint: 'Manchester NH' },
    };
    const market = buildMarketDefinition({ mission });
    const plan = createHypothesisInvestigationPlan({ mission, marketDefinition: market });

    assert.equal(plan.version, 'SPEC-177');
    assert.ok(plan.hypotheses.length > 0);
    assert.ok(plan.questions.length > 0);
    assert.ok(plan.evidenceRequirements.length > 0);
    assert.ok(plan.tasks.length > 0);

    const taskLabels = plan.tasks.map((t) => t.label).join(' ');
    assert.ok(!taskLabels.includes('Google Places'));
    assert.ok(!taskLabels.includes('property manager Bedford NH'));

    const phases = plan.tasks.map((t) => t.evidenceType);
    assert.ok(phases.includes(INVESTIGATIVE_EVIDENCE.IDENTITY));
    assert.ok(
      phases.includes(INVESTIGATIVE_EVIDENCE.DECISION_MAKERS) ||
        phases.includes(INVESTIGATIVE_EVIDENCE.CLEANING)
    );
  });

  it('provider contract reports evidence produced, confidence, coverage, and limitations', () => {
    const report = normalizeProviderReport(
      {
        candidates: [{ id: '1', name: 'Test PM', phone: '603-555-0100', placeId: 'p1' }],
        errors: [],
      },
      {
        providerId: 'google_maps',
        providerLabel: 'Google Maps',
        evidenceType: INVESTIGATIVE_EVIDENCE.IDENTITY,
        task: 'Collect business identities',
        confidence: 0.9,
        coverage: 0.85,
      }
    );

    assert.ok(report.evidenceProduced.includes('identity'));
    assert.ok(report.confidence > 0);
    assert.ok(report.coverage > 0);
    assert.equal(report.status, 'completed');
    assert.ok(Array.isArray(report.limitations));
  });

  it('operator explainability: Max can explain LinkedIn selection', () => {
    const market = propertyManagerMarket();
    const plan = createHypothesisInvestigationPlan({
      mission: propertyManagerMission(),
      marketDefinition: market,
    });

    const linkedinAssignment = plan.assignedProviders.find((a) => a.providerId === 'linkedin');
    if (linkedinAssignment) {
      const explanation = explainProviderForOperator(linkedinAssignment, plan);
      assert.match(explanation, /decision maker|organizational role/i);
    }

    const explanation = explainProviderUsage(plan, 'linkedin');
    if (explanation) {
      assert.match(explanation, /LinkedIn|decision maker|organizational/i);
    }
  });

  it('executes investigation tasks end-to-end via injected adapter', async () => {
    const mission = propertyManagerMission();
    const market = propertyManagerMarket();
    const searchDefinition = buildAcquisitionSearchDefinition({
      tenantId: '10',
      geography: { label: 'Manchester NH', state: 'NH' },
      segments: ['property_management'],
    });

    const adapter = makePlacesAdapter(() => [
      { id: 'pm-1', name: 'Granite Property Management', address: '100 Main St, Manchester NH', phone: '603-555-0100', placeId: 'place-1' },
      { id: 'pm-2', name: 'Lakes Region PM', address: '200 Elm St, Bedford NH', phone: '603-555-0200', placeId: 'place-2' },
    ]);

    const result = await runHypothesisDrivenDiscovery({
      mission,
      marketDefinition: { ...market, searchDefinition },
      searchDefinition,
      adapters: [adapter],
      opts: { maxIterations: 3, requireEstablishedIdentity: true },
    });

    assert.ok(result.investigationPlan);
    assert.equal(result.investigationPlan.version, 'SPEC-177');
    assert.ok(result.executedTasks.length > 0);
    assert.ok(result.candidates.length >= 1);
    assert.ok(result.investigationState);
    assert.ok(result.investigationState.investigationPlan);
    assert.ok(result.investigationState.satisfiedEvidence || result.investigationState.outstandingEvidence);
  });

  it('planner assigns providers by evidence type mapping, not hardcoded queries', () => {
    assert.ok(EVIDENCE_TO_PROVIDERS[INVESTIGATIVE_EVIDENCE.IDENTITY].includes('google_maps'));
    assert.ok(EVIDENCE_TO_PROVIDERS[INVESTIGATIVE_EVIDENCE.DECISION_MAKERS].includes('linkedin'));
    assert.ok(EVIDENCE_TO_PROVIDERS[INVESTIGATIVE_EVIDENCE.REVIEWS].includes('google_maps'));

    const registry = createDefaultProviderRegistry();
    const identityAssignments = assignProvidersForEvidence(INVESTIGATIVE_EVIDENCE.IDENTITY, { registry });
    assert.ok(identityAssignments.every((a) => a.task.startsWith('Collect')));
    assert.ok(identityAssignments.every((a) => a.rationale.length > 0));
  });
});
