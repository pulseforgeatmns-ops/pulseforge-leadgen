'use strict';

/**
 * Trace APPROVE_DISCOVERY → TME → Scout.discover → artifact → SEC
 * for Anchor PM mission without runScout override.
 */

const amo = require('../packages/acquisition-mission');
const { SPECIALISTS } = amo;
const { resolveCanonicalObjective } = require('../packages/max/workspace/ResolvedObjective');
const {
  advancePlanAfterApproval,
  advanceDiscoveryAfterApproval,
} = require('../packages/max/workspace/AmoOperatorApproval');
const {
  buildScoutDiscoveryArtifact,
} = require('../packages/scout/adapters/ScoutDiscoveryArtifact');
const { normalizeScoutDiscoveryPayload } = require('../packages/acquisition-mission/DiscoveryPayload');
const { fromScoutLegacyOutput } = require('../packages/acquisition-mission/SpecialistExecutionContract');

const BROAD_ANCHOR_OBJECTIVE =
  'Acquire one new recurring cleaning client for Anchor Cleaning in Greater Manchester. ' +
  'Prioritize high-fit commercial and property-management opportunities, including ' +
  'short-term rental operators where appropriate.';

function pmCandidates() {
  const now = new Date().toISOString();
  return [
    {
      id: 'pm-granite',
      tenantId: '10',
      name: 'Granite Property Management',
      companyName: 'Granite Property Management',
      industry: 'property_management',
      location: 'Manchester, NH',
      address: '100 Elm St, Manchester, NH',
      website: 'https://granitepm.example',
      phone: '603-555-0101',
      placeId: 'place-granite-pm',
      icpScore: 82,
      discoveredAt: now,
      signals: [],
      evidence: [],
    },
    {
      id: 'pm-mill',
      tenantId: '10',
      name: 'Mill City Property Management',
      companyName: 'Mill City Property Management',
      industry: 'property_management',
      location: 'Bedford, NH',
      address: '22 South River Rd, Bedford, NH',
      placeId: 'place-mill-pm',
      icpScore: 76,
      discoveredAt: now,
    },
  ];
}

function operationalPlacesProvider(candidates) {
  return {
    id: 'public_business_places',
    sourceType: 'public_business_data',
    available: () => true,
    lastExecution: {
      providerId: 'google_places',
      executed: true,
      abortReason: null,
      queries: [{ city: 'Manchester', status: 'OK', query: 'property management company Manchester NH' }],
      totals: { queries: 1, results: candidates.length, retries: 0, latencyMs: 12 },
      errors: [],
    },
    async discover() {
      return {
        source: 'public_business_places',
        sourceType: 'public_business_data',
        candidates,
        coverage: { queries: 1 },
        errors: [],
        available: true,
        execution: this.lastExecution,
      };
    },
    collectEvidence: async () => candidates,
    search: async () => candidates,
  };
}

function summarizeStage(label, data) {
  const payload = data.payload || data.discoveryPayload || data.contributions || data;
  return {
    label,
    blocked: payload.blocked,
    outcome: payload.outcome,
    status: data.status,
    qualifiedCount: payload.qualifiedCount,
    candidateUniverseCount:
      payload.candidateUniverseCount != null
        ? payload.candidateUniverseCount
        : Array.isArray(payload.candidateUniverse)
          ? payload.candidateUniverse.length
          : null,
    discoveryStatus: payload.discoveryStatus,
    providerExecutionCount: Array.isArray(payload.providerExecution)
      ? payload.providerExecution.length
      : 0,
    evidenceCount: Array.isArray(payload.evidence) ? payload.evidence.length : 0,
    requiredPrecondition: data.blocked?.requiredPrecondition || null,
  };
}

async function main() {
  const engine = amo.createAcquisitionMissionEngine();
  const mission = engine.create({
    tenantId: '10',
    clientId: 10,
    objective: BROAD_ANCHOR_OBJECTIVE,
    resolvedObjective: resolveCanonicalObjective({ question: BROAD_ANCHOR_OBJECTIVE }),
  });

  const planResult = await advancePlanAfterApproval({
    engine,
    mission,
    tenantId: '10',
    question: 'Approved. Proceed with this plan.',
  });

  const candidates = pmCandidates();
  const useInjectedDiscover = process.argv.includes('--inject-discover');
  const discoveryResult = await advanceDiscoveryAfterApproval({
    engine,
    mission: planResult.snapshot.mission,
    tenantId: '10',
    question: 'Approved. Begin Discovery.',
    allowFixtureFallback: false,
    enablePlaces: true,
    placesProvider: operationalPlacesProvider(candidates),
    ...(useInjectedDiscover
      ? {
          scoutCompanies: candidates,
          discover: async () => candidates,
        }
      : {}),
  });

  const scoutResult = discoveryResult.scoutResult || {};
  const intel = scoutResult.intelligenceResult || scoutResult;
  const rawPayload = intel.payload || scoutResult.payload || {};

  const artifact = buildScoutDiscoveryArtifact(scoutResult);
  const normalized = normalizeScoutDiscoveryPayload(scoutResult, { discoveryArtifact: artifact });
  const sec = fromScoutLegacyOutput(
    { ...scoutResult, discoveryPayload: normalized, payload: normalized },
    { specialist: SPECIALISTS.SCOUT, transactionId: discoveryResult.transactionId || 'audit-tx' }
  );

  const trace = {
    missionId: mission.id,
    transactionId: discoveryResult.transactionId,
    executionOutcome: discoveryResult.executionOutcome,
    stages: [
      summarizeStage('scout_intelligence_payload', rawPayload),
      summarizeStage('artifact', artifact),
      summarizeStage('normalized_contribution', normalized),
      summarizeStage('sec_output', sec),
    ],
    pendingOperatorDecision: discoveryResult.snapshot?.mission?.pendingOperatorDecision || null,
    artifactPath: 'ScoutDiscoveryExecutor.mapScoutIntelligenceToDiscoveryPayload → buildScoutDiscoveryArtifact',
  };

  console.log(JSON.stringify(trace, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
