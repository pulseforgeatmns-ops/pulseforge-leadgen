'use strict';

/**
 * SPEC-184 — Provider Execution Continuity (ADR-099).
 *
 * providerExecution must survive normalization, persistence, validation errors,
 * rollback inspection, and operator presentation.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const amo = require('../index');
const {
  buildScoutDiscoveryArtifact,
} = require('../../scout/adapters/ScoutDiscoveryArtifact');
const { normalizeScoutDiscoveryPayload } = require('../DiscoveryPayload');
const {
  presentationFromDiscoveryPayload,
  formatDiscoveryResultsProse,
  findLatestDiscoveryContribution,
} = require('../DiscoveryPresentation');
const {
  normalizeProviderExecution,
  formatProviderExecutionProse,
  extractProviderExecutionFromOutput,
} = require('../../scout/coverage/ProviderExecution');
const { assertEvidenceAttached } = require('../TransactionalExecution');
const {
  advancePlanAfterApproval,
  advanceDiscoveryAfterApproval,
} = require('../../max/workspace/AmoOperatorApproval');
const { buildExecutionMissionResponse } = require('../../max/workspace/AcquisitionMissionExecution');
const { resetEngine } = require('../../../services/acquisitionMission');
const { INVESTIGATIVE_EVIDENCE } = require('../../scout/coverage/EvidenceRequirements');

const OBJECTIVE = 'Find property managers who outsource cleaning in Greater Manchester.';

function sampleProviderReport(overrides = {}) {
  return {
    providerId: 'google_maps',
    providerLabel: 'Google Maps',
    evidenceType: INVESTIGATIVE_EVIDENCE.IDENTITY,
    status: 'empty',
    rawResultCount: 18,
    mappedCandidateCount: 0,
    candidates: [],
    execution: {
      providerId: 'google_places',
      executed: true,
      queries: [
        {
          query: 'property management company Manchester NH',
          httpStatus: 200,
          googleStatus: 'ZERO_RESULTS',
          resultCount: 18,
          latencyMs: 412,
        },
      ],
      totals: { queries: 1, results: 18, retries: 0, latencyMs: 412 },
      errors: [],
    },
    ...overrides,
  };
}

function scoutResultWithProviderExecution(report = sampleProviderReport()) {
  return {
    status: 'completed',
    confidence: 0.74,
    payload: {
      opportunities: [
        {
          companyId: 'co-harbor',
          name: 'Harbor Property Mgmt',
          fit: 0.72,
          signals: [{ type: 'portfolio', label: 'Multi-unit portfolio', source: 'website' }],
          evidenceRefs: [
            {
              id: 'ev-harbor',
              label: 'Portfolio page lists managed units',
              snapshot: { source: 'website', companyName: 'Harbor Property Mgmt' },
            },
          ],
        },
      ],
      qualifiedCount: 1,
      providerExecution: [report],
    },
  };
}

describe('SPEC-184 — Provider Execution Continuity', () => {
  beforeEach(() => {
    resetEngine();
  });

  it('normalizeScoutDiscoveryPayload preserves providerExecution from artifact', () => {
    const scoutResult = scoutResultWithProviderExecution();
    const artifact = buildScoutDiscoveryArtifact(scoutResult);
    assert.ok(Array.isArray(artifact.providerExecution));
    assert.equal(artifact.providerExecution.length, 1);

    const contribution = normalizeScoutDiscoveryPayload(scoutResult, { discoveryArtifact: artifact });
    assert.ok(Array.isArray(contribution.providerExecution));
    assert.equal(contribution.providerExecution.length, 1);
    assert.equal(contribution.providerExecution[0].provider, 'Google Maps');
    assert.equal(contribution.providerExecution[0].query, 'property management company Manchester NH');
    assert.equal(contribution.providerExecution[0].httpStatus, 200);
    assert.equal(contribution.providerExecution[0].googleStatus, 'ZERO_RESULTS');
    assert.equal(contribution.providerExecution[0].latencyMs, 412);
    assert.equal(contribution.providerExecution[0].results, 18);
    assert.equal(contribution.providerExecution[0].qualified, 0);
    assert.equal(contribution.providerExecution[0].observational, true);
  });

  it('Discovery presentation renders provider execution diagnostics', () => {
    const contribution = normalizeScoutDiscoveryPayload(scoutResultWithProviderExecution());
    const presentation = presentationFromDiscoveryPayload(contribution);
    assert.equal(presentation.providerExecution.length, 1);

    const prose = formatDiscoveryResultsProse(contribution);
    assert.match(prose, /Provider Execution/i);
    assert.match(prose, /Google Maps/);
    assert.match(prose, /property management company Manchester NH/);
    assert.match(prose, /HTTP: 200/);
    assert.match(prose, /Google Status: ZERO_RESULTS/);
    assert.match(prose, /Latency: 412 ms/);
    assert.match(prose, /Results: 18/);
    assert.match(prose, /Qualified: 0/);
    assert.match(prose, /Evidence Requirement: identity/);
  });

  it('REQUEST_DENIED provider execution surfaces HTTP and status in presentation', () => {
    const report = sampleProviderReport({
      status: 'failed',
      error: 'google_places_status_REQUEST_DENIED',
      rawResultCount: 0,
      mappedCandidateCount: 0,
      execution: {
        providerId: 'google_places',
        executed: true,
        queries: [
          {
            query: 'property management company Manchester NH',
            httpStatus: 403,
            googleStatus: 'REQUEST_DENIED',
            googleError: 'The provided API key is invalid.',
            resultCount: 0,
            latencyMs: 95,
          },
        ],
        totals: { queries: 1, results: 0, retries: 0, latencyMs: 95 },
        errors: [
          {
            code: 'google_places_status_REQUEST_DENIED',
            message: 'The provided API key is invalid.',
            httpStatus: 403,
            googleStatus: 'REQUEST_DENIED',
          },
        ],
      },
    });
    const prose = formatProviderExecutionProse([report]);
    assert.match(prose, /HTTP: 403/);
    assert.match(prose, /Google Status: REQUEST_DENIED/);
  });

  it('validation errors preserve provider diagnostics via extractProviderExecutionFromOutput', () => {
    const contribution = normalizeScoutDiscoveryPayload({
      status: 'completed',
      payload: {
        opportunities: [],
        qualifiedCount: 0,
        providerExecution: [sampleProviderReport()],
      },
    });
    const output = {
      scoutResult: { payload: { providerExecution: [sampleProviderReport()] } },
      discoveryPayload: contribution,
    };

    assert.throws(() => assertEvidenceAttached(contribution), /Contribution must attach evidence/);

    const extracted = extractProviderExecutionFromOutput(output);
    assert.equal(extracted.length, 1);
    assert.equal(extracted[0].googleStatus, 'ZERO_RESULTS');
  });

  it('discovery contribution persists providerExecution with mission commit', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: OBJECTIVE,
      targetSegment: 'Law Firms',
    });

    const planResult = await advancePlanAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved. Proceed with this plan.',
    });

    const providerReport = sampleProviderReport();
    const discoveryResult = await advanceDiscoveryAfterApproval({
      engine,
      mission: planResult.snapshot.mission,
      tenantId: '10',
      question: 'Approved. Begin Discovery.',
      runScout: async () => scoutResultWithProviderExecution(providerReport),
    });

    const snapshot = discoveryResult.snapshot;
    const contribution = findLatestDiscoveryContribution(snapshot.contributions || []);
    assert.ok(contribution);
    assert.ok(Array.isArray(contribution.payload.providerExecution));
    assert.equal(contribution.payload.providerExecution.length, 1);
    assert.equal(contribution.payload.providerExecution[0].googleStatus, 'ZERO_RESULTS');
  });

  it('rolled-back validation response includes provider diagnostics for operator', () => {
    const providerExecution = normalizeProviderExecution([sampleProviderReport()]);
    const response = buildExecutionMissionResponse({
      mission: { id: 'm-1', title: 'Test Mission', objective: OBJECTIVE, stage: 'discover' },
      snapshot: { workspace: {}, health: { label: 'Healthy' } },
      action: 'discovery_approved',
      question: 'Approved. Begin Discovery.',
      executionResult: {
        rolledBack: true,
        error: {
          message: 'Contribution must attach evidence.',
          rollbackReason: 'Contribution must attach evidence.',
          tmeClass: 'validation',
          details: { providerExecution },
        },
      },
    });

    assert.match(response.prose, /Discovery could not execute/);
    assert.match(response.prose, /Provider Execution/i);
    assert.match(response.prose, /Google Maps/);
    assert.match(response.prose, /ZERO_RESULTS/);
    assert.match(response.comm.evidenceStatus, /Contribution must attach evidence/);
    assert.match(response.comm.evidenceStatus, /ZERO_RESULTS/);
  });

  it('providerExecution is observational and does not satisfy evidence validation', () => {
    const contribution = normalizeScoutDiscoveryPayload({
      status: 'completed',
      payload: {
        opportunities: [],
        qualifiedCount: 0,
        providerExecution: [sampleProviderReport()],
      },
    });
    assert.equal(contribution.providerExecution.length, 1);
    assert.throws(() => assertEvidenceAttached(contribution), /Contribution must attach evidence/);
  });
});
