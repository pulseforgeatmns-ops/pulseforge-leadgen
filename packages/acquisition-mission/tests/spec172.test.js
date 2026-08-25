'use strict';

/**
 * SPEC-172 — Canonical Scout Evidence Handoff.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const amo = require('../index');
const {
  collectCanonicalScoutEvidence,
  buildScoutDiscoveryArtifact,
  assertScoutEvidenceHandoff,
  deduplicateCanonicalEvidence,
  SCOUT_EVIDENCE_HANDOFF_VIOLATION,
} = require('../../scout/adapters/ScoutDiscoveryArtifact');
const { normalizeScoutDiscoveryPayload } = require('../DiscoveryPayload');
const { assertEvidenceAttached } = require('../TransactionalExecution');
const {
  advancePlanAfterApproval,
  advanceDiscoveryAfterApproval,
} = require('../../max/workspace/AmoOperatorApproval');
const { resetEngine } = require('../../../services/acquisitionMission');

const OBJECTIVE = 'Acquire commercial cleaning customers in Manchester NH for law firms.';

function baseEvidence(id, label, source, company) {
  return {
    id,
    label,
    snapshot: { source, companyName: company },
  };
}

describe('SPEC-172 — Canonical Scout Evidence Handoff', () => {
  beforeEach(() => {
    resetEngine();
  });

  it('Scenario A — supported opportunity evidence survives handoff', () => {
    const scoutResult = {
      status: 'completed',
      confidence: 0.74,
      payload: {
        opportunities: [
          {
            companyId: 'co-1',
            name: 'Harbor Law Group',
            fit: 0.78,
            signals: [{ type: 'hiring', label: 'Hiring ops manager', source: 'job_board' }],
            evidenceRefs: [baseEvidence('ev-1', 'Job posting', 'job_board', 'Harbor Law Group')],
          },
        ],
        qualifiedCount: 1,
      },
    };

    const artifact = buildScoutDiscoveryArtifact(scoutResult);
    assert.equal(artifact.evidence.length, 1);
    assert.equal(artifact.evidence[0].id, 'ev-1');

    const payload = normalizeScoutDiscoveryPayload(scoutResult);
    assertScoutEvidenceHandoff(artifact, payload);
    assert.equal(payload.evidence.length, 1);
    assert.doesNotThrow(() => assertEvidenceAttached(payload));
  });

  it('Scenario B — fit candidate only evidence survives handoff (AUDIT-051)', () => {
    const scoutResult = {
      status: 'completed',
      confidence: 0.62,
      payload: {
        opportunities: [],
        fitCandidates: [
          {
            companyId: 'co-fit',
            name: 'Summit STR Management',
            fit: 0.81,
            signals: [{ type: 'portfolio', label: '12 active STR listings', source: 'website' }],
            evidenceRefs: [
              baseEvidence('ev-str', 'STR portfolio page lists 12 units', 'website', 'Summit STR Management'),
            ],
          },
        ],
        qualifiedCount: 0,
      },
    };

    const artifact = buildScoutDiscoveryArtifact(scoutResult);
    assert.equal(artifact.evidence.length, 1);
    assert.equal(artifact.evidence[0].claim, 'STR portfolio page lists 12 units');

    const payload = normalizeScoutDiscoveryPayload(scoutResult);
    assertScoutEvidenceHandoff(artifact, payload);
    assert.equal(payload.evidence.length, 1);
    assert.doesNotThrow(() => assertEvidenceAttached(payload));
  });

  it('Scenario C — top-level evidenceRefs survive handoff', () => {
    const scoutResult = {
      status: 'completed',
      evidenceRefs: [baseEvidence('ev-top', 'Market scan completed', 'google_places', null)],
      payload: {
        opportunities: [],
        qualifiedCount: 0,
      },
    };

    const artifact = buildScoutDiscoveryArtifact(scoutResult);
    assert.equal(artifact.evidence.length, 1);

    const payload = normalizeScoutDiscoveryPayload(scoutResult);
    assert.equal(payload.evidence.length, 1);
    assert.doesNotThrow(() => assertEvidenceAttached(payload));
  });

  it('Scenario D — investigative reasoning evidence graph and MIR survive handoff', () => {
    const scoutResult = {
      status: 'completed',
      intelligenceResult: {
        status: 'completed',
        confidence: 0.7,
        payload: { opportunities: [], qualifiedCount: 0 },
      },
      pipeline: {
        missionIntelligenceReport: {
          recommendation: { summary: 'Investigate STR operators first.', confidence: 0.7 },
          evidenceGraphSummary: { nodeCount: 3, edgeCount: 2, byType: { EVIDENCE: 1 } },
          businessUnderstanding: { items: [{ entity: 'Summit STR', assertions: ['Manages rentals'] }] },
        },
        investigationState: {
          evidenceGraph: {
            nodes: [
              {
                id: 'ev-graph-1',
                type: 'EVIDENCE',
                label: 'Rental portfolio page',
                data: { source: 'website', kind: 'web_presence', observedAt: '2026-08-01T00:00:00.000Z' },
              },
            ],
            edges: [],
          },
        },
      },
    };

    const artifact = buildScoutDiscoveryArtifact(scoutResult);
    assert.equal(artifact.evidence.length, 1);
    assert.ok(artifact.missionIntelligenceReport);
    assert.ok(artifact.missionIntelligenceReport.evidenceGraphSummary);
    assert.equal(artifact.missionIntelligenceReport.recommendation.summary, 'Investigate STR operators first.');

    const payload = normalizeScoutDiscoveryPayload(scoutResult);
    assert.ok(payload.missionIntelligenceReport);
    assert.equal(payload.evidence.length, 1);
    assert.doesNotThrow(() => assertEvidenceAttached(payload));
  });

  it('Scenario E — duplicate evidence deduplicates to one canonical record', () => {
    const shared = baseEvidence('ev-dup', 'Same job posting', 'job_board', 'Harbor Law Group');
    const scoutResult = {
      status: 'completed',
      evidenceRefs: [shared],
      payload: {
        opportunities: [
          {
            companyId: 'co-1',
            name: 'Harbor Law Group',
            evidenceRefs: [shared],
          },
        ],
        fitCandidates: [
          {
            companyId: 'co-1',
            name: 'Harbor Law Group',
            evidenceRefs: [shared],
          },
        ],
      },
    };

    const collected = collectCanonicalScoutEvidence(scoutResult);
    assert.equal(collected.length, 1);
    assert.ok(collected[0].provenance);

    const payload = normalizeScoutDiscoveryPayload(scoutResult);
    assert.equal(payload.evidence.length, 1);
  });

  it('Scenario F — legitimately blocked discovery does not fabricate evidence', () => {
    const scoutResult = {
      status: 'blocked',
      summary: 'Required sources unavailable.',
      payload: {
        opportunities: [],
        fitCandidates: [],
        outcome: 'blocked',
        qualifiedCount: 0,
      },
    };

    const artifact = buildScoutDiscoveryArtifact(scoutResult);
    assert.equal(artifact.blocked, true);
    assert.equal(artifact.evidence.length, 0);

    const payload = normalizeScoutDiscoveryPayload(scoutResult);
    assert.equal(payload.blocked, true);
    assert.equal(payload.evidence.length, 0);
    assert.doesNotThrow(() => assertEvidenceAttached(payload, { required: false }));
    assert.doesNotThrow(() => assertScoutEvidenceHandoff(artifact, payload));
  });

  it('Scenario G — evidence lost during normalization fails with SCOUT_EVIDENCE_HANDOFF_VIOLATION', () => {
    const artifact = {
      blocked: false,
      evidence: [{ id: 'ev-1', source: 'job_board', claim: 'Hiring signal' }],
    };
    const emptyPayload = { blocked: false, evidence: [], buyingSignals: [] };

    assert.throws(
      () => assertScoutEvidenceHandoff(artifact, emptyPayload),
      (err) => err.code === SCOUT_EVIDENCE_HANDOFF_VIOLATION
    );
  });

  it('deduplicateCanonicalEvidence keeps the richest representation', () => {
    const deduped = deduplicateCanonicalEvidence([
      {
        id: 'ev-1',
        source: 'job_board',
        claim: 'Hiring signal',
        provenance: { kind: 'observed', source: 'job_board' },
      },
      {
        id: 'ev-1',
        source: 'job_board',
        claim: 'Hiring signal',
        observedAt: '2026-08-01T00:00:00.000Z',
        entityId: 'co-1',
        provenance: { kind: 'observed', source: 'job_board', collectedFrom: 'payload.opportunities' },
        confidence: 0.82,
        originalRef: 'ev-1',
      },
    ]);
    assert.equal(deduped.length, 1);
    assert.equal(deduped[0].observedAt, '2026-08-01T00:00:00.000Z');
    assert.equal(deduped[0].confidence, 0.82);
  });

  it('Scenario H — end-to-end discovery commit with fit-candidate evidence', async () => {
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

    const discoveryResult = await advanceDiscoveryAfterApproval({
      engine,
      mission: planResult.snapshot.mission,
      tenantId: '10',
      question: 'Approved. Begin Discovery.',
      runScout: async () => ({
        status: 'completed',
        confidence: 0.74,
        payload: {
          opportunities: [],
          fitCandidates: [
            {
              companyId: 'co-anchor',
              name: 'Anchor Property Mgmt',
              fit: 0.79,
              signals: [{ type: 'portfolio', label: 'Multi-unit portfolio', source: 'website' }],
              evidenceRefs: [
                baseEvidence('ev-anchor', 'Portfolio page lists 8 units', 'website', 'Anchor Property Mgmt'),
              ],
            },
          ],
          qualifiedCount: 0,
        },
        pipeline: {
          missionIntelligenceReport: {
            recommendation: { summary: 'Prioritize Anchor Property Mgmt.', confidence: 0.74 },
          },
        },
      }),
    });

    assert.equal(discoveryResult.executionOutcome, 'completed');
    assert.equal(discoveryResult.discovery.specialist, 'scout');
    assert.ok(discoveryResult.discovery.payload.evidence.length >= 1);
    assert.ok(discoveryResult.discovery.payload.missionIntelligenceReport);
    assert.equal(
      discoveryResult.snapshot.mission.pendingOperatorDecision.kind,
      'prioritization_approval'
    );
    const scoutContributions = discoveryResult.snapshot.contributions.filter(
      (row) => row.specialist === 'scout' && row.kind === 'discovery'
    );
    assert.ok(scoutContributions.length >= 1);
    assert.equal(discoveryResult.snapshot.workspace.scout.state, 'complete');
  });
});
