'use strict';

/**
 * SPEC-174 — Canonical Evidence Coverage.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const amo = require('../index');
const {
  collectCanonicalScoutEvidence,
  buildScoutDiscoveryArtifact,
  assertScoutEvidenceCoverage,
  collectExportableEvidenceIdentities,
  SCOUT_EVIDENCE_COVERAGE_VIOLATION,
  SCOUT_EVIDENCE_SOURCES,
  GRAPH_NODE_EVIDENCE_CLASSIFICATION,
  EVIDENCE_CLASSIFICATION,
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

describe('SPEC-174 — Canonical Evidence Coverage', () => {
  beforeEach(() => {
    resetEngine();
  });

  it('Scenario 1 — candidateUniverse[].evidence is exported', () => {
    const scoutResult = {
      status: 'completed',
      payload: {
        opportunities: [],
        qualifiedCount: 0,
        candidateUniverse: [
          {
            candidate_id: 'crm-1',
            name: 'Harbor Law Group',
            evidence: [
              baseEvidence('ev-universe', 'CRM record shows 40 staff', 'existing_repository', 'Harbor Law Group'),
            ],
          },
        ],
      },
    };

    const artifact = buildScoutDiscoveryArtifact(scoutResult);
    assert.equal(artifact.evidence.length, 1);
    assert.equal(artifact.evidence[0].id, 'ev-universe');
    assert.equal(artifact.evidence[0].originalLocation, 'payload.candidateUniverse[].evidence');
    assert.equal(
      artifact.evidence[0].provenance.normalizationPath,
      'ScoutDiscoveryArtifact.normalizeCanonicalEvidenceItem'
    );
  });

  it('Scenario 2 — evaluatedCandidates[].evidence is exported', () => {
    const scoutResult = {
      status: 'completed',
      payload: {
        opportunities: [],
        qualifiedCount: 0,
        evaluatedCandidates: [
          {
            companyId: 'co-eval',
            name: 'Summit STR Management',
            evidence: [
              baseEvidence('ev-eval', 'Website portfolio page', 'website', 'Summit STR Management'),
            ],
          },
        ],
      },
    };

    const artifact = buildScoutDiscoveryArtifact(scoutResult);
    assert.equal(artifact.evidence.length, 1);
    assert.equal(artifact.evidence[0].claim, 'Website portfolio page');
    assert.equal(artifact.evidence[0].originalLocation, 'payload.evaluatedCandidates[].evidence');
  });

  it('Scenario 3 — investigation graph evidence nodes are exported', () => {
    const scoutResult = {
      status: 'completed',
      intelligenceResult: {
        status: 'completed',
        payload: { opportunities: [], qualifiedCount: 0 },
      },
      pipeline: {
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
    assert.equal(artifact.evidence[0].originalLocation, 'investigationGraph.nodes[EVIDENCE]');
  });

  it('Scenario 4 — legacy discoveryReport.evidenceSources export when payload.evidence is empty', () => {
    const scoutResult = {
      status: 'completed',
      discoveryReport: {
        outcome: 'completed',
        evidenceSources: [
          { source: 'google_places', attempted: true, succeeded: true },
          { source: 'linkedin', attempted: true, succeeded: false },
        ],
      },
      payload: {
        opportunities: [],
        qualifiedCount: 0,
        evidence: [],
      },
    };

    const artifact = buildScoutDiscoveryArtifact(scoutResult);
    assert.equal(artifact.evidence.length, 1);
    assert.equal(artifact.evidence[0].source, 'google_places');
    assert.equal(artifact.evidence[0].originalLocation, 'discoveryReport.evidenceSources');
  });

  it('Scenario 5 — mixed evidence deduplicates to one canonical item', () => {
    const shared = baseEvidence('ev-dup', 'Same job posting', 'job_board', 'Harbor Law Group');
    const scoutResult = {
      status: 'completed',
      evidenceRefs: [shared],
      payload: {
        opportunities: [{ companyId: 'co-1', name: 'Harbor Law Group', evidenceRefs: [shared] }],
        candidateUniverse: [
          { candidate_id: 'co-1', name: 'Harbor Law Group', evidence: [shared] },
        ],
        qualifiedCount: 1,
      },
    };

    const collected = collectCanonicalScoutEvidence(scoutResult);
    assert.equal(collected.length, 1);
    assert.doesNotThrow(() => assertScoutEvidenceCoverage(scoutResult, collected));
  });

  it('Scenario 6 — hypothesis graph nodes are not exported', () => {
    const scoutResult = {
      status: 'completed',
      pipeline: {
        investigationState: {
          evidenceGraph: {
            nodes: [
              {
                id: 'hyp-1',
                type: 'HYPOTHESIS',
                label: 'STR operators dominate Manchester',
                data: { text: 'STR operators dominate Manchester' },
              },
              {
                id: 'ev-graph-2',
                type: 'EVIDENCE',
                label: 'Places result',
                data: { source: 'google_places', kind: 'directory_listing' },
              },
            ],
            edges: [],
          },
        },
      },
      payload: { opportunities: [], qualifiedCount: 0 },
    };

    assert.equal(GRAPH_NODE_EVIDENCE_CLASSIFICATION.hypothesis, EVIDENCE_CLASSIFICATION.INTERNAL);

    const artifact = buildScoutDiscoveryArtifact(scoutResult);
    assert.equal(artifact.evidence.length, 1);
    assert.equal(artifact.evidence[0].id, 'ev-graph-2');
  });

  it('Scenario 7 — coverage assertion passes when all exportable sources are traversed', () => {
    const scoutResult = {
      status: 'completed',
      evidenceRefs: [baseEvidence('ev-top', 'Market scan', 'google_places', null)],
      payload: {
        opportunities: [],
        fitCandidates: [
          {
            companyId: 'co-fit',
            name: 'Anchor Property Mgmt',
            evidenceRefs: [baseEvidence('ev-fit', 'Portfolio page', 'website', 'Anchor Property Mgmt')],
          },
        ],
        candidateUniverse: [
          {
            candidate_id: 'co-fit',
            evidence: [baseEvidence('ev-uni', 'CRM note', 'existing_repository', 'Anchor Property Mgmt')],
          },
        ],
        qualifiedCount: 0,
      },
    };

    const exportable = collectExportableEvidenceIdentities(scoutResult);
    const canonical = collectCanonicalScoutEvidence(scoutResult);
    assert.equal(exportable.length, canonical.length);
    assert.doesNotThrow(() => assertScoutEvidenceCoverage(scoutResult, canonical));
    assert.ok(SCOUT_EVIDENCE_SOURCES.some((source) => source.id === 'payload.candidateUniverse.evidence'));
  });

  it('Scenario 8 — end-to-end Anchor mission advances to Prioritization Review', async () => {
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
          candidateUniverse: [
            {
              candidate_id: 'co-anchor',
              name: 'Anchor Property Mgmt',
              evidence: [
                baseEvidence('ev-anchor-crm', 'Existing CRM record', 'existing_repository', 'Anchor Property Mgmt'),
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
    assert.ok(discoveryResult.discovery.payload.evidence.length >= 1);
    assert.doesNotThrow(() => assertEvidenceAttached(discoveryResult.discovery.payload));
    assert.equal(
      discoveryResult.snapshot.mission.pendingOperatorDecision.kind,
      'prioritization_approval'
    );
  });

  it('coverage violation throws SCOUT_EVIDENCE_COVERAGE_VIOLATION', () => {
    const scoutResult = {
      status: 'completed',
      payload: {
        candidateUniverse: [
          {
            candidate_id: 'co-1',
            evidence: [baseEvidence('ev-orphan', 'Orphan evidence', 'website', 'Test Co')],
          },
        ],
      },
    };

    assert.throws(
      () => assertScoutEvidenceCoverage(scoutResult, []),
      (err) => err.code === SCOUT_EVIDENCE_COVERAGE_VIOLATION
    );
  });

  it('normalizeScoutDiscoveryPayload preserves canonical evidence from all sources', () => {
    const scoutResult = {
      status: 'completed',
      payload: {
        opportunities: [],
        evaluatedCandidates: [
          {
            companyId: 'co-eval',
            evidence: [baseEvidence('ev-eval-2', 'Evaluated signal', 'job_board', 'Test Co')],
          },
        ],
        qualifiedCount: 0,
      },
    };

    const payload = normalizeScoutDiscoveryPayload(scoutResult);
    assert.equal(payload.evidence.length, 1);
    assert.match(payload.evidence[0].label, /Evaluated signal/);
  });
});
