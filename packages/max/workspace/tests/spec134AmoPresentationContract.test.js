'use strict';
const { createTestAmoRuntime, runtimeProviderFromEngine, createHydratingTestRuntime } = require('./amoTestRuntime');

/**
 * SPEC-134 — Preserve AMO Presentation Contract.
 * Ownership may append metadata. It must not replace AMO presentation.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const {
  buildOwnershipMissionResponse,
  maybeHandleAcquisitionOwnershipTurn,
  AMO_SOURCES,
} = require('../AcquisitionOwnership');
const {
  createAcquisitionOwnershipAudit,
  clearAcquisitionOwnershipAuditLog,
} = require('../audit/AcquisitionOwnershipAudit');

const ANCHOR_OBJECTIVE =
  'I want to acquire one recurring commercial cleaning client from a short-term rental operator in the Greater Manchester area.';

const DISCOVERY_PAYLOAD = {
  companies: [
    { id: 'co-harbor', name: 'Harbor Law Group' },
    { id: 'co-granite', name: 'Granite Legal Partners' },
  ],
  buyingSignals: ['Hiring operations manager'],
  decisionMakers: ['Office Manager'],
  confidence: 0.8,
  evidence: [{ label: 'Places hit', source: 'Google Places' }],
  qualifiedCount: 2,
  summary: 'Scout discovery completed for the active mission.',
};

const CI_EVIDENCE = {
  attached: true,
  blueprintId: 'bp-10',
  sectionsAttached: ['idealCustomers', 'geography'],
  known: ['ICP: law firms and accounting practices', 'Geography: Greater Manchester'],
  strategicEvidence: {
    icp: 'law firms and accounting practices',
    geography: 'Greater Manchester',
    unknowns: [],
  },
  constraints: [],
};

function assertAmoPresentation(comm, prose, structured) {
  assert.deepEqual(comm.sources, AMO_SOURCES.slice());
  assert.ok(comm.sources.includes('acquisition_mission'));
  assert.ok(comm.sources.includes('scout'));
  assert.equal(comm.sources.includes('Mission Engine'), false);
  assert.equal(comm.sources.includes('Client Intelligence'), false);
  assert.doesNotMatch(prose, /Mission Engine/);
  assert.doesNotMatch(prose, /Client Intelligence/);
  assert.doesNotMatch(prose, /Blueprint attached/);
  assert.doesNotMatch(prose, /Continue in mission workspace\?/);
  assert.match(prose, /acquisition_mission/);
  assert.match(prose, /scout/);
  assert.equal(structured.metadata.presentationContract, 'amo');
  assert.equal(structured.metadata.missionRuntime, 'AMO');
  assert.equal(structured.metadata.acquisitionOwnership, true);
}

describe('SPEC-134 — Preserve AMO Presentation Contract', { concurrency: false }, () => {
  beforeEach(() => {
    clearAcquisitionOwnershipAuditLog();
  });

  it('create path keeps AMO sources and AMO-stage operator decision', async () => {
    const amoEngine = amo.createAcquisitionMissionEngine();
    const turn = await maybeHandleAcquisitionOwnershipTurn({
      question: ANCHOR_OBJECTIVE,
      context: { tenantId: '10' },
      acquisitionMissionRuntime: createTestAmoRuntime({ engine: amoEngine }),
      persist: false,
      audit: createAcquisitionOwnershipAudit(),
      cieService: {
        getApprovedClientBlueprint: async () => ({
          id: 'bp-10',
          status: 'approved',
          normalizedFacts: {
            business_name: 'Anchor Cleaning',
            ideal_customers: ['law firms'],
            geography: ['Greater Manchester'],
            growth_focus: 'recurring commercial cleaning clients',
            commercial_preference: true,
          },
        }),
      },
    });

    assert.ok(turn);
    assert.equal(turn.created, true);
    const comm = turn.structured.metadata.missionCommunicationPayload;
    assertAmoPresentation(comm, turn.prose, turn.structured);
    assert.equal(comm.evidenceStatus, 'Mission state');
    assert.ok(comm.operatorDecision);
    assert.notEqual(comm.operatorDecision, 'Continue in mission workspace?');
    assert.match(turn.prose, /Operator Decision/);
    const blueprintEvidence = turn.structured.supportingEvidence.filter(
      (row) => row.kind === 'blueprint'
    );
    assert.ok(blueprintEvidence.length >= 1);
  });

  it('resume with Scout discovery presents the artifact and AMO evidence', () => {
    const amoEngine = amo.createAcquisitionMissionEngine();
    const mission = amoEngine.create({
      tenantId: '10',
      objective: ANCHOR_OBJECTIVE,
      targetSegment: 'Short-term rental operators',
      planApproved: true,
    });
    amoEngine.contribute(mission.id, {
      specialist: 'scout',
      kind: 'discovery',
      payload: DISCOVERY_PAYLOAD,
    });

    const snapshot = amoEngine.inspect(mission.id, { tenantId: '10' });
    snapshot.mission = {
      ...snapshot.mission,
      pendingOperatorDecision: null,
    };

    const response = buildOwnershipMissionResponse({
      mission: snapshot.mission,
      snapshot,
      created: false,
      ciEvidence: CI_EVIDENCE,
      question: ANCHOR_OBJECTIVE,
    });

    assertAmoPresentation(response.comm, response.prose, response.structured);
    assert.ok(response.comm.discoveryResults);
    assert.equal(response.comm.discoveryResults.companies.length, 2);
    assert.match(response.prose, /Scout Discovery/i);
    assert.match(response.prose, /Harbor Law Group/);
    assert.match(response.prose, /Granite Legal Partners/);
    assert.equal(response.comm.evidenceStatus, DISCOVERY_PAYLOAD.summary);
    assert.equal(response.comm.operatorDecision, 'Approve prioritization?');
    assert.ok(
      response.structured.supportingEvidence.some((row) => row.kind === 'blueprint')
    );
    assert.equal(
      response.structured.metadata.missionCommunicationPayload.evidenceStatus,
      DISCOVERY_PAYLOAD.summary
    );
  });

  it('does not let Blueprint or Mission Engine fields overwrite AMO communication', () => {
    const amoEngine = amo.createAcquisitionMissionEngine();
    const mission = amoEngine.create({
      tenantId: '10',
      objective: 'Acquire one recurring commercial cleaning client.',
      targetSegment: 'Commercial',
    });
    const snapshot = amoEngine.inspect(mission.id, { tenantId: '10' });
    const response = buildOwnershipMissionResponse({
      mission: snapshot.mission,
      snapshot,
      created: false,
      ciEvidence: CI_EVIDENCE,
      question: 'Acquire one recurring commercial cleaning client',
    });

    assertAmoPresentation(response.comm, response.prose, response.structured);
    assert.equal(response.comm.headline, 'Mission Resumed');
    const pending = snapshot.mission.pendingOperatorDecision;
    assert.equal(
      response.comm.operatorDecision,
      pending.clarificationPrompt || pending.prompt
    );
    assert.notEqual(response.comm.evidenceStatus, '✓ Blueprint attached');
    assert.notEqual(response.comm.evidenceStatus, 'Mission context only');
  });
});
