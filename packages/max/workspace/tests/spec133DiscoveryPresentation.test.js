'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const {
  presentationFromDiscoveryPayload,
  formatDiscoveryResultsProse,
  findLatestDiscoveryContribution,
} = require('../../../acquisition-mission/DiscoveryPresentation');
const {
  buildMissionCommunication,
  formatMissionProse,
} = require('../MissionCommunication');
const {
  advanceDiscoveryAfterApproval,
  buildDiscoveryApprovalProse,
} = require('../AmoOperatorApproval');
const { buildExecutionMissionResponse } = require('../AcquisitionMissionExecution');

const DISCOVERY_PAYLOAD = {
  companies: [
    { id: 'co-harbor', name: 'Harbor Law Group' },
    { id: 'co-granite', name: 'Granite Legal Partners' },
  ],
  prospects: [{ id: 'p-1', name: 'Alex Morgan', title: 'Office Manager' }],
  buyingSignals: ['Hiring operations manager'],
  decisionMakers: ['Office Manager'],
  confidence: 0.8,
  evidence: ['Google Places', 'website hire page'],
  qualifiedCount: 2,
  summary: 'Scout discovery completed for the active mission.',
};

describe('SPEC-133 — Discovery Artifact Presentation', () => {
  it('builds presentation directly from discovery payload without discarding fields', () => {
    const presentation = presentationFromDiscoveryPayload(DISCOVERY_PAYLOAD);
    assert.equal(presentation.companies.length, 2);
    assert.equal(presentation.prospects.length, 1);
    assert.equal(presentation.rankedProspects.length, 2);
    assert.equal(presentation.rankedProspects[0].name, 'Harbor Law Group');
    assert.equal(presentation.rankedProspects[1].name, 'Granite Legal Partners');
    assert.deepEqual(presentation.buyingSignals, ['Hiring operations manager']);
    assert.deepEqual(presentation.evidence, ['Google Places', 'website hire page']);
    assert.equal(presentation.confidence, 0.8);
    assert.equal(presentation.qualifiedCount, 2);
    assert.equal(presentation.summary, DISCOVERY_PAYLOAD.summary);
  });

  it('renders ranked prospects, evidence, buying signals, confidence, and summary', () => {
    const prose = formatDiscoveryResultsProse(DISCOVERY_PAYLOAD);
    assert.match(prose, /Scout Discovery/i);
    assert.match(prose, /Found 2 prospects/i);
    assert.match(prose, /Harbor Law Group/);
    assert.match(prose, /Granite Legal Partners/);
    assert.match(prose, /Buying Signals/i);
    assert.match(prose, /Hiring operations manager/);
    assert.match(prose, /Supporting Evidence/i);
    assert.match(prose, /Google Places/);
    assert.match(prose, /Confidence/i);
    assert.match(prose, /0\.80/);
    assert.match(prose, /Discovery Summary/i);
  });

  it('places Scout Discovery before Operator Decision in mission prose', () => {
    const comm = buildMissionCommunication({
      headline: 'Mission Updated',
      mission: 'Commercial Acquisition',
      operatorDecision: 'Approve prioritization?',
      discoveryResults: presentationFromDiscoveryPayload(DISCOVERY_PAYLOAD),
    });
    const prose = formatMissionProse(comm);
    const discoveryIdx = prose.indexOf('Scout Discovery');
    const decisionIdx = prose.indexOf('Operator Decision');
    assert.ok(discoveryIdx >= 0);
    assert.ok(decisionIdx > discoveryIdx);
    assert.match(prose, /Approve prioritization\?/);
    assert.match(prose, /Harbor Law Group/);
  });

  it('buildExecutionMissionResponse renders discovery artifact from executionResult.discovery.payload', () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: 'Acquire commercial cleaning customers in Manchester for law firms.',
      targetSegment: 'Law Firms',
    });

    const executionResult = {
      alreadyExecuted: false,
      discovery: { payload: DISCOVERY_PAYLOAD },
      executionOutcome: 'completed',
      approvalPhase: 'waiting_for_next_decision',
    };
    const snapshot = engine.inspect(mission.id, { tenantId: '10' });
    snapshot.workspace = snapshot.workspace || {};
    snapshot.workspace.scout = { state: 'complete', label: 'Discovery Complete' };

    const response = buildExecutionMissionResponse({
      mission: snapshot.mission,
      snapshot,
      action: 'discovery_approved',
      question: 'Approved. Begin Discovery.',
      executionResult,
    });

    assert.match(response.prose, /Scout Discovery/i);
    assert.match(response.prose, /Harbor Law Group/);
    assert.match(response.prose, /Buying Signals/i);
    assert.match(response.prose, /Supporting Evidence/i);
    assert.match(response.prose, /Approve prioritization\?/);
    assert.ok(response.comm.discoveryResults);
    assert.equal(response.comm.discoveryResults.companies.length, 2);
    assert.equal(response.structured.metadata.missionCommunicationPayload.discoveryResults.companies.length, 2);
  });

  it('buildDiscoveryApprovalProse uses discovery payload without reconstructing', () => {
    const prose = buildDiscoveryApprovalProse({
      alreadyExecuted: false,
      discovery: { payload: DISCOVERY_PAYLOAD },
      executionOutcome: 'completed',
      approvalPhase: 'waiting_for_next_decision',
    });
    assert.match(prose, /Scout Discovery/i);
    assert.match(prose, /Granite Legal Partners/);
    assert.match(prose, /Approve prioritization\?/);
  });

  it('inspect snapshot exposes discoveryArtifact from scout contribution payload', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: 'Acquire commercial cleaning customers in Manchester.',
      targetSegment: 'Law Firms',
    });

    await advanceDiscoveryAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved. Begin Discovery.',
      allowFixtureFallback: true,
    });

    const snapshot = engine.inspect(mission.id, { tenantId: '10' });
    assert.ok(snapshot.discoveryArtifact);
    assert.ok(snapshot.discoveryArtifact.rankedProspects.length >= 1);
    assert.ok(snapshot.discoveryArtifact.buyingSignals.length >= 1);
    assert.ok(snapshot.discoveryArtifact.evidence.length >= 1);

    const contribution = findLatestDiscoveryContribution(snapshot.contributions);
    assert.ok(contribution);
    assert.deepEqual(
      snapshot.discoveryArtifact.companies,
      contribution.payload.companies
    );
  });
});
