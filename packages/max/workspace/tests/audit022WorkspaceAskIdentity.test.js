'use strict';

/**
 * AUDIT-022 — Workspace Request / Response Identity
 * Verifies the discovery-approval "approved" turn preserves payload identity
 * from WorkspaceEngine.ask() through simulated JSON and prose extraction.
 * Not an actual HTTP/browser integration test or full-payload comparison.
 */

const { describe, it, beforeEach, after } = require('node:test');
const assert = require('node:assert/strict');

const amo = require('../../../acquisition-mission');
const { STAGES } = amo;
const { createHermeticWorkspaceEngine: createWorkspaceEngine } = require('./workspaceTestHarness');
const { createTestAmoRuntime } = require('./amoTestRuntime');
const approval = require('../AmoOperatorApproval');
const realAdvance = approval.advanceDiscoveryAfterApproval;
let fixtureCalls = 0;
approval.advanceDiscoveryAfterApproval = input => realAdvance({ ...input, runScout: async () => { fixtureCalls += 1; return approval.fixtureScoutDiscoveryResult(); } });
after(() => { approval.advanceDiscoveryAfterApproval = realAdvance; });
const { advancePlanAfterApproval } = require('../AmoOperatorApproval');
const {
  identitySnapshot,
  assertIdentityEqual,
  proseHeadline,
  responseHash,
} = require('../audit/WorkspaceAskIdentityAudit');

const OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester NH for law firms.';

function simulateAskWorkspaceWrapper(engineResult, { awarenessHeadline = null } = {}) {
  const result = { ...engineResult };
  if (awarenessHeadline && result.prose) {
    result.prose = `${awarenessHeadline}\n\n${result.prose}`;
  }
  result.awareness = { headline: awarenessHeadline || null };
  result.suggestions = ['suggestion-a'];
  return result;
}

function simulateHttpJson(payload) {
  return JSON.parse(JSON.stringify(payload));
}

function simulateDomHeadline(payload) {
  return proseHeadline(payload.prose);
}

describe('AUDIT-022 workspace ask request/response identity', { concurrency: false }, () => {
  let amoEngine;
  let workspace;
  let mission;
  let sessionId;

  beforeEach(async () => {
    fixtureCalls = 0;
    amoEngine = amo.createAcquisitionMissionEngine();
    mission = amoEngine.create({
      tenantId: '10',
      objective: OBJECTIVE,
      targetSegment: 'Law Firms',
    });

    await advancePlanAfterApproval({
      engine: amoEngine,
      mission,
      tenantId: '10',
      question: 'Approved.',
    });

    const snapshot = amoEngine.inspect(mission.id, { tenantId: '10' });
    assert.equal(snapshot.mission.stage, STAGES.DISCOVER);
    assert.equal(snapshot.mission.structuredMissionApproved, true);
    assert.equal(snapshot.mission.pendingOperatorDecision.kind, 'discovery_approval');

    workspace = createWorkspaceEngine({
      acquisitionMissionRuntime: createTestAmoRuntime({ engine: amoEngine }),
      missionsEnabled: true,
      resolverEnabled: true,
      disableLlm: true,
    });

    const opened = workspace.open({ tenantId: '10', page: 'command-deck' });
    sessionId = opened.sessionId;
    const session = workspace._sessions.get(sessionId);
    session.context.missionId = mission.id;
    session.context.acquisitionMissionId = mission.id;
  });

  it('preserves discovery execution identity from WorkspaceEngine through DOM prose', async () => {
    const requestId = 'audit022-req-discovery';
    const partC = await workspace.ask({
      sessionId,
      question: 'approved',
    });

    assert.ok(fixtureCalls > 0, 'Explicit Scout fixture was called');
    assert.equal(partC.resolution.action, 'discovery_approved');
    assert.equal(partC.resolution.reason, 'acquisition_mission_discovery_approved');
    assert.equal(
      partC.structured.metadata.missionCommunicationPayload.headline,
      'Mission Updated'
    );

    const snapshotC = identitySnapshot(partC, {
      requestId,
      sessionId,
      missionId: mission.id,
      stage: STAGES.DISCOVER,
      executionAction: 'discovery_approved',
    });
    assert.equal(snapshotC.headline, 'Mission Updated');
    assert.equal(snapshotC.proseHeadline, 'Mission Updated');

    const partD = simulateAskWorkspaceWrapper(partC, {
      awarenessHeadline: 'Since you opened the workspace',
    });
    const snapshotD = identitySnapshot(partD, snapshotC);
    assert.notEqual(partD.prose, partC.prose);
    assert.equal(snapshotD.headline, 'Mission Updated');
    assert.equal(snapshotD.proseHeadline, 'Since you opened the workspace');

    const partE = simulateHttpJson(partD);
    const snapshotE = identitySnapshot(partE, snapshotC);
    assertIdentityEqual(snapshotD, snapshotE, 'Part D → Part E (HTTP JSON)');

    const domHeadline = simulateDomHeadline(partE);
    assert.equal(domHeadline, 'Since you opened the workspace');
    assert.notEqual(domHeadline, 'Mission Plan Approved');
  });

  it('plan approval and discovery approval produce distinct identity fingerprints', async () => {
    const freshEngine = amo.createAcquisitionMissionEngine();
    const freshMission = freshEngine.create({
      tenantId: '10',
      objective: OBJECTIVE,
      targetSegment: 'Law Firms',
    });

    const freshWorkspace = createWorkspaceEngine({
      acquisitionMissionRuntime: createTestAmoRuntime({ engine: freshEngine }),
      missionsEnabled: true,
      resolverEnabled: true,
      disableLlm: true,
    });
    const opened = freshWorkspace.open({ tenantId: '10', page: 'command-deck' });
    const freshSessionId = opened.sessionId;
    const session = freshWorkspace._sessions.get(freshSessionId);
    session.context.missionId = freshMission.id;
    session.context.acquisitionMissionId = freshMission.id;

    const planTurn = await freshWorkspace.ask({
      sessionId: freshSessionId,
      question: 'approved',
    });

    assert.equal(
      planTurn.structured.metadata.missionCommunicationPayload.headline,
      'Mission Plan Approved'
    );
    assert.match(planTurn.prose, /Mission Plan Approved/);
    assert.match(planTurn.prose, /Approve discovery\?/);

    const discoveryTurn = await workspace.ask({
      sessionId,
      question: 'approved',
    });

    assert.equal(
      discoveryTurn.structured.metadata.missionCommunicationPayload.headline,
      'Mission Updated'
    );

    assert.notEqual(responseHash(planTurn), responseHash(discoveryTurn));
  });

  it('appendMaxResponse uses incoming prose — no workspace state refetch in command-deck', () => {
    const deckSrc = require('fs').readFileSync(
      require('path').join(__dirname, '../../../../public/command-deck/command-deck.js'),
      'utf8'
    );
    const start = deckSrc.indexOf('async function appendMaxResponse');
    const end = deckSrc.indexOf('async function askWorkspace', start + 1);
    assert.ok(start >= 0, 'appendMaxResponse boundary exists');
    assert.ok(end > start, 'askWorkspace boundary follows appendMaxResponse');
    const fn = deckSrc.slice(start, end);
    assert.ok(fn.trim().length > 0, 'renderer source slice is nonempty');
    assert.doesNotMatch(fn, /apiRequest\(/);
    assert.doesNotMatch(fn, /fetch\(/);
    assert.doesNotMatch(fn, /loadDeck\(/);
    assert.doesNotMatch(fn, /openMissionWorkspace\(/);
    assert.match(fn, /result\.prose/);
  });
});


describe('AUDIT-022 selected-field fingerprint scope', () => {
  it('ignores body-only prose changes with the same headline', () => {
    const original = { sessionId: 'fixture-session', prose: 'Mission Updated\nOriginal body' };
    const edited = { ...original, prose: 'Mission Updated\nDifferent body' };
    assert.equal(responseHash(original), responseHash(edited));
  });

  it('detects a changed selected field and reports the fingerprint mismatch', () => {
    const original = { sessionId: 'fixture-session', prose: 'Mission Updated\nBody' };
    const edited = { ...original, sessionId: 'other-session' };
    assert.notEqual(responseHash(original), responseHash(edited));
    assert.throws(
      () => assertIdentityEqual(identitySnapshot(original), identitySnapshot(edited), 'selected field'),
      error => error.code === 'AUDIT_022_IDENTITY_MISMATCH'
        && error.mismatches.some(row => row.field === 'sessionId')
        && error.mismatches.some(row => row.field === 'responseHash')
    );
  });
});
