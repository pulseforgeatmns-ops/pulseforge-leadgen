'use strict';

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const amo = require('../../../acquisition-mission');
const { createTestAmoRuntime } = require('./amoTestRuntime');
const { maybeHandleAcquisitionMissionExecution } = require('../AcquisitionMissionExecution');
const { advancePlanAfterApproval } = require('../AmoOperatorApproval');
const {
  EXECUTION_INTENTS,
  EXECUTION_SOURCES,
  createExecutionRequestFromChat,
  createExecutionRequestFromApprovalButton,
  canonicalRequestShape,
  clearExecutionRouterAudit,
} = amo;

const ANCHOR_OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester NH for law firms.';

describe('SPEC-171 — execution surfaces produce Canonical Execution Requests', () => {
  let engine;
  let mission;

  beforeEach(() => {
    clearExecutionRouterAudit();
    engine = amo.createAcquisitionMissionEngine();
    mission = engine.create({
      tenantId: '10',
      objective: ANCHOR_OBJECTIVE,
      targetSegment: 'Law Firms',
    });
  });

  it('chat approval submits a CER and returns it on the turn', async () => {
    await advancePlanAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved.',
      operatorId: 'jacob',
    });

    const turn = await maybeHandleAcquisitionMissionExecution({
      question: 'Approved. Begin Discovery.',
      operatorId: 'jacob',
      context: { tenantId: '10', missionId: mission.id },
      acquisitionMissionRuntime: createTestAmoRuntime({ engine }),
      allowFixtureFallback: true,
    });

    assert.ok(turn);
    assert.ok(turn.executionRequest);
    assert.equal(turn.executionRequest.spec, 'SPEC-171');
    assert.equal(turn.executionRequest.source, EXECUTION_SOURCES.CHAT);
    assert.equal(turn.executionRequest.intent, EXECUTION_INTENTS.APPROVE_DISCOVERY);
    assert.match(turn.executionRequest.id, /^cer_/);
    assert.equal(turn.executionResult.executionOutcome, 'completed');
  });

  it('chat and button CERs match on intent for the same mission', () => {
    const chat = createExecutionRequestFromChat({
      action: 'discovery_approved',
      missionId: mission.id,
      operatorId: 'jacob',
      objective: mission.objective,
      question: 'approve',
    });
    const button = createExecutionRequestFromApprovalButton({
      intent: EXECUTION_INTENTS.APPROVE_DISCOVERY,
      missionId: mission.id,
      operatorId: 'jacob',
      objective: mission.objective,
    });
    assert.deepEqual(canonicalRequestShape(chat).intent, canonicalRequestShape(button).intent);
    assert.equal(chat.source, EXECUTION_SOURCES.CHAT);
    assert.equal(button.source, EXECUTION_SOURCES.APPROVAL_BUTTON);
  });

  it('API executeCanonical submits a CER for a pending discovery approval', async () => {
    const { executeCanonical } = require('../../../../services/acquisitionMission');
    const { createTestAmoRuntime } = require('./amoTestRuntime');
    await advancePlanAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved.',
      operatorId: 'jacob',
    });
    const runtime = createTestAmoRuntime({ engine, persist: false });
    const routed = await executeCanonical({
      tenantId: '10',
      missionId: mission.id,
      intent: EXECUTION_INTENTS.APPROVE_DISCOVERY,
      source: EXECUTION_SOURCES.API,
      operatorId: 'jacob',
      question: 'Approved. Begin Discovery.',
      allowFixtureFallback: true,
    }, { runtime, persist: false });
    assert.equal(routed.request.source, EXECUTION_SOURCES.API);
    assert.equal(routed.request.intent, EXECUTION_INTENTS.APPROVE_DISCOVERY);
    assert.equal(routed.specialist, 'scout');
    assert.equal(routed.executionResult.executionOutcome, 'completed');
  });

  it('workspace approval buttons submit CER intents to the execute API', () => {
    const html = fs.readFileSync(
      path.join(__dirname, '../../../../public/acquisition-missions.html'),
      'utf8'
    );
    assert.match(html, /\/api\/v1\/amo\/missions\//);
    assert.match(html, /\/execute/);
    assert.match(html, /data-intent/);
    assert.match(html, /APPROVE_DISCOVERY/);
    assert.match(html, /approval_button/);
    assert.doesNotMatch(html, /approveDiscovery\(/);
  });
});
