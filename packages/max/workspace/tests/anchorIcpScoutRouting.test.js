'use strict';

/**
 * Anchor Cleaning — ICP decision vs Scout discovery vs ProspectList detection.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const { createMissionEngine } = require('../../../mission-engine');
const { createBuiltinRegistry } = require('../../../capabilities');
const { detectOperatorProspectListInMessage } = require('../../../mission-engine/OperatorArtifactInjection');
const { isMissionExecutionCommand } = require('../ExecutionLanguageDetection');
const { createWorkspaceEngine } = require('../WorkspaceEngine');
const { createTestAmoRuntime } = require('./amoTestRuntime');
const {
  fixtureScoutDiscoveryResult,
} = require('../AmoOperatorApproval');
const {
  shouldHandleAnchorIcpScoutCombinedTurn,
  parseAnchorIcpDecision,
} = require('../AnchorIcpScoutRouting');

const ACCEPTANCE_MESSAGE = [
  'Scout: update Anchor Cleaning prospecting criteria',
  '',
  "Decision: refining Anchor's ICP away from small professional offices",
  'First controlled batch: 25 daycares, 25 industrial, 15 schools, 15 property managers, 10 larger offices',
  'Outreach remains disabled',
].join('\n');

describe('Anchor ICP + Scout combined routing', () => {
  it('does not treat category batch composition as ProspectList detection', () => {
    const detected = detectOperatorProspectListInMessage(ACCEPTANCE_MESSAGE);
    assert.equal(detected.detected, false);
    assert.equal(detected.promptImport, false);
    assert.equal(detected.prospectCount, 0);
    assert.equal(detected.rejectedAsProspectingCriteria, true);
  });

  it('does not treat outreach-disabled constraint as mission execution command', () => {
    assert.equal(isMissionExecutionCommand(ACCEPTANCE_MESSAGE), false);
  });

  it('classifies the acceptance message as combined ICP + Scout turn', () => {
    assert.equal(
      shouldHandleAnchorIcpScoutCombinedTurn({
        question: ACCEPTANCE_MESSAGE,
        context: { tenantId: '10' },
      }),
      true
    );
    const decision = parseAnchorIcpDecision(ACCEPTANCE_MESSAGE);
    assert.match(decision.summary || '', /small professional offices/i);
    assert.equal(decision.outreachDisabled, true);
    assert.equal(decision.scoutExecutionRequested, true);
    assert.equal(decision.batchComposition.length, 5);
  });

  it('acceptance turn records ICP and starts Scout without Prospect List Detected', async () => {
    const amoEngine = amo.createAcquisitionMissionEngine();
    const mission = amoEngine.create({
      tenantId: '10',
      objective: 'Acquire recurring commercial cleaning customers in Manchester NH.',
      targetSegment: 'Facilities',
    });

    const workspace = createWorkspaceEngine({
      disableLlm: true,
      acquisitionMissionRuntime: createTestAmoRuntime({ engine: amoEngine }),
      missionsEnabled: true,
      scoutAcquisitionOpts: {
        allowFixtureFallback: true,
        fixtureScoutDiscoveryResult,
      },
    });

    const opened = await workspace.open({ tenantId: '10' });
    const result = await workspace.ask({
      sessionId: opened.sessionId,
      question: ACCEPTANCE_MESSAGE,
      context: { tenantId: '10', missionId: mission.id },
    });

    const answer = result.prose || result.structured.answer || '';
    assert.match(answer, /Decision recorded/i);
    assert.match(answer, /Scout discovery started/i);
    assert.match(answer, /Outreach remains disabled/i);
    assert.match(answer, /controlled prospect batch/i);
    assert.doesNotMatch(answer, /Prospect List Detected/i);
    assert.doesNotMatch(answer, /2 Companies/i);

    const snapshot = amoEngine.inspect(mission.id, { tenantId: '10' });
    const icpContribution = (snapshot.contributions || []).find(
      (row) =>
        row.specialist === 'max' &&
        row.kind === 'constraints' &&
        row.payload &&
        row.payload.anchorProspectingCriteria
    );
    assert.ok(icpContribution, 'expected ICP decision contribution on mission');
  });

  it('legacy mission create does not prompt-import category batch lines', async () => {
    const missionEngine = createMissionEngine({
      registry: createBuiltinRegistry({ discovery: { useFixture: true } }),
    });
    const mission = await missionEngine.createFromObjective({
      objective: ACCEPTANCE_MESSAGE,
      tenantId: '10',
      clientId: 10,
      execute: false,
    });
    assert.notEqual(mission.operatorProspectList?.detected, true);
    assert.notEqual(mission.operatorProspectList?.promptImport, true);
    assert.equal(
      mission.deliverables && mission.deliverables.pendingOperatorImport,
      undefined
    );
  });
});
