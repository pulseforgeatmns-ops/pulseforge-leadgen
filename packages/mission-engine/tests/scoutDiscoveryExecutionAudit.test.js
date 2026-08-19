'use strict';

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const {
  createMissionEngine,
  executeCurrentStage,
  EXECUTOR_IDS,
  clearMissionStageAuditLog,
  clearScoutDiscoveryAuditLog,
  listScoutDiscoveryAuditLog,
  DISCOVERY_OUTCOMES,
  DISCOVERY_STRATEGIES,
  buildDiscoveryExecutionReport,
  formatDiscoveryOperatorResponse,
} = require('..');
const { createBuiltinRegistry } = require('../../capabilities');
const { createWorkspaceEngine } = require('../../max/workspace');

function testEngine() {
  return createMissionEngine({
    registry: createBuiltinRegistry({ discovery: { useFixture: true } }),
    resolverEnabled: true,
  });
}

describe('AUDIT-006 scout discovery instrumentation', () => {
  beforeEach(() => {
    clearMissionStageAuditLog();
    clearScoutDiscoveryAuditLog();
  });

  it('emits SCOUT_DISCOVERY_STRATEGY through MISSION_DISCOVERY_RESPONSE on execute', async () => {
    const engine = testEngine();
    const mission = await engine.createFromObjective({
      objective: 'Build Campaign 001 for Anchor Cleaning.',
      tenantId: '10',
      clientId: 10,
      execute: false,
    });

    const execution = await executeCurrentStage({
      mission,
      missionEngine: engine,
      operatorId: 'operator-1',
      message: 'Approved. Begin Scout discovery.',
    });

    assert.equal(execution.executorId, EXECUTOR_IDS.SCOUT_DISCOVERY);
    assert.ok(execution.result.discoveryReport);

    const logs = listScoutDiscoveryAuditLog();
    const events = logs.map((l) => l.event);
    assert.ok(events.includes('SCOUT_DISCOVERY_STRATEGY'));
    assert.ok(events.includes('SCOUT_DISCOVERY_OUTCOME'));
    assert.ok(events.includes('MISSION_DISCOVERY_UPDATE'));
    assert.ok(events.includes('MISSION_DISCOVERY_RESPONSE'));
    assert.ok(events.some((e) => e === 'SCOUT_EVIDENCE_SOURCE'));

    const strategy = logs.find((l) => l.event === 'SCOUT_DISCOVERY_STRATEGY');
    assert.equal(strategy.discoveryStrategy, DISCOVERY_STRATEGIES.EXTERNAL_DISCOVERY);
    assert.equal(strategy.missionId, mission.id);

    const outcome = logs.find((l) => l.event === 'SCOUT_DISCOVERY_OUTCOME');
    assert.ok(
      [
        DISCOVERY_OUTCOMES.COMPLETED,
        DISCOVERY_OUTCOMES.PARTIAL,
        DISCOVERY_OUTCOMES.BLOCKED,
      ].includes(outcome.outcome)
    );
    assert.equal(outcome.externalDiscoveryAttempted, true);
    assert.equal(outcome.storedIntelligenceOnly, false);
  });

  it('records evidence sources with Market Intelligence skipped', async () => {
    const engine = testEngine();
    const mission = await engine.createFromObjective({
      objective: 'Build Campaign 001 for Anchor Cleaning.',
      tenantId: '10',
      clientId: 10,
      execute: false,
    });

    await executeCurrentStage({
      mission,
      missionEngine: engine,
      message: 'Approved. Begin Scout discovery.',
    });

    const sourceLogs = listScoutDiscoveryAuditLog().filter(
      (l) => l.event === 'SCOUT_EVIDENCE_SOURCE'
    );
    assert.ok(sourceLogs.length >= 4);

    const mi = sourceLogs.find((l) => l.source === 'Market Intelligence Store');
    assert.ok(mi);
    assert.equal(mi.skipped, true);
    assert.equal(mi.attempted, false);

    const external = sourceLogs.find((l) => l.source === 'External Search');
    assert.ok(external);
    assert.equal(external.attempted, true);
  });

  it('operator response uses mission execution outcome prose, not advisory gap', async () => {
    const missionEngine = testEngine();
    const workspace = createWorkspaceEngine({
      missionEngine,
      missionsEnabled: true,
      resolverEnabled: true,
      disableLlm: true,
    });

    const first = await workspace.ask({
      question: 'Build Campaign 001 for Anchor Cleaning.',
      context: { tenantId: '10', page: 'command-deck' },
    });

    await missionEngine.store.update({
      id: first.mission.id,
      status: 'planning',
    });

    clearScoutDiscoveryAuditLog();

    const second = await workspace.ask({
      sessionId: first.sessionId,
      question: 'Approved. Begin Scout discovery.',
    });

    const answer = second.prose || second.structured.answer;
    assert.match(answer, /Mission Updated/i);
    assert.match(answer, /Stage:\s*Discovery/i);
    assert.match(answer, /Outcome:/i);
    assert.doesNotMatch(
      answer,
      /I do not yet have enough live market or prospect evidence/i
    );

    const responseLog = listScoutDiscoveryAuditLog().find(
      (l) => l.event === 'MISSION_DISCOVERY_RESPONSE'
    );
    assert.ok(responseLog);
    assert.equal(responseLog.operatorResponseKind, 'mission_execution_outcome');
  });
});

describe('AUDIT-006 discovery execution report', () => {
  it('maps blocked zero-prospect missions to DISCOVERY_BLOCKED with reason', () => {
    const report = buildDiscoveryExecutionReport({
      id: 'msn_blocked',
      objectiveText: 'STR operators Manchester',
      status: 'waiting',
      blockingIssues: [
        'Discovery returned zero verified companies. Campaign generation cannot continue.',
      ],
      progress: {
        stageOutcome: 'blocked',
        stageOutcomeLabel: 'Blocked',
      },
      stageReview: {
        capabilityId: 'prospect_discovery',
        outcome: 'blocked',
        blockingIssues: [
          'Discovery returned zero verified companies. Campaign generation cannot continue.',
        ],
      },
      deliverables: {
        lastGate: {
          capabilityId: 'prospect_discovery',
          outcome: 'blocked',
          blockingIssues: [
            'Discovery returned zero verified companies. Campaign generation cannot continue.',
          ],
        },
      },
    });

    assert.equal(report.outcome, DISCOVERY_OUTCOMES.BLOCKED);
    assert.ok(report.blockReason);
    assert.match(report.blockReason, /zero verified companies/i);

    const prose = formatDiscoveryOperatorResponse(report);
    assert.match(prose, /Outcome: BLOCKED/i);
    assert.match(prose, /Reason:/i);
    assert.match(prose, /Next Recommendation:/i);
  });
});
