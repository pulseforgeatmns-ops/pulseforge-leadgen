'use strict';

const { it } = require('node:test');
const assert = require('node:assert/strict');
const { isNonExecutingOperatingTurn, isMissionExecutionCommand } = require('../ExecutionLanguageDetection');
const { analyzeOperatorIntent } = require('../OperatorIntent');
const { isAdvisoryNextStepQuestion, shouldClaimClientIntelligenceTurn } = require('../ClientIntelligenceContext');
const { createHermeticWorkspaceEngine: createWorkspaceEngine } = require('./workspaceTestHarness');
const { createTestAmoRuntime } = require('./amoTestRuntime');
const amo = require('../../../acquisition-mission');

it('past reports and historical questions do not become mission execution', async () => {
  for (const question of [
    'Quick operating update: Campaign 001 was mailed yesterday. Follow-up should begin tomorrow.',
    'Did follow-up begin?',
    'What outreach has already been sent?',
  ]) {
    assert.equal(isNonExecutingOperatingTurn(question), true, question);
    assert.equal(isMissionExecutionCommand(question), false, question);
    const intent = await analyzeOperatorIntent({ question, session: { id: 'report', context: { tenantId: '10' } }, resolveMission: false });
    assert.equal(intent.executionRequested, false, question);
    assert.equal(intent.mutatesMission, false, question);
  }
});

it('a report cannot suppress a separate explicit execution or creation command', () => {
  for (const question of [
    'Campaign 001 was mailed yesterday. Approved. Begin discovery.',
    'Campaign 001 was mailed yesterday. Please start outreach.',
    'Campaign 001 was mailed yesterday. I want you to send the next batch.',
    'Campaign 001 was mailed yesterday; run the next campaign.',
    'Create a brand-new acquisition mission. Do not resume any existing mission.',
  ]) {
    assert.equal(isNonExecutingOperatingTurn(question), false, question);
  }
  assert.equal(isMissionExecutionCommand('Approved. Begin discovery.'), true);
  assert.equal(isMissionExecutionCommand('Do not resume the old mission.'), false);
});

it('only an advisory plan without execution state can claim an ambiguous next-step question', () => {
  const session = { context: { activeClientReasoning: { planSteps: ['Define criteria', 'Build account set'] } } };
  assert.equal(isAdvisoryNextStepQuestion("What's next after that?", session), true);
  assert.equal(isAdvisoryNextStepQuestion('Show execution state. What is next?', session), false);
  assert.equal(shouldClaimClientIntelligenceTurn('Show execution state. What is next?', session, { approvedBlueprint: true }), false);
  session.context.executionState = { executionId: 'real-execution', status: 'paused' };
  assert.equal(isAdvisoryNextStepQuestion("What's next after that?", session), false);
  assert.equal(shouldClaimClientIntelligenceTurn("What's next after that?", session, { approvedBlueprint: true }), false);
  assert.equal(isAdvisoryNextStepQuestion("What's next?", { context: {} }), false);
});

it('explicit execution inspection still reports missing execution state with an advisory plan', async () => {
  const engine = createWorkspaceEngine({ disableLlm: true, missionsEnabled: false, acquisitionMissionRuntime: createTestAmoRuntime({ engine: amo.createAcquisitionMissionEngine() }) });
  const opened = engine.open({ tenantId: '10' });
  engine._sessions.get(opened.sessionId).context.activeClientReasoning = { planSteps: ['Define criteria', 'Build account set'] };
  const result = await engine.ask({ sessionId: opened.sessionId, question: 'Show execution state. What is next?' });
  assert.equal(result.workspaceOwnership.owner, 'execution_state_manager');
  assert.match(result.prose, /No Execution State is recorded/);
  assert.equal(result.mission, null);
});

it('independent market requests bypass legacy default resume while real continuation still binds', async () => {
  const legacy = { id: 'legacy-active', status: 'review_required', type: 'campaign_creation', objectiveText: 'Build Campaign 001 for Anchor Cleaning.' };
  const input = {
    session: { id: 'legacy-session', context: { tenantId: '10' } },
    resolveMission: false,
    missionsEnabled: true,
    missionEngine: { activeMissionResolver: { resolveActiveMission: async () => legacy } },
  };
  const market = await analyzeOperatorIntent({ ...input, question: 'Monitor Microsoft.' });
  assert.equal(market.missionContinuationRequested, false);
  assert.equal(market.executionRequested, false);
  assert.equal(market.mutatesMission, false);
  for (const question of ['Continue.', 'Resume.', 'Show the progress.']) {
    const continuation = await analyzeOperatorIntent({ ...input, question });
    assert.equal(continuation.missionContinuationRequested, true, question);
    assert.equal(continuation.missionContinuationClassification, 'resume', question);
  }
});

it('identity Why retains its referent but explicit decision questions and approvals still bind', async () => {
  const mission = { id: 'latent-mission', pendingOperatorDecision: { kind: 'plan_approval', prompt: 'Approve mission plan?' } };
  const session = { id: 'identity-session', context: { tenantId: '10', conversationalState: {
    subject: 'identity', owner: 'conversation_identity', activeObject: 'max', depth: 1,
    lastQuestion: 'What is your role?', lastIntent: 'explain', confidence: 0.97,
  } } };
  const before = JSON.stringify(mission);
  const why = await analyzeOperatorIntent({ question: 'Why?', session, mission, resolveMission: false });
  assert.equal(why.subject, 'identity');
  assert.equal(why.conversationLocked, true);
  assert.equal(why.pendingDecisionResolution.outcome, 'unrelated');
  const clarification = await analyzeOperatorIntent({ question: 'What exactly am I approving?', session, mission, resolveMission: false });
  assert.equal(clarification.pendingDecisionResolution.outcome, 'question');
  assert.equal(clarification.executionRequested, false);
  const approval = await analyzeOperatorIntent({ question: 'Approved.', session, mission, resolveMission: false });
  assert.equal(approval.pendingDecisionResolution.resolved, true);
  assert.equal(approval.executionRequested, true);
  assert.equal(JSON.stringify(mission), before);
});

it('Why after an actually surfaced pending clarification remains mission-owned without execution', async () => {
  const amoEngine = amo.createAcquisitionMissionEngine();
  const mission = amoEngine.create({ tenantId: '10', objective: 'Acquire commercial cleaning customers in Manchester NH for law firms.', targetSegment: 'Law Firms' });
  const before = JSON.stringify(amoEngine.get(mission.id, '10'));
  const engine = createWorkspaceEngine({ disableLlm: true, acquisitionMissionRuntime: createTestAmoRuntime({ engine: amoEngine }) });
  const clarification = await engine.ask({ question: 'What exactly am I approving?', context: { tenantId: '10', acquisitionMissionId: mission.id } });
  assert.equal(clarification.workspaceOwnership.owner, 'active_mission');
  assert.equal(clarification.domainDecision.reason, 'pending_decision_turn_ownership');
  assert.equal(clarification.structured.metadata.missionCommunicationPayload.operatorDecision, 'Approve mission plan?');
  const why = await engine.ask({ sessionId: clarification.sessionId, question: 'Why?' });
  assert.equal(why.workspaceOwnership.owner, 'active_mission');
  assert.equal(why.domainDecision.reason, 'pending_decision_turn_ownership');
  assert.equal(why.resolution.action, 'clarify');
  assert.match(why.prose, /Approve mission plan/i);
  assert.equal(JSON.stringify(amoEngine.get(mission.id, '10')), before);
});

it('a new turn without a selected response contract clears the earlier CIE prohibition', async () => {
  const engine = createWorkspaceEngine({ disableLlm: true, missionsEnabled: false, acquisitionMissionRuntime: createTestAmoRuntime({ engine: amo.createAcquisitionMissionEngine() }) });
  const opened = engine.open({ tenantId: '10' });
  const session = engine._sessions.get(opened.sessionId);
  session.context.responseContract = require('../ResponseContract').getResponseContract('unknown_analysis');
  session.context.lastResponseContract = 'unknown_analysis';
  assert.equal(session.context.responseContract.forbidsCieClaim, true);
  await engine.ask({ sessionId: opened.sessionId, question: 'Tell me a joke.' });
  assert.equal(session.context.responseContract, null);
  assert.equal(session.context.lastResponseContract, null);
});
