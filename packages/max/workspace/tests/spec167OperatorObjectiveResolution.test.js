'use strict';

/**
 * SPEC-167 — Operator Objective Resolution Engine acceptance tests.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const { MESSAGE_TYPES } = require('../MessageType');
const {
  PRIMARY_OBJECTIVES,
  SUPPORTING_OBJECTIVES,
  EXECUTION_MODIFIERS,
  CONVERSATION_MODIFIERS,
  REQUIRED_CAPABILITIES,
} = require('../PrimaryObjective');
const {
  resolveOperatorObjective,
  resolveExecutionContract,
  extractCandidateObjectives,
} = require('../OperatorObjectiveResolutionEngine');
const { resolveRoutingDecision } = require('../ObjectiveRoutingMap');
const { buildExecutionContract } = require('../ExecutionContract');
const {
  inspectExecutionState,
  formatObjectiveResolution,
} = require('../ExecutionInspectionOperator');
const { WORKSPACE_OWNERS } = require('../WorkspaceOwnershipResolver');
const { createWorkspaceEngine } = require('../WorkspaceEngine');
const { createTestAmoRuntime } = require('./amoTestRuntime');

const AUDIT_046_PROMPT = [
  'Create a production acquisition mission.',
  'Execute autonomously.',
  'Explain your reasoning naturally.',
].join('\n');

describe('SPEC-167 — Operator Objective Resolution Engine', () => {
  describe('Scenario 1 — Compound Executive Directive', () => {
    it('resolves MISSION_CREATION with execution and conversation modifiers', () => {
      const resolution = resolveOperatorObjective({ question: AUDIT_046_PROMPT });
      assert.equal(resolution.primaryObjective, PRIMARY_OBJECTIVES.MISSION_CREATION);
      assert.ok(resolution.executionModifiers.includes(EXECUTION_MODIFIERS.AUTONOMOUS));
      assert.ok(
        resolution.conversationModifiers.includes(CONVERSATION_MODIFIERS.SHOW_REASONING) ||
          resolution.conversationModifiers.includes(CONVERSATION_MODIFIERS.NATURAL_REASONING)
      );
      assert.equal(resolution.routingDecision.owner, WORKSPACE_OWNERS.MISSION_CREATION);
      assert.equal(resolution.routingDecision.pipeline, 'MissionRuntime');
    });

    it('builds execution contract from resolution', () => {
      const { executionContract } = resolveExecutionContract({ question: AUDIT_046_PROMPT });
      assert.equal(
        executionContract.objectiveResolution.primaryObjective,
        PRIMARY_OBJECTIVES.MISSION_CREATION
      );
      assert.equal(executionContract.executionPolicy.executionPolicy, 'autonomous');
      assert.ok(executionContract.requiredCapabilities.length >= 3);
    });
  });

  describe('Scenario 2 — Business Question', () => {
    it('resolves BUSINESS_INTELLIGENCE with CONCISE conversation modifier', () => {
      const resolution = resolveOperatorObjective({
        question: 'How is Anchor Cleaning doing?\n\nBe concise.',
      });
      assert.equal(resolution.primaryObjective, PRIMARY_OBJECTIVES.BUSINESS_INTELLIGENCE);
      assert.ok(resolution.conversationModifiers.includes(CONVERSATION_MODIFIERS.CONCISE));
      assert.equal(resolution.routingDecision.reason, 'primary_objective:business_intelligence');
    });
  });

  describe('Scenario 3 — Mission Continuation', () => {
    it('resolves MISSION_EXECUTION with PAUSE_ON_APPROVAL modifier', () => {
      const resolution = resolveOperatorObjective({
        question: 'Continue the mission.\n\nPause only when operator approval is required.',
        hasActiveMission: true,
      });
      assert.equal(resolution.primaryObjective, PRIMARY_OBJECTIVES.MISSION_EXECUTION);
      assert.ok(resolution.executionModifiers.includes(EXECUTION_MODIFIERS.PAUSE_ON_APPROVAL));
    });
  });

  describe('Scenario 4 — Identity + Execution', () => {
    it('promotes mission creation; identity becomes supporting', () => {
      const resolution = resolveOperatorObjective({
        question: 'Who are you?\n\nCreate a mission.',
      });
      assert.equal(resolution.primaryObjective, PRIMARY_OBJECTIVES.MISSION_CREATION);
      assert.ok(resolution.supportingObjectives.includes(SUPPORTING_OBJECTIVES.IDENTITY));
    });
  });

  describe('Scenario 5 — Inspection', () => {
    it('execution inspection returns objective resolution fields', () => {
      const stored = resolveOperatorObjective({ question: AUDIT_046_PROMPT });
      const session = {
        context: {
          lastObjectiveResolution: stored,
        },
      };
      const result = inspectExecutionState({
        question: 'Why did you execute this?',
        session,
      });
      assert.equal(result.reason, 'routing_decision_inspected');
      assert.equal(result.structured.metadata.objectiveResolution.primaryObjective, PRIMARY_OBJECTIVES.MISSION_CREATION);
      assert.ok(result.prose.includes('Primary Objective'));
      assert.ok(result.prose.includes('Routing Decision'));
    });

    it('formatObjectiveResolution includes all fields', () => {
      const resolution = resolveOperatorObjective({ question: AUDIT_046_PROMPT });
      const prose = formatObjectiveResolution(resolution);
      assert.match(prose, /Primary Objective/);
      assert.match(prose, /Execution Modifiers/);
      assert.match(prose, /Conversation Modifiers/);
      assert.match(prose, /Required Capabilities/);
      assert.match(prose, /Routing Decision/);
    });
  });

  describe('Scenario 6 — AUDIT-046 Regression', () => {
    it('never routes to workspace_operation or general_conversation', () => {
      const resolution = resolveOperatorObjective({ question: AUDIT_046_PROMPT });
      assert.notEqual(resolution.primaryObjective, PRIMARY_OBJECTIVES.WORKSPACE_OPERATION);
      assert.notEqual(resolution.primaryObjective, PRIMARY_OBJECTIVES.GENERAL_CONVERSATION);
      assert.equal(resolution.primaryObjective, PRIMARY_OBJECTIVES.MISSION_CREATION);
    });

    it('includes required capabilities for production acquisition mission', () => {
      const resolution = resolveOperatorObjective({ question: AUDIT_046_PROMPT });
      assert.ok(resolution.requiredCapabilities.includes(REQUIRED_CAPABILITIES.SCOUT_INTELLIGENCE));
      assert.ok(
        resolution.requiredCapabilities.includes(REQUIRED_CAPABILITIES.OPPORTUNITY_INTELLIGENCE)
      );
      assert.ok(resolution.requiredCapabilities.includes(REQUIRED_CAPABILITIES.OUTCOME_LEARNING));
    });

    it('routes to Mission Runtime', () => {
      const routing = resolveRoutingDecision(PRIMARY_OBJECTIVES.MISSION_CREATION);
      assert.equal(routing.owner, WORKSPACE_OWNERS.MISSION_CREATION);
      assert.equal(routing.pipeline, 'MissionRuntime');
    });

    it('workspace integration executes via MIEP, not session-only acknowledgement', async () => {
      const amoEngine = amo.createAcquisitionMissionEngine();
      const runtime = createTestAmoRuntime({ engine: amoEngine });
      const engine = createWorkspaceEngine({
        disableLlm: true,
        acquisitionMissionRuntime: runtime,
      });

      const opened = await engine.open({ tenantId: '10' });
      const result = await engine.ask({
        sessionId: opened.sessionId,
        question: AUDIT_046_PROMPT,
      });

      assert.equal(result.objectiveResolution.primaryObjective, PRIMARY_OBJECTIVES.MISSION_CREATION);
      assert.equal(result.messageClassification.type, MESSAGE_TYPES.MISSION_CREATION);
      assert.notEqual(result.resolution?.action, 'session_configured');
      assert.equal(result.metadata.miep, true);
    });
  });

  describe('Semantic resolution', () => {
    const equivalentPhrases = [
      'Create a mission for Anchor Cleaning.',
      'Launch mission for Anchor Cleaning.',
      'Start acquisition for Anchor Cleaning.',
      'Begin campaign for Anchor Cleaning.',
      'Kick off investigation for Anchor Cleaning.',
      'Open new mission for Anchor Cleaning.',
    ];

    for (const phrase of equivalentPhrases) {
      it(`converges "${phrase.slice(0, 30)}..." to MISSION_CREATION`, () => {
        const resolution = resolveOperatorObjective({ question: phrase });
        assert.equal(resolution.primaryObjective, PRIMARY_OBJECTIVES.MISSION_CREATION);
      });
    }
  });

  describe('Candidate extraction', () => {
    it('extracts multiple candidates from compound directive', () => {
      const candidates = extractCandidateObjectives(AUDIT_046_PROMPT);
      const objectives = candidates.map((c) => c.objective);
      assert.ok(objectives.includes(PRIMARY_OBJECTIVES.MISSION_CREATION));
    });

    it('modifier-only messages resolve to WORKSPACE_OPERATION', () => {
      const resolution = resolveOperatorObjective({
        question: 'Execute autonomously.\nExplain your reasoning naturally.',
      });
      assert.equal(resolution.primaryObjective, PRIMARY_OBJECTIVES.WORKSPACE_OPERATION);
    });
  });

  describe('Execution contract shape', () => {
    it('includes all required fields', () => {
      const resolution = resolveOperatorObjective({ question: AUDIT_046_PROMPT });
      const contract = buildExecutionContract(resolution);
      assert.ok(contract.objectiveResolution);
      assert.ok(contract.executionPolicy);
      assert.ok(contract.reasoningPolicy);
      assert.ok(contract.conversationPolicy);
      assert.ok(Array.isArray(contract.requiredCapabilities));
    });
  });
});
