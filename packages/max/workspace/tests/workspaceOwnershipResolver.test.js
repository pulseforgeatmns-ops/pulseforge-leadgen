'use strict';
const { createTestAmoRuntime, runtimeProviderFromEngine, createHydratingTestRuntime } = require('./amoTestRuntime');

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const {
  resolveWorkspaceOwner,
  WORKSPACE_OWNERS,
  claimsBlueprintOwnership,
  claimsSpecialistOwnership,
} = require('../WorkspaceOwnershipResolver');
const {
  createWorkspaceOwnershipAudit,
  clearWorkspaceOwnershipAuditLog,
  listWorkspaceOwnershipAuditLog,
} = require('../audit/WorkspaceOwnershipAudit');
const { createWorkspaceEngine } = require('../WorkspaceEngine');

const ANCHOR_OBJECTIVE =
  'I want to acquire one recurring commercial cleaning client in Greater Manchester.';

describe('SPEC-125 — Workspace Ownership-First Runtime', () => {
  beforeEach(() => {
    clearWorkspaceOwnershipAuditLog();
  });

  it('selects mission_creation for acquisition objectives before blueprint', async () => {
    const ownership = await resolveWorkspaceOwner({
      question: ANCHOR_OBJECTIVE,
      context: { tenantId: '10' },
    });
    assert.equal(ownership.owner, WORKSPACE_OWNERS.MISSION_CREATION);
    assert.equal(ownership.reason, 'acquisition_objective_precedence');
  });

  it('selects mission_creation for Operate Anchor phrasing', async () => {
    const ownership = await resolveWorkspaceOwner({
      question: 'Operate Anchor Cleaning outreach for Manchester law firms',
      context: { tenantId: '10' },
    });
    assert.equal(ownership.owner, WORKSPACE_OWNERS.MISSION_CREATION);
    assert.match(ownership.reason, /mission/);
  });

  it('selects blueprint for ICP and positioning questions', async () => {
    assert.equal(
      claimsBlueprintOwnership('What is our ICP for commercial cleaning?', {
        context: { tenantId: '10' },
      }),
      true
    );
    const ownership = await resolveWorkspaceOwner({
      question: 'How should we describe our positioning vs competitors?',
      context: { tenantId: '10' },
    });
    assert.equal(ownership.owner, WORKSPACE_OWNERS.BLUEPRINT);
  });

  it('selects mission_inspection before knowledge retrieval', async () => {
    const amoEngine = amo.createAcquisitionMissionEngine();
    amoEngine.create({
      tenantId: '10',
      objective: 'Acquire commercial cleaning customers.',
      targetSegment: 'Law Firms',
    });
    const ownership = await resolveWorkspaceOwner({
      question: 'Why is the progress 40%?',
      context: { tenantId: '10' },
      acquisitionMissionRuntime: createTestAmoRuntime({ engine: amoEngine }),
    });
    assert.equal(ownership.owner, WORKSPACE_OWNERS.MISSION_INSPECTION);
  });

  it('routes find prospects to specialist_scout', async () => {
    const ownership = await resolveWorkspaceOwner({
      question: 'Find commercial cleaning prospects in Manchester',
      context: { tenantId: '10' },
    });
    assert.equal(ownership.owner, WORKSPACE_OWNERS.SPECIALIST_SCOUT);
    assert.equal(ownership.specialist, 'scout');
  });

  it('routes write outreach to specialist_paige', async () => {
    const ownership = await resolveWorkspaceOwner({
      question: 'Write outreach email for law firm campaign',
      context: { tenantId: '10' },
    });
    assert.equal(ownership.owner, WORKSPACE_OWNERS.SPECIALIST_PAIGE);
    assert.equal(ownership.specialist, 'paige');
  });

  it('routes coach call to specialist_cal', async () => {
    const claim = claimsSpecialistOwnership('Help me prepare for a discovery call');
    assert.equal(claim.owner, WORKSPACE_OWNERS.SPECIALIST_CAL);
    assert.equal(claim.specialist, 'cal');
  });

  it('falls back to reasoning when no owner claims', async () => {
    const ownership = await resolveWorkspaceOwner({
      question: 'Hello',
      context: { tenantId: '10' },
    });
    assert.equal(ownership.owner, WORKSPACE_OWNERS.REASONING);
    assert.equal(ownership.fallback, true);
  });

  it('emits WORKSPACE_OWNER_SELECTED audit event', () => {
    const audit = createWorkspaceOwnershipAudit();
    audit.logOwnerSelected({
      owner: WORKSPACE_OWNERS.MISSION_CREATION,
      reason: 'acquisition_objective',
      confidence: 0.96,
      question: ANCHOR_OBJECTIVE,
    });
    const row = audit.log.find((entry) => entry.event === 'WORKSPACE_OWNER_SELECTED');
    assert.ok(row);
    assert.equal(row.owner, WORKSPACE_OWNERS.MISSION_CREATION);
    assert.equal(row.reason, 'acquisition_objective');
    assert.equal(row.confidence, 0.96);
  });

  it('emits WORKSPACE_OWNER_FALLBACK when pipeline misses claimed owner', () => {
    const audit = createWorkspaceOwnershipAudit();
    audit.logOwnerFallback({
      claimedOwner: WORKSPACE_OWNERS.SPECIALIST_SCOUT,
      fallbackOwner: WORKSPACE_OWNERS.REASONING,
      reason: 'specialist_unhandled',
      question: 'Find prospects',
    });
    const row = audit.log.find((entry) => entry.event === 'WORKSPACE_OWNER_FALLBACK');
    assert.ok(row);
    assert.equal(row.claimedOwner, WORKSPACE_OWNERS.SPECIALIST_SCOUT);
    assert.equal(row.fallbackOwner, WORKSPACE_OWNERS.REASONING);
  });

  it('WorkspaceEngine.ask creates mission for Operate Anchor objective', async () => {
    const amoEngine = amo.createAcquisitionMissionEngine();
    const engine = createWorkspaceEngine({
      acquisitionMissionRuntime: createTestAmoRuntime({ engine: amoEngine }),
      missionsEnabled: true,
      missionEngine: {
        activeMissionResolver: {
          resolveActiveMission: async () => null,
          resolve: async () => ({ action: 'intelligence' }),
          clearActiveMission: async () => {},
        },
        toCard: (m) => m,
      },
    });

    const result = await engine.ask({
      question: ANCHOR_OBJECTIVE,
      context: { tenantId: '10', clientId: '10' },
    });

    assert.equal(result.workspaceOwnership.owner, WORKSPACE_OWNERS.MISSION_CREATION);
    assert.match(result.prose, /Mission Created|Mission Resumed/);
    assert.doesNotMatch(result.prose, /advisory guidance/i);
    assert.ok(result.mission);
  });

  it('WorkspaceEngine.ask inspects mission before reasoning for progress questions', async () => {
    const amoEngine = amo.createAcquisitionMissionEngine();
    const mission = amoEngine.create({
      tenantId: '10',
      objective: 'Acquire commercial cleaning customers in Manchester NH.',
      targetSegment: 'Commercial Law Firms',
    });
    amoEngine.contribute(mission.id, {
      specialist: 'scout',
      payload: { companies: [{ id: 1 }], evidence: ['places'], qualifiedCount: 1 },
    });
    amoEngine.progress(mission.id, { role: 'max' }, { stage: amo.STAGES.UNDERSTAND });

    const engine = createWorkspaceEngine({
      acquisitionMissionRuntime: createTestAmoRuntime({ engine: amoEngine }),
      missionsEnabled: true,
      missionEngine: {
        activeMissionResolver: {
          resolveActiveMission: async () => null,
          resolve: async () => ({ action: 'intelligence' }),
          clearActiveMission: async () => {},
        },
        toCard: (m) => m,
      },
    });

    const result = await engine.ask({
      question: 'Why is the progress 40%?',
      context: { tenantId: '10', missionId: mission.id, clientId: '10' },
    });

    assert.equal(result.workspaceOwnership.owner, WORKSPACE_OWNERS.MISSION_INSPECTION);
    assert.equal(result.domainDecision.reason, 'mission_inspection');
    assert.equal(result.structured.metadata.missionInspection, true);
  });

  // SPEC-216 — Resolved pending decision retains active mission ownership
  describe('SPEC-216 — Resolved pending decision ownership', () => {
    it('retains active_mission ownership when pending decision resolves to REQUEST_REVISION', async () => {
      const missionWithPending = {
        id: 'test-mission-1',
        tenantId: '10',
        pendingOperatorDecision: {
          kind: 'execution_approval',
          prompt: 'Should we send the prepared outreach?',
        },
      };

      const operatorIntent = {
        pendingDecisionResolution: {
          pending: true,
          resolved: true,
          resolvedFromPendingDecision: true,
          decisionKind: 'execution_approval',
          action: 'request_revision',
          outcome: 'request_revision',
          confidence: 0.95,
          missionId: missionWithPending.id,
          executionIntent: 'REVISE_PREPARED_OUTREACH',
          executionAction: 'revise_prepared_outreach',
        },
      };

      const ownership = await resolveWorkspaceOwner({
        question: 'Do not authorize this outreach. Regenerate and return for approval.',
        context: { tenantId: '10', missionId: missionWithPending.id, clientId: '10' },
        operatorIntent,
        session: { id: 'test-session-1', context: {} },
        missionEngine: {
          activeMissionResolver: {
            resolveActiveMission: async () => missionWithPending,
          },
        },
        missionsEnabled: true,
        resolverEnabled: true,
      });

      assert.equal(ownership.owner, WORKSPACE_OWNERS.ACTIVE_MISSION);
      assert.equal(ownership.reason, 'pending_decision_turn_ownership');
      assert.notEqual(ownership.reason, 'mission_creation');
      assert.equal(ownership.confidence, 0.98);
    });

    it('approval of pending execution_approval does not fall to mission_creation', async () => {
      const missionWithPending = {
        id: 'test-mission-2',
        tenantId: '10',
        pendingOperatorDecision: {
          kind: 'execution_approval',
          prompt: 'Ready to execute?',
        },
      };

      const operatorIntent = {
        pendingDecisionResolution: {
          pending: true,
          resolved: true,
          resolvedFromPendingDecision: true,
          decisionKind: 'execution_approval',
          action: 'approve_execution',
          outcome: 'affirm',
          confidence: 0.98,
          missionId: missionWithPending.id,
          executionIntent: 'APPROVE_EXECUTION',
          executionAction: 'approve_execution',
        },
      };

      const ownership = await resolveWorkspaceOwner({
        question: 'Yes, authorize it.',
        context: { tenantId: '10', missionId: missionWithPending.id, clientId: '10' },
        operatorIntent,
        session: { id: 'test-session-2', context: {} },
        missionEngine: {
          activeMissionResolver: {
            resolveActiveMission: async () => missionWithPending,
          },
        },
        missionsEnabled: true,
        resolverEnabled: true,
      });

      assert.equal(ownership.owner, WORKSPACE_OWNERS.ACTIVE_MISSION);
      assert.equal(ownership.reason, 'pending_decision_turn_ownership');
      assert.notEqual(ownership.reason, 'mission_creation');
    });

    it('rejection of pending execution_approval does not fall to mission_creation', async () => {
      const missionWithPending = {
        id: 'test-mission-3',
        tenantId: '10',
        pendingOperatorDecision: {
          kind: 'execution_approval',
          prompt: 'Ready to execute?',
        },
      };

      const operatorIntent = {
        pendingDecisionResolution: {
          pending: true,
          resolved: true,
          resolvedFromPendingDecision: true,
          decisionKind: 'execution_approval',
          action: 'cancel',
          outcome: 'reject',
          confidence: 0.98,
          missionId: missionWithPending.id,
          executionIntent: 'CANCEL_PLAN',
          executionAction: 'cancel_plan',
        },
      };

      const ownership = await resolveWorkspaceOwner({
        question: 'No, do not send this.',
        context: { tenantId: '10', missionId: missionWithPending.id, clientId: '10' },
        operatorIntent,
        session: { id: 'test-session-3', context: {} },
        missionEngine: {
          activeMissionResolver: {
            resolveActiveMission: async () => missionWithPending,
          },
        },
        missionsEnabled: true,
        resolverEnabled: true,
      });

      assert.equal(ownership.owner, WORKSPACE_OWNERS.ACTIVE_MISSION);
      assert.equal(ownership.reason, 'pending_decision_turn_ownership');
      assert.notEqual(ownership.reason, 'mission_creation');
    });

    it('ambiguous pending decision does not trigger canonical action ownership', async () => {
      const missionWithPending = {
        id: 'test-mission-4',
        tenantId: '10',
        pendingOperatorDecision: {
          kind: 'execution_approval',
          prompt: 'Ready to execute?',
        },
      };

      const operatorIntent = {
        pendingDecisionResolution: {
          pending: true,
          resolved: false,
          outcome: 'ambiguous',
          decisionKind: 'execution_approval',
          missionId: missionWithPending.id,
        },
      };

      const ownership = await resolveWorkspaceOwner({
        question: 'continuee',
        context: { tenantId: '10', missionId: missionWithPending.id, clientId: '10' },
        operatorIntent,
        session: { id: 'test-session-4', context: {} },
        missionEngine: {
          activeMissionResolver: {
            resolveActiveMission: async () => missionWithPending,
          },
        },
        missionsEnabled: true,
        resolverEnabled: true,
      });

      assert.notEqual(ownership.reason, 'resolved_pending_canonical_action');
    });

    it('resolved pending decision without executionIntent does not claim canonical action', async () => {
      const missionWithPending = {
        id: 'test-mission-5',
        tenantId: '10',
        pendingOperatorDecision: {
          kind: 'discovery_approval',
          prompt: 'Approve discovery?',
        },
      };

      const operatorIntent = {
        pendingDecisionResolution: {
          pending: true,
          resolved: true,
          resolvedFromPendingDecision: true,
          decisionKind: 'discovery_approval',
          action: 'continue_investigation',
          outcome: 'question',
          missionId: missionWithPending.id,
        },
      };

      const ownership = await resolveWorkspaceOwner({
        question: 'What specifically should we investigate?',
        context: { tenantId: '10', missionId: missionWithPending.id, clientId: '10' },
        operatorIntent,
        session: { id: 'test-session-5', context: {} },
        missionEngine: {
          activeMissionResolver: {
            resolveActiveMission: async () => missionWithPending,
          },
        },
        missionsEnabled: true,
        resolverEnabled: true,
      });

      assert.notEqual(ownership.reason, 'resolved_pending_canonical_action');
    });
  });
});
