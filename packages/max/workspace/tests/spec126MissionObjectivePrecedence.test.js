'use strict';
const { createTestAmoRuntime, runtimeProviderFromEngine, createHydratingTestRuntime } = require('./amoTestRuntime');

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const {
  resolveWorkspaceOwner,
  WORKSPACE_OWNERS,
  claimsObjectivePersistence,
} = require('../WorkspaceOwnershipResolver');
const {
  createWorkspaceOwnershipAudit,
  clearWorkspaceOwnershipAuditLog,
  listWorkspaceOwnershipAuditLog,
} = require('../audit/WorkspaceOwnershipAudit');
const { createWorkspaceEngine } = require('../WorkspaceEngine');
const { createMemoryStore } = require('../../../../services/operatorObjectives');
const { hasExecutionLanguage } = require('../ExecutionLanguageDetection');

const ACQUIRE_OBJECTIVE =
  'I want to acquire one recurring commercial cleaning client in Greater Manchester.';

describe('SPEC-126 — Mission Creation Takes Precedence Over Objective Persistence', () => {
  beforeEach(() => {
    clearWorkspaceOwnershipAuditLog();
  });

  it('detects execution language verbs', () => {
    assert.equal(hasExecutionLanguage('Create a mission to acquire clients'), true);
    assert.equal(hasExecutionLanguage('Operate through Pulseforge'), true);
    assert.equal(hasExecutionLanguage('Remember my goal for the launch'), false);
  });

  it('selects mission_creation for "Create a mission..." not objective_persistence', async () => {
    const ownership = await resolveWorkspaceOwner({
      question:
        'Create a mission to acquire one recurring commercial cleaning client in Greater Manchester.',
      context: { tenantId: '10' },
    });
    assert.equal(ownership.owner, WORKSPACE_OWNERS.MISSION_CREATION);
    assert.notEqual(ownership.owner, WORKSPACE_OWNERS.OBJECTIVE_PERSISTENCE);
  });

  it('selects mission_creation for Operate through Pulseforge phrasing', async () => {
    const ownership = await resolveWorkspaceOwner({
      question: 'Operate through Pulseforge for Anchor Cleaning outreach',
      context: { tenantId: '10' },
    });
    assert.equal(ownership.owner, WORKSPACE_OWNERS.MISSION_CREATION);
  });

  it('blocks objective persistence when execution language is present', () => {
    assert.equal(
      claimsObjectivePersistence(
        'Create a mission and save this objective for later'
      ),
      null
    );
    assert.equal(
      claimsObjectivePersistence('Run the acquisition campaign objective'),
      null
    );
  });

  it('selects objective_persistence for explicit save/track requests', async () => {
    const ownership = await resolveWorkspaceOwner({
      question:
        'Save this as an active objective: build qualified attention for the public Max launch.',
      context: { tenantId: '1' },
    });
    assert.equal(ownership.owner, WORKSPACE_OWNERS.OBJECTIVE_PERSISTENCE);
  });

  it('selects objective_persistence for remember my goal', async () => {
    const ownership = await resolveWorkspaceOwner({
      question: 'Remember my goal to expand into Boston next quarter.',
      context: { tenantId: '1' },
    });
    assert.equal(ownership.owner, WORKSPACE_OWNERS.OBJECTIVE_PERSISTENCE);
  });

  it('acquisition objectives prefer mission_creation over objective_persistence', async () => {
    const ownership = await resolveWorkspaceOwner({
      question: ACQUIRE_OBJECTIVE,
      context: { tenantId: '10' },
    });
    assert.equal(ownership.owner, WORKSPACE_OWNERS.MISSION_CREATION);
    assert.equal(claimsObjectivePersistence(ACQUIRE_OBJECTIVE), null);
  });

  it('emits WORKSPACE_OWNER for MissionCreation never ObjectivePersistence together', async () => {
    const audit = createWorkspaceOwnershipAudit();
    const ownership = await resolveWorkspaceOwner({
      question:
        'Create a mission to acquire one recurring commercial cleaning client.',
      context: { tenantId: '10' },
    });
    audit.logOwnerSelected({ ...ownership, question: 'Create a mission…' });

    const ownerEvents = audit.log.filter((entry) => entry.event === 'WORKSPACE_OWNER');
    assert.equal(ownerEvents.length, 1);
    assert.equal(ownerEvents[0].owner, 'MissionCreation');
    assert.ok(
      !audit.log.some(
        (entry) =>
          entry.event === 'WORKSPACE_OWNER' && entry.owner === 'ObjectivePersistence'
      )
    );
  });

  it('emits WORKSPACE_OWNER ObjectivePersistence for explicit persistence only', async () => {
    const audit = createWorkspaceOwnershipAudit();
    const ownership = await resolveWorkspaceOwner({
      question: 'Track this objective: evidence-gated public Max launch.',
      context: { tenantId: '1' },
    });
    audit.logOwnerSelected({ ...ownership, question: 'Track this objective…' });

    const ownerEvents = audit.log.filter((entry) => entry.event === 'WORKSPACE_OWNER');
    assert.equal(ownerEvents.length, 1);
    assert.equal(ownerEvents[0].owner, 'ObjectivePersistence');
  });

  it('WorkspaceEngine.ask creates mission for Create a mission phrasing', async () => {
    const amoEngine = amo.createAcquisitionMissionEngine();
    const store = createMemoryStore();
    const engine = createWorkspaceEngine({
      acquisitionMissionRuntime: createTestAmoRuntime({ engine: amoEngine }),
      missionsEnabled: true,
      operatorObjectiveOpts: { store },
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
      question:
        'Create a mission to acquire one recurring commercial cleaning client in Greater Manchester.',
      context: { tenantId: '10', clientId: '10' },
    });

    assert.equal(result.workspaceOwnership.owner, WORKSPACE_OWNERS.MISSION_CREATION);
    assert.match(result.prose, /Mission Created|Mission Resumed/);
    assert.doesNotMatch(result.prose, /saved.*as an active/i);
    assert.ok(result.mission);
  });

  it('WorkspaceEngine.ask persists objective for explicit save request not mission', async () => {
    const store = createMemoryStore();
    const engine = createWorkspaceEngine({
      disableLlm: true,
      missionsEnabled: false,
      operatorObjectiveOpts: { store },
    });

    const result = await engine.ask({
      question:
        'Save this as an active objective: evidence-gated public Max launch over three weeks.',
      context: { tenantId: '1', page: 'command-deck' },
    });

    assert.equal(
      result.workspaceOwnership.owner,
      WORKSPACE_OWNERS.OBJECTIVE_PERSISTENCE
    );
    assert.equal(result.mission, null);
    assert.match(result.prose, /saved.*as an active/i);
    assert.equal(result.domainDecision.reason, 'operator_objective_established');
  });

  it('WorkspaceEngine.ask creates mission for Operate through Pulseforge', async () => {
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
      question: 'Operate through Pulseforge for Anchor Cleaning outreach',
      context: { tenantId: '10', clientId: '10' },
    });

    assert.equal(result.workspaceOwnership.owner, WORKSPACE_OWNERS.MISSION_CREATION);
    assert.match(result.prose, /Mission Created|Mission Resumed/);
    assert.ok(result.mission);
  });
});
