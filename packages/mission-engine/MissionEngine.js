'use strict';

/**
 * MissionEngine facade — createFromObjective → plan → execute → review.
 */

const { createBuiltinRegistry } = require('../capabilities');
const { createMissionPlanner } = require('./MissionPlanner');
const { createMissionExecutor } = require('./MissionExecutor');
const { createInMemoryMissionStore } = require('./MissionStore');
const { routeIntent, ROUTE_KINDS } = require('./IntentRouter');
const {
  MISSION_STATUS,
  AUDIT_KINDS,
  REVIEW_ACTIONS,
  missionEnabled,
} = require('./types');
const {
  createActiveMissionResolver,
} = require('./ActiveMissionResolver');
const {
  createInMemoryActiveMissionBindingStore,
} = require('./ActiveMissionBindingStore');

class MissionEngine {
  /**
   * @param {object} [deps]
   * @param {object} [deps.registry]
   * @param {object} [deps.store]
   * @param {object} [deps.planner]
   * @param {object} [deps.executor]
   * @param {object} [deps.activeMissionResolver]
   * @param {object} [deps.bindings]
   * @param {boolean} [deps.resolverEnabled]
   */
  constructor(deps = {}) {
    this._registry = deps.registry || createBuiltinRegistry();
    this._store = deps.store || createInMemoryMissionStore();
    this._planner =
      deps.planner || createMissionPlanner({ registry: this._registry });
    this._executor =
      deps.executor ||
      createMissionExecutor({
        registry: this._registry,
        store: this._store,
      });
    this._bindings =
      deps.bindings || createInMemoryActiveMissionBindingStore();
    this._resolver =
      deps.activeMissionResolver ||
      createActiveMissionResolver({
        missionEngine: this,
        bindings: this._bindings,
        enabled: deps.resolverEnabled,
      });
  }

  get registry() {
    return this._registry;
  }

  get store() {
    return this._store;
  }

  get planner() {
    return this._planner;
  }

  get executor() {
    return this._executor;
  }

  /** @returns {import('./ActiveMissionResolver').ActiveMissionResolver} */
  get activeMissionResolver() {
    return this._resolver;
  }

  get bindings() {
    return this._bindings;
  }

  /**
   * First routing layer.
   * @param {string} objective
   */
  route(objective) {
    return routeIntent(objective);
  }

  /**
   * Create, plan, and execute a mission from a business objective.
   * Ends in review_required — never auto-outreach.
   *
   * @param {object} input
   * @param {string} input.objective
   * @param {string|number} input.tenantId
   * @param {string|number} [input.clientId]
   * @param {object} [input.constraints]
   * @param {string} [input.createdBy]
   * @param {string} [input.missionType]
   * @param {boolean} [input.execute=true]
   */
  async createFromObjective(input) {
    if (!input || !String(input.objective || '').trim()) {
      throw new Error('objective is required');
    }
    if (input.tenantId == null) {
      throw new Error('tenantId is required');
    }

    const decision = routeIntent(input.objective);
    const missionType =
      input.missionType ||
      (decision.kind === ROUTE_KINDS.MISSION ? decision.missionType : null);

    const draft = this._planner.plan({
      objective: input.objective,
      missionType: missionType || undefined,
      tenantId: input.tenantId,
      clientId: input.clientId != null ? input.clientId : input.tenantId,
      constraints: input.constraints || { targetCount: 50 },
      createdBy: input.createdBy,
    });

    draft.status = MISSION_STATUS.REQUESTED;
    let mission = await this._store.create(draft);

    mission = await this._store.update({
      id: mission.id,
      status: MISSION_STATUS.PLANNING,
    });
    await this._store.appendAudit({
      missionId: mission.id,
      kind: AUDIT_KINDS.PLAN,
      payload: {
        type: mission.type,
        plannerVersion:
          (mission.plan && mission.plan.plannerVersion) || null,
        steps: (mission.plan.steps || []).map((s) => s.capabilityId),
        selectedStages:
          (mission.plan && mission.plan.selectedStages) || null,
        reviewGates: (mission.plan && mission.plan.reviewGates) || null,
        reasoning: (mission.plan && mission.plan.reasoning) || null,
        explanation: (mission.plan && mission.plan.explanation) || null,
        validation:
          (mission.plan &&
            mission.plan.executionGraph &&
            mission.plan.executionGraph.validation) ||
          null,
        confidence: mission.confidence,
        durationEstimateMs: mission.durationEstimateMs,
        discoveryProfile: mission.discoveryProfile
          ? {
              id: mission.discoveryProfile.id,
              name: mission.discoveryProfile.name,
              version: mission.discoveryProfile.version,
            }
          : null,
        clientPlaybook: mission.clientPlaybook
          ? {
              id: mission.clientPlaybook.id,
              name: mission.clientPlaybook.name,
              version: mission.clientPlaybook.version,
            }
          : null,
      },
    });

    if (input.execute === false) {
      return mission;
    }

    return this._executor.execute(mission.id);
  }

  async get(id) {
    return this._store.get(id);
  }

  async list(query) {
    return this._store.list(query);
  }

  async listAudit(missionId) {
    return this._store.listAudit(missionId);
  }

  /**
   * Operator review actions. Approve does NOT send outreach (ADR-003).
   * @param {object} input
   */
  async review(input) {
    if (!input || !input.missionId) throw new Error('missionId is required');
    const action = String(input.action || '').toLowerCase();
    if (!Object.values(REVIEW_ACTIONS).includes(action)) {
      throw new Error(`Invalid review action: ${action}`);
    }

    let mission = await this._store.get(input.missionId);
    if (!mission) throw new Error(`Unknown mission: ${input.missionId}`);

    if (action === REVIEW_ACTIONS.RUN_AGAIN) {
      // Reset plan step statuses and re-execute
      const steps = ((mission.plan && mission.plan.steps) || []).map((s) => ({
        ...s,
        status: 'queued',
        error: undefined,
      }));
      mission = await this._store.update({
        id: mission.id,
        status: MISSION_STATUS.PLANNING,
        plan: { ...mission.plan, steps },
        review: null,
        deliverables: null,
        completedAt: null,
        progress: {
          completedSteps: 0,
          totalSteps: steps.length,
          currentStage: 'Planning Mission',
          currentCapabilityId: null,
          percent: 0,
          counts: null,
        },
      });
      await this._store.appendAudit({
        missionId: mission.id,
        kind: AUDIT_KINDS.REVIEW,
        payload: { action, actor: input.actor || null },
      });
      return this._executor.execute(mission.id);
    }

    const review = {
      action,
      actor: input.actor || null,
      at: new Date().toISOString(),
      notes: input.notes || null,
      edits: input.edits || null,
      outboundSent: false,
    };

    let status = MISSION_STATUS.REVIEWED;
    if (action === REVIEW_ACTIONS.REJECT) {
      status = MISSION_STATUS.COMPLETED;
    } else if (action === REVIEW_ACTIONS.APPROVE) {
      // Approved for downstream — still no auto-send
      status = MISSION_STATUS.REVIEWED;
    } else if (action === REVIEW_ACTIONS.EDIT) {
      status = MISSION_STATUS.REVIEW_REQUIRED;
    }

    mission = await this._store.update({
      id: mission.id,
      status,
      review,
    });
    await this._store.appendAudit({
      missionId: mission.id,
      kind: AUDIT_KINDS.REVIEW,
      payload: review,
    });
    return mission;
  }

  /**
   * Card payload for Command Deck Operations.
   * @param {object} mission
   */
  toCard(mission) {
    if (!mission) return null;
    const progress = mission.progress || {};
    const eta =
      mission.startedAt && mission.durationEstimateMs
        ? new Date(
            new Date(mission.startedAt).getTime() + mission.durationEstimateMs
          ).toISOString()
        : null;
    return {
      id: mission.id,
      title: mission.title || mission.objectiveText,
      type: mission.type,
      status: mission.status,
      progress: {
        completedSteps: progress.completedSteps || 0,
        totalSteps: progress.totalSteps || 0,
        percent: progress.percent || 0,
        currentStage: progress.currentStage || null,
        counts: progress.counts || null,
      },
      startedAt: mission.startedAt,
      estimatedCompletion: eta,
      createdAt: mission.createdAt,
      objectiveText: mission.objectiveText,
    };
  }

  /**
   * Full Mission Workspace payload.
   * @param {string} missionId
   */
  async getWorkspace(missionId) {
    const mission = await this.get(missionId);
    if (!mission) return null;
    const audit = await this.listAudit(missionId);
    return {
      mission,
      card: this.toCard(mission),
      objective: mission.objectiveText,
      plan: mission.plan,
      executionGraph:
        (mission.plan && mission.plan.executionGraph) || null,
      explanation: (mission.plan && mission.plan.explanation) || null,
      currentStage:
        (mission.progress && mission.progress.currentStage) || null,
      completedStages: ((mission.plan && mission.plan.steps) || [])
        .filter((s) => s.status === 'completed')
        .map((s) => s.stageId || s.capabilityId),
      upcomingStages: ((mission.plan && mission.plan.steps) || [])
        .filter((s) => s.status === 'queued' || s.status === 'stale')
        .map((s) => s.stageId || s.capabilityId),
      reviewGates: (mission.plan && mission.plan.reviewGates) || [],
      dependencies:
        (mission.plan &&
          mission.plan.executionGraph &&
          mission.plan.executionGraph.edges) ||
        [],
      progress: mission.progress,
      evidence: collectEvidence(mission),
      results: mission.deliverables,
      review: mission.review,
      audit,
      actions: ['approve', 'reject', 'edit', 'run_again'],
      outboundBlocked: true,
    };
  }
}

function collectEvidence(mission) {
  const steps =
    (mission.deliverables && mission.deliverables.stepResults) || [];
  const evidence = [];
  for (const step of steps) {
    for (const e of step.evidence || []) {
      evidence.push({ ...e, capabilityId: step.capabilityId, name: step.name });
    }
  }
  return evidence;
}

/**
 * @param {object} [deps]
 */
function createMissionEngine(deps = {}) {
  return new MissionEngine(deps);
}

module.exports = {
  MissionEngine,
  createMissionEngine,
  missionEnabled,
};
