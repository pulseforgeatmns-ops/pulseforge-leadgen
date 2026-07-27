'use strict';

/**
 * MissionExecutor — runs execution graph via CapabilityRunner only (SPEC-022).
 * Never contains business-specific agent branching.
 */

const {
  createCapabilityRunner,
  CAPABILITY_RESULT_STATUS,
} = require('../capabilities');
const {
  MISSION_STATUS,
  AUDIT_KINDS,
  STAGE_LABELS,
} = require('./types');

class MissionExecutor {
  /**
   * @param {object} deps
   * @param {import('@pulseforge/capabilities').CapabilityRegistry} deps.registry
   * @param {object} deps.store - MissionStore
   * @param {import('@pulseforge/capabilities').CapabilityRunner} [deps.runner]
   */
  constructor(deps) {
    if (!deps || !deps.registry) {
      throw new Error('MissionExecutor requires registry');
    }
    if (!deps.store) {
      throw new Error('MissionExecutor requires store');
    }
    this._registry = deps.registry;
    this._store = deps.store;
    this._runner =
      deps.runner ||
      createCapabilityRunner({
        registry: deps.registry,
      });
  }

  /**
   * Wire mid-capability progress into mission.progress + audit (SPEC-024).
   * @param {string} missionId
   * @param {object} step
   * @param {number} stepIndex
   * @param {number} totalSteps
   */
  _bindProgress(missionId, step, stepIndex, totalSteps) {
    return async (event) => {
      if (!event || event.kind !== 'progress') return;
      const stage =
        (event.payload && (event.payload.stage || event.payload.message)) ||
        step.stageLabel;
      const counts =
        event.payload &&
        (event.payload.verified != null || event.payload.targetCount != null)
          ? {
              completed: event.payload.verified || event.payload.discovered || 0,
              total: event.payload.targetCount || event.payload.candidates || 0,
            }
          : null;
      try {
        await this._store.update({
          id: missionId,
          progress: {
            completedSteps: stepIndex,
            totalSteps,
            currentStage: stage,
            currentCapabilityId: step.capabilityId,
            percent: Math.round((stepIndex / Math.max(totalSteps, 1)) * 100),
            counts,
          },
        });
        await this._store.appendAudit({
          missionId,
          kind: AUDIT_KINDS.PROGRESS,
          capabilityId: step.capabilityId,
          payload: {
            stage,
            ...(event.payload || {}),
          },
        });
      } catch {
        // Progress updates are best-effort
      }
    };
  }

  /**
   * Execute a planned mission through to review_required (or failed/waiting).
   * @param {string} missionId
   * @returns {Promise<object>}
   */
  async execute(missionId) {
    let mission = await this._store.get(missionId);
    if (!mission) throw new Error(`Unknown mission: ${missionId}`);

    if (mission.plan && Array.isArray(mission.plan.missingPrerequisites)) {
      const missing = mission.plan.missingPrerequisites;
      if (missing.length) {
        mission = await this._store.update({
          id: mission.id,
          status: MISSION_STATUS.WAITING,
          progress: {
            ...mission.progress,
            currentStage: 'Blocked — missing capabilities',
          },
        });
        await this._store.appendAudit({
          missionId: mission.id,
          kind: AUDIT_KINDS.STATUS,
          payload: { status: MISSION_STATUS.WAITING, missing },
        });
        return mission;
      }
    }

    const startedAt = mission.startedAt || new Date().toISOString();
    mission = await this._store.update({
      id: mission.id,
      status: MISSION_STATUS.EXECUTING,
      startedAt,
    });
    await this._store.appendAudit({
      missionId: mission.id,
      kind: AUDIT_KINDS.STATUS,
      payload: { status: MISSION_STATUS.EXECUTING },
    });

    const steps = (mission.plan && mission.plan.steps) || [];
    /** @type {object} */
    let priorOutputs = {};
    const stepResults = [];

    for (let i = 0; i < steps.length; i += 1) {
      const step = steps[i];
      const capabilityId = step.capabilityId;

      await this._store.appendAudit({
        missionId: mission.id,
        kind: AUDIT_KINDS.STEP_START,
        capabilityId,
        payload: { index: i, name: step.name },
      });

      mission = await this._store.update({
        id: mission.id,
        progress: {
          completedSteps: i,
          totalSteps: steps.length,
          currentStage: step.stageLabel || STAGE_LABELS[capabilityId] || step.name,
          currentCapabilityId: capabilityId,
          percent: Math.round((i / Math.max(steps.length, 1)) * 100),
          counts: priorOutputs.prospectCount
            ? {
                completed: priorOutputs.prospectCount,
                total: (mission.constraints && mission.constraints.targetCount) || priorOutputs.prospectCount,
              }
            : null,
        },
        plan: {
          ...mission.plan,
          steps: steps.map((s, idx) =>
            idx === i ? { ...s, status: 'running' } : s
          ),
        },
      });

      const onProgress = this._bindProgress(
        mission.id,
        step,
        i,
        steps.length
      );
      const stepRunner = createCapabilityRunner({
        registry: this._registry,
        onProgress,
      });

      const runResult = await stepRunner.run({
        capabilityId,
        context: {
          missionId: mission.id,
          tenantId: mission.tenantId,
          clientId: mission.clientId,
          objective: mission.objectiveText,
          constraints: mission.constraints || {},
          inputs: {
            ...priorOutputs,
            priorOutputs,
            prospects: priorOutputs.prospects,
          },
          knowledge: {},
        },
      });

      if (runResult.result.status !== CAPABILITY_RESULT_STATUS.COMPLETED) {
        const failedSteps = steps.map((s, idx) =>
          idx === i
            ? { ...s, status: 'failed', error: runResult.result.errors }
            : idx < i
              ? { ...s, status: 'completed' }
              : s
        );
        mission = await this._store.update({
          id: mission.id,
          status: MISSION_STATUS.WAITING,
          plan: { ...mission.plan, steps: failedSteps },
          progress: {
            ...mission.progress,
            currentStage: `Paused — ${step.name} failed`,
            currentCapabilityId: capabilityId,
          },
        });
        await this._store.appendAudit({
          missionId: mission.id,
          kind: AUDIT_KINDS.STEP_FAIL,
          capabilityId,
          payload: { errors: runResult.result.errors },
        });
        return mission;
      }

      priorOutputs = {
        ...priorOutputs,
        ...runResult.result.outputs,
      };
      stepResults.push({
        capabilityId,
        name: runResult.name,
        result: runResult.result,
      });

      const completedSteps = steps.map((s, idx) =>
        idx <= i ? { ...s, status: 'completed' } : s
      );
      mission = await this._store.update({
        id: mission.id,
        plan: { ...mission.plan, steps: completedSteps },
        progress: {
          completedSteps: i + 1,
          totalSteps: steps.length,
          currentStage: step.stageLabel || step.name,
          currentCapabilityId: capabilityId,
          percent: Math.round(((i + 1) / Math.max(steps.length, 1)) * 100),
          counts: priorOutputs.prospectCount
            ? {
                completed: priorOutputs.prospectCount,
                total:
                  (mission.constraints && mission.constraints.targetCount) ||
                  priorOutputs.targetCount ||
                  priorOutputs.prospectCount,
              }
            : null,
        },
      });
      await this._store.appendAudit({
        missionId: mission.id,
        kind: AUDIT_KINDS.STEP_OK,
        capabilityId,
        payload: {
          duration: runResult.result.duration,
          outputsKeys: Object.keys(runResult.result.outputs || {}),
        },
      });
    }

    const completedAt = new Date().toISOString();
    mission = await this._store.update({
      id: mission.id,
      status: MISSION_STATUS.REVIEW_REQUIRED,
      completedAt,
      deliverables: {
        campaign: priorOutputs.campaign || null,
        prospects: priorOutputs.prospects || null,
        rankedCount: priorOutputs.rankedCount || null,
        discoveryProfile: priorOutputs.discoveryProfile || null,
        reviewPackage: priorOutputs.reviewPackage || null,
        summary: priorOutputs.summary || null,
        rejected: priorOutputs.rejected || null,
        suggestedNextActions: priorOutputs.suggestedNextActions || null,
        outboundBlocked: true,
        stepResults: stepResults.map((s) => ({
          capabilityId: s.capabilityId,
          name: s.name,
          status: s.result.status,
          evidence: s.result.evidence,
          artifacts: s.result.artifacts,
        })),
      },
      progress: {
        completedSteps: steps.length,
        totalSteps: steps.length,
        currentStage: STAGE_LABELS.review_required,
        currentCapabilityId: null,
        percent: 100,
        counts: priorOutputs.prospectCount
          ? {
              completed: priorOutputs.prospectCount,
              total:
                (mission.constraints && mission.constraints.targetCount) ||
                priorOutputs.prospectCount,
            }
          : null,
      },
    });
    await this._store.appendAudit({
      missionId: mission.id,
      kind: AUDIT_KINDS.STATUS,
      payload: {
        status: MISSION_STATUS.REVIEW_REQUIRED,
        outboundBlocked: true,
      },
    });

    return mission;
  }
}

function createMissionExecutor(deps) {
  return new MissionExecutor(deps);
}

module.exports = {
  MissionExecutor,
  createMissionExecutor,
};
