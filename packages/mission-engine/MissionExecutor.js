'use strict';

/**
 * MissionExecutor — runs execution graph via CapabilityRunner only (SPEC-022).
 * Never contains business-specific agent branching.
 */

const {
  createCapabilityRunner,
  CAPABILITY_RESULT_STATUS,
  CAPABILITY_EXECUTION_MODES,
  resolveCapabilityExecutionMode,
} = require('../capabilities');
const {
  MISSION_STATUS,
  AUDIT_KINDS,
  STAGE_LABELS,
  artifactBusEnabled,
} = require('./types');
const {
  evaluatePipelineGate,
  artifactValidationEnabled,
  STAGE_OUTCOMES,
  STAGE_OUTCOME_LABELS,
} = require('./PipelineGate');
const { createArtifactBus, ARTIFACT_EVENTS } = require('./ArtifactBus');
const { getStage } = require('./StageLibrary');

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
    const useBus = artifactBusEnabled();
    const bus = useBus
      ? createArtifactBus({
          snapshot:
            (mission.deliverables && mission.deliverables.artifactBus) || null,
        })
      : null;
    /** @type {object[]} */
    let consumedForStep = [];

    for (let i = 0; i < steps.length; i += 1) {
      const step = steps[i];
      const capabilityId = step.capabilityId;
      const stageDef = getStage(step.stageId) || null;
      const produces = (stageDef && stageDef.produces) || [];
      const consumes = (stageDef && stageDef.consumes) || [];

      // SPEC-039: skip completed steps when resuming after a partial modify
      if (step.status === 'completed') {
        const priorDeliverable =
          (mission.deliverables &&
            Array.isArray(mission.deliverables.stepResults) &&
            mission.deliverables.stepResults.find(
              (s) => s.capabilityId === capabilityId
            )) ||
          null;
        const outputs =
          (priorDeliverable &&
            (priorDeliverable.outputs ||
              (priorDeliverable.result && priorDeliverable.result.outputs))) ||
          null;
        if (outputs && typeof outputs === 'object') {
          priorOutputs = {
            ...priorOutputs,
            ...outputs,
          };
        }
        // Keep Satisfied (Operator Supplied) Discovery outputs in the final
        // stepResults trail so resume / Workspace still show the full list.
        if (priorDeliverable) {
          stepResults.push(normalizePreservedStepResult(priorDeliverable, step));
        }
        continue;
      }

      // SPEC-042: resolve inputs from Artifact Bus (validated latest revisions)
      if (bus) {
        const eventCountBefore = bus.events(mission.id).length;
        const resolved = bus.resolveInputs(mission.id, consumes, {
          consumer: capabilityId,
          stageId: step.stageId || null,
        });
        consumedForStep = resolved.artifacts;
        priorOutputs = {
          ...priorOutputs,
          ...resolved.priorOutputs,
        };
        await this._appendArtifactEvents(mission.id, capabilityId, bus, {
          since: eventCountBefore,
        });
      }

      await this._store.appendAudit({
        missionId: mission.id,
        kind: AUDIT_KINDS.STEP_START,
        capabilityId,
        payload: {
          index: i,
          name: step.name,
          stageId: step.stageId || null,
          consumedArtifacts: consumedForStep.map((a) => ({
            id: a.id,
            artifactType: a.artifactType,
            revision: a.revision,
          })),
        },
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
          // ADR-034: capabilities consume Mission Plan, never raw operator text
          objective: planningObjectiveFromMission(mission),
          missionPlan:
            (mission.plan && mission.plan.missionPlan) ||
            mission.missionPlan ||
            null,
          missionIntent:
            (mission.plan && mission.plan.missionIntent) ||
            mission.missionIntent ||
            null,
          // SPEC-058: diagnostic vs execution reporting (planning unchanged)
          executionMode: resolveMissionExecutionMode(mission, stageDef),
          constraints: mission.constraints || {},
          inputs: {
            ...priorOutputs,
            priorOutputs,
            prospects: priorOutputs.prospects,
            artifacts: consumedForStep,
          },
          knowledge: {},
        },
      });

      // SPEC-040 / ADR-026: business artifact validation gate
      let gate = null;
      if (artifactValidationEnabled()) {
        gate = evaluatePipelineGate({
          capabilityId,
          runResult,
          context: {
            constraints: mission.constraints || {},
            inputs: {
              prospects: priorOutputs.prospects,
            },
            priorOutputs,
          },
          mission,
        });
      } else if (
        runResult.result.status === CAPABILITY_RESULT_STATUS.BLOCKED
      ) {
        const precondition =
          (runResult.result.outputs &&
            runResult.result.outputs.preconditionDiagnostics) ||
          null;
        gate = {
          outcome: STAGE_OUTCOMES.BLOCKED,
          outcomeLabel: STAGE_OUTCOME_LABELS[STAGE_OUTCOMES.BLOCKED],
          advance: false,
          publishOutputs: false,
          blockingIssues: (runResult.result.errors || []).map((e) =>
            typeof e === 'string'
              ? e
              : e.failedPrecondition || e.message || String(e)
          ),
          warnings: runResult.result.warnings || [],
          publishedArtifacts: [],
          quarantinedArtifacts: [],
          validation: { passed: false, reason: 'capability_blocked' },
          reviewSummary: precondition
            ? { preconditionDiagnostics: precondition }
            : null,
        };
      } else if (runResult.result.status !== CAPABILITY_RESULT_STATUS.COMPLETED) {
        gate = {
          outcome: STAGE_OUTCOMES.FAILED,
          outcomeLabel: STAGE_OUTCOME_LABELS[STAGE_OUTCOMES.FAILED],
          advance: false,
          publishOutputs: false,
          blockingIssues: (runResult.result.errors || []).map((e) =>
            typeof e === 'string' ? e : e.message || String(e)
          ),
          warnings: runResult.result.warnings || [],
          publishedArtifacts: [],
          quarantinedArtifacts: runResult.result.artifacts || [],
          validation: { passed: false },
          reviewSummary: null,
        };
      } else {
        gate = {
          outcome: STAGE_OUTCOMES.COMPLETED,
          outcomeLabel: STAGE_OUTCOME_LABELS[STAGE_OUTCOMES.COMPLETED],
          advance: true,
          publishOutputs: true,
          blockingIssues: [],
          warnings: runResult.result.warnings || [],
          publishedArtifacts: runResult.result.artifacts || [],
          quarantinedArtifacts: [],
          validation: { passed: true },
          reviewSummary: null,
        };
      }

      // SPEC-042: publish / quarantine typed artifacts on the bus
      /** @type {object[]} */
      let busPublished = [];
      if (bus) {
        const eventCountBefore = bus.events(mission.id).length;
        busPublished = bus.publishFromGate({
          missionId: mission.id,
          stageId: step.stageId || null,
          producer: capabilityId,
          produces,
          outputs: runResult.result.outputs || {},
          gate,
          priorArtifacts: consumedForStep,
        });
        await this._appendArtifactEvents(mission.id, capabilityId, bus, {
          since: eventCountBefore,
        });
      }

      if (!gate.advance) {
        const stepStatus =
          gate.outcome === STAGE_OUTCOMES.FAILED ? 'failed' : 'blocked';
        const failedSteps = steps.map((s, idx) =>
          idx === i
            ? {
                ...s,
                status: stepStatus,
                outcome: gate.outcome,
                outcomeLabel: gate.outcomeLabel,
                error: gate.blockingIssues,
                blockingIssues: gate.blockingIssues,
                warnings: gate.warnings,
                reviewSummary: gate.reviewSummary,
              }
            : idx < i
              ? { ...s, status: 'completed' }
              : s
        );
        const blockLabel =
          gate.outcome === STAGE_OUTCOMES.BLOCKED
            ? `Blocked — ${gate.blockingIssues[0] || step.name}`
            : `Paused — ${step.name} failed`;
        const preconditionDiagnostics =
          (runResult.result.outputs &&
            runResult.result.outputs.preconditionDiagnostics) ||
          (runResult.result.errors &&
            runResult.result.errors[0] &&
            runResult.result.errors[0].diagnosis) ||
          (runResult.result.errors && runResult.result.errors[0]) ||
          null;
        mission = await this._store.update({
          id: mission.id,
          status: MISSION_STATUS.WAITING,
          plan: { ...mission.plan, steps: failedSteps },
          progress: {
            ...mission.progress,
            currentStage: blockLabel,
            currentCapabilityId: capabilityId,
            stageOutcome: gate.outcome,
            stageOutcomeLabel: gate.outcomeLabel,
          },
          blockingIssues: gate.blockingIssues,
          stageReview: {
            capabilityId,
            outcome: gate.outcome,
            outcomeLabel: gate.outcomeLabel,
            blockingIssues: gate.blockingIssues,
            warnings: gate.warnings,
            reviewSummary: gate.reviewSummary,
            quarantinedArtifacts: gate.quarantinedArtifacts,
            publishedArtifacts: gate.publishedArtifacts,
            preconditionDiagnostics,
          },
          deliverables: {
            ...(mission.deliverables || {}),
            stepResults,
            lastGate: {
              capabilityId,
              outcome: gate.outcome,
              blockingIssues: gate.blockingIssues,
              quarantinedArtifacts: gate.quarantinedArtifacts,
              preconditionDiagnostics,
            },
            preconditionDiagnostics,
            artifactBus: bus ? bus.toJSON() : undefined,
          },
        });
        await this._store.appendAudit({
          missionId: mission.id,
          kind:
            gate.outcome === STAGE_OUTCOMES.BLOCKED
              ? AUDIT_KINDS.STAGE_BLOCKED
              : AUDIT_KINDS.STEP_FAIL,
          capabilityId,
          payload: {
            outcome: gate.outcome,
            outcomeLabel: gate.outcomeLabel,
            blockingIssues: gate.blockingIssues,
            warnings: gate.warnings,
            validation: gate.validation,
            quarantinedArtifacts: gate.quarantinedArtifacts,
            busArtifacts: busPublished.map((a) => ({
              id: a.id,
              artifactType: a.artifactType,
              revision: a.revision,
              validationStatus: a.validationStatus,
            })),
            errors:
              gate.outcome === STAGE_OUTCOMES.FAILED
                ? runResult.result.errors
                : undefined,
          },
        });
        return mission;
      }

      if (gate.publishOutputs) {
        priorOutputs = {
          ...priorOutputs,
          ...runResult.result.outputs,
        };
      }
      stepResults.push({
        capabilityId,
        name: runResult.name,
        result: runResult.result,
        outcome: gate.outcome,
        outcomeLabel: gate.outcomeLabel,
        publishedArtifacts: gate.publishedArtifacts,
        quarantinedArtifacts: gate.quarantinedArtifacts,
        busArtifacts: busPublished,
        validation: gate.validation,
        warnings: gate.warnings,
        reviewSummary: gate.reviewSummary,
      });

      const completedSteps = steps.map((s, idx) => {
        if (idx < i) {
          // Preserve earlier stage outcomes (e.g. Satisfied Operator Supplied)
          return { ...s, status: 'completed' };
        }
        if (idx === i) {
          return {
            ...s,
            status: 'completed',
            outcome: gate.outcome,
            outcomeLabel: gate.outcomeLabel,
            warnings: gate.warnings,
            reviewSummary: gate.reviewSummary,
          };
        }
        return s;
      });
      const stageLabel =
        gate.outcome === STAGE_OUTCOMES.COMPLETED_WITH_WARNINGS
          ? `${step.stageLabel || step.name} — ${gate.outcomeLabel}`
          : step.stageLabel || step.name;
      mission = await this._store.update({
        id: mission.id,
        plan: { ...mission.plan, steps: completedSteps },
        progress: {
          completedSteps: i + 1,
          totalSteps: steps.length,
          currentStage: stageLabel,
          currentCapabilityId: capabilityId,
          percent: Math.round(((i + 1) / Math.max(steps.length, 1)) * 100),
          stageOutcome: gate.outcome,
          stageOutcomeLabel: gate.outcomeLabel,
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
        deliverables: {
          ...(mission.deliverables || {}),
          artifactBus: bus ? bus.toJSON() : undefined,
        },
      });
      await this._store.appendAudit({
        missionId: mission.id,
        kind:
          gate.outcome === STAGE_OUTCOMES.COMPLETED_WITH_WARNINGS
            ? AUDIT_KINDS.STAGE_WARNINGS
            : AUDIT_KINDS.STEP_OK,
        capabilityId,
        payload: {
          duration: runResult.result.duration,
          outputsKeys: Object.keys(runResult.result.outputs || {}),
          outcome: gate.outcome,
          outcomeLabel: gate.outcomeLabel,
          warnings: gate.warnings,
          publishedArtifacts: gate.publishedArtifacts,
          busArtifacts: busPublished.map((a) => ({
            id: a.id,
            artifactType: a.artifactType,
            revision: a.revision,
            validationStatus: a.validationStatus,
            summary: a.summary,
          })),
          validation: gate.validation,
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
        clientPlaybook: priorOutputs.clientPlaybook ||
          (mission.constraints && mission.constraints.clientPlaybook) ||
          null,
        clientPlaybookId:
          priorOutputs.clientPlaybookId ||
          (mission.constraints && mission.constraints.clientPlaybookId) ||
          null,
        clientPlaybookVersion:
          priorOutputs.clientPlaybookVersion ||
          (mission.constraints && mission.constraints.clientPlaybookVersion) ||
          null,
        reviewPackage: priorOutputs.reviewPackage || null,
        summary: priorOutputs.summary || null,
        rejected: priorOutputs.rejected || null,
        suggestedNextActions: priorOutputs.suggestedNextActions || null,
        proposal: priorOutputs.proposal || null,
        document: priorOutputs.document || null,
        html: priorOutputs.html || null,
        proposalId: priorOutputs.proposalId || null,
        outboundBlocked: true,
        artifactBus: bus ? bus.toJSON() : null,
        artifactGraph: bus ? bus.getArtifactGraph(mission.id) : null,
        stepResults: stepResults.map((s) => ({
          capabilityId: s.capabilityId,
          name: s.name,
          status: s.result.status,
          outcome: s.outcome || null,
          outcomeLabel: s.outcomeLabel || null,
          evidence: s.result.evidence,
          artifacts: s.publishedArtifacts || s.result.artifacts,
          publishedArtifacts: s.publishedArtifacts || [],
          quarantinedArtifacts: s.quarantinedArtifacts || [],
          busArtifacts: (s.busArtifacts || []).map((a) => ({
            id: a.id,
            artifactType: a.artifactType,
            revision: a.revision,
            validationStatus: a.validationStatus,
            summary: a.summary,
          })),
          validation: s.validation || null,
          warnings: s.warnings || s.result.warnings || [],
          reviewSummary: s.reviewSummary || null,
          outputs: s.result.outputs || {},
          result: { outputs: s.result.outputs || {} },
        })),
        stageOutcomes: stepResults.map((s) => ({
          capabilityId: s.capabilityId,
          outcome: s.outcome,
          outcomeLabel: s.outcomeLabel,
          reviewSummary: s.reviewSummary,
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

  /**
   * Mirror Artifact Bus events into mission audit (SPEC-042).
   * @param {string} missionId
   * @param {string|null} capabilityId
   * @param {import('./ArtifactBus').ArtifactBus} bus
   * @param {object} [opts]
   * @param {number} [opts.since]
   * @param {string[]} [opts.onlyKinds]
   */
  async _appendArtifactEvents(missionId, capabilityId, bus, opts = {}) {
    const since = opts.since != null ? opts.since : 0;
    const events = bus.events(missionId).slice(since);
    const only = opts.onlyKinds || null;
    for (const ev of events) {
      if (only && !only.includes(ev.type)) continue;
      const kind = artifactEventToAuditKind(ev.type);
      if (!kind) continue;
      await this._store.appendAudit({
        missionId,
        kind,
        capabilityId,
        payload: ev,
      });
    }
  }
}

function artifactEventToAuditKind(type) {
  switch (type) {
    case ARTIFACT_EVENTS.PUBLISHED:
      return AUDIT_KINDS.ARTIFACT_PUBLISHED;
    case ARTIFACT_EVENTS.VALIDATED:
      return AUDIT_KINDS.ARTIFACT_VALIDATED;
    case ARTIFACT_EVENTS.QUARANTINED:
      return AUDIT_KINDS.ARTIFACT_QUARANTINED;
    case ARTIFACT_EVENTS.SUPERSEDED:
      return AUDIT_KINDS.ARTIFACT_SUPERSEDED;
    case ARTIFACT_EVENTS.CONSUMED:
      return AUDIT_KINDS.ARTIFACT_CONSUMED;
    default:
      return null;
  }
}

/**
 * Normalize a previously completed stepResult (e.g. operator-injected Discovery)
 * into the shape MissionExecutor pushes for newly run steps.
 * @param {object} prior
 * @param {object} step
 */
function normalizePreservedStepResult(prior, step) {
  const outputs =
    (prior && prior.outputs) ||
    (prior && prior.result && prior.result.outputs) ||
    {};
  const evidence =
    (prior && prior.evidence) ||
    (prior && prior.result && prior.result.evidence) ||
    [];
  return {
    capabilityId: prior.capabilityId || step.capabilityId,
    name: prior.name || step.name || step.stageLabel,
    result: {
      status: CAPABILITY_RESULT_STATUS.COMPLETED,
      outputs,
      evidence,
      warnings: prior.warnings || [],
      errors: [],
      artifacts: prior.artifacts || prior.publishedArtifacts || [],
    },
    outcome: prior.outcome || step.outcome || null,
    outcomeLabel: prior.outcomeLabel || step.outcomeLabel || null,
    publishedArtifacts: prior.publishedArtifacts || [],
    quarantinedArtifacts: prior.quarantinedArtifacts || [],
    busArtifacts: prior.busArtifacts || [],
    validation: prior.validation || null,
    warnings: prior.warnings || [],
    reviewSummary: prior.reviewSummary || null,
  };
}

/**
 * ADR-034 — capabilities receive Mission Plan objective, never raw operator NL.
 * @param {object} mission
 * @returns {string}
 */
function planningObjectiveFromMission(mission) {
  const plan =
    (mission && mission.plan && mission.plan.missionPlan) ||
    (mission && mission.missionPlan) ||
    null;
  if (plan && plan.objective) {
    const parts = [String(plan.objective).trim()];
    if (plan.subject) parts.push(`for ${plan.subject}`);
    return parts.join(' ');
  }
  // Legacy missions without Mission Plan IR — use stored objective as-is
  return String((mission && mission.objectiveText) || '').trim();
}

/**
 * SPEC-058 — derive capability execution mode from mission intent / stage.
 * Does not alter Mission Planning.
 * @param {object} mission
 * @param {object|null} stageDef
 * @returns {string}
 */
function resolveMissionExecutionMode(mission, stageDef) {
  if (stageDef && stageDef.diagnostic === true) {
    return CAPABILITY_EXECUTION_MODES.DIAGNOSTIC;
  }
  const intent =
    (mission && mission.plan && mission.plan.missionIntent) ||
    (mission && mission.missionIntent) ||
    null;
  return resolveCapabilityExecutionMode(
    {
      missionIntent: intent,
      executionMode:
        (mission &&
          mission.constraints &&
          mission.constraints.executionMode) ||
        null,
    },
    stageDef && stageDef.diagnostic ? { diagnostic: true } : null
  );
}

function createMissionExecutor(deps) {
  return new MissionExecutor(deps);
}

module.exports = {
  MissionExecutor,
  createMissionExecutor,
  planningObjectiveFromMission,
};
