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
  STAGE_OUTCOMES,
  isTerminalStatus,
  missionEnabled,
} = require('./types');
const {
  createActiveMissionResolver,
} = require('./ActiveMissionResolver');
const {
  createInMemoryActiveMissionBindingStore,
} = require('./ActiveMissionBindingStore');
const { createArtifactBus } = require('./ArtifactBus');
const {
  publishOperatorProspectList,
  detectOperatorProspectListInMessage,
} = require('./OperatorArtifactInjection');
const { STAGE_OUTCOME_LABELS } = require('./PipelineGate');
const { BUILTIN_IDS } = require('../capabilities/types');

/** Max objective length when operator pastes a prospect list into Mission chat. */
const OBJECTIVE_MAX_CHARS = 100000;

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
   * When the objective embeds a validated prospect list (SPEC-043), skips
   * Discovery and injects the list onto the Artifact Bus before resume.
   *
   * @param {object} input
   * @param {string} input.objective
   * @param {string|number} input.tenantId
   * @param {string|number} [input.clientId]
   * @param {object} [input.constraints]
   * @param {string} [input.createdBy]
   * @param {string} [input.missionType]
   * @param {boolean} [input.execute=true]
   * @param {boolean} [input.detectOperatorList=true]
   * @param {object} [input.operatorProspectList] - precomputed detection result
   */
  async createFromObjective(input) {
    if (!input || !String(input.objective || '').trim()) {
      throw new Error('objective is required');
    }
    if (input.tenantId == null) {
      throw new Error('tenantId is required');
    }

    const objectiveRaw = String(input.objective)
      .trim()
      .slice(0, OBJECTIVE_MAX_CHARS);
    console.info('[mission-objective-len]', {
      stage: 'createFromObjective',
      chars: objectiveRaw.length,
      inputChars: String(input.objective || '').length,
      trimmedChars: String(input.objective || '').trim().length,
      sliced: String(input.objective || '').trim().length > OBJECTIVE_MAX_CHARS,
      newlines: (objectiveRaw.match(/\n/g) || []).length,
    });
    const detection =
      input.operatorProspectList ||
      (input.detectOperatorList === false
        ? null
        : detectOperatorProspectListInMessage(objectiveRaw));
    const planningObjective =
      (detection &&
        detection.detected &&
        detection.objectiveText &&
        String(detection.objectiveText).trim()) ||
      objectiveRaw;

    const decision = routeIntent(planningObjective);
    const missionType =
      input.missionType ||
      (decision.kind === ROUTE_KINDS.MISSION ? decision.missionType : null);

    const constraints = {
      ...(input.constraints || { targetCount: 50 }),
    };
    if (
      detection &&
      detection.autoInject &&
      detection.prospectCount > 0 &&
      (input.constraints == null || input.constraints.targetCount == null)
    ) {
      constraints.targetCount = detection.prospectCount;
    }

    // SPEC-051: surface operator / catalog ProspectList before capability selection
    const availableArtifacts = Array.isArray(input.availableArtifacts)
      ? [...input.availableArtifacts]
      : [];
    if (detection && detection.detected && detection.prospectCount > 0) {
      availableArtifacts.push({
        type: 'ProspectList',
        source: 'operator_import',
        confidence: detection.confidence || 'High',
        freshness: 'Operator Supplied',
        compatible: true,
        pending: !detection.autoInject,
        producer: 'operator_import',
        prospectCount: detection.prospectCount,
      });
    }

    // SPEC-052: natural-language rejection stays reviewable, never executable
    const artifactValidationFailures = [];
    if (detection && detection.validationFailure) {
      artifactValidationFailures.push(detection.validationFailure);
    }

    const draft = this._planner.plan({
      objective: planningObjective,
      missionType: missionType || undefined,
      tenantId: input.tenantId,
      clientId: input.clientId != null ? input.clientId : input.tenantId,
      constraints,
      createdBy: input.createdBy,
      availableArtifacts: availableArtifacts.length ? availableArtifacts : null,
      previousMissionArtifacts: input.previousMissionArtifacts || null,
      workspaceArtifacts: input.workspaceArtifacts || null,
    });

    // Keep full operator prompt (including pasted list) on the Mission record.
    draft.objectiveText = objectiveRaw;

    draft.status = MISSION_STATUS.REQUESTED;
    let mission = await this._store.create(draft);

    mission = await this._store.update({
      id: mission.id,
      status: MISSION_STATUS.PLANNING,
      ...(artifactValidationFailures.length
        ? {
            deliverables: {
              ...(mission.deliverables || {}),
              artifactValidationFailures,
            },
          }
        : {}),
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
        operatorProspectList:
          detection && detection.detected
            ? {
                confidence: detection.confidence,
                autoInject: detection.autoInject,
                promptImport: detection.promptImport,
                prospectCount: detection.prospectCount,
              }
            : null,
        artifactResolution:
          (mission.plan && mission.plan.artifactResolution) || null,
        artifactValidationFailures: artifactValidationFailures.length
          ? artifactValidationFailures
          : null,
      },
    });

    const shouldAutoInject =
      detection &&
      detection.autoInject &&
      detection.paste &&
      (this._missionHasDiscoveryStage(mission) ||
        this._missionResolvedProspectList(mission));

    if (shouldAutoInject) {
      if (input.execute === false) {
        mission = await this._store.update({
          id: mission.id,
          deliverables: {
            ...(mission.deliverables || {}),
            pendingOperatorImport: {
              paste: detection.paste,
              source: detection.source,
              prospectCount: detection.prospectCount,
              confidence: detection.confidence,
              autoInjectPending: true,
              detectedAt: new Date().toISOString(),
            },
          },
        });
        mission.operatorProspectList = detection;
        return mission;
      }

      const injected = await this.injectProspectList({
        missionId: mission.id,
        paste: detection.paste,
        source: detection.source,
        createdBy: input.createdBy || 'operator',
        execute: true,
      });
      injected.mission.operatorProspectList = {
        ...detection,
        injected: true,
        artifactId: injected.artifact && injected.artifact.id,
      };
      return injected.mission;
    }

    if (detection && detection.promptImport && detection.paste) {
      mission = await this._store.update({
        id: mission.id,
        deliverables: {
          ...(mission.deliverables || {}),
          pendingOperatorImport: {
            paste: detection.paste,
            source: detection.source,
            prospectCount: detection.prospectCount,
            confidence: detection.confidence,
            errors:
              (detection.validation && detection.validation.errors) || [],
            warnings:
              (detection.validation && detection.validation.warnings) || [],
            detectedAt: new Date().toISOString(),
          },
        },
      });
      mission.operatorProspectList = detection;
    }

    if (input.execute === false) {
      if (detection && detection.detected) {
        mission.operatorProspectList = detection;
      }
      return mission;
    }

    mission = await this._executor.execute(mission.id);
    if (detection && detection.detected) {
      mission.operatorProspectList = {
        ...detection,
        injected: Boolean(
          mission.operatorProspectList && mission.operatorProspectList.injected
        ),
      };
      // Preserve pending import from store after execute merge
      const fresh = await this.get(mission.id);
      if (
        fresh &&
        fresh.deliverables &&
        fresh.deliverables.pendingOperatorImport
      ) {
        mission.deliverables = {
          ...(mission.deliverables || {}),
          pendingOperatorImport: fresh.deliverables.pendingOperatorImport,
        };
      }
    }

    // SPEC-052: preserve reviewable validation failures across execute overwrite
    if (artifactValidationFailures.length) {
      const nextDeliverables = {
        ...(mission.deliverables || {}),
        artifactValidationFailures,
      };
      mission = await this._store.update({
        id: mission.id,
        deliverables: nextDeliverables,
      });
    }

    return mission;
  }

  _missionHasDiscoveryStage(mission) {
    const steps = (mission && mission.plan && mission.plan.steps) || [];
    return steps.some(
      (s) =>
        s.stageId === 'prospect_discovery' ||
        s.capabilityId === BUILTIN_IDS.PROSPECT_DISCOVERY
    );
  }

  /**
   * SPEC-051 — ProspectList already resolved at plan time (Discovery skipped).
   * @param {object} mission
   */
  _missionResolvedProspectList(mission) {
    const resolution =
      (mission && mission.plan && mission.plan.artifactResolution) || null;
    if (!resolution || !Array.isArray(resolution.resolved)) return false;
    return resolution.resolved.some(
      (r) =>
        r &&
        (r.type === 'ProspectList' || r.type === 'prospect_list') &&
        r.compatible !== false
    );
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
    const bus = createArtifactBus({
      snapshot:
        (mission.deliverables && mission.deliverables.artifactBus) || null,
    });
    const artifacts = bus.listMissionArtifacts(mission.id);
    const artifactGraph =
      (mission.deliverables && mission.deliverables.artifactGraph) ||
      bus.getArtifactGraph(mission.id);
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
      artifacts,
      artifactGraph,
      artifactEvents: bus.events(mission.id),
      review: mission.review,
      audit,
      actions: workspaceActions(mission),
      recoveryActions: discoveryRecoveryActions(mission),
      pendingOperatorImport:
        (mission.deliverables && mission.deliverables.pendingOperatorImport) ||
        null,
      artifactValidationFailures:
        (mission.deliverables &&
          mission.deliverables.artifactValidationFailures) ||
        [],
      outboundBlocked: true,
    };
  }

  /**
   * SPEC-042 — compare two artifact revisions on a mission.
   * @param {string} missionId
   * @param {string} artifactIdA
   * @param {string} artifactIdB
   */
  async compareArtifacts(missionId, artifactIdA, artifactIdB) {
    const mission = await this.get(missionId);
    if (!mission) throw new Error(`Unknown mission: ${missionId}`);
    const bus = createArtifactBus({
      snapshot:
        (mission.deliverables && mission.deliverables.artifactBus) || null,
    });
    return bus.compareArtifacts(artifactIdA, artifactIdB);
  }

  /**
   * SPEC-042 — replay plan from an artifact revision.
   * @param {string} missionId
   * @param {string} artifactId
   */
  async replayFromArtifact(missionId, artifactId) {
    const mission = await this.get(missionId);
    if (!mission) throw new Error(`Unknown mission: ${missionId}`);
    const bus = createArtifactBus({
      snapshot:
        (mission.deliverables && mission.deliverables.artifactBus) || null,
    });
    const planStageIds = ((mission.plan && mission.plan.steps) || []).map(
      (s) => s.stageId || s.capabilityId
    );
    return bus.replayFromArtifact(missionId, artifactId, { planStageIds });
  }

  /**
   * SPEC-043 — operator injects a validated ProspectList onto the Artifact Bus.
   * Marks Discovery Satisfied (Operator Supplied) when Discovery is on the plan,
   * or publishes onto a plan that already resolved ProspectList (SPEC-051).
   *
   * @param {object} input
   * @param {string} input.missionId
   * @param {object[]} [input.prospects]
   * @param {string} [input.csv]
   * @param {string} [input.paste]
   * @param {string} [input.source]
   * @param {string} [input.createdBy]
   * @param {boolean} [input.execute=true]
   */
  async injectProspectList(input = {}) {
    if (!input.missionId) throw new Error('missionId is required');
    let mission = await this.get(input.missionId);
    if (!mission) throw new Error(`Unknown mission: ${input.missionId}`);
    if (isTerminalStatus(mission.status)) {
      const err = new Error('Cannot inject artifacts into a terminal Mission');
      err.code = 'mission_terminal';
      throw err;
    }

    const steps = (mission.plan && mission.plan.steps) || [];
    const discoveryIdx = steps.findIndex(
      (s) =>
        s.stageId === 'prospect_discovery' ||
        s.capabilityId === BUILTIN_IDS.PROSPECT_DISCOVERY
    );
    const discoverySkipped =
      discoveryIdx < 0 && this._missionResolvedProspectList(mission);
    if (discoveryIdx < 0 && !discoverySkipped) {
      const err = new Error('Mission plan has no Discovery stage');
      err.code = 'no_discovery_stage';
      throw err;
    }

    const bus = createArtifactBus({
      snapshot:
        (mission.deliverables && mission.deliverables.artifactBus) || null,
    });

    const published = publishOperatorProspectList({
      bus,
      missionId: mission.id,
      stageId: discoverySkipped
        ? 'operator_injection'
        : 'prospect_discovery',
      prospects: input.prospects,
      csv: input.csv,
      paste: input.paste,
      source: input.source,
      createdBy: input.createdBy || 'operator',
      targetCount:
        (mission.constraints && mission.constraints.targetCount) || undefined,
    });

    if (!published.ok) {
      const failure =
        published.validationFailure ||
        {
          title: 'Artifact Validation',
          artifactType: 'ProspectList',
          status: 'FAILED',
          reasons: published.errors || ['ProspectList validation failed'],
          remainsPlainText: false,
          createdAt: new Date().toISOString(),
        };
      const priorFailures = Array.isArray(
        mission.deliverables && mission.deliverables.artifactValidationFailures
      )
        ? mission.deliverables.artifactValidationFailures
        : [];
      await this._store.update({
        id: mission.id,
        deliverables: {
          ...(mission.deliverables || {}),
          artifactValidationFailures: [...priorFailures, failure],
        },
      });
      const err = new Error(
        (published.errors && published.errors[0]) ||
          'ProspectList validation failed'
      );
      err.code = 'prospect_list_invalid';
      err.errors = published.errors;
      err.warnings = published.warnings;
      err.validationFailure = failure;
      throw err;
    }

    const outcome = STAGE_OUTCOMES.SATISFIED_OPERATOR_SUPPLIED;
    const outcomeLabel =
      STAGE_OUTCOME_LABELS[outcome] || 'Satisfied (Operator Supplied)';

    // Preserve completed upstream/peer steps; satisfy Discovery when present;
    // invalidate downstream that depended on a prior ProspectList when present.
    const pivotIdx = discoveryIdx >= 0 ? discoveryIdx : -1;
    const nextSteps = steps.map((s, idx) => {
      if (pivotIdx >= 0 && idx === pivotIdx) {
        return {
          ...s,
          status: 'completed',
          outcome,
          outcomeLabel,
          error: undefined,
          blockingIssues: [],
          warnings: published.warnings || [],
          reviewSummary: {
            stageStatus: outcomeLabel,
            publishedCount:
              (published.payload && published.payload.prospectCount) || 0,
            operatorSupplied: true,
          },
        };
      }
      if (pivotIdx >= 0 && idx > pivotIdx && s.status === 'completed') {
        return { ...s, status: 'stale' };
      }
      if (
        pivotIdx >= 0 &&
        idx > pivotIdx &&
        (s.status === 'blocked' || s.status === 'failed' || s.status === 'running')
      ) {
        return { ...s, status: 'queued', error: undefined, blockingIssues: [] };
      }
      // SPEC-051: Discovery already omitted — keep queued downstream ready
      if (discoverySkipped && (s.status === 'blocked' || s.status === 'failed')) {
        return { ...s, status: 'queued', error: undefined, blockingIssues: [] };
      }
      return s;
    });

    const payload = published.payload || {};
    const stepResult = {
      capabilityId: BUILTIN_IDS.PROSPECT_DISCOVERY,
      name: discoverySkipped ? 'ProspectList (resolved)' : 'Discovery',
      stageId: discoverySkipped ? 'artifact_resolution' : 'prospect_discovery',
      outcome,
      outcomeLabel,
      operatorSupplied: true,
      outputs: {
        prospects: payload.prospects,
        prospectCount: payload.prospectCount,
        targetCount: payload.targetCount,
        summary: payload.summary,
      },
      evidence: [
        {
          kind: 'operator_artifact',
          summary: `Operator supplied ProspectList (${payload.prospectCount} prospects) via ${published.source}`,
        },
      ],
    };

    const priorStepResults = Array.isArray(
      mission.deliverables && mission.deliverables.stepResults
    )
      ? mission.deliverables.stepResults.filter(
          (s) => s.capabilityId !== BUILTIN_IDS.PROSPECT_DISCOVERY
        )
      : [];

    const nextDeliverables = {
      ...(mission.deliverables || {}),
      stepResults: [...priorStepResults, stepResult],
      artifactBus: bus.toJSON(),
      artifactGraph: bus.getArtifactGraph(mission.id),
      lastInjection: {
        artifactId: published.artifact.id,
        artifactType: published.artifact.artifactType,
        revision: published.artifact.revision,
        producer: published.producer,
        source: published.source,
        at: new Date().toISOString(),
      },
    };
    delete nextDeliverables.pendingOperatorImport;

    const resolutionPatch =
      discoverySkipped && mission.plan && mission.plan.artifactResolution
        ? {
            artifactResolution: {
              ...mission.plan.artifactResolution,
              resolved: (
                mission.plan.artifactResolution.resolved || []
              ).map((r) =>
                r.type === 'ProspectList' || r.type === 'prospect_list'
                  ? {
                      ...r,
                      pending: false,
                      artifactId: published.artifact.id,
                      revision: published.artifact.revision,
                      source: 'operator_import',
                      sourceLabel: 'Operator Import',
                    }
                  : r
              ),
            },
          }
        : {};

    mission = await this._store.update({
      id: mission.id,
      status: MISSION_STATUS.WAITING,
      plan: { ...mission.plan, steps: nextSteps, ...resolutionPatch },
      blockingIssues: [],
      stageReview: {
        capabilityId: BUILTIN_IDS.PROSPECT_DISCOVERY,
        outcome,
        outcomeLabel,
        blockingIssues: [],
        warnings: published.warnings || [],
        reviewSummary: stepResult.reviewSummary || null,
        publishedArtifacts: [
          {
            type: 'prospect_list',
            artifactId: published.artifact.id,
            revision: published.artifact.revision,
            validationStatus: published.artifact.validationStatus,
          },
        ],
        quarantinedArtifacts: [],
        operatorSupplied: true,
        discoverySkippedByResolution: discoverySkipped,
      },
      progress: {
        ...(mission.progress || {}),
        currentStage: discoverySkipped
          ? `ProspectList — ${outcomeLabel}`
          : `Discovery — ${outcomeLabel}`,
        currentCapabilityId: discoverySkipped
          ? null
          : BUILTIN_IDS.PROSPECT_DISCOVERY,
        stageOutcome: outcome,
        stageOutcomeLabel: outcomeLabel,
        counts: {
          completed: payload.prospectCount || 0,
          total:
            (mission.constraints && mission.constraints.targetCount) ||
            payload.prospectCount ||
            0,
        },
      },
      deliverables: nextDeliverables,
    });

    await this._store.appendAudit({
      missionId: mission.id,
      kind: AUDIT_KINDS.ARTIFACT_INJECTED,
      capabilityId: BUILTIN_IDS.PROSPECT_DISCOVERY,
      payload: {
        artifactId: published.artifact.id,
        artifactType: published.artifact.artifactType,
        revision: published.artifact.revision,
        producer: published.producer,
        source: published.source,
        validationStatus: published.artifact.validationStatus,
        prospectCount: payload.prospectCount,
        warnings: published.warnings || [],
        createdBy: input.createdBy || 'operator',
        provenance: published.artifact.metadata &&
          published.artifact.metadata.provenance,
        discoverySkippedByResolution: discoverySkipped,
      },
    });
    await this._store.appendAudit({
      missionId: mission.id,
      kind: AUDIT_KINDS.STAGE_SATISFIED,
      capabilityId: BUILTIN_IDS.PROSPECT_DISCOVERY,
      payload: {
        outcome,
        outcomeLabel,
        stageId: discoverySkipped
          ? 'artifact_resolution'
          : 'prospect_discovery',
        artifactId: published.artifact.id,
        discoverySkippedByResolution: discoverySkipped,
      },
    });

    if (input.execute === false) {
      return {
        mission,
        artifact: published.artifact,
        warnings: published.warnings || [],
        executed: false,
      };
    }

    mission = await this._executor.execute(mission.id);
    return {
      mission,
      artifact: published.artifact,
      warnings: published.warnings || [],
      executed: true,
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

function isDiscoveryBlocked(mission) {
  if (!mission || mission.status !== MISSION_STATUS.WAITING) return false;
  const steps = (mission.plan && mission.plan.steps) || [];
  const discovery = steps.find(
    (s) =>
      s.stageId === 'prospect_discovery' ||
      s.capabilityId === BUILTIN_IDS.PROSPECT_DISCOVERY
  );
  if (discovery && (discovery.status === 'blocked' || discovery.status === 'failed')) {
    return true;
  }
  const stage = mission.stageReview;
  if (
    stage &&
    (stage.capabilityId === BUILTIN_IDS.PROSPECT_DISCOVERY ||
      stage.capabilityId === 'prospect_discovery') &&
    (stage.outcome === STAGE_OUTCOMES.BLOCKED ||
      stage.outcome === STAGE_OUTCOMES.FAILED)
  ) {
    return true;
  }
  return false;
}

function discoveryRecoveryActions(mission) {
  if (isDiscoveryBlocked(mission)) {
    return [
      {
        id: 'retry_discovery',
        label: 'Retry Discovery',
        action: 'run_again',
      },
      {
        id: 'import_prospect_list',
        label: 'Import Prospect List',
        action: 'inject_prospect_list',
      },
      {
        id: 'cancel_mission',
        label: 'Cancel Mission',
        action: 'reject',
      },
    ];
  }

  const pending =
    mission &&
    mission.deliverables &&
    mission.deliverables.pendingOperatorImport;
  if (pending && pending.paste && !pending.autoInjectPending) {
    return [
      {
        id: 'import_prospect_list',
        label: 'Import detected Prospect List',
        action: 'inject_prospect_list',
        prefill: true,
      },
    ];
  }
  return [];
}

function workspaceActions(mission) {
  const base = ['approve', 'reject', 'edit', 'run_again'];
  if (isDiscoveryBlocked(mission)) {
    return [...base, 'import_prospect_list'];
  }
  const pending =
    mission &&
    mission.deliverables &&
    mission.deliverables.pendingOperatorImport;
  if (pending && pending.paste && !pending.autoInjectPending) {
    return [...base, 'import_prospect_list'];
  }
  return base;
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
  isDiscoveryBlocked,
  discoveryRecoveryActions,
};
