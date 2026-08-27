'use strict';

/**
 * SPEC-118 — Acquisition Mission engine.
 * Max manages missions. Capabilities contribute under contract.
 */

const {
  STAGES,
  STAGE_ORDER,
  STAGE_LABELS,
  SPECIALISTS,
  EVENT_KINDS,
  CONTRIBUTION_KINDS,
  BLOCKER_KINDS,
  clone,
  asText,
  nowIso,
  newId,
  amoError,
  round2,
} = require('./types');
const { createMission } = require('./Mission');
const { assertContract } = require('./Contracts');
const {
  nextStage,
  specialistContext,
  canEnter,
  assertActorCanProgress,
  progressPercent,
  applyStageTransition,
  derivePendingOperatorDecisionForStage,
} = require('./Lifecycle');
const { createEvent, formatTimeline } = require('./Timeline');
const { buildSharedContext, formatSharedContext } = require('./Context');
const { buildWorkspace, formatWorkspace } = require('./Workspace');
const { createBlocker, inferBlockers, currentBlocker } = require('./Blockers');
const { buildHealth, formatHealth } = require('./Health');
const { recordSegmentOutcome, summarizeLearning, formatLearning } = require('./Learning');
const {
  capturePrediction,
  resolvePrediction,
  buildOutcomeReviewSection,
  summarizeOrganizationalLearning,
  formatOutcomeLearningReport,
  isTerminalOutcomeType,
  pendingPredictionsForMission,
} = require('./OutcomeLearning');
const { explainWhy, formatExplain } = require('./Explain');
const { createObservation, formatMemory } = require('./Memory');
const {
  createCommunicationObservation,
  isCommunicationObservation,
} = require('./CommunicationObservation');
const { createMemoryAmoStore } = require('./Store');
const {
  INSPECTION_PROPERTIES,
  inspectQuestion,
  formatInspection,
} = require('./Inspection');
const {
  findLatestDiscoveryContribution,
  presentationFromDiscoveryPayload,
} = require('./DiscoveryPresentation');
const {
  assertMissionStateConsistent,
  presentableOperatorDecision,
  hasPendingPrioritizationApproval,
} = require('./PendingOperatorDecision');
const { isStructuredMissionApproved } = require('./StructuredMission');
const { OPERATOR_DECISION_KINDS } = require('./types');
const { buildExecutionReview, isExecutionApproved } = require('./ExecutionApproval');

function actorRole(actor) {
  if (!actor) return '';
  if (typeof actor === 'string') return actor.toLowerCase();
  return asText(actor.role || actor.specialist || actor.id).toLowerCase();
}

function contributionLabel(specialist, kind, payload = {}) {
  if (specialist === SPECIALISTS.SCOUT) return 'Scout completed discovery';
  if (specialist === SPECIALISTS.MAX && kind === CONTRIBUTION_KINDS.PRIORITIZATION) {
    return 'Max ranked prospects';
  }
  if (specialist === SPECIALISTS.PAIGE) {
    const variant = payload.variantLabel || payload.variant || (payload.variants && payload.variants[0] && payload.variants[0].label);
    return variant ? `Paige generated ${variant}` : 'Paige generated variants';
  }
  if (specialist === SPECIALISTS.EMMETT) return 'Emmett approved capacity';
  if (specialist === SPECIALISTS.OPERATOR && kind === CONTRIBUTION_KINDS.EDIT) {
    return payload.field === 'cta' ? 'Operator edited CTA' : 'Operator edited mission';
  }
  if (specialist === SPECIALISTS.OPERATOR && kind === CONTRIBUTION_KINDS.APPROVAL) {
    return 'Operator approved mission';
  }
  return `${specialist} contributed ${kind}`;
}

function extrasFrom(store, mission) {
  const contributions = store.listContributions(mission.id);
  const outcomes = store.listOutcomes(mission.id);
  const events = store.listEvents(mission.id);
  const learning = store.listLearning(mission.tenantId).filter((row) => !row.missionId || row.missionId === mission.id);
  const emmett = [...contributions].reverse().find((row) => row.specialist === SPECIALISTS.EMMETT);
  const capacity = emmett && emmett.payload && emmett.payload.capacity;
  const warmup = emmett && emmett.payload && (emmett.payload.warmup || emmett.payload.deliverability);
  const governor = emmett && emmett.payload && emmett.payload.governor;
  const replies = outcomes.filter((row) => row.type === 'reply').length;
  const meetings = outcomes.filter((row) => /meeting|walkthrough/.test(row.type || '')).length;
  const queued = events.some((row) => row.kind === EVENT_KINDS.QUEUED || row.kind === EVENT_KINDS.LAUNCHED)
    || outcomes.some((row) => row.type === 'queued' || row.type === 'sent');
  return {
    paigeGenerating: contributions.some((row) => row.specialist === SPECIALISTS.PAIGE && row.payload && row.payload.generating),
    deliverabilityPaused: Boolean(
      governor && (governor.outcome === 'pause' || governor.outcome === 'emergency')
    ),
    warmupRequired: Boolean(warmup && (warmup.status === 'warming' || warmup.warmup === true)),
    queuedOrLaunched: queued,
    hasOutcomes: outcomes.length > 0,
    hasLearning: learning.length > 0,
    replies,
    meetings,
    capacityRemaining: capacity && (capacity.remaining != null ? capacity.remaining : capacity.recommended),
    capacityAvailable: Boolean(capacity && (capacity.recommended || capacity.available)),
    qualifiedCount: null,
    missionId: mission.id,
  };
}

function maybeAutoAdvanceToReady(store, mission, contributions, ctx, extra) {
  if (mission.stage !== STAGES.PREPARE) return null;
  const combined = { ...ctx, ...extra, missionId: mission.id };
  const gate = canEnter(STAGES.READY, combined);
  if (!gate.ok) return null;
  const { from } = applyStageTransition(mission, STAGES.READY, { contributions });
  store.addEvent(createEvent({
    missionId: mission.id,
    kind: EVENT_KINDS.STAGE_TRANSITION,
    specialist: SPECIALISTS.MAX,
    label: `${from} → ${STAGES.READY}`,
    payload: { from, to: STAGES.READY, automatic: true },
  }));
  return { from, to: STAGES.READY };
}

function refresh(store, mission) {
  const contributions = store.listContributions(mission.id);
  const extra = extrasFrom(store, mission);
  const ctx = specialistContext(contributions, extra);
  if (extra.qualifiedCount == null) extra.qualifiedCount = ctx.prospectCount;
  maybeAutoAdvanceToReady(store, mission, contributions, ctx, extra);
  const missionExtras = { ...extra, missionId: mission.id };
  if (mission.stage === STAGES.READY) {
    if (isExecutionApproved(contributions, mission.id, missionExtras)) {
      mission.pendingOperatorDecision = null;
    } else {
      const pending = derivePendingOperatorDecisionForStage(mission, STAGES.READY, contributions);
      if (pending) {
        mission.pendingOperatorDecision = pending;
      }
    }
  }
  const refreshedCtx = specialistContext(contributions, missionExtras);
  const inferred = inferBlockers(mission, { ...refreshedCtx, ...extra });
  const manual = (mission.blockers || []).filter((row) => row.manual);
  mission.blockers = [...manual, ...inferred.filter((row) => !manual.some((m) => m.kind === row.kind))];
  mission.progressPercent = progressPercent(mission.stage, refreshedCtx);
  mission.status = STAGE_LABELS[mission.stage] || mission.status;
  mission.updatedAt = nowIso();
  return { mission: store.putMission(mission), ctx: { ...refreshedCtx, ...extra }, contributions };
}

function createAcquisitionMissionEngine(opts = {}) {
  const store = opts.store || createMemoryAmoStore();

  function create(input = {}) {
    const mission = createMission(input);
    store.putMission(mission);
    store.addEvent(createEvent({
      missionId: mission.id,
      kind: EVENT_KINDS.MISSION_CREATED,
      specialist: mission.createdBy || SPECIALISTS.MAX,
      at: mission.createdAt,
      label: 'Mission Created',
    }));
    const refreshed = refresh(store, mission);
    return refreshed.mission;
  }

  function get(id, tenantId) {
    const mission = store.getMission(id);
    if (!mission) return null;
    if (tenantId != null && String(mission.tenantId) !== String(tenantId)) {
      throw amoError('amo_tenant_mismatch', 'Mission does not belong to this tenant.');
    }
    return mission;
  }

  function requireMission(id, tenantId) {
    const mission = store.requireMission(id);
    if (tenantId != null && String(mission.tenantId) !== String(tenantId)) {
      throw amoError('amo_tenant_mismatch', 'Mission does not belong to this tenant.');
    }
    return mission;
  }

  function contribute(missionId, input = {}, contributeOpts = {}) {
    const mission = requireMission(missionId, contributeOpts.tenantId);
    const specialist = asText(input.specialist).toLowerCase();
    const payload = input.payload && typeof input.payload === 'object' ? input.payload : {};
    try {
      assertContract(specialist, payload);
    } catch (err) {
      store.addEvent(createEvent({
        missionId: mission.id,
        kind: EVENT_KINDS.CONTRACT_REJECTED,
        specialist,
        label: `Contract rejected: ${err.message}`,
        payload: { code: err.code, message: err.message },
      }));
      throw err;
    }
    const kind = asText(input.kind) || (
      specialist === SPECIALISTS.SCOUT ? CONTRIBUTION_KINDS.DISCOVERY
        : specialist === SPECIALISTS.MAX ? CONTRIBUTION_KINDS.PRIORITIZATION
          : specialist === SPECIALISTS.PAIGE ? CONTRIBUTION_KINDS.VARIANTS
            : specialist === SPECIALISTS.EMMETT ? CONTRIBUTION_KINDS.CAPACITY
              : CONTRIBUTION_KINDS.EDIT
    );
    const row = store.addContribution({
      id: asText(input.id) || newId('contrib'),
      missionId: mission.id,
      specialist,
      kind,
      payload: clone(payload),
      at: nowIso(input.at || input.now),
    });
    const label = asText(input.label) || contributionLabel(specialist, kind, payload);
    store.addEvent(createEvent({
      missionId: mission.id,
      kind: kind === CONTRIBUTION_KINDS.EDIT ? EVENT_KINDS.OPERATOR_EDIT : EVENT_KINDS.CONTRIBUTION,
      specialist,
      at: row.at,
      label,
      payload: { contributionId: row.id, kind },
    }));
    if (payload.queuedCount != null) {
      store.addEvent(createEvent({
        missionId: mission.id,
        kind: EVENT_KINDS.QUEUED,
        specialist,
        at: row.at,
        label: `${payload.queuedCount} emails queued`,
      }));
    }
    if (payload.launched === true) {
      store.addEvent(createEvent({
        missionId: mission.id,
        kind: EVENT_KINDS.LAUNCHED,
        specialist,
        at: row.at,
        label: 'Campaign launched',
      }));
    }
    if (specialist === SPECIALISTS.SCOUT && kind === CONTRIBUTION_KINDS.DISCOVERY) {
      if (payload.confidence != null) {
        mission.confidence = round2(payload.confidence);
      }
      const mir = payload.missionIntelligenceReport || payload.missionReport;
      if (mir && mir.recommendation) {
        const prediction = capturePrediction({
          tenantId: mission.tenantId,
          missionId: mission.id,
          recommendation: mir.recommendation,
          strategicDecision: mir.strategicDecision,
          opportunity: mir.opportunityIntelligence?.topOpportunity || mir.topOpportunities?.[0],
          judgmentResult: mir.judgmentResult,
          expectedOutcome: mir.strategicDecision?.expectedBusinessOutcome,
          confidence: mir.recommendation.confidence ?? mir.strategicDecision?.tradeoff?.confidencePercent,
        });
        store.addPrediction(prediction);
        store.addEvent(createEvent({
          missionId: mission.id,
          kind: EVENT_KINDS.LEARNING,
          specialist: SPECIALISTS.MAX,
          label: 'Prediction captured',
          payload: { predictionId: prediction.id, summary: prediction.recommendation.summary },
        }));
      }
      if (mission.stage === STAGES.DISCOVER && isStructuredMissionApproved(mission)) {
        mission.pendingOperatorDecision = {
          stage: STAGES.DISCOVER,
          kind: OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL,
          prompt: 'Approve prioritization?',
        };
      }
      store.putMission(mission);
    }
    refresh(store, mission);
    return { mission: store.getMission(mission.id), contribution: row };
  }

  function progress(missionId, actor, progressOpts = {}) {
    assertActorCanProgress(actor);
    const mission = requireMission(missionId, progressOpts.tenantId);
    const contributions = store.listContributions(mission.id);
    const extra = extrasFrom(store, mission);
    const ctx = specialistContext(contributions, extra);
    const target = asText(progressOpts.stage).toLowerCase() || nextStage(mission.stage);
    if (stageIndexSafe(target) <= stageIndexSafe(mission.stage) && !progressOpts.allowSame) {
      throw amoError('amo_already_at_stage', `Mission is already at ${mission.stage}.`);
    }
    const gate = canEnter(target, { ...ctx, ...extra });
    if (!gate.ok) throw amoError('amo_stage_blocked', gate.reason);
    if (
      target === STAGES.UNDERSTAND &&
      mission.stage === STAGES.DISCOVER &&
      hasPendingPrioritizationApproval({ mission, contributions })
    ) {
      throw amoError(
        'amo_prioritization_pending',
        'Prioritization approval must be consumed before Understanding.'
      );
    }
    const { from } = applyStageTransition(mission, target, { contributions });
    store.addEvent(createEvent({
      missionId: mission.id,
      kind: EVENT_KINDS.STAGE_TRANSITION,
      specialist: actorRole(actor) || SPECIALISTS.MAX,
      label: `${from} → ${target}`,
      payload: { from, to: target },
    }));
    refresh(store, mission);
    return store.getMission(mission.id);
  }

  function setBlocker(missionId, input = {}, blockerOpts = {}) {
    const mission = requireMission(missionId, blockerOpts.tenantId);
    const blocker = createBlocker({ ...input, manual: true });
    blocker.manual = true;
    mission.blockers = [...(mission.blockers || []).filter((row) => row.kind !== blocker.kind), blocker];
    store.addEvent(createEvent({
      missionId: mission.id,
      kind: EVENT_KINDS.BLOCKER_SET,
      specialist: input.specialist || null,
      label: blocker.label,
      payload: { kind: blocker.kind },
    }));
    store.putMission(mission);
    refresh(store, mission);
    return store.getMission(mission.id);
  }

  function clearBlocker(missionId, kind, clearOpts = {}) {
    const mission = requireMission(missionId, clearOpts.tenantId);
    const before = (mission.blockers || []).length;
    mission.blockers = (mission.blockers || []).filter((row) => row.kind !== kind);
    if (mission.blockers.length !== before) {
      store.addEvent(createEvent({
        missionId: mission.id,
        kind: EVENT_KINDS.BLOCKER_CLEARED,
        label: `Cleared ${kind}`,
        payload: { kind },
      }));
    }
    store.putMission(mission);
    refresh(store, mission);
    return store.getMission(mission.id);
  }

  function recordObservation(missionId, input = {}, obsOpts = {}) {
    const mission = requireMission(missionId, obsOpts.tenantId);
    const row = store.addObservation(createObservation({ ...input, missionId: mission.id }));
    store.addEvent(createEvent({
      missionId: mission.id,
      kind: EVENT_KINDS.OBSERVATION,
      specialist: row.specialist,
      at: row.at,
      label: `${row.specialist} observed`,
      payload: isCommunicationObservation(row)
        ? {
          kind: row.kind,
          category: row.category,
          eventType: row.eventType,
          prospectId: row.prospectId,
          evidence: row.evidence,
        }
        : { observation: row.observation },
    }));
    return row;
  }

  function recordCommunicationObservation(missionId, providerEvent = {}, obsOpts = {}) {
    const structured = createCommunicationObservation(providerEvent);
    if (!structured) return null;
    const mission = requireMission(missionId, obsOpts.tenantId);
    const existing = store.listObservations(mission.id).find((row) => row.id === structured.id);
    if (existing) return existing;
    return recordObservation(mission.id, structured, obsOpts);
  }

  function recordOutcome(missionId, input = {}, outcomeOpts = {}) {
    const mission = requireMission(missionId, outcomeOpts.tenantId);
    const row = store.addOutcome({
      id: asText(input.id) || newId('out'),
      missionId: mission.id,
      tenantId: mission.tenantId,
      type: asText(input.type || input.outcomeType),
      segment: asText(input.segment || input.vertical) || null,
      prospectId: input.prospectId || null,
      at: nowIso(input.at || input.now),
      payload: clone(input.payload || {}),
    });
    const labels = {
      open: 'First open',
      reply: 'Reply received',
      meeting_booked: 'Walkthrough booked',
      walkthrough_booked: 'Walkthrough booked',
      queued: `${(input.payload && input.payload.count) || ''} emails queued`.trim(),
    };
    store.addEvent(createEvent({
      missionId: mission.id,
      kind: row.type === 'queued' ? EVENT_KINDS.QUEUED : EVENT_KINDS.OUTCOME,
      specialist: input.specialist || null,
      at: row.at,
      label: asText(input.label) || labels[row.type] || `Outcome: ${row.type}`,
    }));
    if (isTerminalOutcomeType(row.type)) {
      autoEvaluatePendingPredictions(mission, row, input);
    }
    refresh(store, mission);
    return row;
  }

  function autoEvaluatePendingPredictions(mission, outcomeRow, input = {}) {
    const pending = pendingPredictionsForMission(store.listPredictions(mission.id), mission.id);
    for (const prediction of pending) {
      const result = resolvePrediction(prediction, {
        actualOutcome: outcomeRow.type,
        at: outcomeRow.at,
        prospectId: outcomeRow.prospectId,
        notes: input.notes || (outcomeRow.payload && outcomeRow.payload.notes),
        primaryCause: input.primaryCause,
        secondaryCause: input.secondaryCause,
        lesson: input.lesson,
      });
      store.putPrediction(result.prediction);
      store.addEvaluation(result.evaluation);
      for (const learning of result.learnings) store.addOutcomeLearning(learning);
      store.addEvent(createEvent({
        missionId: mission.id,
        kind: EVENT_KINDS.LEARNING,
        specialist: SPECIALISTS.MAX,
        at: result.evaluation.evaluatedAt,
        label: `Outcome evaluated: ${result.evaluation.accuracy}`,
        payload: {
          evaluationId: result.evaluation.id,
          predictionId: prediction.id,
          accuracy: result.evaluation.accuracy,
          autoApplied: false,
        },
      }));
    }
  }

  function captureMissionPrediction(missionId, input = {}, opts = {}) {
    const mission = requireMission(missionId, opts.tenantId);
    const prediction = capturePrediction({
      ...input,
      tenantId: mission.tenantId,
      missionId: mission.id,
    });
    store.addPrediction(prediction);
    store.addEvent(createEvent({
      missionId: mission.id,
      kind: EVENT_KINDS.LEARNING,
      specialist: SPECIALISTS.MAX,
      label: 'Prediction captured',
      payload: { predictionId: prediction.id, summary: prediction.recommendation.summary },
    }));
    return prediction;
  }

  function evaluateOutcomeLearning(missionId, input = {}, opts = {}) {
    const mission = requireMission(missionId, opts.tenantId);
    const predictionId = asText(input.predictionId);
    const pending = pendingPredictionsForMission(store.listPredictions(mission.id), mission.id);
    const prediction = predictionId
      ? pending.find((p) => p.id === predictionId) || store.listPredictions(mission.id).find((p) => p.id === predictionId)
      : pending[0];
    if (!prediction) {
      throw amoError('amo_prediction_not_found', 'No pending prediction to evaluate.');
    }
    const result = resolvePrediction(prediction, input);
    store.putPrediction(result.prediction);
    store.addEvaluation(result.evaluation);
    for (const learning of result.learnings) store.addOutcomeLearning(learning);
    store.addEvent(createEvent({
      missionId: mission.id,
      kind: EVENT_KINDS.LEARNING,
      specialist: SPECIALISTS.MAX,
      at: result.evaluation.evaluatedAt,
      label: `Outcome evaluated: ${result.evaluation.accuracy}`,
      payload: {
        evaluationId: result.evaluation.id,
        predictionId: prediction.id,
        accuracy: result.evaluation.accuracy,
        autoApplied: false,
      },
    }));
    refresh(store, mission);
    return result;
  }

  function recordLearning(missionId, input = {}, learnOpts = {}) {
    const mission = missionId ? requireMission(missionId, learnOpts.tenantId) : null;
    const row = recordSegmentOutcome({
      ...input,
      tenantId: (mission && mission.tenantId) || input.tenantId,
      missionId: mission ? mission.id : input.missionId || null,
    });
    row.autoApplied = false;
    store.addLearning(row);
    if (mission) {
      store.addEvent(createEvent({
        missionId: mission.id,
        kind: EVENT_KINDS.LEARNING,
        label: `Learning: ${row.segment}`,
        payload: { learningId: row.id, autoApplied: false },
      }));
      refresh(store, mission);
    }
    return row;
  }

  function inspect(missionId, inspectOpts = {}) {
    const mission = requireMission(missionId, inspectOpts.tenantId);
    const contributions = store.listContributions(mission.id);
    const extra = extrasFrom(store, mission);
    const ctx = specialistContext(contributions, extra);
    const learningRows = store.listLearning(mission.tenantId);
    const learning = summarizeLearning(learningRows);
    const outcomeLearning = buildOutcomeReviewSection({
      predictions: store.listPredictions(mission.id),
      evaluations: store.listEvaluations(mission.id),
      outcomeLearnings: store.listOutcomeLearnings(mission.tenantId, mission.id),
      allowPending: true,
    });
    const explainExtras = {
      ...extra,
      previousReplyRate: inspectOpts.previousReplyRate,
      objectiveReason: inspectOpts.objectiveReason,
      qualifiedCount: extra.qualifiedCount || ctx.prospectCount,
    };
    const health = buildHealth(mission, { ...ctx, ...extra, learningSummary: learning.learningSummary }, {
      replies: extra.replies,
      meetings: extra.meetings,
      capacityRemaining: extra.capacityRemaining,
      learning: learning.learningSummary,
    });
    const workspace = buildWorkspace(mission, ctx);
    const context = buildSharedContext(mission, contributions);
    const timeline = formatTimeline(store.listEvents(mission.id));
    const why = explainWhy(mission, contributions, explainExtras);
    const discoveryContribution = findLatestDiscoveryContribution(contributions);
    const discoveryArtifact = discoveryContribution
      ? presentationFromDiscoveryPayload(discoveryContribution.payload || {})
      : null;
    assertMissionStateConsistent(mission, { contributions });
    const progression = require('./MissionProgression');
    const { buildWorkspaceContext } = require('./WorkspaceMode');
    const progressionSnapshot = {
      mission,
      contributions,
      progression: {
        stage: progression.deriveProgressionStage({ mission, contributions }),
        pause: progression.deriveMissionPause({ mission, contributions }),
        block: progression.deriveExecutionBlock({ mission, contributions }),
        presentation: progression.formatMissionProgressPresentation({ mission, contributions }),
      },
    };
    const workspaceContext = buildWorkspaceContext({
      missionId: mission.id,
      snapshot: progressionSnapshot,
    });
    return {
      spec: 'SPEC-118',
      mission,
      workspaceContext,
      executableDecision: presentableOperatorDecision({ mission, contributions }),
      executionReview: mission.stage === STAGES.READY
        ? buildExecutionReview(mission, contributions)
        : null,
      workspace,
      health,
      context,
      timeline,
      why,
      learning,
      outcomeLearning,
      observations: formatMemory(store.listObservations(mission.id)),
      contributions,
      outcomes: store.listOutcomes(mission.id),
      executionRecords: store.listExecutionRecords
        ? store.listExecutionRecords(mission.id)
        : [],
      blocker: currentBlocker(mission.blockers),
      discoveryArtifact,
      progression: progressionSnapshot.progression,
    };
  }

  function answerOperator(question, input = {}) {
    const tenantId = asText(input.tenantId);
    if (!tenantId) throw amoError('amo_tenant_required', 'tenantId is required.');
    const missions = store.listMissions(tenantId);
    const mission = input.missionId
      ? requireMission(input.missionId, tenantId)
      : missions[0];
    if (!mission) {
      return {
        kind: 'no_mission',
        prose: 'No acquisition mission is on file for this workspace.',
        invented: false,
      };
    }
    const snapshot = inspect(mission.id, { tenantId, ...input });
    const inspection = inspectQuestion(question, snapshot, {
      logger: input.inspectionLogger,
      silent: input.silentInspection === true,
    });

    if (inspection && inspection.resolved) {
      if (inspection.kind === 'explain') {
        return {
          kind: 'explain',
          prose: formatExplain(snapshot.why),
          structured: snapshot.why,
          mission,
          missionContext: inspection.missionContext,
          inspection: {
            property: inspection.property,
            pipeline: inspection.pipeline,
            resolved: true,
          },
          invented: false,
        };
      }
      if (inspection.kind === 'workspace') {
        return {
          kind: 'workspace',
          prose: formatWorkspace(snapshot.workspace),
          structured: snapshot.workspace,
          mission,
          missionContext: inspection.missionContext,
          inspection: {
            property: inspection.property,
            pipeline: inspection.pipeline,
            resolved: true,
          },
          invented: false,
        };
      }
      if (inspection.property === INSPECTION_PROPERTIES.HEALTH) {
        const healthExplain = inspection.structured;
        return {
          kind: 'health',
          prose: `${formatHealth(snapshot.health)}\n\n${formatInspection(healthExplain)}`.trim(),
          structured: { ...snapshot.health, derivation: healthExplain },
          mission,
          missionContext: inspection.missionContext,
          inspection: {
            property: inspection.property,
            pipeline: inspection.pipeline,
            resolved: true,
          },
          invented: false,
        };
      }
      if (inspection.property === INSPECTION_PROPERTIES.BLOCKER) {
        const blockerExplain = inspection.structured;
        const blocker = snapshot.blocker;
        return {
          kind: 'blocker',
          prose: blockerExplain.prose || formatInspection(blockerExplain),
          structured: blocker ? { ...blocker, derivation: blockerExplain } : blockerExplain,
          mission,
          missionContext: inspection.missionContext,
          inspection: {
            property: inspection.property,
            pipeline: inspection.pipeline,
            resolved: true,
          },
          invented: false,
        };
      }
      return {
        kind: 'inspection',
        prose: inspection.prose,
        structured: inspection.structured,
        mission,
        missionContext: inspection.missionContext,
        inspection: {
          property: inspection.property,
          pipeline: inspection.pipeline,
          resolved: true,
        },
        invented: false,
      };
    }

    if (inspection && inspection.kind === 'fallback') {
      return {
        kind: 'inspection_fallback',
        prose: null,
        structured: inspection.missionContext,
        mission,
        inspection: {
          property: null,
          pipeline: inspection.pipeline,
          resolved: false,
          reason: inspection.reason,
        },
        invented: false,
      };
    }

    return {
      kind: 'workspace',
      prose: formatWorkspace(snapshot.workspace),
      structured: snapshot.workspace,
      mission,
      invented: false,
    };
  }

  return {
    store,
    create,
    get,
    require: requireMission,
    requireMission,
    list: (tenantId) => store.listMissions(tenantId),
    contribute,
    progress,
    setBlocker,
    clearBlocker,
    recordObservation,
    recordCommunicationObservation,
    recordOutcome,
    recordLearning,
    capturePrediction: captureMissionPrediction,
    evaluateOutcomeLearning,
    outcomeLearning: (id, o) => inspect(id, o).outcomeLearning,
    inspect,
    workspace: (id, o) => inspect(id, o).workspace,
    health: (id, o) => inspect(id, o).health,
    context: (id, o) => inspect(id, o).context,
    timeline: (id, o) => inspect(id, o).timeline,
    explainWhy: (id, o) => inspect(id, o).why,
    learning: (tenantId) => ({
      ...summarizeLearning(store.listLearning(tenantId)),
      outcomeLearning: summarizeOrganizationalLearning(
        store.listEvaluations(null).filter((row) => String(row.tenantId) === String(tenantId)),
        store.listOutcomeLearnings(tenantId)
      ),
    }),
    formatOutcomeLearningReport,
    answerOperator,
    formatWorkspace,
    formatHealth,
    formatExplain,
    formatLearning,
    formatSharedContext,
    BLOCKER_KINDS,
    STAGES,
  };
}

function stageIndexSafe(stage) {
  const idx = STAGE_ORDER.indexOf(stage);
  return idx < 0 ? 0 : idx;
}

module.exports = {
  createAcquisitionMissionEngine,
  round2,
};
