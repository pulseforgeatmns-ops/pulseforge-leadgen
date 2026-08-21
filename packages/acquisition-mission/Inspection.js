'use strict';

/**
 * SPEC-122 — Mission Context Inspection.
 * Active mission state is authoritative for mission-owned properties.
 * Operators inspect work; no durable business knowledge retrieval required.
 */

const {
  STAGES,
  STAGE_ORDER,
  STAGE_LABELS,
  STAGE_PROGRESS_BASE,
  SPECIALISTS,
  round2,
} = require('./types');
const { specialistContext, progressPercent } = require('./Lifecycle');
const { currentBlocker } = require('./Blockers');
const { collectEvidence } = require('./Explain');

const INSPECTION_PROPERTIES = Object.freeze({
  PROGRESS: 'progress',
  STAGE: 'stage',
  HEALTH: 'health',
  CONFIDENCE: 'confidence',
  WAITING: 'waiting',
  BLOCKER: 'blocker',
  TIMELINE: 'timeline',
  NEXT: 'next',
  RECOMMENDATION: 'recommendation',
  EXPLAIN: 'explain',
  WORKSPACE: 'workspace',
});

const PLANNING_REQUIREMENTS = Object.freeze([
  {
    key: 'targetCustomer',
    label: 'Target customer defined',
    check: (mission) =>
      Boolean(mission.targetSegment) ||
      Boolean(mission.structuredMission && mission.structuredMission.market && mission.structuredMission.market.segment),
  },
  {
    key: 'geography',
    label: 'Geography confirmed',
    check: (mission) =>
      Boolean(mission.structuredMission && mission.structuredMission.geography && mission.structuredMission.geography.region) ||
      /manchester|charleston|nashville|location|area/i.test(String(mission.objective || '')),
  },
  {
    key: 'objective',
    label: 'Business objective defined',
    check: (mission) =>
      Boolean(mission.structuredMission && mission.structuredMission.objective) ||
      Boolean(mission.objective),
  },
  {
    key: 'planApproved',
    label: 'Mission plan approved',
    check: (mission) =>
      Boolean(mission.structuredMission && mission.structuredMission.immutable) ||
      !mission.missionPlanDraft,
  },
]);

function capitalize(value) {
  const text = String(value || '');
  return text ? text.charAt(0).toUpperCase() + text.slice(1) : text;
}

function referencesMissionState(question) {
  const q = String(question || '').trim().toLowerCase();
  if (!q) return false;
  return (
    /\b(?:why|what).*\b(?:progress|mission progress)\b/.test(q) ||
    /\bprogress\b.*\bbased\b|\bwhat is the \d+\s*%/.test(q) ||
    /\d+\s*%\s*(?:progress|complete|done)\b/.test(q) ||
    /\b(?:why|what).*\b(?:stage|phase|discovery|planning|preparing|executing)\b/.test(q) ||
    /\b(?:why|what).*\bwait(?:ing)?\b|\bwaiting\s+(?:on|for)\b|\bwhat are we waiting\b/.test(q) ||
    /\b(?:why|how).*\bconfidence\b|\bconfidence\b.*\b(?:why|based|from)\b/.test(q) ||
    /\b(?:why|how).*\b(?:mission )?health\b|\bmission health\b|\bhow is outreach\b/.test(q) ||
    /\bwhat(?:'s| is) blocking\b|\bblocker\b|\bwhy (?:aren'?t|isn'?t) (?:we|this mission|the mission) (?:moving|progressing)\b/.test(q) ||
    /\bwhat changed\b|\bwhat(?:'s| has) changed\b/.test(q) ||
    /\bwhat happens next\b|\bnext step\b|\bwhat(?:'s| is) next\b/.test(q) ||
    /\bwhy this recommendation\b/.test(q) ||
    /why is this mission|why (?:does|do) this mission exist|why are we (?:doing|running) this mission|how is (?:the )?mission\b|mission workspace|where are we\b|mission progress|mission status/.test(q)
  );
}

function classifyInspectionQuestion(question) {
  const q = String(question || '').trim().toLowerCase();
  if (!q) return null;

  if (/why is this mission|why (?:does|do) this mission exist|why are we (?:doing|running) this\b/.test(q)) {
    return INSPECTION_PROPERTIES.EXPLAIN;
  }
  if (/\b(?:what|why).*\bprogress\b|\bprogress\b.*\bbased\b|\d+\s*%\s*progress|\bwhat is the \d+%/.test(q)) {
    return INSPECTION_PROPERTIES.PROGRESS;
  }
  if (/\bwhy (?:are we|is (?:the mission|this)) in\b|\bwhy.*\b(?:stage|phase|discovery|planning|preparing)\b|\bcurrent phase\b/.test(q)) {
    return INSPECTION_PROPERTIES.STAGE;
  }
  if (/\bwhy.*\bconfidence\b|\bconfidence\b.*\b(?:why|based|from)\b|\bwhy is confidence\b/.test(q)) {
    return INSPECTION_PROPERTIES.CONFIDENCE;
  }
  if (/\bwhy.*\bhealth\b|\bwhy is health\b|\bhow is (?:the )?mission\b|\bhow is outreach\b|\bmission health\b/.test(q)) {
    return INSPECTION_PROPERTIES.HEALTH;
  }
  if (/\b(?:why|what).*\bwait(?:ing)?\b|\bwaiting\s+(?:on|for)\b|\bwhat are we waiting\b/.test(q)) {
    return INSPECTION_PROPERTIES.WAITING;
  }
  if (/\bwhat(?:'s| is) blocking\b|\bblocker\b|\bwhy (?:aren'?t|isn'?t) (?:we|this) (?:moving|progressing)\b/.test(q)) {
    return INSPECTION_PROPERTIES.BLOCKER;
  }
  if (/\bwhat changed\b|\bwhat(?:'s| has) changed\b|\bwhat(?:'s| is) new\b/.test(q)) {
    return INSPECTION_PROPERTIES.TIMELINE;
  }
  if (/\bwhat happens next\b|\bnext step\b|\bwhat(?:'s| is) next\b|\bwhat comes next\b/.test(q)) {
    return INSPECTION_PROPERTIES.NEXT;
  }
  if (/\bwhy this recommendation\b|\bwhy (?:that|this) recommend/.test(q)) {
    return INSPECTION_PROPERTIES.RECOMMENDATION;
  }
  if (/mission workspace|mission status|mission progress|where are we\b/.test(q)) {
    return INSPECTION_PROPERTIES.WORKSPACE;
  }
  return null;
}

function resolveExecutor(ctx, mission) {
  if (!ctx.scoutComplete) return { current: 'ScoutDiscoveryExecutor', next: 'ScoutDiscoveryExecutor' };
  if (!ctx.maxComplete && mission.stage === STAGES.PLAN) {
    return { current: 'MaxPrioritizationExecutor', next: 'MaxPrioritizationExecutor' };
  }
  if (!ctx.paigeComplete && (mission.stage === STAGES.PREPARE || mission.stage === STAGES.PLAN)) {
    return { current: 'PaigeVariantExecutor', next: 'PaigeVariantExecutor' };
  }
  if (ctx.paigeComplete && !ctx.emmettComplete) {
    return { current: 'EmmettCapacityExecutor', next: 'EmmettCapacityExecutor' };
  }
  if (!ctx.operatorApproved && (mission.stage === STAGES.READY || mission.stage === STAGES.PREPARE)) {
    return { current: 'OperatorApprovalGate', next: 'OperatorApprovalGate' };
  }
  if (mission.stage === STAGES.EXECUTE) {
    return { current: 'EmmettSendExecutor', next: 'EmmettSendExecutor' };
  }
  return { current: 'MaxOrchestrator', next: 'MaxOrchestrator' };
}

function buildMissionContext(snapshot) {
  const mission = snapshot.mission || {};
  const workspace = snapshot.workspace || {};
  const health = snapshot.health || {};
  const blocker = snapshot.blocker || currentBlocker(mission.blockers || []);
  const contributions = snapshot.contributions || [];
  const ctx = specialistContext(contributions, {});
  const executors = resolveExecutor(ctx, mission);

  return {
    spec: 'SPEC-122',
    missionId: mission.id,
    stage: mission.stage,
    stageLabel: STAGE_LABELS[mission.stage] || mission.status,
    progress: mission.progressPercent,
    progressBasis: explainProgressBasis(mission, ctx).summary,
    health: health.label || health.status || 'Healthy',
    healthBasis: explainHealthBasis(mission, snapshot, ctx).summary,
    waitingOn: blocker ? blocker.label : null,
    confidence: round2(mission.confidence),
    confidenceBasis: explainConfidenceBasis(mission, snapshot).summary,
    currentExecutor: executors.current,
    nextExecutor: executors.next,
    status: mission.status,
    blockers: mission.blockers || [],
    timeline: snapshot.timeline || [],
  };
}

function completedStages(stage) {
  const idx = STAGE_ORDER.indexOf(stage);
  return idx < 0 ? 0 : idx + 1;
}

function explainProgressBasis(mission, ctx) {
  const base = STAGE_PROGRESS_BASE[mission.stage] != null ? STAGE_PROGRESS_BASE[mission.stage] : 8;
  const bonuses = [];
  if (ctx.scoutComplete) bonuses.push({ label: 'Scout discovery complete', points: 12 });
  if (ctx.maxComplete) bonuses.push({ label: 'Max prioritization complete', points: 8 });
  if (ctx.paigeComplete) bonuses.push({ label: 'Paige variants ready', points: 8 });
  if (ctx.emmettComplete) bonuses.push({ label: 'Emmett capacity approved', points: 6 });
  if (ctx.operatorApproved) bonuses.push({ label: 'Operator approval recorded', points: 6 });

  const planningComplete = PLANNING_REQUIREMENTS.filter((req) => req.check(mission));
  const planningRemaining = PLANNING_REQUIREMENTS.filter((req) => !req.check(mission));

  const stageActivities = STAGE_ORDER.map((stage) => ({
    stage,
    label: STAGE_LABELS[stage],
    complete: STAGE_ORDER.indexOf(stage) < STAGE_ORDER.indexOf(mission.stage),
    current: stage === mission.stage,
  }));

  const bonusTotal = bonuses.reduce((sum, row) => sum + row.points, 0);
  const computed = progressPercent(mission.stage, ctx);

  return {
    property: INSPECTION_PROPERTIES.PROGRESS,
    value: mission.progressPercent,
    summary: `${planningComplete.length} of ${PLANNING_REQUIREMENTS.length} planning requirements satisfied; ${completedStages(mission.stage)} of ${STAGE_ORDER.length} stages entered`,
    derivedFrom: [
      { label: 'Stage base progress', detail: `${STAGE_LABELS[mission.stage] || mission.stage} base = ${base}%` },
      ...bonuses.map((row) => ({ label: row.label, detail: `+${row.points}%` })),
      { label: 'Computed total', detail: `${base}% + ${bonusTotal}% bonus = ${computed}% (capped before next stage)` },
    ],
    planning: {
      complete: planningComplete.map((row) => row.label),
      remaining: planningRemaining.map((row) => row.label),
    },
    stages: stageActivities,
    headline: 'Mission Progress',
  };
}

function explainConfidenceBasis(mission, snapshot) {
  const contributions = snapshot.contributions || [];
  const learning = snapshot.learning || {};
  const factors = [];
  const scoutDiscovery = [...contributions]
    .reverse()
    .find((row) => row.specialist === SPECIALISTS.SCOUT && row.kind === 'discovery');
  const scoutPayload = scoutDiscovery && scoutDiscovery.payload ? scoutDiscovery.payload : {};
  const breakdown = scoutPayload.confidenceBreakdown;

  if (mission.targetSegment) {
    factors.push({ label: 'Target definition', detail: `Segment: ${mission.targetSegment}` });
  }
  if (mission.objective) {
    factors.push({ label: 'Mission objective', detail: 'Operator objective recorded at mission creation' });
  }
  if (/manchester|charleston|nashville|location|area|hooksett|auburn/i.test(String(mission.objective || ''))) {
    factors.push({ label: 'Known geography', detail: 'Service area captured in mission objective' });
  }
  if (scoutDiscovery) {
    factors.push({
      label: 'Scout discovery evidence',
      detail: breakdown
        ? `Overall ${breakdown.overall}; evidence ${breakdown.evidence}; fit ${breakdown.fit}`
        : 'Discovery contribution attached to mission',
    });
    if (breakdown) {
      factors.push({ label: 'Discovery confidence', detail: String(breakdown.discovery) });
      factors.push({ label: 'Evidence confidence', detail: String(breakdown.evidence) });
      factors.push({ label: 'Market confidence', detail: String(breakdown.market) });
      factors.push({ label: 'Fit confidence', detail: String(breakdown.fit) });
      factors.push({ label: 'Completeness', detail: String(breakdown.completeness) });
    }
  }
  if (learning.segments && learning.segments.length) {
    factors.push({ label: 'Historical evidence', detail: `${learning.segments.length} segment learning record(s) on file` });
  } else if (snapshot.why && snapshot.why.reasons && snapshot.why.reasons.some((r) => /reply rate/i.test(r))) {
    factors.push({ label: 'Historical evidence', detail: 'Prior campaign reply rate referenced' });
  }
  if (!snapshot.outcomes || !snapshot.outcomes.length) {
    factors.push({ label: 'No campaign results yet', detail: 'Confidence reflects planning evidence, not live send outcomes' });
  }

  const value =
    breakdown && breakdown.overall != null
      ? round2(breakdown.overall)
      : scoutPayload.confidence != null
        ? round2(scoutPayload.confidence)
        : round2(mission.confidence);

  return {
    property: INSPECTION_PROPERTIES.CONFIDENCE,
    value,
    summary: factors.map((row) => row.label).join('; ') || 'Mission creation inputs',
    derivedFrom: factors,
    headline: 'Confidence',
    breakdown: breakdown || null,
  };
}

function explainHealthBasis(mission, snapshot, ctx) {
  const health = snapshot.health || {};
  const blocker = snapshot.blocker || currentBlocker(mission.blockers || []);
  const factors = [];

  if (!blocker) {
    factors.push({ label: 'No blockers', detail: 'No active blocker on mission record' });
  } else if (/operator/i.test(blocker.kind || blocker.label || '')) {
    factors.push({ label: 'Waiting only on operator approval', detail: blocker.label });
  } else {
    factors.push({ label: 'Active blocker', detail: blocker.label });
  }

  factors.push({ label: 'Mission active', detail: `Stage: ${STAGE_LABELS[mission.stage] || mission.status}` });

  const inputsComplete =
    Boolean(mission.objective) &&
    Boolean(mission.targetSegment) &&
    (ctx.scoutComplete || mission.stage === STAGES.DISCOVER);
  if (inputsComplete) {
    factors.push({ label: 'Required inputs complete', detail: 'Objective, segment, and discovery path satisfied for current stage' });
  }

  return {
    property: INSPECTION_PROPERTIES.HEALTH,
    value: health.status || health.label || 'Healthy',
    summary: factors.map((row) => row.label).join('; '),
    derivedFrom: factors,
    headline: 'Health',
  };
}

function explainWaiting(snapshot) {
  const blocker = snapshot.blocker || currentBlocker((snapshot.mission || {}).blockers || []);
  const workspace = snapshot.workspace || {};
  const waitingSpecialist = (workspace.specialists || []).find(
    (row) => row.state === 'waiting' || row.state === 'generating' || row.state === 'approval_required'
  );

  return {
    property: INSPECTION_PROPERTIES.WAITING,
    value: blocker ? blocker.label : (waitingSpecialist ? waitingSpecialist.label : 'Nothing'),
    summary: blocker ? blocker.reason || blocker.label : 'Mission can proceed without waiting',
    derivedFrom: blocker
      ? [{ label: blocker.label, detail: blocker.reason || blocker.label }]
      : waitingSpecialist
        ? [{ label: capitalize(waitingSpecialist.id), detail: waitingSpecialist.label }]
        : [{ label: 'No wait state', detail: 'All specialists complete for current stage' }],
    headline: 'Waiting On',
  };
}

function explainStage(snapshot) {
  const mission = snapshot.mission || {};
  const contributions = snapshot.contributions || [];
  const ctx = specialistContext(contributions, {});

  const reasons = [];
  if (mission.stage === STAGES.DISCOVER) {
    reasons.push('Mission opened in Discover to gather qualified prospects.');
  }
  if (mission.stage === STAGES.UNDERSTAND && ctx.scoutComplete) {
    reasons.push('Scout discovery completed; mission advanced to Understand.');
  }
  if (mission.stage === STAGES.PLAN && ctx.maxHasObjectives) {
    reasons.push('Max objectives recorded; mission is in Plan.');
  }
  if (mission.stage === STAGES.PREPARE) {
    reasons.push('Prioritization complete; Prepare stage waits for Paige variants and Emmett capacity.');
  }
  if (mission.stage === STAGES.READY) {
    reasons.push('Variants and capacity ready; awaiting operator approval before Execute.');
  }
  if (!reasons.length) {
    reasons.push(`Current stage is ${STAGE_LABELS[mission.stage] || mission.stage} based on lifecycle position.`);
  }

  return {
    property: INSPECTION_PROPERTIES.STAGE,
    value: STAGE_LABELS[mission.stage] || mission.stage,
    summary: reasons[0],
    derivedFrom: reasons.map((reason) => ({ label: 'Stage gate', detail: reason })),
    headline: 'Current Phase',
  };
}

function explainBlocker(snapshot) {
  const blocker = snapshot.blocker || currentBlocker((snapshot.mission || {}).blockers || []);
  return {
    property: INSPECTION_PROPERTIES.BLOCKER,
    value: blocker ? blocker.label : 'None',
    summary: blocker ? blocker.reason || blocker.label : 'No blocker. The mission can proceed.',
    derivedFrom: blocker
      ? [{ label: blocker.label, detail: blocker.reason || blocker.label }]
      : [{ label: 'Clear', detail: 'No inferred or manual blockers' }],
    headline: 'Blocker',
  };
}

function explainTimeline(snapshot) {
  const timeline = snapshot.timeline || [];
  const recent = timeline.slice(-5);
  return {
    property: INSPECTION_PROPERTIES.TIMELINE,
    value: recent.length ? recent[recent.length - 1].label : 'No events',
    summary: recent.length
      ? `${recent.length} recent mission event(s) on timeline`
      : 'No timeline events recorded yet',
    derivedFrom: recent.map((row) => ({ label: row.clock || row.at, detail: row.label })),
    headline: 'What Changed',
  };
}

function explainNext(snapshot) {
  const mission = snapshot.mission || {};
  const workspace = snapshot.workspace || {};
  const blocker = snapshot.blocker;
  let next = 'Continue in mission workspace.';
  if (blocker) {
    next = `Resolve: ${blocker.label}`;
  } else if (workspace.paige && workspace.paige.state === 'generating') {
    next = 'Paige completes variant generation, then Emmett reviews capacity.';
  } else if (workspace.operator && workspace.operator.state === 'approval_required') {
    next = 'Operator approval required before Execute.';
  } else if (workspace.scout && workspace.scout.state === 'waiting') {
    next = 'Scout discovery must complete before Understand.';
  } else if (mission.stage === STAGES.EXECUTE) {
    next = 'Emmett queues and launches approved sends.';
  }

  const executors = resolveExecutor(specialistContext(snapshot.contributions || [], {}), mission);
  return {
    property: INSPECTION_PROPERTIES.NEXT,
    value: next,
    summary: next,
    derivedFrom: [
      { label: 'Current executor', detail: executors.current },
      { label: 'Next executor', detail: executors.next },
    ],
    headline: 'What Happens Next',
  };
}

function explainRecommendation(snapshot) {
  const learning = snapshot.learning || {};
  const max = [...(snapshot.contributions || [])].reverse().find((row) => row.specialist === SPECIALISTS.MAX);
  const recommendations = [];
  if (learning.recommendation) recommendations.push(learning.recommendation);
  if (max && max.payload && Array.isArray(max.payload.recommendations)) {
    recommendations.push(...max.payload.recommendations);
  }

  return {
    property: INSPECTION_PROPERTIES.RECOMMENDATION,
    value: recommendations[0] || 'No recommendation on file',
    summary: recommendations[0] || 'Max has not recorded a segment recommendation yet',
    derivedFrom: recommendations.length
      ? recommendations.map((row) => ({ label: 'Recommendation', detail: String(row) }))
      : [{ label: 'Pending', detail: 'Awaiting Max prioritization or learning outcomes' }],
    headline: 'Recommendation',
  };
}

function explainMetric(property, snapshot) {
  switch (property) {
    case INSPECTION_PROPERTIES.PROGRESS:
      return explainProgressBasis(snapshot.mission, specialistContext(snapshot.contributions || [], {}));
    case INSPECTION_PROPERTIES.STAGE:
      return explainStage(snapshot);
    case INSPECTION_PROPERTIES.HEALTH:
      return explainHealthBasis(snapshot.mission, snapshot, specialistContext(snapshot.contributions || [], {}));
    case INSPECTION_PROPERTIES.CONFIDENCE:
      return explainConfidenceBasis(snapshot.mission, snapshot);
    case INSPECTION_PROPERTIES.WAITING:
      return explainWaiting(snapshot);
    case INSPECTION_PROPERTIES.BLOCKER:
      return explainBlocker(snapshot);
    case INSPECTION_PROPERTIES.TIMELINE:
      return explainTimeline(snapshot);
    case INSPECTION_PROPERTIES.NEXT:
      return explainNext(snapshot);
    case INSPECTION_PROPERTIES.RECOMMENDATION:
      return explainRecommendation(snapshot);
    default:
      return null;
  }
}

function formatInspection(explanation) {
  if (!explanation) return '';
  const lines = [explanation.headline || capitalize(explanation.property), ''];
  if (explanation.value != null && explanation.property !== INSPECTION_PROPERTIES.TIMELINE) {
    lines.push(String(explanation.value), '');
  }
  if (explanation.property === INSPECTION_PROPERTIES.PROGRESS && explanation.planning) {
    lines.push('Current Phase', '', explanation.stages && explanation.stages.find((row) => row.current)
      ? explanation.stages.find((row) => row.current).label
      : '', '');
    if (explanation.planning.complete.length) {
      lines.push('Completed', '');
      for (const item of explanation.planning.complete) lines.push(`✓ ${item}`, '');
    }
    if (explanation.planning.remaining.length) {
      lines.push('Remaining', '');
      for (const item of explanation.planning.remaining) lines.push(`□ ${item}`, '');
    }
    if (explanation.stages) {
      const remainingStages = explanation.stages.filter((row) => !row.complete && !row.current);
      for (const stage of remainingStages.slice(0, 5)) {
        lines.push(`□ ${stage.label}`, '');
      }
    }
  }
  lines.push('Derived From', '');
  for (const row of explanation.derivedFrom || []) {
    lines.push(`• ${row.label}${row.detail ? `: ${row.detail}` : ''}`);
  }
  if (explanation.summary) {
    lines.push('', explanation.summary);
  }
  return lines.join('\n').trim();
}

function emitMissionInspection(event, opts = {}) {
  const payload = {
    event: event.fallback ? 'MISSION_INSPECTION_FALLBACK' : 'MISSION_INSPECTION',
    property: event.property || null,
    resolved: event.resolved !== false,
    pipeline: event.pipeline || 'MissionInspection',
    durationMs: event.durationMs != null ? event.durationMs : null,
    timestamp: event.timestamp || new Date().toISOString(),
    reason: event.reason || null,
    missionId: event.missionId || null,
  };
  if (typeof opts.logger === 'function') {
    opts.logger(payload);
  } else if (opts.silent !== true) {
    console.info('[amo:inspection]', JSON.stringify(payload));
  }
  return payload;
}

function inspectQuestion(question, snapshot, opts = {}) {
  const started = Date.now();
  const property = classifyInspectionQuestion(question);
  const mission = snapshot.mission || {};

  if (!property) {
    if (referencesMissionState(question)) {
      const fallback = {
        resolved: false,
        property: null,
        pipeline: 'Retrieval',
        reason: 'Property not present',
        kind: 'fallback',
        missionContext: buildMissionContext(snapshot),
      };
      emitMissionInspection({
        fallback: true,
        property: null,
        resolved: false,
        pipeline: 'Retrieval',
        reason: 'Property not present',
        missionId: mission.id,
        durationMs: Date.now() - started,
      }, opts);
      return fallback;
    }
    return null;
  }

  if (property === INSPECTION_PROPERTIES.EXPLAIN) {
    const result = {
      resolved: true,
      property,
      pipeline: 'MissionInspection',
      kind: 'explain',
      structured: snapshot.why,
      missionContext: buildMissionContext(snapshot),
    };
    emitMissionInspection({
      property,
      resolved: true,
      missionId: mission.id,
      durationMs: Date.now() - started,
    }, opts);
    return result;
  }

  if (property === INSPECTION_PROPERTIES.WORKSPACE) {
    const result = {
      resolved: true,
      property,
      pipeline: 'MissionInspection',
      kind: 'workspace',
      structured: snapshot.workspace,
      missionContext: buildMissionContext(snapshot),
    };
    emitMissionInspection({
      property,
      resolved: true,
      missionId: mission.id,
      durationMs: Date.now() - started,
    }, opts);
    return result;
  }

  const explanation = explainMetric(property, snapshot);
  const result = {
    resolved: true,
    property,
    pipeline: 'MissionInspection',
    kind: 'inspection',
    structured: explanation,
    prose: formatInspection(explanation),
    missionContext: buildMissionContext(snapshot),
  };
  emitMissionInspection({
    property,
    resolved: true,
    missionId: mission.id,
    durationMs: Date.now() - started,
  }, opts);
  return result;
}

module.exports = {
  INSPECTION_PROPERTIES,
  referencesMissionState,
  classifyInspectionQuestion,
  buildMissionContext,
  explainMetric,
  explainProgressBasis,
  explainConfidenceBasis,
  explainHealthBasis,
  formatInspection,
  inspectQuestion,
  emitMissionInspection,
};
