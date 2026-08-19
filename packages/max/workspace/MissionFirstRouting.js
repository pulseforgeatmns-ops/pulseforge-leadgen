'use strict';

/**
 * SPEC-119 — Mission-First Routing Architecture.
 *
 * Routing determines ownership. Reasoning determines behavior.
 * Mission continuation is evaluated before cognitive/domain classifiers.
 */

const { classifyMessage, MESSAGE_CLASS } = require('../../mission-engine/classifyMessage');
const { ROUTE_KINDS, MISSION_TYPES } = require('../../mission-engine/types');
const {
  selectExecutionDomain,
  attachDomainContext,
  EXECUTION_DOMAINS,
  toRouteDecision,
} = require('./ExecutionDomain');
const {
  composeMissionResponse,
  composeActiveMissionResponse,
} = require('./MissionResponse');

const PIPELINES = Object.freeze({
  MISSION_ENGINE: 'Mission Engine',
  GENERAL: 'General',
});

/** Minimum continuation confidence before Mission Engine owns the turn. */
const CONTINUATION_THRESHOLD = 0.7;

const CONTINUATION_SIGNALS = [
  /\bapprov(e|al)\b/i,
  /\bcontinue\b/i,
  /\bproceed\b/i,
  /\bbegin\b/i,
  /\bnext\b/i,
  /\bstatus\b/i,
  /\bprogress\b/i,
  /\bblockers?\b/i,
  /\bupdate\b/i,
  /\bpause\b/i,
  /\bresume\b/i,
  /\bmodify\b/i,
  /\breview\s+results?\b/i,
  /\bapprove\s+draft\b/i,
  /\bexecute\s+stage\b/i,
  /\bwhy\b/i,
  /\brun\s+again\b/i,
  /\bshow\s+(me\s+)?(the\s+)?(progress|status|evidence)\b/i,
];

const EXPLICIT_ESCAPE = [
  /\bforget\s+(this\s+)?mission\b/i,
  /\babandon\s+(the\s+)?(current\s+)?mission\b/i,
  /\bstart\s+over\b/i,
  /\blet'?s\s+talk\s+about\b/i,
  /\bexplain\s+embeddings?\b/i,
  /\bcreate\s+a\s+new\s+acquisition\s+mission\b/i,
  /\bnew\s+acquisition\s+mission\b/i,
  /\bunrelated\s+conversation\b/i,
  /\bstop\s+(this\s+)?mission\b/i,
  /\bexit\s+(this\s+)?mission\b/i,
  /\bleave\s+(this\s+)?mission\b/i,
];

/** @type {object[]} */
const _routingLog = [];

/**
 * @param {string} question
 * @returns {{ explicit: boolean, reason: string|null }}
 */
function evaluateMissionEscape(question) {
  const q = String(question || '').trim();
  for (const re of EXPLICIT_ESCAPE) {
    if (re.test(q)) {
      return { explicit: true, reason: `explicit_escape:${re.source}` };
    }
  }
  return { explicit: false, reason: null };
}

/**
 * Semantic continuation evaluation against an active Mission.
 * @param {string} question
 * @param {object} activeMission
 * @returns {{ continues: boolean, confidence: number, classification: string, reason: string, explicitNew: boolean }}
 */
function evaluateMissionContinuation(question, activeMission) {
  const { classification, reason } = classifyMessage(question, activeMission);
  let confidence;

  switch (classification) {
    case MESSAGE_CLASS.RESUME:
      confidence = reason === 'default_resume_active' ? 0.78 : 0.95;
      break;
    case MESSAGE_CLASS.MODIFY:
      confidence = 0.92;
      break;
    case MESSAGE_CLASS.DIAGNOSE:
      confidence = 0.9;
      break;
    case MESSAGE_CLASS.CLARIFY:
      confidence = 0.88;
      break;
    case MESSAGE_CLASS.NEW_MISSION:
      confidence = reason === 'explicit_new' ? 0.96 : 0.84;
      break;
    default:
      confidence = 0.5;
  }

  const lower = String(question || '').toLowerCase();
  for (const signal of CONTINUATION_SIGNALS) {
    if (signal.test(lower)) {
      confidence = Math.min(1, confidence + 0.04);
    }
  }

  return {
    continues: confidence >= CONTINUATION_THRESHOLD,
    confidence,
    classification,
    reason,
    explicitNew: classification === MESSAGE_CLASS.NEW_MISSION,
  };
}

/**
 * @param {object} entry
 */
function logMissionRouting(entry) {
  const row = {
    event: 'MISSION_ROUTING',
    missionFound: Boolean(entry.missionFound),
    missionId: entry.missionId || null,
    continuationConfidence:
      entry.continuationConfidence != null ? entry.continuationConfidence : null,
    selectedPipeline: entry.selectedPipeline || null,
    routingReason: entry.routingReason || null,
    stage: entry.stage || null,
    capability: entry.capability || null,
    timestamp: new Date().toISOString(),
  };
  _routingLog.push(row);
  if (typeof process !== 'undefined' && process.env.NODE_ENV !== 'test') {
    console.info('[MISSION_ROUTING]', JSON.stringify(row));
  }
  return row;
}

/**
 * @param {object} entry
 */
function logMissionRoutingOverride(entry) {
  const row = {
    event: 'MISSION_ROUTING_OVERRIDE',
    missionId: entry.missionId || null,
    mission: entry.missionLabel || entry.missionId || null,
    previousPipeline: entry.previousPipeline || PIPELINES.GENERAL,
    newPipeline: PIPELINES.MISSION_ENGINE,
    reason: entry.reason || 'Active Mission continuation.',
    timestamp: new Date().toISOString(),
  };
  _routingLog.push(row);
  if (typeof process !== 'undefined' && process.env.NODE_ENV !== 'test') {
    console.info('[MISSION_ROUTING_OVERRIDE]', JSON.stringify(row));
  }
  return row;
}

function currentStageForMission(mission) {
  if (!mission) return null;
  return (
    (mission.progress && mission.progress.currentStage) ||
    mission.status ||
    null
  );
}

function currentCapabilityForMission(mission) {
  if (!mission) return null;
  const steps = (mission.plan && mission.plan.steps) || [];
  const active =
    steps.find((s) => s.status === 'executing') ||
    steps.find((s) => s.status === 'queued' || s.status === 'stale') ||
    null;
  return active ? active.capabilityId || active.name || null : null;
}

function legacyPipelineLabel(question, session) {
  const domainDecision = selectExecutionDomain(question, {
    previousDomain: session.executionDomain || null,
  });
  if (
    domainDecision.domain === EXECUTION_DOMAINS.MISSION_EXECUTION ||
    domainDecision.domain === EXECUTION_DOMAINS.MISSION_DIAGNOSTICS
  ) {
    return PIPELINES.MISSION_ENGINE;
  }
  return PIPELINES.GENERAL;
}

function domainDecisionForMission(mission, resolution, session) {
  const diagnostic =
    resolution.classification === MESSAGE_CLASS.DIAGNOSE ||
    resolution.action === 'diagnosed' ||
    mission.type === MISSION_TYPES.CAMPAIGN_REVIEW;
  const domain = diagnostic
    ? EXECUTION_DOMAINS.MISSION_DIAGNOSTICS
    : EXECUTION_DOMAINS.MISSION_EXECUTION;
  const previousDomain = session.executionDomain || null;
  return {
    domain,
    missionIntent: null,
    missionType: mission.type,
    routeKind: ROUTE_KINDS.MISSION,
    reason: `mission_first_${resolution.action || 'continue'}`,
    confidence: 1,
    domainSwitched: Boolean(previousDomain && previousDomain !== domain),
    previousDomain,
  };
}

function composeMissionBlockedResponse(mission, question, reason, card) {
  const title = mission.title || mission.id;
  const stage = currentStageForMission(mission);
  const answer = [
    `Mission: ${title}`,
    stage ? `Current stage: ${stage}.` : null,
    'Unable to continue.',
    reason ? `Reason: ${reason}.` : null,
    'Mission remains active. No progress lost.',
  ]
    .filter(Boolean)
    .join(' ');

  return composeActiveMissionResponse({
    resolution: {
      action: 'blocked',
      classification: MESSAGE_CLASS.RESUME,
      resolutionPath: 'mission_blocked',
      mission,
      reason,
      diagnosis: { summary: answer },
    },
    question,
    card: card || null,
    executionDomain: EXECUTION_DOMAINS.MISSION_EXECUTION,
  });
}

/**
 * Highest-priority routing — load active Mission and evaluate continuation
 * before cognitive/domain classifiers run.
 *
 * @param {object} input
 * @returns {Promise<object|null>}
 */
async function maybeHandleMissionFirstTurn(input) {
  const {
    question,
    session,
    missionEngine,
    missionsEnabled,
    resolverEnabled,
    rawContext,
    envelopeSwitch,
    presentation,
    sessions,
  } = input;

  if (!missionsEnabled || !missionEngine || resolverEnabled === false) {
    return null;
  }

  const resolver = missionEngine.activeMissionResolver;
  if (!resolver) return null;

  const activeMission = await resolver.resolveActiveMission(session.id);
  const missionFound = Boolean(activeMission);

  if (!missionFound) {
    logMissionRouting({
      missionFound: false,
      continuationConfidence: null,
      selectedPipeline: null,
      routingReason: 'no_active_mission',
    });
    return null;
  }

  const escape = evaluateMissionEscape(question);
  if (escape.explicit) {
    await resolver.clearActiveMission(session.id);
    logMissionRouting({
      missionFound: true,
      missionId: activeMission.id,
      continuationConfidence: 0,
      selectedPipeline: PIPELINES.GENERAL,
      routingReason: escape.reason,
      stage: currentStageForMission(activeMission),
      capability: currentCapabilityForMission(activeMission),
    });
    return null;
  }

  const continuation = evaluateMissionContinuation(question, activeMission);

  logMissionRouting({
    missionFound: true,
    missionId: activeMission.id,
    continuationConfidence: continuation.confidence,
    selectedPipeline: continuation.continues
      ? PIPELINES.MISSION_ENGINE
      : PIPELINES.GENERAL,
    routingReason: continuation.continues
      ? `continuation:${continuation.classification}:${continuation.reason}`
      : `continuation_below_threshold:${continuation.reason}`,
    stage: currentStageForMission(activeMission),
    capability: currentCapabilityForMission(activeMission),
  });

  if (!continuation.continues) {
    return null;
  }

  const previousPipeline = legacyPipelineLabel(question, session);
  if (previousPipeline === PIPELINES.GENERAL) {
    logMissionRoutingOverride({
      missionId: activeMission.id,
      missionLabel: activeMission.title || activeMission.id,
      previousPipeline,
      reason: 'Active Mission continuation.',
    });
  }

  let resolution;
  try {
    resolution = await resolver.resolve({
      sessionId: session.id,
      message: question,
      tenantId: session.context.tenantId,
      clientId: session.context.tenantId,
      operatorId: (session && session.operator) || null,
    });
  } catch (err) {
    const structured = composeMissionBlockedResponse(
      activeMission,
      question,
      err.message || 'Mission execution error',
      missionEngine.toCard(activeMission)
    );
    return buildMissionFirstResult({
      session,
      sessions,
      presentation,
      structured,
      mission: activeMission,
      resolution: {
        action: 'blocked',
        reason: err.message,
      },
      route: {
        kind: ROUTE_KINDS.MISSION,
        missionType: activeMission.type,
        reason: 'mission_blocked',
        missionIntent: null,
        executionDomain: EXECUTION_DOMAINS.MISSION_EXECUTION,
      },
      domainDecision: domainDecisionForMission(activeMission, { action: 'blocked' }, session),
      envelopeSwitch,
      rawContext,
      missionRouting: {
        missionFound: true,
        continuationConfidence: continuation.confidence,
        selectedPipeline: PIPELINES.MISSION_ENGINE,
        routingReason: 'mission_blocked',
        override: previousPipeline === PIPELINES.GENERAL,
      },
    });
  }

  if (resolution.action === 'intelligence') {
    return null;
  }

  const mission = resolution.mission || activeMission;
  const domainDecision = domainDecisionForMission(mission, resolution, session);
  session.executionDomain = domainDecision.domain;
  if (session.context && typeof session.context === 'object') {
    session.context.executionDomain = domainDecision.domain;
    session.context._answerCorpus = 'mission';
  }

  const domainAttach = attachDomainContext({
    session,
    decision: domainDecision,
    incomingContext: rawContext,
    mission,
  });

  let structured;
  if (resolution.action === 'created' && mission) {
    structured = composeMissionResponse({
      mission,
      question,
      card: missionEngine.toCard(mission),
      executionDomain: domainDecision.domain,
    });
  } else if (mission) {
    structured = composeActiveMissionResponse({
      resolution,
      question,
      card: missionEngine.toCard(mission),
      executionDomain: domainDecision.domain,
    });
  } else {
    structured = composeMissionBlockedResponse(
      activeMission,
      question,
      'Mission state unavailable.',
      missionEngine.toCard(activeMission)
    );
  }

  const route = {
    kind: ROUTE_KINDS.MISSION,
    missionType: mission.type,
    reason: resolution.resolutionPath || 'mission_first',
    missionIntent: null,
    executionDomain: domainDecision.domain,
  };

  return buildMissionFirstResult({
    session,
    sessions,
    presentation,
    structured,
    mission,
    resolution,
    route,
    domainDecision,
    domainAttach,
    envelopeSwitch,
    missionRouting: {
      missionFound: true,
      continuationConfidence: continuation.confidence,
      selectedPipeline: PIPELINES.MISSION_ENGINE,
      routingReason: `continuation:${continuation.classification}`,
      override: previousPipeline === PIPELINES.GENERAL,
    },
  });
}

async function buildMissionFirstResult(input) {
  const {
    session,
    sessions,
    presentation,
    structured,
    mission,
    resolution,
    route,
    domainDecision,
    domainAttach,
    envelopeSwitch,
    missionRouting,
  } = input;

  const presented = await presentation.present(structured);
  let prose = presented.prose;
  const switchLines = [
    envelopeSwitch,
    domainAttach && domainAttach.domainSwitch,
  ].filter(Boolean);
  if (switchLines.length) {
    prose = `${switchLines.join('\n')}\n\n${prose}`;
  }

  sessions.appendMessage(session.id, {
    role: 'max',
    text: prose,
    structured,
  });

  const context = (domainAttach && domainAttach.context) || session.context;

  return {
    sessionId: session.id,
    prose,
    structured,
    metadata: presented.metadata,
    contextSwitch: envelopeSwitch,
    domainSwitch: (domainAttach && domainAttach.domainSwitch) || null,
    context,
    presentation: presented.presentation,
    route: route.kind,
    mission,
    resolution,
    executionDomain: route.executionDomain || domainDecision.domain,
    domainDecision,
    executionContext:
      (domainAttach && domainAttach.executionContext) || {
        domain: domainDecision.domain,
        routeKind: ROUTE_KINDS.MISSION,
        reason: route.reason,
        missionType: mission.type,
        missionId: mission.id,
      },
    missionRouting,
  };
}

function listMissionRoutingLog() {
  return _routingLog.map((row) => ({ ...row }));
}

function clearMissionRoutingLog() {
  _routingLog.length = 0;
}

module.exports = {
  PIPELINES,
  CONTINUATION_THRESHOLD,
  evaluateMissionEscape,
  evaluateMissionContinuation,
  logMissionRouting,
  logMissionRoutingOverride,
  maybeHandleMissionFirstTurn,
  listMissionRoutingLog,
  clearMissionRoutingLog,
};
