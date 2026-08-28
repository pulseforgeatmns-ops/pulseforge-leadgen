'use strict';

/**
 * SPEC-127 — Active Mission Lock.
 * SPEC-130 — Hydrate AMO before resolving acquisition missions.
 * When an active mission exists, execution commands bind to the mission and
 * General Conversation / Daily Briefing cannot claim the turn unless the
 * operator explicitly exits.
 */

const { isActiveMissionStatus } = require('../../mission-engine/types');
const {
  resolveTenantId,
  resolveAcquisitionMissionRuntime,
  resolveAcquisitionEngine,
  assertRuntimeEngine,
} = require('./WorkspaceMissionInspection');
const {
  isMissionExecutionCommand,
  MISSION_EXECUTION_COMMAND_RES,
} = require('./ExecutionLanguageDetection');
const {
  ensureAmoTenantHydrated,
  logAmoActiveResolved,
} = require('./AmoWorkspaceHydration');
const { isMissionPlanningTurn } = require('./MissionPlanningTurn');
const { buildStructuredResponse } = require('./WorkspaceTypes');
const askPathTrace = require('./audit/AskPathTrace');

const UNRESOLVED_BOUND_MISSION_REASON = 'unresolved_bound_mission_context';

const BLOCKED_DOMAIN_GENERAL = 'general_conversation';
const BLOCKED_DOMAIN_BRIEFING = 'morning_briefing';
const WORKSPACE_DOMAIN = 'workspace';

/** Only these may leave an active mission for briefing / general conversation. */
const EXPLICIT_MISSION_EXIT_RES = [
  /\bcancel\s+(?:the\s+)?mission\b/i,
  /\bpause\s+(?:the\s+)?mission\b/i,
  /\bleave\s+(?:the\s+)?mission\b/i,
  /\bend\s+(?:the\s+)?mission\b/i,
  /\btoday'?s\s+brief(ing)?\b/i,
  /\bnew\s+topic\b/i,
  /\bforget\s+(?:this\s+)?mission\b/i,
  /\babandon\s+(?:the\s+)?(?:current\s+)?mission\b/i,
  /\bexit\s+(?:this\s+)?mission\b/i,
  /\bstop\s+(?:this\s+)?mission\b/i,
];

const BLOCKED_DOMAINS = new Set([
  BLOCKED_DOMAIN_GENERAL,
  BLOCKED_DOMAIN_BRIEFING,
]);

function normalizeText(text) {
  return String(text || '').replace(/\s+/g, ' ').trim();
}

/**
 * @param {string} text
 * @returns {{ explicit: boolean, reason: string|null }}
 */
function isExplicitMissionExit(text) {
  const q = normalizeText(text);
  for (const re of EXPLICIT_MISSION_EXIT_RES) {
    if (re.test(q)) {
      return { explicit: true, reason: `explicit_exit:${re.source}` };
    }
  }
  return { explicit: false, reason: null };
}

function resolveSessionBoundMissionId(input = {}) {
  const sessionCtx =
    (input.session && input.session.context) ||
    (input.context && typeof input.context === 'object' ? input.context : {});
  return sessionCtx.missionId || sessionCtx.acquisitionMissionId || null;
}

function isAmoSessionBoundMissionClaim(input = {}, boundId) {
  if (!boundId) return false;
  const sessionCtx =
    (input.session && input.session.context) ||
    (input.context && typeof input.context === 'object' ? input.context : {});
  if (sessionCtx.acquisitionMissionId === boundId) return true;
  if (sessionCtx.acquisitionOwner === 'AMO' && sessionCtx.missionId === boundId) return true;
  return false;
}

/** list() is for discovery; session-bound IDs resolve through get(). */
function pickAcquisitionMission(missions) {
  return missions.find((row) => row && row.stage !== 'improve') || null;
}

/**
 * @param {object} input
 * @returns {Promise<{ mission: object|null, unresolvedBoundMissionId: string|null }>}
 */
async function resolveAcquisitionActiveMission(input = {}) {
  askPathTrace.traceEnter('resolveAcquisitionActiveMission');
  const tenantId = resolveTenantId(input);
  const runtime = resolveAcquisitionMissionRuntime(input);
  const engine = runtime.engine();
  assertRuntimeEngine(engine, runtime);

  await ensureAmoTenantHydrated(input);

  const boundId = resolveSessionBoundMissionId(input);
  if (boundId) {
    const bound = engine.get(boundId, tenantId);
    if (bound && bound.stage !== 'improve') {
      logAmoActiveResolved(bound, tenantId);
      askPathTrace.traceEarlyReturn('resolveAcquisitionActiveMission', 'session_bound_mission', {
        missionId: bound.id,
      });
      return { mission: bound, unresolvedBoundMissionId: null };
    }
    if (!bound && isAmoSessionBoundMissionClaim(input, boundId)) {
      askPathTrace.traceEarlyReturn('resolveAcquisitionActiveMission', UNRESOLVED_BOUND_MISSION_REASON, {
        boundMissionId: boundId,
      });
      return { mission: null, unresolvedBoundMissionId: String(boundId) };
    }
  }

  const missions = engine.list(tenantId);
  const resolved = pickAcquisitionMission(missions);
  if (resolved) {
    logAmoActiveResolved(resolved, tenantId);
  }
  askPathTrace.traceEarlyReturn('resolveAcquisitionActiveMission', resolved ? 'mission_found' : 'no_mission', {
    missionId: resolved && resolved.id,
  });
  return { mission: resolved, unresolvedBoundMissionId: null };
}

function buildUnresolvedBoundMissionResponse(boundMissionId, input = {}) {
  const prose =
    'This conversation is still bound to an acquisition mission, but I cannot reload that mission right now. ' +
    'Please reopen the mission workspace or restate your decision after the mission is available again.';
  const structured = buildStructuredResponse({
    answer: prose,
    reasoning: [],
    supportingEvidence: [],
    contradictingEvidence: [],
    confidence: 1,
    nextInvestigations: [],
    recommendedActions: [],
    confidenceContributors: ['spec_201', UNRESOLVED_BOUND_MISSION_REASON],
    timelineReferences: [],
    relatedEntities: boundMissionId
      ? [{ id: boundMissionId, type: 'acquisition_mission', name: boundMissionId }]
      : [],
    metadata: {
      spec: 'SPEC-201',
      unresolvedBoundMissionId: boundMissionId || null,
      tenantId: resolveTenantId(input) || null,
    },
  });
  return {
    reason: UNRESOLVED_BOUND_MISSION_REASON,
    prose,
    structured,
    mission: null,
    action: 'blocked',
    unresolvedBoundMission: true,
    boundMissionId: boundMissionId || null,
  };
}

/**
 * @param {object} input
 * @param {boolean} [input.detectExecution=false] — SPEC-140: execution language is illegal
 *   until ownership is established. Only set true on the reasoning fallback path after
 *   ownership resolution has already selected a non-mission owner.
 * @returns {Promise<{ active: boolean, mission: object|null, source: 'legacy'|'amo'|null, missionId: string|null, executionCommand: boolean, explicitExit: boolean, exitReason: string|null }>}
 */
async function resolveActiveMissionLock(input = {}) {
  askPathTrace.traceEnter('resolveActiveMissionLock');
  const question = normalizeText(input.question);
  const operatorIntent = input.operatorIntent || null;
  const executionCommand = operatorIntent
    ? operatorIntent.executionRequested
    : input.detectExecution === true && isMissionExecutionCommand(question);
  const exit = isExplicitMissionExit(question);

  const amoResolution = await resolveAcquisitionActiveMission(input);
  const amoMission = amoResolution.mission;
  if (amoResolution.unresolvedBoundMissionId) {
    askPathTrace.traceEarlyReturn('resolveActiveMissionLock', UNRESOLVED_BOUND_MISSION_REASON, {
      boundMissionId: amoResolution.unresolvedBoundMissionId,
      executionCommand,
    });
    return {
      active: true,
      mission: null,
      source: 'amo',
      missionId: amoResolution.unresolvedBoundMissionId,
      executionCommand,
      explicitExit: exit.explicit,
      exitReason: exit.reason,
      unresolvedBoundMission: true,
      boundMissionId: amoResolution.unresolvedBoundMissionId,
    };
  }
  if (amoMission) {
    const planningTurn = operatorIntent
      ? operatorIntent.planningRequested
      : isMissionPlanningTurn(amoMission, question);
    const cancelDuringPlanning = planningTurn && /\bcancel\b/i.test(question);
    askPathTrace.traceEarlyReturn('resolveActiveMissionLock', 'amo_active', {
      missionId: amoMission.id,
      executionCommand: executionCommand || planningTurn,
    });
    return {
      active: true,
      mission: amoMission,
      source: 'amo',
      missionId: amoMission.id,
      executionCommand: executionCommand || planningTurn,
      explicitExit: cancelDuringPlanning ? false : exit.explicit,
      exitReason: cancelDuringPlanning ? null : exit.reason,
      unresolvedBoundMission: false,
      boundMissionId: null,
    };
  }

  if (
    input.missionsEnabled !== false &&
    input.missionEngine &&
    input.missionEngine.activeMissionResolver &&
    input.resolverEnabled !== false &&
    input.session &&
    input.session.id
  ) {
    const legacy = await input.missionEngine.activeMissionResolver.resolveActiveMission(
      input.session.id
    );
    if (legacy && isActiveMissionStatus(legacy.status)) {
      askPathTrace.traceEarlyReturn('resolveActiveMissionLock', 'legacy_active', {
        missionId: legacy.id,
        executionCommand,
      });
      return {
        active: true,
        mission: legacy,
        source: 'legacy',
        missionId: legacy.id,
        executionCommand,
        explicitExit: exit.explicit,
        exitReason: exit.reason,
      };
    }
  }

  askPathTrace.traceEarlyReturn('resolveActiveMissionLock', 'no_active_mission', {
    executionCommand,
  });
  return {
    active: false,
    mission: null,
    source: null,
    missionId: null,
    executionCommand,
    explicitExit: exit.explicit,
    exitReason: exit.reason,
    unresolvedBoundMission: false,
    boundMissionId: null,
  };
}

/**
 * Apply SPEC-127 domain guard before General Conversation / Daily Briefing.
 * @param {object} decision
 * @param {object} lockState
 * @param {string} question
 * @returns {{ decision: object, guarded: boolean, blockedDomain: string|null }}
 */
function guardExecutionDomain(decision, lockState, question, operatorIntent = null) {
  if (!lockState || !lockState.active || lockState.explicitExit) {
    return { decision, guarded: false, blockedDomain: null };
  }

  if (!BLOCKED_DOMAINS.has(decision.domain)) {
    return { decision, guarded: false, blockedDomain: null };
  }

  const blockedDomain = decision.domain;
  const reason =
    (operatorIntent && operatorIntent.executionRequested) ||
    lockState.executionCommand
      ? 'mission_execution_command'
      : 'active_mission_lock';

  return {
    decision: {
      ...decision,
      domain: WORKSPACE_DOMAIN,
      missionIntent: null,
      missionType: lockState.source === 'amo' ? 'acquisition_mission' : decision.missionType,
      routeKind: 'intelligence',
      reason,
      confidence: 0.96,
      domainSwitched: false,
      previousDomain: decision.previousDomain,
      activeMissionGuard: true,
      blockedDomain,
    },
    guarded: true,
    blockedDomain,
  };
}

module.exports = {
  MISSION_EXECUTION_COMMAND_RES,
  EXPLICIT_MISSION_EXIT_RES,
  BLOCKED_DOMAINS,
  BLOCKED_DOMAIN_GENERAL,
  BLOCKED_DOMAIN_BRIEFING,
  WORKSPACE_DOMAIN,
  UNRESOLVED_BOUND_MISSION_REASON,
  normalizeText,
  isMissionExecutionCommand,
  isExplicitMissionExit,
  resolveSessionBoundMissionId,
  isAmoSessionBoundMissionClaim,
  resolveAcquisitionActiveMission,
  resolveActiveMissionLock,
  guardExecutionDomain,
  buildUnresolvedBoundMissionResponse,
};
