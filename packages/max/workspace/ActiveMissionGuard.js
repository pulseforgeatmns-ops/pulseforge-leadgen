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
  resolveAcquisitionEngine,
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
const askPathTrace = require('./audit/AskPathTrace');

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

function pickAcquisitionMission(missions, input = {}) {
  const sessionCtx =
    (input.session && input.session.context) ||
    (input.context && typeof input.context === 'object' ? input.context : {});
  const boundId = sessionCtx.missionId || sessionCtx.acquisitionMissionId || null;
  if (boundId) {
    const bound = missions.find((row) => row && row.id === boundId);
    if (bound && bound.stage !== 'improve') return bound;
  }
  return missions.find((row) => row && row.stage !== 'improve') || null;
}

/**
 * @param {object} input
 * @returns {Promise<object|null>}
 */
async function resolveAcquisitionActiveMission(input = {}) {
  askPathTrace.traceEnter('resolveAcquisitionActiveMission');
  const tenantId = resolveTenantId(input);
  const engine = resolveAcquisitionEngine(input);
  if (!engine || !tenantId || typeof engine.list !== 'function') {
    askPathTrace.traceEarlyReturn('resolveAcquisitionActiveMission', 'no_engine_or_tenant');
    return null;
  }

  await ensureAmoTenantHydrated(input);

  const missions = engine.list(tenantId);
  const resolved = pickAcquisitionMission(missions, input);
  if (resolved) {
    logAmoActiveResolved(resolved, tenantId);
  }
  askPathTrace.traceEarlyReturn('resolveAcquisitionActiveMission', resolved ? 'mission_found' : 'no_mission', {
    missionId: resolved && resolved.id,
  });
  return resolved;
}

/**
 * @param {object} input
 * @returns {Promise<{ active: boolean, mission: object|null, source: 'legacy'|'amo'|null, missionId: string|null, executionCommand: boolean, explicitExit: boolean, exitReason: string|null }>}
 */
async function resolveActiveMissionLock(input = {}) {
  askPathTrace.traceEnter('resolveActiveMissionLock');
  const question = normalizeText(input.question);
  const executionCommand = isMissionExecutionCommand(question);
  const exit = isExplicitMissionExit(question);

  const amoMission = await resolveAcquisitionActiveMission(input);
  if (amoMission) {
    const planningTurn = isMissionPlanningTurn(amoMission, question);
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
  };
}

/**
 * Apply SPEC-127 domain guard before General Conversation / Daily Briefing.
 * @param {object} decision
 * @param {object} lockState
 * @param {string} question
 * @returns {{ decision: object, guarded: boolean, blockedDomain: string|null }}
 */
function guardExecutionDomain(decision, lockState, question) {
  if (!lockState || !lockState.active || lockState.explicitExit) {
    return { decision, guarded: false, blockedDomain: null };
  }

  if (!BLOCKED_DOMAINS.has(decision.domain)) {
    return { decision, guarded: false, blockedDomain: null };
  }

  const blockedDomain = decision.domain;
  const reason = isMissionExecutionCommand(question)
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
  normalizeText,
  isMissionExecutionCommand,
  isExplicitMissionExit,
  resolveAcquisitionActiveMission,
  resolveActiveMissionLock,
  guardExecutionDomain,
};
