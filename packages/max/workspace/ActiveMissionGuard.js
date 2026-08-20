'use strict';

/**
 * SPEC-127 — Active Mission Lock.
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

function resolveAcquisitionActiveMission(input = {}) {
  const tenantId = resolveTenantId(input);
  const engine = resolveAcquisitionEngine(input);
  if (!engine || !tenantId || typeof engine.list !== 'function') {
    return null;
  }
  const missions = engine.list(tenantId);
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
 * @returns {Promise<{ active: boolean, mission: object|null, source: 'legacy'|'amo'|null, missionId: string|null, executionCommand: boolean, explicitExit: boolean, exitReason: string|null }>}
 */
async function resolveActiveMissionLock(input = {}) {
  const question = normalizeText(input.question);
  const executionCommand = isMissionExecutionCommand(question);
  const exit = isExplicitMissionExit(question);

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

  const amoMission = resolveAcquisitionActiveMission(input);
  if (amoMission) {
    return {
      active: true,
      mission: amoMission,
      source: 'amo',
      missionId: amoMission.id,
      executionCommand,
      explicitExit: exit.explicit,
      exitReason: exit.reason,
    };
  }

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
