'use strict';

/**
 * SPEC-125 — Workspace Ownership-First Runtime.
 * Resolve exactly one pipeline owner before intent classification or retrieval.
 *
 * Ownership order:
 *   Active Mission → Mission Creation → Blueprint → Mission Inspection →
 *   Specialist Commands → Knowledge Retrieval → Reasoning (fallback)
 */

const {
  evaluateMissionContinuation,
  evaluateMissionEscape,
  CONTINUATION_THRESHOLD,
} = require('./MissionFirstRouting');
const {
  shouldInspectActiveMission,
  resolveAcquisitionEngine,
  resolveTenantId,
  looksLikeAcquisitionMissionQuestion,
} = require('./WorkspaceMissionInspection');
const { isOperatorOperatingUpdate } = require('./OperatorOperatingUpdate');
const { shouldRetrieveOperatingEvidence } = require('./OperatingEvidenceRetrieval');
const { isActiveWorkFollowUpCue, getActiveWorkContext, activeContextHasEntities } = require('./ActiveWorkContext');
const {
  detectAcquisitionObjective,
  normalizeObjectiveText,
} = require('./AcquisitionObjectiveDetection');

const WORKSPACE_OWNERS = Object.freeze({
  ACTIVE_MISSION: 'active_mission',
  MISSION_CREATION: 'mission_creation',
  BLUEPRINT: 'blueprint',
  MISSION_INSPECTION: 'mission_inspection',
  SPECIALIST_INTERROGATION: 'specialist_interrogation',
  SPECIALIST_SCOUT: 'specialist_scout',
  SPECIALIST_PAIGE: 'specialist_paige',
  SPECIALIST_CAL: 'specialist_cal',
  SPECIALIST_DIRECTION: 'specialist_direction',
  KNOWLEDGE_RETRIEVAL: 'knowledge_retrieval',
  REASONING: 'reasoning',
});

/** Mission Engine keywords — bind immediately when not superseded by Blueprint topics. */
const MISSION_KEYWORD_RE =
  /\b(acquire|mission|operate|continue|resume|execute|progress|stage|blocker|approve|discovery|prioritization|outreach|execution|learning)\b/i;

const MISSION_OPERATE_RE =
  /\boperate\b.{0,40}\b(?:anchor|client|account|campaign|mission)\b/i;

const BLUEPRINT_TOPIC_RE =
  /\b(?:icp|ideal customer(?:s| profile)?|target customer|who (?:do|should) we (?:target|serve)|our goals?|(?:business )?objectives?|growth focus|pricing|price point|how much (?:do|should) we charge|our services?|what we (?:do|offer)|offerings?|positioning|brand voice|differentiation|value prop(?:osition)?)\b/i;

const SCOUT_COMMAND_RE =
  /\b(?:find|search for|identify|discover|locate)\b.{0,40}\b(?:prospects?|leads?|companies|opportunities|accounts|commercial)\b|\b(?:analyze|analyse|assess|evaluate)\b.{0,30}\b(?:market|segment|vertical|opportunit)\b|\bwhere should we (?:be )?look\b/i;

const PAIGE_COMMAND_RE =
  /\b(?:write|draft|create|compose)\b.{0,40}\b(?:outreach|email|sequence|campaign|content|post|linkedin|copy)\b|\b(?:review|critique|improve)\b.{0,30}\b(?:campaign|content|copy|email|post)\b|\b(?:ask|get|have)\s+paige\b/i;

const CAL_COMMAND_RE =
  /\b(?:coach(?:\s+me\s+for)?|role[- ]?play)\b.{0,40}\b(?:call|discovery|meeting|conversation)\b|\bhelp me (?:prep(?:are)?|practice)\b.{0,30}\b(?:call|pitch|discovery)\b|\bprepare (?:me )?for (?:a |the )?(?:discovery )?call\b/i;

function claimsActiveDeskWorkflow(question) {
  const q = String(question || '').trim();
  if (!q) return false;
  try {
    const active = require('./ActiveWorkContext');
    if (active.isProceedWithCallScriptReviewRequest(q)) return true;
    if (active.isCallScriptDecisionRecordRequest(q)) return true;
    if (active.isCallScriptReviewRequest(q)) return true;
    if (active.isPacketReviewRequest(q)) return true;
    if (active.isFillableTableRequest(q)) return true;
    if (active.isCanarySummaryJudgmentRequest(q)) return true;
    if (active.isFocusedCanaryWorkOrderRequest(q)) return true;
  } catch (_) {
    /* ActiveWorkContext unavailable */
  }
  return false;
}

const INTERROGATION_RE =
  /\b(?:what did|what has|show me what|explain what)\b.{0,30}\b(?:scout|paige|find|search|investigation)\b|\bwhy did (?:you|scout|paige)\b/i;

function normalizeQuestion(question) {
  return String(question || '').replace(/\s+/g, ' ').trim();
}

function hasAcquisitionMissionContext(input = {}) {
  const session = input.session || null;
  const ctx = input.context || (session && session.context) || {};
  if (ctx.missionId || ctx.acquisitionMissionId || ctx.acquisitionOwner) return true;
  const tenantId = resolveTenantId(input);
  const engine = resolveAcquisitionEngine(input);
  if (!engine || !tenantId) return false;
  const missions = typeof engine.list === 'function' ? engine.list(tenantId) : [];
  return missions.some((row) => row && row.stage !== 'improve');
}

function isAcquisitionObjectiveForMission(question) {
  const q = normalizeObjectiveText(question);
  if (!detectAcquisitionObjective(q)) return false;
  if (looksLikeAcquisitionMissionQuestion(q)) return false;
  return true;
}

function missionOwnsAcquisitionRequest(question, opts = {}) {
  if (!isAcquisitionObjectiveForMission(question)) return false;
  if (opts.hasActiveMission && shouldInspectActiveMission(question, true)) {
    return false;
  }
  return true;
}

function claimsBlueprintOwnership(question, input = {}) {
  const q = normalizeQuestion(question);
  if (!q) return false;
  if (isAcquisitionObjectiveForMission(q)) return false;
  if (MISSION_KEYWORD_RE.test(q) && hasAcquisitionMissionContext(input)) return false;
  if (!BLUEPRINT_TOPIC_RE.test(q)) return false;
  return true;
}

function claimsMissionCreation(question, input = {}) {
  const q = normalizeQuestion(question);
  if (!q) return false;
  if (isAcquisitionObjectiveForMission(q)) {
    return {
      owner: WORKSPACE_OWNERS.MISSION_CREATION,
      reason: 'acquisition_objective',
      confidence: 0.96,
    };
  }
  if (MISSION_OPERATE_RE.test(q)) {
    return {
      owner: WORKSPACE_OWNERS.MISSION_CREATION,
      reason: 'mission_operate_command',
      confidence: 0.94,
    };
  }
  if (MISSION_KEYWORD_RE.test(q) && hasAcquisitionMissionContext(input)) {
    if (shouldInspectActiveMission(q, true)) {
      return null;
    }
    return {
      owner: WORKSPACE_OWNERS.MISSION_CREATION,
      reason: 'mission_keyword_with_context',
      confidence: 0.88,
    };
  }
  if (missionOwnsAcquisitionRequest(q, { hasActiveMission: hasAcquisitionMissionContext(input) })) {
    return {
      owner: WORKSPACE_OWNERS.MISSION_CREATION,
      reason: 'mission_owns_acquisition',
      confidence: 0.92,
    };
  }
  return null;
}

function claimsMissionInspection(question, input = {}) {
  const q = normalizeQuestion(question);
  if (!q) return false;
  const hasActive = hasAcquisitionMissionContext(input);
  return shouldInspectActiveMission(q, hasActive);
}

function claimsSpecialistOwnership(question) {
  const q = normalizeQuestion(question);
  if (!q) return null;

  if (INTERROGATION_RE.test(q)) {
    return {
      owner: WORKSPACE_OWNERS.SPECIALIST_INTERROGATION,
      reason: 'specialist_trace_interrogation',
      confidence: 0.9,
      specialist: null,
    };
  }
  if (CAL_COMMAND_RE.test(q) && !claimsActiveDeskWorkflow(q)) {
    return {
      owner: WORKSPACE_OWNERS.SPECIALIST_CAL,
      reason: 'cal_call_coaching',
      confidence: 0.92,
      specialist: 'cal',
    };
  }
  if (SCOUT_COMMAND_RE.test(q)) {
    return {
      owner: WORKSPACE_OWNERS.SPECIALIST_SCOUT,
      reason: 'scout_command',
      confidence: 0.9,
      specialist: 'scout',
    };
  }
  if (PAIGE_COMMAND_RE.test(q)) {
    return {
      owner: WORKSPACE_OWNERS.SPECIALIST_PAIGE,
      reason: 'paige_command',
      confidence: 0.9,
      specialist: 'paige',
    };
  }
  return null;
}

function claimsKnowledgeRetrieval(question) {
  const q = normalizeQuestion(question);
  if (!q) return false;
  if (isOperatorOperatingUpdate(q)) return true;
  if (shouldRetrieveOperatingEvidence(q)) return true;
  return false;
}

/**
 * @param {object} input
 * @param {string} input.question
 * @param {object} [input.session]
 * @param {object} [input.context]
 * @param {object} [input.missionEngine]
 * @param {boolean} [input.missionsEnabled]
 * @param {boolean} [input.resolverEnabled]
 * @returns {Promise<{ owner: string, reason: string, confidence: number, specialist?: string|null, fallback?: boolean }>}
 */
async function resolveWorkspaceOwner(input = {}) {
  const question = normalizeQuestion(input.question);
  if (!question) {
    return {
      owner: WORKSPACE_OWNERS.REASONING,
      reason: 'empty_question',
      confidence: 1,
      specialist: null,
      fallback: true,
    };
  }

  // Active desk workflows (canary call-script review, packet review, tables)
  if (claimsActiveDeskWorkflow(question)) {
    return {
      owner: WORKSPACE_OWNERS.ACTIVE_MISSION,
      reason: 'active_desk_workflow',
      confidence: 0.95,
      specialist: null,
    };
  }

  // 1 — Active Mission (legacy mission continuation)
  if (
    input.missionsEnabled !== false &&
    input.missionEngine &&
    input.missionEngine.activeMissionResolver &&
    input.resolverEnabled !== false &&
    input.session &&
    input.session.id
  ) {
    const escape = evaluateMissionEscape(question);
    const activeMission = await input.missionEngine.activeMissionResolver.resolveActiveMission(
      input.session.id
    );
    if (activeMission && escape.explicit) {
      return {
        owner: WORKSPACE_OWNERS.ACTIVE_MISSION,
        reason: 'mission_explicit_escape',
        confidence: 0.98,
        specialist: null,
      };
    }
    if (activeMission && !escape.explicit) {
      const continuation = evaluateMissionContinuation(question, activeMission);
      if (continuation.continues && continuation.confidence >= CONTINUATION_THRESHOLD) {
        return {
          owner: WORKSPACE_OWNERS.ACTIVE_MISSION,
          reason: `active_mission_${continuation.classification}`,
          confidence: continuation.confidence,
          specialist: null,
        };
      }
    }
  }

  // Active desk continuation cues bind to active mission workspace
  if (input.session) {
    const prior = getActiveWorkContext(input.session);
    if (activeContextHasEntities(prior) && isActiveWorkFollowUpCue(question)) {
      return {
        owner: WORKSPACE_OWNERS.ACTIVE_MISSION,
        reason: 'active_work_continuation',
        confidence: 0.86,
        specialist: null,
      };
    }
  }

  // 2 — Mission Creation
  const missionClaim = claimsMissionCreation(question, input);
  if (missionClaim) return { ...missionClaim, specialist: null };

  // 3 — Blueprint
  if (claimsBlueprintOwnership(question, input)) {
    return {
      owner: WORKSPACE_OWNERS.BLUEPRINT,
      reason: 'blueprint_topic',
      confidence: 0.88,
      specialist: null,
    };
  }

  // 4 — Mission Inspection (always before retrieval)
  if (claimsMissionInspection(question, input)) {
    return {
      owner: WORKSPACE_OWNERS.MISSION_INSPECTION,
      reason: 'mission_inspection',
      confidence: 0.94,
      specialist: null,
    };
  }

  // 5 — Specialist Commands
  const specialistClaim = claimsSpecialistOwnership(question);
  if (specialistClaim) return specialistClaim;

  // 6 — Knowledge Retrieval
  if (claimsKnowledgeRetrieval(question)) {
    return {
      owner: WORKSPACE_OWNERS.KNOWLEDGE_RETRIEVAL,
      reason: isOperatorOperatingUpdate(question)
        ? 'operating_update'
        : 'knowledge_retrieval',
      confidence: 0.84,
      specialist: null,
    };
  }

  // 7 — Reasoning fallback
  return {
    owner: WORKSPACE_OWNERS.REASONING,
    reason: 'no_owner_claim',
    confidence: 0.5,
    specialist: null,
    fallback: true,
  };
}

module.exports = {
  WORKSPACE_OWNERS,
  MISSION_KEYWORD_RE,
  BLUEPRINT_TOPIC_RE,
  SCOUT_COMMAND_RE,
  PAIGE_COMMAND_RE,
  CAL_COMMAND_RE,
  resolveWorkspaceOwner,
  isAcquisitionObjectiveForMission,
  missionOwnsAcquisitionRequest,
  claimsBlueprintOwnership,
  claimsMissionCreation,
  claimsMissionInspection,
  claimsSpecialistOwnership,
  claimsActiveDeskWorkflow,
  claimsKnowledgeRetrieval,
};
