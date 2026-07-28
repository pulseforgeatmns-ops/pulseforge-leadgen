'use strict';

/**
 * SPEC-039 — classify operator messages against an active Mission.
 * Explicit New Mission wins. Diagnostics never become IntentRouter creates.
 */

const { MESSAGE_CLASS } = require('./types');

const EXPLICIT_NEW = [
  /\bnew\s+mission\b/i,
  /\bstart\s+over\b/i,
  /\bcreate\s+another\s+campaign\b/i,
  /\bstart\s+(a\s+)?new\s+campaign\b/i,
  /\babandon\s+(the\s+)?(current\s+)?mission\b/i,
];

const RESUME = [
  /^(continue|resume)\b/i,
  /\brun\s+again\b/i,
  /\bre-?run\b/i,
  /\bshow\s+(me\s+)?(the\s+)?progress\b/i,
  /\bopen\s+(the\s+)?review\b/i,
  /\bwhat\s+failed\b/i,
  /\bshow\s+(me\s+)?(the\s+)?evidence\b/i,
  /\bshow\s+(me\s+)?(the\s+)?status\b/i,
  /\bmission\s+status\b/i,
  /\bwhere\s+are\s+we\b/i,
];

const DIAGNOSE = [
  /\bwhy\s+did\s+(this|it|campaign\s+review)\s+fail\b/i,
  /\bwhy\s+(did|has|is)\b.+\bfail/i,
  /\binvestigate\b/i,
  /\bexplain\s+(the\s+)?ranking\b/i,
  /\bexplain\s+(what|why|how)\b/i,
  /\bshow\s+(me\s+)?(the\s+)?audit\b/i,
  /\baudit\s+log\b/i,
  /\bwhat\s+went\s+wrong\b/i,
  /\bdiagnos(e|is|tic)\b/i,
  /\bdebug\b/i,
];

const MODIFY = [
  /\buse\s+[a-z]/i,
  /\binstead\s+of\b/i,
  /\bactually\b/i,
  /\bchange\s+(the\s+)?(discovery\s+)?profile\b/i,
  /\bchange\s+(to|the)\b/i,
  /\bincrease\s+(the\s+)?(target\s+)?(count|to)\b/i,
  /\bdecrease\s+(the\s+)?(target\s+)?(count|to)\b/i,
  /\btarget\s+count\s*(to|=|:)?\s*\d+/i,
  /\bremove\s+/i,
  /\bexclude\s+/i,
  /\badd\s+/i,
  /\bupdate\s+/i,
  /\bswitch\s+(to|the)\b/i,
];

/**
 * Heuristic: message looks like a brand-new business objective
 * (different campaign id, generate proposal, research X, etc.).
 * @param {string} lower
 * @param {object|null} activeMission
 */
function looksLikeNewObjective(lower, activeMission) {
  if (/\bbuild\s+campaign\s+(\d+)\b/.test(lower)) {
    const m = /\bbuild\s+campaign\s+(\d+)\b/.exec(lower);
    const existing = activeMission && /campaign\s+(\d+)/i.exec(
      activeMission.objectiveText || activeMission.title || ''
    );
    if (m && existing && m[1] !== existing[1]) return true;
    if (m && !existing) return true;
    // Same campaign "build" while active → treat as resume/modify, not new
    if (m && existing && m[1] === existing[1]) return false;
  }
  if (/\bcreate\s+campaign\s+(\d+)\b/.test(lower)) return true;
  if (/\b(generate|create|draft|write|build)\s+(a\s+)?(sales\s+)?proposal\b/.test(lower)) {
    return true;
  }
  if (/\bresearch\s+[a-z0-9]/.test(lower) && !/\bresearch\s+(why|how|the\s+failure)/.test(lower)) {
    return true;
  }
  if (/\bfind\s+(the\s+)?(best\s+)?\d*\s*(commercial\s+)?(cleaning\s+)?prospects?\b/.test(lower)) {
    return true;
  }
  return false;
}

/**
 * @param {string} message
 * @param {object|null} [activeMission]
 * @returns {{ classification: string, reason: string }}
 */
function classifyMessage(message, activeMission = null) {
  const q = String(message || '').trim();
  if (!q) {
    return { classification: MESSAGE_CLASS.CLARIFY, reason: 'empty' };
  }
  const lower = q.toLowerCase();

  for (const re of EXPLICIT_NEW) {
    if (re.test(lower)) {
      return { classification: MESSAGE_CLASS.NEW_MISSION, reason: 'explicit_new' };
    }
  }

  if (looksLikeNewObjective(lower, activeMission)) {
    return { classification: MESSAGE_CLASS.NEW_MISSION, reason: 'new_objective' };
  }

  // Without an active Mission, caller should not use attach classes —
  // still return NEW_MISSION so IntentRouter can create.
  if (!activeMission) {
    return { classification: MESSAGE_CLASS.NEW_MISSION, reason: 'no_active' };
  }

  for (const re of DIAGNOSE) {
    if (re.test(lower)) {
      return { classification: MESSAGE_CLASS.DIAGNOSE, reason: 'diagnose_pattern' };
    }
  }

  for (const re of RESUME) {
    if (re.test(lower)) {
      return { classification: MESSAGE_CLASS.RESUME, reason: 'resume_pattern' };
    }
  }

  for (const re of MODIFY) {
    if (re.test(lower)) {
      return { classification: MESSAGE_CLASS.MODIFY, reason: 'modify_pattern' };
    }
  }

  // Default with active Mission: attach as resume (show / continue context)
  // rather than spawning a sibling Mission via IntentRouter.
  return { classification: MESSAGE_CLASS.RESUME, reason: 'default_resume_active' };
}

module.exports = {
  classifyMessage,
  looksLikeNewObjective,
  MESSAGE_CLASS,
};
