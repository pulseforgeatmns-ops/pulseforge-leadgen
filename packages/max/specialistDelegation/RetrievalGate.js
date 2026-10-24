'use strict';

/**
 * SPEC-102 — retrieval-before-delegation gate.
 *
 * Delegation is expensive. Thinking is cheaper. Memory retrieval is cheaper
 * still. Max only asks a specialist when new external work is required.
 */

const {
  COGNITIVE_MODES,
  NEVER_DELEGATE_MODES,
  classifyCognitiveMode,
  forbidsSpecialistDelegation,
  looksLikeInvestigation,
} = require('./CognitiveMode');

const SCOUT_REUSE_FOLLOWUP_RE =
  /\b(which (?:four|\d+)|why is (?:this|that)(?: one)? strongest|find more like|number (?:two|2)|more like number)\b/i;

const UNKNOWN_ANSWER = "I don't currently know.";

function modeKind(mode) {
  if (!mode) return COGNITIVE_MODES.UNCLASSIFIED;
  return typeof mode === 'string' ? mode : mode.kind;
}

/**
 * May Max create a new specialist delegation for this turn?
 */
function mayCreateDelegation(mode, extras = {}) {
  const kind = modeKind(mode);
  if (NEVER_DELEGATE_MODES.includes(kind)) return false;
  if (kind === COGNITIVE_MODES.PLANNING) return false;
  if (kind === COGNITIVE_MODES.UNCLASSIFIED) return false;

  if (kind === COGNITIVE_MODES.RECOMMENDATION) {
    return meetsInvestigationThreshold({
      ...extras,
      mode,
      requireExplicitInvestigation: true,
    });
  }

  if (kind === COGNITIVE_MODES.INVESTIGATION) return true;
  if (kind === COGNITIVE_MODES.EXECUTION) return true;
  return false;
}

/**
 * May the existing specialist path run (including reuse / explain)?
 * Follow-ups about prior Scout results may enter the path without
 * creating a new delegation.
 */
function mayEnterSpecialistPath(mode, extras = {}) {
  const question = String(extras.question || '');
  if (mayCreateDelegation(mode, extras)) return true;
  if (SCOUT_REUSE_FOLLOWUP_RE.test(question)) return true;
  return false;
}

/**
 * Investigation threshold from SPEC-102 §6.
 */
function meetsInvestigationThreshold(input = {}) {
  const question = String(input.question || '');
  const mode = input.mode || classifyCognitiveMode(question, input);
  const kind = modeKind(mode);

  if (kind === COGNITIVE_MODES.INVESTIGATION || mode.explicitInvestigation) {
    return true;
  }
  if (looksLikeInvestigation(question)) return true;
  if (input.operatorRequestedInvestigation === true) return true;
  if (input.needsExternalInformation === true) return true;
  if (input.knowledgeStale === true) return true;
  if (input.coverageInsufficient === true) return true;
  if (input.specialistUniqueCapability === true) return true;

  if (input.requireExplicitInvestigation) return false;
  return false;
}

function shouldInvokeSpecialist(question, input = {}) {
  const mode = input.mode || classifyCognitiveMode(question, input);
  if (forbidsSpecialistDelegation(mode)) return false;
  return mayCreateDelegation(mode, { ...input, question, mode });
}

module.exports = {
  UNKNOWN_ANSWER,
  SCOUT_REUSE_FOLLOWUP_RE,
  mayCreateDelegation,
  mayEnterSpecialistPath,
  meetsInvestigationThreshold,
  shouldInvokeSpecialist,
};
