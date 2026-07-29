'use strict';

/**
 * IntentRouter — cold-start Mission vs Intelligence gate
 * (SPEC-022 / ADR-027 / SPEC-055 / ADR-039).
 *
 * Decides: Is this a Mission?
 * MissionPlanner decides: How do we accomplish it? (execution graph)
 *
 * Operator intent is the authority — never the currently displayed
 * conversation (Morning Brief, Command Deck, etc.). Keyword matching is a
 * fast path; Intent Understanding owns semantic mission detection so
 * MissionIntent creation is not bypassed when keywords miss.
 *
 * Stage keywords (review, mail package, ready to print) must not collapse a
 * multi-outcome Build Campaign objective into a single-stage Mission type.
 * Prefer broad campaign / discovery intents; let the planner augment stages.
 */

const { MISSION_TYPES, ROUTE_KINDS } = require('./types');

/**
 * @typedef {object} RouteDecision
 * @property {'mission'|'intelligence'} kind
 * @property {string|null} missionType
 * @property {string} reason
 * @property {object|null} [missionIntent] - MissionIntent when understood
 */

/**
 * Route an operator prompt.
 * @param {string} objective
 * @returns {RouteDecision}
 */
function routeIntent(objective) {
  const q = String(objective || '').trim();
  if (!q) {
    return {
      kind: ROUTE_KINDS.INTELLIGENCE,
      missionType: null,
      reason: 'empty',
      missionIntent: null,
    };
  }

  const lower = q.toLowerCase();

  // 1) Intent Understanding first (SPEC-055) — operator intent owns routing
  const understood = routeFromIntentUnderstanding(q);
  if (understood) {
    return understood;
  }

  // 2) Keyword mission seed for types Understanding does not yet cover
  const missionType = matchMissionType(lower, q);
  if (missionType) {
    return {
      kind: ROUTE_KINDS.MISSION,
      missionType,
      reason: `matched_${missionType}`,
      missionIntent: null,
    };
  }

  // 3) Intelligence-specific Q&A (monitor / summarize / explain) — not missions
  if (isIntelligenceOnly(lower)) {
    return {
      kind: ROUTE_KINDS.INTELLIGENCE,
      missionType: null,
      reason: 'intelligence_specific',
      missionIntent: null,
    };
  }

  // Default: questions / explain / investigate stay on intelligence surface
  return {
    kind: ROUTE_KINDS.INTELLIGENCE,
    missionType: null,
    reason: 'default_intelligence',
    missionIntent: null,
  };
}

/**
 * Elevate confident MissionIntent categories to Mission routing.
 * @param {string} objective
 * @returns {RouteDecision|null}
 */
function routeFromIntentUnderstanding(objective) {
  // Lazy requires avoid circular load with MissionPlanner ↔ IntentRouter.
  const { understandIntent } = require('./IntentUnderstanding');
  const { resolveMissionTypeFromIntent } = require('./CapabilityPlanner');
  const {
    INTENT_CATEGORIES,
    INTENT_CONFIDENCE_THRESHOLD,
  } = require('./MissionIntent');

  const missionIntent = understandIntent(objective);
  const category =
    missionIntent.intentCategory || missionIntent.matchedIntent || null;

  // Help / unknown stay on the intelligence surface (Q&A), not Mission Engine.
  if (
    !category ||
    category === INTENT_CATEGORIES.UNKNOWN ||
    category === INTENT_CATEGORIES.OPERATOR_HELP
  ) {
    return null;
  }

  if (
    missionIntent.needsClarification ||
    Number(missionIntent.confidence) < INTENT_CONFIDENCE_THRESHOLD
  ) {
    return null;
  }

  const missionType = resolveMissionTypeFromIntent(missionIntent);
  if (!missionType) {
    return null;
  }

  return {
    kind: ROUTE_KINDS.MISSION,
    missionType,
    reason: `understood_${category}`,
    missionIntent,
  };
}

/**
 * @param {string} lower
 * @param {string} original
 * @returns {string|null}
 */
function matchMissionType(lower, original) {
  // SPEC-041: broad build/create campaign wins over later-stage keywords
  // (review / mail package / ready to print). Planner augments the graph.
  const isCampaignBuild =
    /\bbuild\s+campaign\b/.test(lower) ||
    /\bcreate\s+campaign\b/.test(lower) ||
    /\bnew\s+campaign\b/.test(lower) ||
    /\blaunch\s+campaign\b/.test(lower) ||
    /\bprepare\s+(a\s+)?campaign\b/.test(lower) ||
    /\bbuild\s+(a\s+)?(q\d\s+)?outreach\s+campaign\b/.test(lower);

  if (isCampaignBuild) {
    return MISSION_TYPES.CAMPAIGN_CREATION;
  }

  // Operator Inbox (SPEC-037) — coordination surface
  if (
    /\boperator\s+inbox\b/.test(lower) ||
    /\bshow\s+(my\s+)?inbox\b/.test(lower) ||
    /\bopen\s+(the\s+)?inbox\b/.test(lower) ||
    /\binbox\s+items?\b/.test(lower) ||
    /\boutstanding\s+work\b/.test(lower) ||
    /\bwhat\s+needs\s+(my\s+)?attention\b/.test(lower)
  ) {
    return MISSION_TYPES.OPERATOR_INBOX;
  }

  // Outcome Intelligence (SPEC-036) — after execution / response capture
  if (
    /\boutcome\s+intelligence\b/.test(lower) ||
    /\bcapture\s+(campaign\s+)?outcomes?\b/.test(lower) ||
    /\bcampaign\s+outcomes?\b/.test(lower) ||
    /\blearnings?\s+from\s+(the\s+)?campaign\b/.test(lower) ||
    /\boutcome\s+summary\b/.test(lower) ||
    /\breview\s+(campaign\s+)?recommendations\b/.test(lower) ||
    /\bconclude\s+(the\s+)?mission\b/.test(lower)
  ) {
    return MISSION_TYPES.OUTCOME_INTELLIGENCE;
  }

  // Direct Mail Execution (SPEC-035) — focused objective only
  if (
    /\bdirect\s+mail\s+execution\b/.test(lower) ||
    /\bexecute\s+(the\s+)?(direct\s+)?mail\b/.test(lower) ||
    /\bexecute\s+(the\s+)?campaign\b/.test(lower) ||
    /\bprint\s+(and\s+)?mail\b/.test(lower) ||
    /\bmail\s+(the\s+)?campaign\b/.test(lower) ||
    /\bmark\s+(all\s+)?mailed\b/.test(lower) ||
    /\bprint\s+session\b/.test(lower) ||
    /\bassemble\s+(mail\s+)?packages?\b/.test(lower)
  ) {
    return MISSION_TYPES.DIRECT_MAIL_EXECUTION;
  }

  // Campaign Review (SPEC-034) — focused review-only objectives
  if (
    /\bcampaign\s+review\b/.test(lower) ||
    /\breview\s+(the\s+)?campaign\b/.test(lower) ||
    /\breview\s+campaign\s+\d+\b/.test(lower) ||
    /\bapprove\s+(the\s+)?campaign\b/.test(lower) ||
    /\bcampaign\s+approval\b/.test(lower) ||
    /\bready\s+to\s+print\b/.test(lower)
  ) {
    return MISSION_TYPES.CAMPAIGN_REVIEW;
  }

  // Mail Package Generation (SPEC-033) — focused mail-only objectives
  if (
    /\b(generate|create|build|prepare|print)\s+(a\s+)?(mail|direct\s*mail)\s+packages?\b/.test(
      lower
    ) ||
    /\bmail\s+packages?\b/.test(lower) ||
    /\bdirect\s+mail\s+packages?\b/.test(lower) ||
    /\bmail\s+merge\b/.test(lower) ||
    /\baddress\s+labels?\b/.test(lower) ||
    /\bprint[- ]ready\s+(mail|letters?)\b/.test(lower)
  ) {
    return MISSION_TYPES.MAIL_PACKAGE_GENERATION;
  }

  // Campaign Creation — numeric campaign id still seeds a full pipeline,
  // unless the operator is asking for diagnostics / investigation (SPEC-055).
  if (/\bcampaign\s+\d+\b/.test(lower)) {
    if (
      /\b(audit|diagnose|diagnostic|diagnostics)\b/.test(lower) ||
      /\bwhat('?s| is)\s+wrong\b/.test(lower) ||
      /\bwhy\s+(isn'?t|is\s+not|didn'?t|doesn'?t|won'?t|did|does|would|failed)\b/.test(
        lower
      ) ||
      /\bfigure\s+out\s+why\b/.test(lower) ||
      /\b(failed|failing|broken|stuck)\b/.test(lower)
    ) {
      // Defer to Intent Understanding — do not collapse into creation.
      return null;
    }
    return MISSION_TYPES.CAMPAIGN_CREATION;
  }

  // Overflow Partner Search (before generic prospect discovery)
  if (
    /\boverflow\b/.test(lower) ||
    /\bpartner\s+search\b/.test(lower) ||
    /\boverflow\s+partner\b/.test(lower)
  ) {
    return MISSION_TYPES.OVERFLOW_PARTNER_SEARCH;
  }

  // Acquisition Search
  if (
    /\bacquisition\b/.test(lower) ||
    /\bapproaching\s+retirement\b/.test(lower) ||
    /\bowners?\s+retiring\b/.test(lower) ||
    /\bbuy\s+(a\s+)?(business|company)\b/.test(lower)
  ) {
    return MISSION_TYPES.ACQUISITION_SEARCH;
  }

  // Competitor Research / Intelligence as mission (research work, not "monitor X")
  if (
    /\bcompetitor\s+research\b/.test(lower) ||
    /\bresearch\s+(our\s+)?competitors?\b/.test(lower) ||
    /\bcompetitor\s+intelligence\b/.test(lower)
  ) {
    return MISSION_TYPES.COMPETITOR_RESEARCH;
  }

  // Knowledge Refresh
  if (
    /\bknowledge\s+refresh\b/.test(lower) ||
    /\brefresh\s+knowledge\b/.test(lower) ||
    /\bsync\s+knowledge\b/.test(lower)
  ) {
    return MISSION_TYPES.KNOWLEDGE_REFRESH;
  }

  // Weekly Brief (mission to generate)
  if (
    /\bweekly\s+brief\b/.test(lower) ||
    /\bgenerate\s+(a\s+)?weekly\s+brief\b/.test(lower) ||
    /\bbuild\s+(a\s+)?weekly\s+brief\b/.test(lower)
  ) {
    return MISSION_TYPES.WEEKLY_BRIEF;
  }

  // Proposal Generation (SPEC-027B) — before generic campaign patterns
  if (
    /\b(generate|create|draft|write|build)\s+(a\s+)?(sales\s+)?proposal\b/.test(
      lower
    ) ||
    /\bproposal\s+for\b/.test(lower) ||
    /\bcommercial\s+growth\s+proposal\b/.test(lower) ||
    /\bproposal\s+generator\b/.test(lower)
  ) {
    return MISSION_TYPES.PROPOSAL_GENERATION;
  }

  // Market Research (as business objective — not "summarize Nvidia")
  if (
    /\bmarket\s+research\b/.test(lower) ||
    /\bresearch\s+the\s+.+\s+market\b/.test(lower)
  ) {
    return MISSION_TYPES.MARKET_RESEARCH;
  }

  // Prospect Discovery
  if (
    /\bfind\s+(the\s+)?(best\s+)?\d*\s*(commercial\s+)?(cleaning\s+)?prospects?\b/.test(
      lower
    ) ||
    /\bdiscover\s+(prospects?|companies|leads)\b/.test(lower) ||
    /\bprospect\s+discovery\b/.test(lower) ||
    /\bfind\s+\d+\s+(prospects?|leads|companies)\b/.test(lower) ||
    /\bfind\s+.+\s+prospects?\s+in\b/.test(lower)
  ) {
    return MISSION_TYPES.PROSPECT_DISCOVERY;
  }

  void original;
  return null;
}

/**
 * True when the prompt is clearly market/intelligence Q&A, not a mission.
 * @param {string} lower
 */
function isIntelligenceOnly(lower) {
  if (
    /^(monitor|summarize|show|explain|why|what|how|compare)\b/.test(lower)
  ) {
    // "Show me competitor changes" / "Monitor Microsoft" / "Summarize Nvidia"
    if (
      /\bmonitor\b/.test(lower) ||
      /\bsummarize\b/.test(lower) ||
      /\bcompetitor\s+changes\b/.test(lower) ||
      /\bwhy\s+is\b/.test(lower) ||
      /\branked\b/.test(lower) ||
      /\bconfidence\b/.test(lower) ||
      /\bevidence\b/.test(lower) ||
      /\bovernight\b/.test(lower) ||
      /\bchanged\b/.test(lower) ||
      /\bwatch\b/.test(lower) ||
      /\balert\b/.test(lower)
    ) {
      return true;
    }
  }

  if (/\bmonitor\s+[a-z0-9][\w.-]*/i.test(lower)) return true;
  if (/\bsummarize\s+[a-z0-9][\w.-]*/i.test(lower)) return true;
  if (/\bshow\s+competitor\b/.test(lower)) return true;
  if (/\bcompetitor\s+changes\b/.test(lower)) return true;

  return false;
}

module.exports = {
  routeIntent,
  routeFromIntentUnderstanding,
  matchMissionType,
  isIntelligenceOnly,
  ROUTE_KINDS,
  MISSION_TYPES,
};
