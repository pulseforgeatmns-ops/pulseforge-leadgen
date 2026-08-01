'use strict';

/**
 * Execution Domain routing — operator intent owns the subsystem.
 *
 * Required sequence (never inverted):
 *   Operator Input
 *   → Intent Understanding
 *   → Select Execution Domain
 *   → Select/Attach Context
 *   → Execute
 *
 * The active conversation provides context only. It never selects the
 * execution domain or which subsystem handles the request.
 */

const {
  understandIntent,
  INTENT_CATEGORIES,
  INTENT_CONFIDENCE_THRESHOLD,
  resolveMissionTypeFromIntent,
  routeIntent,
  matchMissionType,
  isIntelligenceOnly,
  ROUTE_KINDS,
  MISSION_TYPES,
} = require('../../mission-engine');
const { normalizeContext } = require('./ContextEnvelope');
const { PAGE_TYPES } = require('./WorkspaceTypes');

/** Registered execution domains — each owns its requests once selected. */
const EXECUTION_DOMAINS = Object.freeze({
  MISSION_EXECUTION: 'mission_execution',
  MISSION_DIAGNOSTICS: 'mission_diagnostics',
  MORNING_BRIEFING: 'morning_briefing',
  MARKET_INTELLIGENCE: 'market_intelligence',
  WORKSPACE: 'workspace',
  GENERAL_CONVERSATION: 'general_conversation',
});

const MISSION_DOMAINS = new Set([
  EXECUTION_DOMAINS.MISSION_EXECUTION,
  EXECUTION_DOMAINS.MISSION_DIAGNOSTICS,
]);

/** Intent categories that mean Mission Diagnostics. */
const DIAGNOSTIC_CATEGORIES = new Set([
  INTENT_CATEGORIES.CAMPAIGN_DIAGNOSTICS,
  INTENT_CATEGORIES.DISCOVERY_INVESTIGATION,
  INTENT_CATEGORIES.DIAGNOSTICS,
]);

/** Intent categories that mean Mission Execution (non-diagnostic). */
const MISSION_EXECUTION_CATEGORIES = new Set([
  INTENT_CATEGORIES.CAMPAIGN_EXECUTION,
  INTENT_CATEGORIES.CAMPAIGN_REVIEW,
  INTENT_CATEGORIES.CAMPAIGN_CREATION,
  INTENT_CATEGORIES.PROSPECT_DISCOVERY,
  INTENT_CATEGORIES.GENERATE_MESSAGING,
  INTENT_CATEGORIES.BUILD_BUSINESS_INTELLIGENCE,
  INTENT_CATEGORIES.REVIEW_PROSPECT,
  INTENT_CATEGORIES.GENERATE_PROPOSAL,
  INTENT_CATEGORIES.MAIL_PACKAGE_GENERATION,
  INTENT_CATEGORIES.EXPORT_CAMPAIGN,
  INTENT_CATEGORIES.IMPORT_PROSPECT_LIST,
  INTENT_CATEGORIES.OUTCOME_INTELLIGENCE,
  INTENT_CATEGORIES.OPERATOR_INBOX,
]);

/**
 * @typedef {object} DomainDecision
 * @property {string} domain
 * @property {object|null} missionIntent
 * @property {string|null} missionType
 * @property {'mission'|'intelligence'} routeKind
 * @property {string} reason
 * @property {number} confidence
 * @property {boolean} domainSwitched
 * @property {string|null} previousDomain
 */

/**
 * Select the execution domain from operator input.
 * Intent Understanding runs first. Active conversation is ignored.
 *
 * @param {string} text
 * @param {object} [opts]
 * @param {string|null} [opts.previousDomain]
 * @returns {DomainDecision}
 */
function selectExecutionDomain(text, opts = {}) {
  const q = String(text || '').trim();
  const previousDomain = opts.previousDomain || null;

  if (!q) {
    return finalizeDomain({
      domain: EXECUTION_DOMAINS.GENERAL_CONVERSATION,
      missionIntent: null,
      missionType: null,
      routeKind: ROUTE_KINDS.INTELLIGENCE,
      reason: 'empty',
      confidence: 0,
      previousDomain,
    });
  }

  // 1) Intent Understanding — always first (SPEC-055 / ADR-039)
  const missionIntent = understandIntent(q);
  const category =
    missionIntent.intentCategory || missionIntent.matchedIntent || null;
  const confident =
    category &&
    category !== INTENT_CATEGORIES.UNKNOWN &&
    category !== INTENT_CATEGORIES.OPERATOR_HELP &&
    !missionIntent.needsClarification &&
    Number(missionIntent.confidence) >= INTENT_CONFIDENCE_THRESHOLD;

  if (confident && DIAGNOSTIC_CATEGORIES.has(category)) {
    return finalizeDomain({
      domain: EXECUTION_DOMAINS.MISSION_DIAGNOSTICS,
      missionIntent,
      missionType:
        resolveMissionTypeFromIntent(missionIntent) ||
        MISSION_TYPES.CAMPAIGN_REVIEW,
      routeKind: ROUTE_KINDS.MISSION,
      reason: `understood_${category}`,
      confidence: missionIntent.confidence,
      previousDomain,
    });
  }

  if (confident && MISSION_EXECUTION_CATEGORIES.has(category)) {
    return finalizeDomain({
      domain: EXECUTION_DOMAINS.MISSION_EXECUTION,
      missionIntent,
      missionType:
        resolveMissionTypeFromIntent(missionIntent) ||
        matchMissionType(q.toLowerCase(), q) ||
        MISSION_TYPES.CAMPAIGN_CREATION,
      routeKind: ROUTE_KINDS.MISSION,
      reason: `understood_${category}`,
      confidence: missionIntent.confidence,
      previousDomain,
    });
  }

  // 2) Keyword mission seed for categories Intent Understanding does not cover
  //    (overflow partner, acquisition, weekly brief, …) — still intent-driven,
  //    never conversation-driven.
  const keywordType = matchMissionType(q.toLowerCase(), q);
  if (keywordType) {
    return finalizeDomain({
      domain: EXECUTION_DOMAINS.MISSION_EXECUTION,
      missionIntent: missionIntent.confidence >= 0.35 ? missionIntent : null,
      missionType: keywordType,
      routeKind: ROUTE_KINDS.MISSION,
      reason: `matched_${keywordType}`,
      confidence: 0.9,
      previousDomain,
    });
  }

  // 3) Non-mission domains — still from operator language, not active convo
  const briefing = classifyMorningBriefing(q);
  if (briefing) {
    return finalizeDomain({
      domain: EXECUTION_DOMAINS.MORNING_BRIEFING,
      missionIntent: null,
      missionType: null,
      routeKind: ROUTE_KINDS.INTELLIGENCE,
      reason: briefing.reason,
      confidence: briefing.confidence,
      previousDomain,
    });
  }

  const market = classifyMarketIntelligence(q);
  if (market) {
    return finalizeDomain({
      domain: EXECUTION_DOMAINS.MARKET_INTELLIGENCE,
      missionIntent: null,
      missionType: null,
      routeKind: ROUTE_KINDS.INTELLIGENCE,
      reason: market.reason,
      confidence: market.confidence,
      previousDomain,
    });
  }

  const workspace = classifyWorkspace(q);
  if (workspace) {
    return finalizeDomain({
      domain: EXECUTION_DOMAINS.WORKSPACE,
      missionIntent: null,
      missionType: null,
      routeKind: ROUTE_KINDS.INTELLIGENCE,
      reason: workspace.reason,
      confidence: workspace.confidence,
      previousDomain,
    });
  }

  // Contextual intelligence Q&A (evidence / rank / why) stays on Workspace
  // when a prior workspace/briefing domain is active — reuse compatible context.
  if (isIntelligenceOnly(q.toLowerCase())) {
    const reuseWorkspace =
      previousDomain === EXECUTION_DOMAINS.WORKSPACE ||
      previousDomain === EXECUTION_DOMAINS.MORNING_BRIEFING ||
      previousDomain == null;
    return finalizeDomain({
      domain: reuseWorkspace
        ? EXECUTION_DOMAINS.WORKSPACE
        : EXECUTION_DOMAINS.MARKET_INTELLIGENCE,
      missionIntent: null,
      missionType: null,
      routeKind: ROUTE_KINDS.INTELLIGENCE,
      reason: 'intelligence_specific',
      confidence: 0.85,
      previousDomain,
    });
  }

  return finalizeDomain({
    domain: EXECUTION_DOMAINS.GENERAL_CONVERSATION,
    missionIntent:
      category === INTENT_CATEGORIES.OPERATOR_HELP ? missionIntent : null,
    missionType: null,
    routeKind: ROUTE_KINDS.INTELLIGENCE,
    reason:
      category === INTENT_CATEGORIES.OPERATOR_HELP
        ? 'operator_help'
        : 'general_conversation',
    confidence: Number(missionIntent.confidence) || 0.2,
    previousDomain,
  });
}

/**
 * After domain selection: reuse compatible context or attach a domain-owned one.
 * Previous conversations are preserved on the session; they do not intercept.
 *
 * @param {object} input
 * @param {object} input.session
 * @param {DomainDecision} input.decision
 * @param {object|null} [input.incomingContext] - raw envelope from client
 * @param {object|null} [input.mission]
 * @returns {{ context: object, contextSwitch: string|null, domainSwitch: string|null, executionContext: object }}
 */
function attachDomainContext(input) {
  const session = input.session;
  const decision = input.decision;
  const mission = input.mission || null;

  const base =
    input.incomingContext && typeof input.incomingContext === 'object'
      ? normalizeContext({
          ...session.context,
          ...input.incomingContext,
          tenantId:
            (input.incomingContext.tenantId != null
              ? input.incomingContext.tenantId
              : session.context.tenantId),
        })
      : session.context;

  const previousDomain = session.executionDomain || null;
  const domainSwitched =
    previousDomain != null && previousDomain !== decision.domain;

  let context = { ...base };
  let domainSwitch = null;

  if (isMissionDomain(decision.domain)) {
    // Mission owns the request — briefing/market surfaces must not answer.
    context = {
      ...context,
      page: PAGE_TYPES.COMMAND_DECK,
      executionDomain: decision.domain,
      missionFocus: mission
        ? {
            id: mission.id,
            title: mission.title || null,
            type: mission.type || decision.missionType,
            status: mission.status || null,
          }
        : {
            id: null,
            title: null,
            type: decision.missionType,
            status: 'planning',
          },
      // Keep briefing/deck as ambient evidence only; ResponseComposer must not
      // treat them as the answer corpus for mission domains.
      _answerCorpus: 'mission',
    };
    if (domainSwitched || previousDomain == null) {
      domainSwitch = domainSwitchMessage(decision.domain, previousDomain, mission);
    }
  } else if (decision.domain === EXECUTION_DOMAINS.MORNING_BRIEFING) {
    context = {
      ...context,
      executionDomain: decision.domain,
      _answerCorpus: 'briefing',
      missionFocus: null,
    };
    if (domainSwitched) {
      domainSwitch = domainSwitchMessage(decision.domain, previousDomain, null);
    }
  } else if (decision.domain === EXECUTION_DOMAINS.MARKET_INTELLIGENCE) {
    context = {
      ...context,
      // Keep the operator's page envelope; domain owns the answer corpus only.
      executionDomain: decision.domain,
      _answerCorpus: 'market',
      missionFocus: null,
    };
    if (domainSwitched) {
      domainSwitch = domainSwitchMessage(decision.domain, previousDomain, null);
    }
  } else if (decision.domain === EXECUTION_DOMAINS.WORKSPACE) {
    context = {
      ...context,
      executionDomain: decision.domain,
      _answerCorpus: 'workspace',
      missionFocus: null,
    };
    if (domainSwitched) {
      domainSwitch = domainSwitchMessage(decision.domain, previousDomain, null);
    }
  } else {
    context = {
      ...context,
      executionDomain: decision.domain,
      _answerCorpus: 'general',
      missionFocus: null,
    };
    if (domainSwitched) {
      domainSwitch = domainSwitchMessage(decision.domain, previousDomain, null);
    }
  }

  session.executionDomain = decision.domain;
  session.previousExecutionDomain = previousDomain;
  session.context = context;
  session.updatedAt = new Date().toISOString();

  return {
    context,
    contextSwitch: null,
    domainSwitch,
    executionContext: {
      domain: decision.domain,
      previousDomain,
      domainSwitched: Boolean(domainSwitched || (domainSwitch && previousDomain == null && isMissionDomain(decision.domain))),
      answerCorpus: context._answerCorpus,
      missionFocus: context.missionFocus || null,
    },
  };
}

/**
 * Whether the domain must invoke the Mission Engine.
 * @param {string} domain
 */
function isMissionDomain(domain) {
  return MISSION_DOMAINS.has(domain);
}

/**
 * Bridge to legacy routeIntent shape for Mission Engine callers.
 * Domain selection remains authoritative; this is a compatibility view.
 * @param {DomainDecision} decision
 */
function toRouteDecision(decision) {
  return {
    kind: decision.routeKind,
    missionType: decision.missionType,
    reason: decision.reason,
    missionIntent: decision.missionIntent,
    executionDomain: decision.domain,
  };
}

function finalizeDomain(partial) {
  const domainSwitched =
    partial.previousDomain != null &&
    partial.previousDomain !== partial.domain;
  return {
    domain: partial.domain,
    missionIntent: partial.missionIntent || null,
    missionType: partial.missionType || null,
    routeKind: partial.routeKind,
    reason: partial.reason,
    confidence: Number(partial.confidence) || 0,
    domainSwitched,
    previousDomain: partial.previousDomain || null,
  };
}

function classifyMorningBriefing(q) {
  const lower = q.toLowerCase();
  if (
    /\bmorning\s+brief(ing)?\b/.test(lower) ||
    /\btoday'?s\s+brief(ing)?\b/.test(lower) ||
    /\bshow\s+(me\s+)?(the\s+)?brief(ing)?\b/.test(lower) ||
    /\bwhat('?s| is)\s+(in\s+)?(my\s+)?(morning\s+)?brief/.test(lower) ||
    /\bovernight\s+(summary|changes|movement)\b/.test(lower) ||
    /\bbriefing\s+(headline|summary|priorities)\b/.test(lower)
  ) {
    return { reason: 'morning_briefing', confidence: 0.9 };
  }
  return null;
}

function classifyMarketIntelligence(q) {
  const lower = q.toLowerCase();
  if (
    /\bmarket\s+(intelligence|view|overview)\b/.test(lower) ||
    /\bmonitor\b/.test(lower) ||
    /\bsummarize\b/.test(lower) ||
    /\bcompetitor\s+changes\b/.test(lower) ||
    /\bshow\s+competitor\b/.test(lower)
  ) {
    return { reason: 'market_intelligence', confidence: 0.88 };
  }
  return null;
}

function classifyWorkspace(q) {
  const lower = q.toLowerCase();
  if (
    /\bopen\s+(the\s+)?workspace\b/.test(lower) ||
    /\bcommand\s+deck\b/.test(lower) ||
    /\bshow\s+(the\s+)?(priority\s+)?queue\b/.test(lower) ||
    /\bhighest\s+leverage\b/.test(lower) ||
    // Active desk / canary cues — prefer Workspace over General Conversation
    // even when session activeWorkContext was not restored.
    /\bfillable\s+(?:verification\s+)?table\b/.test(lower) ||
    /\bupdate\s+(?:the\s+)?(?:fillable\s+)?(?:verification\s+)?table\b/.test(
      lower
    ) ||
    /\bverification\s+work\s+order\b/.test(lower) ||
    /\bpreparation[-\s]*only\s+canary\b/.test(lower) ||
    /\bcampaign\s+\d+\s+preparation[-\s]*only\b/.test(lower) ||
    /\bsame\s+(?:\d+\s+)?prospects?\b/.test(lower)
  ) {
    return { reason: 'workspace', confidence: 0.85 };
  }
  return null;
}

function domainSwitchMessage(domain, previousDomain, mission) {
  const labels = {
    [EXECUTION_DOMAINS.MISSION_EXECUTION]: 'Mission Workspace',
    [EXECUTION_DOMAINS.MISSION_DIAGNOSTICS]: 'Mission Diagnostics',
    [EXECUTION_DOMAINS.MORNING_BRIEFING]: 'Morning Briefing',
    [EXECUTION_DOMAINS.MARKET_INTELLIGENCE]: 'Market Intelligence',
    [EXECUTION_DOMAINS.WORKSPACE]: 'Workspace',
    [EXECUTION_DOMAINS.GENERAL_CONVERSATION]: 'General Conversation',
  };
  const next = labels[domain] || domain;
  if (!previousDomain) {
    if (isMissionDomain(domain)) {
      return mission && mission.title
        ? `Opening ${next} for ${mission.title}.`
        : `Opening ${next}.`;
    }
    return null;
  }
  const prev = labels[previousDomain] || previousDomain;
  return `Switching from ${prev} to ${next}.`;
}

/**
 * Compatibility: derive domain from a legacy routeIntent decision.
 * Prefer selectExecutionDomain for new call sites.
 * @param {string} text
 */
function selectExecutionDomainCompat(text, opts = {}) {
  const decision = selectExecutionDomain(text, opts);
  // Keep routeIntent available for diagnostics / golden comparisons
  void routeIntent;
  return decision;
}

module.exports = {
  EXECUTION_DOMAINS,
  MISSION_DOMAINS,
  DIAGNOSTIC_CATEGORIES,
  MISSION_EXECUTION_CATEGORIES,
  selectExecutionDomain,
  selectExecutionDomainCompat,
  attachDomainContext,
  isMissionDomain,
  toRouteDecision,
  classifyMorningBriefing,
  classifyMarketIntelligence,
  classifyWorkspace,
};
