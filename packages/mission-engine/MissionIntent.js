'use strict';

/**
 * MissionIntent — semantic intermediate between operator language and
 * executable MissionPlan (SPEC-055 / ADR-039).
 *
 * MissionIntent is descriptive. MissionPlan remains executable.
 * Capabilities never parse language; they consume MissionPlan only.
 */

const MISSION_INTENT_VERSION = '1.0.0';

/** Intent categories — goals, not capability ids. */
const INTENT_CATEGORIES = Object.freeze({
  CAMPAIGN_EXECUTION: 'campaign_execution',
  CAMPAIGN_REVIEW: 'campaign_review',
  CAMPAIGN_CREATION: 'campaign_creation',
  CAMPAIGN_DIAGNOSTICS: 'campaign_diagnostics',
  DISCOVERY_INVESTIGATION: 'discovery_investigation',
  PROSPECT_DISCOVERY: 'prospect_discovery',
  GENERATE_MESSAGING: 'generate_messaging',
  BUILD_BUSINESS_INTELLIGENCE: 'build_business_intelligence',
  REVIEW_PROSPECT: 'review_prospect',
  GENERATE_PROPOSAL: 'generate_proposal',
  MAIL_PACKAGE_GENERATION: 'mail_package_generation',
  EXPORT_CAMPAIGN: 'export_campaign',
  IMPORT_PROSPECT_LIST: 'import_prospect_list',
  OUTCOME_INTELLIGENCE: 'outcome_intelligence',
  OPERATOR_INBOX: 'operator_inbox',
  OPERATOR_HELP: 'operator_help',
  DIAGNOSTICS: 'diagnostics',
  UNKNOWN: 'unknown',
});

const INTENT_LABELS = Object.freeze({
  [INTENT_CATEGORIES.CAMPAIGN_EXECUTION]: 'Campaign Execution',
  [INTENT_CATEGORIES.CAMPAIGN_REVIEW]: 'Campaign Review',
  [INTENT_CATEGORIES.CAMPAIGN_CREATION]: 'Campaign Creation',
  [INTENT_CATEGORIES.CAMPAIGN_DIAGNOSTICS]: 'Campaign Diagnostics',
  [INTENT_CATEGORIES.DISCOVERY_INVESTIGATION]: 'Discovery Investigation',
  [INTENT_CATEGORIES.PROSPECT_DISCOVERY]: 'Prospect Discovery',
  [INTENT_CATEGORIES.GENERATE_MESSAGING]: 'Generate Messaging',
  [INTENT_CATEGORIES.BUILD_BUSINESS_INTELLIGENCE]: 'Build Business Intelligence',
  [INTENT_CATEGORIES.REVIEW_PROSPECT]: 'Review Prospect',
  [INTENT_CATEGORIES.GENERATE_PROPOSAL]: 'Generate Proposal',
  [INTENT_CATEGORIES.MAIL_PACKAGE_GENERATION]: 'Mail Package Generation',
  [INTENT_CATEGORIES.EXPORT_CAMPAIGN]: 'Export Campaign',
  [INTENT_CATEGORIES.IMPORT_PROSPECT_LIST]: 'Import Prospect List',
  [INTENT_CATEGORIES.OUTCOME_INTELLIGENCE]: 'Outcome Intelligence',
  [INTENT_CATEGORIES.OPERATOR_INBOX]: 'Operator Inbox',
  [INTENT_CATEGORIES.OPERATOR_HELP]: 'Operator Help',
  [INTENT_CATEGORIES.DIAGNOSTICS]: 'Diagnostics',
  [INTENT_CATEGORIES.UNKNOWN]: 'Unknown',
});

const INTENT_DOMAINS = Object.freeze({
  DIRECT_MAIL: 'direct_mail',
  DISCOVERY: 'discovery',
  INTELLIGENCE: 'intelligence',
  PROPOSAL: 'proposal',
  CAMPAIGN: 'campaign',
  OPERATOR: 'operator',
  GENERAL: 'general',
});

const INTENT_MODES = Object.freeze({
  EXECUTION: 'execution',
  REVIEW: 'review',
  INVESTIGATION: 'investigation',
  DIAGNOSTICS: 'diagnostics',
  GENERATION: 'generation',
  HELP: 'help',
});

/** Auto-proceed when confidence >= this; otherwise ask for clarification. */
const INTENT_CONFIDENCE_THRESHOLD = 0.75;

/**
 * Default evidence requirements by intent (SPEC-056).
 * Descriptive only — does not choose capabilities.
 * Kept here so MissionIntent can declare requiresEvidence without importing
 * the Evidence Planner (avoids circular deps).
 */
const DEFAULT_INTENT_EVIDENCE = Object.freeze({
  [INTENT_CATEGORIES.CAMPAIGN_DIAGNOSTICS]: Object.freeze([
    'DiscoveryExecution',
    'DiscoveryTrace',
    'DiscoveryDiagnostics',
    'MissionState',
  ]),
  [INTENT_CATEGORIES.DISCOVERY_INVESTIGATION]: Object.freeze([
    'DiscoveryExecution',
    'ProviderSelection',
    'CandidateCounts',
    'VerificationResults',
    'Exceptions',
    'DiscoveryTrace',
    'DiscoveryDiagnostics',
  ]),
  [INTENT_CATEGORIES.DIAGNOSTICS]: Object.freeze([
    'MissionState',
    'MissionDiagnostics',
    'CapabilityExecution',
    'CapabilityFailure',
  ]),
  [INTENT_CATEGORIES.CAMPAIGN_REVIEW]: Object.freeze(['MissionState']),
  [INTENT_CATEGORIES.OUTCOME_INTELLIGENCE]: Object.freeze(['MissionState']),
});

/**
 * @param {object} [partial]
 * @returns {object} mission_intent (frozen)
 */
function buildMissionIntent(partial = {}) {
  const category =
    partial.intentCategory ||
    partial.matchedIntent ||
    INTENT_CATEGORIES.UNKNOWN;
  const matchedIntent = partial.matchedIntent || category;
  const confidence = clampConfidence(partial.confidence);
  const alternateIntents = normalizeAlternates(partial.alternateIntents);
  const needsClarification =
    partial.needsClarification != null
      ? Boolean(partial.needsClarification)
      : category === INTENT_CATEGORIES.UNKNOWN ||
        confidence < INTENT_CONFIDENCE_THRESHOLD;

  const target =
    partial.target && typeof partial.target === 'object'
      ? { ...partial.target }
      : {};
  const constraints =
    partial.constraints && typeof partial.constraints === 'object'
      ? { ...partial.constraints }
      : {};
  const parameters =
    partial.parameters && typeof partial.parameters === 'object'
      ? { ...partial.parameters }
      : {};
  const options =
    partial.options && typeof partial.options === 'object'
      ? { ...partial.options }
      : {};

  const requiresEvidence = normalizeEvidenceList(
    partial.requiresEvidence != null
      ? partial.requiresEvidence
      : DEFAULT_INTENT_EVIDENCE[matchedIntent] ||
          DEFAULT_INTENT_EVIDENCE[category] ||
          []
  );

  return Object.freeze({
    version: partial.version || MISSION_INTENT_VERSION,
    goal: String(partial.goal || '').trim(),
    intentCategory: category,
    matchedIntent,
    domain: partial.domain || INTENT_DOMAINS.GENERAL,
    mode: partial.mode || INTENT_MODES.EXECUTION,
    target: Object.freeze(target),
    constraints: Object.freeze(constraints),
    parameters: Object.freeze(parameters),
    options: Object.freeze(options),
    diagnostics: Boolean(partial.diagnostics),
    requiresEvidence: Object.freeze(requiresEvidence),
    confidence,
    alternateIntents: Object.freeze(alternateIntents),
    needsClarification,
    clarificationPrompt: partial.clarificationPrompt
      ? String(partial.clarificationPrompt)
      : needsClarification
        ? defaultClarificationPrompt(matchedIntent, alternateIntents)
        : null,
    notes: Object.freeze(
      Array.isArray(partial.notes)
        ? partial.notes.map((n) => String(n).trim()).filter(Boolean)
        : []
    ),
    sourceText:
      partial.sourceText != null ? String(partial.sourceText) : null,
    label: INTENT_LABELS[matchedIntent] || INTENT_LABELS[category] || category,
    createdAt: partial.createdAt || new Date().toISOString(),
  });
}

/**
 * Operator-facing summary for Review Workspace (SPEC-055).
 * @param {object} intent
 * @returns {object}
 */
function summarizeMissionIntent(intent) {
  const i =
    intent && typeof intent === 'object'
      ? intent
      : buildMissionIntent({});
  const campaign =
    (i.target && i.target.campaign) ||
    (i.parameters && i.parameters.campaign) ||
    null;
  const subject =
    (i.target && i.target.subject) ||
    (i.parameters && (i.parameters.client || i.parameters.subject)) ||
    null;
  return {
    operatorRequest: i.sourceText || i.goal || '',
    goal: i.goal || '',
    understoodIntent: i.label || INTENT_LABELS[i.matchedIntent] || i.matchedIntent,
    intentCategory: i.intentCategory || i.matchedIntent,
    confidence: i.confidence,
    confidencePercent:
      i.confidence != null ? Math.round(Number(i.confidence) * 100) : null,
    domain: i.domain || null,
    mode: i.mode || null,
    diagnostics: Boolean(i.diagnostics),
    requiresEvidence: Array.isArray(i.requiresEvidence)
      ? [...i.requiresEvidence]
      : [],
    target: {
      campaign: campaign || null,
      subject: subject || null,
    },
    alternateIntents: (i.alternateIntents || []).map((a) => ({
      intent: a.intent || a.category,
      label: a.label || INTENT_LABELS[a.intent || a.category] || a.intent,
      confidence: a.confidence,
    })),
    needsClarification: Boolean(i.needsClarification),
    clarificationPrompt: i.clarificationPrompt || null,
  };
}

function clampConfidence(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 0;
  if (n < 0) return 0;
  if (n > 1) return 1;
  return n;
}

function normalizeAlternates(list) {
  if (!Array.isArray(list)) return [];
  return list
    .filter((a) => a && (a.intent || a.category))
    .map((a) => {
      const intent = a.intent || a.category;
      return Object.freeze({
        intent,
        category: intent,
        label: a.label || INTENT_LABELS[intent] || intent,
        confidence: clampConfidence(a.confidence),
      });
    });
}

function defaultClarificationPrompt(matched, alternates) {
  const primary = INTENT_LABELS[matched] || matched || 'your request';
  const alts = (alternates || [])
    .slice(0, 3)
    .map((a) => a.label || INTENT_LABELS[a.intent] || a.intent)
    .filter(Boolean);
  if (!alts.length) {
    return `I'm not sure what you want to accomplish. Did you mean ${primary}?`;
  }
  return `I'm not sure what you want to accomplish. Did you mean ${primary}, or ${alts.join(', ')}?`;
}

function intentLabel(category) {
  return INTENT_LABELS[category] || category || 'Unknown';
}

function normalizeEvidenceList(list) {
  if (!Array.isArray(list)) return [];
  const seen = new Set();
  const out = [];
  for (const item of list) {
    const t = String(item || '').trim();
    if (!t || seen.has(t)) continue;
    seen.add(t);
    out.push(t);
  }
  return out;
}

/**
 * @param {string} category
 * @returns {string[]}
 */
function defaultEvidenceForIntent(category) {
  const mapped = DEFAULT_INTENT_EVIDENCE[category];
  return mapped ? [...mapped] : [];
}

module.exports = {
  MISSION_INTENT_VERSION,
  INTENT_CATEGORIES,
  INTENT_LABELS,
  INTENT_DOMAINS,
  INTENT_MODES,
  INTENT_CONFIDENCE_THRESHOLD,
  DEFAULT_INTENT_EVIDENCE,
  buildMissionIntent,
  summarizeMissionIntent,
  intentLabel,
  defaultEvidenceForIntent,
};
