'use strict';

/**
 * Intent Understanding — operator language → MissionIntent (SPEC-055 / ADR-039).
 *
 * Answers: What is the operator trying to accomplish?
 * Not: Which capability name appears in the sentence?
 *
 * Capabilities never parse language. This module owns language.
 */

const {
  INTENT_CATEGORIES,
  INTENT_DOMAINS,
  INTENT_MODES,
  INTENT_LABELS,
  INTENT_CONFIDENCE_THRESHOLD,
  buildMissionIntent,
} = require('./MissionIntent');

/**
 * @typedef {object} IntentCandidate
 * @property {string} intent
 * @property {number} confidence
 * @property {string} [goal]
 * @property {string} [domain]
 * @property {string} [mode]
 * @property {boolean} [diagnostics]
 */

/**
 * Understand operator natural language into a MissionIntent.
 * @param {string} text
 * @param {object} [opts]
 * @returns {object} mission_intent
 */
function understandIntent(text, opts = {}) {
  const sourceText = String(text || '').trim();
  if (!sourceText) {
    return buildMissionIntent({
      goal: '',
      intentCategory: INTENT_CATEGORIES.UNKNOWN,
      matchedIntent: INTENT_CATEGORIES.UNKNOWN,
      confidence: 0,
      needsClarification: true,
      clarificationPrompt: 'What would you like to accomplish?',
      sourceText: '',
      createdAt: opts.now || new Date().toISOString(),
    });
  }

  const lower = sourceText.toLowerCase().replace(/\s+/g, ' ').trim();
  const extracted = extractTargets(sourceText, lower);
  const scored = scoreIntentCandidates(lower, sourceText, extracted);
  scored.sort((a, b) => b.confidence - a.confidence);

  const best = scored[0] || {
    intent: INTENT_CATEGORIES.UNKNOWN,
    confidence: 0,
    goal: sourceText.split(/[.!?]/)[0].trim() || sourceText,
    domain: INTENT_DOMAINS.GENERAL,
    mode: INTENT_MODES.HELP,
    diagnostics: false,
  };

  const alternates = scored
    .slice(1)
    .filter((c) => c.confidence >= 0.35 && c.intent !== best.intent)
    .slice(0, 4)
    .map((c) => ({
      intent: c.intent,
      label: INTENT_LABELS[c.intent] || c.intent,
      confidence: c.confidence,
    }));

  const needsClarification =
    best.intent === INTENT_CATEGORIES.UNKNOWN ||
    best.confidence < INTENT_CONFIDENCE_THRESHOLD;

  return buildMissionIntent({
    goal: best.goal || defaultGoal(best.intent, extracted),
    intentCategory: best.intent,
    matchedIntent: best.intent,
    domain: best.domain || defaultDomain(best.intent),
    mode: best.mode || defaultMode(best.intent),
    target: {
      campaign: extracted.campaign || null,
      subject: extracted.subject || null,
    },
    parameters: {
      ...(extracted.campaign ? { campaign: extracted.campaign } : {}),
      ...(extracted.subject ? { client: extracted.subject } : {}),
      ...(extracted.prospectList
        ? { prospectList: extracted.prospectList }
        : {}),
      ...(extracted.market ? { market: extracted.market } : {}),
    },
    constraints: {
      reuseExistingArtifacts:
        best.diagnostics ||
        /\breuse\b/.test(lower) ||
        /\bexisting\b/.test(lower) ||
        extracted.prospectList === 'current',
    },
    options: extracted.options || {},
    diagnostics: Boolean(best.diagnostics),
    confidence: best.confidence,
    alternateIntents: alternates,
    needsClarification,
    notes: extracted.notes || [],
    sourceText,
    createdAt: opts.now || new Date().toISOString(),
  });
}

/**
 * Score semantic intent candidates (not capability aliases).
 * @param {string} lower
 * @param {string} sourceText
 * @param {object} extracted
 * @returns {IntentCandidate[]}
 */
function scoreIntentCandidates(lower, sourceText, extracted) {
  /** @type {IntentCandidate[]} */
  const out = [];

  const push = (intent, confidence, extra = {}) => {
    if (confidence <= 0) return;
    out.push({
      intent,
      confidence: Math.min(1, confidence),
      goal: extra.goal || defaultGoal(intent, extracted),
      domain: extra.domain || defaultDomain(intent),
      mode: extra.mode || defaultMode(intent),
      diagnostics: Boolean(extra.diagnostics),
    });
  };

  // --- Diagnostics / investigation (before execution so "audit" wins) ---
  if (
    /\b(audit|diagnose|diagnostic|diagnostics)\b/.test(lower) ||
    /\bwhat('?s| is)\s+wrong\b/.test(lower) ||
    /\bwhy\s+(isn'?t|is\s+not|didn'?t|doesn'?t|won'?t)\b/.test(lower) ||
    /\bfigure\s+out\s+why\b/.test(lower) ||
    /\bwhat\s+happened\s+(to|with)\b/.test(lower) ||
    /\b(failed|failing|broken|stuck)\b/.test(lower) ||
    /\bend[- ]to[- ]end\s+(execution\s+)?audit\b/.test(lower) ||
    /\bexecution\s+audit\b/.test(lower)
  ) {
    if (
      /\bcampaign\b/.test(lower) ||
      extracted.campaign ||
      /\bexecution\b/.test(lower) ||
      /\bmail\b/.test(lower)
    ) {
      push(INTENT_CATEGORIES.CAMPAIGN_DIAGNOSTICS, 0.93, {
        diagnostics: true,
        mode: INTENT_MODES.DIAGNOSTICS,
        domain: INTENT_DOMAINS.DIRECT_MAIL,
        goal: extracted.campaign
          ? `Diagnose Campaign ${extracted.campaign}`
          : 'Diagnose Campaign',
      });
      push(INTENT_CATEGORIES.CAMPAIGN_REVIEW, 0.45);
      push(INTENT_CATEGORIES.DIAGNOSTICS, 0.4);
    } else if (/\bdiscover/.test(lower) || /\bprospect/.test(lower)) {
      push(INTENT_CATEGORIES.DISCOVERY_INVESTIGATION, 0.92, {
        diagnostics: true,
        mode: INTENT_MODES.INVESTIGATION,
        domain: INTENT_DOMAINS.DISCOVERY,
      });
    } else {
      push(INTENT_CATEGORIES.DIAGNOSTICS, 0.85, {
        diagnostics: true,
        mode: INTENT_MODES.DIAGNOSTICS,
      });
    }
  }

  if (
    /\bwhy\s+(isn'?t|is\s+not|didn'?t|doesn'?t)\s+discovery\b/.test(lower) ||
    /\bdiscovery\s+(isn'?t|not)\s+finding\b/.test(lower) ||
    /\bdiscovery\s+(finding\s+)?(anyone|nobody|nothing|zero)\b/.test(lower) ||
    /\binvestigat(e|ion).*\bdiscovery\b/.test(lower) ||
    /\bdiscovery\b.*\binvestigat/.test(lower)
  ) {
    push(INTENT_CATEGORIES.DISCOVERY_INVESTIGATION, 0.95, {
      diagnostics: true,
      mode: INTENT_MODES.INVESTIGATION,
      domain: INTENT_DOMAINS.DISCOVERY,
      goal: 'Investigate Discovery',
    });
    push(INTENT_CATEGORIES.DIAGNOSTICS, 0.42);
    push(INTENT_CATEGORIES.PROSPECT_DISCOVERY, 0.38);
  }

  // --- Campaign creation (build/create/launch) ---
  if (
    /\b(build|create|launch|prepare|new)\s+(a\s+)?(q\d\s+)?(outreach\s+)?campaign\b/.test(
      lower
    )
  ) {
    push(INTENT_CATEGORIES.CAMPAIGN_CREATION, 0.96, {
      mode: INTENT_MODES.GENERATION,
      domain: INTENT_DOMAINS.CAMPAIGN,
      goal: extracted.campaign
        ? `Build Campaign ${extracted.campaign}`
        : 'Build Campaign',
    });
    push(INTENT_CATEGORIES.CAMPAIGN_EXECUTION, 0.3);
  }

  // --- Campaign execution (run / execute / mail) ---
  if (
    /\brun\s+(the\s+)?campaign\b/.test(lower) ||
    /\bexecute\s+(the\s+)?campaign\b/.test(lower) ||
    /\bdirect\s+mail\s+execution\b/.test(lower) ||
    /\bexecute\s+(the\s+)?(direct\s+)?mail\b/.test(lower) ||
    /\bprint\s+(and\s+)?mail\b/.test(lower) ||
    /\bmail\s+(the\s+)?campaign\b/.test(lower) ||
    /\bmark\s+(all\s+)?mailed\b/.test(lower)
  ) {
    // Prefer diagnostics when audit language is also present
    const auditBoost = /\b(audit|diagnos|wrong|failed)\b/.test(lower)
      ? -0.5
      : 0;
    push(INTENT_CATEGORIES.CAMPAIGN_EXECUTION, 0.94 + auditBoost, {
      mode: INTENT_MODES.EXECUTION,
      domain: INTENT_DOMAINS.DIRECT_MAIL,
      goal: extracted.campaign
        ? `Run Campaign ${extracted.campaign}`
        : 'Run Campaign',
    });
    push(INTENT_CATEGORIES.CAMPAIGN_REVIEW, 0.35);
  }

  // --- Campaign review ---
  if (
    /\bcampaign\s+review\b/.test(lower) ||
    /\breview\s+(the\s+)?campaign\b/.test(lower) ||
    /\bapprove\s+(the\s+)?campaign\b/.test(lower) ||
    /\bcampaign\s+approval\b/.test(lower) ||
    (/^review\.?$/.test(lower) && extracted.campaign)
  ) {
    push(INTENT_CATEGORIES.CAMPAIGN_REVIEW, 0.92, {
      mode: INTENT_MODES.REVIEW,
      domain: INTENT_DOMAINS.CAMPAIGN,
      goal: extracted.campaign
        ? `Review Campaign ${extracted.campaign}`
        : 'Review Campaign',
    });
  }

  // --- Prospect discovery ---
  if (
    /\b(find|discover)\s+.+\b(prospects?|leads|companies)\b/.test(lower) ||
    /\bprospect\s+discovery\b/.test(lower) ||
    /\brun\s+discovery\b/.test(lower)
  ) {
    if (!/\bwhy\b/.test(lower) && !/\bisn'?t\b/.test(lower)) {
      push(INTENT_CATEGORIES.PROSPECT_DISCOVERY, 0.9, {
        mode: INTENT_MODES.EXECUTION,
        domain: INTENT_DOMAINS.DISCOVERY,
        goal: 'Discover Prospects',
      });
    }
  }

  // --- Messaging / mail packages ---
  if (
    /\b(generate|create|build|prepare|print)\s+(a\s+)?(mail|direct\s*mail)\s+packages?\b/.test(
      lower
    ) ||
    /\bmail\s+packages?\b/.test(lower) ||
    /\bgenerate\s+messaging\b/.test(lower) ||
    /\bwrite\s+(the\s+)?(letters?|messaging)\b/.test(lower)
  ) {
    if (/\bmessaging\b/.test(lower) || /\bletters?\b/.test(lower)) {
      push(INTENT_CATEGORIES.GENERATE_MESSAGING, 0.88, {
        mode: INTENT_MODES.GENERATION,
        domain: INTENT_DOMAINS.DIRECT_MAIL,
      });
    }
    push(INTENT_CATEGORIES.MAIL_PACKAGE_GENERATION, 0.9, {
      mode: INTENT_MODES.GENERATION,
      domain: INTENT_DOMAINS.DIRECT_MAIL,
      goal: extracted.campaign
        ? `Generate Mail Packages for Campaign ${extracted.campaign}`
        : 'Generate Mail Packages',
    });
  }

  // --- Business intelligence ---
  if (
    /\bbuild\s+business\s+intelligence\b/.test(lower) ||
    /\bbusiness\s+intelligence\b/.test(lower) ||
    /\banalyze\s+(the\s+)?(company|business|prospect)\b/.test(lower)
  ) {
    push(INTENT_CATEGORIES.BUILD_BUSINESS_INTELLIGENCE, 0.9, {
      mode: INTENT_MODES.GENERATION,
      domain: INTENT_DOMAINS.INTELLIGENCE,
      goal: 'Build Business Intelligence',
    });
  }

  // --- Proposal ---
  if (
    /\b(generate|create|draft|write|build)\s+(a\s+)?(sales\s+)?proposal\b/.test(
      lower
    )
  ) {
    push(INTENT_CATEGORIES.GENERATE_PROPOSAL, 0.93, {
      mode: INTENT_MODES.GENERATION,
      domain: INTENT_DOMAINS.PROPOSAL,
      goal: extracted.subject
        ? `Generate Proposal for ${extracted.subject}`
        : 'Generate Proposal',
    });
  }

  // --- Import / export ---
  if (
    /\bimport\s+(a\s+)?(prospect\s*)?lists?\b/.test(lower) ||
    /\bupload\s+(a\s+)?(prospect\s*)?lists?\b/.test(lower) ||
    /\battach(ed)?\s+(a\s+)?prospect\s*lists?\b/.test(lower)
  ) {
    push(INTENT_CATEGORIES.IMPORT_PROSPECT_LIST, 0.9, {
      mode: INTENT_MODES.EXECUTION,
      domain: INTENT_DOMAINS.DISCOVERY,
      goal: 'Import Prospect List',
    });
  }
  if (
    /\bexport\s+(the\s+)?campaign\b/.test(lower) ||
    /\bdownload\s+(the\s+)?campaign\b/.test(lower)
  ) {
    push(INTENT_CATEGORIES.EXPORT_CAMPAIGN, 0.88, {
      mode: INTENT_MODES.EXECUTION,
      domain: INTENT_DOMAINS.CAMPAIGN,
      goal: 'Export Campaign',
    });
  }

  // --- Outcome intelligence ---
  if (
    /\boutcome\s+intelligence\b/.test(lower) ||
    /\bcapture\s+(campaign\s+)?outcomes?\b/.test(lower) ||
    /\bcampaign\s+outcomes?\b/.test(lower) ||
    /\blearnings?\s+from\s+(the\s+)?campaign\b/.test(lower)
  ) {
    push(INTENT_CATEGORIES.OUTCOME_INTELLIGENCE, 0.92, {
      mode: INTENT_MODES.REVIEW,
      domain: INTENT_DOMAINS.CAMPAIGN,
      goal: extracted.campaign
        ? `Capture Campaign Outcomes for Campaign ${extracted.campaign}`
        : 'Capture Campaign Outcomes',
    });
  }

  // --- Operator inbox / help ---
  if (
    /\boperator\s+inbox\b/.test(lower) ||
    /\bshow\s+(my\s+)?inbox\b/.test(lower) ||
    /\bwhat\s+needs\s+(my\s+)?attention\b/.test(lower)
  ) {
    push(INTENT_CATEGORIES.OPERATOR_INBOX, 0.92, {
      mode: INTENT_MODES.HELP,
      domain: INTENT_DOMAINS.OPERATOR,
    });
  }
  if (
    /\bhow\s+do\s+i\b/.test(lower) ||
    /\bhelp\s+(me\s+)?(with|understand)\b/.test(lower) ||
    /\bwhat\s+can\s+(you|i)\b/.test(lower) ||
    /\boperator\s+help\b/.test(lower)
  ) {
    push(INTENT_CATEGORIES.OPERATOR_HELP, 0.8, {
      mode: INTENT_MODES.HELP,
      domain: INTENT_DOMAINS.OPERATOR,
    });
  }

  // --- Review prospect ---
  if (
    /\breview\s+(the\s+)?prospect\b/.test(lower) ||
    /\bprospect\s+review\b/.test(lower)
  ) {
    push(INTENT_CATEGORIES.REVIEW_PROSPECT, 0.88, {
      mode: INTENT_MODES.REVIEW,
      domain: INTENT_DOMAINS.DISCOVERY,
    });
  }

  // Soft signal: bare "run … Campaign NNN" without audit language
  if (
    !out.length &&
    /\brun\b/.test(lower) &&
    (/\bcampaign\b/.test(lower) || extracted.campaign)
  ) {
    push(INTENT_CATEGORIES.CAMPAIGN_EXECUTION, 0.82, {
      mode: INTENT_MODES.EXECUTION,
      domain: INTENT_DOMAINS.DIRECT_MAIL,
      goal: extracted.campaign
        ? `Run Campaign ${extracted.campaign}`
        : 'Run Campaign',
    });
  }

  if (!out.length) {
    // Keep a weak unknown so callers always get a structured intent
    push(INTENT_CATEGORIES.UNKNOWN, 0.2, {
      goal: sourceText.split(/[.!?]/)[0].trim() || sourceText,
      mode: INTENT_MODES.HELP,
    });
  }

  // Dedupe by intent — keep highest confidence
  const byIntent = new Map();
  for (const c of out) {
    const prev = byIntent.get(c.intent);
    if (!prev || c.confidence > prev.confidence) byIntent.set(c.intent, c);
  }
  return [...byIntent.values()];
}

function extractTargets(sourceText, lower) {
  /** @type {Record<string, unknown>} */
  const out = { options: {}, notes: [] };

  const campaign = /campaign\s+(\d+)/i.exec(sourceText);
  if (campaign) out.campaign = campaign[1];

  const forMatch =
    /\bfor\s+([A-Z][\w]*(?:\s+[A-Z][\w]*){0,4})(?:\s+using\b|\s+with\b|[.,]|$)/.exec(
      sourceText
    );
  if (forMatch) {
    const name = forMatch[1].trim();
    if (!/^(the|a|an|campaign|review|mail)$/i.test(name)) {
      out.subject = name;
    }
  }

  if (/\bcurrent\s+prospect\s*lists?\b/i.test(sourceText)) {
    out.prospectList = 'current';
  } else if (
    /\b(using|with)\s+(the\s+)?(attached|operator)\s+prospect\s*lists?\b/i.test(
      sourceText
    )
  ) {
    out.prospectList = 'attached';
  }

  const market =
    /\bin\s+(?:the\s+)?([A-Za-z][\w\s]+?)\s+market\b/i.exec(sourceText) ||
    /\bmarket:\s*([^.,;]+)/i.exec(sourceText);
  if (market) out.market = market[1].trim();

  if (/\bdry\s*run\b/.test(lower)) out.options.dryRun = true;
  if (/\bshadow\s*mode\b/.test(lower)) out.options.shadowMode = true;
  if (/\bready\s+to\s+print\b/.test(lower)) {
    out.options.readyToPrint = true;
    out.options.review = true;
    out.options.approvalRequired = true;
  }

  // Guidance fragments → notes (never execution)
  if (/\breview\s+human\s+test\b/.test(lower)) {
    out.notes.push('Review Human Test results and generated letters.');
  } else if (/\bgenerated\s+letters?\b/.test(lower) && /\breview\b/.test(lower)) {
    out.notes.push(
      sourceText.match(/[^.!?]*\b(generated\s+letters?|human\s+test)[^.!?]*/i)?.[0]
        ?.trim() || 'Review generated letters.'
    );
  }

  return out;
}

function defaultGoal(intent, extracted = {}) {
  const campaign = extracted.campaign;
  switch (intent) {
    case INTENT_CATEGORIES.CAMPAIGN_EXECUTION:
      return campaign ? `Run Campaign ${campaign}` : 'Run Campaign';
    case INTENT_CATEGORIES.CAMPAIGN_REVIEW:
      return campaign ? `Review Campaign ${campaign}` : 'Review Campaign';
    case INTENT_CATEGORIES.CAMPAIGN_CREATION:
      return campaign ? `Build Campaign ${campaign}` : 'Build Campaign';
    case INTENT_CATEGORIES.CAMPAIGN_DIAGNOSTICS:
      return campaign ? `Diagnose Campaign ${campaign}` : 'Diagnose Campaign';
    case INTENT_CATEGORIES.DISCOVERY_INVESTIGATION:
      return 'Investigate Discovery';
    case INTENT_CATEGORIES.PROSPECT_DISCOVERY:
      return 'Discover Prospects';
    case INTENT_CATEGORIES.GENERATE_MESSAGING:
      return 'Generate Messaging';
    case INTENT_CATEGORIES.BUILD_BUSINESS_INTELLIGENCE:
      return 'Build Business Intelligence';
    case INTENT_CATEGORIES.GENERATE_PROPOSAL:
      return extracted.subject
        ? `Generate Proposal for ${extracted.subject}`
        : 'Generate Proposal';
    case INTENT_CATEGORIES.MAIL_PACKAGE_GENERATION:
      return campaign
        ? `Generate Mail Packages for Campaign ${campaign}`
        : 'Generate Mail Packages';
    case INTENT_CATEGORIES.EXPORT_CAMPAIGN:
      return 'Export Campaign';
    case INTENT_CATEGORIES.IMPORT_PROSPECT_LIST:
      return 'Import Prospect List';
    case INTENT_CATEGORIES.OUTCOME_INTELLIGENCE:
      return campaign
        ? `Capture Campaign Outcomes for Campaign ${campaign}`
        : 'Capture Campaign Outcomes';
    case INTENT_CATEGORIES.OPERATOR_INBOX:
      return 'Open Operator Inbox';
    case INTENT_CATEGORIES.OPERATOR_HELP:
      return 'Operator Help';
    case INTENT_CATEGORIES.DIAGNOSTICS:
      return 'Run Diagnostics';
    case INTENT_CATEGORIES.REVIEW_PROSPECT:
      return 'Review Prospect';
    default:
      return 'Understand Request';
  }
}

function defaultDomain(intent) {
  switch (intent) {
    case INTENT_CATEGORIES.CAMPAIGN_EXECUTION:
    case INTENT_CATEGORIES.CAMPAIGN_DIAGNOSTICS:
    case INTENT_CATEGORIES.MAIL_PACKAGE_GENERATION:
    case INTENT_CATEGORIES.GENERATE_MESSAGING:
      return INTENT_DOMAINS.DIRECT_MAIL;
    case INTENT_CATEGORIES.DISCOVERY_INVESTIGATION:
    case INTENT_CATEGORIES.PROSPECT_DISCOVERY:
    case INTENT_CATEGORIES.IMPORT_PROSPECT_LIST:
    case INTENT_CATEGORIES.REVIEW_PROSPECT:
      return INTENT_DOMAINS.DISCOVERY;
    case INTENT_CATEGORIES.BUILD_BUSINESS_INTELLIGENCE:
      return INTENT_DOMAINS.INTELLIGENCE;
    case INTENT_CATEGORIES.GENERATE_PROPOSAL:
      return INTENT_DOMAINS.PROPOSAL;
    case INTENT_CATEGORIES.OPERATOR_INBOX:
    case INTENT_CATEGORIES.OPERATOR_HELP:
      return INTENT_DOMAINS.OPERATOR;
    case INTENT_CATEGORIES.CAMPAIGN_CREATION:
    case INTENT_CATEGORIES.CAMPAIGN_REVIEW:
    case INTENT_CATEGORIES.EXPORT_CAMPAIGN:
    case INTENT_CATEGORIES.OUTCOME_INTELLIGENCE:
      return INTENT_DOMAINS.CAMPAIGN;
    default:
      return INTENT_DOMAINS.GENERAL;
  }
}

function defaultMode(intent) {
  switch (intent) {
    case INTENT_CATEGORIES.CAMPAIGN_DIAGNOSTICS:
    case INTENT_CATEGORIES.DIAGNOSTICS:
      return INTENT_MODES.DIAGNOSTICS;
    case INTENT_CATEGORIES.DISCOVERY_INVESTIGATION:
      return INTENT_MODES.INVESTIGATION;
    case INTENT_CATEGORIES.CAMPAIGN_REVIEW:
    case INTENT_CATEGORIES.REVIEW_PROSPECT:
    case INTENT_CATEGORIES.OUTCOME_INTELLIGENCE:
      return INTENT_MODES.REVIEW;
    case INTENT_CATEGORIES.CAMPAIGN_CREATION:
    case INTENT_CATEGORIES.GENERATE_MESSAGING:
    case INTENT_CATEGORIES.GENERATE_PROPOSAL:
    case INTENT_CATEGORIES.MAIL_PACKAGE_GENERATION:
    case INTENT_CATEGORIES.BUILD_BUSINESS_INTELLIGENCE:
      return INTENT_MODES.GENERATION;
    case INTENT_CATEGORIES.OPERATOR_HELP:
    case INTENT_CATEGORIES.OPERATOR_INBOX:
      return INTENT_MODES.HELP;
    default:
      return INTENT_MODES.EXECUTION;
  }
}

module.exports = {
  understandIntent,
  scoreIntentCandidates,
  extractTargets,
  defaultGoal,
};
