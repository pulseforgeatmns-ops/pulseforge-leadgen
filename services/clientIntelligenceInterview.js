'use strict';

/**
 * SPEC-083 — Client Intelligence Engine (CIE) thin-slice v1.
 * SPEC-084 — Interview experience helpers (understanding progress, executive summary, resume).
 * SPEC-085 — Executive Business Brief (client-facing synthesis after interview).
 * Text interview → evidence → confidence → Business Blueprint → approve → playbook handoff.
 * Does not invent campaign strategy or activate Scout/Composer.
 */

const crypto = require('crypto');
const defaultPool = require('../db');
const {
  createPlaybookFromApprovedBlueprint,
} = require('./clientIntelligencePlaybookHandoff');

const SESSION_STATUSES = Object.freeze([
  'NEW',
  'DISCOVERY',
  'CLARIFICATION',
  'VALIDATION',
  'BLUEPRINT_GENERATION',
  'CLIENT_REVIEW',
  'APPROVED',
]);

const ALLOWED_TRANSITIONS = Object.freeze({
  NEW: ['DISCOVERY'],
  DISCOVERY: ['CLARIFICATION'],
  CLARIFICATION: ['VALIDATION'],
  VALIDATION: ['BLUEPRINT_GENERATION'],
  BLUEPRINT_GENERATION: ['CLIENT_REVIEW'],
  CLIENT_REVIEW: ['APPROVED', 'DISCOVERY'],
  APPROVED: [],
});

const BLUEPRINT_SECTIONS = Object.freeze([
  'identity',
  'services',
  'idealCustomers',
  'avoidCustomers',
  'targetMarkets',
  'competitiveAdvantages',
  'brandVoice',
  'campaignGoals',
  'successMetrics',
]);

const EVIDENCE_TYPES = Object.freeze([
  'EXPLICIT',
  'INFERRED',
  'OBSERVED',
  'CLIENT_EDITED',
]);

const NEXT_ACTIONS = Object.freeze([
  'ASK',
  'CLARIFY',
  'SUMMARIZE',
  'VALIDATE',
  'GENERATE_BLUEPRINT',
  'COMPLETE',
]);

const EXPLICIT_CONFIDENCE = 0.64;
const SPECIFICITY_BUMP = 0.1;
const CONFIRMATION_BUMP = 0.1;
const CONSISTENCY_BUMP = 0.08;
const CORROBORATION_BUMP = 0.07;
const AMBIGUITY_PENALTY = 0.15;
const CONTRADICTION_PENALTY = 0.22;
const INFERRED_CONFIDENCE = 0.48;
const UNKNOWN_CONFIDENCE = 0.18;
const MAX_CONFIDENCE = 0.98;
const MIN_SECTION_CONFIDENCE = 0.55;
const GENERATED_BY = 'CIE-v1';
const REFLECTION_EVERY_N = 3;

const CONFIRMATION_RE =
  /\b(yes|correct|exactly|that'?s right|confirm|confirmed|agreed|accurate)\b/i;
const AMBIGUITY_RE =
  /\b(maybe|perhaps|not sure|unsure|kind of|sort of|various|etc\.?|something like|i think|probably|roughly|around|whatever|idk|tbd)\b/i;

const SECTION_TITLES = Object.freeze({
  identity: 'Identity',
  services: 'Services',
  idealCustomers: 'Ideal Customers',
  avoidCustomers: 'Customers to Avoid',
  targetMarkets: 'Target Markets',
  competitiveAdvantages: 'Competitive Advantages',
  brandVoice: 'Brand Voice',
  campaignGoals: 'Campaign Goals',
  successMetrics: 'Success Metrics',
});

const QUESTION_BANK = Object.freeze([
  {
    id: 'identity',
    stage: 'Identity',
    section: 'identity',
    prompt:
      "Tell me about the business — what's the name, and how would you describe what you do today?",
    goal: 'Capture business identity',
    askedBecause: 'Identity is required for every Business Blueprint section downstream.',
  },
  {
    id: 'services',
    stage: 'Services',
    section: 'services',
    prompt: 'Tell me about the services your business provides today.',
    goal: 'Capture services',
    askedBecause: 'Services describe what the business delivers to customers.',
  },
  {
    id: 'ideal_customers',
    stage: 'Ideal Customers',
    section: 'idealCustomers',
    prompt:
      'Who do you most want to work with? Paint me a picture of the ideal customer — roles, business types, or segments.',
    goal: 'Capture ideal customers',
    askedBecause: 'ICP understanding feeds playbook idealCustomer fields after approval.',
  },
  {
    id: 'avoid_customers',
    stage: 'Avoid Customers',
    section: 'avoidCustomers',
    prompt:
      "Are there customers or segments you'd rather not take on — and what's usually the reason?",
    goal: 'Capture avoid list',
    askedBecause: 'Avoidance constraints protect targeting quality.',
  },
  {
    id: 'target_markets',
    stage: 'Markets',
    section: 'targetMarkets',
    prompt:
      'Where should we focus first — geography, verticals, or both? Walk me through the markets that matter.',
    goal: 'Capture target markets',
    askedBecause: 'Markets bound discovery and campaign geography later.',
  },
  {
    id: 'advantages',
    stage: 'Advantages',
    section: 'competitiveAdvantages',
    prompt:
      'When a great-fit customer chooses you over someone else, what usually tips the decision?',
    goal: 'Capture competitive advantages',
    askedBecause: 'Advantages ground messaging without inventing strategy.',
  },
  {
    id: 'brand_voice',
    stage: 'Brand Voice',
    section: 'brandVoice',
    prompt:
      'If I were writing as your brand tomorrow, how should it sound — tone, personality, anything we should avoid?',
    goal: 'Capture brand voice',
    askedBecause: 'Brand voice constrains later language without choosing channels.',
  },
  {
    id: 'campaign_goals',
    stage: 'Goals',
    section: 'campaignGoals',
    prompt:
      'Looking at the next 90 days, what business outcomes would make this growth work feel successful?',
    goal: 'Capture campaign goals',
    askedBecause: 'Goals describe desired outcomes, not sequences or offers.',
  },
  {
    id: 'success_metrics',
    stage: 'Success Metrics',
    section: 'successMetrics',
    prompt:
      "How will we know it's working — which numbers or signals do you actually watch?",
    goal: 'Capture success metrics',
    askedBecause: 'Metrics define success for the Business Blueprint.',
  },
]);
class ClientIntelligenceError extends Error {
  /**
   * @param {string} code
   * @param {string} message
   * @param {number} [status]
   */
  constructor(code, message, status = 400) {
    super(message);
    this.name = 'ClientIntelligenceError';
    this.code = code;
    this.status = status;
  }
}

function nowIso() {
  return new Date().toISOString();
}

function newId() {
  return crypto.randomUUID();
}

function asText(value) {
  if (value == null) return null;
  const s = String(value).trim();
  return s || null;
}

function asClientId(value) {
  if (value == null || value === '') {
    throw new ClientIntelligenceError('invalid_client_id', 'client id is required');
  }
  const n = Number(value);
  if (!Number.isFinite(n)) {
    throw new ClientIntelligenceError('invalid_client_id', 'client id must be numeric');
  }
  return Math.trunc(n);
}

function clampConfidence(n) {
  const x = Number(n);
  if (!Number.isFinite(x)) return 0;
  return Math.max(0, Math.min(MAX_CONFIDENCE, Math.round(x * 1000) / 1000));
}

function emptySection() {
  return {
    summary: '',
    confidence: 0,
    evidenceIds: [],
    unknowns: [],
  };
}

function emptySections() {
  const out = {};
  for (const key of BLUEPRINT_SECTIONS) out[key] = emptySection();
  return out;
}

function assertTransition(from, to) {
  const allowed = ALLOWED_TRANSITIONS[from] || [];
  if (!allowed.includes(to)) {
    throw new ClientIntelligenceError(
      'invalid_transition',
      `Cannot transition from ${from} to ${to}`
    );
  }
}

function advanceStatus(session, to) {
  assertTransition(session.status, to);
  session.status = to;
  return session;
}

function answerLooksEmpty(text) {
  const s = String(text || '').trim().toLowerCase();
  if (!s) return true;
  return /^(n\/?a|none|no|nothing|nope|nil|unknown|not sure|-)$/i.test(s);
}

function looksLikeConfirmation(text) {
  return CONFIRMATION_RE.test(String(text || ''));
}

function looksAmbiguous(text) {
  return AMBIGUITY_RE.test(String(text || ''));
}

/**
 * Specificity signals — never response length.
 * Named entities (2+ capitalized tokens), concrete domain terms, numeric facts.
 */
function hasSpecificitySignals(statement) {
  const s = String(statement || '').trim();
  if (!s) return false;
  if (/\b\d+(\.\d+)?%?\b/.test(s)) return true;
  if (
    /\b(commercial|residential|recurring|property managers?|homeowners?|law firms?|manchester|charleston|myrtle|nashville|premium|enterprise|b2b|b2c|weekly|monthly|quarterly)\b/i.test(
      s
    )
  ) {
    return true;
  }
  // Multi-word proper name / place (e.g. "South Carolina", "Aji Home Services")
  if (/\b[A-Z][a-zA-Z0-9&'.-]+(?:\s+[A-Z][a-zA-Z0-9&'.-]+){1,4}\b/.test(s)) return true;
  return false;
}

function detectContradiction(previousStatements, nextText) {
  const next = String(nextText || '').trim().toLowerCase();
  if (!next) return false;
  for (const prev of previousStatements || []) {
    const p = String(prev || '').trim().toLowerCase();
    if (!p) continue;
    // Same category, opposite polarity on a shared noun phrase (simple heuristic)
    const prevNeg = /\b(not|never|no longer|don't|do not|avoid)\b/.test(p);
    const nextNeg = /\b(not|never|no longer|don't|do not|avoid)\b/.test(next);
    if (prevNeg !== nextNeg) {
      const tokens = p
        .split(/\W+/)
        .filter((t) => t.length > 4)
        .slice(0, 6);
      if (tokens.some((t) => next.includes(t))) return true;
    }
  }
  return false;
}

function tokenizeSignificant(text) {
  return String(text || '')
    .toLowerCase()
    .split(/\W+/)
    .filter((t) => t.length > 4);
}

function isConsistentRepeat(previousStatements, nextText) {
  const nextTokens = new Set(tokenizeSignificant(nextText));
  if (nextTokens.size === 0) return false;
  for (const prev of previousStatements || []) {
    const prevTokens = tokenizeSignificant(prev);
    const overlap = prevTokens.filter((t) => nextTokens.has(t)).length;
    if (overlap >= 2) return true;
  }
  return false;
}

/**
 * Confidence must NOT use response length.
 * Increases: explicit, confirmation, consistency, corroboration, specificity.
 * Decreases: contradiction / ambiguity / missing information.
 */
function scoreEvidenceConfidence({
  type,
  statement,
  priorStatements,
  isConfirmation,
  hasCorroboration,
}) {
  if (answerLooksEmpty(statement)) return UNKNOWN_CONFIDENCE;
  let score = type === 'EXPLICIT' || type === 'CLIENT_EDITED' ? EXPLICIT_CONFIDENCE : INFERRED_CONFIDENCE;
  if (hasSpecificitySignals(statement)) score += SPECIFICITY_BUMP;
  if (looksAmbiguous(statement)) score -= AMBIGUITY_PENALTY;
  if (isConfirmation) score += CONFIRMATION_BUMP;
  if (isConsistentRepeat(priorStatements, statement)) score += CONSISTENCY_BUMP;
  if (hasCorroboration) score += CORROBORATION_BUMP;
  if (detectContradiction(priorStatements, statement)) score -= CONTRADICTION_PENALTY;
  return clampConfidence(score);
}

function capitalizeSentence(text) {
  const s = String(text || '').trim();
  if (!s) return '';
  return s.charAt(0).toUpperCase() + s.slice(1);
}

function ensurePeriod(text) {
  const s = String(text || '').trim().replace(/\s+/g, ' ');
  if (!s) return '';
  return /[.!?]$/.test(s) ? s : `${s}.`;
}

function firstSentence(text) {
  const s = String(text || '').trim();
  if (!s) return '';
  const parts = s.split(/(?<=[.!?])\s+/);
  return parts[0] || s;
}

function stripLeadingWeAre(text) {
  return String(text || '')
    .trim()
    .replace(/^(we are|we're|i am|i'm|this is|our company is|the business is)\s+/i, '');
}

/**
 * Consultant-style section summary (2–4 sentences). Never a raw transcript dump.
 */
function summarizeSection(sectionKey, statements) {
  const cleaned = (statements || [])
    .map((s) => String(s || '').trim())
    .filter((s) => s && !/^Unknown:/i.test(s) && !answerLooksEmpty(s));
  if (!cleaned.length) return '';

  const latest = cleaned[cleaned.length - 1];

  switch (sectionKey) {
    case 'identity': {
      const dash = latest.match(/^(.+?)\s*[—–-]\s*(.+)$/);
      if (dash) {
        const name = dash[1].trim().replace(/[.!?,]+$/, '');
        const desc = stripLeadingWeAre(dash[2]).replace(/^(a|an|the)\s+/i, '');
        return [
          ensurePeriod(`${name} is ${/^[aeiou]/i.test(desc) ? 'an' : 'a'} ${desc}`),
          'This identity framing is how the operator describes the business today, and it anchors every other Blueprint section.',
        ].join(' ');
      }
      const named = latest.match(
        /^([A-Z][a-zA-Z0-9&'.-]+(?:\s+[A-Z][a-zA-Z0-9&'.-]+){0,5})\s+(?:is|are|provides?|offers?|does)\s+(.+)$/i
      );
      if (named) {
        return [
          ensurePeriod(`${named[1].trim()} is ${stripLeadingWeAre(named[2]).replace(/^(a|an|the)\s+/i, '')}`),
          'Understanding of the business starts from this operator-stated identity.',
        ].join(' ');
      }
      return [
        ensurePeriod(
          `The business is understood as ${stripLeadingWeAre(latest).replace(/^(a|an|the)\s+/i, '')}`
        ),
        'This identity note will ground services, markets, and messaging downstream.',
      ].join(' ');
    }
    case 'services':
      return [
        ensurePeriod(
          `Today the business delivers ${stripLeadingWeAre(latest).replace(/^(we (sell|offer|provide|do)|services? (include|are))\s+/i, '')}`
        ),
        'Service understanding reflects what is actually sold now, not aspirational packaging.',
      ].join(' ');
    case 'idealCustomers':
      return [
        ensurePeriod(
          `Ideal customers are ${stripLeadingWeAre(latest).replace(/^(our ideal (customer|client)s? (are|is)|we (want|prefer|target))\s+/i, '')}`
        ),
        'This ICP picture prioritizes fit over volume.',
      ].join(' ');
    case 'avoidCustomers':
      return [
        ensurePeriod(
          `The business prefers to avoid ${stripLeadingWeAre(latest).replace(/^(we (avoid|don't want|do not want|should avoid)|avoid)\s+/i, '')}`
        ),
        'These constraints protect targeting quality and should stay visible in the Blueprint.',
      ].join(' ');
    case 'targetMarkets':
      return [
        ensurePeriod(
          `Priority markets center on ${stripLeadingWeAre(latest).replace(/^(we (focus|serve|cover|target)|markets? (are|include))\s+/i, '')}`
        ),
        'Geography and vertical focus here bound where discovery should concentrate first.',
      ].join(' ');
    case 'competitiveAdvantages':
      return [
        ensurePeriod(
          `Competitive edge is described as ${stripLeadingWeAre(latest).replace(/^(we (are|offer|have)|our (edge|advantage) is)\s+/i, '')}`
        ),
        'This is operator-stated differentiation — useful for messaging, not an invented strategy claim.',
      ].join(' ');
    case 'brandVoice':
      return [
        ensurePeriod(
          `Brand voice should read as ${stripLeadingWeAre(latest).replace(/^(we (sound|are)|voice (is|should be)|brand (is|should))\s+/i, '')}`
        ),
        'Tone guidance constrains later language without choosing channels or campaigns.',
      ].join(' ');
    case 'campaignGoals':
      return [
        ensurePeriod(
          `Near-term growth goals focus on ${stripLeadingWeAre(latest).replace(/^(we want to|our goal is to|goals? (are|include))\s+/i, '')}`
        ),
        'These are desired business outcomes for the next phase of work, not execution tactics.',
      ].join(' ');
    case 'successMetrics':
      return [
        ensurePeriod(
          `Success will be judged by ${stripLeadingWeAre(latest).replace(/^(we (track|measure|watch)|metrics? (are|include)|success (is|means))\s+/i, '')}`
        ),
        'These signals define whether the engagement is working from the client\'s perspective.',
      ].join(' ');
    default:
      return [
        ensurePeriod(capitalizeSentence(latest)),
        cleaned.length > 1
          ? ensurePeriod(`Earlier notes in this area remain consistent with that understanding`)
          : ensurePeriod(`This section reflects current operator understanding`),
      ].join(' ');
  }
}

function computeProgress(sectionState) {
  let completed = 0;
  for (const key of BLUEPRINT_SECTIONS) {
    const section = sectionState && sectionState[key];
    if (section && String(section.summary || '').trim() && !answerLooksEmpty(section.summary)) {
      completed += 1;
    }
  }
  const total = BLUEPRINT_SECTIONS.length;
  return {
    label: 'Business Understanding',
    completed,
    total,
    percent: total ? Math.round((completed / total) * 100) : 0,
  };
}

const UNDERSTANDING_STATUS_LABELS = Object.freeze({
  ready: 'Ready',
  building: 'Building…',
  learning: 'Still learning…',
  waiting: 'Waiting for more information…',
});

/**
 * Redacted live progress for the interview panel — never includes summaries.
 * @returns {{ label: string, sections: Array<object> }}
 */
function buildUnderstandingProgress(sectionState) {
  const sections = BLUEPRINT_SECTIONS.map((key) => {
    const section = (sectionState && sectionState[key]) || emptySection();
    const confidence = clampConfidence(section.confidence || 0);
    const evidenceCount = Array.isArray(section.evidenceIds) ? section.evidenceIds.length : 0;
    const hasSummary =
      Boolean(String(section.summary || '').trim()) && !answerLooksEmpty(section.summary);
    const unknowns = [...(section.unknowns || [])].filter(Boolean);
    let status = 'waiting';
    const pct = Math.round(confidence * 100);
    if (evidenceCount > 0 || hasSummary) {
      if (pct >= 81) status = 'ready';
      else if (pct >= 51) status = 'building';
      else status = 'learning';
    }
    return {
      key,
      title: SECTION_TITLES[key] || key,
      confidence,
      confidencePercent: pct,
      evidenceCount,
      unknowns,
      status,
      statusLabel: UNDERSTANDING_STATUS_LABELS[status],
    };
  });
  return {
    label: 'Business Understanding',
    sections,
  };
}

/**
 * Strip Blueprint / interview boilerplate so only substantive meaning remains.
 * Never leak implementation language into CEO-facing copy.
 */
function isMetaConsultantSentence(sentence) {
  const s = String(sentence || '').trim();
  if (!s) return true;
  return (
    /\b(blueprint|operator-stated|operator understanding|downstream|discovery should|ICP picture|engagement is working|evidenceIds?|sectionKey|CIE-v?\d*|prompt|token|json|payload)\b/i.test(
      s
    ) ||
    /anchors every other/i.test(s) ||
    /useful for messaging/i.test(s) ||
    /not an invented strategy/i.test(s) ||
    /not execution tactics/i.test(s) ||
    /without choosing channels/i.test(s) ||
    /from the client's perspective/i.test(s) ||
    /not aspirational packaging/i.test(s) ||
    /should stay visible in the Blueprint/i.test(s) ||
    /Understanding of the business starts/i.test(s) ||
    /will ground services, markets/i.test(s) ||
    /Earlier notes in this area/i.test(s) ||
    /reflects current operator/i.test(s) ||
    /Service understanding reflects/i.test(s) ||
    /These constraints protect targeting/i.test(s) ||
    /Tone guidance constrains/i.test(s) ||
    /These are desired business outcomes for the next phase/i.test(s) ||
    /These signals define whether/i.test(s) ||
    /Geography and vertical focus here bound/i.test(s) ||
    /identity framing is how the operator/i.test(s) ||
    /This identity note will ground/i.test(s) ||
    /^Unknown:/i.test(s) ||
    /^Missing clear answer/i.test(s) ||
    /^No evidence yet/i.test(s)
  );
}

function scrubArtifactLanguage(text) {
  return String(text || '')
    .replace(/\bICP\b/g, 'ideal customer')
    .replace(/\bBlueprint\b/gi, '')
    .replace(/\boperator-stated\b/gi, '')
    .replace(/\boperator understanding\b/gi, 'understanding')
    .replace(/\b(evidenceIds?|sectionKey|CIE-v?\d*)\b/gi, '')
    .replace(/\s{2,}/g, ' ')
    .replace(/\s+([,.;:!?])/g, '$1')
    .trim();
}

function substantiveSentences(summary, limit = 2) {
  const parts = String(summary || '')
    .split(/(?<=[.!?])\s+/)
    .map((s) => s.trim())
    .filter(Boolean)
    .filter((s) => !isMetaConsultantSentence(s))
    .map((s) => scrubArtifactLanguage(s))
    .filter(Boolean);
  return parts.slice(0, limit);
}

function coreClaim(summary) {
  const sentences = substantiveSentences(summary, 1);
  if (!sentences.length) return '';
  return sentences[0].replace(/[.!?]+$/, '').trim();
}

function polishPhrase(text) {
  return String(text || '')
    .replace(/\bfriendly professional\b/i, 'friendly and professional')
    .replace(/\s+(voice|tone)$/i, '')
    .replace(/\s{2,}/g, ' ')
    .trim();
}

/**
 * Lowercase the lead character when embedding a phrase mid-sentence,
 * unless it looks like a proper noun or acronym.
 */
function midSentence(text) {
  const s = polishPhrase(text);
  if (!s) return s;
  if (/^[A-Z]{2,}(?:\b|[0-9])/.test(s)) return s;
  if (/^[A-Z][a-zA-Z0-9&'.-]+(?:\s+[A-Z][a-zA-Z0-9&'.-]+){1,}/.test(s)) return s;
  return s.charAt(0).toLowerCase() + s.slice(1);
}

/**
 * Extract commercial substance from a Blueprint first sentence as a noun phrase.
 * Used only when weaving into a larger sentence — never left as a bare Mad-Lib slot.
 */
function extractSubstance(claim, patterns) {
  const raw = polishPhrase(claim);
  if (!raw) return '';
  for (const [re, group = 1] of patterns) {
    const m = raw.match(re);
    if (m && m[group]) return polishPhrase(m[group]);
  }
  return polishPhrase(raw);
}

/** @deprecated Prefer normalizeClaim / midSentence — retained for observation helpers. */
function softenClaim(claim) {
  return midSentence(
    String(claim || '').replace(
      /^(Today the business delivers|Ideal customers are|The business prefers to avoid|Priority markets center on|Competitive edge is described as|Brand voice should read as|Near-term growth goals focus on|Success will be judged by|The business is understood as|Progress will be judged by)\s+/i,
      ''
    )
  );
}

function asGerundPhrase(claim) {
  const text = softenClaim(claim);
  if (!text) return text;
  if (/^(book|grow|build|increase|improve|win|close|hire|launch|expand)\b/i.test(text)) {
    return text.replace(
      /^(book|grow|build|increase|improve|win|close|hire|launch|expand)\b/i,
      (m) => `${m.toLowerCase()}ing`
    );
  }
  return text;
}

function humanizeUnknownLabel(raw) {
  const text = String(raw || '').trim();
  if (!text) return '';
  if (/^Pricing philosophy$/i.test(text)) return 'pricing philosophy';
  if (/^Capacity$/i.test(text) || /^capacity$/i.test(text)) {
    return 'capacity and delivery constraints';
  }
  const missing = text.match(/^Missing clear answer for\s+(.+)$/i);
  if (missing) {
    const key = missing[1].trim();
    const map = {
      identity: 'how the business defines itself',
      services: 'the full service mix',
      idealCustomers: 'who the ideal customer really is',
      avoidCustomers: 'which customers to decline',
      targetMarkets: 'where to concentrate first',
      competitiveAdvantages: 'what wins the buying decision',
      brandVoice: 'how the brand should sound',
      campaignGoals: 'the near-term growth priority',
      successMetrics: 'how success will be measured',
      capacity: 'capacity and delivery constraints',
    };
    return map[key] || key.replace(/([A-Z])/g, ' $1').toLowerCase().trim();
  }
  const noEvidence = text.match(/^No evidence yet for\s+(.+)$/i);
  if (noEvidence) {
    return humanizeUnknownLabel(`Missing clear answer for ${noEvidence[1]}`);
  }
  return scrubArtifactLanguage(
    text
      .replace(/^Unknown:\s*/i, '')
      .replace(/\b(blueprint|evidenceIds?|sectionKey)\b/gi, '')
  );
}

function joinPolished(sentences) {
  return sentences
    .map((s) => ensurePeriod(String(s || '').trim()))
    .filter(Boolean)
    .slice(0, 4)
    .join(' ');
}

/**
 * Normalize a Blueprint summary into a complete CEO-ready sentence for a given facet.
 * Prefers rewriting known interview-summary patterns over fragment insertion.
 */
function normalizeClaim(kind, summary) {
  const claim = coreClaim(summary);
  if (!claim) return '';

  switch (kind) {
    case 'identity': {
      let sentence = claim;
      if (/^The business is understood as\s+/i.test(sentence)) {
        sentence = sentence.replace(
          /^The business is understood as\s+/i,
          'This is a business built around '
        );
      }
      if (
        /\bis an?\s+.+\bcleaning$/i.test(sentence) &&
        !/\b(company|service|business|firm|studio|practice)\b/i.test(sentence)
      ) {
        sentence = sentence.replace(/\bcleaning$/i, 'cleaning company');
      }
      return capitalizeSentence(sentence);
    }
    case 'services': {
      const offer = midSentence(
        extractSubstance(claim, [
          [/^Today the business delivers\s+(.+)$/i],
          [/^The business (?:delivers|offers|provides|sells)\s+(.+)$/i],
          [/^Services? (?:include|are|center on)\s+(.+)$/i],
        ])
      );
      return `Day to day, the company creates value by delivering ${offer}`;
    }
    case 'ideal': {
      const who = midSentence(
        extractSubstance(claim, [
          [/^Ideal customers are\s+(.+)$/i],
          [/^The (?:ideal|best) (?:customers?|clients?) (?:are|is)\s+(.+)$/i],
          [/^Customers worth (?:pursuing|winning) are\s+(.+)$/i],
        ])
      );
      return `The relationships worth winning are with ${who}`;
    }
    case 'avoid': {
      const who = midSentence(
        extractSubstance(claim, [
          [/^The business prefers to avoid\s+(.+)$/i],
          [/^Avoid(?:s|ing)?\s+(.+)$/i],
          [/^The business (?:should|will) (?:avoid|decline)\s+(.+)$/i],
        ])
      );
      return `Just as deliberately, it declines ${who}`;
    }
    case 'markets': {
      const where = midSentence(
        extractSubstance(claim, [
          [/^Priority markets center on\s+(.+)$/i],
          [/^Markets? (?:center on|include|are|focus on)\s+(.+)$/i],
          [/^Geography (?:centers on|focuses on)\s+(.+)$/i],
        ])
      );
      return `Near-term commercial attention belongs in ${where}`;
    }
    case 'advantages': {
      const edge = midSentence(
        extractSubstance(claim, [
          [/^Competitive edge is described as\s+(.+)$/i],
          [/^Differentiation (?:is|centers on)\s+(.+)$/i],
          [/^Customers choose (?:us|this business) (?:for|because of)\s+(.+)$/i],
          [/^The (?:edge|advantage) is\s+(.+)$/i],
        ])
      );
      return `Customers choose this business for ${edge} — a concrete reason to prefer it over a generic alternative`;
    }
    case 'voice': {
      const tone = midSentence(
        extractSubstance(claim, [
          [/^Brand voice should read as\s+(.+)$/i],
          [/^Voice (?:should (?:read|feel|be)|is|feels)\s+(.+)$/i],
          [/^Tone (?:should be|is)\s+(.+)$/i],
        ])
      );
      return `That promise should sound ${tone} in every customer-facing moment`;
    }
    case 'goals': {
      const outcome = asGerundPhrase(
        midSentence(
          extractSubstance(claim, [
            [/^Near-term growth goals focus on\s+(.+)$/i],
            [/^Goals? (?:focus on|are|include)\s+(.+)$/i],
            [/^The (?:near-term |next )?priority is\s+(.+)$/i],
          ])
        )
      );
      return `For the next phase of growth, the organizing outcome is ${outcome}`;
    }
    case 'metrics': {
      const signals = midSentence(
        extractSubstance(claim, [
          [/^Success will be judged by\s+(.+)$/i],
          [/^Progress will be judged by\s+(.+)$/i],
          [/^Metrics? (?:are|include|center on)\s+(.+)$/i],
          [/^We (?:track|measure|watch)\s+(.+)$/i],
        ])
      );
      return `Success will be judged by ${signals}`;
    }
    default:
      return capitalizeSentence(claim);
  }
}

function composeWhoYouAre(identity, services) {
  const id = normalizeClaim('identity', identity);
  const svc = normalizeClaim('services', services);
  const sentences = [];
  if (id && svc) {
    sentences.push(id);
    sentences.push(svc);
    sentences.push(
      'Identity and offer together form the center of gravity any growth advice must respect — not an afterthought to be reverse-engineered later.'
    );
  } else if (id) {
    sentences.push(id);
    sentences.push(
      'That identity is clear enough to orient strategy; what the company sells still needs a sharper commercial definition.'
    );
  } else if (svc) {
    sentences.push(svc);
    sentences.push(
      'The offer is visible, but a tighter statement of who the company is — beyond what it sells — would give this picture a firmer center.'
    );
  } else {
    sentences.push('The business identity is still taking shape.');
    sentences.push(
      'A concise statement of who you are and what you deliver would give every later recommendation a firmer center of gravity.'
    );
  }
  return joinPolished(sentences);
}

function composeWhoYouServe(ideal, avoid, markets) {
  const idealS = normalizeClaim('ideal', ideal);
  const avoidS = normalizeClaim('avoid', avoid);
  const marketS = normalizeClaim('markets', markets);
  const sentences = [];
  if (idealS) sentences.push(idealS);
  if (avoidS) sentences.push(avoidS);
  if (marketS) sentences.push(marketS);

  if (sentences.length >= 2) {
    sentences.push(
      'Taken together, this is a disciplined beachhead: fit over volume, and geography chosen to match that fit.'
    );
  } else if (sentences.length === 1) {
    sentences.push(
      'Sharpening who is a strong fit — and who is not — will keep commercial effort concentrated where it compounds.'
    );
  } else {
    sentences.push('The ideal customer and market focus are not yet fully drawn.');
    sentences.push(
      'Defining who belongs in the book of business — and who does not — will sharpen every commercial decision that follows.'
    );
  }
  return joinPolished(sentences);
}

function composeWhyChooseYou(advantages, brandVoice) {
  const adv = normalizeClaim('advantages', advantages);
  const voice = normalizeClaim('voice', brandVoice);
  const sentences = [];
  if (adv) sentences.push(adv);
  if (voice) sentences.push(voice);

  if (adv && voice) {
    sentences.push(
      'Differentiation and tone must reinforce each other so the market experiences the same promise the business actually keeps.'
    );
  } else if (adv) {
    sentences.push(
      'Protecting and articulating that edge matters more than inventing a broader claim the company does not own.'
    );
  } else if (voice) {
    sentences.push(
      'The voice is clear; the commercial reason a strong-fit customer should choose you still needs a sharper expression.'
    );
  } else {
    sentences.push('What wins the decision — and how that win should sound — remains under-specified.');
    sentences.push(
      'A crisp point of difference, stated in the company’s own language, would make growth work feel authentic rather than generic.'
    );
  }
  return joinPolished(sentences);
}

function composeWhereHeaded(goals) {
  const goal = normalizeClaim('goals', goals);
  if (goal) {
    return joinPolished([
      goal,
      'That outcome should set priorities, sequencing, and what the team deliberately declines so focus is not diluted.',
      'Recommendations earn their keep only when they move the business meaningfully toward this direction.',
    ]);
  }
  return joinPolished([
    'Near-term direction is still open.',
    'Naming the business outcome that would make the next ninety days feel successful would turn activity into a coherent agenda.',
  ]);
}

function composeWhatSuccess(metrics) {
  const metric = normalizeClaim('metrics', metrics);
  if (metric) {
    return joinPolished([
      metric,
      'Those are commercial signals, not vanity activity — they tell the truth about whether the work is creating value.',
      'If they move, the approach is working; if they stall, the approach should be questioned quickly.',
    ]);
  }
  return joinPolished([
    'The measures of success have not yet been named with enough precision.',
    'Agreeing on a small set of watched signals would make it obvious whether the relationship is creating value.',
  ]);
}

const DEFAULT_LEARN_MORE_TOPICS = [
  'Pricing philosophy',
  'Capacity planning',
  'Seasonality',
  'Hiring strategy',
  'Operational bottlenecks',
  'Referral sources',
  'Technology stack',
];

function titleCasePhrase(text) {
  return String(text || '')
    .trim()
    .replace(/\s+/g, ' ')
    .replace(/\b\w/g, (ch) => ch.toUpperCase());
}

function collectUnknownLabels(sections) {
  const s = (key) => (sections && sections[key]) || emptySection();
  const unknownLabels = [];
  for (const key of BLUEPRINT_SECTIONS) {
    for (const u of s(key).unknowns || []) {
      const label = humanizeUnknownLabel(u);
      if (label && !unknownLabels.includes(label)) unknownLabels.push(label);
    }
    if (!String(s(key).summary || '').trim() || answerLooksEmpty(s(key).summary)) {
      const label = humanizeUnknownLabel(`Missing clear answer for ${key}`);
      if (label && !unknownLabels.includes(label)) unknownLabels.push(label);
    }
  }
  return unknownLabels;
}

/**
 * SPEC-085 — always identify meaningful unknowns. Never return "nothing outstanding."
 */
function composeLearnMoreItems(unknownLabels) {
  const cleaned = [...new Set((unknownLabels || []).map(humanizeUnknownLabel).filter(Boolean))];
  const items = cleaned.map((label) => titleCasePhrase(label));
  for (const fallback of DEFAULT_LEARN_MORE_TOPICS) {
    if (items.length >= 4) break;
    if (!items.some((item) => item.toLowerCase() === fallback.toLowerCase())) {
      items.push(fallback);
    }
  }
  return items.slice(0, 5);
}

function composeLearnMoreBody(items) {
  const list = (items || []).filter(Boolean);
  if (!list.length) {
    return joinPolished([
      'A few practical areas still deserve a closer look before recommendations get specific.',
      'Future recommendations become more confident as these areas become understood.',
    ]);
  }
  return joinPolished([
    'A few practical areas still deserve a closer look before recommendations get specific.',
    'Future recommendations become more confident as these areas become understood.',
  ]);
}

function sectionFilled(section) {
  return Boolean(section && String(section.summary || '').trim() && !answerLooksEmpty(section.summary));
}

function sectionConfidence(section) {
  const conf = Number(section && section.confidence);
  if (!Number.isFinite(conf)) return 0;
  return Math.max(0, Math.min(1, conf));
}

/**
 * Evidence-connected observations — not recommendations, not strategy.
 * Maximum five.
 */
function composeObservations(sections) {
  const s = (key) => (sections && sections[key]) || emptySection();
  const observations = [];

  const identity = coreClaim(s('identity').summary);
  const services = coreClaim(s('services').summary);
  const ideal = coreClaim(s('idealCustomers').summary);
  const avoid = coreClaim(s('avoidCustomers').summary);
  const markets = coreClaim(s('targetMarkets').summary);
  const advantages = coreClaim(s('competitiveAdvantages').summary);
  const voice = coreClaim(s('brandVoice').summary);
  const goals = coreClaim(s('campaignGoals').summary);
  const metrics = coreClaim(s('successMetrics').summary);

  if (advantages) {
    observations.push(
      `Your positioning around ${softenClaim(advantages)} appears consistently when you describe why customers choose you.`
    );
  }
  if (ideal && avoid) {
    observations.push(
      'Your commercial focus is unusually clear: you name both the relationships worth pursuing and the ones you prefer to decline.'
    );
  } else if (ideal) {
    observations.push(
      `Your ideal-customer picture — centered on ${softenClaim(ideal).replace(/^(are\s+)/i, '')} — gives outreach a disciplined starting point.`
    );
  }
  if (goals && /recurr|relationship|retain|lifetime|loyal/i.test(String(s('campaignGoals').summary || '') + String(s('idealCustomers').summary || ''))) {
    observations.push(
      'Your long-term emphasis leans toward durable relationships rather than purely transactional growth.'
    );
  } else if (goals) {
    observations.push(
      `Near-term direction is already framed around ${asGerundPhrase(goals)}, which gives later work a clear success test.`
    );
  }
  if (markets) {
    observations.push(
      `Geographic and market attention appears concentrated first in ${softenClaim(markets)}, which keeps discovery from spreading too thin.`
    );
  }
  if (voice && advantages) {
    observations.push(
      `Differentiation and voice reinforce each other: the promise you describe should sound ${softenClaim(voice)} in market.`
    );
  } else if (voice) {
    observations.push(
      `Brand tone is already intentional — ${softenClaim(voice)} — which helps keep later messaging authentic.`
    );
  }
  if (metrics) {
    observations.push(
      `Success is anchored in business outcomes such as ${softenClaim(metrics)}, not vanity activity.`
    );
  }
  if (identity && services && observations.length < 3) {
    observations.push(
      'Identity and offer already form a coherent foundation any growth recommendation should respect.'
    );
  }
  if (!observations.length) {
    observations.push(
      'The conversation establishes a workable foundation, though several themes still need more evidence before they can be stated with high confidence.'
    );
  }
  return observations.slice(0, 5).map((line) => ensurePeriod(line));
}

function starsFromConfidence(conf) {
  const c = Math.max(0, Math.min(1, Number(conf) || 0));
  if (c >= 0.9) return 5;
  if (c >= 0.75) return 4;
  if (c >= 0.55) return 3;
  if (c >= 0.35) return 2;
  if (c > 0) return 1;
  return 1;
}

function averageConfidence(values) {
  const nums = values.filter((v) => Number.isFinite(v));
  if (!nums.length) return 0;
  return nums.reduce((a, b) => a + b, 0) / nums.length;
}

/**
 * Assessment scores derive only from observed section confidence.
 * Explanations always reference evidence patterns — never fabricated.
 */
function composeAssessment(sections) {
  const s = (key) => (sections && sections[key]) || emptySection();

  const clarityParts = [sectionConfidence(s('identity')), sectionConfidence(s('services'))];
  const focusParts = [
    sectionConfidence(s('idealCustomers')),
    sectionConfidence(s('avoidCustomers')),
    sectionConfidence(s('targetMarkets')),
  ];
  const diffParts = [
    sectionConfidence(s('competitiveAdvantages')),
    sectionConfidence(s('brandVoice')),
  ];
  const growthParts = [
    sectionConfidence(s('campaignGoals')),
    sectionConfidence(s('successMetrics')),
  ];

  const clarity = averageConfidence(clarityParts);
  const focus = averageConfidence(focusParts);
  const diff = averageConfidence(diffParts);
  const growth = averageConfidence(growthParts);
  const overall = averageConfidence([clarity, focus, diff, growth]);

  const identityClaim = coreClaim(s('identity').summary);
  const idealClaim = coreClaim(s('idealCustomers').summary);
  const avoidClaim = coreClaim(s('avoidCustomers').summary);
  const advClaim = coreClaim(s('competitiveAdvantages').summary);
  const goalClaim = coreClaim(s('campaignGoals').summary);
  const metricClaim = coreClaim(s('successMetrics').summary);

  const ratings = [
    {
      label: 'Business Clarity',
      stars: starsFromConfidence(clarity),
      explanation: identityClaim
        ? `Supported by a clear identity statement${sectionFilled(s('services')) ? ' and a concrete service mix' : ''}.`
        : 'Identity and service mix are still lightly sketched in the conversation.',
    },
    {
      label: 'Market Focus',
      stars: starsFromConfidence(focus),
      explanation:
        idealClaim && avoidClaim
          ? 'Supported by both a named ideal customer and explicit constraints on who not to serve.'
          : idealClaim || coreClaim(s('targetMarkets').summary)
            ? 'Supported by customer or market focus signals, with room to sharpen the full beachhead.'
            : 'Customer and market focus still need more specific evidence.',
    },
    {
      label: 'Differentiation',
      stars: starsFromConfidence(diff),
      explanation: advClaim
        ? `Supported by stated advantages around ${softenClaim(advClaim)}.`
        : 'Competitive reason-to-choose is not yet evidenced with enough specificity.',
    },
    {
      label: 'Growth Readiness',
      stars: starsFromConfidence(growth),
      explanation:
        goalClaim && metricClaim
          ? 'Supported by named near-term outcomes and business metrics for judging progress.'
          : goalClaim || metricClaim
            ? 'Direction or success measures are present; pairing both would raise readiness further.'
            : 'Near-term outcomes and success measures are still open.',
    },
  ];

  return {
    ratings,
    confidencePercent: Math.round(overall * 100),
    confidenceNote:
      'Confidence reflects how consistently the conversation evidenced each theme — not a grade of the business itself.',
  };
}

function composeConversationStarters(sections, learnMoreItems) {
  const s = (key) => (sections && sections[key]) || emptySection();
  const starters = [];

  if (sectionFilled(s('idealCustomers'))) {
    starters.push(
      'Which customer segments generate the highest lifetime value — and how you recognize them early.'
    );
  }
  if (sectionFilled(s('competitiveAdvantages'))) {
    starters.push(
      'Whether your pricing reflects the premium positioning you described.'
    );
  }
  if (sectionFilled(s('avoidCustomers')) || sectionFilled(s('idealCustomers'))) {
    starters.push(
      'How referral partnerships compare with outbound acquisition for the relationships you want most.'
    );
  }
  if (sectionFilled(s('campaignGoals'))) {
    starters.push(
      'What would make the next ninety days feel unmistakably successful from the owner\'s chair.'
    );
  }
  if (sectionFilled(s('successMetrics'))) {
    starters.push(
      'Which leading indicators you trust before lagging revenue numbers move.'
    );
  }

  for (const topic of learnMoreItems || []) {
    if (starters.length >= 4) break;
    const lower = String(topic).toLowerCase();
    if (/pric/.test(lower)) {
      starters.push('How pricing decisions get made when demand is strong versus soft.');
    } else if (/capacit/.test(lower)) {
      starters.push('Where capacity starts to constrain growth before marketing does.');
    } else if (/season/.test(lower)) {
      starters.push('How seasonality shapes staffing, cash flow, and outreach timing.');
    } else if (/hir/.test(lower)) {
      starters.push('What a strong hire looks like for the next stage of the business.');
    } else if (/referral/.test(lower)) {
      starters.push('Which referral sources historically produce the cleanest fit.');
    } else if (/technolog|stack/.test(lower)) {
      starters.push('Which tools actually carry the customer relationship day to day.');
    } else if (/operational|bottleneck/.test(lower)) {
      starters.push('Where work most often slows between winning a job and delivering it well.');
    }
  }

  const unique = [];
  for (const line of starters) {
    if (!unique.includes(line)) unique.push(line);
  }
  if (!unique.length) {
    unique.push(
      'Which customer segments generate the highest lifetime value.',
      'Whether your pricing reflects your positioning.',
      'How referral partnerships compare with outbound acquisition.'
    );
  }
  return unique.slice(0, 4);
}

/**
 * SPEC-085 — Executive Business Brief.
 * CEO-facing synthesis from a senior consultant. Never concatenates raw
 * interview wording or exposes implementation metadata.
 */
function buildExecutiveSummary(sections) {
  const s = (key) => (sections && sections[key]) || emptySection();
  const unknownLabels = collectUnknownLabels(sections);
  const learnMoreItems = composeLearnMoreItems(unknownLabels);
  const observations = composeObservations(sections);
  const assessment = composeAssessment(sections);
  const conversations = composeConversationStarters(sections, learnMoreItems);

  return {
    title: 'Executive Business Brief',
    subtitle: 'Prepared by Max',
    tagline: 'A working picture for leadership review',
    sections: [
      {
        id: 'whoYouAre',
        title: 'Who You Are',
        kind: 'prose',
        body: composeWhoYouAre(s('identity').summary, s('services').summary),
      },
      {
        id: 'whoYouServe',
        title: 'Who You Serve',
        kind: 'prose',
        body: composeWhoYouServe(
          s('idealCustomers').summary,
          s('avoidCustomers').summary,
          s('targetMarkets').summary
        ),
      },
      {
        id: 'whyChooseYou',
        title: 'Why Customers Choose You',
        kind: 'prose',
        body: composeWhyChooseYou(
          s('competitiveAdvantages').summary,
          s('brandVoice').summary
        ),
      },
      {
        id: 'whereHeaded',
        title: "Where You're Headed",
        kind: 'prose',
        body: composeWhereHeaded(s('campaignGoals').summary),
      },
      {
        id: 'successLooksLike',
        title: 'Success Looks Like',
        kind: 'prose',
        body: composeWhatSuccess(s('successMetrics').summary),
      },
      {
        id: 'observations',
        title: 'Initial Observations',
        kind: 'list',
        body: 'These observations connect themes from our conversation. They are not recommendations.',
        items: observations,
      },
      {
        id: 'assessment',
        title: "Max's Initial Assessment",
        kind: 'assessment',
        ratings: assessment.ratings,
        confidencePercent: assessment.confidencePercent,
        body: assessment.confidenceNote,
      },
      {
        id: 'learnMore',
        title: "Areas I'd Like To Learn More",
        kind: 'list',
        body: composeLearnMoreBody(learnMoreItems),
        items: learnMoreItems,
      },
      {
        id: 'conversations',
        title: "Conversations I'd Recommend Next",
        kind: 'list',
        body: "I'd enjoy exploring:",
        items: conversations,
      },
    ],
  };
}

/** @deprecated Use buildExecutiveSummary — alias retained for SPEC-085 naming clarity. */
function buildExecutiveBusinessBrief(sections) {
  return buildExecutiveSummary(sections);
}

function sectionStateFromSession(session) {
  return (session && session.interview_state && session.interview_state.sectionState) || emptySections();
}

function withExperienceFields(session, payload = {}) {
  const sectionState = sectionStateFromSession(session);
  const out = {
    ...payload,
    progress: payload.progress || computeProgress(sectionState),
    understanding: buildUnderstandingProgress(sectionState),
  };
  if (payload.blueprint && payload.blueprint.sections) {
    out.executiveSummary = buildExecutiveSummary(payload.blueprint.sections);
  }
  return out;
}

function shouldReflect(stepIndex) {
  const answered = Number(stepIndex) || 0;
  return answered > 0 && answered % REFLECTION_EVERY_N === 0;
}

function buildReflection(sectionState, answeredCount) {
  const priorityKeys = ['identity', 'services', 'idealCustomers', 'campaignGoals'];
  const filled = priorityKeys
    .map((key) => {
      const section = sectionState && sectionState[key];
      if (!section || !String(section.summary || '').trim()) return null;
      return { key, summary: section.summary };
    })
    .filter(Boolean);
  const fallback = BLUEPRINT_SECTIONS.map((key) => {
    const section = sectionState && sectionState[key];
    if (!section || !String(section.summary || '').trim()) return null;
    return { key, summary: section.summary };
  }).filter(Boolean);
  const source = filled.length ? filled : fallback;
  if (!source.length) return null;

  const openings = [
    "Thanks, that's helpful. Here's what I'm hearing so far",
    'Let me make sure I understand',
    "Here's what I'm taking away so far",
  ];
  const opener =
    openings[Math.max(0, Math.floor(answeredCount / REFLECTION_EVERY_N) - 1) % openings.length];
  const directionTitles = {
    identity: 'Identity',
    services: 'Services',
    idealCustomers: 'Ideal Customer',
    campaignGoals: 'Direction',
  };
  const snippets = source.slice(0, 4).map((row) => {
    const title = directionTitles[row.key] || SECTION_TITLES[row.key] || row.key;
    return `${title}: ${firstSentence(row.summary)}`;
  });
  return `${opener}…\n\n${snippets.join('\n')}`;
}

function currentQuestion(state) {
  if (!state || state.mode === 'notes') return null;
  const idx = Number(state.stepIndex) || 0;
  if (idx >= QUESTION_BANK.length) return null;
  return { index: idx, question: QUESTION_BANK[idx] };
}

function initialInterviewState({ notes } = {}) {
  return {
    mode: notes ? 'notes' : 'interactive',
    stepIndex: 0,
    done: Boolean(notes),
    answers: {},
    sectionState: emptySections(),
    contradictions: [],
    notes: notes ? String(notes) : null,
    blueprintId: null,
    lastReflectionAt: 0,
  };
}

function createMemoryStore() {
  /** @type {Map<string, object>} */
  const sessions = new Map();
  /** @type {Map<string, object[]>} */
  const turnsBySession = new Map();
  /** @type {Map<string, object>} */
  const evidence = new Map();
  /** @type {Map<string, object[]>} */
  const evidenceBySession = new Map();
  /** @type {Map<string, object>} */
  const blueprints = new Map();
  /** @type {Map<string, object[]>} */
  const blueprintsByClient = new Map();

  return {
    kind: 'memory',
    async insertSession(row) {
      const copy = { ...row };
      sessions.set(copy.id, copy);
      turnsBySession.set(copy.id, []);
      evidenceBySession.set(copy.id, []);
      return { ...copy };
    },
    async getSession(id) {
      const row = sessions.get(String(id));
      return row ? { ...row, interview_state: { ...row.interview_state } } : null;
    },
    async updateSession(id, patch) {
      const cur = sessions.get(String(id));
      if (!cur) return null;
      const next = {
        ...cur,
        ...patch,
        interview_state: patch.interview_state
          ? { ...patch.interview_state }
          : cur.interview_state,
        updated_at: new Date(),
      };
      sessions.set(String(id), next);
      return { ...next, interview_state: { ...next.interview_state } };
    },
    async insertTurn(row) {
      const copy = { ...row };
      const list = turnsBySession.get(copy.session_id) || [];
      list.push(copy);
      turnsBySession.set(copy.session_id, list);
      return { ...copy };
    },
    async updateTurn(id, patch) {
      for (const [sessionId, list] of turnsBySession.entries()) {
        const idx = list.findIndex((t) => t.id === String(id));
        if (idx >= 0) {
          list[idx] = { ...list[idx], ...patch };
          turnsBySession.set(sessionId, list);
          return { ...list[idx] };
        }
      }
      return null;
    },
    async listTurns(sessionId) {
      return (turnsBySession.get(String(sessionId)) || []).map((t) => ({ ...t }));
    },
    async insertEvidence(row) {
      const copy = { ...row };
      evidence.set(copy.id, copy);
      const list = evidenceBySession.get(copy.session_id) || [];
      list.push(copy);
      evidenceBySession.set(copy.session_id, list);
      return { ...copy };
    },
    async listEvidence(sessionId) {
      return (evidenceBySession.get(String(sessionId)) || []).map((e) => ({ ...e }));
    },
    async insertBlueprint(row) {
      const copy = { ...row };
      const key = `${copy.id}@${copy.version}`;
      blueprints.set(key, copy);
      const list = blueprintsByClient.get(String(copy.client_id)) || [];
      list.push(copy);
      blueprintsByClient.set(String(copy.client_id), list);
      return { ...copy };
    },
    async getBlueprint(id, version) {
      if (version != null) {
        const row = blueprints.get(`${id}@${version}`);
        return row ? { ...row } : null;
      }
      const matches = [...blueprints.values()].filter((b) => b.id === String(id));
      matches.sort((a, b) => new Date(b.created_at) - new Date(a.created_at));
      return matches[0] ? { ...matches[0] } : null;
    },
    async updateBlueprint(id, version, patch) {
      const key = `${id}@${version}`;
      const cur = blueprints.get(key);
      if (!cur) return null;
      const next = { ...cur, ...patch, updated_at: new Date() };
      blueprints.set(key, next);
      const list = blueprintsByClient.get(String(next.client_id)) || [];
      const idx = list.findIndex((b) => b.id === id && b.version === version);
      if (idx >= 0) list[idx] = next;
      return { ...next };
    },
    async listBlueprintsForClient(clientId, { status } = {}) {
      let rows = (blueprintsByClient.get(String(clientId)) || []).map((b) => ({ ...b }));
      if (status) rows = rows.filter((b) => b.status === status);
      rows.sort((a, b) => new Date(b.created_at) - new Date(a.created_at));
      return rows;
    },
    async supersedeBlueprints(logicalId, exceptVersion) {
      for (const [key, row] of blueprints.entries()) {
        if (row.id === logicalId && row.version !== exceptVersion && row.status === 'approved') {
          const next = { ...row, status: 'superseded', updated_at: new Date() };
          blueprints.set(key, next);
        }
      }
    },
  };
}

function createPostgresStore(pool) {
  return {
    kind: 'postgres',
    async insertSession(row) {
      const result = await pool.query(
        `INSERT INTO cie_interview_sessions (
           id, client_id, status, started_at, completed_at, current_stage,
           summary, confidence_score, interview_state, created_at, updated_at
         ) VALUES (
           $1,$2,$3,COALESCE($4,NOW()),$5,$6,$7,$8,$9::jsonb,NOW(),NOW()
         ) RETURNING *`,
        [
          row.id,
          row.client_id,
          row.status,
          row.started_at || null,
          row.completed_at || null,
          row.current_stage,
          row.summary || null,
          row.confidence_score,
          JSON.stringify(row.interview_state || {}),
        ]
      );
      return normalizeSessionRow(result.rows[0]);
    },
    async getSession(id) {
      const result = await pool.query(
        `SELECT * FROM cie_interview_sessions WHERE id = $1`,
        [String(id)]
      );
      return result.rows[0] ? normalizeSessionRow(result.rows[0]) : null;
    },
    async updateSession(id, patch) {
      const fields = [];
      const params = [];
      let n = 1;
      const map = {
        status: 'status',
        completed_at: 'completed_at',
        current_stage: 'current_stage',
        summary: 'summary',
        confidence_score: 'confidence_score',
        interview_state: 'interview_state',
      };
      for (const [key, col] of Object.entries(map)) {
        if (Object.prototype.hasOwnProperty.call(patch, key)) {
          if (key === 'interview_state') {
            fields.push(`${col} = $${n}::jsonb`);
            params.push(JSON.stringify(patch[key] || {}));
          } else {
            fields.push(`${col} = $${n}`);
            params.push(patch[key]);
          }
          n += 1;
        }
      }
      fields.push('updated_at = NOW()');
      params.push(String(id));
      const result = await pool.query(
        `UPDATE cie_interview_sessions SET ${fields.join(', ')}
         WHERE id = $${n} RETURNING *`,
        params
      );
      return result.rows[0] ? normalizeSessionRow(result.rows[0]) : null;
    },
    async insertTurn(row) {
      const result = await pool.query(
        `INSERT INTO cie_interview_turns (
           id, session_id, speaker, message, goal, asked_because,
           derived_evidence, created_at
         ) VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,COALESCE($8,NOW()))
         RETURNING *`,
        [
          row.id,
          row.session_id,
          row.speaker,
          row.message,
          row.goal || null,
          row.asked_because || null,
          JSON.stringify(row.derived_evidence || []),
          row.created_at || null,
        ]
      );
      return normalizeTurnRow(result.rows[0]);
    },
    async updateTurn(id, patch) {
      const fields = [];
      const params = [];
      let n = 1;
      if (Object.prototype.hasOwnProperty.call(patch, 'derived_evidence')) {
        fields.push(`derived_evidence = $${n}::jsonb`);
        params.push(JSON.stringify(patch.derived_evidence || []));
        n += 1;
      }
      if (Object.prototype.hasOwnProperty.call(patch, 'message')) {
        fields.push(`message = $${n}`);
        params.push(patch.message);
        n += 1;
      }
      if (!fields.length) return null;
      params.push(String(id));
      const result = await pool.query(
        `UPDATE cie_interview_turns SET ${fields.join(', ')} WHERE id = $${n} RETURNING *`,
        params
      );
      return result.rows[0] ? normalizeTurnRow(result.rows[0]) : null;
    },
    async listTurns(sessionId) {
      const result = await pool.query(
        `SELECT * FROM cie_interview_turns WHERE session_id = $1 ORDER BY created_at ASC`,
        [String(sessionId)]
      );
      return result.rows.map(normalizeTurnRow);
    },
    async insertEvidence(row) {
      const result = await pool.query(
        `INSERT INTO cie_evidence (
           id, client_id, session_id, source, source_turn_id, category,
           statement, confidence, type, created_at
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,COALESCE($10,NOW()))
         RETURNING *`,
        [
          row.id,
          row.client_id,
          row.session_id,
          row.source,
          row.source_turn_id || null,
          row.category,
          row.statement,
          row.confidence,
          row.type,
          row.created_at || null,
        ]
      );
      return normalizeEvidenceRow(result.rows[0]);
    },
    async listEvidence(sessionId) {
      const result = await pool.query(
        `SELECT * FROM cie_evidence WHERE session_id = $1 ORDER BY created_at ASC`,
        [String(sessionId)]
      );
      return result.rows.map(normalizeEvidenceRow);
    },
    async insertBlueprint(row) {
      const result = await pool.query(
        `INSERT INTO cie_business_blueprints (
           id, client_id, session_id, version, status, generated_by, sections,
           confidence_summary, playbook_id, playbook_version, section_provenance,
           parent_blueprint_id, created_at, updated_at
         ) VALUES (
           $1,$2,$3,$4,$5,$6,$7::jsonb,$8::jsonb,$9,$10,$11::jsonb,$12,NOW(),NOW()
         ) RETURNING *`,
        [
          row.id,
          row.client_id,
          row.session_id,
          row.version,
          row.status,
          row.generated_by,
          JSON.stringify(row.sections || {}),
          JSON.stringify(row.confidence_summary || {}),
          row.playbook_id || null,
          row.playbook_version || null,
          JSON.stringify(row.section_provenance || {}),
          row.parent_blueprint_id || null,
        ]
      );
      return normalizeBlueprintRow(result.rows[0]);
    },
    async getBlueprint(id, version) {
      if (version != null) {
        const result = await pool.query(
          `SELECT * FROM cie_business_blueprints WHERE id = $1 AND version = $2`,
          [String(id), String(version)]
        );
        return result.rows[0] ? normalizeBlueprintRow(result.rows[0]) : null;
      }
      const result = await pool.query(
        `SELECT * FROM cie_business_blueprints WHERE id = $1
         ORDER BY created_at DESC LIMIT 1`,
        [String(id)]
      );
      return result.rows[0] ? normalizeBlueprintRow(result.rows[0]) : null;
    },
    async updateBlueprint(id, version, patch) {
      const fields = [];
      const params = [];
      let n = 1;
      const map = {
        status: 'status',
        sections: 'sections',
        confidence_summary: 'confidence_summary',
        playbook_id: 'playbook_id',
        playbook_version: 'playbook_version',
        section_provenance: 'section_provenance',
      };
      for (const [key, col] of Object.entries(map)) {
        if (Object.prototype.hasOwnProperty.call(patch, key)) {
          if (
            key === 'sections' ||
            key === 'confidence_summary' ||
            key === 'section_provenance'
          ) {
            fields.push(`${col} = $${n}::jsonb`);
            params.push(JSON.stringify(patch[key] || {}));
          } else {
            fields.push(`${col} = $${n}`);
            params.push(patch[key]);
          }
          n += 1;
        }
      }
      fields.push('updated_at = NOW()');
      params.push(String(id), String(version));
      const result = await pool.query(
        `UPDATE cie_business_blueprints SET ${fields.join(', ')}
         WHERE id = $${n} AND version = $${n + 1} RETURNING *`,
        params
      );
      return result.rows[0] ? normalizeBlueprintRow(result.rows[0]) : null;
    },
    async listBlueprintsForClient(clientId, { status } = {}) {
      const params = [Number(clientId)];
      let sql = `SELECT * FROM cie_business_blueprints WHERE client_id = $1`;
      if (status) {
        params.push(status);
        sql += ` AND status = $2`;
      }
      sql += ` ORDER BY created_at DESC`;
      const result = await pool.query(sql, params);
      return result.rows.map(normalizeBlueprintRow);
    },
    async supersedeBlueprints(logicalId, exceptVersion) {
      await pool.query(
        `UPDATE cie_business_blueprints SET status = 'superseded', updated_at = NOW()
         WHERE id = $1 AND version <> $2 AND status = 'approved'`,
        [String(logicalId), String(exceptVersion)]
      );
    },
  };
}

function normalizeSessionRow(r) {
  return {
    id: r.id,
    client_id: r.client_id,
    status: r.status,
    started_at: r.started_at,
    completed_at: r.completed_at,
    current_stage: r.current_stage,
    summary: r.summary,
    confidence_score: r.confidence_score != null ? Number(r.confidence_score) : null,
    interview_state:
      typeof r.interview_state === 'string'
        ? JSON.parse(r.interview_state)
        : r.interview_state || {},
    created_at: r.created_at,
    updated_at: r.updated_at,
  };
}

function normalizeTurnRow(r) {
  return {
    id: r.id,
    session_id: r.session_id,
    speaker: r.speaker,
    message: r.message,
    goal: r.goal,
    asked_because: r.asked_because,
    derived_evidence:
      typeof r.derived_evidence === 'string'
        ? JSON.parse(r.derived_evidence)
        : r.derived_evidence || [],
    created_at: r.created_at,
  };
}

function normalizeEvidenceRow(r) {
  return {
    id: r.id,
    client_id: r.client_id,
    session_id: r.session_id,
    source: r.source,
    source_turn_id: r.source_turn_id,
    category: r.category,
    statement: r.statement,
    confidence: Number(r.confidence),
    type: r.type,
    created_at: r.created_at,
  };
}

function normalizeBlueprintRow(r) {
  return {
    id: r.id,
    client_id: r.client_id,
    session_id: r.session_id,
    version: r.version,
    status: r.status,
    generated_by: r.generated_by,
    sections:
      typeof r.sections === 'string' ? JSON.parse(r.sections) : r.sections || {},
    confidence_summary:
      typeof r.confidence_summary === 'string'
        ? JSON.parse(r.confidence_summary)
        : r.confidence_summary || {},
    playbook_id: r.playbook_id,
    playbook_version: r.playbook_version,
    section_provenance:
      typeof r.section_provenance === 'string'
        ? JSON.parse(r.section_provenance)
        : r.section_provenance || {},
    parent_blueprint_id: r.parent_blueprint_id,
    created_at: r.created_at,
    updated_at: r.updated_at,
  };
}

async function resolveStore(opts = {}) {
  if (opts.store) return opts.store;
  if (opts.pool) return createPostgresStore(opts.pool);
  return createPostgresStore(defaultPool);
}

function publicSession(session, extras = {}) {
  return {
    id: session.id,
    clientId: session.client_id,
    status: session.status,
    startedAt: session.started_at,
    completedAt: session.completed_at,
    currentStage: session.current_stage,
    summary: session.summary,
    confidenceScore: session.confidence_score,
    ...extras,
  };
}

function publicBlueprint(bp) {
  if (!bp) return null;
  return {
    id: bp.id,
    clientId: bp.client_id,
    sessionId: bp.session_id,
    version: bp.version,
    status: bp.status,
    generatedBy: bp.generated_by,
    sections: bp.sections,
    confidenceSummary: bp.confidence_summary,
    playbookId: bp.playbook_id,
    playbookVersion: bp.playbook_version,
    sectionProvenance: bp.section_provenance,
    parentBlueprintId: bp.parent_blueprint_id,
    createdAt: bp.created_at,
    updatedAt: bp.updated_at,
  };
}

function publicEvidence(e) {
  return {
    id: e.id,
    clientId: e.client_id,
    sessionId: e.session_id,
    source: e.source,
    sourceTurnId: e.source_turn_id,
    category: e.category,
    statement: e.statement,
    confidence: e.confidence,
    type: e.type,
    createdAt: e.created_at,
  };
}

function publicTurn(t) {
  return {
    id: t.id,
    sessionId: t.session_id,
    speaker: t.speaker,
    message: t.message,
    goal: t.goal,
    askedBecause: t.asked_because,
    derivedEvidence: t.derived_evidence,
    timestamp: t.created_at,
  };
}

async function applySectionUpdate(store, session, sectionKey, statement, type, turnId) {
  const state = session.interview_state || initialInterviewState();
  const sectionState = state.sectionState || emptySections();
  const section = sectionState[sectionKey] || emptySection();
  const priorEvidence = (await store.listEvidence(session.id)).filter(
    (e) => e.category === sectionKey
  );
  const priorStatements = priorEvidence
    .map((e) => e.statement)
    .filter((s) => s && !/^Unknown:/i.test(String(s)));
  const empty = answerLooksEmpty(statement);
  const isConfirmation = looksLikeConfirmation(statement);
  const hasCorroboration = priorEvidence.length >= 1 && !empty;
  const contradiction = detectContradiction(priorStatements, statement);

  const confidence = scoreEvidenceConfidence({
    type: empty ? 'INFERRED' : type,
    statement,
    priorStatements,
    isConfirmation,
    hasCorroboration,
  });

  const evidenceRow = await store.insertEvidence({
    id: newId(),
    client_id: session.client_id,
    session_id: session.id,
    source: `Interview Turn`,
    source_turn_id: turnId,
    category: sectionKey,
    statement: empty ? `Unknown: ${sectionKey}` : String(statement).trim(),
    confidence,
    type: empty ? 'INFERRED' : type,
    created_at: new Date(),
  });

  const unknowns = [...(section.unknowns || [])];
  if (empty) {
    const label = `Missing clear answer for ${sectionKey}`;
    if (!unknowns.includes(label)) unknowns.push(label);
  } else {
    // Clear matching unknown when we get a real answer
    const filtered = unknowns.filter((u) => !u.includes(sectionKey));
    unknowns.length = 0;
    unknowns.push(...filtered);
  }

  let nextConfidence = confidence;
  if (contradiction) {
    nextConfidence = clampConfidence(Math.min(section.confidence || confidence, confidence));
    state.contradictions = [
      ...(state.contradictions || []),
      { section: sectionKey, statement: String(statement).trim(), at: nowIso() },
    ];
  } else if (!empty) {
    const prior = Number(section.confidence) || 0;
    if (prior <= 0) {
      nextConfidence = confidence;
    } else {
      nextConfidence = clampConfidence(prior * 0.35 + confidence * 0.65);
    }
  } else if (empty && (!section.summary || answerLooksEmpty(section.summary))) {
    nextConfidence = UNKNOWN_CONFIDENCE;
  }

  let summary = section.summary || '';
  if (!empty) {
    if (type === 'CLIENT_EDITED') {
      summary = String(statement).trim();
    } else {
      summary = summarizeSection(sectionKey, [...priorStatements, String(statement).trim()]);
    }
  }

  sectionState[sectionKey] = {
    summary,
    confidence: empty
      ? section.summary && !answerLooksEmpty(section.summary)
        ? section.confidence || UNKNOWN_CONFIDENCE
        : UNKNOWN_CONFIDENCE
      : nextConfidence,
    evidenceIds: [...(section.evidenceIds || []), evidenceRow.id],
    unknowns,
  };
  state.sectionState = sectionState;
  session.interview_state = state;

  return { evidenceRow, contradiction, sectionState };
}

function confidenceSummaryFromSections(sections) {
  const summary = {};
  for (const key of BLUEPRINT_SECTIONS) {
    summary[key] = clampConfidence((sections[key] && sections[key].confidence) || 0);
  }
  return summary;
}

function overallConfidence(summary) {
  const vals = BLUEPRINT_SECTIONS.map((k) => summary[k] || 0);
  if (!vals.length) return 0;
  return clampConfidence(vals.reduce((a, b) => a + b, 0) / vals.length);
}

function buildSectionsFromState(sectionState) {
  const sections = emptySections();
  for (const key of BLUEPRINT_SECTIONS) {
    const src = (sectionState && sectionState[key]) || emptySection();
    sections[key] = {
      summary: src.summary || '',
      confidence: clampConfidence(src.confidence || 0),
      evidenceIds: [...(src.evidenceIds || [])],
      unknowns: [...(src.unknowns || [])],
    };
    if (!sections[key].summary && !sections[key].unknowns.length) {
      sections[key].unknowns.push(`No evidence yet for ${key}`);
    }
  }
  return sections;
}

async function generateBlueprint(store, session) {
  if (session.status !== 'BLUEPRINT_GENERATION') {
    advanceStatus(session, 'BLUEPRINT_GENERATION');
  }
  const sections = buildSectionsFromState(session.interview_state.sectionState);
  const confidence_summary = confidenceSummaryFromSections(sections);
  const blueprint = await store.insertBlueprint({
    id: newId(),
    client_id: session.client_id,
    session_id: session.id,
    version: '1.0',
    status: 'in_review',
    generated_by: GENERATED_BY,
    sections,
    confidence_summary,
    playbook_id: null,
    playbook_version: null,
    section_provenance: {},
    parent_blueprint_id: null,
    created_at: new Date(),
    updated_at: new Date(),
  });
  advanceStatus(session, 'CLIENT_REVIEW');
  session.interview_state = {
    ...session.interview_state,
    blueprintId: blueprint.id,
    blueprintVersion: blueprint.version,
  };
  session.confidence_score = overallConfidence(confidence_summary);
  session.summary = `Draft Business Blueprint ${blueprint.id}@${blueprint.version}`;
  session.current_stage = 'Client Review';
  await store.updateSession(session.id, {
    status: session.status,
    current_stage: session.current_stage,
    summary: session.summary,
    confidence_score: session.confidence_score,
    interview_state: session.interview_state,
  });
  return blueprint;
}

async function advanceThroughLifecycleToBlueprint(store, session) {
  // DISCOVERY → CLARIFICATION → VALIDATION → BLUEPRINT_GENERATION → CLIENT_REVIEW
  if (session.status === 'DISCOVERY') {
    advanceStatus(session, 'CLARIFICATION');
  }
  if (session.status === 'CLARIFICATION') {
    advanceStatus(session, 'VALIDATION');
  }
  if (session.status === 'VALIDATION') {
    // validation gate: required sections should have some evidence; still generate with unknowns
    await store.updateSession(session.id, {
      status: 'VALIDATION',
      interview_state: session.interview_state,
      current_stage: 'Validation',
    });
    advanceStatus(session, 'BLUEPRINT_GENERATION');
  }
  await store.updateSession(session.id, {
    status: session.status,
    interview_state: session.interview_state,
  });
  return generateBlueprint(store, session);
}

function extractNotesIntoSections(notes) {
  const text = String(notes || '').trim();
  const sentences = text
    .split(/(?<=[.!?])\s+|\n+/)
    .map((s) => s.trim())
    .filter(Boolean);
  const patterns = [
    { re: /\b(we are|company|business|dba|called)\b/i, section: 'identity' },
    { re: /\b(service|offer|provide|sell|product)\b/i, section: 'services' },
    { re: /\b(ideal|icp|customer|clientele|buyer)\b/i, section: 'idealCustomers' },
    { re: /\b(avoid|not a fit|do not want|no longer serve)\b/i, section: 'avoidCustomers' },
    { re: /\b(market|geo|region|city|county|vertical)\b/i, section: 'targetMarkets' },
    { re: /\b(advantage|differen|better|unique|moat)\b/i, section: 'competitiveAdvantages' },
    { re: /\b(voice|tone|sound|brand|professional|friendly|premium)\b/i, section: 'brandVoice' },
    { re: /\b(goal|grow|book|appointments|revenue|pipeline)\b/i, section: 'campaignGoals' },
    { re: /\b(metric|kpi|measure|success|roi|close rate)\b/i, section: 'successMetrics' },
  ];
  const assigned = {};
  for (const sentence of sentences) {
    for (const p of patterns) {
      if (assigned[p.section]) continue;
      if (p.re.test(sentence)) {
        assigned[p.section] = sentence;
        break;
      }
    }
  }
  // leftover sentences fill identity if missing
  if (!assigned.identity && sentences[0]) assigned.identity = sentences[0];
  return assigned;
}

/**
 * Start interview for a client.
 * @param {{ clientId: number|string, notes?: string, source?: string }} input
 */
async function startClientInterview(input = {}, opts = {}) {
  const store = await resolveStore(opts);
  const clientId = asClientId(input.clientId);
  const notes = asText(input.notes);
  const state = initialInterviewState({ notes });
  const session = await store.insertSession({
    id: newId(),
    client_id: clientId,
    status: 'NEW',
    started_at: new Date(),
    completed_at: null,
    current_stage: 'Identity',
    summary: null,
    confidence_score: null,
    interview_state: state,
  });

  advanceStatus(session, 'DISCOVERY');
  session.status = 'DISCOVERY';

  if (notes) {
    const mapped = extractNotesIntoSections(notes);
    const systemTurn = await store.insertTurn({
      id: newId(),
      session_id: session.id,
      speaker: 'system',
      message: notes,
      goal: 'Notes-mode ingestion',
      asked_because: 'Operator provided notes instead of interactive Q&A.',
      derived_evidence: [],
      created_at: new Date(),
    });
    for (const section of BLUEPRINT_SECTIONS) {
      const statement = mapped[section] || '';
      await applySectionUpdate(
        store,
        session,
        section,
        statement || '',
        statement ? 'EXPLICIT' : 'INFERRED',
        systemTurn.id
      );
    }
    session.interview_state.done = true;
    session.interview_state.mode = 'notes';
    await store.updateSession(session.id, {
      status: 'DISCOVERY',
      interview_state: session.interview_state,
      current_stage: 'Notes',
    });
    const blueprint = await advanceThroughLifecycleToBlueprint(store, session);
    return withExperienceFields(session, {
      interviewId: session.id,
      ...publicSession(session),
      mode: 'notes',
      nextAction: 'COMPLETE',
      question: null,
      message: 'Notes ingested. Draft Business Blueprint is ready for review.',
      blueprint: publicBlueprint(blueprint),
    });
  }

  const q = currentQuestion(session.interview_state);
  session.current_stage = q.question.stage;
  const assistantTurn = await store.insertTurn({
    id: newId(),
    session_id: session.id,
    speaker: 'assistant',
    message: q.question.prompt,
    goal: q.question.goal,
    asked_because: q.question.askedBecause,
    derived_evidence: [],
    created_at: new Date(),
  });
  await store.updateSession(session.id, {
    status: 'DISCOVERY',
    current_stage: session.current_stage,
    interview_state: session.interview_state,
  });

  return withExperienceFields(session, {
    interviewId: session.id,
    ...publicSession(session),
    mode: 'interactive',
    nextAction: 'ASK',
    question: {
      id: q.question.id,
      prompt: q.question.prompt,
      stage: q.question.stage,
      section: q.question.section,
      goal: q.question.goal,
      askedBecause: q.question.askedBecause,
    },
    message: q.question.prompt,
    turnId: assistantTurn.id,
    blueprint: null,
  });
}

async function postInterviewMessage(sessionId, message, opts = {}) {
  const store = await resolveStore(opts);
  const session = await store.getSession(sessionId);
  if (!session) {
    throw new ClientIntelligenceError('not_found', 'Interview session not found', 404);
  }
  if (session.status === 'APPROVED') {
    throw new ClientIntelligenceError(
      'interview_complete',
      'Interview already approved; start a new session to recalibrate'
    );
  }
  if (session.status === 'CLIENT_REVIEW') {
    throw new ClientIntelligenceError(
      'awaiting_review',
      'Blueprint is ready for review; use revise/approve APIs'
    );
  }
  if (!['DISCOVERY', 'CLARIFICATION', 'VALIDATION'].includes(session.status)) {
    throw new ClientIntelligenceError(
      'invalid_status',
      `Cannot accept messages in status ${session.status}`
    );
  }

  const text = String(message || '').trim();
  if (!text) {
    throw new ClientIntelligenceError('empty_message', 'message is required');
  }

  const state = session.interview_state || initialInterviewState();
  const q = currentQuestion(state);

  // Refinement pass after resume: free-form note updates then regenerate blueprint.
  if (!q && state.refinementPass) {
    const clientTurn = await store.insertTurn({
      id: newId(),
      session_id: session.id,
      speaker: 'client',
      message: text,
      goal: 'Refine Business Blueprint understanding',
      asked_because: 'Client returned to the interview to refine Max\'s understanding.',
      derived_evidence: [],
      created_at: new Date(),
    });
    const mapped = extractNotesIntoSections(text);
    const evidenceIds = [];
    const sectionsToUpdate = Object.keys(mapped).length
      ? Object.keys(mapped)
      : ['identity'];
    for (const section of sectionsToUpdate) {
      const statement = mapped[section] || text;
      const { evidenceRow } = await applySectionUpdate(
        store,
        session,
        section,
        statement,
        'EXPLICIT',
        clientTurn.id
      );
      evidenceIds.push(evidenceRow.id);
    }
    await store.updateTurn(clientTurn.id, { derived_evidence: evidenceIds });
    state.refinementPass = false;
    state.done = true;
    session.interview_state = state;
    await store.updateSession(session.id, {
      status: 'DISCOVERY',
      interview_state: state,
      current_stage: 'Refinement',
    });
    const blueprint = await advanceThroughLifecycleToBlueprint(store, session);
    return withExperienceFields(await store.getSession(session.id), {
      interviewId: session.id,
      ...publicSession(await store.getSession(session.id)),
      nextAction: 'GENERATE_BLUEPRINT',
      question: null,
      message: 'Draft Business Blueprint is ready for review.',
      blueprint: publicBlueprint(blueprint),
      reflection: null,
    });
  }

  if (!q) {
    const blueprint = await advanceThroughLifecycleToBlueprint(store, session);
    return withExperienceFields(await store.getSession(session.id), {
      interviewId: session.id,
      ...publicSession(await store.getSession(session.id)),
      nextAction: 'COMPLETE',
      question: null,
      message: 'Draft Business Blueprint is ready for review.',
      blueprint: publicBlueprint(blueprint),
      reflection: null,
    });
  }

  const clientTurn = await store.insertTurn({
    id: newId(),
    session_id: session.id,
    speaker: 'client',
    message: text,
    goal: q.question.goal,
    asked_because: q.question.askedBecause,
    derived_evidence: [],
    created_at: new Date(),
  });

  const { evidenceRow, contradiction } = await applySectionUpdate(
    store,
    session,
    q.question.section,
    text,
    'EXPLICIT',
    clientTurn.id
  );

  await store.updateTurn(clientTurn.id, {
    derived_evidence: [evidenceRow.id],
  });

  state.answers = { ...(state.answers || {}), [q.question.id]: text };
  state.stepIndex = (Number(state.stepIndex) || 0) + 1;
  if (state.stepIndex >= QUESTION_BANK.length) state.done = true;
  session.interview_state = state;
  session.current_stage = q.question.stage;

  if (contradiction) {
    // stay in discovery but surface clarify action; lifecycle still no-skip at end
    await store.updateSession(session.id, {
      interview_state: state,
      current_stage: session.current_stage,
      status: 'DISCOVERY',
    });
  }

  if (state.done) {
    await store.updateSession(session.id, {
      status: 'DISCOVERY',
      interview_state: state,
      current_stage: session.current_stage,
    });
    const blueprint = await advanceThroughLifecycleToBlueprint(store, session);
    return withExperienceFields(await store.getSession(session.id), {
      interviewId: session.id,
      ...publicSession(await store.getSession(session.id)),
      nextAction: 'GENERATE_BLUEPRINT',
      question: null,
      message: 'Draft Business Blueprint is ready for review.',
      evidence: publicEvidence(evidenceRow),
      blueprint: publicBlueprint(blueprint),
      reflection: null,
    });
  }

  let reflection = null;
  if (shouldReflect(state.stepIndex) && state.lastReflectionAt !== state.stepIndex) {
    reflection = buildReflection(state.sectionState, state.stepIndex);
    if (reflection) {
      state.lastReflectionAt = state.stepIndex;
      await store.insertTurn({
        id: newId(),
        session_id: session.id,
        speaker: 'assistant',
        message: reflection,
        goal: 'Reflect current understanding',
        asked_because: 'Lightweight conversational summary before continuing the fixed question bank.',
        derived_evidence: [],
        created_at: new Date(),
      });
    }
  }

  const nextQ = currentQuestion(state);
  session.current_stage = nextQ.question.stage;
  await store.insertTurn({
    id: newId(),
    session_id: session.id,
    speaker: 'assistant',
    message: nextQ.question.prompt,
    goal: nextQ.question.goal,
    asked_because: nextQ.question.askedBecause,
    derived_evidence: [],
    created_at: new Date(),
  });
  session.interview_state = state;
  await store.updateSession(session.id, {
    status: 'DISCOVERY',
    current_stage: session.current_stage,
    interview_state: state,
  });

  return withExperienceFields(await store.getSession(session.id), {
    interviewId: session.id,
    ...publicSession(await store.getSession(session.id)),
    nextAction: contradiction ? 'CLARIFY' : 'ASK',
    question: {
      id: nextQ.question.id,
      prompt: nextQ.question.prompt,
      stage: nextQ.question.stage,
      section: nextQ.question.section,
      goal: nextQ.question.goal,
      askedBecause: nextQ.question.askedBecause,
    },
    message: nextQ.question.prompt,
    reflection,
    evidence: publicEvidence(evidenceRow),
    contradiction: contradiction || false,
    blueprint: null,
  });
}

async function getInterview(sessionId, opts = {}) {
  const store = await resolveStore(opts);
  const session = await store.getSession(sessionId);
  if (!session) {
    throw new ClientIntelligenceError('not_found', 'Interview session not found', 404);
  }
  const turns = await store.listTurns(session.id);
  const evidence = await store.listEvidence(session.id);
  let blueprint = null;
  if (session.interview_state && session.interview_state.blueprintId) {
    blueprint = await store.getBlueprint(
      session.interview_state.blueprintId,
      session.interview_state.blueprintVersion
    );
  }
  const q = currentQuestion(session.interview_state);
  return withExperienceFields(session, {
    interviewId: session.id,
    ...publicSession(session),
    turns: turns.map(publicTurn),
    evidence: evidence.map(publicEvidence),
    blueprint: publicBlueprint(blueprint),
    question: q
      ? {
          id: q.question.id,
          prompt: q.question.prompt,
          stage: q.question.stage,
          section: q.question.section,
          goal: q.question.goal,
          askedBecause: q.question.askedBecause,
        }
      : null,
    sectionState: (session.interview_state && session.interview_state.sectionState) || {},
  });
}

async function getInterviewBlueprint(sessionId, opts = {}) {
  const detail = await getInterview(sessionId, opts);
  if (!detail.blueprint) {
    throw new ClientIntelligenceError(
      'blueprint_not_ready',
      'Blueprint not generated yet for this interview'
    );
  }
  return detail.blueprint;
}

async function getClientBlueprint(clientId, opts = {}) {
  const store = await resolveStore(opts);
  const id = asClientId(clientId);
  const approved = await store.listBlueprintsForClient(id, { status: 'approved' });
  if (approved[0]) return publicBlueprint(approved[0]);
  const any = await store.listBlueprintsForClient(id);
  if (any[0]) return publicBlueprint(any[0]);
  throw new ClientIntelligenceError('not_found', 'No blueprint for client', 404);
}

function bumpBlueprintVersion(version) {
  const m = String(version || '1.0').match(/^(\d+)(?:\.(\d+))?$/);
  if (!m) return '1.1';
  const major = Number(m[1]);
  const minor = Number(m[2] || 0) + 1;
  return `${major}.${minor}`;
}

/**
 * Client revise: creates a new blueprint version; never overwrites approved.
 * Edits become CLIENT_EDITED evidence.
 */
async function reviseBlueprint(blueprintId, revisions = {}, opts = {}) {
  const store = await resolveStore(opts);
  const current = await store.getBlueprint(blueprintId);
  if (!current) {
    throw new ClientIntelligenceError('not_found', 'Blueprint not found', 404);
  }
  if (current.status === 'approved') {
    // new version from approved
  } else if (!['draft', 'in_review'].includes(current.status)) {
    throw new ClientIntelligenceError(
      'invalid_status',
      `Cannot revise blueprint in status ${current.status}`
    );
  }

  const session = await store.getSession(current.session_id);
  if (!session) {
    throw new ClientIntelligenceError('not_found', 'Interview session not found', 404);
  }

  const sections = buildSectionsFromState(current.sections);
  const sectionEdits = revisions.sections || revisions;
  const editTurn = await store.insertTurn({
    id: newId(),
    session_id: session.id,
    speaker: 'client',
    message: `Blueprint revision: ${JSON.stringify(sectionEdits)}`,
    goal: 'Client blueprint edit',
    asked_because: 'Client corrected understanding before approval.',
    derived_evidence: [],
    created_at: new Date(),
  });

  const derived = [];
  for (const key of BLUEPRINT_SECTIONS) {
    if (!Object.prototype.hasOwnProperty.call(sectionEdits, key)) continue;
    const edit = sectionEdits[key];
    const summary =
      typeof edit === 'string'
        ? edit
        : edit && edit.summary != null
          ? String(edit.summary)
          : null;
    if (summary == null) continue;
    const { evidenceRow } = await applySectionUpdate(
      store,
      session,
      key,
      summary,
      'CLIENT_EDITED',
      editTurn.id
    );
    derived.push(evidenceRow.id);
    sections[key] = {
      ...(sections[key] || emptySection()),
      summary,
      confidence: clampConfidence(
        Math.max((sections[key] && sections[key].confidence) || 0, EXPLICIT_CONFIDENCE)
      ),
      evidenceIds: [
        ...((sections[key] && sections[key].evidenceIds) || []),
        evidenceRow.id,
      ],
      unknowns: (sections[key] && sections[key].unknowns
        ? sections[key].unknowns.filter((u) => !u.includes(key))
        : []),
    };
  }

  const nextVersion =
    current.status === 'approved'
      ? bumpBlueprintVersion(current.version)
      : current.version;

  if (current.status === 'approved') {
    const next = await store.insertBlueprint({
      id: current.id,
      client_id: current.client_id,
      session_id: current.session_id,
      version: nextVersion,
      status: 'in_review',
      generated_by: GENERATED_BY,
      sections,
      confidence_summary: confidenceSummaryFromSections(sections),
      playbook_id: null,
      playbook_version: null,
      section_provenance: {},
      parent_blueprint_id: current.id,
      created_at: new Date(),
      updated_at: new Date(),
    });
    session.interview_state = {
      ...session.interview_state,
      blueprintId: next.id,
      blueprintVersion: next.version,
      sectionState: sections,
    };
    if (session.status === 'APPROVED') {
      // reopen for review without skipping — only CLIENT_REVIEW is allowed after generation;
      // for recalibration of approved, keep session APPROVED and track new in_review blueprint
    } else {
      session.status = 'CLIENT_REVIEW';
    }
    await store.updateSession(session.id, {
      interview_state: session.interview_state,
      confidence_score: overallConfidence(next.confidence_summary),
    });
    return publicBlueprint(next);
  }

  const confidence_summary = confidenceSummaryFromSections(sections);
  const updated = await store.updateBlueprint(current.id, current.version, {
    sections,
    confidence_summary,
    status: 'in_review',
  });
  session.interview_state = {
    ...session.interview_state,
    sectionState: sections,
    blueprintId: updated.id,
    blueprintVersion: updated.version,
  };
  await store.updateSession(session.id, {
    interview_state: session.interview_state,
    confidence_score: overallConfidence(confidence_summary),
  });
  return publicBlueprint(updated);
}

/**
 * Resume discovery after CLIENT_REVIEW so the client can refine understanding.
 */
async function resumeInterview(sessionId, opts = {}) {
  const store = await resolveStore(opts);
  const session = await store.getSession(sessionId);
  if (!session) {
    throw new ClientIntelligenceError('not_found', 'Interview session not found', 404);
  }
  if (session.status === 'APPROVED') {
    throw new ClientIntelligenceError(
      'interview_complete',
      'Interview already approved; start a new session to recalibrate'
    );
  }
  if (session.status !== 'CLIENT_REVIEW') {
    throw new ClientIntelligenceError(
      'invalid_status',
      `Can only resume from CLIENT_REVIEW (was ${session.status})`
    );
  }

  advanceStatus(session, 'DISCOVERY');
  const state = {
    ...(session.interview_state || initialInterviewState()),
    done: false,
    refinementPass: true,
  };
  // Keep stepIndex past the bank so free-form refinement messages are accepted.
  if ((Number(state.stepIndex) || 0) < QUESTION_BANK.length) {
    state.stepIndex = QUESTION_BANK.length;
  }
  session.interview_state = state;
  session.current_stage = 'Refinement';

  const prompt =
    'What would you like to refine or add? Share anything that would sharpen my understanding.';
  await store.insertTurn({
    id: newId(),
    session_id: session.id,
    speaker: 'assistant',
    message: prompt,
    goal: 'Invite refinement of Business Blueprint understanding',
    asked_because: 'Client chose to refine before approving the Executive Summary or Blueprint.',
    derived_evidence: [],
    created_at: new Date(),
  });

  await store.updateSession(session.id, {
    status: 'DISCOVERY',
    current_stage: session.current_stage,
    interview_state: state,
  });

  return withExperienceFields(session, {
    interviewId: session.id,
    ...publicSession(session),
    nextAction: 'ASK',
    question: null,
    message: prompt,
    blueprint: null,
    reflection: null,
    resumed: true,
  });
}

/**
 * Approve blueprint: immutable snapshot + pending_review playbook handoff.
 */
async function approveBlueprint(blueprintId, opts = {}) {
  const store = await resolveStore(opts);
  const current = await store.getBlueprint(blueprintId);
  if (!current) {
    throw new ClientIntelligenceError('not_found', 'Blueprint not found', 404);
  }
  if (current.status === 'approved') {
    return {
      blueprint: publicBlueprint(current),
      playbook: current.playbook_id
        ? {
            id: current.playbook_id,
            version: current.playbook_version,
            status: 'pending_review',
          }
        : null,
      alreadyApproved: true,
    };
  }
  if (!['draft', 'in_review'].includes(current.status)) {
    throw new ClientIntelligenceError(
      'invalid_status',
      `Cannot approve blueprint in status ${current.status}`
    );
  }

  const session = await store.getSession(current.session_id);
  if (!session) {
    throw new ClientIntelligenceError('not_found', 'Interview session not found', 404);
  }
  if (session.status !== 'CLIENT_REVIEW') {
    throw new ClientIntelligenceError(
      'invalid_transition',
      `Session must be CLIENT_REVIEW to approve (was ${session.status})`
    );
  }

  const handoffOpts = { ...opts };
  if (store.kind === 'memory' && !handoffOpts.playbookStore) {
    handoffOpts.useMemoryPlaybookStore = true;
  }
  const handoff = await createPlaybookFromApprovedBlueprint(current, handoffOpts);
  const approved = await store.updateBlueprint(current.id, current.version, {
    status: 'approved',
    playbook_id: handoff.playbook.id,
    playbook_version: handoff.playbook.version,
    section_provenance: handoff.sectionProvenance,
  });
  await store.supersedeBlueprints(current.id, current.version);

  advanceStatus(session, 'APPROVED');
  session.completed_at = new Date();
  await store.updateSession(session.id, {
    status: 'APPROVED',
    completed_at: session.completed_at,
    summary: `Approved Business Blueprint ${approved.id}@${approved.version}`,
    interview_state: {
      ...session.interview_state,
      blueprintId: approved.id,
      blueprintVersion: approved.version,
      playbookId: handoff.playbook.id,
      playbookVersion: handoff.playbook.version,
    },
  });

  return {
    blueprint: publicBlueprint(approved),
    playbook: handoff.playbook,
    sectionProvenance: handoff.sectionProvenance,
    alreadyApproved: false,
  };
}

module.exports = {
  SESSION_STATUSES,
  ALLOWED_TRANSITIONS,
  BLUEPRINT_SECTIONS,
  EVIDENCE_TYPES,
  NEXT_ACTIONS,
  QUESTION_BANK,
  SECTION_TITLES,
  GENERATED_BY,
  MIN_SECTION_CONFIDENCE,
  ClientIntelligenceError,
  createMemoryStore,
  createPostgresStore,
  scoreEvidenceConfidence,
  summarizeSection,
  computeProgress,
  buildUnderstandingProgress,
  buildExecutiveSummary,
  buildExecutiveBusinessBrief,
  buildReflection,
  hasSpecificitySignals,
  looksAmbiguous,
  assertTransition,
  startClientInterview,
  postInterviewMessage,
  resumeInterview,
  getInterview,
  getInterviewBlueprint,
  getClientBlueprint,
  reviseBlueprint,
  approveBlueprint,
  detectContradiction,
  answerLooksEmpty,
};
