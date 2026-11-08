'use strict';

/**
 * P0 question-directed reasoning (follow-up to #1089).
 *
 * Routing (OperationalEvidenceContinuity.detectOperationalAnalysisFollowUp)
 * decides THAT a turn is read-only analysis. This module decides WHAT the
 * analysis is about, so the answer can be proportional to the question
 * instead of always rendering the portfolio-wide report:
 *
 *   {
 *     question,         // the operator's text (verbatim)
 *     affirmative,      // the text with negated constraint clauses removed
 *     subject,          // 'mission' | 'portfolio'
 *     subjectIds,       // resolved evidence mission ids (mission subject)
 *     reference,        // how the subject was resolved (id / description / focus)
 *     unresolved,       // a named mission that could not be matched (id or phrase)
 *     candidates,       // ambiguous matches (operator must pick)
 *     facets,           // what is being asked (see MISSION_FACETS / PORTFOLIO_FACETS)
 *     intent,           // explanation | diagnosis | evaluation | comparison | portfolio_overview
 *     responseScope,    // 'focused' | 'portfolio'
 *     actionAuthorized, // always false: the analytical route never carries authority
 *   }
 *
 * Mission resolution is grounded only in the tenant's evidence record (ids,
 * titles, objectives, dates) and in the session's operational focus (the
 * mission the previous analytical turn was about). Nothing here reads a
 * database, a mission runtime, or a pending decision.
 */

const { stripNegatedDirectives } = require('./NegatedDirective');

// ---------------------------------------------------------------------------
// Facets
// ---------------------------------------------------------------------------

const MISSION_FACETS = Object.freeze({
  CLASSIFICATION: 'classification', // why is it stale / uncertain / classified as …
  RECONSIDERATION: 'reconsideration', // what would justify treating it as current again
  RELEVANCE: 'relevance', // is it still relevant to the objective
  ABANDONMENT: 'abandonment', // does stale mean abandon it
  HISTORY: 'history', // what happened to it / when was it last touched
  COMPARISON: 'comparison', // compare two missions
  EVIDENCE: 'evidence', // generic "what do we know about it" (default)
});

const PORTFOLIO_FACETS = Object.freeze({
  OBJECTIVE: 'objective',
  RELEVANCE: 'relevance',
  SAFETY: 'safety',
  RECOMMENDATION: 'recommendation',
  OVERVIEW: 'overview',
});

const MISSION_FACET_PATTERNS = [
  [MISSION_FACETS.COMPARISON, /\bcompar(?:e|ed|ing|ison)\b|\bversus\b|\bvs\.?\s|\bmore\s+recent\b|\bwhich\s+(?:one\s+)?(?:has|is|was)\b|\bdifference\s+between\b/i],
  [MISSION_FACETS.RECONSIDERATION, /\bjustif(?:y|ies|ied)\b|\breconsider|\breclassif|\btreat(?:ed|ing)?\b[^.?!]{0,40}\bas\s+(?:current|active|live)\b|\b(?:current|active)\s+again\b|\b(?:become|be|count\s+as)\s+(?:current|active)\b|\bwhat\s+would\s+(?:it\s+take|make|change)\b|\brevive\b|\bbring\s+(?:it|this|that)\s+back\b/i],
  [MISSION_FACETS.ABANDONMENT, /\babandon|\bdead\b|\bgive\s+up\b|\bwrite\s+(?:it\s+)?off\b|\bobsolete\b|\bsuperseded\b|\bdrop\s+(?:it|this|that|the)\b|\bshut\s+(?:it\s+)?down\b/i],
  [MISSION_FACETS.RELEVANCE, /\brelevan(?:t|ce|cy)\b|\brelat(?:e|es|ed|ion|ionship)\b[^.?!]{0,60}\bobjective\b|\bstill\s+(?:matter|matters|useful|needed|valid|worth)\b|\b(?:current|present|canonical)\s+(?:business\s+)?objective\b|\balign(?:ed|s|ment)?\b/i],
  [MISSION_FACETS.CLASSIFICATION, /\bclassif(?:y|ied|ication)\b|\bcategori[sz]|\b(?:why|how)\b[^.?!]{0,60}\b(?:stale|uncertain|ambiguous|historical|active|completed|current)\b|\bwhat\s+makes\s+(?:it|this|that)\b|\bconsidered\s+(?:stale|historical|uncertain)\b/i],
  [MISSION_FACETS.HISTORY, /\bwhat\s+happened\b|\bhistory\b|\btimeline\b|\blast\s+(?:activity|touched|updated|recorded|event|change)\b|\bwhen\s+(?:was|did|is)\b|\bhow\s+(?:old|long)\b/i],
];

const PORTFOLIO_FACET_PATTERNS = [
  [PORTFOLIO_FACETS.OVERVIEW, /\b(?:full|complete|comprehensive|whole|entire|overall)\s+(?:operational\s+)?(?:portfolio|picture|analysis|report|summary|overview|view)\b|\bsummari[sz]e\s+(?:the\s+)?(?:whole|entire|full)?\s*(?:portfolio|everything|operations?)\b|\beverything\b|\bportfolio\s+(?:summary|overview|analysis|report)\b/i],
  [PORTFOLIO_FACETS.OBJECTIVE, /\bcanonical\b|\b(?:business|primary|main|single)\s+objective\b|\bobjective\b/i],
  [PORTFOLIO_FACETS.RELEVANCE, /\b(?:genuinely\s+)?(?:current|relevant|historical|ambiguous|stale|active|uncertain)\b[^.?!]{0,40}\bmissions?\b|\bmissions?\b[^.?!]{0,40}\b(?:current|relevant|historical|ambiguous|stale|active|uncertain)\b|\bpending\s+(?:approvals?|decisions?)\b/i],
  [PORTFOLIO_FACETS.SAFETY, /\boutbound\b|\b(?:safety\s+)?gate\b|\bkill[\s-]?switch\b|\bmaintenance\b|\bpaused?\b|\bfailures?\b|\berrors?\b|\bblockers?\b|\btouchpoints?\b/i],
  [PORTFOLIO_FACETS.RECOMMENDATION, /\brecommend|\bnext\s+(?:reliability\s+|validation\s+)*step\b|\bvalidation\s+step\b|\bwhat\s+should\s+(?:I|we)\s+(?:do|check|validate)\b/i],
];

/** Facets in the order the operator asked them (first match position). */
function detectMissionFacets(text) {
  return MISSION_FACET_PATTERNS
    .map(([f, re]) => {
      const m = re.exec(text);
      return m ? { f, at: m.index } : null;
    })
    .filter(Boolean)
    .sort((a, b) => a.at - b.at)
    .map((x) => x.f);
}

function detectPortfolioFacets(text) {
  return PORTFOLIO_FACET_PATTERNS.filter(([, re]) => re.test(text)).map(([f]) => f);
}

// ---------------------------------------------------------------------------
// Mission reference resolution
// ---------------------------------------------------------------------------

const MONTHS = ['january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', 'october', 'november', 'december'];
const MONTH_RE = '(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|june?|july?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)';
const DATE_IN_PHRASE_RE = new RegExp(`\\b${MONTH_RE}\\.?\\s+(\\d{1,2})(?:st|nd|rd|th)?\\b`, 'i');

/** Mission-id shaped token: am_x, mission_crm_…, a uuid. */
const MISSION_ID_TOKEN_RE = /\b(?:am|mission|msn|mis|amo)_[a-z0-9_]{3,}\b|\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b/gi;

/** "the <descriptor…> mission" — the descriptor names one mission. */
const DESCRIPTOR_WORD = "(?!(?:the|this|that|these|those|a|an|it|its|is|are|was|were|be|to|for|with|from|by|about|on|in|of|and|or|but|our|my|your|their|than|as)\\b)[A-Za-z0-9][\\w'’.-]*";
const DESCRIBED_MISSION_RE = new RegExp(`\\b(?:the|that|this|our)\\s+((?:${DESCRIPTOR_WORD}\\s+){1,6}?)mission\\b(?!s)`, 'gi');

/** Descriptors that do not identify a particular mission on their own. */
const GENERIC_DESCRIPTOR_WORDS = new Set([
  'current', 'active', 'selected', 'same', 'other', 'acquisition', 'new', 'next', 'previous', 'last', 'first',
  'second', 'whole', 'entire', 'my', 'our', 'your', 'this', 'that', 'one', 'above', 'mentioned', 'named',
  'specific', 'stale', 'historical', 'ambiguous', 'uncertain', 'completed', 'bound', 'open', 'existing',
]);

/** Pronoun / anaphor reference to the mission in focus. */
const FOCUS_ANAPHOR_RE = /\b(?:it|its|it['’]s|this\s+(?:one|mission)|that\s+(?:one|mission)|the\s+mission|the\s+same\s+mission|them|both|they|those\s+two|these\s+two|the\s+two)\b/i;

const STOPWORDS = new Set(['the', 'a', 'an', 'of', 'for', 'and', 'to', 'in', 'on', 'with', 'mission', 'missions', 's']);

function tokens(text) {
  return String(text || '')
    .toLowerCase()
    .replace(/[_—–-]+/g, ' ')
    .split(/[^a-z0-9]+/)
    .filter((t) => t && !STOPWORDS.has(t));
}

/** Loose stem: preparation ≈ prepare ≈ prepared; governed ≈ governance. */
function stem(t) {
  return t.length > 5 ? t.slice(0, 5) : t;
}

function parsePhraseDate(phrase) {
  const m = DATE_IN_PHRASE_RE.exec(phrase);
  if (!m) return null;
  const month = MONTHS.findIndex((name) => name.startsWith(m[1].toLowerCase().slice(0, 3))) + 1;
  const day = Number(m[2]);
  if (!month || day < 1 || day > 31) return null;
  return { month, day, text: m[0] };
}

function missionDates(m) {
  return [m.createdAt, m.lastActivityAt, m.updatedAt, m.lastEvent && m.lastEvent.at]
    .filter(Boolean)
    .map((ts) => new Date(ts))
    .filter((d) => !Number.isNaN(d.getTime()));
}

function titleMentionsDate(m, date) {
  const text = `${m.title || ''} ${m.objective || ''}`;
  const re = new RegExp(`\\b${MONTH_RE}\\.?\\s+0?${date.day}(?:st|nd|rd|th)?\\b`, 'ig');
  let hit;
  while ((hit = re.exec(text))) {
    if (MONTHS.findIndex((name) => name.startsWith(hit[1].toLowerCase().slice(0, 3))) + 1 === date.month) return true;
  }
  const iso = new RegExp(`\\b\\d{4}-${String(date.month).padStart(2, '0')}-${String(date.day).padStart(2, '0')}\\b`);
  return iso.test(text) || iso.test(String(m.id || ''));
}

function matchesDate(m, date) {
  if (titleMentionsDate(m, date)) return true;
  return missionDates(m).some((d) => d.getUTCMonth() + 1 === date.month && d.getUTCDate() === date.day);
}

/**
 * Score a descriptor against one mission. Every descriptive word must match
 * (title, objective, or id); a date in the phrase must match a persisted date
 * or the title. Returns 0 for no match.
 */
function scoreDescriptor(descriptor, m) {
  const date = parsePhraseDate(descriptor);
  const words = tokens(date ? descriptor.replace(date.text, ' ') : descriptor).filter((w) => !GENERIC_DESCRIPTOR_WORDS.has(w));
  if (!words.length && !date) return 0;
  if (date && !matchesDate(m, date)) return 0;
  const hay = new Set([...tokens(m.title), ...tokens(m.objective), ...tokens(m.id)].map(stem));
  const titleHay = new Set([...tokens(m.title), ...tokens(m.id)].map(stem));
  let score = date ? 2 : 0;
  for (const w of words) {
    const s = stem(w);
    if (!hay.has(s)) return 0;
    score += titleHay.has(s) ? 2 : 1;
  }
  return score;
}

function extractDescriptors(text) {
  const out = [];
  DESCRIBED_MISSION_RE.lastIndex = 0;
  let m;
  while ((m = DESCRIBED_MISSION_RE.exec(text))) {
    const descriptor = m[1].trim();
    const meaningful = tokens(descriptor).filter((w) => !GENERIC_DESCRIPTOR_WORDS.has(w));
    if (meaningful.length || parsePhraseDate(descriptor)) out.push({ phrase: `${m[0]}`, descriptor });
  }
  return out;
}

function extractIdTokens(text) {
  const out = [];
  const seen = new Set();
  for (const hit of String(text).match(MISSION_ID_TOKEN_RE) || []) {
    const id = hit.toLowerCase();
    if (!seen.has(id)) {
      seen.add(id);
      out.push(hit);
    }
  }
  return out;
}

/**
 * Resolve mission references in the (affirmative) text against the evidence.
 * @returns {{ ids: string[], references: object[], unresolved: object[], candidates: object[] }}
 */
function resolveMissionReferences(text, record) {
  const missions = record && Array.isArray(record.missions) ? record.missions : [];
  const byLowerId = new Map(missions.map((m) => [String(m.id).toLowerCase(), m]));
  const ids = [];
  const references = [];
  const unresolved = [];
  const candidates = [];
  const add = (id, ref) => {
    if (!ids.includes(id)) ids.push(id);
    references.push(ref);
  };

  for (const token of extractIdTokens(text)) {
    const m = byLowerId.get(token.toLowerCase());
    if (m) add(m.id, { kind: 'id', text: token, missionId: m.id });
    else unresolved.push({ kind: 'id', text: token });
  }
  // Ids embedded in free text without a recognised prefix.
  const lower = String(text).toLowerCase();
  for (const m of missions) {
    const id = String(m.id).toLowerCase();
    if (id.length >= 6 && lower.includes(id) && !ids.includes(m.id)) add(m.id, { kind: 'id', text: m.id, missionId: m.id });
  }

  for (const { phrase, descriptor } of extractDescriptors(text)) {
    // A descriptor that merely repeats an id already resolved adds nothing.
    if (extractIdTokens(descriptor).length) continue;
    const scored = missions
      .map((m) => ({ m, score: scoreDescriptor(descriptor, m) }))
      .filter((x) => x.score > 0)
      .sort((a, b) => b.score - a.score);
    if (!scored.length) {
      unresolved.push({ kind: 'description', text: phrase.trim() });
      continue;
    }
    const top = scored.filter((x) => x.score === scored[0].score);
    if (top.length === 1) {
      add(top[0].m.id, { kind: 'description', text: phrase.trim(), missionId: top[0].m.id });
    } else {
      candidates.push({ text: phrase.trim(), missionIds: top.map((x) => x.m.id) });
    }
  }
  return { ids, references, unresolved, candidates };
}

// ---------------------------------------------------------------------------
// Focus (conversational referent)
// ---------------------------------------------------------------------------

/** Focus older than this is not used to resolve a pronoun. */
const FOCUS_TTL_MS = 30 * 60 * 1000;

function currentFocus(session, tenantId, now = new Date()) {
  const f = session ? session.operationalFocus || null : null;
  if (!f || String(f.tenantId) !== String(tenantId) || !Array.isArray(f.missionIds) || !f.missionIds.length) return null;
  const age = now.getTime() - new Date(f.at).getTime();
  if (!Number.isFinite(age) || age > FOCUS_TTL_MS) return null;
  return f;
}

function setFocus(session, { tenantId, missionIds, at }) {
  if (!session || typeof session !== 'object') return null;
  session.operationalFocus = missionIds && missionIds.length
    ? { tenantId: String(tenantId), missionIds: [...missionIds], at: at || new Date().toISOString() }
    : null;
  return session.operationalFocus;
}

function clearFocus(session) {
  if (session && typeof session === 'object' && session.operationalFocus) session.operationalFocus = null;
}

// ---------------------------------------------------------------------------
// Frame
// ---------------------------------------------------------------------------

/** Interrogative / explanatory form — required for a mission-focused turn. */
const INTERROGATIVE_RE =
  /\?|^\s*(?:and\s+|so\s+|ok(?:ay)?,?\s+|then\s+)?(?:why|what|how|which|when|where|who|is|are|was|were|does|do|did|has|have|had|should|would|could|can|explain|describe|compare|tell\s+me|walk\s+me|help\s+me\s+understand|summari[sz]e)\b|\b(?:explain|describe|compare|tell\s+me|walk\s+me\s+through|help\s+me\s+understand)\b/i;

function isInterrogative(text) {
  return INTERROGATIVE_RE.test(String(text || ''));
}

function intentFor(subject, facets) {
  if (subject === 'portfolio') return facets.includes(PORTFOLIO_FACETS.OVERVIEW) || facets.length !== 1 ? 'portfolio_overview' : 'explanation';
  if (facets.includes(MISSION_FACETS.COMPARISON)) return 'comparison';
  if (facets.includes(MISSION_FACETS.RELEVANCE) || facets.includes(MISSION_FACETS.ABANDONMENT)) return 'evaluation';
  if (facets.includes(MISSION_FACETS.RECONSIDERATION)) return 'diagnosis';
  return 'explanation';
}

/**
 * Build the analytical frame for a turn.
 * @param {string} question
 * @param {object|null} record evidence record (may be null)
 * @param {{ focus?: object|null }} [options]
 */
function buildAnalyticalFrame(question, record, options = {}) {
  const raw = String(question || '').trim();
  const affirmative = stripNegatedDirectives(raw) || '';
  const resolved = resolveMissionReferences(affirmative, record);
  let subjectIds = resolved.ids;
  let reference = resolved.references.length ? resolved.references[0].kind : null;

  const focus = options.focus || null;
  const usesAnaphor = FOCUS_ANAPHOR_RE.test(affirmative);
  if (!subjectIds.length && !resolved.unresolved.length && !resolved.candidates.length && focus && usesAnaphor) {
    const known = new Set(((record && record.missions) || []).map((m) => m.id));
    subjectIds = focus.missionIds.filter((id) => known.has(id) || !record);
    if (subjectIds.length) reference = 'focus';
  } else if (
    subjectIds.length === 1 &&
    focus &&
    usesAnaphor &&
    detectMissionFacets(affirmative).includes(MISSION_FACETS.COMPARISON)
  ) {
    // "How does it compare with the outbound mission?" — add the focused referent.
    for (const id of focus.missionIds) if (!subjectIds.includes(id)) subjectIds = [...subjectIds, id];
  }

  const namesMission = subjectIds.length > 0 || resolved.unresolved.length > 0 || resolved.candidates.length > 0;
  const subject = namesMission ? 'mission' : 'portfolio';
  let facets;
  if (subject === 'mission') {
    facets = detectMissionFacets(affirmative);
    if (subjectIds.length > 1 && !facets.includes(MISSION_FACETS.COMPARISON)) facets.unshift(MISSION_FACETS.COMPARISON);
    if (!facets.length) facets = [MISSION_FACETS.EVIDENCE];
  } else {
    facets = detectPortfolioFacets(affirmative);
  }
  const portfolioFull =
    subject === 'portfolio' && (!facets.length || facets.includes(PORTFOLIO_FACETS.OVERVIEW) || facets.filter((f) => f !== PORTFOLIO_FACETS.OVERVIEW).length >= 3);

  return {
    question: raw,
    affirmative,
    subject,
    subjectIds,
    reference,
    references: resolved.references,
    unresolved: resolved.unresolved,
    candidates: resolved.candidates,
    usesAnaphor,
    facets,
    intent: intentFor(subject, portfolioFull ? [PORTFOLIO_FACETS.OVERVIEW] : facets),
    responseScope: subject === 'mission' || !portfolioFull ? 'focused' : 'portfolio',
    actionAuthorized: false,
  };
}

/** Compact, log-safe view of a frame (no titles, objectives, or question text). */
function frameTrace(frame) {
  if (!frame) return null;
  return {
    subject: frame.subject,
    intent: frame.intent,
    responseScope: frame.responseScope,
    facets: frame.facets,
    reference: frame.reference,
    subjectCount: frame.subjectIds.length,
    unresolved: frame.unresolved.map((u) => u.kind),
    ambiguous: frame.candidates.length,
    actionAuthorized: false,
  };
}

module.exports = {
  MISSION_FACETS,
  PORTFOLIO_FACETS,
  FOCUS_TTL_MS,
  buildAnalyticalFrame,
  resolveMissionReferences,
  detectMissionFacets,
  detectPortfolioFacets,
  isInterrogative,
  currentFocus,
  setFocus,
  clearFocus,
  frameTrace,
};
