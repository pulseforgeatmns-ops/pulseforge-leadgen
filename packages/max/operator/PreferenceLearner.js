'use strict';

const { INTENT_TAGS, INTERACTION_TYPES } = require('./OperatorTypes');
const {
  buildSuggestions,
  isActiveDeskWorkflow,
  resolveActiveWorkContext,
} = require('../workspace/SuggestionEngine');

/**
 * Max conversational preference learning (SPEC-012).
 * Still deterministic. Still grounded. Only reorders suggestion chips.
 */

const INTENT_PATTERNS = Object.freeze([
  {
    tag: INTENT_TAGS.COMPARE,
    re: /\b(compare|versus|vs\.?|similar compan)/i,
  },
  {
    tag: INTENT_TAGS.CONFIDENCE,
    re: /\b(confidence|how sure|certainty|trust this)/i,
  },
  {
    tag: INTENT_TAGS.EVIDENCE,
    re: /\b(evidence|signal|inspect|supporting|proof)/i,
  },
  {
    tag: INTENT_TAGS.REASONING,
    re: /\b(explain|why|reasoning|walk through)/i,
  },
  {
    tag: INTENT_TAGS.POLICY,
    re: /\b(policy|allowed|approval|block)/i,
  },
  {
    tag: INTENT_TAGS.RISK,
    re: /\b(risk|contradict|danger|watch out)/i,
  },
  {
    tag: INTENT_TAGS.WATCH,
    re: /\b(watch alert|watches)\b/i,
  },
  {
    tag: INTENT_TAGS.CHANGE,
    re: /\b(changed|overnight|what shifted|movement)/i,
  },
  {
    tag: INTENT_TAGS.TIMELINE,
    re: /\b(timeline|history|recently)\b/i,
  },
]);

/**
 * In-process preference counters per tenant.
 */
class PreferenceLearner {
  constructor() {
    /** @type {Map<string, Record<string, number>>} */
    this._intents = new Map();
  }

  /**
   * @param {string} tenantId
   * @param {string} text - question or interaction hint
   */
  observeText(tenantId, text) {
    const tid = String(tenantId || '');
    if (!tid || !text) return this.snapshot(tid);
    const counts = this._intents.get(tid) || blankCounts();
    const tags = detectIntents(String(text));
    for (const tag of tags) {
      counts[tag] = (counts[tag] || 0) + 1;
    }
    this._intents.set(tid, counts);
    return this.snapshot(tid);
  }

  /**
   * Observe an InteractionEvent that carries preference signal.
   * @param {object} event
   */
  observeEvent(event) {
    if (!event || !event.tenantId) return null;
    if (event.type === INTERACTION_TYPES.ASKED_MAX) {
      const q =
        (event.payload && (event.payload.question || event.payload.text)) ||
        '';
      if (q) return this.observeText(event.tenantId, q);
    }
    if (event.type === INTERACTION_TYPES.COMPARED_COMPANIES) {
      return this.observeText(event.tenantId, 'compare companies');
    }
    if (event.type === INTERACTION_TYPES.OPENED_EVIDENCE) {
      return this.observeText(event.tenantId, 'inspect evidence');
    }
    if (event.type === INTERACTION_TYPES.EXPANDED_REASONING) {
      return this.observeText(event.tenantId, 'explain confidence reasoning');
    }
    if (event.type === INTERACTION_TYPES.OPENED_TIMELINE) {
      return this.observeText(event.tenantId, 'timeline history');
    }
    return this.snapshot(event.tenantId);
  }

  /**
   * @param {string} tenantId
   */
  snapshot(tenantId) {
    const counts = this._intents.get(String(tenantId || '')) || blankCounts();
    const ranked = Object.keys(counts)
      .filter((k) => counts[k] > 0)
      .sort((a, b) => counts[b] - counts[a] || a.localeCompare(b));
    return {
      tenantId: String(tenantId || ''),
      counts: { ...counts },
      topIntents: ranked.slice(0, 5),
    };
  }

  /**
   * Build personalized suggestion chips from context + preferences.
   * @param {object} context - normalized MaxContext
   * @param {string} tenantId
   * @returns {string[]}
   */
  personalizedSuggestions(context, tenantId) {
    const base = buildSuggestions(context);
    // Active desk workflows own chip selection — do not reorder toward
    // briefing/market preference habits (e.g. overnight / top opportunity).
    const awc = resolveActiveWorkContext(context);
    if (awc && isActiveDeskWorkflow(awc)) return base;
    const prefs = this.snapshot(tenantId);
    if (!prefs.topIntents.length) return base;
    return rankSuggestions(base, prefs.topIntents);
  }

  clear() {
    this._intents.clear();
  }
}

function detectIntents(text) {
  const found = [];
  for (const { tag, re } of INTENT_PATTERNS) {
    if (re.test(text)) found.push(tag);
  }
  return found;
}

function blankCounts() {
  const o = {};
  for (const tag of Object.values(INTENT_TAGS)) o[tag] = 0;
  return o;
}

/**
 * Stable reorder: prefer suggestions matching top intents, keep the rest.
 * @param {string[]} suggestions
 * @param {string[]} topIntents
 */
function rankSuggestions(suggestions, topIntents) {
  const scored = suggestions.map((s, index) => {
    let boost = 0;
    const lower = s.toLowerCase();
    for (let i = 0; i < topIntents.length; i++) {
      const tag = topIntents[i];
      if (suggestionMatchesIntent(lower, tag)) {
        boost += (topIntents.length - i) * 10;
      }
    }
    return { s, index, boost };
  });
  scored.sort((a, b) => b.boost - a.boost || a.index - b.index);
  return scored.map((x) => x.s);
}

function suggestionMatchesIntent(lower, tag) {
  switch (tag) {
    case INTENT_TAGS.COMPARE:
      return /compare/.test(lower);
    case INTENT_TAGS.CONFIDENCE:
      return /confidence|sure/.test(lower);
    case INTENT_TAGS.EVIDENCE:
      return /evidence|signal|supporting/.test(lower);
    case INTENT_TAGS.REASONING:
      return /explain|why|reasoning|walk/.test(lower);
    case INTENT_TAGS.POLICY:
      return /policy|approval|allowed/.test(lower);
    case INTENT_TAGS.RISK:
      return /risk|contradict/.test(lower);
    case INTENT_TAGS.WATCH:
      return /watch/.test(lower);
    case INTENT_TAGS.CHANGE:
      return /changed|overnight|shift/.test(lower);
    case INTENT_TAGS.TIMELINE:
      return /recent|history|timeline|movement/.test(lower);
    default:
      return false;
  }
}

module.exports = {
  PreferenceLearner,
  detectIntents,
  rankSuggestions,
  INTENT_PATTERNS,
};
