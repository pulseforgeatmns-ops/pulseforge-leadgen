'use strict';

/**
 * Canonical adapter: Blueprint section keys and persisted aliases → interview
 * normalizedFacts keys consumed by sectionsFromNormalizedFacts.
 *
 * Run immediately before Blueprint composition (prepareNormalizedFactsForBrief).
 * No prose heuristics — key mapping and value-shape merge only.
 */

/** Blueprint review / section key → canonical normalizedFacts field */
const BLUEPRINT_SECTION_TO_CANONICAL_FACT = Object.freeze({
  identity: ['business_name', 'business_description'],
  services: ['services'],
  idealCustomers: ['ideal_customers'],
  avoidCustomers: ['disqualified_customers'],
  targetMarkets: ['geography', 'vertical_focus'],
  competitiveAdvantages: ['differentiation'],
  brandVoice: ['brand_voice'],
  campaignGoals: ['ninety_day_outcomes'],
  successMetrics: ['success_metrics'],
});

/**
 * Top-level alias keys that may appear on persisted normalizedFacts (camelCase,
 * canonical projection names, or legacy slots) → single canonical field.
 */
const ALIAS_TO_CANONICAL_FACT = Object.freeze({
  businessName: 'business_name',
  businessDescription: 'business_description',
  idealCustomers: 'ideal_customers',
  avoidCustomers: 'disqualified_customers',
  disqualifiedCustomers: 'disqualified_customers',
  avoid_customers: 'disqualified_customers',
  targetMarkets: 'geography',
  target_markets: 'geography',
  competitiveAdvantages: 'differentiation',
  brandVoice: 'brand_voice',
  campaignGoals: 'ninety_day_outcomes',
  campaign_goals: 'ninety_day_outcomes',
  growth_goals: 'ninety_day_outcomes',
  successMetrics: 'success_metrics',
});

const LIST_CANONICAL_FIELDS = new Set([
  'services',
  'ideal_customers',
  'ideal_customer_traits',
  'disqualified_customers',
  'geography',
  'success_metrics',
  'qualification_signals',
]);

const SCALAR_CANONICAL_FIELDS = new Set([
  'business_name',
  'business_description',
  'differentiation',
  'brand_voice',
  'ninety_day_outcomes',
  'vertical_focus',
  'growth_focus',
]);

const EPISTEMIC_BAG_KEYS = ['epistemic_states', 'hypotheses', 'evidence_statements'];

function coerceList(value) {
  if (value == null || value === '') return [];
  return Array.isArray(value) ? value : [value];
}

function mergeScalar(existing, incoming) {
  if (incoming == null || incoming === '') return existing;
  if (existing == null || existing === '') {
    return typeof incoming === 'string' ? incoming.trim() : incoming;
  }
  return existing;
}

function mergeList(existing, incoming) {
  const out = [...(existing || [])];
  for (const item of coerceList(incoming)) {
    const text = String(item || '').trim();
    if (!text) continue;
    if (!out.some((prior) => String(prior).trim() === text)) out.push(item);
  }
  return out;
}

function remapEpistemicBag(bag) {
  if (!bag || typeof bag !== 'object') return {};
  const next = { ...bag };
  for (const [alias, canonical] of Object.entries(ALIAS_TO_CANONICAL_FACT)) {
    if (alias === canonical) continue;
    if (!Object.prototype.hasOwnProperty.call(bag, alias)) continue;
    const value = bag[alias];
    if (value == null || value === '') {
      delete next[alias];
      continue;
    }
    // Alias keys (Blueprint section ids) win over default UNRESOLVED on canonical slots.
    next[canonical] = value;
    delete next[alias];
  }
  return next;
}

/**
 * Merge alias keys from raw persisted facts into a cloneNormalizedFacts-shaped object.
 *
 * @param {object|null|undefined} rawFacts persisted normalizedFacts (any key shape)
 * @param {object} canonical cloneNormalizedFacts() output to mutate
 * @returns {object} canonical facts with aliases folded in
 */
function adaptNormalizedFactsKeys(rawFacts, canonical) {
  const raw = rawFacts || {};
  const next = canonical;

  for (const [alias, target] of Object.entries(ALIAS_TO_CANONICAL_FACT)) {
    if (!Object.prototype.hasOwnProperty.call(raw, alias)) continue;
    const value = raw[alias];
    if (value == null || value === '') continue;

    if (LIST_CANONICAL_FIELDS.has(target)) {
      next[target] = mergeList(next[target], value);
    } else if (SCALAR_CANONICAL_FIELDS.has(target)) {
      next[target] = mergeScalar(next[target], value);
    }
  }

  for (const bagKey of EPISTEMIC_BAG_KEYS) {
    next[bagKey] = remapEpistemicBag({
      ...(next[bagKey] || {}),
      ...(raw[bagKey] || {}),
    });
  }

  return next;
}

module.exports = {
  BLUEPRINT_SECTION_TO_CANONICAL_FACT,
  ALIAS_TO_CANONICAL_FACT,
  adaptNormalizedFactsKeys,
};
