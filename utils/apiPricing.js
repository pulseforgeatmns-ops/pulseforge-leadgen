'use strict';

// Verified 2026-10-14. Estimates exclude free tiers, discounts, tax and credits.
// Places reservations cover the highest public field tier; historical rows
// have no field masks and therefore cannot be reconciled to exact SKUs.
const PLACES_UPPER_BOUND = Object.freeze({
  text_search: 0.04, search_text: 0.04, nearby_search: 0.04,
  place_details: 0.025, place_details_v1: 0.025, find_place: 0.025,
  autocomplete: 0.00283, geocode: 0.005,
});
const CLAUDE_RATES = Object.freeze({
  'claude-opus-5-5': [4, 20, 0.05],
  'claude-sonnet-5-5': [2, 10, 0.05],
  'claude-sonnet-4-6': [3, 15],
  'claude-sonnet-4-5': [3, 15],
  'claude-sonnet-4': [3, 15],
  'claude-haiku-4-5': [1, 5],
  'claude-3-5-haiku': [0.8, 4],
  'claude-opus-4-8': [5, 25],
  'claude-opus-4-7': [5, 25],
  'claude-opus-4-6': [5, 25],
  'claude-opus-4-5': [5, 25],
  'claude-opus-4-1': [15, 75],
  'claude-opus-4': [15, 75],
});

function modelRates(model) {
  const name = String(model || '');
  // Accept exact aliases and dated snapshots, never an unknown future version.
  const key = Object.keys(CLAUDE_RATES).find(k => name === k || new RegExp(`^${k}-\\d{8}$`).test(name));
  return key ? CLAUDE_RATES[key] : null;
}
function tokenCount(value) {
  return Number.isSafeInteger(value) && value >= 0 ? value : null;
}
function sanitizedUsage(usage = {}) {
  const result = {};
  for (const k of ['input_tokens', 'output_tokens', 'cache_read_input_tokens', 'cache_creation_input_tokens']) {
    const n = tokenCount(usage[k]);
    if (n !== null) result[k] = n;
  }
  if (usage.input_tokens + (usage.cache_read_input_tokens || 0) + (usage.cache_creation_input_tokens || 0) > 200000) return null;
  const creation = usage.cache_creation || {};
  for (const k of ['ephemeral_5m_input_tokens', 'ephemeral_1h_input_tokens']) {
    const n = tokenCount(creation[k]);
    if (n !== null) (result.cache_creation ||= {})[k] = n;
  }
  return result;
}
function estimateClaudeUsage(model, rawUsage, params = {}) {
  const rates = modelRates(model);
  const usage = sanitizedUsage(rawUsage);
  if (!rates || usage.input_tokens == null || usage.output_tokens == null) return null;
  // Server tools, fast mode and batch pricing are not silently treated as standard.
  if (params.inference_geo === 'us' || params.speed === 'fast' || (params.tools || []).some(t => t.type && t.type !== 'custom')) return null;
  const [input, output, cacheRead = 0.1] = rates;
  if (usage.input_tokens + (usage.cache_read_input_tokens || 0) + (usage.cache_creation_input_tokens || 0) > 200000) return null;
  const creation = usage.cache_creation || {};
  const five = creation.ephemeral_5m_input_tokens || 0;
  const hour = creation.ephemeral_1h_input_tokens || 0;
  const extraCreation = Math.max(0, (usage.cache_creation_input_tokens || 0) - five - hour);
  // Unknown cache duration: use 1h price (2x), rather than under-reporting it.
  const inputCharge = usage.input_tokens + cacheRead * (usage.cache_read_input_tokens || 0)
    + 1.25 * five + 2 * (hour + extraCreation);
  return (inputCharge * input + usage.output_tokens * output) / 1e6;
}
function estimateClaudeReservation(params = {}) {
  const rates = modelRates(params.model);
  if (!rates || !Number.isSafeInteger(params.max_tokens) || params.max_tokens < 1 || params.inference_geo === 'us' || params.speed === 'fast') return null;
  const blocks = [...(Array.isArray(params.system) ? params.system : []), ...(params.messages || []).flatMap(m => Array.isArray(m.content) ? m.content : [])];
  if (blocks.some(b => b.type && !['text', 'tool_use', 'tool_result'].includes(b.type))) return null;
  if ((params.tools || []).some(t => t.type && t.type !== 'custom')) return null;
  // A conservative text-only token bound. Unknown modalities stay unpriced.
  // Long contexts may have premium pricing on older models; don't pretend to know.
  const bytes = Buffer.byteLength(JSON.stringify({ system: params.system, messages: params.messages, tools: params.tools }), 'utf8') + 4096;
  if (bytes > 180000) return null;
  return (bytes * rates[0] * 2 + params.max_tokens * rates[1]) / 1e6;
}

module.exports = { PLACES_UPPER_BOUND, CLAUDE_RATES, modelRates, sanitizedUsage, estimateClaudeUsage, estimateClaudeReservation };
