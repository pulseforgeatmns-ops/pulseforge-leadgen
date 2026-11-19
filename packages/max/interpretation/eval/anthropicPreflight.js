'use strict';

/**
 * Free availability and compatibility check for an Anthropic interpretation
 * model (P0-MAX-REASONING-001). One Models API lookup (`models.retrieve`): no
 * tokens, no spend. Run before any paid evaluation call.
 *
 * The model must exist under its exact id, allow at least MAX_TOKENS of
 * output, support structured outputs (the interpreter's json_schema output),
 * and, when the run turns thinking off, accept `thinking: {type: "disabled"}`.
 * The response is read defensively: a missing capability is reported as
 * unknown, and only an explicit `supported: false` fails the check.
 */

const { MAX_TOKENS } = require('../producers/interpretationPrompt');

function leaf(caps, path) {
  let node = caps;
  for (const key of path) {
    if (!node || typeof node !== 'object') return null;
    node = node[key];
  }
  return node && typeof node === 'object' && typeof node.supported === 'boolean' ? node.supported : null;
}

/**
 * @param {{ models: { retrieve: Function } }} client
 * @param {string} model
 * @param {{ thinking?: null|'disabled' }} [options]
 */
async function anthropicPreflight(client, model, { thinking = null } = {}) {
  if (!client || !client.models || typeof client.models.retrieve !== 'function') {
    return { available: false, model, reason: 'preflight_client_unsupported' };
  }
  let info;
  try {
    info = await client.models.retrieve(model);
  } catch (err) {
    const status = err && Number.isInteger(err.status) ? err.status : null;
    return { available: false, model, httpStatus: status, reason: status ? `preflight_http_${status}` : `preflight_request_failed:${err && err.name ? err.name : 'error'}` };
  }
  const caps = info && info.capabilities;
  const result = {
    model,
    id: info && typeof info.id === 'string' ? info.id : null,
    displayName: info && typeof info.display_name === 'string' ? info.display_name : null,
    maxInputTokens: info && Number.isSafeInteger(info.max_input_tokens) ? info.max_input_tokens : null,
    maxOutputTokens: info && Number.isSafeInteger(info.max_tokens) ? info.max_tokens : null,
    structuredOutputs: leaf(caps, ['structured_outputs']),
    thinkingDisabledAccepted: leaf(caps, ['thinking', 'types', 'disabled']),
    thinkingRequested: thinking,
  };
  if (result.id !== model) return { available: false, ...result, reason: 'preflight_model_id_mismatch' };
  if (result.maxOutputTokens !== null && result.maxOutputTokens < MAX_TOKENS) return { available: false, ...result, reason: 'preflight_output_limit_below_request' };
  if (result.structuredOutputs === false) return { available: false, ...result, reason: 'preflight_structured_outputs_unsupported' };
  if (thinking === 'disabled' && result.thinkingDisabledAccepted === false) return { available: false, ...result, reason: 'preflight_thinking_disabled_unsupported' };
  return { available: true, ...result };
}

module.exports = { anthropicPreflight };
