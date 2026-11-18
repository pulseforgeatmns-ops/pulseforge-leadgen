'use strict';

/**
 * Interpretation provider port (P0-MAX-REASONING-001).
 *
 * A provider turns the provider-neutral interpretation prompt into one call
 * and normalizes the answer:
 *
 *   complete({ model, text, context, transcript, timeoutMs, signal })
 *     → { finish: 'complete'|'truncated'|'refused'|'other', rawStopReason,
 *         text, servedModel, usage }
 *
 * and exposes `buildRequest(...)` (the exact request, for offline pricing).
 * Providers never read credentials, never retry, and never validate frames:
 * the ModelFrameProducer owns validation and degradation for every provider.
 */

const { createAnthropicProvider, NAME: ANTHROPIC } = require('./AnthropicProvider');
const { createGeminiProvider, NAME: GEMINI } = require('./GeminiProvider');

const PROVIDERS = Object.freeze([ANTHROPIC, GEMINI]);

function isProvider(value) {
  return Boolean(value) && typeof value.complete === 'function' && typeof value.name === 'string';
}

module.exports = { PROVIDERS, ANTHROPIC, GEMINI, createAnthropicProvider, createGeminiProvider, isProvider };
