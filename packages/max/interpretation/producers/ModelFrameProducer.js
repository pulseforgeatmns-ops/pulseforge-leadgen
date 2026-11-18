'use strict';

/**
 * Model-backed TurnFrame producer (PROTOTYPE, NOT WIRED).
 *
 * One small structured-output call per operator turn. The model sees only:
 * a stable instruction prompt (cacheable), the candidate referent list
 * (id + kind + short label), the bound pending-decision id, the current focus,
 * the last few redacted transcript lines, and the operator message as data.
 * It never sees evidence rows, customer records, credentials, or tools.
 *
 * Every response is validated by validateTurnFrame. Timeout, HTTP error,
 * refusal, truncation, non-JSON or schema violation all degrade to the
 * lexical producer with producer = 'model_degraded_to_lexical', which can
 * never nominate an execution candidate.
 *
 * Transport is a provider (../providers): Anthropic (`messages.create`
 * client, the original path, request unchanged) or Gemini (the metered
 * generateContent client). Passing `client` keeps the original Anthropic
 * behaviour. Whatever the provider, every response goes through the same
 * validation and degradation below. In production the client must be metered
 * (utils/meteredAnthropic.forAgent or utils/meteredGemini) so every paid call
 * is reserved/settled in the API spend ledger.
 */

const { performance } = require('node:perf_hooks');
const { validateTurnFrame } = require('../TurnFrame');
const { produceLexicalFrame } = require('./LexicalFrameProducer');
const { SYSTEM_PROMPT, MAX_TOKENS, buildUserContent } = require('./interpretationPrompt');
const { createAnthropicProvider, buildRequestParams } = require('../providers/AnthropicProvider');
const { isProvider } = require('../providers');

// The model is explicit configuration (interpreterModel.js,
// MAX_INTERPRETATION_MODEL), never an implicit default here.
const DEFAULT_MODEL = null;
const DEFAULT_TIMEOUT_MS = 1500;

/** Degradation reason for a non-complete finish (stable across providers). */
function stopDegradation(finish, rawStopReason) {
  if (finish === 'truncated') return 'stop_reason_max_tokens';
  if (finish === 'refused') return 'stop_reason_refusal';
  return `stop_reason_${String(rawStopReason || 'missing').toLowerCase()}`;
}

function withTimeout(promise, ms, controller) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => {
      controller.abort();
      reject(Object.assign(new Error('interpreter_timeout'), { code: 'timeout' }));
    }, ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

class ModelFrameProducer {
  /**
   * @param {{ client?: { messages: { create: Function } }, provider?: object, model: string, timeoutMs?: number }} options
   */
  constructor({ client, provider, model = DEFAULT_MODEL, timeoutMs = DEFAULT_TIMEOUT_MS } = {}) {
    if (provider !== undefined) {
      if (!isProvider(provider)) throw new Error('ModelFrameProducer requires a valid interpretation provider');
    } else if (!client || !client.messages || typeof client.messages.create !== 'function') {
      throw new Error('ModelFrameProducer requires an injected Anthropic-compatible client');
    }
    if (typeof model !== 'string' || !model.trim()) {
      throw new Error('ModelFrameProducer requires an explicit model (MAX_INTERPRETATION_MODEL)');
    }
    this._provider = provider || createAnthropicProvider({ client });
    this.provider = this._provider.name;
    this.model = model;
    this.timeoutMs = timeoutMs;
  }

  /**
   * @returns {Promise<{ frame: object, telemetry: object }>}
   */
  async produce({ text, context, transcript = [] }) {
    const started = performance.now();
    const controller = new AbortController();
    const telemetry = { provider: this.provider, model: this.model, servedModel: null, latencyMs: 0, usage: null, degradedReason: null, stopReason: null };
    try {
      const response = await withTimeout(
        this._provider.complete({ model: this.model, text, context, transcript, timeoutMs: this.timeoutMs, signal: controller.signal }),
        this.timeoutMs,
        controller
      );
      telemetry.latencyMs = Math.round(performance.now() - started);
      telemetry.stopReason = response && response.rawStopReason ? response.rawStopReason : null;
      telemetry.servedModel = response && typeof response.servedModel === 'string' ? response.servedModel : null;
      telemetry.usage = response && response.usage ? response.usage : null;
      if (!response || response.finish !== 'complete') {
        return this._degrade({ text, context }, telemetry, stopDegradation(response && response.finish, telemetry.stopReason));
      }
      if (typeof response.text !== 'string') return this._degrade({ text, context }, telemetry, 'no_text_block');
      let raw;
      try {
        raw = JSON.parse(response.text);
      } catch (_) {
        return this._degrade({ text, context }, telemetry, 'invalid_json');
      }
      const frame = validateTurnFrame(raw, { ...context, producer: 'model' });
      return { frame, telemetry };
    } catch (error) {
      telemetry.latencyMs = Math.round(performance.now() - started);
      const reason = error && error.code === 'invalid_turn_frame' ? 'invalid_frame'
        : error && error.code === 'timeout' ? 'timeout'
          : Number.isInteger(error && error.status) ? `http_${error.status}` : 'provider_error';
      return this._degrade({ text, context }, telemetry, reason);
    }
  }

  _degrade({ text, context }, telemetry, reason) {
    telemetry.degradedReason = reason;
    const lexical = produceLexicalFrame({ text, context });
    // Re-validate the lexical interpretation under the degraded producer label
    // so the frame can never be an execution candidate.
    const { version, producer, tenantId, clarificationRequired, executionCandidate, authority, repairs, ...raw } = lexical;
    const frame = validateTurnFrame(
      { ...raw, referents: raw.referents.map((r) => ({ ...r })), ambiguities: raw.ambiguities.map((a) => ({ ...a, candidateIds: [...a.candidateIds] })), requestedAction: { ...raw.requestedAction }, evidenceNeeds: [...raw.evidenceNeeds], facets: [...raw.facets] },
      { ...context, producer: 'model_degraded_to_lexical' }
    );
    return { frame, telemetry };
  }
}

module.exports = { ModelFrameProducer, SYSTEM_PROMPT, DEFAULT_MODEL, DEFAULT_TIMEOUT_MS, MAX_TOKENS, buildUserContent, buildRequestParams, stopDegradation };
