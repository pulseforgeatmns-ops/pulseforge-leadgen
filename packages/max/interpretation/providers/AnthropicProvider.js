'use strict';

/**
 * Anthropic transport for the TurnFrame interpreter (provider port).
 *
 * Byte-for-byte the request the ModelFrameProducer sent before the port
 * existed (buildRequestParams), so existing evaluation history and the shadow
 * observer stay comparable. The client is injected (`messages.create` shape of
 * @anthropic-ai/sdk); metering is the caller's responsibility.
 */

const { TURN_FRAME_OUTPUT_SCHEMA } = require('../TurnFrame');
const { SYSTEM_PROMPT, MAX_TOKENS, buildUserContent } = require('../producers/interpretationPrompt');

const NAME = 'anthropic';

/**
 * Opt-in thinking settings. null (default) sends no `thinking` field: the
 * historical request, byte for byte. 'disabled' adds `thinking: {type:
 * "disabled"}` for models whose thinking is on by default (e.g. Claude Haiku
 * 5.5, which accepts it at effort `high` or below; this request uses `low`), so
 * thinking tokens neither consume MAX_TOKENS nor add latency.
 */
const THINKING_SETTINGS = Object.freeze([null, 'disabled']);

function checkThinking(thinking) {
  if (!THINKING_SETTINGS.includes(thinking)) throw new Error(`Anthropic provider thinking must be one of: ${THINKING_SETTINGS.map(String).join(', ')}`);
  return thinking;
}

/** The exact request body sent to the Messages API. */
function buildRequestParams({ model, text, context, transcript = [], thinking = null }) {
  const params = {
    model,
    max_tokens: MAX_TOKENS,
    system: [{ type: 'text', text: SYSTEM_PROMPT, cache_control: { type: 'ephemeral' } }],
    messages: [{ role: 'user', content: buildUserContent({ text, context, transcript }) }],
    output_config: { effort: 'low', format: { type: 'json_schema', schema: TURN_FRAME_OUTPUT_SCHEMA } },
  };
  if (checkThinking(thinking) === 'disabled') params.thinking = { type: 'disabled' };
  return params;
}

function finishFor(stopReason) {
  if (stopReason === 'end_turn') return 'complete';
  if (stopReason === 'max_tokens') return 'truncated';
  if (stopReason === 'refusal') return 'refused';
  return 'other';
}

function createAnthropicProvider({ client, thinking = null } = {}) {
  if (!client || !client.messages || typeof client.messages.create !== 'function') {
    throw new Error('Anthropic provider requires an injected Anthropic-compatible client');
  }
  checkThinking(thinking);
  return {
    name: NAME,
    thinking,
    buildRequest: (input) => buildRequestParams({ ...input, thinking }),
    /**
     * @returns {Promise<{ finish: string, rawStopReason: string|null, text: string|null, servedModel: string|null, usage: object|null }>}
     */
    async complete({ model, text, context, transcript = [], timeoutMs, signal }) {
      const response = await client.messages.create(
        buildRequestParams({ model, text, context, transcript, thinking }),
        { signal, timeout: timeoutMs, maxRetries: 0 }
      );
      const rawStopReason = response && response.stop_reason ? response.stop_reason : null;
      const block = ((response && response.content) || []).find((b) => b && b.type === 'text');
      return {
        finish: finishFor(rawStopReason),
        rawStopReason,
        text: block ? block.text : null,
        // The model the provider reports having served (may differ from the request).
        servedModel: response && typeof response.model === 'string' ? response.model : null,
        usage: response && response.usage ? {
          input_tokens: response.usage.input_tokens ?? null,
          output_tokens: response.usage.output_tokens ?? null,
          cache_read_input_tokens: response.usage.cache_read_input_tokens ?? null,
          cache_creation_input_tokens: response.usage.cache_creation_input_tokens ?? null,
        } : null,
      };
    },
  };
}

module.exports = { NAME, THINKING_SETTINGS, createAnthropicProvider, buildRequestParams };
