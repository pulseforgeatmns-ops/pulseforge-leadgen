'use strict';

/**
 * Google Gemini transport for the TurnFrame interpreter (provider port).
 *
 * Wraps the existing metered Gemini client (`utils/meteredGemini.createGeminiClient`
 * shape: `generateContent({ model, body }, { timeout, apiUsageContext })`), which
 * reserves and settles every call through injectable reserve/settle. The
 * evaluation injects its in-process cap; the shadow observer would use the
 * shared ledger. This module never reads credentials itself.
 *
 * Same instruction text and data payload as every other provider
 * (interpretationPrompt). Structured output is requested as JSON constrained
 * by TURN_FRAME_OUTPUT_SCHEMA; whatever comes back is still validated by
 * validateTurnFrame, so a provider that ignores or loosens the schema can only
 * produce a degraded (lexical, never-executable) frame.
 *
 * Verification status (2026-11-18): the request field names below follow the
 * Gemini REST API as used elsewhere in this repo (systemInstruction, contents,
 * generationConfig.maxOutputTokens, thinkingConfig.thinkingBudget) plus the JSON
 * structured-output fields. Google's documentation was not reachable from the
 * build environment, so structured-output compatibility and the model's
 * availability are confirmed by the evaluation CLI's live preflight and by the
 * first-call stop rules (a rejected request stops the run after one call).
 */

const { TURN_FRAME_OUTPUT_SCHEMA } = require('../TurnFrame');
const { SYSTEM_PROMPT, MAX_TOKENS, buildUserContent } = require('../producers/interpretationPrompt');

const NAME = 'gemini';

// Finish reasons that mean the provider withheld or blocked the answer.
const REFUSAL_FINISH = new Set(['SAFETY', 'RECITATION', 'BLOCKLIST', 'PROHIBITED_CONTENT', 'SPII', 'IMAGE_SAFETY', 'LANGUAGE']);

/** The exact `{ model, body }` handed to generateContent (also priced by the eval plan). */
function buildRequest({ model, text, context, transcript = [] }) {
  return {
    model,
    body: {
      systemInstruction: { parts: [{ text: SYSTEM_PROMPT }] },
      contents: [{ role: 'user', parts: [{ text: buildUserContent({ text, context, transcript }) }] }],
      generationConfig: {
        maxOutputTokens: MAX_TOKENS,
        temperature: 0,
        responseMimeType: 'application/json',
        responseJsonSchema: TURN_FRAME_OUTPUT_SCHEMA,
        thinkingConfig: { thinkingBudget: 0 },
      },
    },
  };
}

function finishFor(body) {
  const blocked = body && body.promptFeedback && body.promptFeedback.blockReason;
  if (blocked) return { finish: 'refused', rawStopReason: `PROMPT_BLOCKED_${blocked}` };
  const candidate = body && Array.isArray(body.candidates) ? body.candidates[0] : null;
  const raw = candidate && candidate.finishReason ? String(candidate.finishReason) : null;
  if (raw === 'STOP') return { finish: 'complete', rawStopReason: raw };
  if (raw === 'MAX_TOKENS') return { finish: 'truncated', rawStopReason: raw };
  if (raw && REFUSAL_FINISH.has(raw)) return { finish: 'refused', rawStopReason: raw };
  return { finish: 'other', rawStopReason: raw };
}

function textOf(body) {
  const candidate = body && Array.isArray(body.candidates) ? body.candidates[0] : null;
  const parts = candidate && candidate.content && Array.isArray(candidate.content.parts) ? candidate.content.parts : [];
  // Thought summaries are never part of the answer.
  const texts = parts.filter((p) => p && !p.thought && typeof p.text === 'string').map((p) => p.text);
  return texts.length ? texts.join('') : null;
}

function usageOf(meta) {
  if (!meta || typeof meta !== 'object') return null;
  const n = (v) => (Number.isSafeInteger(v) && v >= 0 ? v : null);
  return {
    input_tokens: n(meta.promptTokenCount),
    output_tokens: n(meta.candidatesTokenCount),
    thinking_tokens: n(meta.thoughtsTokenCount),
    cache_read_input_tokens: n(meta.cachedContentTokenCount),
    total_tokens: n(meta.totalTokenCount),
  };
}

function createGeminiProvider({ client, tenantId = null } = {}) {
  if (!client || typeof client.generateContent !== 'function') {
    throw new Error('Gemini provider requires an injected metered Gemini client');
  }
  return {
    name: NAME,
    buildRequest,
    async complete({ model, text, context, transcript = [], timeoutMs }) {
      // The client owns its abort timer; give it the producer's budget so a
      // timed-out request is cancelled, not left running past the deadline.
      const body = await client.generateContent(buildRequest({ model, text, context, transcript }), {
        timeout: timeoutMs,
        apiUsageContext: { tenantId: tenantId ?? (context && context.tenantId) ?? null },
      });
      const { finish, rawStopReason } = finishFor(body);
      return {
        finish,
        rawStopReason,
        text: textOf(body),
        servedModel: body && typeof body.modelVersion === 'string' ? body.modelVersion : null,
        usage: usageOf(body && body.usageMetadata),
        timing: body && body.__geminiTiming ? { ...body.__geminiTiming } : null,
      };
    },
  };
}

module.exports = { NAME, createGeminiProvider, buildRequest, finishFor, textOf, usageOf, REFUSAL_FINISH };
