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
 * (interpretationPrompt), with one appended legend line. Structured output is
 * requested in the compact wire format (CompactTurnFrame: short keys, flat
 * action, identical enum values) to cut output tokens and therefore latency.
 * The producer expands it strictly to the canonical shape and then runs the
 * unchanged repairUnsafeAuthorization + validateTurnFrame, so a provider that
 * ignores or loosens the schema can only produce a degraded (lexical,
 * never-executable) frame.
 *
 * Verification status (2026-11-18): the request field names below follow the
 * Gemini REST API as used elsewhere in this repo (systemInstruction, contents,
 * generationConfig.maxOutputTokens, thinkingConfig.thinkingBudget) plus the JSON
 * structured-output fields. Google's documentation was not reachable from the
 * build environment, so structured-output compatibility and the model's
 * availability are confirmed by the evaluation CLI's live preflight and by the
 * first-call stop rules (a rejected request stops the run after one call).
 */

const { SYSTEM_PROMPT, MAX_TOKENS, buildUserContent } = require('../producers/interpretationPrompt');
const { COMPACT_FORMAT, COMPACT_TURN_FRAME_OUTPUT_SCHEMA, COMPACT_OUTPUT_LEGEND } = require('../CompactTurnFrame');

const NAME = 'gemini';

// Finish reasons that mean the provider withheld or blocked the answer.
const REFUSAL_FINISH = new Set(['SAFETY', 'RECITATION', 'BLOCKLIST', 'PROHIBITED_CONTENT', 'SPII', 'IMAGE_SAFETY', 'LANGUAGE']);

/** System instruction: the unchanged provider-neutral prompt plus the compact key legend. */
const SYSTEM_INSTRUCTION = `${SYSTEM_PROMPT}\n\n${COMPACT_OUTPUT_LEGEND}`;

/** The exact `{ model, body }` handed to generateContent (also priced by the eval plan). */
function buildRequest({ model, text, context, transcript = [] }) {
  return {
    model,
    body: {
      systemInstruction: { parts: [{ text: SYSTEM_INSTRUCTION }] },
      contents: [{ role: 'user', parts: [{ text: buildUserContent({ text, context, transcript }) }] }],
      generationConfig: {
        maxOutputTokens: MAX_TOKENS,
        temperature: 0,
        responseMimeType: 'application/json',
        responseJsonSchema: COMPACT_TURN_FRAME_OUTPUT_SCHEMA,
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

/** Shape one Gemini body into the provider-port response. */
function responseOf(body) {
  const { finish, rawStopReason } = finishFor(body);
  return {
    finish,
    rawStopReason,
    text: textOf(body),
    outputFormat: COMPACT_FORMAT,
    servedModel: body && typeof body.modelVersion === 'string' ? body.modelVersion : null,
    usage: usageOf(body && body.usageMetadata),
    timing: body && body.__geminiTiming ? { ...body.__geminiTiming } : null,
  };
}

const MAX_PARALLEL_REQUESTS = 2;

/**
 * Resolve with the first attempt that returned a complete answer with text.
 * If none does: a provider rejection (HTTP 4xx) wins, so configuration errors
 * stay visible to the stop rules; otherwise the first non-complete response
 * (e.g. truncation, refusal) is returned; otherwise the first error is thrown.
 * Every attempt keeps running to its own end and settles its own reservation.
 */
function firstUsable(attempts) {
  return new Promise((resolve, reject) => {
    const results = new Array(attempts.length);
    let pending = attempts.length;
    let done = false;
    attempts.forEach((attempt, index) => {
      attempt
        .then(
          (response) => {
            results[index] = { ok: true, response };
            if (!done && response.finish === 'complete' && typeof response.text === 'string') {
              done = true;
              resolve({ response, winner: index });
            }
          },
          (error) => { results[index] = { ok: false, error }; },
        )
        .finally(() => {
          pending -= 1;
          if (pending > 0 || done) return;
          done = true;
          const rejected = results.find((x) => !x.ok && x.error && Number.isInteger(x.error.status) && x.error.status >= 400 && x.error.status < 500);
          if (rejected) return reject(rejected.error);
          const answered = results.findIndex((x) => x.ok);
          if (answered >= 0) return resolve({ response: results[answered].response, winner: answered });
          return reject(results[0].error);
        });
    });
  });
}

/**
 * @param {{ client: object, tenantId?: string|number|null, parallelRequests?: 1|2 }} options
 *   parallelRequests: identical requests issued at once per turn; the first
 *   complete answer is used (tail-latency mitigation). Default 1 = the
 *   historical single request. Each request is reserved and settled by the
 *   metered client on its own, so spend accounting covers every attempt.
 */
function createGeminiProvider({ client, tenantId = null, parallelRequests = 1 } = {}) {
  if (!client || typeof client.generateContent !== 'function') {
    throw new Error('Gemini provider requires an injected metered Gemini client');
  }
  if (!Number.isInteger(parallelRequests) || parallelRequests < 1 || parallelRequests > MAX_PARALLEL_REQUESTS) {
    throw new Error(`Gemini provider parallelRequests must be an integer from 1 to ${MAX_PARALLEL_REQUESTS}`);
  }
  return {
    name: NAME,
    parallelRequests,
    buildRequest,
    async complete({ model, text, context, transcript = [], timeoutMs }) {
      // The client owns its abort timer; give it the producer's budget so a
      // timed-out request is cancelled, not left running past the deadline.
      const request = buildRequest({ model, text, context, transcript });
      const options = {
        timeout: timeoutMs,
        apiUsageContext: { tenantId: tenantId ?? (context && context.tenantId) ?? null },
      };
      if (parallelRequests === 1) return responseOf(await client.generateContent(request, options));
      const attempts = Array.from({ length: parallelRequests }, () => client.generateContent(request, options).then(responseOf));
      const { response, winner } = await firstUsable(attempts);
      return { ...response, parallel: { issued: parallelRequests, winner } };
    },
  };
}

module.exports = { NAME, OUTPUT_FORMAT: COMPACT_FORMAT, MAX_PARALLEL_REQUESTS, firstUsable, SYSTEM_INSTRUCTION, createGeminiProvider, buildRequest, finishFor, textOf, usageOf, REFUSAL_FINISH };
