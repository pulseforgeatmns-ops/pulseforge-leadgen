'use strict';

/**
 * PresentationEngine — ADR-005.
 * Claude (or deterministic fallback) translates a StructuredResponseObject
 * into natural language. Never queries, scores, ranks, or invents.
 */

const DEFAULT_MODEL = process.env.MAX_WORKSPACE_MODEL || process.env.MAX_CHAT_MODEL || 'claude-sonnet-4-6';
const { PRESENTATION_IDENTITY } = require('../identity/MaxIdentity');

const PRESENTATION_TIMEOUT_MS = 30000;

/** Operator-actionable guidance per sanitized model error code. */
const MODEL_ERROR_ACTIONS = Object.freeze({
  provider_auth: 'Check ANTHROPIC_API_KEY for this environment.',
  rate_limited: 'Provider rate limit reached; retry shortly.',
  provider_overloaded: 'Provider overloaded or unavailable; retry shortly.',
  timeout: 'Model request timed out; retry.',
  provider_unreachable: 'Could not reach the model provider; check network egress.',
  spend_blocked: 'Blocked by the API spend policy; review API usage limits.',
  invalid_request: 'Model request rejected; check MAX_WORKSPACE_MODEL.',
  provider_error: 'Model request failed; check server logs.',
});

/**
 * Map a provider/SDK/spend-guard error to a stable, non-sensitive code.
 * Never returns provider message text.
 * @param {unknown} err
 * @returns {string}
 */
function classifyModelError(err) {
  const e = err || {};
  const status = Number.isInteger(e.status) ? e.status : null;
  const name = String(e.name || '');
  const code = String(e.code || '');
  if (name === 'ApiSpendError' || /^api_/.test(code)) return 'spend_blocked';
  if (status === 401 || status === 403 || name === 'AuthenticationError' || name === 'PermissionDeniedError') {
    return 'provider_auth';
  }
  if (status === 429 || name === 'RateLimitError') return 'rate_limited';
  if (status === 529 || status === 503 || status === 502 || name === 'InternalServerError') {
    return 'provider_overloaded';
  }
  if (name === 'APIConnectionTimeoutError' || /timed?\s*out|ETIMEDOUT/i.test(String(e.message || ''))) {
    return 'timeout';
  }
  if (name === 'APIConnectionError' || /ECONNREFUSED|ENOTFOUND|ECONNRESET/.test(code)) {
    return 'provider_unreachable';
  }
  if (status === 400 || status === 404 || name === 'BadRequestError' || name === 'NotFoundError') {
    return 'invalid_request';
  }
  if (status != null && status >= 500) return 'provider_overloaded';
  return 'provider_error';
}

const MISSION_PRESENTATION_SYSTEM = `${PRESENTATION_IDENTITY}
You receive a verified Structured Response Object with mission-oriented communication (SPEC-121).

Rules:
- Lead with mission state: what is happening, current stage, and what the operator should do next.
- Do not expose internal reasoning labels (Known, Inference, Unknown, Evidence Needed) unless metadata.showReasoningExpanded is true.
- Do not invent entities, scores, evidence, confidence, or policy outcomes.
- Preserve the mission structure: Mission → Status → Stage → Progress → Next Step → Operator Decision.
- If metadata.showReasoningDisclosure is true, end with "▼ Show reasoning" — do not expand reasoning inline.
- Keep a calm, operational tone. No fluff.`;

const DEFAULT_PRESENTATION_SYSTEM = `${PRESENTATION_IDENTITY}
You receive a verified Structured Response Object.
Rules:
- Communicate only what is in the object.
- Do not invent entities, scores, evidence, confidence, or policy outcomes.
- Do not calculate, rank, or infer new business intelligence.
- If metadata.unavailable lists gaps, say so plainly.
- Preserve meaning exactly.
- Structure the reply as: direct answer, then brief reasoning, then invite next investigation.
- Keep a calm, operational tone. No fluff.`;

class PresentationEngine {
  /**
   * @param {object} [options]
   * @param {object} [options.anthropic] - Anthropic client instance
   * @param {string} [options.model]
   * @param {boolean} [options.disableLlm=false]
   */
  constructor(options = {}) {
    this._anthropic = options.anthropic || null;
    this._model = options.model || DEFAULT_MODEL;
    this._disableLlm = options.disableLlm === true;
  }

  /**
   * @param {object} structured - StructuredResponseObject
   * @returns {Promise<{ prose: string, structured: object, metadata: object, presentation: string }>}
   */
  async present(structured) {
    if (!structured || typeof structured !== 'object') {
      throw new Error('PresentationEngine requires StructuredResponseObject');
    }

    const metadata = structured.metadata || {};
    const conversationContract = metadata.conversationContract || null;
    const executionBlocked =
      conversationContract && conversationContract.executionAllowed === false;

    // SPEC-155 — read-only conversation contract blocks mission presentation.
    if (executionBlocked && metadata.missionCommunication === true) {
      const readOnlyStructured = {
        ...structured,
        metadata: {
          ...metadata,
          missionCommunication: false,
          readOnlyConversation: true,
          conversationContract,
        },
      };
      const prose = formatDeterministicProse(readOnlyStructured);
      return {
        prose,
        structured: readOnlyStructured,
        metadata: {
          ...readOnlyStructured.metadata,
          presentation: 'conversation_contract_read_only',
        },
        presentation: 'conversation_contract_read_only',
      };
    }

    let prose;
    let presentation = 'fallback';

    // SPEC-121 — mission communication is already operator-formatted.
    if (metadata.missionCommunication === true) {
      prose = formatDeterministicProse(structured);
      return {
        prose,
        structured,
        metadata: {
          ...metadata,
          presentation: 'mission_communication',
        },
        presentation: 'mission_communication',
      };
    }

    // SPEC-149A — identity responses are canonical organizational prose; do not rephrase.
    if (metadata.identityConversation === true) {
      prose = structured.answer ? String(structured.answer).trim() : '';
      return {
        prose,
        structured,
        metadata: {
          ...metadata,
          presentation: 'identity_conversation',
        },
        presentation: 'identity_conversation',
      };
    }

    // SPEC-150 — session inspection is a stored-state snapshot; do not rephrase.
    if (metadata.sessionInspection === true) {
      prose = structured.answer ? String(structured.answer).trim() : '';
      return {
        prose,
        structured,
        metadata: {
          ...metadata,
          presentation: 'session_inspection',
        },
        presentation: 'session_inspection',
      };
    }

    // SPEC-149 — session configuration acknowledgement is already the operator artifact.
    if (metadata.sessionConfiguration === true) {
      prose = structured.answer ? String(structured.answer).trim() : '';
      return {
        prose,
        structured,
        metadata: {
          ...metadata,
          presentation: 'session_configuration',
        },
        presentation: 'session_configuration',
      };
    }

    // SPEC-147 — conversational intelligence responses are already natural-language prose.
    if (metadata.conversationalIntelligence === true) {
      prose = formatDeterministicProse(structured);
      return {
        prose,
        structured,
        metadata: {
          ...metadata,
          presentation: 'conversational_intelligence',
        },
        presentation: 'conversational_intelligence',
      };
    }

    // Strict output-shape / packet-review artifact mode: answer is already the
    // operator artifact — do not append Reasoning / Unavailable / Next, and do
    // not rephrase via Claude.
    if (metadata.strictOutputShape === true) {
      prose = formatDeterministicProse(structured);
      return {
        prose,
        structured,
        metadata: {
          ...metadata,
          presentation: 'strict_output_shape',
        },
        presentation: 'strict_output_shape',
      };
    }

    // Packet-review / call-script-review artifacts default to answer-only
    // unless debug explicitly disabled strict shaping (strictOutputShape === false).
    if (
      (metadata.packetReview === true || metadata.callScriptReview === true) &&
      metadata.strictOutputShape !== false
    ) {
      const answerOnly = {
        ...structured,
        reasoning: [],
        nextInvestigations: [],
        metadata: {
          ...metadata,
          unavailable: [],
          strictOutputShape: true,
        },
      };
      prose = formatDeterministicProse(answerOnly);
      return {
        prose,
        structured: answerOnly,
        metadata: {
          ...answerOnly.metadata,
          presentation: 'strict_output_shape',
        },
        presentation: 'strict_output_shape',
      };
    }

    // P0 response integrity: the answer is always the verified structured
    // object; the model only phrases it. Record whether a model completion
    // actually happened so a deterministic fallback is never mistaken for one,
    // and never surface raw provider error text to the client.
    let modelCompletion;
    if (!this._disableLlm && this._anthropic && process.env.ANTHROPIC_API_KEY) {
      try {
        prose = await this._presentWithClaude(structured);
        presentation = 'claude';
        modelCompletion = { attempted: true, ok: true };
      } catch (err) {
        const errorCode = classifyModelError(err);
        prose = formatDeterministicProse(structured);
        presentation = 'fallback_error';
        modelCompletion = {
          attempted: true,
          ok: false,
          errorCode,
          action: MODEL_ERROR_ACTIONS[errorCode] || MODEL_ERROR_ACTIONS.provider_error,
        };
        console.warn('[max-presentation] model completion failed; deterministic prose used', {
          errorCode,
          httpStatus: err && Number.isInteger(err.status) ? err.status : null,
        });
      }
    } else {
      prose = formatDeterministicProse(structured);
      modelCompletion = {
        attempted: false,
        ok: false,
        errorCode: this._disableLlm ? 'model_disabled' : 'model_not_configured',
      };
    }

    const outMetadata = { ...metadata, presentation, modelCompletion };
    if (modelCompletion.attempted && !modelCompletion.ok) {
      outMetadata.presentationError = modelCompletion.errorCode;
    } else {
      delete outMetadata.presentationError;
    }

    return {
      prose,
      structured,
      metadata: outMetadata,
      presentation,
    };
  }

  async _presentWithClaude(structured) {
    const metadata = structured.metadata || {};
    const system =
      metadata.missionCommunication === true
        ? MISSION_PRESENTATION_SYSTEM
        : DEFAULT_PRESENTATION_SYSTEM;
    const message = await this._anthropic.messages.create(
      {
        model: this._model,
        max_tokens: 900,
        system,
        messages: [
          {
            role: 'user',
            content: `Translate this verified Structured Response Object into natural language for an operator. Preserve every fact; do not add any.\n\n${JSON.stringify(structured, null, 2)}`,
          },
        ],
      },
      // Bound the operator's wait; the SDK default (10 min) outlives the HTTP request.
      { timeout: PRESENTATION_TIMEOUT_MS }
    );

    const prose = (Array.isArray(message && message.content) ? message.content : [])
      .filter((part) => part.type === 'text')
      .map((part) => part.text)
      .join('\n')
      .trim();

    if (!prose) {
      return formatDeterministicProse(structured);
    }
    return prose;
  }
}

/**
 * Deterministic formatter — used when Claude is unavailable.
 * @param {object} structured
 */
function formatDeterministicProse(structured) {
  const metadata = (structured && structured.metadata) || {};
  const strictShape =
    metadata.strictOutputShape === true || metadata.missionCommunication === true;

  if (strictShape) {
    return structured.answer ? String(structured.answer).trim() : '';
  }

  const lines = [];
  if (structured.answer) {
    lines.push(String(structured.answer));
  }
  if (Array.isArray(structured.reasoning) && structured.reasoning.length) {
    lines.push('');
    lines.push('Reasoning:');
    for (const bullet of structured.reasoning) {
      lines.push(`• ${bullet}`);
    }
  }
  const unavailable = Array.isArray(metadata.unavailable)
    ? metadata.unavailable
    : [];
  if (unavailable.length) {
    lines.push('');
    lines.push(
      `Unavailable in current context: ${unavailable.join(', ')}.`
    );
  }
  if (
    Array.isArray(structured.nextInvestigations) &&
    structured.nextInvestigations.length
  ) {
    lines.push('');
    lines.push(
      `Next: ${structured.nextInvestigations[0]}`
    );
  }
  return lines.join('\n').trim();
}

module.exports = {
  PresentationEngine,
  formatDeterministicProse,
  classifyModelError,
  MODEL_ERROR_ACTIONS,
  PRESENTATION_TIMEOUT_MS,
};
