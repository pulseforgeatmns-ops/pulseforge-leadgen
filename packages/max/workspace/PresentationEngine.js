'use strict';

/**
 * PresentationEngine — ADR-005.
 * Gemini (or deterministic fallback) translates a StructuredResponseObject
 * into natural language. Never queries, scores, ranks, or invents.
 */

const DEFAULT_MODEL = process.env.MAX_PRESENTATION_MODEL || 'gemini-2.5-flash-lite';
const { PRESENTATION_IDENTITY } = require('../identity/MaxIdentity');
const { logPresentationFailure } = require('./PresentationDiagnostics');

const PRESENTATION_TIMEOUT_MS = 30000;

/** Operator-actionable guidance per sanitized model error code. */
const MODEL_ERROR_ACTIONS = Object.freeze({
  provider_auth: 'Check GEMINI_API_KEY and Gemini API access for this environment.',
  rate_limited: 'Provider rate limit reached; retry shortly.',
  provider_overloaded: 'Provider overloaded or unavailable; retry shortly.',
  timeout: 'Model request timed out; retry.',
  provider_unreachable: 'Could not reach the model provider; check network egress.',
  spend_blocked: 'Blocked by the API spend policy; review API usage limits.',
  billing_unavailable: 'Model provider billing is unavailable. Check the Google AI account billing for this key, then retry.',
  invalid_request: 'Model request rejected; check MAX_PRESENTATION_MODEL.',
  provider_error: 'Model request failed; check server logs.',
});

const BILLING_REFUSAL_RE = /\bcredit balance\b|\binsufficient (?:credit|credits|funds|balance)\b|\bplans?\s*&\s*billing\b|\bbilling\b.{0,40}\b(?:required|disabled|unavailable|issue)\b/i;

/** Inspect provider error text for a billing refusal. The text itself never leaves this function. */
function isBillingRefusal(e) {
  if (e.status === 402) return true;
  const body = e.error && typeof e.error === 'object' ? e.error : null;
  const inner = body && body.error && typeof body.error === 'object' ? body.error : body;
  const texts = [inner && inner.message, e.message].filter((t) => typeof t === 'string');
  return texts.some((t) => BILLING_REFUSAL_RE.test(t));
}

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
  // Billing refusals may arrive as HTTP 400 or 402. Provider text is never returned or logged.
  if ((status === 400 || status === 402 || name === 'BadRequestError') && isBillingRefusal(e)) {
    return 'billing_unavailable';
  }
  if (status === 429 || name === 'RateLimitError') return 'rate_limited';
  if (status === 529 || status === 503 || status === 502 || name === 'InternalServerError') {
    return 'provider_overloaded';
  }
  if (name === 'APIConnectionTimeoutError' || name === 'AbortError' || /timed?\s*out|ETIMEDOUT/i.test(String(e.message || ''))) {
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
   * @param {object} [options.gemini] - Gemini client instance
   * @param {string} [options.model]
   * @param {boolean} [options.disableLlm=false]
   */
  constructor(options = {}) {
    this._gemini = options.gemini || null;
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
    // Record citations and calculated positions must survive presentation exactly.
    if (metadata.recordIntelligence === true) {
      return { prose: structured.answer, structured, metadata: { ...metadata,
        presentation: 'prospect_records' }, presentation: 'prospect_records' };
    }
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

    // P0 #1087 — operational inspection is a persisted-state report; never rephrase
    // (a model rewrite could invent progress or drop unavailable-source disclosures).
    // P0 #1089 — operational analysis is equally grounded; it is not rephrased either.
    if (metadata.operationalInspection === true || metadata.operationalAnalysis === true) {
      const kind = metadata.operationalAnalysis === true ? 'operational_analysis' : 'operational_inspection';
      prose = structured.answer ? String(structured.answer).trim() : '';
      return {
        prose,
        structured,
        metadata: {
          ...metadata,
          presentation: kind,
        },
        presentation: kind,
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
    // not rephrase via a model writer.
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
    if (!this._disableLlm && this._gemini && process.env.GEMINI_API_KEY) {
      try {
        prose = await this._presentWithGemini(structured);
        presentation = 'gemini';
        modelCompletion = { attempted: true, ok: true };
      } catch (err) {
        let errorCode = 'provider_error';
        try { errorCode = classifyModelError(err); } catch (_) { /* malformed SDK error */ }
        prose = formatDeterministicProse(structured);
        presentation = 'fallback_error';
        modelCompletion = {
          attempted: true,
          ok: false,
          errorCode,
          action: MODEL_ERROR_ACTIONS[errorCode] || MODEL_ERROR_ACTIONS.provider_error,
        };
        logPresentationFailure(err, errorCode);
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

  async _presentWithGemini(structured) {
    const metadata = structured.metadata || {};
    const system =
      metadata.missionCommunication === true
        ? MISSION_PRESENTATION_SYSTEM
        : DEFAULT_PRESENTATION_SYSTEM;
    const response = await this._gemini.generateContent({
      model: this._model,
      body: {
        systemInstruction: { parts: [{ text: system }] },
        contents: [{ role: 'user', parts: [{
          text: `Translate this verified Structured Response Object into natural language for an operator. Preserve every fact; do not add any.\n\n${JSON.stringify(structured, null, 2)}`,
        }] }],
        generationConfig: { maxOutputTokens: 900, thinkingConfig: { thinkingBudget: 0 } },
      },
    }, { timeout: PRESENTATION_TIMEOUT_MS });

    const parts = response && response.candidates && response.candidates[0]
      && response.candidates[0].content && response.candidates[0].content.parts;
    const prose = (Array.isArray(parts) ? parts : [])
      .filter((part) => typeof part.text === 'string')
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
 * Deterministic formatter — used when Gemini is unavailable.
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
