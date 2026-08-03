'use strict';

/**
 * PresentationEngine — ADR-005.
 * Claude (or deterministic fallback) translates a StructuredResponseObject
 * into natural language. Never queries, scores, ranks, or invents.
 */

const DEFAULT_MODEL = process.env.MAX_WORKSPACE_MODEL || process.env.MAX_CHAT_MODEL || 'claude-sonnet-4-6';

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
    let prose;
    let presentation = 'fallback';

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

    if (!this._disableLlm && this._anthropic && process.env.ANTHROPIC_API_KEY) {
      try {
        prose = await this._presentWithClaude(structured);
        presentation = 'claude';
      } catch (err) {
        prose = formatDeterministicProse(structured);
        presentation = 'fallback_error';
        metadata.presentationError =
          err && err.message ? String(err.message) : 'llm_failed';
      }
    } else {
      prose = formatDeterministicProse(structured);
    }

    return {
      prose,
      structured,
      metadata: {
        ...metadata,
        presentation,
      },
      presentation,
    };
  }

  async _presentWithClaude(structured) {
    const message = await this._anthropic.messages.create({
      model: this._model,
      max_tokens: 900,
      system: `You are Max, the Pulseforge intelligence advisor.
You are a PRESENTATION ENGINE only (ADR-005).
You receive a verified Structured Response Object.
Rules:
- Communicate only what is in the object.
- Do not invent entities, scores, evidence, confidence, or policy outcomes.
- Do not calculate, rank, or infer new business intelligence.
- If metadata.unavailable lists gaps, say so plainly.
- Preserve meaning exactly.
- Structure the reply as: direct answer, then brief reasoning, then invite next investigation.
- Keep a calm, advisor tone. No fluff.`,
      messages: [
        {
          role: 'user',
          content: `Translate this verified Structured Response Object into natural language for an operator. Preserve every fact; do not add any.\n\n${JSON.stringify(structured, null, 2)}`,
        },
      ],
    });

    const prose = message.content
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
  const strictShape = metadata.strictOutputShape === true;

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
};
