'use strict';

const { randomUUID } = require('crypto');
const {
  OUTCOMES,
  DIRECTIONS,
  HYPOTHESES,
  CONFIDENCE_LEVELS,
  CHART_SNAPSHOT,
} = require('./types');

/**
 * TradeBuilder — assembles Trade + Evidence graph from answers + extraction.
 *
 * Observation graph:
 *   Screenshot → Trade → Evidence → Claim → Outcome
 */
class TradeBuilder {
  /**
   * @param {object} [deps]
   * @param {() => string} [deps.idFactory]
   * @param {() => string} [deps.now]
   */
  constructor(deps = {}) {
    this._idFactory =
      typeof deps.idFactory === 'function' ? deps.idFactory : () => randomUUID();
    this._now =
      typeof deps.now === 'function'
        ? deps.now
        : () => new Date().toISOString();
  }

  /**
   * @param {object} input
   * @param {object} input.screenshot
   * @param {object} input.observation - chart_snapshot observation
   * @param {object} input.answers - { result, direction, hypothesis, confidence }
   * @param {object} [input.extraction]
   * @param {string} [input.entryTime]
   * @param {string} [input.subjectId]
   * @returns {{ trade: object, evidence: object[], claim: object, outcome: object, graph: object }}
   */
  build(input = {}) {
    if (!input.screenshot || !input.screenshot.id) {
      throw new Error('TradeBuilder requires screenshot');
    }
    if (!input.observation) {
      throw new Error('TradeBuilder requires chart_snapshot observation');
    }

    const answers = normalizeAnswers(input.answers || {});
    const extraction = input.extraction || {};
    const entryTime = String(
      input.entryTime ||
        extraction.timestamp ||
        input.observation.observedAt ||
        this._now()
    );
    const tradeId = String(input.id || this._idFactory());
    const subjectId =
      input.subjectId ||
      extraction.symbol ||
      input.observation.subjectId ||
      null;

    const trade = Object.freeze({
      id: tradeId,
      entryTime,
      direction: answers.direction,
      hypothesis: answers.hypothesis,
      confidence: answers.confidence,
      result: answers.result,
      screenshotId: input.screenshot.id,
      observationId: input.observation.id,
      subjectId,
      symbol: extraction.symbol != null ? extraction.symbol : null,
      exchange: extraction.exchange != null ? extraction.exchange : null,
      timeframe: extraction.timeframe != null ? extraction.timeframe : null,
      currentPrice:
        extraction.currentPrice != null ? extraction.currentPrice : null,
      indicatorsVisible:
        extraction.indicatorsVisible != null
          ? extraction.indicatorsVisible
          : null,
      atr: extraction.atr != null ? extraction.atr : null,
      vwap: extraction.vwap != null ? extraction.vwap : null,
      volume: extraction.volume != null ? extraction.volume : null,
      chartImageHash:
        extraction.chartImageHash ||
        input.screenshot.imageHash ||
        null,
      screenshotDimensions:
        extraction.screenshotDimensions ||
        (input.screenshot.width != null || input.screenshot.height != null
          ? Object.freeze({
              width: input.screenshot.width,
              height: input.screenshot.height,
            })
          : null),
      capturedAt: this._now(),
      extractionStatus: input.extractionStatus || 'pending',
    });

    const claimId = `claim:trade:${tradeId}`;
    const claim = Object.freeze({
      id: claimId,
      claimType: hypothesisToClaimType(answers.hypothesis),
      type: hypothesisToClaimType(answers.hypothesis),
      subject: subjectId,
      subjectId,
      statement: `${answers.direction} ${answers.hypothesis}`,
      confidence: confidenceToScore(answers.confidence),
      tradeId,
      hypothesis: answers.hypothesis,
      direction: answers.direction,
    });

    const outcomeId = `outcome:trade:${tradeId}`;
    const outcome = Object.freeze({
      id: outcomeId,
      claimId,
      tradeId,
      result: answers.result,
      verdict: answers.result === OUTCOMES.WIN ? 'correct' : 'incorrect',
      subject: subjectId,
      subjectId,
      observedAt: entryTime,
    });

    const screenshotEvidence = Object.freeze({
      id: `ev:screenshot:${input.screenshot.id}`,
      type: 'screenshot_evidence',
      role: 'supporting',
      observationId: input.observation.id,
      observationType: CHART_SNAPSHOT,
      screenshotId: input.screenshot.id,
      tradeId,
      claimId,
      imageHash: input.screenshot.imageHash,
      immutable: true,
    });

    const tradeEvidence = Object.freeze({
      id: `ev:trade:${tradeId}`,
      type: 'trade_evidence',
      role: 'supporting',
      tradeId,
      claimId,
      observationId: input.observation.id,
      screenshotId: input.screenshot.id,
      hypothesis: answers.hypothesis,
      direction: answers.direction,
      result: answers.result,
      confidence: answers.confidence,
    });

    const evidence = Object.freeze([screenshotEvidence, tradeEvidence]);

    const graph = Object.freeze({
      screenshot: Object.freeze({
        id: input.screenshot.id,
        observationId: input.observation.id,
      }),
      trade: Object.freeze({ id: tradeId }),
      evidence: Object.freeze(evidence.map((e) => e.id)),
      claim: Object.freeze({ id: claimId }),
      outcome: Object.freeze({ id: outcomeId }),
      edges: Object.freeze([
        Object.freeze({
          from: input.screenshot.id,
          to: tradeId,
          type: 'SCREENSHOT_OF',
        }),
        Object.freeze({
          from: tradeId,
          to: screenshotEvidence.id,
          type: 'PRODUCES_EVIDENCE',
        }),
        Object.freeze({
          from: tradeId,
          to: tradeEvidence.id,
          type: 'PRODUCES_EVIDENCE',
        }),
        Object.freeze({
          from: tradeEvidence.id,
          to: claimId,
          type: 'SUPPORTS',
        }),
        Object.freeze({
          from: claimId,
          to: outcomeId,
          type: 'RESOLVED_BY',
        }),
      ]),
    });

    return Object.freeze({
      trade,
      evidence,
      claim,
      outcome,
      graph,
    });
  }
}

/**
 * @param {object} answers
 */
function normalizeAnswers(answers) {
  const result = normalizeChoice(answers.result || answers.outcome, OUTCOMES);
  const direction = normalizeChoice(answers.direction, DIRECTIONS);
  const hypothesis = normalizeHypothesis(answers.hypothesis);
  const confidence = normalizeConfidence(answers.confidence);

  if (!result) {
    throw new Error('Trade requires Outcome (Win / Loss)');
  }
  if (!direction) {
    throw new Error('Trade requires Direction (Long / Short)');
  }
  if (!hypothesis) {
    throw new Error('Trade requires Hypothesis');
  }
  if (confidence == null) {
    throw new Error('Trade requires Confidence (1–5)');
  }

  return Object.freeze({ result, direction, hypothesis, confidence });
}

/**
 * @param {unknown} value
 * @param {Record<string, string>} enumMap
 */
function normalizeChoice(value, enumMap) {
  if (value == null) return null;
  const raw = String(value).trim();
  for (const option of Object.values(enumMap)) {
    if (option.toLowerCase() === raw.toLowerCase()) return option;
  }
  return null;
}

/**
 * @param {unknown} value
 */
function normalizeHypothesis(value) {
  if (value == null) return null;
  const raw = String(value).trim().toLowerCase().replace(/[_-]+/g, ' ');
  for (const option of Object.values(HYPOTHESES)) {
    if (option.toLowerCase() === raw) return option;
  }
  if (raw === 'meanreversion') return HYPOTHESES.MEAN_REVERSION;
  return null;
}

/**
 * @param {unknown} value
 * @returns {number|null}
 */
function normalizeConfidence(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  const rounded = Math.round(n);
  return CONFIDENCE_LEVELS.includes(rounded) ? rounded : null;
}

/**
 * @param {string} hypothesis
 */
function hypothesisToClaimType(hypothesis) {
  return String(hypothesis)
    .toLowerCase()
    .replace(/\s+/g, '_');
}

/**
 * Map 1–5 chip to 0–1 confidence score for claims.
 * @param {number} level
 */
function confidenceToScore(level) {
  return Math.round((Number(level) / 5) * 1000) / 1000;
}

/**
 * @param {object} [deps]
 * @returns {TradeBuilder}
 */
function createTradeBuilder(deps) {
  return new TradeBuilder(deps);
}

module.exports = {
  TradeBuilder,
  createTradeBuilder,
  normalizeAnswers,
  normalizeConfidence,
  hypothesisToClaimType,
  confidenceToScore,
};
