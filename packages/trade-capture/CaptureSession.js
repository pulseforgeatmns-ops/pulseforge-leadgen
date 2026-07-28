'use strict';

const { randomUUID } = require('crypto');
const {
  SESSION_STATUS,
  OPERATOR_QUESTIONS,
  OUTCOMES,
  DIRECTIONS,
  HYPOTHESES,
  CONFIDENCE_LEVELS,
} = require('./types');

/**
 * CaptureSession — one operator capture flow.
 *
 * Step 1: paste/drop screenshot → session created
 * Step 2: four chip answers (no typing)
 * Step 3: done — background work continues outside the session wait path
 */
class CaptureSession {
  /**
   * @param {object} seed
   * @param {object} seed.screenshot
   * @param {object} seed.observation
   * @param {string} [seed.id]
   * @param {string} [seed.createdAt]
   * @param {object} [seed.answers]
   * @param {object} [seed.metadata]
   */
  constructor(seed) {
    if (!seed || !seed.screenshot || !seed.observation) {
      throw new Error('CaptureSession requires screenshot and observation');
    }

    this.id = String(seed.id || randomUUID());
    this.createdAt = String(seed.createdAt || new Date().toISOString());
    this.screenshot = seed.screenshot;
    this.observation = seed.observation;
    this.status = SESSION_STATUS.AWAITING_ANSWERS;
    /** @type {{ result?: string, direction?: string, hypothesis?: string, confidence?: number }} */
    this.answers = { ...(seed.answers || {}) };
    this.metadata = Object.freeze({ ...(seed.metadata || {}) });
    this.capturedAt = null;
    this.tradeId = null;
    this.questions = OPERATOR_QUESTIONS;
  }

  /**
   * Apply one or more chip answers. No typing required.
   * @param {object} patch
   * @returns {CaptureSession}
   */
  answer(patch = {}) {
    this._assertOpen();
    if (patch.result != null || patch.outcome != null) {
      this.answers.result = assertOption(
        patch.result || patch.outcome,
        Object.values(OUTCOMES),
        'Outcome'
      );
    }
    if (patch.direction != null) {
      this.answers.direction = assertOption(
        patch.direction,
        Object.values(DIRECTIONS),
        'Direction'
      );
    }
    if (patch.hypothesis != null) {
      this.answers.hypothesis = assertHypothesis(patch.hypothesis);
    }
    if (patch.confidence != null) {
      this.answers.confidence = assertConfidence(patch.confidence);
    }
    return this;
  }

  /**
   * True when all four operator questions are answered.
   */
  isComplete() {
    return (
      this.answers.result != null &&
      this.answers.direction != null &&
      this.answers.hypothesis != null &&
      this.answers.confidence != null
    );
  }

  /**
   * Missing question ids (for UI hints — never blocks paste).
   * @returns {string[]}
   */
  missingAnswers() {
    const missing = [];
    if (this.answers.result == null) missing.push('result');
    if (this.answers.direction == null) missing.push('direction');
    if (this.answers.hypothesis == null) missing.push('hypothesis');
    if (this.answers.confidence == null) missing.push('confidence');
    return missing;
  }

  /**
   * Mark session captured. Caller persists Trade via TradeBuilder.
   * @param {string} [tradeId]
   */
  markCaptured(tradeId) {
    this._assertOpen();
    if (!this.isComplete()) {
      throw new Error(
        `CaptureSession incomplete; missing: ${this.missingAnswers().join(', ')}`
      );
    }
    this.status = SESSION_STATUS.CAPTURED;
    this.capturedAt = new Date().toISOString();
    this.tradeId = tradeId ? String(tradeId) : null;
    return this;
  }

  /**
   * @param {'processing'|'complete'} status
   */
  markBackground(status) {
    if (status === 'processing') {
      this.status = SESSION_STATUS.PROCESSING;
    } else if (status === 'complete') {
      this.status = SESSION_STATUS.COMPLETE;
    }
    return this;
  }

  /**
   * Immutable snapshot for APIs / labs.
   */
  snapshot() {
    return Object.freeze({
      id: this.id,
      createdAt: this.createdAt,
      status: this.status,
      screenshotId: this.screenshot.id,
      observationId: this.observation.id,
      answers: Object.freeze({ ...this.answers }),
      missingAnswers: Object.freeze(this.missingAnswers()),
      complete: this.isComplete(),
      capturedAt: this.capturedAt,
      tradeId: this.tradeId,
      questions: this.questions,
      metadata: this.metadata,
    });
  }

  _assertOpen() {
    if (
      this.status === SESSION_STATUS.CAPTURED ||
      this.status === SESSION_STATUS.PROCESSING ||
      this.status === SESSION_STATUS.COMPLETE
    ) {
      throw new Error(`CaptureSession ${this.id} is already ${this.status}`);
    }
  }
}

/**
 * @param {unknown} value
 * @param {unknown[]} options
 * @param {string} label
 */
function assertOption(value, options, label) {
  const raw = String(value).trim().toLowerCase();
  const hit = options.find((o) => String(o).toLowerCase() === raw);
  if (!hit) {
    throw new Error(`${label} must be one of: ${options.join(', ')}`);
  }
  return hit;
}

/**
 * @param {unknown} value
 */
function assertHypothesis(value) {
  const raw = String(value).trim().toLowerCase().replace(/[_-]+/g, ' ');
  for (const option of Object.values(HYPOTHESES)) {
    if (option.toLowerCase() === raw) return option;
  }
  if (raw === 'meanreversion') return HYPOTHESES.MEAN_REVERSION;
  throw new Error(
    `Hypothesis must be one of: ${Object.values(HYPOTHESES).join(', ')}`
  );
}

/**
 * @param {unknown} value
 */
function assertConfidence(value) {
  const n = Math.round(Number(value));
  if (!CONFIDENCE_LEVELS.includes(n)) {
    throw new Error('Confidence must be 1, 2, 3, 4, or 5');
  }
  return n;
}

/**
 * @param {object} seed
 * @returns {CaptureSession}
 */
function createCaptureSession(seed) {
  return new CaptureSession(seed);
}

module.exports = {
  CaptureSession,
  createCaptureSession,
};
