'use strict';

const { randomUUID } = require('crypto');
const {
  CAPTURE_RULES,
  OPERATOR_QUESTIONS,
  OUTCOMES,
  CHART_SNAPSHOT,
} = require('./types');
const {
  ScreenshotProcessor,
  createScreenshotProcessor,
} = require('./ScreenshotProcessor');
const {
  CaptureSession,
  createCaptureSession,
} = require('./CaptureSession');
const { TradeBuilder, createTradeBuilder } = require('./TradeBuilder');
const {
  ObservationExtractor,
  createObservationExtractor,
} = require('./ObservationExtractor');

/**
 * CaptureEngine — fastest path from screenshot → Evidence.
 *
 * Operator workflow (<15s):
 *   1. Paste / drop screenshot → session
 *   2. Click Win/Loss · Long/Short · Hypothesis · Confidence
 *   3. Save — OCR / extraction runs in background only
 *
 * @example
 *   const engine = createCaptureEngine({ runExtractorsSync: true });
 *   const session = engine.pasteScreenshot(pngBuffer);
 *   engine.answer(session.id, {
 *     result: 'Win', direction: 'Long', hypothesis: 'Velocity', confidence: 4,
 *   });
 *   const captured = await engine.save(session.id);
 */
class CaptureEngine {
  /**
   * @param {object} [deps]
   * @param {ScreenshotProcessor} [deps.screenshotProcessor]
   * @param {TradeBuilder} [deps.tradeBuilder]
   * @param {ObservationExtractor} [deps.extractor]
   * @param {boolean} [deps.runExtractorsSync]
   * @param {() => string} [deps.idFactory]
   * @param {() => string} [deps.now]
   */
  constructor(deps = {}) {
    this.rules = CAPTURE_RULES;
    this.questions = OPERATOR_QUESTIONS;
    this._idFactory =
      typeof deps.idFactory === 'function' ? deps.idFactory : () => randomUUID();
    this._now =
      typeof deps.now === 'function'
        ? deps.now
        : () => new Date().toISOString();

    this.screenshots = /** @type {Map<string, object>} */ (new Map());
    this.observations = /** @type {Map<string, object>} */ (new Map());
    this.sessions = /** @type {Map<string, CaptureSession>} */ (new Map());
    this.trades = /** @type {Map<string, object>} */ (new Map());
    this.evidence = /** @type {Map<string, object>} */ (new Map());
    this.claims = /** @type {Map<string, object>} */ (new Map());
    this.outcomes = /** @type {Map<string, object>} */ (new Map());
    this.graphs = /** @type {Map<string, object>} */ (new Map());

    this.screenshotProcessor =
      deps.screenshotProcessor ||
      createScreenshotProcessor({
        idFactory: this._idFactory,
        now: this._now,
      });
    this.tradeBuilder =
      deps.tradeBuilder ||
      createTradeBuilder({
        idFactory: this._idFactory,
        now: this._now,
      });
    this.extractor =
      deps.extractor ||
      createObservationExtractor({
        runSync: deps.runExtractorsSync === true,
        extractors: deps.extractors,
      });
  }

  /**
   * Step 1 — paste or drag/drop screenshot. Immediately creates a session.
   *
   * @param {object|Buffer|string} image
   * @param {object} [opts]
   * @returns {CaptureSession}
   */
  pasteScreenshot(image, opts = {}) {
    const { screenshot, observation } = this.screenshotProcessor.process(
      image,
      opts
    );
    this.screenshots.set(screenshot.id, screenshot);
    this.observations.set(observation.id, observation);

    const session = createCaptureSession({
      id: this._idFactory(),
      createdAt: this._now(),
      screenshot,
      observation,
      metadata: {
        source: opts.source || 'paste',
        subjectId: opts.subjectId || null,
      },
    });
    this.sessions.set(session.id, session);
    return session;
  }

  /** Alias for drag/drop UX. */
  dropScreenshot(image, opts = {}) {
    return this.pasteScreenshot(image, { ...opts, source: opts.source || 'drop' });
  }

  /**
   * Step 2 — chip answers (may be called incrementally).
   * @param {string} sessionId
   * @param {object} answers
   * @returns {CaptureSession}
   */
  answer(sessionId, answers) {
    const session = this._requireSession(sessionId);
    session.answer(answers);
    return session;
  }

  /**
   * Step 3 — save. Returns immediately with Trade; extraction is background.
   *
   * @param {string} sessionId
   * @param {object} [opts]
   * @returns {Promise<object>|object}
   */
  save(sessionId, opts = {}) {
    const session = this._requireSession(sessionId);
    if (!session.isComplete()) {
      throw new Error(
        `Cannot save; missing answers: ${session.missingAnswers().join(', ')}`
      );
    }

    const built = this.tradeBuilder.build({
      screenshot: session.screenshot,
      observation: session.observation,
      answers: session.answers,
      subjectId:
        opts.subjectId ||
        (session.metadata && session.metadata.subjectId) ||
        null,
      entryTime: opts.entryTime,
      extractionStatus: 'pending',
    });

    session.markCaptured(built.trade.id);
    this._persistBuilt(built);

    // Queue OCR / indicator / price / metadata / pattern — never awaits for operator
    // unless runExtractorsSync is enabled (tests).
    const queueReceipt = this.extractor.enqueue({
      screenshot: session.screenshot,
      observation: session.observation,
      trade: built.trade,
      onComplete: (result) => this._applyExtraction(built.trade.id, result),
    });

    session.markBackground('processing');

    const buildCaptured = (receipt) => {
      if (receipt && receipt.status === 'complete') {
        session.markBackground('complete');
      }
      return Object.freeze({
        sessionId: session.id,
        trade: this.trades.get(built.trade.id),
        observation: session.observation,
        screenshot: Object.freeze({
          id: session.screenshot.id,
          imageHash: session.screenshot.imageHash,
          width: session.screenshot.width,
          height: session.screenshot.height,
          mimeType: session.screenshot.mimeType,
          immutable: true,
        }),
        evidence: built.evidence,
        claim: built.claim,
        outcome: built.outcome,
        graph: built.graph,
        extraction: Object.freeze({
          blocksCapture: false,
          queued: !(receipt && receipt.status === 'complete'),
          receipt:
            receipt && typeof receipt.then === 'function' ? null : receipt,
        }),
        operatorTimeBudgetMs: 15000,
        mutatesScreenshot: false,
      });
    };

    if (queueReceipt && typeof queueReceipt.then === 'function') {
      // Sync-extractor mode still returns a Promise from enqueue — await only
      // when explicitly configured; operator default path stays non-blocking.
      return queueReceipt.then((receipt) => buildCaptured(receipt));
    }

    return buildCaptured(queueReceipt);
  }

  /**
   * One-shot capture: paste + answer + save.
   * @param {object} input
   * @param {object|Buffer|string} input.screenshot
   * @param {object} input.answers
   * @param {object} [input.opts]
   */
  async capture(input = {}) {
    const session = this.pasteScreenshot(input.screenshot, input.opts || {});
    this.answer(session.id, input.answers || {});
    return this.save(session.id, input.opts || {});
  }

  /**
   * @param {object} [filter]
   * @param {string} [filter.hypothesis]
   * @param {string} [filter.result]
   * @param {string} [filter.direction]
   * @param {number} [filter.confidence]
   * @param {string} [filter.symbol]
   * @param {string} [filter.subjectId]
   * @returns {object[]}
   */
  findTrades(filter = {}) {
    let rows = [...this.trades.values()];
    if (filter.hypothesis != null) {
      const h = String(filter.hypothesis).toLowerCase();
      rows = rows.filter((t) => String(t.hypothesis).toLowerCase() === h);
    }
    if (filter.result != null) {
      const r = String(filter.result).toLowerCase();
      rows = rows.filter((t) => String(t.result).toLowerCase() === r);
    }
    if (filter.direction != null) {
      const d = String(filter.direction).toLowerCase();
      rows = rows.filter((t) => String(t.direction).toLowerCase() === d);
    }
    if (filter.confidence != null) {
      rows = rows.filter((t) => t.confidence === Number(filter.confidence));
    }
    if (filter.symbol != null) {
      const s = String(filter.symbol).toLowerCase();
      rows = rows.filter(
        (t) => t.symbol && String(t.symbol).toLowerCase() === s
      );
    }
    if (filter.subjectId != null) {
      const s = String(filter.subjectId);
      rows = rows.filter((t) => String(t.subjectId || '') === s);
    }
    return rows;
  }

  /**
   * @returns {object[]}
   */
  winningTrades(filter = {}) {
    return this.findTrades({ ...filter, result: OUTCOMES.WIN });
  }

  /**
   * @returns {object[]}
   */
  losingTrades(filter = {}) {
    return this.findTrades({ ...filter, result: OUTCOMES.LOSS });
  }

  /**
   * Side-by-side win vs loss comparison for Laboratory / EQL.
   * @param {object} [filter]
   */
  compareWinningLosing(filter = {}) {
    const winning = this.winningTrades(filter);
    const losing = this.losingTrades(filter);
    return Object.freeze({
      kind: 'compareWinningLosing',
      left: Object.freeze({ label: 'WinningTrades', trades: winning, count: winning.length }),
      right: Object.freeze({ label: 'LosingTrades', trades: losing, count: losing.length }),
      leftId: 'WinningTrades',
      rightId: 'LosingTrades',
      filter: Object.freeze({ ...filter }),
      equal: winning.length === losing.length && stableEqual(winning, losing),
    });
  }

  /**
   * Replay helper: what did the operator see before entering?
   * @param {string} tradeId
   */
  operatorView(tradeId) {
    const trade = this.trades.get(String(tradeId));
    if (!trade) return null;
    const screenshot = this.screenshots.get(trade.screenshotId) || null;
    const observation = this.observations.get(trade.observationId) || null;
    const evidence = [...this.evidence.values()].filter(
      (e) => e.tradeId === trade.id
    );
    return Object.freeze({
      tradeId: trade.id,
      entryTime: trade.entryTime,
      screenshot: screenshot
        ? Object.freeze({
            id: screenshot.id,
            imageHash: screenshot.imageHash,
            width: screenshot.width,
            height: screenshot.height,
            mimeType: screenshot.mimeType,
            bytes: screenshot.bytes,
            immutable: true,
          })
        : null,
      observation,
      extractedObservations: Object.freeze({
        timestamp: trade.entryTime,
        exchange: trade.exchange,
        timeframe: trade.timeframe,
        symbol: trade.symbol,
        currentPrice: trade.currentPrice,
        indicatorsVisible: trade.indicatorsVisible,
        atr: trade.atr,
        vwap: trade.vwap,
        volume: trade.volume,
      }),
      evidence: Object.freeze(evidence),
      claim: this.claims.get(`claim:trade:${trade.id}`) || null,
      outcome: this.outcomes.get(`outcome:trade:${trade.id}`) || null,
    });
  }

  /**
   * Screenshots linked to a trade (EQL: SHOW Screenshots FOR Trade("…")).
   * @param {string} tradeId
   */
  screenshotsForTrade(tradeId) {
    const trade = this.trades.get(String(tradeId));
    if (!trade) return [];
    const shot = this.screenshots.get(trade.screenshotId);
    if (!shot) return [];
    return [
      Object.freeze({
        id: shot.id,
        tradeId: trade.id,
        imageHash: shot.imageHash,
        width: shot.width,
        height: shot.height,
        mimeType: shot.mimeType,
        observationId: trade.observationId,
        observationType: CHART_SNAPSHOT,
        immutable: true,
      }),
    ];
  }

  /**
   * Project into an EQL EvidenceCatalog seed.
   */
  toCatalogSeed() {
    return {
      trades: this.findTrades(),
      screenshots: [...this.screenshots.values()].map((s) =>
        Object.freeze({
          id: s.id,
          imageHash: s.imageHash,
          width: s.width,
          height: s.height,
          mimeType: s.mimeType,
          immutable: true,
          tradeId:
            [...this.trades.values()].find((t) => t.screenshotId === s.id)?.id ||
            null,
        })
      ),
      observations: [...this.observations.values()],
      evidence: [...this.evidence.values()],
      claims: [...this.claims.values()],
      outcomes: [...this.outcomes.values()],
      subjects: uniqueSubjects(this.findTrades()),
    };
  }

  /**
   * Register a custom extractor (OCR, Chart, Pattern, Indicator, CV).
   * @param {{ id: string, extract: Function }} extractor
   */
  registerExtractor(extractor) {
    this.extractor.register(extractor);
    return this;
  }

  getSession(sessionId) {
    return this.sessions.get(String(sessionId)) || null;
  }

  getTrade(tradeId) {
    return this.trades.get(String(tradeId)) || null;
  }

  getScreenshot(screenshotId) {
    return this.screenshots.get(String(screenshotId)) || null;
  }

  /**
   * @param {object} built
   * @private
   */
  _persistBuilt(built) {
    this.trades.set(built.trade.id, built.trade);
    for (const ev of built.evidence) {
      this.evidence.set(ev.id, ev);
    }
    this.claims.set(built.claim.id, built.claim);
    this.outcomes.set(built.outcome.id, built.outcome);
    this.graphs.set(built.trade.id, built.graph);
  }

  /**
   * Merge extraction into trade without mutating screenshot bytes.
   * @param {string} tradeId
   * @param {object} result
   * @private
   */
  _applyExtraction(tradeId, result) {
    const existing = this.trades.get(String(tradeId));
    if (!existing || !result || !result.extraction) return;

    const extraction = result.extraction;
    const updated = Object.freeze({
      ...existing,
      symbol: coalesce(existing.symbol, extraction.symbol),
      exchange: coalesce(existing.exchange, extraction.exchange),
      timeframe: coalesce(existing.timeframe, extraction.timeframe),
      currentPrice: coalesce(existing.currentPrice, extraction.currentPrice),
      indicatorsVisible: coalesce(
        existing.indicatorsVisible,
        extraction.indicatorsVisible
      ),
      atr: coalesce(existing.atr, extraction.atr),
      vwap: coalesce(existing.vwap, extraction.vwap),
      volume: coalesce(existing.volume, extraction.volume),
      chartImageHash: coalesce(existing.chartImageHash, extraction.chartImageHash),
      screenshotDimensions: coalesce(
        existing.screenshotDimensions,
        extraction.screenshotDimensions
      ),
      entryTime:
        existing.entryTime ||
        extraction.timestamp ||
        existing.entryTime,
      extractionStatus: 'complete',
      extraction: extraction,
    });
    this.trades.set(tradeId, updated);

    for (const ev of result.evidence || []) {
      this.evidence.set(ev.id, ev);
    }

    const session = [...this.sessions.values()].find((s) => s.tradeId === tradeId);
    if (session) session.markBackground('complete');
  }

  /**
   * @param {string} sessionId
   * @private
   */
  _requireSession(sessionId) {
    const session = this.sessions.get(String(sessionId));
    if (!session) {
      throw new Error(`Unknown CaptureSession: ${sessionId}`);
    }
    return session;
  }
}

function coalesce(current, next) {
  return current != null ? current : next != null ? next : null;
}

function uniqueSubjects(trades) {
  const seen = new Set();
  const out = [];
  for (const trade of trades) {
    const id = trade.subjectId || trade.symbol;
    if (!id || seen.has(id)) continue;
    seen.add(id);
    out.push({ id, subject: id, subjectId: id });
  }
  return out;
}

function stableEqual(a, b) {
  try {
    return JSON.stringify(a) === JSON.stringify(b);
  } catch {
    return false;
  }
}

/**
 * @param {object} [deps]
 * @returns {CaptureEngine}
 */
function createCaptureEngine(deps) {
  return new CaptureEngine(deps);
}

module.exports = {
  CaptureEngine,
  createCaptureEngine,
};
