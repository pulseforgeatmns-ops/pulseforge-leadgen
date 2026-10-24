'use strict';

const { EXTRACTOR_IDS, EXTRACTION_JOBS } = require('./types');

/**
 * ObservationExtractor — pluggable multi-extractor pipeline.
 *
 * Each extractor contributes Evidence independently.
 * Failures never block capture; unknown values remain null.
 */
class ObservationExtractor {
  /**
   * @param {object} [deps]
   * @param {object[]} [deps.extractors] - { id, extract(ctx) => partial|Promise }
   * @param {boolean} [deps.runSync=false] - when true, run inline (tests); default queue
   */
  constructor(deps = {}) {
    /** @type {Map<string, { id: string, extract: Function }>} */
    this._extractors = new Map();
    const seed = Array.isArray(deps.extractors)
      ? deps.extractors
      : defaultExtractors();
    for (const extractor of seed) {
      this.register(extractor);
    }
    this._runSync = deps.runSync === true;
    /** @type {object[]} */
    this._queue = [];
  }

  /**
   * @param {{ id: string, extract: Function }} extractor
   */
  register(extractor) {
    if (!extractor || !extractor.id || typeof extractor.extract !== 'function') {
      throw new Error('Extractor requires id and extract(ctx)');
    }
    this._extractors.set(String(extractor.id), {
      id: String(extractor.id),
      extract: extractor.extract,
    });
    return this;
  }

  /**
   * List registered extractor ids.
   * @returns {string[]}
   */
  list() {
    return [...this._extractors.keys()];
  }

  /**
   * Queue background extraction. Returns immediately unless runSync.
   *
   * @param {object} ctx
   * @param {object} ctx.screenshot
   * @param {object} ctx.observation
   * @param {object} [ctx.trade]
   * @param {(result: object) => void|Promise<void>} [ctx.onComplete]
   * @returns {Promise<object>|object}
   */
  enqueue(ctx) {
    const job = {
      id: `extract:${ctx.screenshot && ctx.screenshot.id}:${Date.now()}`,
      jobs: Object.values(EXTRACTION_JOBS),
      status: 'queued',
      enqueuedAt: new Date().toISOString(),
      ctx,
    };
    this._queue.push(job);

    if (this._runSync) {
      return this.run(job);
    }

    // Fire-and-forget — operator never waits.
    setImmediate(() => {
      this.run(job).catch(() => {
        // Failures never block capture; swallow.
      });
    });

    return Object.freeze({
      queued: true,
      jobId: job.id,
      status: 'queued',
      blocksCapture: false,
    });
  }

  /**
   * Run all extractors; merge partials. Never throws for extractor failures.
   *
   * @param {object} job
   * @returns {Promise<object>}
   */
  async run(job) {
    job.status = 'running';
    const contributions = [];
    const merged = emptyExtraction();

    for (const extractor of this._extractors.values()) {
      try {
        const partial = await extractor.extract(job.ctx);
        const safe = sanitizePartial(partial);
        contributions.push(
          Object.freeze({
            extractorId: extractor.id,
            ok: true,
            partial: safe,
          })
        );
        mergePartial(merged, safe);
      } catch (err) {
        contributions.push(
          Object.freeze({
            extractorId: extractor.id,
            ok: false,
            error: err && err.message ? String(err.message) : String(err),
            partial: null,
          })
        );
      }
    }

    const result = Object.freeze({
      jobId: job.id,
      status: 'complete',
      blocksCapture: false,
      extraction: Object.freeze(merged),
      contributions: Object.freeze(contributions),
      evidence: Object.freeze(
        contributions
          .filter((c) => c.ok && c.partial)
          .map((c) =>
            Object.freeze({
              id: `ev:extract:${c.extractorId}:${job.ctx.screenshot.id}`,
              type: 'extraction_contribution',
              extractorId: c.extractorId,
              screenshotId: job.ctx.screenshot.id,
              tradeId: job.ctx.trade ? job.ctx.trade.id : null,
              observationId: job.ctx.observation ? job.ctx.observation.id : null,
              payload: c.partial,
              role: 'supporting',
            })
          )
      ),
    });

    job.status = 'complete';
    job.result = result;

    if (typeof job.ctx.onComplete === 'function') {
      try {
        await job.ctx.onComplete(result);
      } catch {
        // ignore callback failures
      }
    }

    return result;
  }

  /**
   * Pending queue length (for tests / observability).
   */
  pendingCount() {
    return this._queue.filter((j) => j.status === 'queued' || j.status === 'running')
      .length;
  }
}

function emptyExtraction() {
  return {
    timestamp: null,
    exchange: null,
    timeframe: null,
    symbol: null,
    currentPrice: null,
    indicatorsVisible: null,
    atr: null,
    vwap: null,
    volume: null,
    chartImageHash: null,
    screenshotDimensions: null,
  };
}

/**
 * @param {unknown} partial
 */
function sanitizePartial(partial) {
  if (!partial || typeof partial !== 'object') return emptyExtraction();
  const base = emptyExtraction();
  for (const key of Object.keys(base)) {
    if (partial[key] !== undefined) {
      base[key] = partial[key] == null ? null : partial[key];
    }
  }
  return base;
}

/**
 * @param {object} target
 * @param {object} partial
 */
function mergePartial(target, partial) {
  for (const [key, value] of Object.entries(partial)) {
    if (value == null) continue;
    if (target[key] == null) {
      target[key] = value;
    }
  }
}

/**
 * Default extractors — stubs that never throw and leave unknowns null.
 * Real OCR / CV can be registered later without changing CaptureEngine.
 */
function defaultExtractors() {
  return [
    {
      id: EXTRACTOR_IDS.OCR,
      extract(ctx) {
        return {
          chartImageHash:
            (ctx.screenshot && ctx.screenshot.imageHash) ||
            (ctx.observation &&
              ctx.observation.payload &&
              ctx.observation.payload.imageHash) ||
            null,
          screenshotDimensions:
            ctx.screenshot &&
            (ctx.screenshot.width != null || ctx.screenshot.height != null)
              ? {
                  width: ctx.screenshot.width,
                  height: ctx.screenshot.height,
                }
              : null,
          // OCR fields intentionally null until a real OCR backend is wired.
          timestamp: null,
          exchange: null,
          timeframe: null,
          symbol: null,
          currentPrice: null,
        };
      },
    },
    {
      id: EXTRACTOR_IDS.CHART,
      extract() {
        return emptyExtraction();
      },
    },
    {
      id: EXTRACTOR_IDS.PATTERN,
      extract() {
        return emptyExtraction();
      },
    },
    {
      id: EXTRACTOR_IDS.INDICATOR,
      extract() {
        return {
          indicatorsVisible: null,
          atr: null,
          vwap: null,
          volume: null,
        };
      },
    },
    {
      id: EXTRACTOR_IDS.COMPUTER_VISION,
      extract() {
        return emptyExtraction();
      },
    },
  ];
}

/**
 * @param {object} [deps]
 * @returns {ObservationExtractor}
 */
function createObservationExtractor(deps) {
  return new ObservationExtractor(deps);
}

module.exports = {
  ObservationExtractor,
  createObservationExtractor,
  emptyExtraction,
  defaultExtractors,
};
