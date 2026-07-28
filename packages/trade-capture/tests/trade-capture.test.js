'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  createCaptureEngine,
  createScreenshotProcessor,
  createObservationExtractor,
  CAPTURE_RULES,
  OUTCOMES,
  DIRECTIONS,
  HYPOTHESES,
  CHART_SNAPSHOT,
  OPERATOR_QUESTIONS,
  hashImageBytes,
} = require('..');

/** Minimal 1×1 PNG */
const TINY_PNG = Buffer.from(
  'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==',
  'base64'
);

describe('SPEC-044 Trade Capture rules', () => {
  it('exports guiding rules and operator chips', () => {
    assert.ok(CAPTURE_RULES.SCREENSHOT_FIRST);
    assert.ok(CAPTURE_RULES.EXTRACTION_NON_BLOCKING);
    assert.ok(CAPTURE_RULES.IMMUTABLE_SCREENSHOTS);
    assert.equal(OUTCOMES.WIN, 'Win');
    assert.equal(DIRECTIONS.LONG, 'Long');
    assert.equal(HYPOTHESES.VELOCITY, 'Velocity');
    assert.equal(CHART_SNAPSHOT, 'chart_snapshot');
    assert.equal(OPERATOR_QUESTIONS.length, 4);
  });
});

describe('SPEC-044 ScreenshotProcessor', () => {
  it('creates immutable chart_snapshot observation with image hash', () => {
    const processor = createScreenshotProcessor({
      idFactory: () => 'shot-1',
      now: () => '2026-07-28T12:00:00.000Z',
    });
    const { screenshot, observation } = processor.process(TINY_PNG, {
      subjectId: 'BTC',
    });

    assert.equal(screenshot.id, 'shot-1');
    assert.equal(screenshot.immutable, true);
    assert.equal(screenshot.imageHash, hashImageBytes(TINY_PNG));
    assert.equal(screenshot.width, 1);
    assert.equal(screenshot.height, 1);
    assert.equal(observation.observationType, CHART_SNAPSHOT);
    assert.equal(observation.payload.chartImageHash, screenshot.imageHash);
    assert.equal(observation.subjectId, 'BTC');

    // Original bytes frozen — reassignment of properties fails silently on freeze;
    // length must remain stable.
    assert.equal(screenshot.bytes.length, TINY_PNG.length);
  });

  it('accepts data URL paste input', () => {
    const processor = createScreenshotProcessor({ idFactory: () => 'shot-2' });
    const dataUrl = `data:image/png;base64,${TINY_PNG.toString('base64')}`;
    const { screenshot } = processor.process(dataUrl);
    assert.equal(screenshot.mimeType, 'image/png');
    assert.equal(screenshot.imageHash, hashImageBytes(TINY_PNG));
  });
});

describe('SPEC-044 Capture flow (<15s operator path)', () => {
  it('paste → four chips → save without waiting on extraction', async () => {
    const engine = createCaptureEngine({
      runExtractorsSync: false,
      idFactory: (() => {
        let n = 0;
        return () => `id-${++n}`;
      })(),
    });

    const t0 = Date.now();
    const session = engine.pasteScreenshot(TINY_PNG, { subjectId: 'BTC' });
    assert.equal(session.status, 'awaiting_answers');
    assert.ok(session.observation.observationType === CHART_SNAPSHOT);

    engine.answer(session.id, {
      result: 'Win',
      direction: 'Long',
      hypothesis: 'Velocity',
      confidence: 4,
    });
    assert.equal(session.isComplete(), true);

    const captured = engine.save(session.id);
    const elapsed = Date.now() - t0;

    assert.ok(elapsed < 15000, `operator path took ${elapsed}ms`);
    assert.equal(captured.extraction.blocksCapture, false);
    assert.equal(captured.trade.result, 'Win');
    assert.equal(captured.trade.direction, 'Long');
    assert.equal(captured.trade.hypothesis, 'Velocity');
    assert.equal(captured.trade.confidence, 4);
    assert.equal(captured.trade.screenshotId, session.screenshot.id);
    assert.equal(captured.mutatesScreenshot, false);
    assert.equal(captured.screenshot.immutable, true);
    assert.ok(captured.graph.edges.length >= 4);
    assert.equal(captured.claim.hypothesis, 'Velocity');
    assert.equal(captured.outcome.verdict, 'correct');
  });

  it('extraction failures never block or mutate the screenshot', async () => {
    const engine = createCaptureEngine({
      runExtractorsSync: true,
      extractors: [
        {
          id: 'ocr',
          extract() {
            throw new Error('OCR offline');
          },
        },
        {
          id: 'chart',
          extract(ctx) {
            return {
              symbol: 'ETH',
              chartImageHash: ctx.screenshot.imageHash,
            };
          },
        },
      ],
    });

    const beforeHash = hashImageBytes(TINY_PNG);
    const captured = await engine.capture({
      screenshot: TINY_PNG,
      answers: {
        result: 'Loss',
        direction: 'Short',
        hypothesis: 'Breakout',
        confidence: 2,
      },
      opts: { subjectId: 'ETH' },
    });

    assert.equal(captured.trade.result, 'Loss');
    const shot = engine.getScreenshot(captured.screenshot.id);
    assert.equal(shot.imageHash, beforeHash);
    assert.equal(hashImageBytes(shot.bytes), beforeHash);
    // Successful extractor still contributed
    assert.equal(captured.trade.symbol, 'ETH');
  });

  it('findTrades filters by hypothesis', async () => {
    const engine = createCaptureEngine({ runExtractorsSync: true });
    await engine.capture({
      screenshot: TINY_PNG,
      answers: {
        result: 'Win',
        direction: 'Long',
        hypothesis: 'Velocity',
        confidence: 5,
      },
    });
    await engine.capture({
      screenshot: Buffer.from('second-shot'),
      answers: {
        result: 'Loss',
        direction: 'Short',
        hypothesis: 'Pullback',
        confidence: 3,
      },
    });

    const velocity = engine.findTrades({ hypothesis: 'Velocity' });
    assert.equal(velocity.length, 1);
    assert.equal(velocity[0].hypothesis, 'Velocity');

    const comparison = engine.compareWinningLosing();
    assert.equal(comparison.left.count, 1);
    assert.equal(comparison.right.count, 1);
    assert.equal(comparison.leftId, 'WinningTrades');
  });

  it('operatorView recreates what the operator saw', async () => {
    const engine = createCaptureEngine({ runExtractorsSync: true });
    const captured = await engine.capture({
      screenshot: TINY_PNG,
      answers: {
        result: 'Win',
        direction: 'Long',
        hypothesis: 'Mean Reversion',
        confidence: 3,
      },
      opts: { subjectId: 'BTC' },
    });

    const view = engine.operatorView(captured.trade.id);
    assert.equal(view.screenshot.immutable, true);
    assert.equal(view.observation.observationType, CHART_SNAPSHOT);
    assert.ok(view.screenshot.bytes);
  });
});

describe('SPEC-044 ObservationExtractor', () => {
  it('supports multiple independent extractors', async () => {
    const extractor = createObservationExtractor({
      runSync: true,
      extractors: [
        { id: 'ocr', extract: () => ({ exchange: 'Coinbase' }) },
        { id: 'indicator', extract: () => ({ atr: 120, vwap: 65000 }) },
        {
          id: 'pattern',
          extract: () => {
            throw new Error('pattern fail');
          },
        },
      ],
    });

    const result = await extractor.enqueue({
      screenshot: { id: 's1', imageHash: 'abc' },
      observation: { id: 'o1' },
      trade: { id: 't1' },
    });

    assert.equal(result.status, 'complete');
    assert.equal(result.extraction.exchange, 'Coinbase');
    assert.equal(result.extraction.atr, 120);
    assert.equal(result.contributions.filter((c) => !c.ok).length, 1);
    assert.equal(result.blocksCapture, false);
  });
});
