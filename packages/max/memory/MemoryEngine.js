'use strict';

const { assertSnapshotRepository } = require('./snapshots/SnapshotRepository');
const { SnapshotEngine } = require('./snapshots/SnapshotEngine');
const {
  InMemorySnapshotRepository,
} = require('./snapshots/InMemorySnapshotRepository');
const { DiffEngine } = require('./diff/DiffEngine');
const { ChangeDetector } = require('./change_detection/ChangeDetector');
const { TimelineBuilder } = require('./timeline/TimelineBuilder');
const { WatchRegistry } = require('./watchers/WatchRegistry');
const { RecommendationEvolution } = require('./history/RecommendationEvolution');
const { TemporalExplanationEngine } = require('./history/TemporalExplanationEngine');
const {
  CHANGE_TYPES,
  WATCH_OPS,
  TREND_DIRECTIONS,
} = require('./snapshots/MemoryTypes');

/**
 * Memory Engine — temporal intelligence over reasoning snapshots.
 *
 * Design: Max shouldn't remember facts. Max remembers transitions.
 * Graph stores state. Memory stores how state changed.
 *
 * APIs: whatChanged, whyChanged, history, trend, scoreHistory, confidenceHistory
 */
class MemoryEngine {
  /**
   * @param {object} deps
   * @param {import('../reasoning/ReasoningEngine').ReasoningEngine} [deps.reasoningEngine]
   * @param {import('./snapshots/SnapshotRepository').SnapshotRepository} [deps.repository]
   * @param {SnapshotEngine} [deps.snapshotEngine]
   * @param {DiffEngine} [deps.diffEngine]
   * @param {ChangeDetector} [deps.changeDetector]
   * @param {TimelineBuilder} [deps.timelineBuilder]
   * @param {WatchRegistry} [deps.watches]
   * @param {RecommendationEvolution} [deps.evolution]
   * @param {TemporalExplanationEngine} [deps.temporalExplanation]
   */
  constructor(deps = {}) {
    this._reasoning = deps.reasoningEngine || null;
    this._repository = deps.repository || new InMemorySnapshotRepository();
    assertSnapshotRepository(this._repository);
    this._snapshots =
      deps.snapshotEngine || new SnapshotEngine({ repository: this._repository });
    this._diff = deps.diffEngine || new DiffEngine();
    this._changes = deps.changeDetector || new ChangeDetector();
    this._timeline = deps.timelineBuilder || new TimelineBuilder({
      diffEngine: this._diff,
      changeDetector: this._changes,
    });
    this._watches = deps.watches || new WatchRegistry();
    this._evolution = deps.evolution || new RecommendationEvolution();
    this._temporalExplain = deps.temporalExplanation || new TemporalExplanationEngine();
  }

  /** @returns {import('./snapshots/SnapshotRepository').SnapshotRepository} */
  get repository() {
    return this._repository;
  }

  /** @returns {WatchRegistry} */
  get watches() {
    return this._watches;
  }

  /**
   * Evaluate (optional) + capture snapshot + compute transition.
   *
   * @param {object} input
   * @param {string} input.tenantId
   * @param {string} input.companyId
   * @param {string} [input.asOf]
   * @param {string} [input.timestamp]
   * @param {object} [input.evaluation] - precomputed evaluate() result
   */
  async remember(input) {
    if (!input || !input.tenantId || !input.companyId) {
      throw new Error('remember requires tenantId and companyId');
    }

    let evaluation = input.evaluation;
    if (!evaluation) {
      if (!this._reasoning) {
        throw new Error('remember requires evaluation or a reasoningEngine');
      }
      evaluation = await this._reasoning.evaluate({
        tenantId: input.tenantId,
        companyId: input.companyId,
        asOf: input.asOf || input.timestamp,
      });
    }

    const previous = await this._repository.latest(input.tenantId, input.companyId);
    const snapshot = await this._snapshots.capture({
      tenantId: input.tenantId,
      companyId: input.companyId,
      timestamp: input.timestamp || input.asOf,
      evaluation,
    });

    const diff = this._diff.diff(previous, snapshot);
    diff.fingerprint = this._diff.fingerprint(diff);
    const changes = this._changes.detect(diff, previous, snapshot);
    const triggeredWatches = this._watches.evaluate({
      diff,
      changes,
      toSnapshot: snapshot,
      fromSnapshot: previous,
    });

    const snapshots = await this._repository.listByCompany(
      input.tenantId,
      input.companyId
    );
    const timeline = this._timeline.build(snapshots);
    const evolution = this._evolution.build({
      snapshots,
      timeline,
      latestDiff: diff,
      latestChanges: changes,
    });

    const temporalExplanation = this._temporalExplain.explain({
      currentExplanation: evaluation.explanation || null,
      toSnapshot: snapshot,
      fromSnapshot: previous,
      diff,
      changes,
      timeline,
    });

    return {
      evaluation,
      snapshot,
      previous,
      diff,
      changes,
      triggeredWatches,
      timeline,
      evolution,
      temporalExplanation,
    };
  }

  /**
   * What changed between two snapshots (or latest vs previous).
   * @param {object} input
   * @param {string} input.tenantId
   * @param {string} input.companyId
   * @param {string} [input.fromSnapshotId]
   * @param {string} [input.toSnapshotId]
   */
  async whatChanged(input) {
    const { from, to } = await this._resolvePair(input);
    const diff = this._diff.diff(from, to);
    diff.fingerprint = this._diff.fingerprint(diff);
    const changes = this._changes.detect(diff, from, to);
    return {
      fromSnapshotId: from ? from.id : null,
      toSnapshotId: to.id,
      scoreDelta: diff.scoreDelta,
      confidenceDelta: diff.confidenceDelta,
      scoreBefore: diff.scoreBefore,
      scoreAfter: diff.scoreAfter,
      newClaims: diff.newClaims,
      removedClaims: diff.removedClaims,
      newEvidence: diff.newEvidence,
      removedEvidence: diff.removedEvidence,
      strategyChanges: diff.strategyChanges.filter((s) => s.changed),
      changes,
      diff,
    };
  }

  /**
   * Why changed — temporal explanation chain.
   * @param {Parameters<MemoryEngine['whatChanged']>[0]} input
   */
  async whyChanged(input) {
    const { from, to } = await this._resolvePair(input);
    const diff = this._diff.diff(from, to);
    diff.fingerprint = this._diff.fingerprint(diff);
    const changes = this._changes.detect(diff, from, to);
    const timeline = this._timeline.build(
      await this._repository.listByCompany(input.tenantId, input.companyId)
    );
    return this._temporalExplain.explain({
      toSnapshot: to,
      fromSnapshot: from,
      diff,
      changes,
      timeline,
    });
  }

  /**
   * Full chronological history for a company.
   * @param {string} tenantId
   * @param {string} companyId
   * @param {{ limit?: number }} [options]
   */
  async history(tenantId, companyId, options) {
    const snapshots = await this._repository.listByCompany(tenantId, companyId, options);
    const timeline = this._timeline.build(snapshots);
    return { snapshots, timeline };
  }

  /**
   * Trend over snapshot history.
   * @param {string} tenantId
   * @param {string} companyId
   */
  async trend(tenantId, companyId) {
    const snapshots = await this._repository.listByCompany(tenantId, companyId);
    const evolution = this._evolution.build({ snapshots });
    return evolution.trend;
  }

  /**
   * @param {string} tenantId
   * @param {string} companyId
   */
  async scoreHistory(tenantId, companyId) {
    const snapshots = await this._repository.listByCompany(tenantId, companyId);
    return snapshots.map((s) => ({
      snapshotId: s.id,
      timestamp: s.timestamp,
      score: s.score,
    }));
  }

  /**
   * @param {string} tenantId
   * @param {string} companyId
   */
  async confidenceHistory(tenantId, companyId) {
    const snapshots = await this._repository.listByCompany(tenantId, companyId);
    return snapshots.map((s) => ({
      snapshotId: s.id,
      timestamp: s.timestamp,
      confidence: s.confidence,
    }));
  }

  /**
   * Recommendation with history / trend / reason / forecast.
   * @param {string} tenantId
   * @param {string} companyId
   */
  async evolve(tenantId, companyId) {
    const snapshots = await this._repository.listByCompany(tenantId, companyId);
    const timeline = this._timeline.build(snapshots);
    const latestChanges =
      timeline.length > 0 ? timeline[timeline.length - 1].changes : [];
    const latestDiff =
      timeline.length > 0 ? timeline[timeline.length - 1].diff : null;
    return this._evolution.build({
      snapshots,
      timeline,
      latestDiff,
      latestChanges,
    });
  }

  /**
   * Register a watch (detection only).
   * @param {Parameters<WatchRegistry['register']>[0]} watch
   */
  watch(watch) {
    return this._watches.register(watch);
  }

  /**
   * @param {object} input
   */
  async _resolvePair(input) {
    const tenantId = String(input.tenantId);
    const companyId = String(input.companyId);
    let to = null;
    let from = null;

    if (input.toSnapshotId) {
      to = await this._repository.getById(tenantId, input.toSnapshotId);
    } else {
      to = await this._repository.latest(tenantId, companyId);
    }
    if (!to) throw new Error(`No snapshots for company ${companyId}`);

    if (input.fromSnapshotId) {
      from = await this._repository.getById(tenantId, input.fromSnapshotId);
      if (!from) throw new Error(`fromSnapshot not found: ${input.fromSnapshotId}`);
    } else {
      const all = await this._repository.listByCompany(tenantId, companyId);
      const idx = all.findIndex((s) => s.id === to.id);
      from = idx > 0 ? all[idx - 1] : null;
    }

    return { from, to };
  }
}

/**
 * @param {object} [options]
 * @param {import('../reasoning/ReasoningEngine').ReasoningEngine} [options.reasoningEngine]
 * @param {import('./snapshots/SnapshotRepository').SnapshotRepository} [options.repository]
 */
function createMemoryEngine(options = {}) {
  return new MemoryEngine(options);
}

module.exports = {
  MemoryEngine,
  createMemoryEngine,
  SnapshotEngine,
  DiffEngine,
  ChangeDetector,
  TimelineBuilder,
  WatchRegistry,
  RecommendationEvolution,
  TemporalExplanationEngine,
  InMemorySnapshotRepository,
  CHANGE_TYPES,
  WATCH_OPS,
  TREND_DIRECTIONS,
};
