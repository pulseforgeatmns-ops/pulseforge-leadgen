'use strict';

const { DiffEngine } = require('../../memory/diff/DiffEngine');
const { ChangeDetector } = require('../../memory/change_detection/ChangeDetector');
const { selectPeriodSnapshots } = require('../digest/PeriodWindow');
const { TREND_DIRECTIONS } = require('../../memory/snapshots/MemoryTypes');

/**
 * Collect per-company memory context for a briefing window.
 * Assembles only — never calls ReasoningEngine.evaluate().
 */
class CompanyContextCollector {
  /**
   * @param {object} deps
   * @param {import('../../memory/MemoryEngine').MemoryEngine} deps.memory
   * @param {DiffEngine} [deps.diffEngine]
   * @param {ChangeDetector} [deps.changeDetector]
   */
  constructor(deps) {
    if (!deps || !deps.memory) {
      throw new Error('CompanyContextCollector requires memory');
    }
    this._memory = deps.memory;
    this._diff = deps.diffEngine || new DiffEngine();
    this._changes = deps.changeDetector || new ChangeDetector();
  }

  /**
   * @param {object} input
   * @param {string} input.tenantId
   * @param {object} input.company - knowledge company node (id, name, ...)
   * @param {{ startMs: number, endMs: number }} input.window
   */
  async collect(input) {
    const tenantId = String(input.tenantId);
    const companyId = String(input.company.id);
    const snapshots = await this._memory.repository.listByCompany(
      tenantId,
      companyId
    );
    const { baseline, latest, inWindow } = selectPeriodSnapshots(
      snapshots,
      input.window
    );

    let diff = null;
    let changes = [];
    let triggeredWatches = [];

    if (latest) {
      const from =
        baseline && baseline.id !== latest.id ? baseline : null;
      diff = this._diff.diff(from, latest);
      diff.fingerprint = this._diff.fingerprint(diff);
      changes = this._changes.detect(diff, from, latest);
      triggeredWatches = this._memory.watches.evaluate({
        diff,
        changes,
        toSnapshot: latest,
        fromSnapshot: from,
      });
    }

    let trend = {
      score: TREND_DIRECTIONS.INSUFFICIENT,
      confidence: TREND_DIRECTIONS.INSUFFICIENT,
    };
    if (snapshots.length >= 1) {
      const evolution = await this._memory.evolve(tenantId, companyId);
      trend = evolution.trend;
    }

    const recommendation = latest && latest.recommendation
      ? latest.recommendation
      : null;

    return {
      companyId,
      companyName:
        (input.company.name != null && input.company.name) ||
        (latest && latest.meta && latest.meta.subjectName) ||
        null,
      company: input.company,
      snapshots,
      baseline,
      latest,
      inWindow,
      diff,
      changes,
      triggeredWatches,
      trend,
      recommendation,
      memoryLookups: 1 + (snapshots.length >= 1 ? 1 : 0), // list + optional evolve path
      queryCount: 0,
    };
  }
}

module.exports = {
  CompanyContextCollector,
};
