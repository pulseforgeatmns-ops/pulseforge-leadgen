'use strict';

const { round } = require('../../reasoning/ReasoningTypes');
const { DiffEngine } = require('../diff/DiffEngine');
const { ChangeDetector } = require('../change_detection/ChangeDetector');

/**
 * Timeline Builder — expose reasoning history as ordered transitions.
 *
 * Monday → Wednesday → Friday for every recommendation subject.
 */
class TimelineBuilder {
  /**
   * @param {object} [deps]
   * @param {DiffEngine} [deps.diffEngine]
   * @param {ChangeDetector} [deps.changeDetector]
   */
  constructor(deps = {}) {
    this._diff = deps.diffEngine || new DiffEngine();
    this._changes = deps.changeDetector || new ChangeDetector();
  }

  /**
   * @param {object[]} snapshots - chronological ascending
   * @returns {object[]}
   */
  build(snapshots) {
    const rows = [...(snapshots || [])].sort((a, b) => {
      const t = Date.parse(a.timestamp) - Date.parse(b.timestamp);
      return t !== 0 ? t : String(a.id).localeCompare(String(b.id));
    });

    /** @type {object[]} */
    const timeline = [];
    for (let i = 0; i < rows.length; i += 1) {
      const current = rows[i];
      const previous = i === 0 ? null : rows[i - 1];
      const diff = this._diff.diff(previous, current);
      diff.fingerprint = this._diff.fingerprint(diff);
      const changes = this._changes.detect(diff, previous, current);

      timeline.push({
        index: i,
        snapshotId: current.id,
        timestamp: current.timestamp,
        score: current.score,
        confidence: current.confidence,
        scoreDelta: previous ? round(current.score - previous.score) : null,
        confidenceDelta: previous ? round(current.confidence - previous.confidence) : null,
        recommendationType: current.recommendation && current.recommendation.type,
        priority: current.recommendation && current.recommendation.priority,
        changes,
        changeTypes: changes.map((c) => c.type).sort(),
        diff,
      });
    }
    return timeline;
  }
}

module.exports = { TimelineBuilder };
