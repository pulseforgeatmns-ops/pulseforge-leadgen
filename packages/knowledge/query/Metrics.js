'use strict';

/**
 * Query performance metrics (SPEC-001C).
 * Instrumentation only — no dashboard.
 */

/**
 * @param {object} repository
 * @returns {string}
 */
function detectRepositoryType(repository) {
  if (!repository) return 'unknown';
  const name = repository.constructor && repository.constructor.name;
  if (name === 'InMemoryGraphRepository') return 'in-memory';
  if (name === 'PersistentGraphRepository') return 'persistent';
  if (typeof repository.pool === 'object' && repository.pool) return 'persistent';
  if (typeof repository._tenants === 'object') return 'in-memory';
  return name ? String(name) : 'unknown';
}

/**
 * Mutable counters for a single query run.
 */
class MetricsCollector {
  /**
   * @param {string} queryName
   * @param {string} repositoryType
   */
  constructor(queryName, repositoryType) {
    this.queryName = queryName;
    this.repositoryType = repositoryType;
    this.nodesVisited = 0;
    this.edgesTraversed = 0;
    this.resultsReturned = 0;
    this._startedAt = process.hrtime.bigint();
  }

  visitNodes(n = 1) {
    this.nodesVisited += n;
  }

  traverseEdges(n = 1) {
    this.edgesTraversed += n;
  }

  setResults(n) {
    this.resultsReturned = n;
  }

  /**
   * @returns {import('./QueryTypes').QueryMetrics}
   */
  finish() {
    const elapsedNs = process.hrtime.bigint() - this._startedAt;
    return {
      queryName: this.queryName,
      executionTimeMs: Number(elapsedNs) / 1e6,
      nodesVisited: this.nodesVisited,
      edgesTraversed: this.edgesTraversed,
      resultsReturned: this.resultsReturned,
      repositoryType: this.repositoryType,
    };
  }
}

/**
 * Ring buffer of recent metrics emissions.
 */
class MetricsSink {
  /**
   * @param {{ maxHistory?: number, onEmit?: (m: import('./QueryTypes').QueryMetrics) => void }} [options]
   */
  constructor(options = {}) {
    this.maxHistory = options.maxHistory == null ? 200 : options.maxHistory;
    this.onEmit = options.onEmit || null;
    /** @type {import('./QueryTypes').QueryMetrics[]} */
    this._history = [];
    /** @type {import('./QueryTypes').QueryMetrics|null} */
    this.last = null;
  }

  /**
   * @param {import('./QueryTypes').QueryMetrics} metrics
   */
  emit(metrics) {
    this.last = metrics;
    this._history.push(metrics);
    if (this._history.length > this.maxHistory) {
      this._history.splice(0, this._history.length - this.maxHistory);
    }
    if (typeof this.onEmit === 'function') {
      this.onEmit(metrics);
    }
  }

  /**
   * @returns {import('./QueryTypes').QueryMetrics[]}
   */
  history() {
    return [...this._history];
  }

  clear() {
    this._history = [];
    this.last = null;
  }
}

module.exports = {
  detectRepositoryType,
  MetricsCollector,
  MetricsSink,
};
