'use strict';

const fs = require('fs');
const path = require('path');
const { randomUUID } = require('crypto');

const DEFAULT_STORE_PATH = path.join(__dirname, '..', 'data', 'cog-results.json');

/**
 * In-memory + file-backed COG result store.
 * Postgres adapter lives in utils/cogSchema.js for production persistence.
 */
class ResultStore {
  /**
   * @param {object} [options]
   * @param {string} [options.storePath]
   * @param {boolean} [options.loadExisting=true]
   */
  constructor(options = {}) {
    this.storePath = options.storePath || DEFAULT_STORE_PATH;
    this.runs = new Map();
    if (options.loadExisting !== false) {
      this._load();
    }
  }

  _load() {
    try {
      if (!fs.existsSync(this.storePath)) return;
      const raw = fs.readFileSync(this.storePath, 'utf8');
      const data = JSON.parse(raw);
      for (const run of data.runs || []) {
        this.runs.set(run.runId, run);
      }
    } catch (error) {
      if (error.code !== 'ENOENT') {
        console.warn('[cog] ResultStore load warning:', error.message);
      }
    }
  }

  _persist() {
    const dir = path.dirname(this.storePath);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
    const payload = {
      version: 1,
      updatedAt: new Date().toISOString(),
      runs: Array.from(this.runs.values()),
    };
    fs.writeFileSync(this.storePath, JSON.stringify(payload, null, 2));
  }

  /**
   * @param {import('../types').CogRunResult} run
   */
  saveRun(run) {
    this.runs.set(run.runId, { ...run });
    this._persist();
    return { ...run };
  }

  getRun(runId) {
    const found = this.runs.get(runId);
    return found ? { ...found } : null;
  }

  listRuns(options = {}) {
    let runs = Array.from(this.runs.values());
    if (options.suiteId) {
      runs = runs.filter(r => r.suiteId === options.suiteId);
    }
    if (options.suiteVersion) {
      runs = runs.filter(r => r.suiteVersion === options.suiteVersion);
    }
    runs.sort((a, b) => String(b.startedAt).localeCompare(String(a.startedAt)));
    if (options.limit) {
      runs = runs.slice(0, options.limit);
    }
    return runs.map(r => ({ ...r }));
  }

  /**
   * Find the most recent run before a given run (for regression baseline).
   */
  getPreviousRun(run) {
    const sameSuite = this.listRuns({ suiteId: run.suiteId })
      .filter(r => r.runId !== run.runId && r.startedAt < run.startedAt);
    return sameSuite[0] || null;
  }

  /**
   * Get latest completed run for a suite.
   */
  getLatestRun(suiteId) {
    const runs = this.listRuns({ suiteId, limit: 1 });
    return runs[0] || null;
  }

  createRunId() {
    return randomUUID();
  }
}

function createResultStore(options) {
  return new ResultStore(options);
}

module.exports = {
  ResultStore,
  createResultStore,
  DEFAULT_STORE_PATH,
};
