'use strict';

const { randomUUID } = require('crypto');

const { COG_VERSION, RUN_STATUS } = require('../types');
const { getSuite } = require('../suites');
const { getDomain } = require('../domains');
const { runBenchmarkConversation } = require('../conversations/ConversationRunner');
const { createResultStore } = require('../results/ResultStore');
const { scoreDomainResult, computeOverallScore } = require('../scoring/ScoringEngine');
const { buildFullReport, detectRegressions } = require('../reports');
const { createMaxAskFn, createStubAskFn } = require('../adapters/MaxAdapter');

/**
 * COG Engine — orchestrates benchmark execution, capture, classification, and storage.
 *
 * Architecture (scoring is last):
 *   Domains → Conversations → Behaviors → Failures → Results → Reports → Scoring
 */
class CogEngine {
  /**
   * @param {object} [options]
   * @param {import('./results/ResultStore').ResultStore} [options.store]
   * @param {import('./conversations/ConversationRunner').AskFn} [options.askFn]
   */
  constructor(options = {}) {
    this.store = options.store || createResultStore(options.storeOptions);
    this.askFn = options.askFn || null;
    this.version = COG_VERSION;
  }

  /**
   * @param {import('./conversations/ConversationRunner').AskFn} askFn
   */
  setAskFn(askFn) {
    this.askFn = askFn;
  }

  async ensureAskFn(options = {}) {
    if (this.askFn) return this.askFn;
    this.askFn = await createMaxAskFn(options.maxOptions);
    return this.askFn;
  }

  /**
   * Run a single cognitive domain benchmark.
   * @param {string} domainId
   * @param {object} [options]
   */
  async runDomain(domainId, options = {}) {
    const domain = getDomain(domainId);
    if (!domain) throw new Error(`Unknown domain: ${domainId}`);

    const askFn = options.askFn || this.askFn || await this.ensureAskFn(options);
    let result = await runBenchmarkConversation(domain, askFn, options);

    if (options.score) {
      const scored = scoreDomainResult(result, domain, { automated: true });
      result = {
        ...result,
        score: scored.score,
        reviewStatus: scored.reviewStatus,
        metadata: { scoring: scored },
      };
    }

    return result;
  }

  /**
   * Run a full versioned benchmark suite.
   * @param {string} suiteId
   * @param {object} [options]
   * @returns {Promise<import('./types').CogRunResult>}
   */
  async runSuite(suiteId, options = {}) {
    const suite = getSuite(suiteId);
    if (!suite) throw new Error(`Unknown suite: ${suiteId}`);

    const askFn = options.askFn || this.askFn || await this.ensureAskFn(options);
    const runId = this.store.createRunId();
    const startedAt = new Date().toISOString();

    const domainIds = options.domainIds || suite.domainIds;
    const domains = [];

    for (const domainId of domainIds) {
      const domain = getDomain(domainId);
      if (!domain) {
        domains.push({
          domainId,
          status: RUN_STATUS.FAILED,
          conversationId: null,
          transcript: [],
          failures: [],
          behaviorResults: [],
          score: null,
          reviewStatus: 'pending',
          error: `Unknown domain: ${domainId}`,
          durationMs: 0,
        });
        continue;
      }

      let result = await runBenchmarkConversation(domain, askFn, options);

      if (options.score) {
        const scored = scoreDomainResult(result, domain, { automated: true });
        result = {
          ...result,
          score: scored.score,
          reviewStatus: scored.reviewStatus,
          metadata: { scoring: scored },
        };
      }

      domains.push(result);
    }

    const hasFailed = domains.some(d => d.status === RUN_STATUS.FAILED);
    const run = {
      runId,
      suiteId: suite.id,
      suiteVersion: suite.version,
      cogVersion: this.version,
      status: hasFailed ? RUN_STATUS.FAILED : RUN_STATUS.COMPLETED,
      startedAt,
      completedAt: new Date().toISOString(),
      domains,
      overallScore: computeOverallScore(domains),
      metadata: options.metadata || {},
    };

    if (options.persist !== false) {
      this.store.saveRun(run);
    }

    return run;
  }

  /**
   * Build report with regression detection for a run.
   * @param {string} runId
   */
  getReport(runId) {
    const run = this.store.getRun(runId);
    if (!run) throw new Error(`Run not found: ${runId}`);
    return buildFullReport(run, this.store);
  }

  /**
   * Compare two runs for regression.
   */
  compareRuns(currentRunId, baselineRunId) {
    const current = this.store.getRun(currentRunId);
    const baseline = this.store.getRun(baselineRunId);
    if (!current) throw new Error(`Run not found: ${currentRunId}`);
    if (!baseline) throw new Error(`Run not found: ${baselineRunId}`);
    return detectRegressions(current, baseline);
  }

  listRuns(options) {
    return this.store.listRuns(options);
  }
}

function createCogEngine(options) {
  return new CogEngine(options);
}

module.exports = {
  CogEngine,
  createCogEngine,
  createStubAskFn,
  createMaxAskFn,
};
