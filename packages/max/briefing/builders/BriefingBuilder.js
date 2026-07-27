'use strict';

const { DigestBuilder } = require('../digest/DigestBuilder');
const { CompanyContextCollector } = require('./CompanyContextCollector');
const { Prioritizer } = require('../priorities/Prioritizer');
const { buildExecutiveSummary } = require('../sections/ExecutiveSummary');
const { buildPriorityQueue } = require('../sections/PriorityQueue');
const { buildChangesSection } = require('../sections/ChangesSection');
const { buildWatchAlertsSection } = require('../sections/WatchAlertsSection');
const { buildRisksSection } = require('../sections/RisksSection');
const { buildRecommendationsSection } = require('../sections/RecommendationsSection');
const { buildMetricsSection } = require('../sections/MetricsSection');
const { applyBriefingTemplate } = require('../templates/BriefingTemplate');
const {
  createPresentationAdapter,
} = require('../presentation/PresentationAdapter');
const { BRIEFING_PERIODS } = require('../BriefingTypes');
const { NODE_TYPES } = require('../../../knowledge');

/**
 * Briefing Builder — single entry point for operational briefings.
 *
 * Philosophy: never computes. Assembles Knowledge + Reasoning + Memory.
 *
 *   await max.brief({ tenantId, asOf, period })
 */
class BriefingBuilder {
  /**
   * @param {object} deps
   * @param {import('../../../knowledge/services/KnowledgeService').KnowledgeService} deps.knowledge
   * @param {import('../../memory/MemoryEngine').MemoryEngine} deps.memory
   * @param {Prioritizer} [deps.prioritizer]
   * @param {DigestBuilder} [deps.digest]
   * @param {CompanyContextCollector} [deps.collector]
   */
  constructor(deps) {
    if (!deps || !deps.knowledge) {
      throw new Error('BriefingBuilder requires knowledge');
    }
    if (!deps.memory) {
      throw new Error('BriefingBuilder requires memory');
    }
    this._knowledge = deps.knowledge;
    this._memory = deps.memory;
    this._prioritizer = deps.prioritizer || new Prioritizer();
    this._digest = deps.digest || new DigestBuilder();
    this._collector =
      deps.collector ||
      new CompanyContextCollector({ memory: deps.memory });
  }

  /**
   * Build a structured Briefing (domain object only).
   *
   * @param {object} input
   * @param {string} input.tenantId
   * @param {string} [input.asOf]
   * @param {'daily'|'weekly'|'monthly'} [input.period='daily']
   * @param {string} [input.periodStart]
   * @param {string} [input.periodEnd]
   * @param {number} [input.priorityLimit]
   * @param {number} [input.recommendationLimit]
   * @param {number} [input.riskLimit]
   * @param {number} [input.changeLimit]
   * @param {string} [input.format] - presentation format when present:true
   * @param {boolean} [input.present=false] - if true, wrap via PresentationAdapter
   */
  async build(input) {
    if (!input || !input.tenantId) {
      throw new Error('brief requires tenantId');
    }

    const started = process.hrtime.bigint();
    const tenantId = String(input.tenantId);
    const window = this._digest.buildWindow({
      period: input.period || BRIEFING_PERIODS.DAILY,
      asOf: input.asOf,
      periodStart: input.periodStart,
      periodEnd: input.periodEnd,
    });

    const companies = await this._knowledge.findCompanies({ tenantId });
    const queryCount = 1;

    // Deterministic company order
    const sortedCompanies = [...companies].sort((a, b) =>
      String(a.id).localeCompare(String(b.id))
    );

    /** @type {object[]} */
    const contexts = [];
    for (const company of sortedCompanies) {
      if (company.type && company.type !== NODE_TYPES.COMPANY) continue;
      const ctx = await this._collector.collect({
        tenantId,
        company,
        window,
      });
      contexts.push(ctx);
    }

    const summary = buildExecutiveSummary({ contexts, window });
    const priorities = buildPriorityQueue(contexts, {
      prioritizer: this._prioritizer,
      limit: input.priorityLimit,
    });
    const changes = buildChangesSection(contexts, {
      limit: input.changeLimit,
    });
    const watchAlerts = buildWatchAlertsSection(contexts);
    const risks = buildRisksSection(contexts, {
      limit: input.riskLimit,
    });
    const recommendations = buildRecommendationsSection(contexts, {
      prioritizer: this._prioritizer,
      limit: input.recommendationLimit,
    });

    const executionTimeMs =
      Number(process.hrtime.bigint() - started) / 1e6;
    const metrics = buildMetricsSection({
      contexts,
      window,
      executionTimeMs,
      queryCount,
      baseQueryCount: queryCount,
    });

    const briefing = applyBriefingTemplate({
      summary,
      priorities,
      changes,
      watchAlerts,
      risks,
      recommendations,
      metrics,
      meta: {
        tenantId,
        period: window.period,
        asOf: window.asOf,
        windowStart: window.start,
        windowEnd: window.end,
        builtAt: new Date().toISOString(),
      },
    });

    if (input.present) {
      const adapter = createPresentationAdapter(input.format || 'structured');
      return adapter.present(briefing);
    }

    return briefing;
  }
}

module.exports = {
  BriefingBuilder,
};
