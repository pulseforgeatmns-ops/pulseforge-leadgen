'use strict';

const { deepFreeze } = require('../reasoning/ReasoningTypes');
const { ACTION_TYPES } = require('../commandDeck/CommandDeckTypes');
const { CompanyContextCollector } = require('../briefing/builders/CompanyContextCollector');
const {
  NAV_TYPES,
  buildRecommendationId,
  buildNavRef,
} = require('./IntelligenceTypes');
const { RelatedIntelligenceBuilder } = require('./RelatedIntelligence');

/**
 * Company Intelligence Composer — assemble company workspace sections.
 * May not score, rank, or invent.
 */
class CompanyIntelligenceComposer {
  /**
   * @param {object} deps
   * @param {import('../../knowledge/services/KnowledgeService').KnowledgeService} deps.knowledge
   * @param {import('../memory/MemoryEngine').MemoryEngine} deps.memory
   * @param {import('../policy/engine/PolicyEngine').PolicyEngine} [deps.policy]
   * @param {RelatedIntelligenceBuilder} [deps.related]
   * @param {CompanyContextCollector} [deps.collector]
   */
  constructor(deps) {
    if (!deps || !deps.knowledge) {
      throw new Error('CompanyIntelligenceComposer requires knowledge');
    }
    if (!deps.memory) {
      throw new Error('CompanyIntelligenceComposer requires memory');
    }
    this._knowledge = deps.knowledge;
    this._memory = deps.memory;
    this._policy = deps.policy || null;
    this._related =
      deps.related ||
      new RelatedIntelligenceBuilder({
        knowledge: deps.knowledge,
        memory: deps.memory,
      });
    this._collector =
      deps.collector ||
      new CompanyContextCollector({ memory: deps.memory });
  }

  /**
   * @param {object} input
   * @param {string} input.tenantId
   * @param {string} input.companyId
   * @param {string} [input.asOf]
   * @param {string} [input.periodStart]
   * @param {string} [input.operator]
   * @returns {Promise<object>} CompanyIntelligenceModel
   */
  async compose(input) {
    if (!input || !input.tenantId) {
      throw new Error('composeCompany requires tenantId');
    }
    if (!input.companyId) {
      throw new Error('composeCompany requires companyId');
    }

    const started = process.hrtime.bigint();
    const tenantId = String(input.tenantId);
    const companyId = String(input.companyId);
    const asOf = input.asOf || new Date().toISOString();
    const periodStart =
      input.periodStart ||
      new Date(Date.parse(asOf) - 7 * 24 * 60 * 60 * 1000).toISOString();

    let company = null;
    try {
      company = await this._knowledge.findNode(tenantId, companyId);
    } catch (_err) {
      company = null;
    }
    if (!company) {
      try {
        const companies = await this._knowledge.findCompanies({
          tenantId,
          limit: 500,
        });
        company = companies.find((c) => c.id === companyId) || null;
      } catch (_err) {
        company = null;
      }
    }

    if (!company) {
      return emptyCompanyModel({
        tenantId,
        companyId,
        asOf,
        reason: 'company_not_found',
        buildTimeMs: elapsedMs(started),
      });
    }

    const window = {
      startMs: Date.parse(periodStart),
      endMs: Date.parse(asOf),
    };

    let ctx = null;
    try {
      ctx = await this._collector.collect({
        tenantId,
        company,
        window,
      });
    } catch (_err) {
      ctx = null;
    }

    let timeline = [];
    try {
      timeline = await this._knowledge.timeline({
        tenantId,
        nodeId: companyId,
      });
    } catch (_err) {
      timeline = [];
    }

    let people = [];
    try {
      people = await this._knowledge.findPeople({
        tenantId,
        companyId,
        limit: 40,
      });
    } catch (_err) {
      people = [];
    }

    let interactions = [];
    try {
      interactions = await this._knowledge.findInteractions({
        tenantId,
        relatedNodeId: companyId,
        limit: 40,
      });
    } catch (_err) {
      interactions = [];
    }

    const recommendation = (ctx && ctx.recommendation) || null;
    const recommendationId = recommendation
      ? recommendation.id
      : buildRecommendationId(tenantId, companyId);

    let policy = null;
    if (this._policy && recommendation) {
      try {
        policy = await this._policy.evaluate({
          tenantId,
          recommendation,
          context: {
            asOf,
            operator: input.operator || null,
            companyId,
          },
        });
      } catch (_err) {
        policy = null;
      }
    }

    const related = await this._related.forCompany({
      tenantId,
      companyId,
      company,
      changes: (ctx && ctx.changes) || [],
    });

    const evidence = collectEvidence(recommendation, timeline);

    const model = {
      kind: 'company_intelligence',
      companyId,
      companyName: company.name || companyId,
      overview: {
        name: company.name || companyId,
        industry:
          (company.metadata && company.metadata.industry) ||
          company.industry ||
          null,
        location:
          (company.metadata && company.metadata.location) ||
          company.location ||
          null,
        confidence:
          company.metadata && company.metadata.confidence != null
            ? Number(company.metadata.confidence)
            : null,
      },
      reasoning: recommendation
        ? {
            recommendationId,
            type: recommendation.type || null,
            priority: recommendation.priority || null,
            score:
              recommendation.score != null
                ? Number(recommendation.score)
                : null,
            confidence:
              recommendation.confidence != null
                ? Number(recommendation.confidence)
                : null,
            recommendedAction: recommendation.recommendedAction || null,
            whyThis:
              (recommendation.reasoningSummary &&
                recommendation.reasoningSummary.whyThis) ||
              [],
            whyNot:
              (recommendation.reasoningSummary &&
                recommendation.reasoningSummary.whyNot) ||
              [],
            whyNow:
              (recommendation.reasoningSummary &&
                recommendation.reasoningSummary.whyNow) ||
              [],
          }
        : null,
      memory: {
        trend: (ctx && ctx.trend) || null,
        snapshotCount: (ctx && ctx.snapshots && ctx.snapshots.length) || 0,
        changes: ((ctx && ctx.changes) || []).slice(0, 20).map((c) => ({
          type: c.type || null,
          field: c.field || null,
          summary: c.summary || c.type || 'change',
          at: c.at || null,
        })),
        watches: ((ctx && ctx.triggeredWatches) || []).slice(0, 10),
      },
      timeline: (timeline || []).slice(0, 24).map((ev) => ({
        id: ev.id || ev.nodeId || null,
        type: ev.type || ev.nodeType || null,
        summary: ev.summary || ev.label || ev.id || '',
        at: ev.at || ev.timestamp || ev.occurredAt || null,
        nav: buildNavRef({
          type: NAV_TYPES.EVIDENCE,
          id: ev.id || ev.nodeId,
          label: ev.summary || ev.label || ev.id,
        }),
      })),
      evidence,
      interactions: (interactions || []).slice(0, 20).map((ix) => ({
        id: ix.id,
        summary: ix.summary || ix.name || ix.id,
        at: ix.occurredAt || ix.timestamp || null,
        nav: buildNavRef({
          type: NAV_TYPES.INTERACTION,
          id: ix.id,
          label: ix.summary || ix.name || ix.id,
        }),
      })),
      people: (people || []).slice(0, 20).map((p) => ({
        id: p.id,
        name: p.name || p.id,
        title: (p.metadata && p.metadata.title) || p.title || null,
      })),
      policy: policy
        ? {
            outcome: policy.outcome,
            auditId: (policy.audit && policy.audit.id) || null,
            reasons: policy.reasons || policy.explanations || [],
          }
        : null,
      recommendations: recommendation
        ? [
            {
              id: recommendationId,
              label:
                recommendation.recommendedAction ||
                recommendation.type ||
                recommendationId,
              score:
                recommendation.score != null
                  ? Number(recommendation.score)
                  : null,
              confidence:
                recommendation.confidence != null
                  ? Number(recommendation.confidence)
                  : null,
              nav: buildNavRef({
                type: NAV_TYPES.RECOMMENDATION,
                id: recommendationId,
                label:
                  recommendation.recommendedAction ||
                  company.name ||
                  recommendationId,
              }),
            },
          ]
        : [],
      related,
      actions: [
        {
          id: 'ask_max',
          type: ACTION_TYPES.ASK_MAX,
          label: 'Ask Max about this company',
          payload: {
            companyId,
            recommendationId,
            page: 'company',
            context: 'company_intelligence',
          },
        },
        recommendation
          ? {
              id: 'review_recommendation',
              type: ACTION_TYPES.REVIEW_RECOMMENDATION,
              label: 'Review recommendation',
              payload: { recommendationId, companyId },
            }
          : null,
        {
          id: 'back_deck',
          type: 'return_deck',
          label: 'Back to Command Deck',
          payload: {},
        },
      ].filter(Boolean),
      empty: false,
      emptyReason: null,
      meta: {
        tenantId,
        asOf,
        generatedAt: asOf,
        buildTimeMs: elapsedMs(started),
        snapshotId: (ctx && ctx.latest && ctx.latest.id) || null,
      },
    };

    return deepFreeze(model);
  }
}

function collectEvidence(recommendation, timeline) {
  const out = [];
  const seen = new Set();
  for (const s of (recommendation && recommendation.supportingSignals) || []) {
    if (!s || s.id == null || seen.has(String(s.id))) continue;
    seen.add(String(s.id));
    out.push({
      id: String(s.id),
      kind: s.kind || 'evidence',
      summary: s.summary || String(s.id),
      polarity: 'supporting',
      nav: buildNavRef({
        type: NAV_TYPES.EVIDENCE,
        id: s.id,
        label: s.summary || s.id,
      }),
    });
  }
  for (const s of (recommendation && recommendation.opposingSignals) || []) {
    if (!s || s.id == null || seen.has(String(s.id))) continue;
    seen.add(String(s.id));
    out.push({
      id: String(s.id),
      kind: s.kind || 'evidence',
      summary: s.summary || String(s.id),
      polarity: 'contradicting',
      nav: buildNavRef({
        type: NAV_TYPES.EVIDENCE,
        id: s.id,
        label: s.summary || s.id,
      }),
    });
  }
  for (const ev of timeline || []) {
    const id = ev.id || ev.nodeId;
    if (!id || seen.has(String(id))) continue;
    seen.add(String(id));
    if (out.length >= 24) break;
    out.push({
      id: String(id),
      kind: ev.type || 'timeline',
      summary: ev.summary || ev.label || String(id),
      polarity: 'timeline',
      nav: buildNavRef({
        type: NAV_TYPES.EVIDENCE,
        id,
        label: ev.summary || ev.label || id,
      }),
    });
  }
  return out;
}

function emptyCompanyModel(input) {
  return deepFreeze({
    kind: 'company_intelligence',
    companyId: input.companyId,
    companyName: null,
    overview: null,
    reasoning: null,
    memory: {
      trend: null,
      snapshotCount: 0,
      changes: [],
      watches: [],
    },
    timeline: [],
    evidence: [],
    interactions: [],
    people: [],
    policy: null,
    recommendations: [],
    related: {
      similarCompanies: [],
      sharedSignals: [],
      competingOpportunities: [],
      recentChanges: [],
      supportingEvidence: [],
      contradictingEvidence: [],
      alternativeRecommendations: [],
      otherRecommendations: [],
      sourceInteractions: [],
    },
    actions: [
      {
        id: 'back_deck',
        type: 'return_deck',
        label: 'Back to Command Deck',
        payload: {},
      },
      {
        id: 'ask_max',
        type: ACTION_TYPES.ASK_MAX,
        label: 'Ask Max',
        payload: { companyId: input.companyId, page: 'company' },
      },
    ],
    empty: true,
    emptyReason: input.reason || 'unavailable',
    meta: {
      tenantId: input.tenantId,
      asOf: input.asOf,
      generatedAt: input.asOf,
      buildTimeMs: input.buildTimeMs || 0,
      snapshotId: null,
    },
  });
}

function elapsedMs(started) {
  const ns = process.hrtime.bigint() - started;
  return Number(ns) / 1e6;
}

module.exports = {
  CompanyIntelligenceComposer,
  createCompanyIntelligenceComposer(deps) {
    return new CompanyIntelligenceComposer(deps);
  },
};
