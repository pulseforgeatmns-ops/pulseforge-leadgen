'use strict';

const {
  createDefaultRuleRegistry,
  RuleRegistry,
} = require('../rules');
const { TenantPolicyStore } = require('./TenantPolicyStore');
const { PolicyAuditLog } = require('../audit/PolicyAuditLog');
const {
  aggregateRuleResults,
  buildDecisionExplanation,
} = require('../evaluation/DecisionAggregator');
const {
  approvalRequired,
  canAutonomousExecute,
  buildApprovalTicket,
} = require('../approvals/ApprovalHelpers');
const { deepFreeze } = require('../../reasoning/ReasoningTypes');

/**
 * Policy Engine — safety layer over recommendations.
 *
 * Reasoning: what should happen?
 * Policy: what is allowed to happen?
 *
 * Never performs reasoning. Never executes actions.
 */
class PolicyEngine {
  /**
   * @param {object} [deps]
   * @param {RuleRegistry} [deps.registry]
   * @param {TenantPolicyStore} [deps.policies]
   * @param {PolicyAuditLog} [deps.auditLog]
   */
  constructor(deps = {}) {
    this._registry = deps.registry || createDefaultRuleRegistry();
    this._policies = deps.policies || new TenantPolicyStore();
    this._auditLog = deps.auditLog || new PolicyAuditLog();
  }

  /** @returns {RuleRegistry} */
  get registry() {
    return this._registry;
  }

  /** @returns {TenantPolicyStore} */
  get policies() {
    return this._policies;
  }

  /** @returns {PolicyAuditLog} */
  get auditLog() {
    return this._auditLog;
  }

  /**
   * Configure tenant policy (data-driven).
   * @param {string} tenantId
   * @param {object} config
   */
  configureTenant(tenantId, config) {
    return this._policies.set(tenantId, config);
  }

  /**
   * Evaluate a recommendation against explicit policy rules.
   *
   * @param {object} input
   * @param {string} input.tenantId
   * @param {object} input.recommendation
   * @param {object} [input.context]
   * @param {string} [input.operator]
   * @param {string} [input.asOf]
   * @returns {Promise<object>} PolicyDecision
   */
  async evaluate(input) {
    if (!input || !input.tenantId) {
      throw new Error('policy.evaluate requires tenantId');
    }
    if (!input.recommendation || typeof input.recommendation !== 'object') {
      throw new Error('policy.evaluate requires recommendation');
    }

    const tenantId = String(input.tenantId);
    const recommendation = input.recommendation;
    const extra = input.context && typeof input.context === 'object'
      ? input.context
      : {};
    const policy = this._policies.get(tenantId);

    const recommendationClone = JSON.parse(JSON.stringify(recommendation));
    const extraClone = JSON.parse(JSON.stringify(extra));
    const context = deepFreeze({
      tenantId,
      recommendation: recommendationClone,
      policy,
      asOf: input.asOf || extraClone.asOf || new Date().toISOString(),
      now: input.asOf || extraClone.now || extraClone.asOf,
      channel: input.channel || extraClone.channel || null,
      risk: extraClone.risk,
      riskScore: extraClone.riskScore,
      risks: extraClone.risks,
      contradictionSeverity: extraClone.contradictionSeverity,
      lastActionAt: extraClone.lastActionAt || extraClone.lastOutreachAt,
      lastOutreachAt: extraClone.lastOutreachAt,
      dailyOutreachCount: extraClone.dailyOutreachCount,
      people: extraClone.people,
      decisionMakers: extraClone.decisionMakers,
      hasVerifiedDecisionMaker: extraClone.hasVerifiedDecisionMaker,
      verifiedDecisionMaker: extraClone.verifiedDecisionMaker,
      evidence: extraClone.evidence,
      evidenceAgeDays: extraClone.evidenceAgeDays,
      newestEvidenceAt: extraClone.newestEvidenceAt,
      evidenceFreshnessAt: extraClone.evidenceFreshnessAt,
      explanation: extraClone.explanation,
      operator: input.operator || extraClone.operator || null,
    });

    const { results, timings } = this._registry.evaluateAll(context);
    const aggregated = aggregateRuleResults(results);

    const explanation = buildDecisionExplanation({
      recommendationId: recommendation.id || null,
      recommendationType: recommendation.type || null,
      score: recommendation.score,
      confidence: recommendation.confidence,
      recommendedAction: recommendation.recommendedAction || null,
      decision: aggregated,
    });

    const decision = {
      allowed: aggregated.allowed,
      requiresApproval: aggregated.requiresApproval,
      blocked: aggregated.blocked,
      warned: aggregated.warned,
      severity: aggregated.severity,
      outcome: aggregated.outcome,
      reason: aggregated.reason,
      matchedRules: aggregated.matchedRules,
      ruleResults: aggregated.ruleResults,
      explanation,
      canAutonomousExecute: canAutonomousExecute(aggregated),
      approvalTicket: approvalRequired(aggregated)
        ? buildApprovalTicket({
            decision: aggregated,
            recommendationId: recommendation.id || null,
            tenantId,
            createdAt: context.asOf,
          })
        : null,
      meta: {
        tenantId,
        recommendationId: recommendation.id || null,
        asOf: context.asOf,
        ruleCount: results.length,
        timings,
      },
    };

    const audit = this._auditLog.record({
      tenantId,
      timestamp: context.asOf,
      recommendationId: recommendation.id || null,
      decision,
      matchedRules: decision.matchedRules,
      operator: input.operator || null,
      meta: {
        outcome: decision.outcome,
        severity: decision.severity,
      },
    });

    decision.audit = audit;
    return decision;
  }
}

/**
 * @param {object} [options]
 */
function createPolicyEngine(options = {}) {
  return new PolicyEngine(options);
}

module.exports = {
  PolicyEngine,
  createPolicyEngine,
  TenantPolicyStore,
  PolicyAuditLog,
  RuleRegistry,
};
