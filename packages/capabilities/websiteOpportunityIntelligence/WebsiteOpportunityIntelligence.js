'use strict';

const {
  CAPABILITY_CATEGORIES,
  buildCapabilityResult,
  buildCapabilityEstimate,
  CAPABILITY_RESULT_STATUS,
  PROGRESS_KINDS,
} = require('../types');
const {
  CAPABILITY_ID,
  CAPABILITY_VERSION,
  WEB_EVENT_TYPES,
} = require('./types');
const { runDeterministicAudit, normalizeDomain } = require('./audit/deterministicAudit');
const { gatherBusinessEvidence } = require('./businessEvidence');
const { computeOpportunityScore, recommendAction } = require('./scoring');
const { computeProjectEconomics } = require('./economics');
const { buildCommercialDiagnosis } = require('./diagnosis');
const { buildWebsiteOpportunityAssessment } = require('./assessment');
const { mergeFindings } = require('./evidence');
const { emitWebEvent } = require('./observability');

function createWebsiteOpportunityIntelligenceCapability(deps = {}) {
  const auditFn = deps.runDeterministicAudit || runDeterministicAudit;

  return {
    id: CAPABILITY_ID,
    version: CAPABILITY_VERSION,
    name: 'Website Opportunity Intelligence',
    description:
      'Deterministic website audit + business evidence + commercial diagnosis for website redesign opportunities',
    category: CAPABILITY_CATEGORIES.INTELLIGENCE,
    outcomeTags: [
      'website_opportunity_assessed',
      'website_audit_complete',
      'commercial_diagnosis_ready',
    ],
    produces: ['website_opportunity_assessment'],
    retryable: true,
    timeoutMs: 120_000,
    supportsRollback: false,
    idempotent: true,
    enabled: true,

    canRun(context) {
      const domain = resolveDomain(context);
      return Boolean(domain);
    },

    estimate(context) {
      return buildCapabilityEstimate({
        durationMs: 8000,
        confidence: resolveDomain(context) ? 0.85 : 0.2,
        notes: ['Read-only deterministic audit — no outbound side effects'],
      });
    },

    async execute(context, runtime = {}) {
      const started = Date.now();
      const emit = (stage, pct) => {
        if (typeof runtime.onProgress === 'function') {
          runtime.onProgress({ kind: PROGRESS_KINDS.PROGRESS, stage, percent: pct, message: stage });
        }
      };

      const input = context.inputs || {};
      const businessInput = input.business || input.prospect || input;
      const domain = resolveDomain(context);
      if (!domain) {
        return buildCapabilityResult({
          status: CAPABILITY_RESULT_STATUS.FAILED,
          errors: [{ message: 'domain required' }],
          duration: Date.now() - started,
        });
      }

      const eventCtx = {
        client_id: context.clientId || context.tenantId,
        tenant_id: context.tenantId || context.clientId,
        mission_id: input.mission_id || context.missionId,
        prospect_id: input.prospect_id || businessInput.prospect_id,
        domain,
        pool: deps.pool,
        onEvent: deps.onEvent,
      };

      await emitWebEvent(WEB_EVENT_TYPES.PROSPECT_DISCOVERED, eventCtx, deps);
      emit('Running deterministic audit', 15);
      await emitWebEvent(WEB_EVENT_TYPES.AUDIT_STARTED, eventCtx, deps);

      const audit = await auditFn(domain, {
        ...deps.audit,
        skipPuppeteer: input.skipPuppeteer ?? deps.skipPuppeteer,
        skipPageSpeed: input.skipPageSpeed ?? deps.skipPageSpeed,
        fixtureAudit: deps.fixtureAudit,
      });

      await emitWebEvent(WEB_EVENT_TYPES.AUDIT_COMPLETED, {
        ...eventCtx,
        evidence_counts: { total: audit.findings?.length || 0 },
      }, deps);

      emit('Gathering business evidence', 35);
      const { business, findings: businessFindings } = gatherBusinessEvidence({
        ...businessInput,
        domain: audit.domain,
      });

      const findings = mergeFindings(audit.findings, businessFindings);
      emit('Computing economics', 55);
      const economics = computeProjectEconomics({
        findings,
        business,
        config: input.economicsConfig || deps.economicsConfig,
      });

      emit('Scoring opportunity', 70);
      const scoring = computeOpportunityScore({ findings, business, economics, audit });
      const recommendation = recommendAction({
        opportunity_score: scoring.opportunity_score,
        score_components: scoring.score_components,
        confidence: scoring.confidence,
        deficiency_only_risk: scoring.deficiency_only_risk,
        economics,
      });

      emit('Building commercial diagnosis', 85);
      const commercial_diagnosis = buildCommercialDiagnosis({
        findings,
        business,
        economics,
        score: scoring,
      });

      await emitWebEvent(WEB_EVENT_TYPES.DIAGNOSIS_COMPLETED, eventCtx, deps);
      await emitWebEvent(WEB_EVENT_TYPES.OPPORTUNITY_SCORED, {
        ...eventCtx,
        score: scoring.opportunity_score,
        confidence: scoring.confidence,
        recommended_action: recommendation.action,
      }, deps);

      const result = buildWebsiteOpportunityAssessment({
        business,
        audit,
        businessFindings,
        findings,
        commercial_diagnosis,
        scoring,
        economics,
        recommendation,
      });

      if (recommendation.action === 'DO_NOT_PURSUE') {
        await emitWebEvent(WEB_EVENT_TYPES.PROSPECT_REJECTED, {
          ...eventCtx,
          recommended_action: recommendation.action,
          payload: { why: recommendation.why },
        }, deps);
      }

      await emitWebEvent(WEB_EVENT_TYPES.ASSESSMENT_CREATED, {
        ...eventCtx,
        score: scoring.opportunity_score,
        confidence: scoring.confidence,
        recommended_action: recommendation.action,
        evidence_counts: { total: findings.length },
      }, deps);

      emit('Complete', 100);

      return buildCapabilityResult({
        status: CAPABILITY_RESULT_STATUS.COMPLETED,
        outputs: result,
        evidence: findings,
        artifacts: [{ type: 'website_opportunity_assessment', payload: result.assessment }],
        duration: Date.now() - started,
        warnings: economics.capacity_warning ? [economics.capacity_warning] : [],
      });
    },
  };
}

function resolveDomain(context) {
  const input = context?.inputs || {};
  const raw =
    input.domain ||
    input.url ||
    input.business?.domain ||
    input.prospect?.domain ||
    input.prospect?.url ||
    null;
  return normalizeDomain(raw);
}

module.exports = {
  createWebsiteOpportunityIntelligenceCapability,
  CAPABILITY_ID,
};
