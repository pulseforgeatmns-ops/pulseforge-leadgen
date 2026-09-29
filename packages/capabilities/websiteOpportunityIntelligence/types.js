'use strict';

const CAPABILITY_ID = 'website_opportunity_intelligence';
const CAPABILITY_VERSION = '1.0.0';

const EVIDENCE_CLASS = Object.freeze({
  MEASURED: 'MEASURED',
  OBSERVED: 'OBSERVED',
  INFERRED: 'INFERRED',
  UNKNOWN: 'UNKNOWN',
});

const RECOMMENDED_ACTIONS = Object.freeze({
  DO_NOT_PURSUE: 'DO_NOT_PURSUE',
  MONITOR: 'MONITOR',
  AUDIT_WORTH_REVIEWING: 'AUDIT_WORTH_REVIEWING',
  HIGH_VALUE_WEBSITE_OPPORTUNITY: 'HIGH_VALUE_WEBSITE_OPPORTUNITY',
});

const SCORE_COMPONENTS = Object.freeze({
  WEBSITE_DEFICIENCY: 'website_deficiency',
  COMMERCIAL_VALUE: 'commercial_value',
  BUYING_SIGNALS: 'buying_signals',
  CONTACTABILITY: 'contactability',
  PROJECT_ECONOMICS: 'project_economics',
});

const SCORE_MAX = Object.freeze({
  [SCORE_COMPONENTS.WEBSITE_DEFICIENCY]: 25,
  [SCORE_COMPONENTS.COMMERCIAL_VALUE]: 25,
  [SCORE_COMPONENTS.BUYING_SIGNALS]: 20,
  [SCORE_COMPONENTS.CONTACTABILITY]: 15,
  [SCORE_COMPONENTS.PROJECT_ECONOMICS]: 15,
});

const DEFAULT_ECONOMICS_CONFIG = Object.freeze({
  contractFloor: 2500,
  operatorHourlyRate: 50,
  operatorCapacityHours: 40,
  capacityWindowDays: 30,
  defaultDirectCosts: 250,
});

const PROHIBITED_CLAIM_PATTERNS = Object.freeze([
  /\blosing customers\b/i,
  /\blosing \$[\d,]+/i,
  /\bcosting you sales\b/i,
  /\bviolates the ada\b/i,
  /\bgoogle is penalizing\b/i,
  /\bcompetitors are outperforming\b/i,
  /\bconversion rate is poor\b/i,
  /\bcustomers are abandoning\b/i,
]);

const DIAGNOSIS_CLASS = Object.freeze({
  HEALTHY_SITE: 'HEALTHY_SITE',
  TARGETED_REMEDIATION: 'TARGETED_REMEDIATION',
  REDESIGN_CANDIDATE: 'REDESIGN_CANDIDATE',
  INSUFFICIENT_EVIDENCE: 'INSUFFICIENT_EVIDENCE',
});

const ECONOMIC_CONFIDENCE = Object.freeze({
  HIGH: 'HIGH',
  MEDIUM: 'MEDIUM',
  LOW: 'LOW',
  UNKNOWN: 'UNKNOWN',
});

const BUYING_SIGNAL_RESEARCH = Object.freeze({
  RESEARCHED: 'researched',
  NOT_RESEARCHED: 'not_researched',
});

const WEB_EVENT_TYPES = Object.freeze({
  PROSPECT_DISCOVERED: 'WEB_PROSPECT_DISCOVERED',
  AUDIT_STARTED: 'WEB_AUDIT_STARTED',
  AUDIT_COMPLETED: 'WEB_AUDIT_COMPLETED',
  DIAGNOSIS_COMPLETED: 'WEB_DIAGNOSIS_COMPLETED',
  OPPORTUNITY_SCORED: 'WEB_OPPORTUNITY_SCORED',
  ASSESSMENT_CREATED: 'WEB_ASSESSMENT_CREATED',
  PROSPECT_REJECTED: 'WEB_PROSPECT_REJECTED',
  COHORT_COMPLETED: 'WEB_COHORT_COMPLETED',
});

function buildFinding(partial = {}) {
  return {
    id: partial.id || `finding_${Date.now()}`,
    evidence_class: partial.evidence_class || EVIDENCE_CLASS.UNKNOWN,
    category: partial.category || 'general',
    summary: String(partial.summary || ''),
    detail: partial.detail != null ? String(partial.detail) : null,
    source: partial.source || 'unknown',
    observed_at: partial.observed_at || new Date().toISOString(),
    confidence: Number.isFinite(Number(partial.confidence)) ? Number(partial.confidence) : null,
    measurement: partial.measurement ?? null,
    ref: partial.ref || null,
    derived_from: Array.isArray(partial.derived_from) ? partial.derived_from : null,
  };
}

function buildAssessmentOutput(partial = {}) {
  return {
    domain: partial.domain || '',
    audited_at: partial.audited_at || new Date().toISOString(),
    technical_evidence: partial.technical_evidence || {},
    business_evidence: partial.business_evidence || {},
    commercial_diagnosis: partial.commercial_diagnosis || {},
    opportunity_score: partial.opportunity_score ?? null,
    score_components: partial.score_components || {},
    confidence: partial.confidence ?? null,
    recommended_action: partial.recommended_action || RECOMMENDED_ACTIONS.DO_NOT_PURSUE,
    evidence_refs: Array.isArray(partial.evidence_refs) ? partial.evidence_refs : [],
    economics: partial.economics || {},
    assessment: partial.assessment || null,
  };
}

module.exports = {
  CAPABILITY_ID,
  CAPABILITY_VERSION,
  EVIDENCE_CLASS,
  RECOMMENDED_ACTIONS,
  SCORE_COMPONENTS,
  SCORE_MAX,
  DEFAULT_ECONOMICS_CONFIG,
  PROHIBITED_CLAIM_PATTERNS,
  DIAGNOSIS_CLASS,
  ECONOMIC_CONFIDENCE,
  BUYING_SIGNAL_RESEARCH,
  WEB_EVENT_TYPES,
  buildFinding,
  buildAssessmentOutput,
};
