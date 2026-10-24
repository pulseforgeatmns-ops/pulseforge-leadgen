'use strict';

/**
 * SPEC-146 — Evidence Conflict Resolution types.
 * ADR-065 — Conflicting Evidence Is Intelligence.
 */

const CONFLICT_CATEGORIES = Object.freeze({
  TEMPORAL: 'temporal',
  SOURCE_AUTHORITY: 'source_authority',
  OBSERVATION: 'observation',
  GENUINE_UNKNOWN: 'genuine_unknown',
});

const CONFLICT_SEVERITY = Object.freeze({
  LOW: 'low',
  MEDIUM: 'medium',
  HIGH: 'high',
  CRITICAL: 'critical',
});

const RESOLUTION_STRATEGIES = Object.freeze({
  FRESHNESS: 'freshness',
  AUTHORITY: 'authority',
  MAJORITY: 'majority',
  CONTEXT: 'context',
  OPERATOR_ESCALATION: 'operator_escalation',
});

const CONFLICT_SUBJECTS = Object.freeze({
  EMPLOYEE_COUNT: 'employee_count',
  PROPERTY_COUNT: 'property_count',
  LISTING_COUNT: 'listing_count',
  OWNERSHIP: 'ownership',
  ADDRESS: 'address',
  PHONE: 'phone',
  REVENUE: 'revenue_estimate',
  SERVICE_AREA: 'service_area',
  DECISION_MAKER: 'decision_maker',
  OPERATING_STATUS: 'operating_status',
  COMPANY_SIZE: 'company_size',
});

function round2(n) {
  return Number(Number(n).toFixed(2));
}

/**
 * Build an EvidenceConflict object.
 * @param {object} partial
 * @returns {object}
 */
function buildEvidenceConflict(partial = {}) {
  const resolution = partial.resolution || {};
  return {
    id: partial.id || `conflict:${Date.now()}`,
    subject: partial.subject || 'unknown',
    entityId: partial.entityId || null,
    conflictingClaims: Array.isArray(partial.conflictingClaims) ? partial.conflictingClaims : [],
    providers: Array.isArray(partial.providers) ? partial.providers : [],
    category: partial.category || CONFLICT_CATEGORIES.GENUINE_UNKNOWN,
    severity: partial.severity || CONFLICT_SEVERITY.MEDIUM,
    confidence: partial.confidence != null ? round2(partial.confidence) : 0,
    resolution: {
      strategy: resolution.strategy || null,
      workingEstimate: resolution.workingEstimate || null,
      reason: resolution.reason || null,
      resolved: resolution.resolved === true,
    },
    unresolvedReason: partial.unresolvedReason || null,
    confidencePenalty: partial.confidencePenalty != null ? partial.confidencePenalty : 0.12,
    description: partial.description || null,
  };
}

/**
 * Build a conflict resolution result bundle.
 * @param {object} partial
 * @returns {object}
 */
function buildConflictResolutionResult(partial = {}) {
  const conflicts = Array.isArray(partial.conflicts) ? partial.conflicts : [];
  const resolved = conflicts.filter((c) => c.resolution?.resolved);
  const outstanding = conflicts.filter((c) => !c.resolution?.resolved);

  return {
    version: 'SPEC-146',
    entityId: partial.entityId || null,
    conflicts,
    detected: conflicts.length,
    resolved: resolved.length,
    outstanding: outstanding.length,
    confidenceAdjustment: partial.confidenceAdjustment != null ? partial.confidenceAdjustment : 0,
    recommendedProviders: partial.recommendedProviders || [],
    investigationTasks: partial.investigationTasks || [],
  };
}

module.exports = {
  CONFLICT_CATEGORIES,
  CONFLICT_SEVERITY,
  RESOLUTION_STRATEGIES,
  CONFLICT_SUBJECTS,
  buildEvidenceConflict,
  buildConflictResolutionResult,
};
