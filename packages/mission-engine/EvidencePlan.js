'use strict';

/**
 * EvidencePlan — required vs available vs missing evidence
 * (SPEC-056 / ADR-040).
 *
 * EvidencePlan is descriptive of information needs.
 * MissionPlan remains the executable capability graph.
 */

const EVIDENCE_PLAN_VERSION = '1.0.0';

/** Canonical evidence / diagnostic artifact type names (PascalCase). */
const EVIDENCE_TYPES = Object.freeze({
  DISCOVERY_EXECUTION: 'DiscoveryExecution',
  DISCOVERY_TRACE: 'DiscoveryTrace',
  DISCOVERY_DIAGNOSTICS: 'DiscoveryDiagnostics',
  CAPABILITY_EXECUTION: 'CapabilityExecution',
  CAPABILITY_FAILURE: 'CapabilityFailure',
  MISSION_DIAGNOSTICS: 'MissionDiagnostics',
  MISSION_STATE: 'MissionState',
  PROVIDER_SELECTION: 'ProviderSelection',
  CANDIDATE_COUNTS: 'CandidateCounts',
  VERIFICATION_RESULTS: 'VerificationResults',
  EXCEPTIONS: 'Exceptions',
});

/** Read-only diagnostic evidence — never mutates business state. */
const DIAGNOSTIC_EVIDENCE_TYPES = Object.freeze(
  new Set(Object.values(EVIDENCE_TYPES))
);

/**
 * @param {object} [partial]
 * @returns {object} evidence_plan (frozen)
 */
function buildEvidencePlan(partial = {}) {
  const required = normalizeTypeList(partial.required);
  const available = normalizeTypeList(partial.available);
  const missing = normalizeTypeList(
    partial.missing != null
      ? partial.missing
      : required.filter((t) => !available.includes(t))
  );
  const acquired = normalizeTypeList(partial.acquired);
  const blocked = normalizeBlocked(partial.blocked);
  const acquisitions = normalizeAcquisitions(partial.acquisitions);
  const satisfied = required.filter(
    (t) => available.includes(t) || acquired.includes(t)
  );

  const unableToAnswer =
    partial.unableToAnswer != null
      ? Boolean(partial.unableToAnswer)
      : blocked.length > 0 &&
        missing.some((t) => blocked.some((b) => b.evidenceType === t));

  return Object.freeze({
    version: partial.version || EVIDENCE_PLAN_VERSION,
    required: Object.freeze(required),
    available: Object.freeze(available),
    missing: Object.freeze(missing),
    acquired: Object.freeze(acquired),
    satisfied: Object.freeze(satisfied),
    acquisitions: Object.freeze(acquisitions),
    blocked: Object.freeze(blocked),
    satisfiedCount: satisfied.length,
    missingCount: missing.length,
    unableToAnswer,
    reason: partial.reason
      ? String(partial.reason)
      : unableToAnswer
        ? formatUnableReason(blocked, missing)
        : null,
    intentCategory: partial.intentCategory || null,
    goal: partial.goal ? String(partial.goal) : null,
    createdAt: partial.createdAt || new Date().toISOString(),
  });
}

/**
 * Operator-facing summary for Review Workspace (SPEC-056).
 * @param {object} plan
 * @returns {object}
 */
function summarizeEvidencePlan(plan) {
  const p =
    plan && typeof plan === 'object' ? plan : buildEvidencePlan({});
  const statusByType = new Map();

  for (const t of p.required || []) {
    statusByType.set(t, {
      evidenceType: t,
      status: 'required',
      label: t,
      reason: null,
    });
  }
  for (const t of p.available || []) {
    statusByType.set(t, {
      evidenceType: t,
      status: 'available',
      label: t,
      reason: null,
    });
  }
  for (const t of p.acquired || []) {
    if ((p.available || []).includes(t)) continue;
    statusByType.set(t, {
      evidenceType: t,
      status: 'acquired',
      label: t,
      reason: null,
    });
  }
  for (const a of p.acquisitions || []) {
    const t = a.evidenceType;
    if (!t) continue;
    if ((p.available || []).includes(t)) continue;
    statusByType.set(t, {
      evidenceType: t,
      status: 'scheduled',
      label: t,
      reason: a.capabilityId
        ? `Acquire via ${a.label || a.capabilityId}`
        : null,
      capabilityId: a.capabilityId || null,
      stageId: a.stageId || null,
    });
  }
  for (const b of p.blocked || []) {
    statusByType.set(b.evidenceType, {
      evidenceType: b.evidenceType,
      status: 'blocked',
      label: b.evidenceType,
      reason: b.reason || 'No producer registered',
    });
  }

  // Preserve required order
  const items = (p.required || []).map(
    (t) =>
      statusByType.get(t) || {
        evidenceType: t,
        status: 'required',
        label: t,
        reason: null,
      }
  );

  return {
    goal: p.goal || null,
    intentCategory: p.intentCategory || null,
    satisfiedCount: p.satisfiedCount != null ? p.satisfiedCount : 0,
    missingCount: p.missingCount != null ? p.missingCount : 0,
    unableToAnswer: Boolean(p.unableToAnswer),
    reason: p.reason || null,
    items,
    available: [...(p.available || [])],
    missing: [...(p.missing || [])],
    acquired: [...(p.acquired || [])],
    blocked: (p.blocked || []).map((b) => ({
      evidenceType: b.evidenceType,
      reason: b.reason,
    })),
  };
}

function isDiagnosticEvidenceType(type) {
  return DIAGNOSTIC_EVIDENCE_TYPES.has(String(type || ''));
}

function normalizeTypeList(list) {
  if (!Array.isArray(list)) return [];
  const seen = new Set();
  const out = [];
  for (const item of list) {
    const t = String(item || '').trim();
    if (!t || seen.has(t)) continue;
    seen.add(t);
    out.push(t);
  }
  return out;
}

function normalizeBlocked(list) {
  if (!Array.isArray(list)) return [];
  return list
    .filter((b) => b && b.evidenceType)
    .map((b) =>
      Object.freeze({
        evidenceType: String(b.evidenceType),
        reason: String(b.reason || 'No producer registered'),
        capabilityId: b.capabilityId || null,
      })
    );
}

function normalizeAcquisitions(list) {
  if (!Array.isArray(list)) return [];
  return list
    .filter((a) => a && a.evidenceType)
    .map((a) =>
      Object.freeze({
        evidenceType: String(a.evidenceType),
        capabilityId: a.capabilityId || null,
        stageId: a.stageId || null,
        label: a.label || null,
        strategy: a.strategy || 'diagnostic_capability',
      })
    );
}

function formatUnableReason(blocked, missing) {
  const parts = [];
  for (const b of blocked || []) {
    parts.push(`${b.evidenceType}: ${b.reason}`);
  }
  if (!parts.length && missing && missing.length) {
    parts.push(`Missing evidence: ${missing.join(', ')}`);
  }
  return parts.length
    ? `Unable to answer. ${parts.join('; ')}`
    : 'Unable to answer.';
}

module.exports = {
  EVIDENCE_PLAN_VERSION,
  EVIDENCE_TYPES,
  DIAGNOSTIC_EVIDENCE_TYPES,
  buildEvidencePlan,
  summarizeEvidencePlan,
  isDiagnosticEvidenceType,
};
