'use strict';

// Historical merge reference from AUDIT-019, not verified deployment evidence.
const SPEC137_MERGED_AT = '2026-08-22T12:52:50.000Z';
const PLAN_KINDS = new Set(['plan_approval', 'plan_edit', 'plan_clarification']);

// Deliberately limited to the four patterns in the historical audit. This is
// not a replacement for assertMissionStateConsistent or a causal diagnosis.
const PATTERNS = [
  {
    id: 'stage_pending_stage_mismatch',
    test: (m) => Boolean(m.pendingOperatorDecision?.stage && m.stage
      && m.pendingOperatorDecision.stage !== m.stage),
    message: 'pendingOperatorDecision.stage differs from mission.stage',
  },
  {
    id: 'discovery_approval_off_discover',
    test: (m) => Boolean(m.pendingOperatorDecision?.kind === 'discovery_approval'
      && m.stage && m.stage !== 'discover'),
    message: 'discovery_approval advertised outside discover stage',
  },
  {
    id: 'plan_decision_off_discover',
    test: (m) => Boolean(PLAN_KINDS.has(m.pendingOperatorDecision?.kind)
      && m.stage && m.stage !== 'discover'),
    message: 'plan decision advertised outside discover stage',
  },
  {
    id: 'approved_plan_stale_pending',
    test: (m) => Boolean((m.structuredMission?.immutable || m.structuredMissionApproved)
      && PLAN_KINDS.has(m.pendingOperatorDecision?.kind)),
    message: 'approved plan still advertises a plan decision',
  },
];

function timestampContext(value) {
  if (value == null || value === '') {
    return { status: 'missing', iso: null, relationToSpec137Merge: 'unknown' };
  }
  // No local-time or date-only strings: the comparison requires a timezone.
  const parts = typeof value === 'string'
    && /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/.exec(value);
  let validType = value instanceof Date;
  if (parts) {
    const [, year, month, day, hour, minute, second] = parts.map(Number);
    const leap = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
    const monthDays = [31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    validType = month >= 1 && month <= 12 && day >= 1 && day <= monthDays[month - 1]
      && hour < 24 && minute < 60 && second < 60;
  }
  const time = validType ? new Date(value).getTime() : NaN;
  if (!Number.isFinite(time)) {
    return { status: 'invalid', iso: null, relationToSpec137Merge: 'unknown' };
  }
  const mergeTime = Date.parse(SPEC137_MERGED_AT);
  return {
    status: 'valid',
    iso: new Date(time).toISOString(),
    relationToSpec137Merge: time < mergeTime ? 'before' : time > mergeTime ? 'after' : 'at',
  };
}

function classifyMission(mission) {
  if (!mission || typeof mission !== 'object' || Array.isArray(mission)) {
    throw new TypeError('A mission object is required.');
  }
  const hits = PATTERNS.filter((pattern) => pattern.test(mission))
    .map(({ id, message }) => ({ id, message }));
  return {
    category: hits.length ? 'inconsistent_origin_unknown' : 'no_known_inconsistency',
    origin: 'unknown',
    hits,
    temporalContext: {
      reference: { kind: 'historical_merge', at: SPEC137_MERGED_AT, deploymentVerified: false },
      createdAt: timestampContext(mission.createdAt),
      updatedAt: timestampContext(mission.updatedAt),
    },
  };
}

module.exports = { SPEC137_MERGED_AT, classifyMission, timestampContext };
