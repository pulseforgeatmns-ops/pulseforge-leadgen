'use strict';

/**
 * SPEC-JEV-005 — Canonical mission resolution for named inspection queries.
 * When the operator names a mission, resolve the canonical record before falling
 * back to active/daily wrapper state.
 */

const { isExplicitStrPrimaryTarget } = require('./MissionNaming');

/** Known canonical Anchor STR mission id (tenant 10). */
const ANCHOR_STR_CANONICAL_MISSION_ID = 'mission_82e8102f-249c-4f44-b88e-2de76b13898e';

const ANCHOR_STR_OBJECTIVE_HINT =
  'Acquire one recurring commercial cleaning client from a short-term rental operator in Greater Manchester area.';

const NAMED_MISSION_PATTERNS = Object.freeze([
  {
    label: 'Anchor STR mission',
    re: /\banchor\s+str\b|\banchor\s+short[- ]term\s+rental\b/i,
    objectiveMatch: isAnchorStrObjective,
    canonicalMissionId: ANCHOR_STR_CANONICAL_MISSION_ID,
  },
  {
    label: 'STR mission',
    re: /\bstr\s+mission\b|\bshort[- ]term\s+rental\s+mission\b/i,
    objectiveMatch: isAnchorStrObjective,
    canonicalMissionId: ANCHOR_STR_CANONICAL_MISSION_ID,
  },
]);

function asText(value) {
  return value == null ? '' : String(value).trim();
}

function normalizeObjective(text) {
  return asText(text).toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
}

function isLawFirmObjective(text) {
  return /\blaw firms?\b/.test(normalizeObjective(text));
}

function isAnchorStrObjective(objective) {
  const n = normalizeObjective(objective);
  if (!n) return false;
  if (isLawFirmObjective(n)) return false;
  const hasCleaning = /commercial cleaning/.test(n);
  const hasStr =
    /short term rental/.test(n) ||
    /\bstr operators?\b/.test(n) ||
    isExplicitStrPrimaryTarget(objective);
  const hasGeo = /manchester/.test(n);
  return hasCleaning && hasStr && hasGeo;
}

function isDailyWrapperMission(mission) {
  const id = mission && mission.id ? String(mission.id) : '';
  return id.startsWith('mission_daily_');
}

function detectNamedMissionRequest(operatorMessage) {
  const text = asText(operatorMessage);
  if (!text) return null;
  for (const pattern of NAMED_MISSION_PATTERNS) {
    if (pattern.re.test(text)) {
      return {
        requested_mission_label: pattern.label,
        objectiveMatch: pattern.objectiveMatch,
        canonical_mission_id: pattern.canonicalMissionId,
      };
    }
  }
  if (
    /\banchor\b/i.test(text) &&
    /\b(?:recurring commercial cleaning|str operator|short[- ]term rental)\b/i.test(text)
  ) {
    return {
      requested_mission_label: 'Anchor STR mission',
      objectiveMatch: isAnchorStrObjective,
      canonical_mission_id: ANCHOR_STR_CANONICAL_MISSION_ID,
    };
  }
  return null;
}

function findMissionById(missions, missionId, getMission) {
  if (!missionId) return null;
  const fromList = (missions || []).find((row) => row.id === missionId);
  if (fromList) return fromList;
  if (typeof getMission === 'function') {
    try {
      return getMission(missionId) || null;
    } catch {
      return null;
    }
  }
  return null;
}

function rankObjectiveMatches(candidates, objectiveMatch) {
  return (candidates || [])
    .filter((row) => row && objectiveMatch(row.objective || row.title || ''))
    .slice()
    .sort((a, b) => {
      const aCanon = a.id === ANCHOR_STR_CANONICAL_MISSION_ID ? 100 : 0;
      const bCanon = b.id === ANCHOR_STR_CANONICAL_MISSION_ID ? 100 : 0;
      if (aCanon !== bCanon) return bCanon - aCanon;
      const aDaily = isDailyWrapperMission(a) ? -50 : 0;
      const bDaily = isDailyWrapperMission(b) ? -50 : 0;
      return bDaily - aDaily;
    });
}

/**
 * Resolve which mission record to inspect for a named or active query.
 * @param {object} input
 * @returns {object}
 */
function resolveInspectionMission(input = {}) {
  const {
    tenantId = null,
    operatorMessage = '',
    activeMission = null,
    candidateMissions = [],
    objectiveMemory = null,
    getMission = null,
  } = input;

  const warnings = [];
  const named = detectNamedMissionRequest(operatorMessage);
  const missions = Array.isArray(candidateMissions) ? candidateMissions : [];

  if (!named) {
    if (activeMission) {
      return {
        mission: activeMission,
        resolution_type: 'active_mission',
        requested_mission_label: null,
        canonical_mission_id: null,
        tenant_id: tenantId,
        warnings,
      };
    }
    const first = missions[0] || null;
    if (first && isDailyWrapperMission(first)) {
      return {
        mission: first,
        resolution_type: 'daily_wrapper',
        requested_mission_label: null,
        canonical_mission_id: null,
        tenant_id: tenantId,
        warnings,
      };
    }
    return {
      mission: first,
      resolution_type: first ? 'active_mission' : 'not_found',
      requested_mission_label: null,
      canonical_mission_id: null,
      tenant_id: tenantId,
      warnings,
    };
  }

  const { requested_mission_label, objectiveMatch, canonical_mission_id } = named;

  const canonical = findMissionById(missions, canonical_mission_id, getMission);
  if (canonical && objectiveMatch(canonical.objective || '')) {
    return {
      mission: canonical,
      resolution_type: 'named_exact',
      requested_mission_label,
      canonical_mission_id,
      tenant_id: tenantId,
      warnings,
    };
  }

  const objectiveMatches = rankObjectiveMatches(missions, objectiveMatch);
  if (objectiveMatches.length) {
    const best = objectiveMatches[0];
    return {
      mission: best,
      resolution_type: isDailyWrapperMission(best) ? 'daily_wrapper' : 'named_objective_match',
      requested_mission_label,
      canonical_mission_id,
      tenant_id: tenantId,
      warnings: isDailyWrapperMission(best)
        ? [
            {
              code: 'named_mission_resolved_to_daily_wrapper',
              severity: 'warning',
              message:
                'Named mission request resolved to a daily/watch mission instead of the canonical record.',
            },
          ]
        : warnings,
    };
  }

  if (canonical) {
    warnings.push({
      code: 'canonical_id_objective_mismatch',
      severity: 'warning',
      message: 'Canonical mission id exists but objective no longer matches the named request.',
    });
    return {
      mission: canonical,
      resolution_type: 'named_exact',
      requested_mission_label,
      canonical_mission_id,
      tenant_id: tenantId,
      warnings,
    };
  }

  const dailyWrapper = missions.find(isDailyWrapperMission) || null;
  if (dailyWrapper) {
    warnings.push({
      code: 'named_mission_not_found_daily_fallback',
      severity: 'warning',
      message:
        'Could not find the original named mission record; only a daily/watch mission is available.',
    });
    return {
      mission: dailyWrapper,
      resolution_type: 'daily_wrapper',
      requested_mission_label,
      canonical_mission_id,
      tenant_id: tenantId,
      warnings,
    };
  }

  if (activeMission && !isDailyWrapperMission(activeMission)) {
    warnings.push({
      code: 'named_mission_not_found_active_fallback',
      severity: 'warning',
      message: 'Could not find the named mission; falling back to active mission.',
    });
    return {
      mission: activeMission,
      resolution_type: 'active_mission',
      requested_mission_label,
      canonical_mission_id,
      tenant_id: tenantId,
      warnings,
    };
  }

  return {
    mission: null,
    resolution_type: 'not_found',
    requested_mission_label,
    canonical_mission_id,
    tenant_id: tenantId,
    warnings: [
      {
        code: 'named_mission_not_found',
        severity: 'warning',
        message: `Could not find mission record for ${requested_mission_label}.`,
      },
    ],
  };
}

module.exports = {
  ANCHOR_STR_CANONICAL_MISSION_ID,
  ANCHOR_STR_OBJECTIVE_HINT,
  detectNamedMissionRequest,
  isAnchorStrObjective,
  isDailyWrapperMission,
  resolveInspectionMission,
};
