'use strict';

/**
 * SPEC-191 — Canonical Mission Identity Resolution.
 *
 * Mission resume compares structured ResolvedObjective identity — never raw English
 * token overlap. Identity = objective geography, target segment, buyer, constraints,
 * and success criteria.
 */

const { asText } = require('./types');

function normalizeRegion(region) {
  return asText(region).toLowerCase().replace(/\s+/g, ' ').trim() || null;
}

function normalizeCityList(cities) {
  return [...new Set((cities || []).map((city) => asText(city).toLowerCase().trim()).filter(Boolean))].sort();
}

function normalizeGeography(geography) {
  if (!geography || typeof geography !== 'object') {
    return { region: null, cities: [] };
  }
  return {
    region: normalizeRegion(geography.region),
    cities: normalizeCityList(geography.cities),
  };
}

function normalizeConstraints(constraints) {
  return [...new Set((constraints || []).map((row) => asText(row).toLowerCase()).filter(Boolean))].sort();
}

function normalizeSuccessCriteria(successCriteria) {
  if (!successCriteria || typeof successCriteria !== 'object') {
    return { type: null, target: null };
  }
  const target =
    successCriteria.target != null
      ? Number(successCriteria.target)
      : successCriteria.recurringClients != null
        ? Number(successCriteria.recurringClients)
        : successCriteria.customers != null
          ? Number(successCriteria.customers)
          : null;
  return {
    type: successCriteria.type ? asText(successCriteria.type) : null,
    target: Number.isFinite(target) ? target : null,
  };
}

/**
 * Build canonical mission identity from a ResolvedObjective.
 * @param {object|null|undefined} resolvedObjective
 * @returns {object|null}
 */
function buildMissionIdentity(resolvedObjective) {
  if (!resolvedObjective || typeof resolvedObjective !== 'object') return null;

  const segmentKey = asText(resolvedObjective.segmentKey || resolvedObjective.market) || null;
  const marketMeta =
    resolvedObjective.marketMeta && typeof resolvedObjective.marketMeta === 'object'
      ? resolvedObjective.marketMeta
      : null;

  return {
    objective: asText(resolvedObjective.objective).replace(/\s+/g, ' ').trim().toLowerCase(),
    geography: normalizeGeography(resolvedObjective.geography),
    targetSegment: segmentKey,
    buyer: marketMeta && marketMeta.buyer ? asText(marketMeta.buyer) : null,
    constraints: normalizeConstraints(resolvedObjective.constraints),
    successCriteria: normalizeSuccessCriteria(resolvedObjective.successCriteria),
  };
}

function arraysEqual(left, right) {
  if (left.length !== right.length) return false;
  for (let i = 0; i < left.length; i += 1) {
    if (left[i] !== right[i]) return false;
  }
  return true;
}

function geographiesMatch(left, right) {
  if (!left && !right) return true;
  if (!left || !right) return false;
  if (left.region && right.region) {
    return left.region === right.region;
  }
  return arraysEqual(left.cities || [], right.cities || []);
}

function successCriteriaMatch(left, right) {
  if (!left && !right) return true;
  if (!left || !right) return false;
  return left.type === right.type && left.target === right.target;
}

/**
 * Compare two canonical mission identities for resume/create decisions.
 * @param {object|null} left
 * @param {object|null} right
 * @returns {boolean}
 */
function missionIdentitiesMatch(left, right) {
  if (!left || !right) return false;
  if (left.targetSegment !== right.targetSegment) return false;
  if (left.buyer !== right.buyer) return false;
  if (!geographiesMatch(left.geography, right.geography)) return false;
  if (!arraysEqual(left.constraints || [], right.constraints || [])) return false;
  if (!successCriteriaMatch(left.successCriteria, right.successCriteria)) return false;
  return true;
}

/**
 * Find a mission whose canonical identity matches the resolved objective.
 * Prefers an active (non-improve) mission when multiple match.
 * @param {object[]} missions
 * @param {object} resolvedObjective
 * @param {(mission: object) => object|null} [resolveLegacyIdentity]
 * @returns {object|null}
 */
function findResumableMissionByIdentity(missions, resolvedObjective, resolveLegacyIdentity) {
  const targetIdentity = buildMissionIdentity(resolvedObjective);
  if (!targetIdentity) return null;

  const identityForMission = (mission) => {
    if (mission.resolvedObjective) return buildMissionIdentity(mission.resolvedObjective);
    if (typeof resolveLegacyIdentity === 'function') return resolveLegacyIdentity(mission);
    return null;
  };

  const active = missions.find((row) => row.stage !== 'improve');
  if (active && missionIdentitiesMatch(identityForMission(active), targetIdentity)) {
    return active;
  }

  return (
    missions.find((row) => missionIdentitiesMatch(identityForMission(row), targetIdentity)) || null
  );
}

module.exports = {
  buildMissionIdentity,
  missionIdentitiesMatch,
  findResumableMissionByIdentity,
  normalizeGeography,
  normalizeConstraints,
  normalizeSuccessCriteria,
};
