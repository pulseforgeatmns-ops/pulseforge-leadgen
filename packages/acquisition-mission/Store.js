'use strict';

/**
 * SPEC-118 — in-memory store. Tenant-scoped. Cross-tenant reads fail closed.
 */

const { clone, asText, nowIso, amoError } = require('./types');

function createMemoryAmoStore(opts = {}) {
  const missions = new Map();
  const events = [];
  const contributions = [];
  const observations = [];
  const outcomes = [];
  const learning = [];

  function putMission(mission) {
    const copy = clone(mission);
    copy.updatedAt = copy.updatedAt || nowIso();
    missions.set(copy.id, copy);
    return clone(copy);
  }

  function getMission(id) {
    const found = missions.get(asText(id));
    return found ? clone(found) : null;
  }

  function requireMission(id) {
    const mission = getMission(id);
    if (!mission) throw amoError('amo_mission_not_found', `Mission not found: ${id}`);
    return mission;
  }

  function listMissions(tenantId) {
    const rows = [...missions.values()].map(clone);
    if (tenantId == null || tenantId === '') return rows;
    const key = String(tenantId);
    return rows
      .filter((row) => String(row.tenantId || row.clientId || '') === key)
      .sort((a, b) => String(b.updatedAt).localeCompare(String(a.updatedAt)));
  }

  function addEvent(row) {
    if (row && row.id && events.some((existing) => existing.id === row.id)) {
      return clone(row);
    }
    events.push(clone(row));
    return clone(row);
  }

  function listEvents(missionId) {
    return events
      .filter((row) => row.missionId === missionId)
      .sort((a, b) => String(a.at).localeCompare(String(b.at)))
      .map(clone);
  }

  function addContribution(row) {
    if (row && row.id && contributions.some((existing) => existing.id === row.id)) {
      return clone(row);
    }
    contributions.push(clone(row));
    return clone(row);
  }

  function listContributions(missionId) {
    return contributions.filter((row) => row.missionId === missionId).map(clone);
  }

  function addObservation(row) {
    if (row && row.id && observations.some((existing) => existing.id === row.id)) {
      return clone(row);
    }
    observations.push(clone(row));
    return clone(row);
  }

  function listObservations(missionId) {
    return observations.filter((row) => row.missionId === missionId).map(clone);
  }

  function addOutcome(row) {
    if (row && row.id && outcomes.some((existing) => existing.id === row.id)) {
      return clone(row);
    }
    outcomes.push(clone(row));
    return clone(row);
  }

  function listOutcomes(missionId) {
    return outcomes.filter((row) => row.missionId === missionId).map(clone);
  }

  function addLearning(row) {
    if (row && row.id && learning.some((existing) => existing.id === row.id)) {
      return clone(row);
    }
    learning.push(clone(row));
    return clone(row);
  }

  function listLearning(tenantId) {
    if (tenantId == null || tenantId === '') return learning.map(clone);
    const key = String(tenantId);
    return learning.filter((row) => String(row.tenantId || '') === key).map(clone);
  }

  for (const extra of opts.seeds || []) putMission(extra);

  return {
    putMission,
    getMission,
    requireMission,
    listMissions,
    addEvent,
    listEvents,
    addContribution,
    listContributions,
    addObservation,
    listObservations,
    addOutcome,
    listOutcomes,
    addLearning,
    listLearning,
  };
}

module.exports = {
  createMemoryAmoStore,
};
