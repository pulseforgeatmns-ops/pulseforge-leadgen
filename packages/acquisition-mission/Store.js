'use strict';

/**
 * SPEC-118 — in-memory store. Tenant-scoped. Cross-tenant reads fail closed.
 */

const { clone, asText, nowIso, amoError } = require('./types');
const { assertMissionStateConsistent } = require('./PendingOperatorDecision');

function createMemoryAmoStore(opts = {}) {
  const missions = new Map();
  const events = [];
  const contributions = [];
  const observations = [];
  const outcomes = [];
  const learning = [];
  const predictions = [];
  const evaluations = [];
  const outcomeLearnings = [];
  const executionRecords = [];
  const interpretations = [];

  function putMission(mission) {
    const missionContributions = contributions.filter((row) => row.missionId === mission.id);
    assertMissionStateConsistent(mission, { contributions: missionContributions });
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

  function updateContribution(id, updater) {
    const index = contributions.findIndex((row) => row.id === id);
    if (index < 0) return null;
    const current = clone(contributions[index]);
    const updated = typeof updater === 'function' ? updater(current) : { ...current, ...updater };
    contributions[index] = clone(updated);
    return clone(updated);
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

  function addPrediction(row) {
    if (row && row.id && predictions.some((existing) => existing.id === row.id)) {
      return clone(row);
    }
    predictions.push(clone(row));
    return clone(row);
  }

  function listPredictions(missionId) {
    if (missionId == null || missionId === '') return predictions.map(clone);
    return predictions.filter((row) => row.missionId === missionId).map(clone);
  }

  function putPrediction(row) {
    const idx = predictions.findIndex((existing) => existing.id === row.id);
    if (idx >= 0) predictions[idx] = clone(row);
    else predictions.push(clone(row));
    return clone(row);
  }

  function addEvaluation(row) {
    if (row && row.id && evaluations.some((existing) => existing.id === row.id)) {
      return clone(row);
    }
    evaluations.push(clone(row));
    return clone(row);
  }

  function listEvaluations(missionId) {
    if (missionId == null || missionId === '') return evaluations.map(clone);
    return evaluations.filter((row) => row.missionId === missionId).map(clone);
  }

  function addOutcomeLearning(row) {
    if (row && row.id && outcomeLearnings.some((existing) => existing.id === row.id)) {
      return clone(row);
    }
    outcomeLearnings.push(clone(row));
    return clone(row);
  }

  function listOutcomeLearnings(tenantId, missionId = null) {
    let rows = outcomeLearnings.map(clone);
    if (tenantId != null && tenantId !== '') {
      const key = String(tenantId);
      rows = rows.filter((row) => String(row.tenantId || '') === key);
    }
    if (missionId != null && missionId !== '') {
      rows = rows.filter((row) => row.missionId === missionId);
    }
    return rows;
  }

  function addExecutionRecord(row) {
    if (!row || !row.id) return null;
    const idx = executionRecords.findIndex((existing) => existing.id === row.id);
    const copy = clone(row);
    if (idx >= 0) executionRecords[idx] = copy;
    else executionRecords.push(copy);
    return clone(copy);
  }

  function listExecutionRecords(missionId, filter = {}) {
    let rows = executionRecords.map(clone);
    if (missionId != null && missionId !== '') {
      rows = rows.filter((row) => row.missionId === missionId);
    }
    if (filter.prospectId) {
      rows = rows.filter((row) => row.prospectId === filter.prospectId);
    }
    if (filter.executionIdentity) {
      rows = rows.filter((row) => row.executionIdentity === filter.executionIdentity);
    }
    if (filter.status) {
      rows = rows.filter((row) => row.status === filter.status);
    }
    return rows.sort((a, b) => String(a.attemptedAt || a.createdAt).localeCompare(String(b.attemptedAt || b.createdAt)));
  }

  function findExecutionRecordByIdentity(executionIdentity) {
    return executionRecords.find((row) => row.executionIdentity === executionIdentity) || null;
  }

  function addInterpretation(row) {
    if (row && row.id && interpretations.some((existing) => existing.id === row.id)) {
      return clone(row);
    }
    interpretations.push(clone(row));
    return clone(row);
  }

  function listInterpretations(missionId) {
    return interpretations.filter((row) => row.missionId === missionId).map(clone);
  }

  function snapshot() {
    return {
      missions: [...missions.entries()].map(([id, row]) => [id, clone(row)]),
      events: events.map(clone),
      contributions: contributions.map(clone),
      observations: observations.map(clone),
      outcomes: outcomes.map(clone),
      learning: learning.map(clone),
      predictions: predictions.map(clone),
      evaluations: evaluations.map(clone),
      outcomeLearnings: outcomeLearnings.map(clone),
      executionRecords: executionRecords.map(clone),
      interpretations: interpretations.map(clone),
    };
  }

  function replaceArray(target, source) {
    target.length = 0;
    for (const row of source || []) target.push(clone(row));
  }

  function restore(snap) {
    if (!snap) return;
    missions.clear();
    for (const [id, row] of snap.missions || []) {
      missions.set(id, clone(row));
    }
    replaceArray(events, snap.events);
    replaceArray(contributions, snap.contributions);
    replaceArray(observations, snap.observations);
    replaceArray(outcomes, snap.outcomes);
    replaceArray(learning, snap.learning);
    replaceArray(predictions, snap.predictions);
    replaceArray(evaluations, snap.evaluations);
    replaceArray(outcomeLearnings, snap.outcomeLearnings);
    replaceArray(executionRecords, snap.executionRecords);
    replaceArray(interpretations, snap.interpretations);
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
    updateContribution,
    listContributions,
    addObservation,
    listObservations,
    addOutcome,
    listOutcomes,
    addLearning,
    listLearning,
    addPrediction,
    listPredictions,
    putPrediction,
    addEvaluation,
    listEvaluations,
    addOutcomeLearning,
    listOutcomeLearnings,
    addExecutionRecord,
    listExecutionRecords,
    findExecutionRecordByIdentity,
    addInterpretation,
    listInterpretations,
    snapshot,
    restore,
  };
}

module.exports = {
  createMemoryAmoStore,
};
