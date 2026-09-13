'use strict';

/**
 * Canonical active Emmett CAPACITY selection for read-only probes and audits.
 * 1. Exclude superseded contributions.
 * 2. Prefer mission revision / pending executionReview pointer when present.
 * 3. Otherwise newest non-superseded row by durable contribution.at.
 */

function unwrapMissionPayload(rowOrPayload) {
  if (!rowOrPayload || typeof rowOrPayload !== 'object') return {};
  if (rowOrPayload.payload && typeof rowOrPayload.payload === 'object' && rowOrPayload.payload.objective) {
    return rowOrPayload.payload;
  }
  return rowOrPayload;
}

function isSupersededContribution(storedRow) {
  const body = storedRow?.payload ?? storedRow;
  if (!body || typeof body !== 'object') return false;
  if (body.superseded === true) return true;
  if (body.payload?.superseded === true) return true;
  return false;
}

function activeCapacityPointer(missionBody = {}) {
  const revisionId = missionBody.revisionState?.emmettContributionId;
  if (revisionId) return String(revisionId);
  const pendingId = missionBody.pendingOperatorDecision?.executionReview
    ?.artifactBinding?.emmettContributionId;
  if (pendingId) return String(pendingId);
  return null;
}

function selectActiveCapacityContribution(missionBody, capacityRows = []) {
  const active = capacityRows.filter((row) => !isSupersededContribution(row));
  if (!active.length) return null;

  const pointer = activeCapacityPointer(missionBody);
  if (pointer) {
    const bound = active.find((row) => String(row.capacity_id || row.id) === pointer);
    if (bound) return bound;
  }

  return [...active].sort((a, b) => {
    const byAt = new Date(b.at).getTime() - new Date(a.at).getTime();
    if (byAt !== 0) return byAt;
    return String(a.capacity_id || a.id).localeCompare(String(b.capacity_id || b.id));
  })[0];
}

async function loadCapacityRowsForMission(db, tenantId, missionId) {
  const { rows } = await db.query(
    `SELECT id AS capacity_id, payload, at
       FROM acquisition_mission_contributions
      WHERE tenant_id = $1
        AND mission_id = $2
        AND specialist = 'emmett'
        AND kind = 'capacity'`,
    [tenantId, missionId]
  );
  return rows;
}

async function loadActiveCapacityForMission(db, tenantId, missionId) {
  const missionResult = await db.query(
    `SELECT id AS mission_id, payload, updated_at
       FROM acquisition_missions
      WHERE tenant_id = $1
        AND id = $2
      LIMIT 1`,
    [tenantId, missionId]
  );
  const missionRow = missionResult.rows[0];
  if (!missionRow) return null;

  const capacityRows = await loadCapacityRowsForMission(db, tenantId, missionId);
  const missionBody = unwrapMissionPayload(missionRow.payload);
  const selected = selectActiveCapacityContribution(missionBody, capacityRows);
  if (!selected) return null;

  return {
    mission_id: missionRow.mission_id,
    capacity_id: selected.capacity_id,
    payload: selected.payload,
    at: selected.at,
  };
}

async function loadUsableReadyCapacity(db, tenantId) {
  const missions = await db.query(
    `SELECT id AS mission_id, payload, updated_at
       FROM acquisition_missions
      WHERE tenant_id = $1
        AND stage = 'ready'
      ORDER BY updated_at DESC`,
    [tenantId]
  );

  for (const missionRow of missions.rows) {
    const capacityRows = await loadCapacityRowsForMission(db, tenantId, missionRow.mission_id);
    const missionBody = unwrapMissionPayload(missionRow.payload);
    const selected = selectActiveCapacityContribution(missionBody, capacityRows);
    if (selected) {
      return {
        mission_id: missionRow.mission_id,
        capacity_id: selected.capacity_id,
        payload: selected.payload,
        at: selected.at,
      };
    }
  }

  return null;
}

module.exports = {
  unwrapMissionPayload,
  isSupersededContribution,
  activeCapacityPointer,
  selectActiveCapacityContribution,
  loadCapacityRowsForMission,
  loadActiveCapacityForMission,
  loadUsableReadyCapacity,
};
