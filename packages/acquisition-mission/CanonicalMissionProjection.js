'use strict';

/**
 * SPEC-169 — Canonical Mission Verification.
 *
 * Persist what you verify. Verify what you persist.
 * Transaction verification compares canonical mission projections, not
 * manually curated subsets.
 */

const CANONICAL_PROJECTION_KEYS = Object.freeze([
  'mission',
  'structuredMission',
  'resolvedObjective',
  'executionPolicy',
  'communicationPolicy',
  'evaluationPolicy',
  'pendingOperatorDecision',
  'contributions',
  'observations',
  'outcomes',
  'events',
]);

/**
 * Derived or ephemeral runtime state. These never participate in verification.
 * They live on inspect()/workspace payloads, not on the durable mission record.
 */
const EPHEMERAL_SNAPSHOT_KEYS = Object.freeze([
  'timeline',
  'workspace',
  'workspaceContext',
  'health',
  'why',
  'learning',
  'outcomeLearning',
  'blocker',
  'discoveryArtifact',
  'progression',
  'executableDecision',
  'context',
  'spec',
]);

/** Presentation-only fields attached to events/observations/timeline rows. */
const PRESENTATION_ROW_KEYS = Object.freeze(['line', 'clock']);

/**
 * Keys that may leak onto a mission record from inspect() but are not durable.
 * Empty by design: durable mission fields automatically participate.
 */
const EPHEMERAL_MISSION_KEYS = new Set();

function isPlainObject(value) {
  return value != null && typeof value === 'object' && !Array.isArray(value) && !(value instanceof Date);
}

function canonicalizeTimestamp(value) {
  if (value instanceof Date) return value.toISOString();
  if (value == null) return value;
  return value;
}

function canonicalizeValue(value) {
  if (value instanceof Date) return value.toISOString();
  if (value == null) return value;
  if (Array.isArray(value)) return value.map(canonicalizeValue);
  if (typeof value === 'object') {
    const out = {};
    for (const key of Object.keys(value).sort()) {
      if (value[key] === undefined) continue;
      out[key] = canonicalizeValue(value[key]);
    }
    return out;
  }
  return value;
}

function stripPresentation(row) {
  if (!isPlainObject(row)) return row;
  const out = {};
  for (const key of Object.keys(row)) {
    if (PRESENTATION_ROW_KEYS.includes(key)) continue;
    out[key] = row[key];
  }
  return out;
}

function sortRecords(rows) {
  return [...rows].sort((a, b) => {
    const idCmp = String((a && a.id) || '').localeCompare(String((b && b.id) || ''));
    if (idCmp !== 0) return idCmp;
    return String((a && a.at) || '').localeCompare(String((b && b.at) || ''));
  });
}

function canonicalMissionRecord(mission) {
  if (!isPlainObject(mission)) return null;
  const record = {};
  for (const key of Object.keys(mission)) {
    if (EPHEMERAL_MISSION_KEYS.has(key)) continue;
    if (EPHEMERAL_SNAPSHOT_KEYS.includes(key)) continue;
    if (PRESENTATION_ROW_KEYS.includes(key)) continue;
    record[key] = mission[key];
  }
  return canonicalizeValue(record);
}

function canonicalizeEvent(event) {
  const row = stripPresentation(event) || {};
  return canonicalizeValue({
    id: row.id || null,
    missionId: row.missionId || null,
    kind: row.kind || null,
    specialist: row.specialist == null ? null : row.specialist,
    label: row.label || null,
    at: canonicalizeTimestamp(row.at) || null,
    payload: isPlainObject(row.payload) ? row.payload : {},
  });
}

function canonicalizeObservation(row) {
  const clean = stripPresentation(row) || {};
  return canonicalizeValue({
    id: clean.id || null,
    missionId: clean.missionId || null,
    specialist: clean.specialist || null,
    observation: clean.observation || null,
    at: canonicalizeTimestamp(clean.at) || null,
  });
}

function canonicalizeContribution(row) {
  return canonicalizeValue(stripPresentation(row) || {});
}

function canonicalizeOutcome(row) {
  return canonicalizeValue(stripPresentation(row) || {});
}

function unwrapSnapshot(snapshot) {
  if (!snapshot || typeof snapshot !== 'object') {
    return {
      mission: null,
      contributions: [],
      events: [],
      observations: [],
      outcomes: [],
    };
  }

  const mission = snapshot.mission != null
    ? snapshot.mission
    : (snapshot.id && (snapshot.stage != null || snapshot.objective != null) ? snapshot : null);

  const events = Array.isArray(snapshot.events)
    ? snapshot.events
    : Array.isArray(snapshot.timeline)
      ? snapshot.timeline
      : [];

  return {
    mission,
    contributions: Array.isArray(snapshot.contributions) ? snapshot.contributions : [],
    events,
    observations: Array.isArray(snapshot.observations) ? snapshot.observations : [],
    outcomes: Array.isArray(snapshot.outcomes) ? snapshot.outcomes : [],
  };
}

/**
 * Build the single canonical verification projection for a mission snapshot.
 * Accepts engine store snapshots, loadMissionSnapshot() results, or inspect()
 * payloads (inspect presentation fields are ignored).
 *
 * @param {object} snapshot
 * @returns {object} CanonicalMissionProjection
 */
function buildCanonicalMissionProjection(snapshot) {
  const unwrapped = unwrapSnapshot(snapshot);
  const mission = canonicalMissionRecord(unwrapped.mission);
  return {
    mission,
    structuredMission: mission ? mission.structuredMission ?? null : null,
    resolvedObjective: mission ? mission.resolvedObjective ?? null : null,
    executionPolicy: mission ? mission.executionPolicy ?? null : null,
    communicationPolicy: mission ? mission.communicationPolicy ?? null : null,
    evaluationPolicy: mission ? mission.evaluationPolicy ?? null : null,
    pendingOperatorDecision: mission ? mission.pendingOperatorDecision ?? null : null,
    contributions: sortRecords(unwrapped.contributions.map(canonicalizeContribution)),
    observations: sortRecords(unwrapped.observations.map(canonicalizeObservation)),
    outcomes: sortRecords(unwrapped.outcomes.map(canonicalizeOutcome)),
    events: sortRecords(unwrapped.events.map(canonicalizeEvent)),
  };
}

/**
 * Durable in-memory snapshot used for persistence verification.
 * Does not use inspect() — presentation is not the verification contract.
 *
 * @param {object} engine
 * @param {string} missionId
 * @param {string|number} [tenantId]
 * @returns {object|null}
 */
function snapshotFromEngine(engine, missionId, tenantId) {
  if (!engine || typeof engine.get !== 'function') return null;
  const mission = engine.get(missionId, tenantId);
  if (!mission) return null;
  const store = engine.store;
  return {
    mission,
    contributions: store && typeof store.listContributions === 'function'
      ? store.listContributions(missionId)
      : [],
    events: store && typeof store.listEvents === 'function'
      ? store.listEvents(missionId)
      : [],
    observations: store && typeof store.listObservations === 'function'
      ? store.listObservations(missionId)
      : [],
    outcomes: store && typeof store.listOutcomes === 'function'
      ? store.listOutcomes(missionId)
      : [],
  };
}

function valueType(value) {
  if (value === undefined) return 'undefined';
  if (value === null) return 'null';
  if (Array.isArray(value)) return 'array';
  return typeof value;
}

function divergenceReason(memory, persisted) {
  if (memory === undefined && persisted !== undefined) return 'missing in memory';
  if (persisted === undefined && memory !== undefined) return 'missing in persisted';
  if (valueType(memory) !== valueType(persisted)) return 'type mismatch';
  return 'value mismatch';
}

function orderedKeys(path, memory, persisted) {
  const keys = new Set([
    ...Object.keys(memory || {}),
    ...Object.keys(persisted || {}),
  ]);
  if (path === '') {
    const specified = CANONICAL_PROJECTION_KEYS.filter((key) => keys.has(key));
    const extra = [...keys].filter((key) => !CANONICAL_PROJECTION_KEYS.includes(key)).sort();
    return specified.concat(extra);
  }
  return [...keys].sort();
}

function walkDiff(path, memory, persisted, acc) {
  if (Object.is(memory, persisted)) return;

  if (isPlainObject(memory) && isPlainObject(persisted)) {
    for (const key of orderedKeys(path, memory, persisted)) {
      const next = path ? `${path}.${key}` : key;
      walkDiff(next, memory[key], persisted[key], acc);
    }
    return;
  }

  if (Array.isArray(memory) && Array.isArray(persisted)) {
    const len = Math.max(memory.length, persisted.length);
    for (let i = 0; i < len; i += 1) {
      walkDiff(`${path}[${i}]`, memory[i], persisted[i], acc);
    }
    return;
  }

  acc.push({
    field: path || '(root)',
    memory,
    persisted,
    reason: divergenceReason(memory, persisted),
  });
}

/**
 * Structural equality + diagnostics for two canonical projections.
 *
 * @param {object} memoryProjection
 * @param {object} persistedProjection
 * @returns {{ equal: boolean, firstDivergence: object|null, fields: object[] }}
 */
function diffCanonicalMissionProjections(memoryProjection, persistedProjection) {
  const fields = [];
  walkDiff('', memoryProjection, persistedProjection, fields);
  return {
    equal: fields.length === 0,
    firstDivergence: fields[0] || null,
    fields,
  };
}

function projectionsEqual(memoryProjection, persistedProjection) {
  return diffCanonicalMissionProjections(memoryProjection, persistedProjection).equal;
}

module.exports = {
  CANONICAL_PROJECTION_KEYS,
  EPHEMERAL_SNAPSHOT_KEYS,
  buildCanonicalMissionProjection,
  snapshotFromEngine,
  diffCanonicalMissionProjections,
  projectionsEqual,
  canonicalizeValue,
};
