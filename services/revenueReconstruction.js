'use strict';

const crypto = require('crypto');
const { canonicalize } = require('../utils/revenuePhase16b');

const RUN_SPECIFIC_FIELDS = new Set(['id', 'created_at', 'updated_at']);

function normalize(value) {
  if (value instanceof Date) return value.toISOString();
  if (Array.isArray(value)) return value.map(normalize);
  if (value && typeof value === 'object') {
    return Object.fromEntries(Object.entries(value)
      .filter(([key]) => !RUN_SPECIFIC_FIELDS.has(key))
      .map(([key, item]) => [key, normalize(item)]));
  }
  return value;
}

function canonicalProjection(rows) {
  return canonicalize(rows
    .map(normalize)
    .sort((a, b) => `${a.client_id}:${a.job_id}`.localeCompare(`${b.client_id}:${b.job_id}`)));
}

function hashProjection(rows) {
  return crypto.createHash('sha256').update(JSON.stringify(canonicalProjection(rows))).digest('hex');
}

function fieldDifferences(expected, actual, path = '$', output = []) {
  if (Object.is(expected, actual)) return output;
  if (Array.isArray(expected) && Array.isArray(actual)) {
    const length = Math.max(expected.length, actual.length);
    for (let index = 0; index < length; index += 1) {
      fieldDifferences(expected[index], actual[index], `${path}[${index}]`, output);
    }
    return output;
  }
  if (expected && actual && typeof expected === 'object' && typeof actual === 'object') {
    const keys = new Set([...Object.keys(expected), ...Object.keys(actual)]);
    for (const key of [...keys].sort()) {
      fieldDifferences(expected[key], actual[key], `${path}.${key}`, output);
    }
    return output;
  }
  output.push({
    field: path,
    reconstructed: expected === undefined ? null : expected,
    persisted: actual === undefined ? null : actual,
  });
  return output;
}

async function captureReconstructionBoundary(db, clientId = 10) {
  const { rows } = await db.query(`
    SELECT recorded_at::text AS recorded_at, event_id
    FROM revenue_events
    WHERE client_id=$1
    ORDER BY recorded_at DESC, event_id DESC
    LIMIT 1
  `, [clientId]);
  if (!rows[0]) throw new Error('Cannot bind reconstruction boundary without revenue events');
  return {
    client_id: Number(clientId),
    recorded_at: rows[0].recorded_at,
    event_id: rows[0].event_id,
  };
}

async function reconstructAtBoundary(db, boundary) {
  if (!boundary || Number(boundary.client_id) !== 10
    || !boundary.recorded_at || !boundary.event_id) {
    throw new Error('A fixed client-10 reconstruction boundary is required');
  }
  const { rows } = await db.query(`
    SELECT event_id, recorded_at, payload_json->'outcome' AS outcome
    FROM revenue_events
    WHERE client_id=$1
      AND event_type='revenue_outcome_updated'
      AND (recorded_at,event_id) <= ($2::timestamptz,$3::uuid)
    ORDER BY recorded_at, event_id
  `, [boundary.client_id, boundary.recorded_at, boundary.event_id]);
  const latestByJob = new Map();
  const unexplained = [];
  for (const row of rows) {
    if (!row.outcome?.job_id) unexplained.push(row.event_id);
    else latestByJob.set(row.outcome.job_id, row.outcome);
  }
  return {
    projection: canonicalProjection([...latestByJob.values()]),
    unexplained_event_ids: unexplained,
  };
}

async function persistedProjection(db, clientId) {
  const { rows } = await db.query(
    'SELECT * FROM revenue_outcomes WHERE client_id=$1 ORDER BY client_id,job_id',
    [clientId]
  );
  return canonicalProjection(rows);
}

async function verifyDeterministicReconstruction(db, boundary) {
  const first = await reconstructAtBoundary(db, boundary);
  const second = await reconstructAtBoundary(db, boundary);
  const persisted = await persistedProjection(db, boundary.client_id);
  const firstHash = hashProjection(first.projection);
  const secondHash = hashProjection(second.projection);
  const persistedHash = hashProjection(persisted);
  const differences = fieldDifferences(first.projection, persisted);
  const unexplained = [...new Set([
    ...first.unexplained_event_ids,
    ...second.unexplained_event_ids,
  ])];
  const passed = firstHash === secondHash
    && firstHash === persistedHash
    && differences.length === 0
    && unexplained.length === 0;
  return {
    status: passed ? 'passed' : 'failed',
    boundary,
    first_reconstruction_hash: firstHash,
    second_reconstruction_hash: secondHash,
    persisted_projection_hash: persistedHash,
    reconstructed_outcome_count: first.projection.length,
    persisted_outcome_count: persisted.length,
    field_differences: differences,
    unexplained_event_ids: unexplained,
    non_destructive: true,
  };
}

module.exports = {
  RUN_SPECIFIC_FIELDS,
  canonicalProjection,
  captureReconstructionBoundary,
  fieldDifferences,
  hashProjection,
  reconstructAtBoundary,
  verifyDeterministicReconstruction,
};
