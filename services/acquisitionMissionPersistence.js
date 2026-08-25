'use strict';

/**
 * SPEC-118 — persist acquisition missions, events, contributions, learning.
 * ADR-075 — core mission tables are writable only inside persistStageCommit().
 */

const {
  acquireMissionDurableLock,
  releaseMissionDurableLock,
  isGlobalLockHeld,
} = require('../packages/acquisition-mission/TransactionalPersistence');
const {
  buildCanonicalMissionProjection,
  snapshotFromEngine,
  diffCanonicalMissionProjections,
} = require('../packages/acquisition-mission/CanonicalMissionProjection');

function defaultPool() {
  return require('../db');
}

/** Depth counter — only persistStageCommit may write core mission tables (ADR-075). */
let stageCommitDepth = 0;

function assertExclusiveMissionWriter(fnName) {
  if (stageCommitDepth <= 0) {
    const err = new Error(
      `${fnName} is not allowed outside persistStageCommit (ADR-075).`
    );
    err.code = 'tme_persistence_exclusivity';
    err.spec = 'ADR-075';
    throw err;
  }
}

async function ensureAcquisitionMissionSchema(pool = defaultPool()) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS acquisition_missions (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      client_id INTEGER,
      stage TEXT NOT NULL,
      status TEXT NOT NULL,
      objective TEXT NOT NULL,
      target_segment TEXT,
      campaign TEXT,
      title TEXT,
      priority TEXT NOT NULL DEFAULT 'normal',
      confidence DOUBLE PRECISION,
      owner TEXT,
      created_by TEXT,
      orchestration_mission_id TEXT,
      payload JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS acquisition_mission_events (
      id TEXT PRIMARY KEY,
      mission_id TEXT NOT NULL REFERENCES acquisition_missions(id) ON DELETE CASCADE,
      tenant_id TEXT NOT NULL,
      kind TEXT NOT NULL,
      specialist TEXT,
      label TEXT,
      payload JSONB NOT NULL DEFAULT '{}'::jsonb,
      at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS acquisition_mission_contributions (
      id TEXT PRIMARY KEY,
      mission_id TEXT NOT NULL REFERENCES acquisition_missions(id) ON DELETE CASCADE,
      tenant_id TEXT NOT NULL,
      specialist TEXT NOT NULL,
      kind TEXT NOT NULL,
      payload JSONB NOT NULL DEFAULT '{}'::jsonb,
      at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS acquisition_mission_observations (
      id TEXT PRIMARY KEY,
      mission_id TEXT NOT NULL REFERENCES acquisition_missions(id) ON DELETE CASCADE,
      tenant_id TEXT NOT NULL,
      specialist TEXT NOT NULL,
      observation TEXT NOT NULL,
      at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS acquisition_mission_outcomes (
      id TEXT PRIMARY KEY,
      mission_id TEXT NOT NULL REFERENCES acquisition_missions(id) ON DELETE CASCADE,
      tenant_id TEXT NOT NULL,
      client_id INTEGER,
      outcome_type TEXT NOT NULL,
      segment TEXT,
      prospect_id INTEGER,
      payload JSONB NOT NULL DEFAULT '{}'::jsonb,
      at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS acquisition_mission_learning (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      mission_id TEXT,
      segment TEXT NOT NULL,
      sends INTEGER,
      replies INTEGER,
      reply_rate DOUBLE PRECISION,
      statement TEXT,
      auto_applied BOOLEAN NOT NULL DEFAULT FALSE,
      payload JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  await ensureOutcomeLearningSchema(pool);
}

async function ensureOutcomeLearningSchema(pool = defaultPool()) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS acquisition_mission_predictions (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      mission_id TEXT NOT NULL REFERENCES acquisition_missions(id) ON DELETE CASCADE,
      opportunity_id TEXT,
      status TEXT NOT NULL DEFAULT 'pending',
      payload JSONB NOT NULL DEFAULT '{}'::jsonb,
      captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      resolved_at TIMESTAMPTZ
    )
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS acquisition_mission_outcome_evaluations (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      mission_id TEXT NOT NULL REFERENCES acquisition_missions(id) ON DELETE CASCADE,
      prediction_id TEXT NOT NULL,
      accuracy TEXT NOT NULL,
      payload JSONB NOT NULL DEFAULT '{}'::jsonb,
      evaluated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS acquisition_mission_outcome_learnings (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      mission_id TEXT,
      evaluation_id TEXT,
      kind TEXT NOT NULL,
      subject TEXT,
      direction TEXT,
      statement TEXT,
      auto_applied BOOLEAN NOT NULL DEFAULT FALSE,
      payload JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
}

function columnOrPayload(rowValue, payloadValue) {
  return rowValue === undefined ? payloadValue : rowValue;
}

function missionFromRow(row) {
  const payload = row.payload && typeof row.payload === 'object' ? row.payload : {};
  return {
    ...payload,
    id: row.id,
    tenantId: columnOrPayload(row.tenant_id, payload.tenantId),
    clientId: columnOrPayload(row.client_id, payload.clientId),
    stage: columnOrPayload(row.stage, payload.stage),
    status: columnOrPayload(row.status, payload.status),
    objective: columnOrPayload(row.objective, payload.objective),
    targetSegment: columnOrPayload(row.target_segment, payload.targetSegment),
    campaign: columnOrPayload(row.campaign, payload.campaign),
    title: columnOrPayload(row.title, payload.title),
    priority: columnOrPayload(row.priority, payload.priority),
    confidence: columnOrPayload(row.confidence, payload.confidence),
    owner: columnOrPayload(row.owner, payload.owner),
    createdBy: columnOrPayload(row.created_by, payload.createdBy),
    orchestrationMissionId: columnOrPayload(row.orchestration_mission_id, payload.orchestrationMissionId),
    createdAt: columnOrPayload(row.created_at, payload.createdAt),
    updatedAt: columnOrPayload(row.updated_at, payload.updatedAt),
  };
}

async function persistMission(mission, pool = defaultPool(), opts = {}) {
  if (!mission?.id) return null;
  if (opts.internalStageCommit !== true) assertExclusiveMissionWriter('persistMission');
  if (opts.skipEnsure !== true) await ensureAcquisitionMissionSchema(pool);
  await pool.query(
    `INSERT INTO acquisition_missions (
       id, tenant_id, client_id, stage, status, objective, target_segment, campaign,
       title, priority, confidence, owner, created_by, orchestration_mission_id, payload, created_at, updated_at
     ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
     ON CONFLICT (id) DO UPDATE SET
       stage = EXCLUDED.stage,
       status = EXCLUDED.status,
       objective = EXCLUDED.objective,
       target_segment = EXCLUDED.target_segment,
       campaign = EXCLUDED.campaign,
       title = EXCLUDED.title,
       priority = EXCLUDED.priority,
       confidence = EXCLUDED.confidence,
       owner = EXCLUDED.owner,
       payload = EXCLUDED.payload,
       updated_at = EXCLUDED.updated_at`,
    [
      mission.id,
      String(mission.tenantId),
      mission.clientId || null,
      mission.stage,
      mission.status,
      mission.objective,
      mission.targetSegment || null,
      mission.campaign || null,
      mission.title || null,
      mission.priority,
      mission.confidence,
      mission.owner || null,
      mission.createdBy || null,
      mission.orchestrationMissionId || null,
      mission,
      mission.createdAt || new Date().toISOString(),
      mission.updatedAt || new Date().toISOString(),
    ]
  );
  return mission;
}

async function persistEvent(event, tenantId, pool = defaultPool(), opts = {}) {
  if (!event?.id) return null;
  if (opts.internalStageCommit !== true) assertExclusiveMissionWriter('persistEvent');
  if (opts.skipEnsure !== true) await ensureAcquisitionMissionSchema(pool);
  await pool.query(
    `INSERT INTO acquisition_mission_events (id, mission_id, tenant_id, kind, specialist, label, payload, at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
     ON CONFLICT (id) DO NOTHING`,
    [event.id, event.missionId, String(tenantId), event.kind, event.specialist, event.label, event, event.at]
  );
  return event;
}

async function persistContribution(row, tenantId, pool = defaultPool(), opts = {}) {
  if (!row?.id) return null;
  if (opts.internalStageCommit !== true) assertExclusiveMissionWriter('persistContribution');
  if (opts.skipEnsure !== true) await ensureAcquisitionMissionSchema(pool);
  await pool.query(
    `INSERT INTO acquisition_mission_contributions (id, mission_id, tenant_id, specialist, kind, payload, at)
     VALUES ($1,$2,$3,$4,$5,$6,$7)
     ON CONFLICT (id) DO NOTHING`,
    [row.id, row.missionId, String(tenantId), row.specialist, row.kind, row, row.at]
  );
  return row;
}

async function persistObservation(row, tenantId, pool = defaultPool(), opts = {}) {
  if (!row?.id) return null;
  if (opts.internalStageCommit !== true) assertExclusiveMissionWriter('persistObservation');
  if (opts.skipEnsure !== true) await ensureAcquisitionMissionSchema(pool);
  await pool.query(
    `INSERT INTO acquisition_mission_observations (id, mission_id, tenant_id, specialist, observation, at)
     VALUES ($1,$2,$3,$4,$5,$6)
     ON CONFLICT (id) DO NOTHING`,
    [row.id, row.missionId, String(tenantId), row.specialist, row.observation, row.at]
  );
  return row;
}

async function persistOutcome(row, pool = defaultPool(), opts = {}) {
  if (!row?.id) return null;
  if (opts.internalStageCommit !== true) assertExclusiveMissionWriter('persistOutcome');
  if (opts.skipEnsure !== true) await ensureAcquisitionMissionSchema(pool);
  await pool.query(
    `INSERT INTO acquisition_mission_outcomes (id, mission_id, tenant_id, client_id, outcome_type, segment, prospect_id, payload, at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
     ON CONFLICT (id) DO NOTHING`,
    [row.id, row.missionId, String(row.tenantId), row.clientId || null, row.type, row.segment, row.prospectId || null, row, row.at]
  );
  return row;
}

async function persistLearning(row, pool = defaultPool()) {
  if (!row?.id) return null;
  await ensureAcquisitionMissionSchema(pool);
  await pool.query(
    `INSERT INTO acquisition_mission_learning (id, tenant_id, mission_id, segment, sends, replies, reply_rate, statement, auto_applied, payload)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,FALSE,$9)
     ON CONFLICT (id) DO NOTHING`,
    [row.id, String(row.tenantId), row.missionId || null, row.segment, row.sends, row.replies, row.replyRate, row.statement, row]
  );
  return row;
}

async function persistPrediction(row, pool = defaultPool()) {
  if (!row?.id) return null;
  await ensureAcquisitionMissionSchema(pool);
  await pool.query(
    `INSERT INTO acquisition_mission_predictions (id, tenant_id, mission_id, opportunity_id, status, payload, captured_at, resolved_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
     ON CONFLICT (id) DO UPDATE SET
       status = EXCLUDED.status,
       payload = EXCLUDED.payload,
       resolved_at = EXCLUDED.resolved_at`,
    [
      row.id,
      String(row.tenantId),
      row.missionId,
      row.opportunityId || null,
      row.status || 'pending',
      row,
      row.capturedAt || new Date().toISOString(),
      row.resolvedAt || null,
    ]
  );
  return row;
}

async function persistOutcomeEvaluation(row, pool = defaultPool()) {
  if (!row?.id) return null;
  await ensureAcquisitionMissionSchema(pool);
  await pool.query(
    `INSERT INTO acquisition_mission_outcome_evaluations (id, tenant_id, mission_id, prediction_id, accuracy, payload, evaluated_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7)
     ON CONFLICT (id) DO NOTHING`,
    [
      row.id,
      String(row.tenantId),
      row.missionId,
      row.predictionId,
      row.accuracy,
      row,
      row.evaluatedAt || new Date().toISOString(),
    ]
  );
  return row;
}

async function persistOutcomeLearning(row, pool = defaultPool()) {
  if (!row?.id) return null;
  await ensureAcquisitionMissionSchema(pool);
  await pool.query(
    `INSERT INTO acquisition_mission_outcome_learnings (id, tenant_id, mission_id, evaluation_id, kind, subject, direction, statement, auto_applied, payload, created_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,FALSE,$9,$10)
     ON CONFLICT (id) DO NOTHING`,
    [
      row.id,
      String(row.tenantId),
      row.missionId || null,
      row.evaluationId || null,
      row.kind,
      row.subject || null,
      row.direction || null,
      row.statement || null,
      row,
      row.at || new Date().toISOString(),
    ]
  );
  return row;
}

async function ensureExecutionAuditSchema(pool = defaultPool()) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS acquisition_mission_execution_audit (
      id TEXT PRIMARY KEY,
      transaction_id TEXT NOT NULL,
      mission_id TEXT,
      tenant_id TEXT,
      mission_version INTEGER,
      specialist TEXT,
      stage TEXT,
      preconditions JSONB NOT NULL DEFAULT '{}'::jsonb,
      duration_ms INTEGER,
      commit_status TEXT NOT NULL,
      rollback_reason TEXT,
      error_class TEXT,
      exception TEXT,
      payload JSONB NOT NULL DEFAULT '{}'::jsonb,
      at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  await pool.query(`
    CREATE INDEX IF NOT EXISTS acquisition_mission_execution_audit_txn_idx
      ON acquisition_mission_execution_audit (transaction_id)
  `);
  await pool.query(`
    CREATE INDEX IF NOT EXISTS acquisition_mission_execution_audit_mission_idx
      ON acquisition_mission_execution_audit (mission_id, at DESC)
  `);
}

async function persistExecutionAudit(row, pool = defaultPool(), opts = {}) {
  if (!row?.id && !row?.transactionId) return null;
  if (opts.skipEnsure !== true) await ensureExecutionAuditSchema(pool);
  const id = row.id || row.transactionId;
  await pool.query(
    `INSERT INTO acquisition_mission_execution_audit (
       id, transaction_id, mission_id, tenant_id, mission_version, specialist, stage,
       preconditions, duration_ms, commit_status, rollback_reason, error_class, exception, payload, at
     ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
     ON CONFLICT (id) DO NOTHING`,
    [
      id,
      row.transactionId,
      row.missionId || null,
      row.tenantId != null ? String(row.tenantId) : null,
      row.missionVersion != null ? row.missionVersion : 0,
      row.specialist || null,
      row.stage || null,
      row.preconditions || {},
      row.durationMs != null ? row.durationMs : 0,
      row.commitStatus,
      row.rollbackReason || null,
      row.errorClass || null,
      row.exception || null,
      row.payload || {},
      row.at || new Date().toISOString(),
    ]
  );
  return row;
}

/**
 * SPEC-131 — persist mission + events + contributions + commit audit in one SQL transaction.
 */
async function persistStageCommit(bundle = {}, pool = defaultPool(), opts = {}) {
  const mission = bundle.mission;
  if (!mission?.id) {
    const err = new Error('Mission is required to persist a stage commit.');
    err.code = 'tme_persistence';
    throw err;
  }
  if (opts.skipEnsure !== true) {
    await ensureAcquisitionMissionSchema(pool);
    await ensureExecutionAuditSchema(pool);
  }

  const lockPool = opts.lockPool || pool;
  let lockHeldHere = false;
  if (opts.skipGlobalLock !== true && !isGlobalLockHeld(mission.id)) {
    const lockResult = await acquireMissionDurableLock(mission.id, lockPool, {
      tryOnly: opts.tryLock !== false,
    });
    lockHeldHere = lockResult.acquired === true;
  }

  const client = typeof pool.connect === 'function' ? await pool.connect() : pool;
  const ownsClient = client !== pool && typeof client.release === 'function';
  const writeOpts = { skipEnsure: true, internalStageCommit: true };

  stageCommitDepth += 1;
  try {
    await client.query('BEGIN');
    await persistMission(mission, client, writeOpts);
    for (const event of bundle.events || []) {
      await persistEvent(event, mission.tenantId, client, writeOpts);
    }
    for (const row of bundle.contributions || []) {
      await persistContribution(row, mission.tenantId, client, writeOpts);
    }
    for (const row of bundle.observations || []) {
      await persistObservation(row, mission.tenantId, client, writeOpts);
    }
    for (const row of bundle.outcomes || []) {
      await persistOutcome(row, client, writeOpts);
    }
    if (bundle.audit) {
      await persistExecutionAudit(bundle.audit, client, writeOpts);
    }
    await client.query('COMMIT');
  } catch (err) {
    try {
      await client.query('ROLLBACK');
    } catch (_) {
      /* rollback best-effort */
    }
    const wrapped = new Error(err.message || 'Persistence failure.');
    wrapped.code = err.code === 'tme_transaction_overlap'
      ? 'tme_transaction_overlap'
      : 'tme_persistence';
    wrapped.cause = err;
    throw wrapped;
  } finally {
    stageCommitDepth -= 1;
    if (ownsClient) client.release();
    if (lockHeldHere) {
      await releaseMissionDurableLock(mission.id, lockPool);
    }
  }
  return mission;
}

/**
 * SPEC-139 — load one mission + tenant-scoped side effects for durable verification.
 */
async function loadMissionSnapshot(missionId, tenantId, pool = defaultPool()) {
  const loaded = await loadTenantMissions(tenantId, pool);
  const mission = loaded.missions.find((row) => row && row.id === missionId) || null;
  if (!mission) return null;
  const contributions = loaded.contributions.filter((row) => row && row.missionId === missionId);
  const events = loaded.events
    .map((row) => {
      const payload = row.payload && typeof row.payload === 'object' ? row.payload : row;
      return {
        id: payload.id || row.id,
        missionId: payload.missionId || row.mission_id,
        kind: payload.kind || row.kind,
        specialist: payload.specialist || row.specialist,
        label: payload.label || row.label,
        at: payload.at || row.at,
        payload: payload.payload || {},
      };
    })
    .filter((row) => row.missionId === missionId);
  const observations = loaded.observations.filter((row) => row && row.missionId === missionId);
  const outcomes = loaded.outcomes.filter((row) => row && row.missionId === missionId);
  return { mission, contributions, events, observations, outcomes };
}

/**
 * SPEC-169 — subset comparator removed. Persistence verification uses the
 * canonical mission projection only. Kept as a compatibility alias.
 */
function comparableMissionState(snapshot) {
  return buildCanonicalMissionProjection(snapshot);
}

/**
 * SPEC-139 / SPEC-169 — persisted canonical projection must match in-memory
 * engine state before commit succeeds.
 */
async function assertPersistedMatchesEngine(engine, missionId, tenantId, pool = defaultPool()) {
  const inMemory = snapshotFromEngine(engine, missionId, tenantId);
  const persisted = await loadMissionSnapshot(missionId, tenantId, pool);
  if (!persisted || !persisted.mission) {
    const err = new Error('Persisted mission snapshot is missing after stage commit.');
    err.code = 'tme_persistence_verify';
    throw err;
  }
  const memoryProjection = buildCanonicalMissionProjection(inMemory);
  const persistedProjection = buildCanonicalMissionProjection(persisted);
  const diff = diffCanonicalMissionProjections(memoryProjection, persistedProjection);
  if (!diff.equal) {
    const first = diff.firstDivergence;
    const err = new Error(
      `Persisted mission does not match committed in-memory mission (${first.field}: ${first.reason}).`
    );
    err.code = 'tme_persistence_verify';
    err.details = {
      firstDivergence: first,
      diff: diff.fields,
      memory: first && first.memory,
      persisted: first && first.persisted,
      reason: first && first.reason,
      field: first && first.field,
    };
    throw err;
  }
  return persisted;
}

/**
 * SPEC-139 — bind durable stage persistence + verification for TME commits.
 * Returns null when persistence is disabled or no pool is available.
 */
function bindStagePersistDurable(input = {}, engine, tenantId) {
  if (typeof input.persistStage === 'function') {
    return (ctx) => input.persistStage(ctx);
  }
  if (input.persist === false || !input.pool || !engine) return null;
  return async (ctx) => {
    const missionId = ctx.missionId;
    await persistStageCommit({
      mission: engine.get(missionId, tenantId),
      events: engine.store.listEvents(missionId),
      contributions: engine.store.listContributions(missionId),
      observations: engine.store.listObservations(missionId),
      outcomes: engine.store.listOutcomes(missionId),
      audit: ctx.audit,
    }, input.pool, { skipGlobalLock: true });
    await assertPersistedMatchesEngine(engine, missionId, tenantId, input.pool);
  };
}

async function loadTenantMissions(tenantId, pool = defaultPool()) {
  await ensureAcquisitionMissionSchema(pool);
  const key = String(tenantId);
  const missions = (await pool.query(
    `SELECT * FROM acquisition_missions WHERE tenant_id = $1 ORDER BY updated_at DESC`,
    [key]
  )).rows.map(missionFromRow);
  const events = (await pool.query(
    `SELECT payload, id, mission_id, kind, specialist, label, at FROM acquisition_mission_events WHERE tenant_id = $1`,
    [key]
  )).rows;
  const contributions = (await pool.query(
    `SELECT payload FROM acquisition_mission_contributions WHERE tenant_id = $1`,
    [key]
  )).rows.map((row) => row.payload);
  const observations = (await pool.query(
    `SELECT id, mission_id, specialist, observation, at FROM acquisition_mission_observations WHERE tenant_id = $1`,
    [key]
  )).rows.map((row) => ({
    id: row.id,
    missionId: row.mission_id,
    specialist: row.specialist,
    observation: row.observation,
    at: row.at,
  }));
  const outcomes = (await pool.query(
    `SELECT payload FROM acquisition_mission_outcomes WHERE tenant_id = $1`,
    [key]
  )).rows.map((row) => row.payload);
  const learning = (await pool.query(
    `SELECT payload FROM acquisition_mission_learning WHERE tenant_id = $1`,
    [key]
  )).rows.map((row) => row.payload);
  const predictions = (await pool.query(
    `SELECT payload FROM acquisition_mission_predictions WHERE tenant_id = $1`,
    [key]
  )).rows.map((row) => row.payload);
  const evaluations = (await pool.query(
    `SELECT payload FROM acquisition_mission_outcome_evaluations WHERE tenant_id = $1`,
    [key]
  )).rows.map((row) => row.payload);
  const outcomeLearnings = (await pool.query(
    `SELECT payload FROM acquisition_mission_outcome_learnings WHERE tenant_id = $1`,
    [key]
  )).rows.map((row) => row.payload);
  return { missions, events, contributions, observations, outcomes, learning, predictions, evaluations, outcomeLearnings };
}

const AMO_TABLES = Object.freeze([
  'acquisition_mission_outcome_learnings',
  'acquisition_mission_outcome_evaluations',
  'acquisition_mission_predictions',
  'acquisition_mission_execution_audit',
  'acquisition_mission_learning',
  'acquisition_mission_outcomes',
  'acquisition_mission_observations',
  'acquisition_mission_contributions',
  'acquisition_mission_events',
  'acquisition_missions',
]);

/**
 * SPEC-138 — row counts for AMO tables (optional tenant scope).
 */
async function countAmoRows(tenantId = null, pool = defaultPool()) {
  await ensureAcquisitionMissionSchema(pool);
  await ensureExecutionAuditSchema(pool);
  const counts = {};
  const tenantClause = tenantId != null ? ' WHERE tenant_id = $1' : '';
  const params = tenantId != null ? [String(tenantId)] : [];
  for (const table of AMO_TABLES) {
    const result = await pool.query(`SELECT COUNT(*)::int AS count FROM ${table}${tenantClause}`, params);
    counts[table] = result.rows[0].count;
  }
  return counts;
}

/**
 * SPEC-138 — delete all AMO workflow state. Business intelligence tables are untouched.
 */
async function deleteAllAmoData(tenantId = null, pool = defaultPool()) {
  await ensureAcquisitionMissionSchema(pool);
  await ensureExecutionAuditSchema(pool);

  const client = typeof pool.connect === 'function' ? await pool.connect() : pool;
  const ownsClient = client !== pool && typeof client.release === 'function';
  const tenantKey = tenantId != null ? String(tenantId) : null;
  const before = await countAmoRows(tenantId, pool);

  try {
    await client.query('BEGIN');
    if (tenantKey) {
      await client.query('DELETE FROM acquisition_mission_execution_audit WHERE tenant_id = $1', [tenantKey]);
      await client.query('DELETE FROM acquisition_mission_learning WHERE tenant_id = $1', [tenantKey]);
      await client.query('DELETE FROM acquisition_mission_outcomes WHERE tenant_id = $1', [tenantKey]);
      await client.query('DELETE FROM acquisition_mission_observations WHERE tenant_id = $1', [tenantKey]);
      await client.query('DELETE FROM acquisition_mission_contributions WHERE tenant_id = $1', [tenantKey]);
      await client.query('DELETE FROM acquisition_mission_events WHERE tenant_id = $1', [tenantKey]);
      await client.query('DELETE FROM acquisition_missions WHERE tenant_id = $1', [tenantKey]);
    } else {
      await client.query('DELETE FROM acquisition_mission_execution_audit');
      await client.query('DELETE FROM acquisition_mission_learning');
      await client.query('DELETE FROM acquisition_mission_outcomes');
      await client.query('DELETE FROM acquisition_mission_observations');
      await client.query('DELETE FROM acquisition_mission_contributions');
      await client.query('DELETE FROM acquisition_mission_events');
      await client.query('DELETE FROM acquisition_missions');
    }
    await client.query('COMMIT');
  } catch (err) {
    try {
      await client.query('ROLLBACK');
    } catch (_) {
      /* rollback best-effort */
    }
    throw err;
  } finally {
    if (ownsClient) client.release();
  }

  const after = await countAmoRows(tenantId, pool);
  return { before, after, tenantId: tenantKey };
}

/**
 * SPEC-138 — clear persisted session references to deleted AMO missions.
 */
async function clearAmoSessionBindings(pool = defaultPool()) {
  const result = await pool.query(`
    UPDATE session
    SET sess = (
      (sess::jsonb - 'context' - '_amoHydration')
      || jsonb_build_object(
        'context',
        COALESCE(sess::jsonb->'context', '{}'::jsonb)
          - 'missionId'
          - 'acquisitionMissionId'
          - 'acquisitionOwner'
      )
    )::json
    WHERE sess::jsonb ? 'context'
      AND (
        sess::jsonb->'context' ? 'missionId'
        OR sess::jsonb->'context' ? 'acquisitionMissionId'
        OR sess::jsonb->'context' ? 'acquisitionOwner'
      )
      OR sess::jsonb ? '_amoHydration'
  `);
  return { sessionsCleared: result.rowCount || 0 };
}

module.exports = {
  ensureAcquisitionMissionSchema,
  ensureOutcomeLearningSchema,
  ensureExecutionAuditSchema,
  persistMission,
  persistEvent,
  persistContribution,
  persistObservation,
  persistOutcome,
  persistLearning,
  persistPrediction,
  persistOutcomeEvaluation,
  persistOutcomeLearning,
  persistExecutionAudit,
  persistStageCommit,
  loadMissionSnapshot,
  comparableMissionState,
  buildCanonicalMissionProjection,
  snapshotFromEngine,
  diffCanonicalMissionProjections,
  assertPersistedMatchesEngine,
  bindStagePersistDurable,
  loadTenantMissions,
  countAmoRows,
  deleteAllAmoData,
  clearAmoSessionBindings,
};
