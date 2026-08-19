'use strict';

/**
 * SPEC-118 — persist acquisition missions, events, contributions, learning.
 */

function defaultPool() {
  return require('../db');
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
}

function missionFromRow(row) {
  const payload = row.payload && typeof row.payload === 'object' ? row.payload : {};
  return {
    ...payload,
    id: row.id,
    tenantId: row.tenant_id,
    clientId: row.client_id,
    stage: row.stage,
    status: row.status,
    objective: row.objective,
    targetSegment: row.target_segment,
    campaign: row.campaign,
    title: row.title,
    priority: row.priority,
    confidence: row.confidence,
    owner: row.owner,
    createdBy: row.created_by,
    orchestrationMissionId: row.orchestration_mission_id,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

async function persistMission(mission, pool = defaultPool()) {
  if (!mission?.id) return null;
  await ensureAcquisitionMissionSchema(pool);
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

async function persistEvent(event, tenantId, pool = defaultPool()) {
  if (!event?.id) return null;
  await ensureAcquisitionMissionSchema(pool);
  await pool.query(
    `INSERT INTO acquisition_mission_events (id, mission_id, tenant_id, kind, specialist, label, payload, at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
     ON CONFLICT (id) DO NOTHING`,
    [event.id, event.missionId, String(tenantId), event.kind, event.specialist, event.label, event, event.at]
  );
  return event;
}

async function persistContribution(row, tenantId, pool = defaultPool()) {
  if (!row?.id) return null;
  await ensureAcquisitionMissionSchema(pool);
  await pool.query(
    `INSERT INTO acquisition_mission_contributions (id, mission_id, tenant_id, specialist, kind, payload, at)
     VALUES ($1,$2,$3,$4,$5,$6,$7)
     ON CONFLICT (id) DO NOTHING`,
    [row.id, row.missionId, String(tenantId), row.specialist, row.kind, row, row.at]
  );
  return row;
}

async function persistObservation(row, tenantId, pool = defaultPool()) {
  if (!row?.id) return null;
  await ensureAcquisitionMissionSchema(pool);
  await pool.query(
    `INSERT INTO acquisition_mission_observations (id, mission_id, tenant_id, specialist, observation, at)
     VALUES ($1,$2,$3,$4,$5,$6)
     ON CONFLICT (id) DO NOTHING`,
    [row.id, row.missionId, String(tenantId), row.specialist, row.observation, row.at]
  );
  return row;
}

async function persistOutcome(row, pool = defaultPool()) {
  if (!row?.id) return null;
  await ensureAcquisitionMissionSchema(pool);
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
  return { missions, events, contributions, observations, outcomes, learning };
}

module.exports = {
  ensureAcquisitionMissionSchema,
  persistMission,
  persistEvent,
  persistContribution,
  persistObservation,
  persistOutcome,
  persistLearning,
  loadTenantMissions,
};
