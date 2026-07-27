'use strict';

/**
 * Postgres MissionStore — durable missions + audit (SPEC-022).
 */

const { newId, AUDIT_KINDS } = require('./types');

const ENSURE_SQL = `
CREATE TABLE IF NOT EXISTS missions (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  client_id INTEGER,
  type TEXT NOT NULL,
  status TEXT NOT NULL,
  objective_text TEXT NOT NULL,
  title TEXT,
  constraints JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_by TEXT,
  priority TEXT DEFAULT 'normal',
  plan JSONB NOT NULL DEFAULT '{}'::jsonb,
  confidence DOUBLE PRECISION,
  duration_estimate_ms INTEGER,
  progress JSONB NOT NULL DEFAULT '{}'::jsonb,
  deliverables JSONB,
  review JSONB,
  started_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS missions_tenant_created_idx
  ON missions (tenant_id, created_at DESC);
CREATE INDEX IF NOT EXISTS missions_client_created_idx
  ON missions (client_id, created_at DESC);
CREATE INDEX IF NOT EXISTS missions_status_idx
  ON missions (status);

CREATE TABLE IF NOT EXISTS mission_audit_events (
  id TEXT PRIMARY KEY,
  mission_id TEXT NOT NULL REFERENCES missions(id) ON DELETE CASCADE,
  at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  kind TEXT NOT NULL,
  capability_id TEXT,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS mission_audit_mission_at_idx
  ON mission_audit_events (mission_id, at ASC);
`;

class PostgresMissionStore {
  /**
   * @param {{ query: Function }} pool
   */
  constructor(pool) {
    if (!pool || typeof pool.query !== 'function') {
      throw new Error('PostgresMissionStore requires pool');
    }
    this._pool = pool;
    this._ensured = false;
  }

  async ensureSchema() {
    if (this._ensured) return;
    await this._pool.query(ENSURE_SQL);
    this._ensured = true;
  }

  async create(mission) {
    await this.ensureSchema();
    const row = mission;
    await this._pool.query(
      `INSERT INTO missions (
        id, tenant_id, client_id, type, status, objective_text, title,
        constraints, created_by, priority, plan, confidence,
        duration_estimate_ms, progress, deliverables, review,
        started_at, completed_at, created_at, updated_at
      ) VALUES (
        $1,$2,$3,$4,$5,$6,$7,
        $8::jsonb,$9,$10,$11::jsonb,$12,
        $13,$14::jsonb,$15::jsonb,$16::jsonb,
        $17,$18,$19,$20
      )`,
      [
        row.id,
        String(row.tenantId),
        row.clientId != null ? Number(row.clientId) || null : null,
        row.type,
        row.status,
        row.objectiveText,
        row.title || null,
        JSON.stringify(row.constraints || {}),
        row.createdBy || null,
        row.priority || 'normal',
        JSON.stringify(row.plan || {}),
        row.confidence != null ? row.confidence : null,
        row.durationEstimateMs != null ? row.durationEstimateMs : null,
        JSON.stringify(row.progress || {}),
        row.deliverables != null ? JSON.stringify(row.deliverables) : null,
        row.review != null ? JSON.stringify(row.review) : null,
        row.startedAt || null,
        row.completedAt || null,
        row.createdAt || new Date().toISOString(),
        row.updatedAt || new Date().toISOString(),
      ]
    );
    await this.appendAudit({
      missionId: row.id,
      kind: AUDIT_KINDS.REQUEST,
      payload: { objectiveText: row.objectiveText, type: row.type },
    });
    return this.get(row.id);
  }

  async get(id) {
    await this.ensureSchema();
    const { rows } = await this._pool.query(
      `SELECT * FROM missions WHERE id = $1`,
      [String(id)]
    );
    return rows[0] ? fromRow(rows[0]) : null;
  }

  async update(patch) {
    await this.ensureSchema();
    if (!patch || !patch.id) throw new Error('update requires id');
    const existing = await this.get(patch.id);
    if (!existing) throw new Error(`Unknown mission: ${patch.id}`);
    const next = {
      ...existing,
      ...patch,
      updatedAt: new Date().toISOString(),
    };
    await this._pool.query(
      `UPDATE missions SET
        status = $2,
        title = $3,
        constraints = $4::jsonb,
        plan = $5::jsonb,
        confidence = $6,
        duration_estimate_ms = $7,
        progress = $8::jsonb,
        deliverables = $9::jsonb,
        review = $10::jsonb,
        started_at = $11,
        completed_at = $12,
        updated_at = $13
      WHERE id = $1`,
      [
        next.id,
        next.status,
        next.title || null,
        JSON.stringify(next.constraints || {}),
        JSON.stringify(next.plan || {}),
        next.confidence != null ? next.confidence : null,
        next.durationEstimateMs != null ? next.durationEstimateMs : null,
        JSON.stringify(next.progress || {}),
        next.deliverables != null ? JSON.stringify(next.deliverables) : null,
        next.review != null ? JSON.stringify(next.review) : null,
        next.startedAt || null,
        next.completedAt || null,
        next.updatedAt,
      ]
    );
    return this.get(next.id);
  }

  async list(query = {}) {
    await this.ensureSchema();
    const limit = Number(query.limit) || 50;
    const params = [];
    const where = [];
    if (query.tenantId != null) {
      params.push(String(query.tenantId));
      where.push(`tenant_id = $${params.length}`);
    }
    if (query.clientId != null) {
      params.push(Number(query.clientId));
      where.push(`client_id = $${params.length}`);
    }
    params.push(limit);
    const sql = `
      SELECT * FROM missions
      ${where.length ? `WHERE ${where.join(' AND ')}` : ''}
      ORDER BY created_at DESC
      LIMIT $${params.length}
    `;
    const { rows } = await this._pool.query(sql, params);
    return rows.map(fromRow);
  }

  async appendAudit(event) {
    await this.ensureSchema();
    const row = {
      id: event.id || newId('aud'),
      missionId: String(event.missionId),
      at: event.at || new Date().toISOString(),
      kind: String(event.kind),
      capabilityId: event.capabilityId || null,
      payload: event.payload || {},
    };
    await this._pool.query(
      `INSERT INTO mission_audit_events (id, mission_id, at, kind, capability_id, payload)
       VALUES ($1,$2,$3,$4,$5,$6::jsonb)`,
      [
        row.id,
        row.missionId,
        row.at,
        row.kind,
        row.capabilityId,
        JSON.stringify(row.payload),
      ]
    );
    return row;
  }

  async listAudit(missionId) {
    await this.ensureSchema();
    const { rows } = await this._pool.query(
      `SELECT * FROM mission_audit_events
       WHERE mission_id = $1
       ORDER BY at ASC`,
      [String(missionId)]
    );
    return rows.map((r) => ({
      id: r.id,
      missionId: r.mission_id,
      at: iso(r.at),
      kind: r.kind,
      capabilityId: r.capability_id,
      payload: r.payload || {},
    }));
  }
}

function fromRow(r) {
  const constraints = r.constraints || {};
  const plan = r.plan || {};
  const profileSnap = constraints.discoveryProfile || null;
  return {
    id: r.id,
    tenantId: r.tenant_id,
    clientId: r.client_id,
    type: r.type,
    status: r.status,
    objectiveText: r.objective_text,
    title: r.title,
    constraints,
    discoveryProfile: profileSnap
      ? {
          id: profileSnap.id,
          name: profileSnap.name,
          version: profileSnap.version,
          selection: 'pinned',
          message:
            plan.discoveryProfileMessage ||
            `Using Discovery Profile: ${profileSnap.name}.`,
        }
      : null,
    createdBy: r.created_by,
    priority: r.priority,
    plan,
    confidence: r.confidence,
    durationEstimateMs: r.duration_estimate_ms,
    progress: r.progress || {},
    deliverables: r.deliverables,
    review: r.review,
    startedAt: iso(r.started_at),
    completedAt: iso(r.completed_at),
    createdAt: iso(r.created_at),
    updatedAt: iso(r.updated_at),
  };
}

function iso(v) {
  if (!v) return null;
  if (typeof v === 'string') return v;
  if (v instanceof Date) return v.toISOString();
  return String(v);
}

async function ensureMissionSchema(pool) {
  await pool.query(ENSURE_SQL);
}

function createPostgresMissionStore(pool) {
  return new PostgresMissionStore(pool);
}

module.exports = {
  PostgresMissionStore,
  createPostgresMissionStore,
  ensureMissionSchema,
  ENSURE_SQL,
};
