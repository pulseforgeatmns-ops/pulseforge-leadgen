'use strict';

/**
 * Postgres Client Playbook store (SPEC-028 / ADR-015).
 * Playbooks are immutable once used — edits create a new version row.
 */

const { buildClientPlaybook, PLAYBOOK_STATUS } = require('./types');
const { seedClientPlaybooks } = require('./seedPlaybooks');
const { bumpVersion } = require('./ClientPlaybookStore');

const ENSURE_SQL = `
CREATE TABLE IF NOT EXISTS client_playbooks (
  id TEXT NOT NULL,
  version TEXT NOT NULL,
  client_id TEXT,
  name TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  target_markets JSONB NOT NULL DEFAULT '[]'::jsonb,
  value_propositions JSONB NOT NULL DEFAULT '[]'::jsonb,
  ideal_customer JSONB NOT NULL DEFAULT '{}'::jsonb,
  brand_voice TEXT NOT NULL DEFAULT 'professional',
  preferred_channels JSONB NOT NULL DEFAULT '[]'::jsonb,
  outreach_sequence JSONB NOT NULL DEFAULT '[]'::jsonb,
  offers JSONB NOT NULL DEFAULT '[]'::jsonb,
  constraints JSONB NOT NULL DEFAULT '[]'::jsonb,
  success_metrics JSONB NOT NULL DEFAULT '[]'::jsonb,
  notes TEXT,
  parent_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (id, version)
);

CREATE INDEX IF NOT EXISTS client_playbooks_client_idx
  ON client_playbooks (client_id);
CREATE INDEX IF NOT EXISTS client_playbooks_status_idx
  ON client_playbooks (status);
CREATE INDEX IF NOT EXISTS client_playbooks_name_idx
  ON client_playbooks (name);
`;

class PostgresClientPlaybookStore {
  /**
   * @param {{ query: Function }} pool
   */
  constructor(pool) {
    if (!pool || typeof pool.query !== 'function') {
      throw new Error('PostgresClientPlaybookStore requires pool');
    }
    this._pool = pool;
    this._ensured = false;
  }

  async ensureSchema() {
    if (this._ensured) return;
    await this._pool.query(ENSURE_SQL);
    this._ensured = true;
  }

  async seedIfEmpty() {
    await this.ensureSchema();
    const { rows } = await this._pool.query(
      `SELECT COUNT(*)::int AS n FROM client_playbooks`
    );
    if (rows[0] && rows[0].n > 0) return { seeded: 0 };
    let seeded = 0;
    for (const playbook of seedClientPlaybooks()) {
      await this._insert(playbook);
      seeded += 1;
    }
    return { seeded };
  }

  async get(id, version) {
    await this.ensureSchema();
    if (version != null) {
      const { rows } = await this._pool.query(
        `SELECT * FROM client_playbooks WHERE id = $1 AND version = $2`,
        [String(id), String(version)]
      );
      return rows[0] ? fromRow(rows[0]) : null;
    }
    const { rows } = await this._pool.query(
      `SELECT * FROM client_playbooks
       WHERE id = $1 AND status IN ('active', 'approved')
       ORDER BY created_at DESC
       LIMIT 1`,
      [String(id)]
    );
    if (rows[0]) return fromRow(rows[0]);
    const fallback = await this._pool.query(
      `SELECT * FROM client_playbooks WHERE id = $1
       ORDER BY created_at DESC LIMIT 1`,
      [String(id)]
    );
    return fallback.rows[0] ? fromRow(fallback.rows[0]) : null;
  }

  async list(query = {}) {
    await this.ensureSchema();
    const status = query.status || PLAYBOOK_STATUS.ACTIVE;
    const params = [];
    let sql = `SELECT DISTINCT ON (id) * FROM client_playbooks`;
    const where = [];
    if (status && status !== 'any') {
      params.push(status);
      where.push(`status = $${params.length}`);
    }
    if (query.clientId != null) {
      params.push(String(query.clientId));
      where.push(`client_id = $${params.length}`);
    }
    if (where.length) sql += ` WHERE ${where.join(' AND ')}`;
    sql += ` ORDER BY id, created_at DESC`;
    const { rows } = await this._pool.query(sql, params);
    return rows.map(fromRow).sort((a, b) => a.name.localeCompare(b.name));
  }

  async getForClient(clientId) {
    const list = await this.list({
      clientId,
      status: PLAYBOOK_STATUS.ACTIVE,
    });
    return list[0] || null;
  }

  async create(playbook) {
    await this.ensureSchema();
    const built = buildClientPlaybook({
      ...playbook,
      version: playbook.version || '1.0',
      status: playbook.status || PLAYBOOK_STATUS.ACTIVE,
    });
    const existing = await this.get(built.id, built.version);
    if (existing) {
      throw new Error(
        `Client playbook already exists: ${built.id}@${built.version}`
      );
    }
    await this._insert(built);
    return built;
  }

  async createVersion(id, changes, meta = {}) {
    const current = await this.get(id);
    if (!current) throw new Error(`Unknown client playbook: ${id}`);
    const nextVersion = bumpVersion(current.version);
    const autoActivate = meta.autoActivate === true;
    const next = buildClientPlaybook({
      ...current,
      ...changes,
      id: current.id,
      clientId: changes.clientId != null ? changes.clientId : current.clientId,
      version: nextVersion,
      parentId: current.id,
      status: autoActivate
        ? PLAYBOOK_STATUS.ACTIVE
        : PLAYBOOK_STATUS.PENDING_REVIEW,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    });
    if (autoActivate) {
      await this._pool.query(
        `UPDATE client_playbooks SET status = 'superseded', updated_at = NOW()
         WHERE id = $1 AND status = 'active'`,
        [String(id)]
      );
    }
    await this._insert(next);
    return next;
  }

  async approveVersion(id, version) {
    await this.ensureSchema();
    await this._pool.query(
      `UPDATE client_playbooks SET status = 'superseded', updated_at = NOW()
       WHERE id = $1 AND status = 'active'`,
      [String(id)]
    );
    const { rows } = await this._pool.query(
      `UPDATE client_playbooks
       SET status = 'active', updated_at = NOW()
       WHERE id = $1 AND version = $2
       RETURNING *`,
      [String(id), String(version)]
    );
    if (!rows[0]) throw new Error(`Unknown version ${version} for ${id}`);
    return fromRow(rows[0]);
  }

  snapshot(playbook) {
    return buildClientPlaybook(playbook);
  }

  async _insert(playbook) {
    const p = buildClientPlaybook(playbook);
    await this._pool.query(
      `INSERT INTO client_playbooks (
        id, version, client_id, name, status,
        target_markets, value_propositions, ideal_customer, brand_voice,
        preferred_channels, outreach_sequence, offers, constraints,
        success_metrics, notes, parent_id, created_at, updated_at
      ) VALUES (
        $1,$2,$3,$4,$5,
        $6::jsonb,$7::jsonb,$8::jsonb,$9,
        $10::jsonb,$11::jsonb,$12::jsonb,$13::jsonb,
        $14::jsonb,$15,$16,$17,$18
      )`,
      [
        p.id,
        p.version,
        p.clientId != null ? String(p.clientId) : null,
        p.name,
        p.status,
        JSON.stringify(p.targetMarkets),
        JSON.stringify(p.valuePropositions),
        JSON.stringify(p.idealCustomer),
        p.brandVoice,
        JSON.stringify(p.preferredChannels),
        JSON.stringify(p.outreachSequence),
        JSON.stringify(p.offers),
        JSON.stringify(p.constraints),
        JSON.stringify(p.successMetrics),
        p.notes,
        p.parentId,
        p.createdAt,
        p.updatedAt,
      ]
    );
  }
}

function fromRow(r) {
  return buildClientPlaybook({
    id: r.id,
    version: r.version,
    clientId: r.client_id,
    name: r.name,
    status: r.status,
    targetMarkets: r.target_markets || [],
    valuePropositions: r.value_propositions || [],
    idealCustomer: r.ideal_customer || {},
    brandVoice: r.brand_voice,
    preferredChannels: r.preferred_channels || [],
    outreachSequence: r.outreach_sequence || [],
    offers: r.offers || [],
    constraints: r.constraints || [],
    successMetrics: r.success_metrics || [],
    notes: r.notes,
    parentId: r.parent_id,
    createdAt: iso(r.created_at),
    updatedAt: iso(r.updated_at),
  });
}

function iso(v) {
  if (!v) return null;
  if (typeof v === 'string') return v;
  if (v instanceof Date) return v.toISOString();
  return String(v);
}

async function ensureClientPlaybookSchema(pool) {
  await pool.query(ENSURE_SQL);
}

function createPostgresClientPlaybookStore(pool) {
  return new PostgresClientPlaybookStore(pool);
}

module.exports = {
  PostgresClientPlaybookStore,
  createPostgresClientPlaybookStore,
  ensureClientPlaybookSchema,
  ENSURE_SQL,
};
