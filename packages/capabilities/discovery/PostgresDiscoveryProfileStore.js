'use strict';

/**
 * Postgres Discovery Profile store (SPEC-024).
 * Profiles are immutable once used — edits create a new version row.
 */

const { buildDiscoveryProfile } = require('./types');
const { seedDiscoveryProfiles } = require('./seedProfiles');
const { bumpVersion } = require('./DiscoveryProfileStore');

const ENSURE_SQL = `
CREATE TABLE IF NOT EXISTS discovery_profiles (
  id TEXT NOT NULL,
  version TEXT NOT NULL,
  name TEXT NOT NULL,
  description TEXT,
  tenant_id TEXT,
  client_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
  industry_targets JSONB NOT NULL DEFAULT '[]'::jsonb,
  geography JSONB NOT NULL DEFAULT '{}'::jsonb,
  target_count INTEGER NOT NULL DEFAULT 50,
  required_signals JSONB NOT NULL DEFAULT '[]'::jsonb,
  preferred_signals JSONB NOT NULL DEFAULT '[]'::jsonb,
  excluded_signals JSONB NOT NULL DEFAULT '[]'::jsonb,
  ranking_weights JSONB NOT NULL DEFAULT '{}'::jsonb,
  minimum_confidence DOUBLE PRECISION NOT NULL DEFAULT 0.75,
  deduplication_rules JSONB NOT NULL DEFAULT '{}'::jsonb,
  review_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
  status TEXT NOT NULL DEFAULT 'active',
  parent_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (id, version)
);

CREATE INDEX IF NOT EXISTS discovery_profiles_name_idx
  ON discovery_profiles (name);
CREATE INDEX IF NOT EXISTS discovery_profiles_status_idx
  ON discovery_profiles (status);
CREATE INDEX IF NOT EXISTS discovery_profiles_tenant_idx
  ON discovery_profiles (tenant_id);
`;

class PostgresDiscoveryProfileStore {
  /**
   * @param {{ query: Function }} pool
   */
  constructor(pool) {
    if (!pool || typeof pool.query !== 'function') {
      throw new Error('PostgresDiscoveryProfileStore requires pool');
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
      `SELECT COUNT(*)::int AS n FROM discovery_profiles`
    );
    if (rows[0] && rows[0].n > 0) return { seeded: 0 };
    let seeded = 0;
    for (const profile of seedDiscoveryProfiles()) {
      await this._insert(profile);
      seeded += 1;
    }
    return { seeded };
  }

  async get(id, version) {
    await this.ensureSchema();
    if (version != null) {
      const { rows } = await this._pool.query(
        `SELECT * FROM discovery_profiles WHERE id = $1 AND version = $2`,
        [String(id), String(version)]
      );
      return rows[0] ? fromRow(rows[0]) : null;
    }
    const { rows } = await this._pool.query(
      `SELECT * FROM discovery_profiles
       WHERE id = $1 AND status IN ('active', 'approved')
       ORDER BY created_at DESC
       LIMIT 1`,
      [String(id)]
    );
    if (rows[0]) return fromRow(rows[0]);
    const fallback = await this._pool.query(
      `SELECT * FROM discovery_profiles WHERE id = $1
       ORDER BY created_at DESC LIMIT 1`,
      [String(id)]
    );
    return fallback.rows[0] ? fromRow(fallback.rows[0]) : null;
  }

  async list(query = {}) {
    await this.ensureSchema();
    const status = query.status || 'active';
    const params = [];
    let sql = `SELECT DISTINCT ON (id) * FROM discovery_profiles`;
    const where = [];
    if (status && status !== 'any') {
      params.push(status);
      where.push(`status = $${params.length}`);
    }
    if (query.tenantId != null) {
      params.push(String(query.tenantId));
      where.push(`(tenant_id IS NULL OR tenant_id = $${params.length})`);
    }
    if (where.length) sql += ` WHERE ${where.join(' AND ')}`;
    sql += ` ORDER BY id, created_at DESC`;
    const { rows } = await this._pool.query(sql, params);
    let out = rows.map(fromRow);
    if (query.clientId != null) {
      const clientId = String(query.clientId);
      out = out.filter(
        (p) =>
          !p.clientIds.length ||
          p.clientIds.some((c) => String(c) === clientId)
      );
    }
    return out.sort((a, b) => a.name.localeCompare(b.name));
  }

  async create(profile) {
    await this.ensureSchema();
    const built = buildDiscoveryProfile({
      ...profile,
      version: profile.version || '1.0',
      status: profile.status || 'active',
    });
    const existing = await this.get(built.id, built.version);
    if (existing) {
      throw new Error(`Discovery profile already exists: ${built.id}@${built.version}`);
    }
    await this._insert(built);
    return built;
  }

  async createVersion(id, changes, meta = {}) {
    const current = await this.get(id);
    if (!current) throw new Error(`Unknown discovery profile: ${id}`);
    const nextVersion = bumpVersion(current.version);
    const autoActivate = meta.autoActivate === true;
    const next = buildDiscoveryProfile({
      ...current,
      ...changes,
      id: current.id,
      version: nextVersion,
      parentId: current.id,
      status: autoActivate ? 'active' : 'pending_review',
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    });
    if (autoActivate) {
      await this._pool.query(
        `UPDATE discovery_profiles SET status = 'superseded', updated_at = NOW()
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
      `UPDATE discovery_profiles SET status = 'superseded', updated_at = NOW()
       WHERE id = $1 AND status = 'active'`,
      [String(id)]
    );
    const { rows } = await this._pool.query(
      `UPDATE discovery_profiles
       SET status = 'active', updated_at = NOW()
       WHERE id = $1 AND version = $2
       RETURNING *`,
      [String(id), String(version)]
    );
    if (!rows[0]) throw new Error(`Unknown version ${version} for ${id}`);
    return fromRow(rows[0]);
  }

  snapshot(profile) {
    return buildDiscoveryProfile(profile);
  }

  async _insert(profile) {
    const p = buildDiscoveryProfile(profile);
    await this._pool.query(
      `INSERT INTO discovery_profiles (
        id, version, name, description, tenant_id, client_ids,
        industry_targets, geography, target_count,
        required_signals, preferred_signals, excluded_signals,
        ranking_weights, minimum_confidence, deduplication_rules,
        review_policy, status, parent_id, created_at, updated_at
      ) VALUES (
        $1,$2,$3,$4,$5,$6::jsonb,
        $7::jsonb,$8::jsonb,$9,
        $10::jsonb,$11::jsonb,$12::jsonb,
        $13::jsonb,$14,$15::jsonb,
        $16::jsonb,$17,$18,$19,$20
      )`,
      [
        p.id,
        p.version,
        p.name,
        p.description,
        p.tenantId,
        JSON.stringify(p.clientIds),
        JSON.stringify(p.industryTargets),
        JSON.stringify(p.geography),
        p.targetCount,
        JSON.stringify(p.requiredSignals),
        JSON.stringify(p.preferredSignals),
        JSON.stringify(p.excludedSignals),
        JSON.stringify(p.rankingWeights),
        p.minimumConfidence,
        JSON.stringify(p.deduplicationRules),
        JSON.stringify(p.reviewPolicy),
        p.status,
        p.parentId,
        p.createdAt,
        p.updatedAt,
      ]
    );
  }
}

function fromRow(r) {
  return buildDiscoveryProfile({
    id: r.id,
    version: r.version,
    name: r.name,
    description: r.description,
    tenantId: r.tenant_id,
    clientIds: r.client_ids || [],
    industryTargets: r.industry_targets || [],
    geography: r.geography || {},
    targetCount: r.target_count,
    requiredSignals: r.required_signals || [],
    preferredSignals: r.preferred_signals || [],
    excludedSignals: r.excluded_signals || [],
    rankingWeights: r.ranking_weights || {},
    minimumConfidence: r.minimum_confidence,
    deduplicationRules: r.deduplication_rules || {},
    reviewPolicy: r.review_policy || {},
    status: r.status,
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

async function ensureDiscoveryProfileSchema(pool) {
  await pool.query(ENSURE_SQL);
}

function createPostgresDiscoveryProfileStore(pool) {
  return new PostgresDiscoveryProfileStore(pool);
}

module.exports = {
  PostgresDiscoveryProfileStore,
  createPostgresDiscoveryProfileStore,
  ensureDiscoveryProfileSchema,
  ENSURE_SQL,
};
