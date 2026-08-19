'use strict';

/**
 * SPEC-113 / SPEC-115 — persist AIC workspaces and published AIMs.
 * Compiler stays in-memory for extraction; this layer survives process restart
 * and binds published models to tenant client_id so Scout never loads a seed.
 */

const defaultPool = require('../db');

async function ensureAicPersistenceSchema(pool = defaultPool) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS aic_workspaces (
      id TEXT PRIMARY KEY,
      client_key TEXT NOT NULL,
      client_id INTEGER,
      status TEXT NOT NULL,
      version INTEGER NOT NULL DEFAULT 1,
      aim_id TEXT,
      payload JSONB NOT NULL DEFAULT '{}'::jsonb,
      is_operating_fact BOOLEAN NOT NULL DEFAULT FALSE,
      compiled_at TIMESTAMPTZ,
      approved_at TIMESTAMPTZ,
      approved_by TEXT,
      published_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS aim_models (
      id TEXT PRIMARY KEY,
      client_key TEXT NOT NULL,
      client_id INTEGER,
      version INTEGER NOT NULL DEFAULT 1,
      status TEXT NOT NULL,
      mission JSONB NOT NULL DEFAULT '{}'::jsonb,
      icp JSONB NOT NULL DEFAULT '{}'::jsonb,
      transformation JSONB NOT NULL DEFAULT '{}'::jsonb,
      pain_ontology JSONB NOT NULL DEFAULT '{}'::jsonb,
      knowledge JSONB NOT NULL DEFAULT '[]'::jsonb,
      payload JSONB NOT NULL DEFAULT '{}'::jsonb,
      is_operating_fact BOOLEAN NOT NULL DEFAULT FALSE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
}

function asJson(value, fallback) {
  if (value == null) return fallback;
  return value;
}

async function persistAicWorkspace(workspace, pool = defaultPool) {
  if (!workspace || !workspace.id) return null;
  await ensureAicPersistenceSchema(pool);
  const payload = JSON.parse(JSON.stringify(workspace));
  await pool.query(
    `INSERT INTO aic_workspaces (
        id, client_key, client_id, status, version, aim_id, payload,
        compiled_at, approved_at, approved_by, published_at, updated_at
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NOW())
      ON CONFLICT (id) DO UPDATE SET
        client_key = EXCLUDED.client_key,
        client_id = EXCLUDED.client_id,
        status = EXCLUDED.status,
        version = EXCLUDED.version,
        aim_id = EXCLUDED.aim_id,
        payload = EXCLUDED.payload,
        compiled_at = EXCLUDED.compiled_at,
        approved_at = EXCLUDED.approved_at,
        approved_by = EXCLUDED.approved_by,
        published_at = EXCLUDED.published_at,
        updated_at = NOW()`,
    [
      workspace.id,
      workspace.clientKey || workspace.client_key || '',
      workspace.clientId || workspace.client_id || null,
      workspace.status || 'new',
      Number(workspace.version) || 1,
      workspace.aimId || null,
      payload,
      workspace.compiledAt || null,
      workspace.approvedAt || null,
      workspace.approvedBy || null,
      workspace.publishedAt || null,
    ]
  );
  return workspace;
}

async function loadAicWorkspace(id, pool = defaultPool) {
  if (!id) return null;
  try {
    const { rows } = await pool.query(
      'SELECT payload FROM aic_workspaces WHERE id = $1 LIMIT 1',
      [id]
    );
    return rows[0]?.payload || null;
  } catch (err) {
    if (/does not exist|relation .* does not exist/i.test(err.message || '')) return null;
    throw err;
  }
}

async function listAicWorkspaces({ clientId, clientKey } = {}, pool = defaultPool) {
  try {
    const params = [];
    const where = [];
    if (clientId != null) {
      params.push(Number(clientId));
      where.push(`client_id = $${params.length}`);
    }
    if (clientKey) {
      params.push(String(clientKey));
      where.push(`client_key = $${params.length}`);
    }
    const sql = `SELECT payload FROM aic_workspaces ${
      where.length ? `WHERE ${where.join(' AND ')}` : ''
    } ORDER BY updated_at DESC`;
    const { rows } = await pool.query(sql, params);
    return rows.map((r) => r.payload).filter(Boolean);
  } catch (err) {
    if (/does not exist|relation .* does not exist/i.test(err.message || '')) return [];
    throw err;
  }
}

async function persistPublishedAim(aim, { clientId } = {}, pool = defaultPool) {
  if (!aim) return null;
  await ensureAicPersistenceSchema(pool);
  const cid = clientId != null ? Number(clientId) : (aim.clientId || aim.client_id || null);
  const copy = JSON.parse(JSON.stringify(aim));
  copy.clientId = cid;
  copy.client_id = cid;
  copy.status = copy.status || 'published';
  await pool.query(
    `INSERT INTO aim_models (
        id, client_key, client_id, version, status,
        mission, icp, transformation, pain_ontology, knowledge, payload, updated_at
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NOW())
      ON CONFLICT (id) DO UPDATE SET
        client_key = EXCLUDED.client_key,
        client_id = EXCLUDED.client_id,
        version = EXCLUDED.version,
        status = EXCLUDED.status,
        mission = EXCLUDED.mission,
        icp = EXCLUDED.icp,
        transformation = EXCLUDED.transformation,
        pain_ontology = EXCLUDED.pain_ontology,
        knowledge = EXCLUDED.knowledge,
        payload = EXCLUDED.payload,
        updated_at = NOW()`,
    [
      copy.id,
      copy.clientKey || copy.client_key || '',
      cid,
      Number(copy.version) || 1,
      copy.status,
      asJson(copy.mission, {}),
      asJson(copy.icp, {}),
      asJson(copy.transformation, {}),
      asJson(copy.painOntology || copy.pain_ontology, {}),
      asJson(copy.knowledge, []),
      copy,
    ]
  );
  return copy;
}

async function loadPublishedAimForClient(clientId, pool = defaultPool) {
  if (clientId == null) return null;
  try {
    const { rows } = await pool.query(
      `SELECT payload, id, client_key, client_id, status, version,
              mission, icp, transformation, pain_ontology, knowledge
       FROM aim_models
       WHERE client_id = $1 AND status IN ('published', 'complete')
       ORDER BY version DESC
       LIMIT 1`,
      [Number(clientId)]
    );
    const row = rows[0];
    if (!row) return null;
    const aim = row.payload && typeof row.payload === 'object' ? row.payload : {};
    return {
      ...aim,
      id: row.id,
      clientKey: row.client_key,
      client_id: row.client_id,
      clientId: row.client_id,
      status: row.status,
      version: row.version,
      mission: row.mission || aim.mission,
      icp: row.icp || aim.icp,
      transformation: row.transformation || aim.transformation,
      painOntology: row.pain_ontology || aim.painOntology,
      knowledge: row.knowledge || aim.knowledge,
    };
  } catch (err) {
    if (/does not exist|relation .* does not exist/i.test(err.message || '')) return null;
    throw err;
  }
}

module.exports = {
  ensureAicPersistenceSchema,
  persistAicWorkspace,
  loadAicWorkspace,
  listAicWorkspaces,
  persistPublishedAim,
  loadPublishedAimForClient,
};
