'use strict';

/**
 * SPEC-070 — Intelligence Seed Libraries query layer.
 * Reference/guidance only — NOT observed evidence.
 * Separate from Market Intelligence observations and Knowledge facts.
 */

const fs = require('fs');
const path = require('path');

const defaultPool = require('../db');

const REPO_ROOT = path.join(__dirname, '..');

const CATEGORIES = Object.freeze([
  'sales_methodology',
  'industry_playbook',
  'offer_positioning',
  'operating_preferences',
  'market_reference',
]);

const SOURCE_TYPES = Object.freeze([
  'curated_operator',
  'public_method_summary',
  'internal_preference',
  'market_background',
]);

const TRUST_LEVELS = Object.freeze(['high', 'medium', 'low', 'provisional']);

function clampLimit(limit, fallback = 100) {
  const n = Number(limit);
  if (!Number.isFinite(n) || n < 1) return fallback;
  return Math.min(Math.floor(n), 500);
}

function parseTruthy(value) {
  if (value === true || value === 1) return true;
  const s = String(value == null ? '' : value).trim().toLowerCase();
  return s === '1' || s === 'true' || s === 'yes';
}

/**
 * Map a DB row to the public seed-library reference shape.
 * Always stamps kind/isEvidence so consumers never treat this as evidence.
 */
function mapSeedLibraryRow(row, { content = undefined, contentLoadError = null } = {}) {
  if (!row) return null;
  const base = {
    kind: 'seed_library_reference',
    isEvidence: false,
    libraryId: row.library_id,
    title: row.title,
    category: row.category,
    sourceType: row.source_type,
    trustLevel: row.trust_level,
    scope: row.scope || {},
    summary: row.summary,
    contentText: row.content_text ?? null,
    contentRef: row.content_ref ?? null,
    tags: Array.isArray(row.tags) ? row.tags : [],
    provenance: row.provenance || {},
    enabled: row.enabled !== false,
    version: Number(row.version) || 1,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
  if (content !== undefined) {
    base.content = content;
    base.contentLoadError = contentLoadError;
  }
  return base;
}

function resolveContent(row) {
  if (row.content_text != null && String(row.content_text).length > 0) {
    return { content: String(row.content_text), contentLoadError: null };
  }
  const ref = row.content_ref ? String(row.content_ref).trim() : '';
  if (!ref) {
    return { content: null, contentLoadError: 'content_missing' };
  }
  if (ref.includes('..') || path.isAbsolute(ref)) {
    return { content: null, contentLoadError: 'content_ref_invalid' };
  }
  const abs = path.join(REPO_ROOT, ref);
  if (!abs.startsWith(REPO_ROOT)) {
    return { content: null, contentLoadError: 'content_ref_invalid' };
  }
  try {
    if (!fs.existsSync(abs)) {
      return { content: null, contentLoadError: 'content_ref_not_found' };
    }
    return { content: fs.readFileSync(abs, 'utf8'), contentLoadError: null };
  } catch (err) {
    return {
      content: null,
      contentLoadError: err && err.message ? String(err.message) : 'content_load_failed',
    };
  }
}

function buildFilterClause(options = {}) {
  const where = [];
  const params = [];
  const includeDisabled = parseTruthy(options.includeDisabled);

  if (!includeDisabled) {
    where.push('enabled = TRUE');
  }

  if (options.category) {
    params.push(String(options.category).trim());
    where.push(`category = $${params.length}`);
  }

  if (options.tag) {
    params.push(String(options.tag).trim());
    where.push(`$${params.length} = ANY(tags)`);
  }

  if (options.trustLevel) {
    params.push(String(options.trustLevel).trim());
    where.push(`trust_level = $${params.length}`);
  }

  if (options.scopeKey) {
    const key = String(options.scopeKey).trim();
    if (options.scopeValue != null && String(options.scopeValue).length) {
      params.push(key);
      params.push(String(options.scopeValue));
      // Match scalar or array membership inside scope JSON
      where.push(
        `(scope->>$${params.length - 1} = $${params.length}
          OR scope->$${params.length - 1} ? $${params.length}
          OR (jsonb_typeof(scope->$${params.length - 1}) = 'array'
              AND scope->$${params.length - 1} ? $${params.length}))`
      );
    } else {
      params.push(key);
      where.push(`scope ? $${params.length}`);
    }
  }

  const q = options.q != null ? String(options.q).trim() : '';
  if (q) {
    params.push(`%${q}%`);
    const p = params.length;
    where.push(
      `(title ILIKE $${p} OR summary ILIKE $${p} OR EXISTS (
         SELECT 1 FROM unnest(tags) t WHERE t ILIKE $${p}
       ))`
    );
  }

  return {
    whereSql: where.length ? `WHERE ${where.join(' AND ')}` : '',
    params,
  };
}

/**
 * List / filter seed libraries. Defaults to enabled rows only.
 */
async function listSeedLibraries(options = {}) {
  const pool = options.pool || defaultPool;
  const limit = clampLimit(options.limit, 100);
  const { whereSql, params } = buildFilterClause(options);
  params.push(limit);

  const result = await pool.query(
    `SELECT *
       FROM intelligence_seed_libraries
      ${whereSql}
      ORDER BY category ASC, title ASC, library_id ASC
      LIMIT $${params.length}`,
    params
  );

  return result.rows.map((row) => mapSeedLibraryRow(row));
}

/**
 * Search alias — same filters as list (includes text match via options.q).
 */
async function searchSeedLibraries(options = {}) {
  return listSeedLibraries(options);
}

/**
 * Fetch one library by id, resolving content_text or content_ref.
 */
async function getSeedLibrary(libraryId, options = {}) {
  const pool = options.pool || defaultPool;
  const id = String(libraryId || '').trim();
  if (!id) return null;

  const includeDisabled = parseTruthy(options.includeDisabled);
  const result = await pool.query(
    `SELECT *
       FROM intelligence_seed_libraries
      WHERE library_id = $1
        AND ($2::boolean OR enabled = TRUE)
      LIMIT 1`,
    [id, includeDisabled]
  );

  const row = result.rows[0];
  if (!row) return null;

  const resolved = resolveContent(row);
  return mapSeedLibraryRow(row, resolved);
}

module.exports = {
  CATEGORIES,
  SOURCE_TYPES,
  TRUST_LEVELS,
  listSeedLibraries,
  searchSeedLibraries,
  getSeedLibrary,
  mapSeedLibraryRow,
  resolveContent,
  buildFilterClause,
  parseTruthy,
};
