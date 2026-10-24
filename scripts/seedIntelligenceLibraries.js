'use strict';

/**
 * SPEC-070 — Upsert curated Intelligence Seed Libraries into Postgres.
 *
 * Usage:
 *   node scripts/seedIntelligenceLibraries.js
 *   node scripts/seedIntelligenceLibraries.js --dir data/seed-libraries
 *
 * Seed libraries are reference/guidance only — not observed evidence.
 * Does not write to market_* or knowledge_* tables.
 */

require('dotenv').config();

const fs = require('fs');
const path = require('path');
const {
  CATEGORIES,
  SOURCE_TYPES,
  TRUST_LEVELS,
} = require('../services/intelligenceSeedLibraryQuery');

const REPO_ROOT = path.join(__dirname, '..');
const DEFAULT_DIR = path.join(REPO_ROOT, 'data', 'seed-libraries');

const UPSERT_SQL = `
INSERT INTO intelligence_seed_libraries (
  library_id, title, category, source_type, trust_level, scope,
  summary, content_text, content_ref, tags, provenance, enabled, version,
  created_at, updated_at
) VALUES (
  $1, $2, $3, $4, $5, $6::jsonb,
  $7, $8, $9, $10::text[], $11::jsonb, $12, $13,
  NOW(), NOW()
)
ON CONFLICT (library_id) DO UPDATE SET
  title = EXCLUDED.title,
  category = EXCLUDED.category,
  source_type = EXCLUDED.source_type,
  trust_level = EXCLUDED.trust_level,
  scope = EXCLUDED.scope,
  summary = EXCLUDED.summary,
  content_text = EXCLUDED.content_text,
  content_ref = EXCLUDED.content_ref,
  tags = EXCLUDED.tags,
  provenance = EXCLUDED.provenance,
  enabled = EXCLUDED.enabled,
  version = EXCLUDED.version,
  updated_at = NOW()
`;

function parseArgs(argv = process.argv.slice(2)) {
  let dir = DEFAULT_DIR;
  for (let i = 0; i < argv.length; i += 1) {
    if (argv[i] === '--dir' && argv[i + 1]) {
      dir = path.resolve(argv[i + 1]);
      i += 1;
    }
  }
  return { dir };
}

function validateSeedEntry(raw, filePath) {
  const errors = [];
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) {
    return { ok: false, errors: [`${filePath}: root must be an object`] };
  }

  const libraryId = String(raw.library_id || '').trim();
  if (!libraryId) errors.push('library_id required');

  const title = String(raw.title || '').trim();
  if (!title) errors.push('title required');

  const category = String(raw.category || '').trim();
  if (!CATEGORIES.includes(category)) {
    errors.push(`category must be one of: ${CATEGORIES.join(', ')}`);
  }

  const sourceType = String(raw.source_type || '').trim();
  if (!SOURCE_TYPES.includes(sourceType)) {
    errors.push(`source_type must be one of: ${SOURCE_TYPES.join(', ')}`);
  }

  const trustLevel = String(raw.trust_level || '').trim();
  if (!TRUST_LEVELS.includes(trustLevel)) {
    errors.push(`trust_level must be one of: ${TRUST_LEVELS.join(', ')}`);
  }

  const summary = String(raw.summary || '').trim();
  if (!summary) errors.push('summary required');

  const contentText =
    raw.content_text == null || raw.content_text === ''
      ? null
      : String(raw.content_text);
  const contentRef =
    raw.content_ref == null || raw.content_ref === ''
      ? null
      : String(raw.content_ref).trim();
  if (!contentText && !contentRef) {
    errors.push('content_text or content_ref required');
  }

  const provenance = raw.provenance;
  if (!provenance || typeof provenance !== 'object' || Array.isArray(provenance)) {
    errors.push('provenance object required');
  } else {
    if (!String(provenance.curated_by || '').trim()) {
      errors.push('provenance.curated_by required');
    }
    if (!String(provenance.curated_at || '').trim()) {
      errors.push('provenance.curated_at required');
    }
    if (!String(provenance.notes || '').trim()) {
      errors.push('provenance.notes required');
    }
  }

  const version = Number(raw.version == null ? 1 : raw.version);
  if (!Number.isInteger(version) || version < 1) {
    errors.push('version must be an integer >= 1');
  }

  const enabled = raw.enabled === undefined ? true : Boolean(raw.enabled);
  const scope =
    raw.scope && typeof raw.scope === 'object' && !Array.isArray(raw.scope)
      ? raw.scope
      : {};
  const tags = Array.isArray(raw.tags)
    ? raw.tags.map((t) => String(t).trim()).filter(Boolean)
    : [];

  if (errors.length) {
    return { ok: false, errors: errors.map((e) => `${filePath}: ${e}`) };
  }

  return {
    ok: true,
    entry: {
      libraryId,
      title,
      category,
      sourceType,
      trustLevel,
      scope,
      summary,
      contentText,
      contentRef,
      tags,
      provenance,
      enabled,
      version,
    },
  };
}

function loadSeedEntries(dir) {
  if (!fs.existsSync(dir)) {
    throw new Error(`seed directory not found: ${dir}`);
  }
  const files = fs
    .readdirSync(dir)
    .filter((f) => f.endsWith('.json'))
    .sort();
  if (!files.length) {
    throw new Error(`no .json seed files in ${dir}`);
  }

  const entries = [];
  const errors = [];
  for (const file of files) {
    const full = path.join(dir, file);
    let raw;
    try {
      raw = JSON.parse(fs.readFileSync(full, 'utf8'));
    } catch (err) {
      errors.push(`${full}: invalid JSON (${err.message})`);
      continue;
    }
    const result = validateSeedEntry(raw, full);
    if (!result.ok) {
      errors.push(...result.errors);
      continue;
    }
    entries.push(result.entry);
  }

  if (errors.length) {
    const err = new Error(`seed validation failed:\n${errors.join('\n')}`);
    err.validationErrors = errors;
    throw err;
  }
  return entries;
}

function upsertParams(entry) {
  return [
    entry.libraryId,
    entry.title,
    entry.category,
    entry.sourceType,
    entry.trustLevel,
    JSON.stringify(entry.scope || {}),
    entry.summary,
    entry.contentText,
    entry.contentRef,
    entry.tags,
    JSON.stringify(entry.provenance),
    entry.enabled,
    entry.version,
  ];
}

async function upsertSeedLibraries(entries, { pool } = {}) {
  if (!pool) {
    throw new Error('pool is required');
  }
  const upserted = [];
  for (const entry of entries) {
    await pool.query(UPSERT_SQL, upsertParams(entry));
    upserted.push({
      libraryId: entry.libraryId,
      version: entry.version,
      enabled: entry.enabled,
      category: entry.category,
    });
  }
  return upserted;
}

async function main() {
  const { dir } = parseArgs();
  const entries = loadSeedEntries(dir);
  const pool = require('../db');
  try {
    const upserted = await upsertSeedLibraries(entries, { pool });
    console.log(
      JSON.stringify(
        {
          ok: true,
          kind: 'seed_library_reference',
          isEvidence: false,
          dir,
          count: upserted.length,
          upserted,
        },
        null,
        2
      )
    );
  } finally {
    // CLI owns the process; ending the shared pool is acceptable on exit only.
    if (pool && typeof pool.end === 'function') {
      await pool.end();
    }
  }
}

module.exports = {
  UPSERT_SQL,
  parseArgs,
  validateSeedEntry,
  loadSeedEntries,
  upsertParams,
  upsertSeedLibraries,
  DEFAULT_DIR,
};

if (require.main === module) {
  main().catch((err) => {
    console.error(err && err.message ? err.message : err);
    process.exitCode = 1;
  });
}
