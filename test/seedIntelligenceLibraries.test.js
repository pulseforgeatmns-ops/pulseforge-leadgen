'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const {
  UPSERT_SQL,
  validateSeedEntry,
  loadSeedEntries,
  upsertParams,
  upsertSeedLibraries,
  DEFAULT_DIR,
} = require('../scripts/seedIntelligenceLibraries');
const {
  CATEGORIES,
  SOURCE_TYPES,
  TRUST_LEVELS,
} = require('../services/intelligenceSeedLibraryQuery');

describe('seedIntelligenceLibraries curated JSON', () => {
  it('loads one enabled versioned entry per initial category with provenance', () => {
    const entries = loadSeedEntries(DEFAULT_DIR);
    assert.ok(entries.length >= 5);

    const byCategory = new Set(entries.map((e) => e.category));
    for (const category of CATEGORIES) {
      assert.ok(byCategory.has(category), `missing category ${category}`);
    }

    for (const entry of entries) {
      assert.ok(SOURCE_TYPES.includes(entry.sourceType));
      assert.ok(TRUST_LEVELS.includes(entry.trustLevel));
      assert.equal(typeof entry.enabled, 'boolean');
      assert.ok(Number.isInteger(entry.version) && entry.version >= 1);
      assert.ok(entry.provenance.curated_by);
      assert.ok(entry.provenance.curated_at);
      assert.ok(entry.provenance.notes);
      assert.ok(entry.contentText || entry.contentRef);
    }
  });

  it('rejects entries missing provenance or invalid enums', () => {
    const bad = validateSeedEntry(
      {
        library_id: 'x.bad',
        title: 'Bad',
        category: 'not_a_category',
        source_type: 'curated_operator',
        trust_level: 'high',
        summary: 'x',
        content_text: 'y',
        provenance: { curated_by: 'a' },
        version: 0,
      },
      'bad.json'
    );
    assert.equal(bad.ok, false);
    assert.ok(bad.errors.some((e) => e.includes('category')));
    assert.ok(bad.errors.some((e) => e.includes('provenance.curated_at')));
    assert.ok(bad.errors.some((e) => e.includes('version')));
  });
});

describe('seedIntelligenceLibraries upsert', () => {
  it('uses ON CONFLICT upsert SQL with enabled and version', () => {
    assert.match(UPSERT_SQL, /INSERT INTO intelligence_seed_libraries/);
    assert.match(UPSERT_SQL, /ON CONFLICT \(library_id\) DO UPDATE/);
    assert.match(UPSERT_SQL, /enabled = EXCLUDED\.enabled/);
    assert.match(UPSERT_SQL, /version = EXCLUDED\.version/);
    assert.equal(UPSERT_SQL.includes('market_'), false);
    assert.equal(UPSERT_SQL.includes('knowledge_'), false);
  });

  it('upserts via mock pool with expected params', async () => {
    const entries = loadSeedEntries(DEFAULT_DIR).slice(0, 1);
    const seen = [];
    const pool = {
      async query(sql, params) {
        seen.push({ sql, params });
        return { rows: [] };
      },
    };
    const upserted = await upsertSeedLibraries(entries, { pool });
    assert.equal(upserted.length, 1);
    assert.equal(seen.length, 1);
    assert.equal(seen[0].sql, UPSERT_SQL);
    assert.deepEqual(seen[0].params, upsertParams(entries[0]));
    assert.equal(upserted[0].libraryId, entries[0].libraryId);
    assert.equal(upserted[0].version, entries[0].version);
  });

  it('seed directory path resolves under data/seed-libraries', () => {
    assert.ok(DEFAULT_DIR.endsWith(path.join('data', 'seed-libraries')));
  });
});
