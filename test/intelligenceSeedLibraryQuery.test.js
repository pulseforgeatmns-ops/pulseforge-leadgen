'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const path = require('path');
const fs = require('fs');
const {
  listSeedLibraries,
  getSeedLibrary,
  mapSeedLibraryRow,
  resolveContent,
  buildFilterClause,
} = require('../services/intelligenceSeedLibraryQuery');

function baseRow(overrides = {}) {
  return {
    library_id: 'sales_methodology.discovery_checklist',
    title: 'Discovery Call Checklist',
    category: 'sales_methodology',
    source_type: 'curated_operator',
    trust_level: 'high',
    scope: { audience: 'global', verticals: [] },
    summary: 'Operator-curated discovery checklist.',
    content_text: 'Confirm decision-maker presence.',
    content_ref: null,
    tags: ['discovery', 'sales'],
    provenance: {
      curated_by: 'pulseforge-ops',
      curated_at: '2026-08-03T00:00:00.000Z',
      notes: 'internal',
    },
    enabled: true,
    version: 2,
    created_at: '2026-08-03T00:00:00.000Z',
    updated_at: '2026-08-03T00:00:00.000Z',
    ...overrides,
  };
}

describe('intelligenceSeedLibraryQuery mapping', () => {
  it('stamps reference guidance shape with provenance, trust_level, and version', () => {
    const mapped = mapSeedLibraryRow(baseRow());
    assert.equal(mapped.kind, 'seed_library_reference');
    assert.equal(mapped.isEvidence, false);
    assert.equal(mapped.libraryId, 'sales_methodology.discovery_checklist');
    assert.equal(mapped.trustLevel, 'high');
    assert.equal(mapped.version, 2);
    assert.equal(mapped.enabled, true);
    assert.equal(mapped.provenance.curated_by, 'pulseforge-ops');
    assert.ok(mapped.provenance.notes);
  });

  it('resolveContent prefers content_text over content_ref', () => {
    const resolved = resolveContent(
      baseRow({
        content_text: 'inline',
        content_ref: 'data/seed-libraries/missing.json',
      })
    );
    assert.equal(resolved.content, 'inline');
    assert.equal(resolved.contentLoadError, null);
  });

  it('resolveContent loads content_ref from repo when content_text absent', () => {
    const seedPath = 'data/seed-libraries/sales_methodology.discovery_checklist.json';
    assert.ok(fs.existsSync(path.join(__dirname, '..', seedPath)));
    const resolved = resolveContent(
      baseRow({ content_text: null, content_ref: seedPath })
    );
    assert.equal(resolved.contentLoadError, null);
    assert.match(resolved.content, /library_id/);
  });
});

describe('intelligenceSeedLibraryQuery filters', () => {
  it('defaults to enabled-only and supports category/tag/scope/q', () => {
    const { whereSql, params } = buildFilterClause({
      category: 'industry_playbook',
      tag: 'cleaning',
      trustLevel: 'high',
      scopeKey: 'verticals',
      scopeValue: 'cleaning',
      q: 'beachhead',
    });
    assert.match(whereSql, /enabled = TRUE/);
    assert.match(whereSql, /category = \$/);
    assert.match(whereSql, /ANY\(tags\)/);
    assert.match(whereSql, /trust_level = \$/);
    assert.match(whereSql, /scope/);
    assert.match(whereSql, /ILIKE/);
    assert.ok(params.includes('industry_playbook'));
    assert.ok(params.includes('cleaning'));
    assert.ok(params.includes('high'));
    assert.ok(params.includes('%beachhead%'));
  });

  it('omits enabled filter when includeDisabled is set', () => {
    const { whereSql } = buildFilterClause({ includeDisabled: true });
    assert.equal(whereSql.includes('enabled = TRUE'), false);
  });
});

describe('intelligenceSeedLibraryQuery with mock pool', () => {
  it('listSeedLibraries maps rows and never marks them as evidence', async () => {
    const pool = {
      async query(sql, params) {
        assert.match(sql, /FROM intelligence_seed_libraries/);
        assert.match(sql, /enabled = TRUE/);
        assert.ok(params.includes('sales_methodology'));
        return { rows: [baseRow(), baseRow({ enabled: false, library_id: 'x.disabled' })] };
      },
    };
    const libraries = await listSeedLibraries({
      pool,
      category: 'sales_methodology',
    });
    assert.equal(libraries.length, 2);
    for (const lib of libraries) {
      assert.equal(lib.isEvidence, false);
      assert.equal(lib.kind, 'seed_library_reference');
      assert.ok(lib.provenance);
      assert.ok(lib.trustLevel);
      assert.ok(lib.version >= 1);
    }
  });

  it('getSeedLibrary returns versioned detail with resolved content', async () => {
    const pool = {
      async query(sql, params) {
        assert.match(sql, /library_id = \$1/);
        assert.equal(params[0], 'sales_methodology.discovery_checklist');
        assert.equal(params[1], false);
        return { rows: [baseRow({ version: 3 })] };
      },
    };
    const library = await getSeedLibrary('sales_methodology.discovery_checklist', {
      pool,
    });
    assert.equal(library.version, 3);
    assert.equal(library.content, 'Confirm decision-maker presence.');
    assert.equal(library.isEvidence, false);
    assert.equal(library.contentLoadError, null);
  });

  it('getSeedLibrary returns null when missing', async () => {
    const pool = {
      async query() {
        return { rows: [] };
      },
    };
    const library = await getSeedLibrary('missing.id', { pool });
    assert.equal(library, null);
  });
});
