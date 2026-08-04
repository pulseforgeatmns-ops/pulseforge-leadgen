'use strict';

/**
 * SPEC-070 — GET-only Intelligence Seed Libraries APIs.
 * Reference/guidance only — not observed evidence.
 * No Max wiring, recommendations, or writes.
 *
 * GET /api/v1/intelligence/seed-libraries
 * GET /api/v1/intelligence/seed-libraries/:libraryId
 */

const express = require('express');
const router = express.Router();
const { requireAuth, requireRole } = require('../middleware/auth');
const {
  listSeedLibraries,
  getSeedLibrary,
} = require('../services/intelligenceSeedLibraryQuery');

const requireAdmin = [requireAuth, requireRole('admin', 'manager')];

function noStore(res) {
  res.set('Cache-Control', 'no-store');
}

router.get('/api/v1/intelligence/seed-libraries', requireAdmin, async (req, res) => {
  try {
    const libraries = await listSeedLibraries({
      category: req.query.category,
      tag: req.query.tag,
      q: req.query.q,
      trustLevel: req.query.trust_level,
      scopeKey: req.query.scope_key || req.query.scope,
      scopeValue: req.query.scope_value,
      includeDisabled: req.query.include_disabled,
      limit: req.query.limit,
    });
    noStore(res);
    return res.json({
      kind: 'seed_library_reference',
      isEvidence: false,
      libraries,
      internal: true,
    });
  } catch (err) {
    console.error('[seed-libraries] list', err);
    return res.status(500).json({
      error: 'seed_libraries_list_failed',
      message: err && err.message ? String(err.message) : 'failed',
    });
  }
});

router.get('/api/v1/intelligence/seed-libraries/:libraryId', requireAdmin, async (req, res) => {
  try {
    const library = await getSeedLibrary(req.params.libraryId, {
      includeDisabled: req.query.include_disabled,
    });
    if (!library) {
      return res.status(404).json({ error: 'seed_library_not_found' });
    }
    noStore(res);
    return res.json({
      kind: 'seed_library_reference',
      isEvidence: false,
      library,
      internal: true,
    });
  } catch (err) {
    console.error('[seed-libraries] detail', err);
    return res.status(500).json({
      error: 'seed_libraries_detail_failed',
      message: err && err.message ? String(err.message) : 'failed',
    });
  }
});

module.exports = router;
