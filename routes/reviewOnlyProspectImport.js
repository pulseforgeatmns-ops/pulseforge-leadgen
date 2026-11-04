'use strict';

const express = require('express');
const path = require('path');
const pool = require('../db');
const { requireAuth, requireRole } = require('../middleware/auth');
const { resolveActiveTenantId } = require('../packages/max/workspace/TenantContextResolver');
const {
  previewReviewOnlyContacts,
  importReviewOnlyContacts,
} = require('../services/reviewOnlyProspectImportService');

const router = express.Router();
const requireWrite = [requireAuth, requireRole('admin', 'manager')];

function tenantId(req) {
  const id = Number(resolveActiveTenantId(req));
  return Number.isSafeInteger(id) && id > 0 ? id : null;
}

function publicError(error) {
  return {
    ok: false,
    error: error.code || 'review_only_import_failed',
    message: error.message || 'Review-only prospect import failed.',
  };
}

router.get('/review-only-prospect-import', requireWrite, (_req, res) => {
  res.sendFile(path.join(__dirname, '..', 'public', 'review-only-prospect-import.html'));
});

router.post('/api/v1/review-only-prospects/preview', requireWrite, (req, res) => {
  try {
    const clientId = tenantId(req);
    if (!clientId) return res.status(400).json({ ok: false, error: 'active_tenant_required' });
    const contacts = previewReviewOnlyContacts(req.body?.contacts);
    return res.json({
      ok: true,
      tenant_id: clientId,
      review_only: true,
      outreach_authorized: false,
      contacts,
    });
  } catch (error) {
    return res.status(422).json(publicError(error));
  }
});

router.post('/api/v1/review-only-prospects/import', requireWrite, async (req, res) => {
  try {
    const clientId = tenantId(req);
    if (!clientId) return res.status(400).json({ ok: false, error: 'active_tenant_required' });
    if (req.body?.confirmation !== 'IMPORT_REVIEW_ONLY') {
      return res.status(409).json({
        ok: false,
        error: 'explicit_review_only_confirmation_required',
        message: 'Preview the contacts and confirm the review-only import.',
      });
    }
    const actor = req.user || req.session?.user || {};
    const result = await importReviewOnlyContacts(req.body?.contacts, {
      clientId,
      actorId: actor.id || null,
      db: pool,
    });
    return res.status(201).json({
      ok: true,
      tenant_id: clientId,
      review_only: true,
      outreach_authorized: false,
      ...result,
    });
  } catch (error) {
    console.error('[review-only-prospect-import]', error);
    const status = error.code && /required|duplicate|limit/.test(error.code) ? 422 : 500;
    return res.status(status).json(publicError(error));
  }
});

module.exports = router;
