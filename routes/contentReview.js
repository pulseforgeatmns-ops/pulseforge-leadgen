'use strict';
const express = require('express');
const { requireAuth, requireRole } = require('../middleware/auth');
const { resolveActiveTenantId } = require('../packages/max/workspace/TenantContextResolver');
const { assertAuthorizedClientSwitch } = require('../utils/tenantAuthorization');

function createContentReviewRouter(deps = {}) {
  const router = express.Router();
  router.get('/api/content-review', requireAuth, requireRole('admin', 'manager'), async (req, res) => {
    const clientId = resolveActiveTenantId(req);
    if (!clientId) return res.status(400).json({ error: 'no_active_client' });
    const access = assertAuthorizedClientSwitch(req.user, clientId);
    if (!access.ok) return res.status(access.status).json({ error: access.error });
    if (req.query?.client_id != null && Number(req.query.client_id) !== clientId) return res.status(403).json({ error: 'tenant_mismatch' });
    try {
      const { readContentReview } = require('../services/contentReview');
      const model = await readContentReview({ clientId, pool: deps.pool || require('../db'), socialStore: deps.socialStore });
      res.set('Cache-Control', 'no-store');
      res.json(model);
    } catch (_) {
      res.status(503).json({ error: 'content_review_unavailable', message: 'Content review is temporarily unavailable. Please try again.' });
    }
  });
  return router;
}
module.exports = createContentReviewRouter();
module.exports.createContentReviewRouter = createContentReviewRouter;
