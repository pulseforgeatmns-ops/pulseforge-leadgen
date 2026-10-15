'use strict';
const express = require('express');
const path = require('path');
const { requireAuth, requireRole } = require('../middleware/auth');
const { getApiUsageReport } = require('../services/apiUsageService');
const { updateApiPolicy, ApiSpendError } = require('../utils/apiSpendControl');
const router = express.Router();
const operatorOnly = [requireAuth, requireRole('admin', 'manager')];

router.get('/api-usage', operatorOnly, (_req, res) => {
  res.set('Cache-Control', 'no-store');
  res.sendFile(path.join(__dirname, '../public/api-usage.html'));
});
router.get('/api/v1/api-usage', operatorOnly, async (req, res) => {
  res.set('Cache-Control', 'no-store');
  try {
    const days = req.query.days == null ? 14 : Number(req.query.days);
    const tenantId = req.query.tenant_id == null ? null : Number(req.query.tenant_id);
    res.json(await getApiUsageReport({ days, tenantId }));
  } catch (err) {
    res.status(/^invalid_report_/.test(err.message) ? 400 : 503).json({ error: /^invalid_report_/.test(err.message) ? err.message : 'api_usage_unavailable' });
  }
});
router.put('/api/v1/api-usage/policies/:provider', operatorOnly, async (req, res) => {
  res.set('Cache-Control', 'no-store');
  // Browser writes require a same-origin request; no policy edits from another site.
  try {
    if (!req.get('origin') || new URL(req.get('origin')).host !== req.get('host')) return res.status(403).json({ error: 'same_origin_required' });
  } catch { return res.status(403).json({ error: 'same_origin_required' }); }
  try {
    // The HTTP caller cannot self-select an actor or grant Max broader authority.
    const expectedUpdatedAt = req.get('If-Match');
    if (!expectedUpdatedAt) return res.status(428).json({ error: 'api_policy_version_required' });
    const policy = await updateApiPolicy(req.params.provider, req.body, { actor: 'operator', expectedUpdatedAt });
    res.json({ policy });
  } catch (err) {
    res.status(err.code === 'api_policy_changed' ? 409 : err instanceof ApiSpendError ? 400 : 503).json({ error: err instanceof ApiSpendError ? err.code : 'api_policy_update_unavailable' });
  }
});
module.exports = router;
