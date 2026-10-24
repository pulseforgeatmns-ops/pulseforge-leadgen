'use strict';

/**
 * Intelligence Navigation APIs (SPEC-010).
 *
 * GET /api/v1/recommendations/:id
 * GET /api/v1/companies/:id/intelligence
 */

const express = require('express');
const router = express.Router();
const { requireAuth, requireRole } = require('../middleware/auth');
const {
  getRequestClientId,
  normalizeClientId,
} = require('../utils/clientContext');
const { getMaxRuntime } = require('../utils/maxRuntime');

const requireDashboardRead = [
  requireAuth,
  requireRole('admin', 'manager', 'viewer', 'client'),
];

/**
 * GET /api/v1/recommendations/:id
 */
router.get(
  '/api/v1/recommendations/:id',
  requireDashboardRead,
  async (req, res) => {
    try {
      const clientId = resolveTenantId(req);
      if (clientId == null) {
        return res.status(400).json({
          error: 'client_id required',
          message: 'Recommendation detail requires an active client context',
        });
      }

      const recommendationId = String(req.params.id || '').trim();
      if (!recommendationId) {
        return res.status(400).json({
          error: 'recommendation_id required',
        });
      }

      const max = await getMaxRuntime();
      const model = await max.composeRecommendation({
        tenantId: String(clientId),
        recommendationId,
        asOf:
          typeof req.query.asOf === 'string' && req.query.asOf.trim()
            ? req.query.asOf.trim()
            : undefined,
        operator:
          (req.session && req.session.user && req.session.user.email) || null,
      });

      res.set('Cache-Control', 'no-store');
      return res.json(model);
    } catch (err) {
      console.error('[recommendation-detail]', err);
      return res.status(500).json({
        error: 'recommendation_compose_failed',
        message: err && err.message ? String(err.message) : 'compose failed',
      });
    }
  }
);

/**
 * GET /api/v1/companies/:id/intelligence
 */
router.get(
  '/api/v1/companies/:id/intelligence',
  requireDashboardRead,
  async (req, res) => {
    try {
      const clientId = resolveTenantId(req);
      if (clientId == null) {
        return res.status(400).json({
          error: 'client_id required',
          message: 'Company intelligence requires an active client context',
        });
      }

      const companyId = String(req.params.id || '').trim();
      if (!companyId) {
        return res.status(400).json({
          error: 'company_id required',
        });
      }

      const max = await getMaxRuntime();
      const model = await max.composeCompany({
        tenantId: String(clientId),
        companyId,
        asOf:
          typeof req.query.asOf === 'string' && req.query.asOf.trim()
            ? req.query.asOf.trim()
            : undefined,
        operator:
          (req.session && req.session.user && req.session.user.email) || null,
      });

      res.set('Cache-Control', 'no-store');
      return res.json(model);
    } catch (err) {
      console.error('[company-intelligence]', err);
      return res.status(500).json({
        error: 'company_compose_failed',
        message: err && err.message ? String(err.message) : 'compose failed',
      });
    }
  }
);

function resolveTenantId(req) {
  if (req.query.client_id != null && req.query.client_id !== '') {
    const role =
      (req.session && req.session.user && req.session.user.role) || null;
    if (role === 'admin' || role === 'manager') {
      return normalizeClientId(req.query.client_id);
    }
  }
  return getRequestClientId(req);
}

module.exports = router;
