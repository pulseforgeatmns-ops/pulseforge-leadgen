'use strict';

/**
 * Live Intelligence Loop APIs (SPEC-011 / ADR-006).
 *
 * GET /api/v1/intelligence/live?since=
 * GET /api/v1/intelligence/timeline/:entityId
 * GET /api/v1/intelligence/notifications?since=
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
 * GET /api/v1/intelligence/live
 * Soft-poll evolution since cursor. Optional recompose via ?refresh=1.
 */
router.get(
  '/api/v1/intelligence/live',
  requireDashboardRead,
  async (req, res) => {
    try {
      const clientId = resolveTenantId(req);
      if (clientId == null) {
        return res.status(400).json({
          error: 'client_id required',
          message: 'Live intelligence requires an active client context',
        });
      }

      const max = await getMaxRuntime();
      const since =
        typeof req.query.since === 'string' ? req.query.since : '';
      const refresh =
        req.query.refresh === '1' ||
        req.query.refresh === 'true' ||
        req.query.evolve === '1';

      let deck = null;
      let observed = null;
      if (refresh) {
        deck = await max.compose({
          tenantId: String(clientId),
          asOf:
            typeof req.query.asOf === 'string' && req.query.asOf.trim()
              ? req.query.asOf.trim()
              : undefined,
          operator:
            (req.session && req.session.user && req.session.user.email) ||
            null,
        });
        observed = {
          cursor: deck.live && deck.live.cursor,
          evolution: (deck.live && deck.live.evolution) || [],
          notifications: (deck.live && deck.live.notifications) || [],
        };
      }

      const payload = max.liveSince({
        tenantId: String(clientId),
        since,
        includeDeck: false,
      });

      res.set('Cache-Control', 'no-store');
      return res.json({
        ...payload,
        deck: refresh ? deck : null,
        evolution:
          (observed && observed.evolution) || payload.evolution || [],
        notifications: mergeNotifications(
          payload.notifications,
          observed && observed.notifications
        ),
      });
    } catch (err) {
      console.error('[intelligence-live]', err);
      return res.status(500).json({
        error: 'live_poll_failed',
        message: err && err.message ? String(err.message) : 'live failed',
      });
    }
  }
);

/**
 * GET /api/v1/intelligence/notifications?since=
 */
router.get(
  '/api/v1/intelligence/notifications',
  requireDashboardRead,
  async (req, res) => {
    try {
      const clientId = resolveTenantId(req);
      if (clientId == null) {
        return res.status(400).json({ error: 'client_id required' });
      }
      const max = await getMaxRuntime();
      const payload = max.liveSince({
        tenantId: String(clientId),
        since: typeof req.query.since === 'string' ? req.query.since : '',
        materialOnly: true,
      });
      res.set('Cache-Control', 'no-store');
      return res.json({
        cursor: payload.cursor,
        notifications: payload.notifications,
        since: payload.since,
      });
    } catch (err) {
      console.error('[intelligence-notifications]', err);
      return res.status(500).json({
        error: 'notifications_failed',
        message: err && err.message ? String(err.message) : 'failed',
      });
    }
  }
);

/**
 * GET /api/v1/intelligence/timeline/:entityId
 */
router.get(
  '/api/v1/intelligence/timeline/:entityId',
  requireDashboardRead,
  async (req, res) => {
    try {
      const clientId = resolveTenantId(req);
      if (clientId == null) {
        return res.status(400).json({ error: 'client_id required' });
      }
      const entityId = String(req.params.entityId || '').trim();
      if (!entityId) {
        return res.status(400).json({ error: 'entity_id required' });
      }
      const max = await getMaxRuntime();
      const timeline = max.liveTimeline({
        tenantId: String(clientId),
        entityId,
        kind:
          typeof req.query.kind === 'string' ? req.query.kind : undefined,
      });
      res.set('Cache-Control', 'no-store');
      return res.json(timeline);
    } catch (err) {
      console.error('[intelligence-timeline]', err);
      return res.status(500).json({
        error: 'timeline_failed',
        message: err && err.message ? String(err.message) : 'failed',
      });
    }
  }
);

function mergeNotifications(a, b) {
  const map = new Map();
  for (const n of a || []) map.set(n.id, n);
  for (const n of b || []) map.set(n.id, n);
  return [...map.values()].sort((x, y) => (x.seq || 0) - (y.seq || 0));
}

function resolveTenantId(req) {
  const fromQuery =
    req.query.client_id != null ? normalizeClientId(req.query.client_id) : null;
  if (fromQuery != null) return fromQuery;
  return getRequestClientId(req);
}

module.exports = router;
