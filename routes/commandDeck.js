'use strict';

/**
 * Command Deck — SPEC-007 API + SPEC-008 UI.
 *
 * GET /api/v1/command-deck → CommandDeckModel
 * GET /command-deck        → render-only HTML surface
 *
 * One API. One payload. One render.
 * The browser never orchestrates intelligence.
 */

const path = require('path');
const express = require('express');
const router = express.Router();
const { requireAuth, requireRole } = require('../middleware/auth');
const {
  getRequestClientId,
  normalizeClientId,
} = require('../utils/clientContext');
const { getMaxRuntime } = require('../utils/maxRuntime');
const { buildOperatorBrief } = require('../services/commandDeckOperatorBrief');
const {
  loadPriorities,
  reconcileDomainPriority,
} = require('../services/commandDeckPriority');
const { createPostgresStore } = require('../services/specialistDirection');
const { getActiveObjectives } = require('../services/operatorObjectives');
const {
  createPostgresAcquisitionState,
  toCommandDeckSignal,
} = require('../services/scoutAcquisitionIntelligence');

const requireDashboardRead = [
  requireAuth,
  requireRole('admin', 'manager', 'viewer', 'client'),
];

/**
 * GET /command-deck — Command Deck UI (SPEC-008)
 */
router.get('/command-deck', requireDashboardRead, (req, res) => {
  res.sendFile(path.join(__dirname, '..', 'public', 'command-deck.html'));
});

/** Static assets for the Command Deck page (CSS / JS). */
router.use(
  '/command-deck',
  express.static(path.join(__dirname, '..', 'public', 'command-deck'), {
    index: false,
    fallthrough: true,
  })
);

/**
 * GET /api/v1/command-deck
 * Query: period=daily|weekly|monthly, asOf=ISO, client_id (optional override for admin)
 */
router.get('/api/v1/command-deck', requireDashboardRead, async (req, res) => {
  try {
    const clientId = resolveTenantId(req);
    if (clientId == null) {
      return res.status(400).json({
        error: 'client_id required',
        message: 'Command Deck requires an active client context',
      });
    }

    const period = normalizePeriod(req.query.period);
    const asOf =
      typeof req.query.asOf === 'string' && req.query.asOf.trim()
        ? req.query.asOf.trim()
        : undefined;

    const max = await getMaxRuntime();
    let missions = [];
    if (max.missionEngine) {
      try {
        const list = await max.missionEngine.list({
          tenantId: String(clientId),
          clientId,
          limit: 20,
        });
        missions = list.map((m) => max.missionEngine.toCard(m));
      } catch (err) {
        console.warn('[command-deck] missions list failed:', err.message);
      }
    }

    let operatorBrief = null;
    try {
      operatorBrief = await buildOperatorBrief(clientId);
    } catch (err) {
      console.warn('[command-deck] operator brief prefetch failed:', err.message);
    }

    const model = await max.compose({
      tenantId: String(clientId),
      period,
      asOf,
      operator:
        (req.session && req.session.user && req.session.user.email) || null,
      missions,
      spatialContext: await buildSpatialContext(clientId, req, operatorBrief),
    });

    try {
      if (operatorBrief) {
        model.operatorBrief = operatorBrief;
        // Surface AO intelligence as the Command Deck brief (replaces empty market-intel state)
        model.morningBrief = {
          headline: operatorBrief.highestLeverage?.title || 'Operator brief ready',
          summary: operatorBrief.narrative,
          generatedAt: operatorBrief.generatedAt,
          marketContext: 'ao_field',
        };
        if (operatorBrief.highestLeverage) {
          model.highestLeverageAction = {
            recommendation: {
              recommendedAction: operatorBrief.highestLeverage.title,
              companyName: null,
            },
          };
        }
      }
    } catch (err) {
      console.warn('[command-deck] operator brief failed:', err.message);
    }

    res.set('Cache-Control', 'no-store');
    return res.json(model);
  } catch (err) {
    console.error('[command-deck]', err);
    return res.status(500).json({
      error: 'command_deck_compose_failed',
      message: err && err.message ? String(err.message) : 'compose failed',
    });
  }
});

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

function normalizePeriod(value) {
  const v = String(value || 'daily').toLowerCase();
  if (v === 'weekly' || v === 'monthly' || v === 'daily') return v;
  return 'daily';
}

/**
 * Build Living Command Deck spatial context from durable stores.
 * @param {number|string} clientId
 * @param {import('express').Request} req
 */
async function buildSpatialContext(clientId, req, operatorBrief = null) {
  const tenantId = String(clientId);
  let pendingRecommendations = [];
  let activeObjectives = [];
  let storedPriorities = new Map();
  let acquisitionIntelligence = null;

  try {
    const store = createPostgresStore();
    const recs = await store.listRecommendations({
      tenantId,
      status: 'pending',
      limit: 5,
    });
    const refined = await store.listRecommendations({
      tenantId,
      status: 'refined',
      limit: 5,
    });
    pendingRecommendations = [...recs, ...refined].map((r) => ({
      id: r.id,
      title: r.title || r.summary,
      summary: r.summary,
      status: r.status,
      channel: r.channel || (r.metadata && r.metadata.channel) || null,
      requiresOperatorDecision: r.status === 'pending' || r.status === 'refined',
    }));
  } catch (err) {
    console.warn('[command-deck] spatial content recs failed:', err.message);
  }

  try {
    activeObjectives = await getActiveObjectives({
      tenantId,
      clientId: Number(clientId),
      limit: 10,
    });
  } catch (err) {
    console.warn('[command-deck] spatial objectives failed:', err.message);
  }

  try {
    storedPriorities = await loadPriorities(tenantId);
  } catch (err) {
    console.warn('[command-deck] spatial priority load failed:', err.message);
  }

  try {
    const aoStore = createPostgresAcquisitionState();
    const aoState = await aoStore.get(tenantId);
    acquisitionIntelligence = toCommandDeckSignal(aoState);
  } catch (err) {
    console.warn('[command-deck] acquisition intelligence load failed:', err.message);
  }

  const lastVisitAt =
    (req.session && req.session.commandDeckLastVisit) || null;

  req.session.commandDeckLastVisit = new Date().toISOString();

  async function reconcilePriority(input) {
    return reconcileDomainPriority({
      tenantId,
      domainId: input.domainId,
      computedPriority: input.computed,
      reason: input.reason,
      evidenceRefs: input.evidenceRefs,
      stored: storedPriorities,
    });
  }

  return {
    pendingRecommendations,
    activeObjectives,
    storedPriorities,
    lastVisitAt,
    operatorBrief,
    reconcilePriority,
    acquisitionIntelligence,
  };
}

module.exports = router;
