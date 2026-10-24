'use strict';

/**
 * SPEC-064 — Relationship Intelligence Interview APIs.
 * Max-owned capture. Mutations only touch relationship_* tables.
 *
 * GET  /api/v1/relationship-intel/readiness
 * POST /api/v1/relationship-intel/interviews
 * POST /api/v1/relationship-intel/interviews/:id/messages
 * GET  /api/v1/relationship-intel/interviews/:id
 * POST /api/v1/relationship-intel/interviews/:id/summarize
 * POST /api/v1/relationship-intel/interviews/:id/commit
 * GET  /api/v1/relationship-intel/interactions
 * GET  /api/v1/relationship-intel/interactions/:id
 */

const express = require('express');
const router = express.Router();
const { requireAuth, requireRole } = require('../middleware/auth');
const {
  RelationshipIntelligenceError,
  startRelationshipInterview,
  answerRelationshipInterview,
  summarizeRelationshipInterview,
  commitRelationshipInterview,
  getInterview,
  listInteractions,
  getInteraction,
  INTERACTION_TYPES,
} = require('../services/relationshipIntelligenceInterview');
const {
  buildRelationshipIntelReadinessReport,
} = require('../services/relationshipIntelligenceReadiness');

const requireAdmin = [requireAuth, requireRole('admin', 'manager')];

function noStore(res) {
  res.set('Cache-Control', 'no-store');
}

function sendError(res, err) {
  if (err instanceof RelationshipIntelligenceError) {
    return res.status(err.status || 400).json({
      error: err.code,
      message: err.message,
    });
  }
  console.error('[relationship-intel]', err);
  return res.status(500).json({
    error: 'relationship_intel_failed',
    message: err && err.message ? String(err.message) : 'failed',
  });
}

router.get('/api/v1/relationship-intel/readiness', requireAdmin, async (req, res) => {
  try {
    const report = await buildRelationshipIntelReadinessReport();
    noStore(res);
    return res.json(report);
  } catch (err) {
    console.error('[relationship-intel] readiness', err);
    return res.status(500).json({
      error: 'relationship_intel_readiness_failed',
      message: err && err.message ? String(err.message) : 'failed',
    });
  }
});

router.post('/api/v1/relationship-intel/interviews', requireAdmin, async (req, res) => {
  try {
    const body = req.body || {};
    if (body.type != null || body.interactionType != null) {
      const type = body.interactionType || body.type;
      if (!INTERACTION_TYPES.includes(String(type))) {
        return res.status(400).json({
          error: 'invalid_interaction_type',
          message: `interaction_type must be one of: ${INTERACTION_TYPES.join(', ')}`,
        });
      }
    }
    const result = await startRelationshipInterview({
      clientId: body.clientId != null ? body.clientId : req.session?.active_client_id,
      companyId: body.companyId,
      contactId: body.contactId,
      opportunityId: body.opportunityId,
      userId: body.userId != null ? body.userId : req.session?.user?.id,
      interactionType: body.interactionType || body.type || 'other',
      occurredAt: body.occurredAt,
      notes: body.notes,
      source: body.source || 'api',
    });
    noStore(res);
    return res.status(201).json(result);
  } catch (err) {
    return sendError(res, err);
  }
});

router.post(
  '/api/v1/relationship-intel/interviews/:id/messages',
  requireAdmin,
  async (req, res) => {
    try {
      const message = req.body && req.body.message;
      if (message == null || String(message).trim() === '') {
        return res.status(400).json({
          error: 'empty_message',
          message: 'message is required',
        });
      }
      const result = await answerRelationshipInterview(req.params.id, message);
      noStore(res);
      return res.json(result);
    } catch (err) {
      return sendError(res, err);
    }
  }
);

router.get('/api/v1/relationship-intel/interviews/:id', requireAdmin, async (req, res) => {
  try {
    const result = await getInterview(req.params.id);
    noStore(res);
    return res.json(result);
  } catch (err) {
    return sendError(res, err);
  }
});

router.post(
  '/api/v1/relationship-intel/interviews/:id/summarize',
  requireAdmin,
  async (req, res) => {
    try {
      const result = await summarizeRelationshipInterview(req.params.id);
      noStore(res);
      return res.json(result);
    } catch (err) {
      return sendError(res, err);
    }
  }
);

router.post(
  '/api/v1/relationship-intel/interviews/:id/commit',
  requireAdmin,
  async (req, res) => {
    try {
      const result = await commitRelationshipInterview(req.params.id);
      noStore(res);
      return res.json(result);
    } catch (err) {
      return sendError(res, err);
    }
  }
);

router.get('/api/v1/relationship-intel/interactions', requireAdmin, async (req, res) => {
  try {
    const result = await listInteractions({
      clientId: req.query.clientId != null ? req.query.clientId : req.session?.active_client_id,
      status: req.query.status,
      companyId: req.query.companyId,
      limit: req.query.limit,
    });
    noStore(res);
    return res.json({ ok: true, interactions: result });
  } catch (err) {
    return sendError(res, err);
  }
});

router.get('/api/v1/relationship-intel/interactions/:id', requireAdmin, async (req, res) => {
  try {
    const result = await getInteraction(req.params.id);
    noStore(res);
    return res.json(result);
  } catch (err) {
    return sendError(res, err);
  }
});

module.exports = router;
