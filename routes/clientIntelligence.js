'use strict';

/**
 * SPEC-083 — Client Intelligence Engine APIs.
 * SPEC-084 — Interview experience (resume + understanding fields).
 * SPEC-085 — Executive Business Brief payload via executiveSummary.
 * SPEC-087 — Growth Infrastructure Readiness start/message.
 * SPEC-088 — Growth Work Continuation (Growth Plan resume + task complete).
 * SPEC-089 — First Campaign Planning Conversation start/message.
 *
 * POST /api/v1/clients/:id/interview/start
 * POST /api/v1/interview/:id/message
 * POST /api/v1/interview/:id/resume
 * GET  /api/v1/interview/:id
 * GET  /api/v1/interview/:id/blueprint
 * POST /api/v1/blueprint/:id/revise
 * POST /api/v1/blueprint/:id/approve
 * POST /api/v1/interview/:id/growth/start
 * POST /api/v1/interview/:id/growth/message
 * POST /api/v1/interview/:id/readiness/start
 * POST /api/v1/interview/:id/readiness/message
 * POST /api/v1/interview/:id/campaign/start
 * POST /api/v1/interview/:id/campaign/message
 * POST /api/v1/interview/:id/growth-plan/tasks/:taskId/complete
 * GET  /api/v1/clients/:id/blueprint
 * GET  /api/v1/client-intel/sessions
 * GET  /api/v1/client-intel/sessions/:id/resume
 * POST /api/v1/client-intel/fixtures/anchor-blueprint
 * GET  /client-intel → UI
 */

const path = require('path');
const express = require('express');
const router = express.Router();
const { requireAuth, requireRole } = require('../middleware/auth');
const {
  ClientIntelligenceError,
  startClientInterview,
  postInterviewMessage,
  resumeInterview,
  getInterview,
  getInterviewBlueprint,
  getClientBlueprint,
  listApprovedBlueprintSessions,
  getResumePayload,
  loadAnchorSampleBlueprint,
  reviseBlueprint,
  approveBlueprint,
  startGrowthConversation,
  postGrowthMessage,
  startInfrastructureReadinessConversation,
  postInfrastructureReadinessMessage,
  startCampaignPlanningConversation,
  postCampaignPlanningMessage,
  completeGrowthPlanTask,
} = require('../services/clientIntelligenceInterview');

const requireOperator = [requireAuth, requireRole('admin', 'manager', 'client')];

function noStore(res) {
  res.set('Cache-Control', 'no-store');
}

function sendError(res, err) {
  if (err instanceof ClientIntelligenceError) {
    return res.status(err.status || 400).json({
      error: err.code,
      message: err.message,
    });
  }
  console.error('[client-intel]', err);
  return res.status(500).json({
    error: 'client_intel_failed',
    message: err && err.message ? String(err.message) : 'failed',
  });
}

function parseBool(value, fallback) {
  if (value == null || value === '') return fallback;
  const s = String(value).trim().toLowerCase();
  if (['1', 'true', 'yes', 'on'].includes(s)) return true;
  if (['0', 'false', 'no', 'off'].includes(s)) return false;
  return fallback;
}

router.get('/client-intel', requireOperator, (req, res) => {
  res.sendFile(path.join(__dirname, '..', 'public', 'client-intel.html'));
});

router.get('/api/v1/client-intel/sessions', requireOperator, async (req, res) => {
  try {
    const result = await listApprovedBlueprintSessions({
      clientId: req.query.clientId,
      includeSamples: parseBool(req.query.includeSamples, true),
      samplesOnly: parseBool(req.query.samplesOnly, false),
      limit: req.query.limit,
    });
    noStore(res);
    return res.json(result);
  } catch (err) {
    return sendError(res, err);
  }
});

router.get(
  '/api/v1/client-intel/sessions/:id/resume',
  requireOperator,
  async (req, res) => {
    try {
      const action = String((req.query && req.query.action) || 'continue');
      const result = await getResumePayload(req.params.id, { action });
      noStore(res);
      return res.json(result);
    } catch (err) {
      return sendError(res, err);
    }
  }
);

router.post(
  '/api/v1/client-intel/fixtures/anchor-blueprint',
  requireOperator,
  async (req, res) => {
    try {
      const body = req.body || {};
      const result = await loadAnchorSampleBlueprint({
        forceNew: Boolean(body.forceNew),
      });
      noStore(res);
      return res.status(result.created ? 201 : 200).json(result);
    } catch (err) {
      return sendError(res, err);
    }
  }
);

router.post(
  '/api/v1/clients/:id/interview/start',
  requireOperator,
  async (req, res) => {
    try {
      const body = req.body || {};
      const result = await startClientInterview({
        clientId: req.params.id,
        notes: body.notes,
        source: body.source || 'api',
      });
      noStore(res);
      return res.status(201).json(result);
    } catch (err) {
      return sendError(res, err);
    }
  }
);

router.post('/api/v1/interview/:id/message', requireOperator, async (req, res) => {
  try {
    const message = req.body && req.body.message;
    if (message == null || String(message).trim() === '') {
      return res.status(400).json({
        error: 'empty_message',
        message: 'message is required',
      });
    }
    const result = await postInterviewMessage(req.params.id, message);
    noStore(res);
    return res.json(result);
  } catch (err) {
    return sendError(res, err);
  }
});

router.post('/api/v1/interview/:id/resume', requireOperator, async (req, res) => {
  try {
    const result = await resumeInterview(req.params.id);
    noStore(res);
    return res.json(result);
  } catch (err) {
    return sendError(res, err);
  }
});

router.get('/api/v1/interview/:id', requireOperator, async (req, res) => {
  try {
    const result = await getInterview(req.params.id);
    noStore(res);
    return res.json(result);
  } catch (err) {
    return sendError(res, err);
  }
});

router.get('/api/v1/interview/:id/blueprint', requireOperator, async (req, res) => {
  try {
    const result = await getInterviewBlueprint(req.params.id);
    noStore(res);
    return res.json(result);
  } catch (err) {
    return sendError(res, err);
  }
});

router.post('/api/v1/blueprint/:id/revise', requireOperator, async (req, res) => {
  try {
    const body = req.body || {};
    const result = await reviseBlueprint(req.params.id, body.sections || body);
    noStore(res);
    return res.json(result);
  } catch (err) {
    return sendError(res, err);
  }
});

router.post('/api/v1/blueprint/:id/approve', requireOperator, async (req, res) => {
  try {
    const result = await approveBlueprint(req.params.id);
    noStore(res);
    return res.json(result);
  } catch (err) {
    return sendError(res, err);
  }
});

router.post(
  '/api/v1/interview/:id/growth/start',
  requireOperator,
  async (req, res) => {
    try {
      const result = await startGrowthConversation(req.params.id);
      noStore(res);
      return res.json(result);
    } catch (err) {
      return sendError(res, err);
    }
  }
);

router.post(
  '/api/v1/interview/:id/growth/message',
  requireOperator,
  async (req, res) => {
    try {
      const message = req.body && req.body.message;
      if (message == null || String(message).trim() === '') {
        return res.status(400).json({
          error: 'empty_message',
          message: 'message is required',
        });
      }
      const result = await postGrowthMessage(req.params.id, message);
      noStore(res);
      return res.json(result);
    } catch (err) {
      return sendError(res, err);
    }
  }
);

router.post(
  '/api/v1/interview/:id/readiness/start',
  requireOperator,
  async (req, res) => {
    try {
      const result = await startInfrastructureReadinessConversation(req.params.id);
      noStore(res);
      return res.json(result);
    } catch (err) {
      return sendError(res, err);
    }
  }
);

router.post(
  '/api/v1/interview/:id/readiness/message',
  requireOperator,
  async (req, res) => {
    try {
      const message = req.body && req.body.message;
      if (message == null || String(message).trim() === '') {
        return res.status(400).json({
          error: 'empty_message',
          message: 'message is required',
        });
      }
      const result = await postInfrastructureReadinessMessage(req.params.id, message);
      noStore(res);
      return res.json(result);
    } catch (err) {
      return sendError(res, err);
    }
  }
);

router.post(
  '/api/v1/interview/:id/campaign/start',
  requireOperator,
  async (req, res) => {
    try {
      const result = await startCampaignPlanningConversation(req.params.id);
      noStore(res);
      return res.json(result);
    } catch (err) {
      return sendError(res, err);
    }
  }
);

router.post(
  '/api/v1/interview/:id/campaign/message',
  requireOperator,
  async (req, res) => {
    try {
      const message = req.body && req.body.message;
      if (message == null || String(message).trim() === '') {
        return res.status(400).json({
          error: 'empty_message',
          message: 'message is required',
        });
      }
      const result = await postCampaignPlanningMessage(req.params.id, message);
      noStore(res);
      return res.json(result);
    } catch (err) {
      return sendError(res, err);
    }
  }
);

router.post(
  '/api/v1/interview/:id/growth-plan/tasks/:taskId/complete',
  requireOperator,
  async (req, res) => {
    try {
      const body = req.body || {};
      const result = await completeGrowthPlanTask(req.params.id, req.params.taskId, {
        note: body.note,
        source: body.source || 'operator',
      });
      noStore(res);
      return res.json(result);
    } catch (err) {
      return sendError(res, err);
    }
  }
);

router.get('/api/v1/clients/:id/blueprint', requireOperator, async (req, res) => {
  try {
    const result = await getClientBlueprint(req.params.id);
    noStore(res);
    return res.json(result);
  } catch (err) {
    return sendError(res, err);
  }
});

module.exports = router;
