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
 * GET  /api/v1/scout/work-requests/:id — read Scout work request (SPEC-077)
 * POST /api/v1/scout/work-requests/:id/execute — run public-source sourcing (SPEC-077)
 * GET  /api/v1/scout/handoffs/:handoffId — read by handoffId (SPEC-077)
 * GET  /api/v1/scout/places-diagnostic — safe Places connectivity probe (Scout path)
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
const {
  executeScoutWorkRequest,
  approveScoutResults,
} = require('../services/scoutHandoff');
const {
  defaultScoutWorkRequestStore,
} = require('../services/scoutWorkRequestStore');
const {
  diagnoseScoutPlaces,
} = require('../services/scoutPlacesDiagnostic');

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

/**
 * Safe Scout Places diagnostic — same legacy Text Search path as sourcing.
 * No CRM writes, outreach, placeholders, or full key logging.
 */
router.get(
  '/api/v1/scout/places-diagnostic',
  requireOperator,
  async (req, res) => {
    try {
      const comparePlacesNew = parseBool(
        req.query.compare_places_new ?? req.query.comparePlacesNew,
        true
      );
      const report = await diagnoseScoutPlaces({
        comparePlacesNew,
        query: req.query.query ? String(req.query.query) : undefined,
      });
      noStore(res);
      return res.status(report.ok ? 200 : 422).json({
        ...report,
        reviewOnly: true,
        crmWritesMade: false,
        outreachCopyGenerated: false,
        accountChangesMade: false,
        placeholdersCreated: false,
        fullKeyLogged: false,
      });
    } catch (err) {
      return sendError(res, err);
    }
  }
);

/**
 * SPEC-077 — read a Scout work request / handoff record (review-only).
 */
router.get(
  '/api/v1/scout/work-requests/:id',
  requireOperator,
  async (req, res) => {
    try {
      const record = defaultScoutWorkRequestStore.getByWorkRequestId(
        req.params.id
      );
      if (!record) {
        return res.status(404).json({
          error: 'not_found',
          message: `Scout work request not found: ${req.params.id}`,
        });
      }
      noStore(res);
      return res.json({
        ok: true,
        reviewOnly: true,
        crmWritesMade: false,
        outreachCopyGenerated: false,
        accountChangesMade: false,
        ...record,
      });
    } catch (err) {
      return sendError(res, err);
    }
  }
);

router.get(
  '/api/v1/scout/handoffs/:handoffId',
  requireOperator,
  async (req, res) => {
    try {
      const record = defaultScoutWorkRequestStore.getByHandoffId(
        req.params.handoffId
      );
      if (!record) {
        return res.status(404).json({
          error: 'not_found',
          message: `Scout handoff not found: ${req.params.handoffId}`,
        });
      }
      noStore(res);
      return res.json({
        ok: true,
        reviewOnly: true,
        crmWritesMade: false,
        outreachCopyGenerated: false,
        accountChangesMade: false,
        ...record,
      });
    } catch (err) {
      return sendError(res, err);
    }
  }
);

/**
 * SPEC-077 — execute public-source Scout sourcing for an approved work request.
 * Never writes CRM, outreach, or account changes.
 */
router.post(
  '/api/v1/scout/work-requests/:id/execute',
  requireOperator,
  async (req, res) => {
    try {
      const result = await executeScoutWorkRequest({
        workRequestId: req.params.id,
        workRequestStore: defaultScoutWorkRequestStore,
      });
      noStore(res);
      return res.status(result.ok ? 200 : 422).json({
        ...result,
        reviewOnly: true,
        crmWritesMade: false,
        outreachCopyGenerated: false,
        accountChangesMade: false,
      });
    } catch (err) {
      return sendError(res, err);
    }
  }
);

/**
 * SPEC-077 — operator approves Scout candidate batch for downstream use.
 * Still no CRM writes / outreach from this step.
 */
router.post(
  '/api/v1/scout/work-requests/:id/approve-results',
  requireOperator,
  async (req, res) => {
    try {
      const record = defaultScoutWorkRequestStore.getByWorkRequestId(
        req.params.id
      );
      if (!record || !record.handoff) {
        return res.status(404).json({
          error: 'not_found',
          message: `Scout work request not found: ${req.params.id}`,
        });
      }
      const approved = approveScoutResults(record.handoff);
      if (approved.ok && approved.handoff) {
        defaultScoutWorkRequestStore.update(req.params.id, {
          handoff: approved.handoff,
          candidateBatch: approved.handoff.candidateBatch,
          resultsApproved: true,
        });
      }
      noStore(res);
      return res.status(approved.ok ? 200 : 422).json({
        ...approved,
        reviewOnly: true,
        crmWritesMade: false,
        outreachCopyGenerated: false,
        accountChangesMade: false,
      });
    } catch (err) {
      return sendError(res, err);
    }
  }
);

module.exports = router;
