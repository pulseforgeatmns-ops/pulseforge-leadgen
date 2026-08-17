'use strict';

/**
 * SPEC-083 — Client Intelligence Engine APIs.
 * SPEC-084 — Interview experience (resume + understanding fields).
 * SPEC-085 — Executive Business Brief payload via executiveSummary.
 * SPEC-087 — Growth Infrastructure Readiness start/message.
 * SPEC-088 — Growth Work Continuation (Growth Plan resume + task complete).
 * SPEC-089 — First Campaign Planning Conversation start/message.
 * SPEC-096 — CIE client-scope authorization (session client is authoritative).
 * SPEC-097 — Onboarding recovery from durable CIE state.
 * SPEC-099 — Explicit interview restart supersedes unfinished unapproved onboarding.
 *
 * POST /api/v1/clients/:id/interview/start
 * POST /api/v1/interview/:id/message
 * POST /api/v1/interview/:id/resume
 * GET  /api/v1/interview/:id
 * GET  /api/v1/interview/:id/blueprint
 * POST /api/v1/blueprint/:id/revise
 * POST /api/v1/blueprint/:id/approve
 * GET  /api/v1/client-intel/onboarding
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
 * GET  /api/v1/clients/:id/cie-lifecycle-audit — read-only interview/Blueprint audit
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
  isClientRole,
  isInternalOperator,
  resolveCieClientId,
  assertCieClientAccess,
  assertRequestedClientMatches,
} = require('../utils/cieAuth');
const {
  ClientIntelligenceError,
  startClientInterview,
  postInterviewMessage,
  resumeInterview,
  getInterview,
  getInterviewBlueprint,
  getClientBlueprint,
  getApprovedClientBlueprint,
  getBlueprintRecord,
  auditClientBlueprintLifecycle,
  listApprovedBlueprintSessions,
  getResumePayload,
  loadAnchorSampleBlueprint,
  resolveClientOnboardingState,
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
const requireInternal = [requireAuth, requireRole('admin', 'manager')];

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

async function loadInterviewScoped(req, sessionId) {
  const detail = await getInterview(sessionId);
  assertCieClientAccess(req, detail.clientId);
  return detail;
}

async function loadBlueprintScoped(req, blueprintId) {
  const bp = await getBlueprintRecord(blueprintId);
  assertCieClientAccess(req, bp.clientId);
  return bp;
}

router.get('/client-intel', requireOperator, (req, res) => {
  res.sendFile(path.join(__dirname, '..', 'public', 'client-intel.html'));
});

/**
 * SPEC-097 — recover onboarding from authenticated client identity.
 * No interview ID required from the client.
 */
router.get('/api/v1/client-intel/onboarding', requireOperator, async (req, res) => {
  try {
    const clientId = resolveCieClientId(
      req,
      isClientRole(req) ? null : req.query.clientId || req.query.client_id
    );
    if (isClientRole(req)) {
      assertRequestedClientMatches(
        req,
        req.query.clientId || req.query.client_id
      );
    }
    const result = await resolveClientOnboardingState(clientId);
    noStore(res);
    return res.json(result);
  } catch (err) {
    return sendError(res, err);
  }
});

router.get('/api/v1/client-intel/sessions', requireOperator, async (req, res) => {
  try {
    if (isClientRole(req)) {
      assertRequestedClientMatches(req, req.query.clientId || req.query.client_id);
    }
    const clientId = resolveCieClientId(
      req,
      isClientRole(req) ? null : req.query.clientId
    );
    const result = await listApprovedBlueprintSessions({
      clientId,
      includeSamples: isClientRole(req)
        ? false
        : parseBool(req.query.includeSamples, true),
      samplesOnly: isClientRole(req)
        ? false
        : parseBool(req.query.samplesOnly, false),
      limit: req.query.limit,
      requireClientId: isClientRole(req),
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
      await loadInterviewScoped(req, req.params.id);
      const action = String((req.query && req.query.action) || 'continue');
      const result = await getResumePayload(req.params.id, { action });
      assertCieClientAccess(req, result.clientId);
      noStore(res);
      return res.json(result);
    } catch (err) {
      return sendError(res, err);
    }
  }
);

router.post(
  '/api/v1/client-intel/fixtures/anchor-blueprint',
  requireInternal,
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
      assertRequestedClientMatches(req, req.params.id);
      const body = req.body || {};
      if (isClientRole(req)) {
        assertRequestedClientMatches(req, body.clientId || body.client_id);
      }
      const clientId = resolveCieClientId(req, req.params.id);
      // SPEC-099 — explicit restart is allowed for the authenticated client scope.
      // Do not infer restart from a bare start call (preserves SPEC-097 resume).
      const restart = Boolean(
        body.restart || body.restartInterview || body.explicit_restart
      );
      // forceNew remains internal-only (fixtures / operator tooling).
      const forceNew =
        isInternalOperator(req) && Boolean(body.forceNew || body.force_new);
      const result = await startClientInterview({
        clientId,
        notes: isClientRole(req) ? undefined : body.notes,
        source: body.source || 'api',
        forceNew,
        restart,
      });
      assertCieClientAccess(req, result.clientId);
      noStore(res);
      return res.status(result.resumedExisting ? 200 : 201).json(result);
    } catch (err) {
      return sendError(res, err);
    }
  }
);

router.post('/api/v1/interview/:id/message', requireOperator, async (req, res) => {
  try {
    await loadInterviewScoped(req, req.params.id);
    const message = req.body && req.body.message;
    if (message == null || String(message).trim() === '') {
      return res.status(400).json({
        error: 'empty_message',
        message: 'message is required',
      });
    }
    const result = await postInterviewMessage(req.params.id, message);
    assertCieClientAccess(req, result.clientId);
    noStore(res);
    return res.json(result);
  } catch (err) {
    return sendError(res, err);
  }
});

router.post('/api/v1/interview/:id/resume', requireOperator, async (req, res) => {
  try {
    await loadInterviewScoped(req, req.params.id);
    const result = await resumeInterview(req.params.id);
    assertCieClientAccess(req, result.clientId);
    noStore(res);
    return res.json(result);
  } catch (err) {
    return sendError(res, err);
  }
});

router.get('/api/v1/interview/:id', requireOperator, async (req, res) => {
  try {
    const result = await loadInterviewScoped(req, req.params.id);
    noStore(res);
    return res.json(result);
  } catch (err) {
    return sendError(res, err);
  }
});

router.get('/api/v1/interview/:id/blueprint', requireOperator, async (req, res) => {
  try {
    await loadInterviewScoped(req, req.params.id);
    const result = await getInterviewBlueprint(req.params.id);
    if (result && result.clientId != null) {
      assertCieClientAccess(req, result.clientId);
    }
    noStore(res);
    return res.json(result);
  } catch (err) {
    return sendError(res, err);
  }
});

router.post('/api/v1/blueprint/:id/revise', requireOperator, async (req, res) => {
  try {
    await loadBlueprintScoped(req, req.params.id);
    const body = req.body || {};
    const result = await reviseBlueprint(req.params.id, body.sections || body);
    assertCieClientAccess(req, result.clientId || (result.blueprint && result.blueprint.clientId));
    noStore(res);
    return res.json(result);
  } catch (err) {
    return sendError(res, err);
  }
});

router.post('/api/v1/blueprint/:id/approve', requireOperator, async (req, res) => {
  try {
    await loadBlueprintScoped(req, req.params.id);
    const result = await approveBlueprint(req.params.id);
    assertCieClientAccess(
      req,
      result.clientId || (result.blueprint && result.blueprint.clientId)
    );
    const clientId =
      result.clientId || (result.blueprint && result.blueprint.clientId);
    if (clientId != null) {
      const { onBlueprintApproved } = require('../services/operatorContextEvents');
      onBlueprintApproved(clientId, {
        blueprintId: result.blueprint && result.blueprint.id,
        blueprintVersion: result.blueprint && result.blueprint.version,
      });
    }
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
      await loadInterviewScoped(req, req.params.id);
      const result = await startGrowthConversation(req.params.id);
      assertCieClientAccess(req, result.clientId);
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
      await loadInterviewScoped(req, req.params.id);
      const message = req.body && req.body.message;
      if (message == null || String(message).trim() === '') {
        return res.status(400).json({
          error: 'empty_message',
          message: 'message is required',
        });
      }
      const result = await postGrowthMessage(req.params.id, message);
      assertCieClientAccess(req, result.clientId);
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
      await loadInterviewScoped(req, req.params.id);
      const result = await startInfrastructureReadinessConversation(req.params.id);
      assertCieClientAccess(req, result.clientId);
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
      await loadInterviewScoped(req, req.params.id);
      const message = req.body && req.body.message;
      if (message == null || String(message).trim() === '') {
        return res.status(400).json({
          error: 'empty_message',
          message: 'message is required',
        });
      }
      const result = await postInfrastructureReadinessMessage(req.params.id, message);
      assertCieClientAccess(req, result.clientId);
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
      await loadInterviewScoped(req, req.params.id);
      const result = await startCampaignPlanningConversation(req.params.id);
      assertCieClientAccess(req, result.clientId);
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
      await loadInterviewScoped(req, req.params.id);
      const message = req.body && req.body.message;
      if (message == null || String(message).trim() === '') {
        return res.status(400).json({
          error: 'empty_message',
          message: 'message is required',
        });
      }
      const result = await postCampaignPlanningMessage(req.params.id, message);
      assertCieClientAccess(req, result.clientId);
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
      await loadInterviewScoped(req, req.params.id);
      const body = req.body || {};
      const result = await completeGrowthPlanTask(req.params.id, req.params.taskId, {
        note: body.note,
        source: body.source || 'operator',
      });
      assertCieClientAccess(req, result.clientId);
      noStore(res);
      return res.json(result);
    } catch (err) {
      return sendError(res, err);
    }
  }
);

router.get('/api/v1/clients/:id/blueprint', requireOperator, async (req, res) => {
  try {
    assertRequestedClientMatches(req, req.params.id);
    const clientId = resolveCieClientId(req, req.params.id);
    const approvedOnly = parseBool(req.query.approvedOnly, isClientRole(req));
    const result = approvedOnly
      ? await getApprovedClientBlueprint(clientId)
      : await getClientBlueprint(clientId);
    if (result && result.clientId != null) {
      assertCieClientAccess(req, result.clientId);
    }
    noStore(res);
    return res.json(result);
  } catch (err) {
    return sendError(res, err);
  }
});

/**
 * Read-only CIE lifecycle audit (admin/manager). No mutations.
 * Inspects interviews + Blueprints for a client to diagnose post-restart state.
 */
router.get(
  '/api/v1/clients/:id/cie-lifecycle-audit',
  requireInternal,
  async (req, res) => {
    try {
      assertRequestedClientMatches(req, req.params.id);
      const clientId = resolveCieClientId(req, req.params.id);
      const report = await auditClientBlueprintLifecycle(clientId);
      noStore(res);
      return res.json(report);
    } catch (err) {
      return sendError(res, err);
    }
  }
);

/**
 * Safe Scout Places diagnostic — same legacy Text Search path as sourcing.
 * No CRM writes, outreach, placeholders, or full key logging.
 */
router.get(
  '/api/v1/scout/places-diagnostic',
  requireInternal,
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
  requireInternal,
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
  requireInternal,
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
  requireInternal,
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
  requireInternal,
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
