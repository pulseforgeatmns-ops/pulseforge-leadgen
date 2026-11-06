'use strict';

/**
 * SPEC-118 — Acquisition Mission Orchestration APIs.
 *
 * GET  /acquisition-missions
 * GET  /api/v1/amo/missions
 * POST /api/v1/amo/missions
 * GET  /api/v1/amo/missions/:id
 * POST /api/v1/amo/missions/:id/contribute
 * POST /api/v1/amo/missions/:id/progress
 * POST /api/v1/amo/missions/:id/execute
 * GET  /api/v1/amo/missions/:id/context
 * GET  /api/v1/amo/missions/:id/explain
 * GET  /api/v1/amo/learning
 * GET  /api/v1/amo/outcome-learning
 * GET  /api/v1/amo/missions/:id/outcome-learning
 * POST /api/v1/amo/missions/:id/outcome-learning/evaluate
 * POST /api/v1/amo/ask
 * GET  /api/v1/amo/missions/:id/authorization
 * POST /api/v1/amo/missions/:id/authorization/amendments
 */

const express = require('express');
const path = require('path');
const router = express.Router();
const { requireAuth, requireRole } = require('../middleware/auth');
const { resolveActiveTenantId } = require('../packages/max/workspace/TenantContextResolver');
const pool = require('../db');
const { observeOperatorHttp } = require('../packages/decision-service/httpObserver');
const {
  createMission,
  inspectMission,
  listMissions,
  contribute,
  progressMission,
  answerOperator,
  executeCanonical,
  previewMissionAuthorization,
  amendMissionAuthorization,
} = require('../services/acquisitionMission');

const requireActor = [requireAuth, requireRole('admin', 'manager', 'client')];

function noStore(res) {
  res.set('Cache-Control', 'no-store');
}

function actorTenantId(req) {
  const user = req.user || (req.session && req.session.user);
  if (user && user.role === 'client') {
    const id = Number(user.client_id);
    return Number.isInteger(id) && id > 0 ? String(id) : null;
  }
  const id = resolveActiveTenantId(req);
  return id != null ? String(id) : null;
}

function actorFrom(req) {
  const user = req.user || (req.session && req.session.user) || {};
  return {
    id: user.id || user.email || 'operator',
    name: user.name || user.email || 'operator',
    role: user.role === 'max' ? 'max' : 'operator',
  };
}

function fail(res, err, fallbackCode, fallbackStatus = 500) {
  const code = (err && err.code) || fallbackCode;
  const status =
    code === 'amo_mission_not_found' ? 404
      : code === 'MISSION_STATE_INCONSISTENT' ? 409
      : code === 'cer_permission_denied' || code === 'cer_policy_blocked'
        || code === 'authorization_constraint_blocked'
        || code === 'authorization_amendment_forbidden'
        || code === 'synthetic_evidence_blocked' ? 403
      : code === 'authorization_amendment_stale' ? 409
      : code === 'authorization_amendment_invalid' || code === 'authorization_amendment_noop' ? 400
      : code === 'MISSION_RUNTIME_BOUNDARY_VIOLATION' ? 409
      : code === 'amo_tenant_required'
        || code === 'amo_tenant_mismatch'
        || code === 'amo_objective_required'
        || code === 'amo_contract_violation'
        || code === 'amo_contract_empty'
        || code === 'amo_stage_blocked'
        || code === 'amo_max_orchestrates'
        || code === 'amo_already_at_stage'
        || code === 'cer_invalid'
        || code === 'cer_unknown_intent'
        || code === 'cer_permission_denied'
        || code === 'cer_policy_blocked'
        || code === 'cer_runtime_owner_required'
        || code === 'MISSION_RUNTIME_BOUNDARY_VIOLATION'
        || code === 'no_tenant'
        ? 400
        : fallbackStatus;
  return res.status(status).json({
    error: code,
    message: String((err && err.message) || err),
  });
}

router.get('/acquisition-missions', requireActor, (_req, res) => {
  res.sendFile(path.join(__dirname, '..', 'public', 'acquisition-missions.html'));
});

router.get('/api/v1/amo/missions', requireActor, async (req, res) => {
  try {
    const tenantId = actorTenantId(req);
    if (tenantId == null) {
      return res.status(400).json({ error: 'no_tenant', message: 'No active client selected.' });
    }
    const missions = await listMissions(tenantId, { pool });
    noStore(res);
    return res.json({ kind: 'acquisition_missions', spec: 'SPEC-118', tenantId, missions });
  } catch (err) {
    console.error('[amo] list', err);
    return fail(res, err, 'amo_list_failed');
  }
});

router.post('/api/v1/amo/missions', requireActor, async (req, res) => {
  try {
    const tenantId = actorTenantId(req);
    if (tenantId == null) {
      return res.status(400).json({ error: 'no_tenant', message: 'No active client selected.' });
    }
    const mission = await createMission({
      ...req.body,
      tenantId,
      clientId: Number(tenantId),
      createdBy: req.body?.createdBy || 'max',
      owner: req.body?.owner || 'Operator',
    }, { pool });
    noStore(res);
    return res.status(201).json({ spec: 'SPEC-118', mission });
  } catch (err) {
    console.error('[amo] create', err);
    return fail(res, err, 'amo_create_failed');
  }
});

router.get('/api/v1/amo/missions/:id', requireActor, async (req, res) => {
  try {
    const tenantId = actorTenantId(req);
    if (tenantId == null) {
      return res.status(400).json({ error: 'no_tenant', message: 'No active client selected.' });
    }
    const snapshot = await inspectMission(req.params.id, { tenantId, pool });
    noStore(res);
    return res.json({ spec: 'SPEC-118', tenantId, ...snapshot });
  } catch (err) {
    console.error('[amo] inspect', err);
    return fail(res, err, 'amo_inspect_failed');
  }
});

router.post('/api/v1/amo/missions/:id/contribute', requireActor, async (req, res) => {
  try {
    const tenantId = actorTenantId(req);
    if (tenantId == null) {
      return res.status(400).json({ error: 'no_tenant', message: 'No active client selected.' });
    }
    const result = await contribute(req.params.id, req.body || {}, { tenantId, pool });
    noStore(res);
    return res.json({ spec: 'SPEC-118', ...result });
  } catch (err) {
    console.error('[amo] contribute', err);
    return fail(res, err, 'amo_contribute_failed');
  }
});

router.post('/api/v1/amo/missions/:id/progress', requireActor, async (req, res) => {
  try {
    const tenantId = actorTenantId(req);
    if (tenantId == null) {
      return res.status(400).json({ error: 'no_tenant', message: 'No active client selected.' });
    }
    const actor = actorFrom(req);
    if (req.body?.asMax === true) actor.role = 'max';
    const mission = await progressMission(
      req.params.id,
      actor,
      { stage: req.body?.stage },
      { tenantId, pool }
    );
    noStore(res);
    return res.json({ spec: 'SPEC-118', mission });
  } catch (err) {
    console.error('[amo] progress', err);
    return fail(res, err, 'amo_progress_failed');
  }
});

router.get('/api/v1/amo/missions/:id/context', requireActor, async (req, res) => {
  try {
    const tenantId = actorTenantId(req);
    if (tenantId == null) {
      return res.status(400).json({ error: 'no_tenant', message: 'No active client selected.' });
    }
    const snapshot = await inspectMission(req.params.id, { tenantId, pool });
    noStore(res);
    return res.json({ spec: 'SPEC-118', context: snapshot.context });
  } catch (err) {
    console.error('[amo] context', err);
    return fail(res, err, 'amo_context_failed');
  }
});

router.get('/api/v1/amo/missions/:id/explain', requireActor, async (req, res) => {
  try {
    const tenantId = actorTenantId(req);
    if (tenantId == null) {
      return res.status(400).json({ error: 'no_tenant', message: 'No active client selected.' });
    }
    const snapshot = await inspectMission(req.params.id, { tenantId, pool });
    noStore(res);
    return res.json({ spec: 'SPEC-118', why: snapshot.why });
  } catch (err) {
    console.error('[amo] explain', err);
    return fail(res, err, 'amo_explain_failed');
  }
});

router.get('/api/v1/amo/learning', requireActor, async (req, res) => {
  try {
    const tenantId = actorTenantId(req);
    if (tenantId == null) {
      return res.status(400).json({ error: 'no_tenant', message: 'No active client selected.' });
    }
    const missions = await listMissions(tenantId, { pool });
    const { getAcquisitionMissionRuntime } = require('../services/acquisitionMissionRuntime');
    noStore(res);
    return res.json({
      spec: 'SPEC-118',
      outcomeLearningSpec: 'SPEC-166',
      tenantId,
      learning: getAcquisitionMissionRuntime().engine().learning(tenantId),
      missionCount: missions.length,
    });
  } catch (err) {
    console.error('[amo] learning', err);
    return fail(res, err, 'amo_learning_failed');
  }
});

router.get('/api/v1/amo/outcome-learning', requireActor, async (req, res) => {
  try {
    const tenantId = actorTenantId(req);
    if (tenantId == null) {
      return res.status(400).json({ error: 'no_tenant', message: 'No active client selected.' });
    }
    const { getAcquisitionMissionRuntime } = require('../services/acquisitionMissionRuntime');
    await listMissions(tenantId, { pool });
    const engine = getAcquisitionMissionRuntime().engine();
    const { summarizeOrganizationalLearning } = require('../packages/acquisition-mission/OutcomeLearning');
    const since = req.query.since || null;
    noStore(res);
    return res.json({
      spec: 'SPEC-166',
      tenantId,
      organizationalLearning: summarizeOrganizationalLearning(
        engine.store.listEvaluations(null).filter((row) => String(row.tenantId) === String(tenantId)),
        engine.store.listOutcomeLearnings(tenantId),
        { since, period: since ? 'custom' : 'all' }
      ),
      autoApplied: false,
    });
  } catch (err) {
    console.error('[amo] outcome-learning', err);
    return fail(res, err, 'amo_outcome_learning_failed');
  }
});

router.get('/api/v1/amo/missions/:id/outcome-learning', requireActor, async (req, res) => {
  try {
    const tenantId = actorTenantId(req);
    if (tenantId == null) {
      return res.status(400).json({ error: 'no_tenant', message: 'No active client selected.' });
    }
    const snapshot = await inspectMission(req.params.id, { tenantId, pool });
    noStore(res);
    return res.json({
      spec: 'SPEC-166',
      missionId: req.params.id,
      outcomeLearning: snapshot.outcomeLearning,
      autoApplied: false,
    });
  } catch (err) {
    console.error('[amo] mission outcome-learning', err);
    return fail(res, err, 'amo_mission_outcome_learning_failed');
  }
});

router.post('/api/v1/amo/missions/:id/outcome-learning/evaluate', requireActor, async (req, res) => {
  try {
    const tenantId = actorTenantId(req);
    if (tenantId == null) {
      return res.status(400).json({ error: 'no_tenant', message: 'No active client selected.' });
    }
    const { getAcquisitionMissionRuntime } = require('../services/acquisitionMissionRuntime');
    await listMissions(tenantId, { pool });
    const result = getAcquisitionMissionRuntime().engine().evaluateOutcomeLearning(
      req.params.id,
      req.body || {},
      { tenantId }
    );
    await getAcquisitionMissionRuntime().persistMissionState(req.params.id, { pool });
    noStore(res);
    return res.json({
      spec: 'SPEC-166',
      ...result,
      autoApplied: false,
    });
  } catch (err) {
    console.error('[amo] evaluate outcome-learning', err);
    return fail(res, err, 'amo_evaluate_outcome_learning_failed', err.code === 'amo_prediction_not_found' ? 404 : 500);
  }
});

router.post('/api/v1/amo/missions/:id/execute', requireActor, async (req, res) => {
  try {
    const tenantId = actorTenantId(req);
    if (tenantId == null) {
      return res.status(400).json({ error: 'no_tenant', message: 'No active client selected.' });
    }
    const actor = actorFrom(req);
    observeOperatorHttp(req, res, {
      clientId: tenantId, missionId: req.params.id, source: 'amo_execute', routeHint: 'mission',
    });
    const routed = await executeCanonical({
      tenantId,
      missionId: req.params.id,
      intent: req.body?.intent,
      source: req.body?.source,
      operatorId: actor.id,
      role: actor.role,
      question: req.body?.question,
      executionMode: req.body?.executionMode,
      payload: req.body?.payload,
      metadata: req.body?.metadata,
      allowFixtureFallback: req.body?.allowFixtureFallback === true,
    }, { pool });
    noStore(res);
    return res.json({
      spec: 'SPEC-171',
      request: routed.request,
      action: routed.action,
      specialist: routed.specialist,
      runtimeOwner: routed.runtimeOwner,
      snapshot: routed.snapshot,
      execution: routed.execution || null,
      executionResult: routed.executionResult && {
        alreadyExecuted: routed.executionResult.alreadyExecuted === true,
        rolledBack: routed.executionResult.rolledBack === true,
        executionOutcome: routed.executionResult.executionOutcome || null,
        transactionId: routed.executionResult.transactionId || null,
        rollbackReason: routed.executionResult.rollbackReason || null,
        blockReason:
          (routed.executionResult.error && routed.executionResult.error.message) ||
          routed.executionResult.rollbackReason ||
          null,
        errorClass:
          (routed.executionResult.error && routed.executionResult.error.tmeClass) ||
          routed.executionResult.errorClass ||
          null,
      },
    });
  } catch (err) {
    console.error('[amo] execute', err);
    return fail(res, err, 'amo_execute_failed');
  }
});

/**
 * P0 — current mission authorization restrictions and the hash an amendment
 * must be bound to. Read-only.
 */
router.get('/api/v1/amo/missions/:id/authorization', requireActor, async (req, res) => {
  try {
    const tenantId = actorTenantId(req);
    if (tenantId == null) return res.status(400).json({ error: 'no_tenant', message: 'No active client selected.' });
    const preview = await previewMissionAuthorization({ tenantId, missionId: req.params.id }, { pool });
    noStore(res);
    return res.json({ spec: 'SPEC-MAX-RELIABILITY-004', ...preview });
  } catch (err) {
    return fail(res, err, 'amo_authorization_preview_failed');
  }
});

/**
 * P0 — governed amendment of mission restrictions. Max never calls this.
 * Admin or manager session; any LOOSENING (lift) needs an admin and a fresh,
 * single-use password confirmation (purpose mission_authorization_amendment)
 * on this exact session. The change commits durably in one TME stage bound to
 * the reviewed constraints hash; existing restrictions apply until it commits.
 */
router.post('/api/v1/amo/missions/:id/authorization/amendments',
  requireAuth, requireRole('admin', 'manager'), async (req, res) => {
    try {
      const tenantId = actorTenantId(req);
      if (tenantId == null) return res.status(400).json({ error: 'no_tenant', message: 'No active client selected.' });
      if (!req.is('application/json')) return res.status(415).json({ error: 'json_required' });
      const user = req.user || (req.session && req.session.user) || {};
      const lift = Array.isArray(req.body?.lift) ? req.body.lift : [];
      if (lift.length) {
        if (user.role !== 'admin') {
          return res.status(403).json({ error: 'authorization_amendment_forbidden', message: 'Lifting a mission restriction requires an admin.' });
        }
        const confirmed = await require('../utils/freshConfirmation').consume(pool, {
          sessionId: req.sessionID, userId: user.id, purpose: 'mission_authorization_amendment',
        }).catch(() => false);
        if (!confirmed) {
          return res.status(403).json({ error: 'fresh_confirmation_required', code: 'fresh_confirmation_required',
            writesMayHaveOccurred: false, retryAllowed: true });
        }
        // The marker was consumed in SQL; never re-save this request's stale snapshot.
        req.session = null;
      }
      const result = await amendMissionAuthorization({
        tenantId,
        missionId: req.params.id,
        change: { lift, add: Array.isArray(req.body?.add) ? req.body.add : [], reason: req.body?.reason },
        expectedConstraintsHash: req.body?.expectedConstraintsHash,
        amendmentId: req.body?.amendmentId,
        approver: { id: user.id != null ? String(user.id) : '', role: user.role, name: user.name || user.email },
      }, { pool });
      noStore(res);
      return res.status(result.alreadyApplied ? 200 : 201).json({ spec: 'SPEC-MAX-RELIABILITY-004', ...result });
    } catch (err) {
      return fail(res, err, 'amo_authorization_amendment_failed');
    }
  });

router.post('/api/v1/amo/ask', requireActor, async (req, res) => {
  try {
    const tenantId = actorTenantId(req);
    if (tenantId == null) {
      return res.status(400).json({ error: 'no_tenant', message: 'No active client selected.' });
    }
    observeOperatorHttp(req, res, {
      clientId: tenantId, missionId: req.body?.missionId, source: 'amo_ask', routeHint: 'inspection',
    });
    const answered = await answerOperator(req.body?.question, {
      tenantId,
      missionId: req.body?.missionId,
      previousReplyRate: req.body?.previousReplyRate,
    }, { pool });
    noStore(res);
    return res.json({ spec: 'SPEC-118', ...answered });
  } catch (err) {
    console.error('[amo] ask', err);
    return fail(res, err, 'amo_ask_failed');
  }
});

module.exports = router;
