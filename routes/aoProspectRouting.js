'use strict';

const express = require('express');
const { requireAuth, requireRole } = require('../middleware/auth');
const { normalizeClientId } = require('../utils/clientContext');
const { ensureAoFieldSchema } = require('../utils/aoFieldSchema');
const { ensureAoProspectRoutingSchema } = require('../utils/aoProspectRoutingSchema');
const routingService = require('../services/aoProspectRoutingService');
const taskService = require('../services/aoProspectTaskService');
const debriefService = require('../services/aoAdvisoryDebriefService');
const inspection = require('../services/aoMissionInspection');

const router = express.Router();

function resolveClientId(req) {
  if (req.user?.role === 'ao') {
    const assigned = Number(req.user.client_id);
    return Number.isInteger(assigned) && assigned > 0 ? assigned : null;
  }
  return normalizeClientId(req.session?.active_client_id || req.user?.client_id || req.query.client_id) || 10;
}

function effectiveAoOwnerId(req) {
  if (req.user.role === 'ao') return req.user.id;
  const override = Number(req.query.ao_owner_id || req.body?.ao_owner_id);
  return Number.isInteger(override) && override > 0 ? override : null;
}

function wrap(handler) {
  return async (req, res, next) => {
    try {
      await ensureAoFieldSchema();
      await ensureAoProspectRoutingSchema();
      await handler(req, res, next);
    } catch (err) {
      console.error('[ao-prospect-routing]', req.method, req.originalUrl, err.message);
      if (!res.headersSent) res.status(500).json({ error: err.message || 'AO prospect routing failed' });
    }
  };
}

router.post('/api/v1/ao/routing/evaluate', requireAuth, requireRole('admin', 'manager', 'ao'), wrap(async (req, res) => {
  const clientId = resolveClientId(req);
  if (!clientId) return res.status(400).json({ error: 'client_id required' });
  const { prospect_id: prospectId } = req.body || {};
  if (!prospectId) return res.status(400).json({ error: 'prospect_id required' });

  const bundle = await taskService.fetchProspectBundle(prospectId, clientId);
  if (!bundle) return res.status(404).json({ error: 'Prospect not found' });

  const availableAos = await taskService.fetchAvailableAos(clientId);
  const routing = routingService.routeProspect({
    prospect: bundle.prospect,
    company: bundle.company,
    touchpoints: bundle.touchpoints,
    availableAos,
    existingAssignment: bundle.prospect,
  });

  res.json({
    prospect_id: prospectId,
    routing,
    formatted_task: taskService.formatAoTask(routing),
  });
}));

router.post('/api/v1/ao/routing/route', requireAuth, requireRole('admin', 'manager', 'ao'), wrap(async (req, res) => {
  const clientId = resolveClientId(req);
  if (!clientId) return res.status(400).json({ error: 'client_id required' });
  const { prospect_id: prospectId, ao_name: aoName } = req.body || {};
  if (!prospectId) return res.status(400).json({ error: 'prospect_id required' });
  const result = await taskService.routeAndPersistProspect({ clientId, prospectId, aoName });
  if (!result) return res.status(404).json({ error: 'Prospect not found' });
  res.json(result);
}));

router.post('/api/v1/ao/tasks/generate-weekly', requireAuth, requireRole('admin', 'manager'), wrap(async (req, res) => {
  const clientId = resolveClientId(req);
  if (!clientId) return res.status(400).json({ error: 'client_id required' });
  const { prospect_ids: prospectIds } = req.body || {};
  const result = await taskService.generateWeeklyAoTasks({ clientId, prospectIds });
  res.json(result);
}));

router.get('/api/v1/ao/tasks', requireAuth, requireRole('admin', 'manager', 'ao'), wrap(async (req, res) => {
  const clientId = resolveClientId(req);
  if (!clientId) return res.status(400).json({ error: 'client_id required' });
  const aoOwnerId = effectiveAoOwnerId(req);
  const tasks = await taskService.listOpenTasks({ clientId, aoOwnerId });
  res.json({ tasks });
}));

router.get('/api/v1/ao/tasks/:id', requireAuth, requireRole('admin', 'manager', 'ao'), wrap(async (req, res) => {
  const clientId = resolveClientId(req);
  if (!clientId) return res.status(400).json({ error: 'client_id required' });
  const task = await taskService.getTaskById(req.params.id, { clientId });
  if (!task) return res.status(404).json({ error: 'Task not found' });
  res.json(task);
}));

router.post('/api/v1/ao/debriefs', requireAuth, requireRole('admin', 'manager', 'ao'), wrap(async (req, res) => {
  const clientId = resolveClientId(req);
  if (!clientId) return res.status(400).json({ error: 'client_id required' });
  const aoOwnerId = effectiveAoOwnerId(req) || req.user.id;
  const {
    prospect_id: prospectId,
    task_id: taskId,
    debrief = {},
  } = req.body || {};
  if (!prospectId) return res.status(400).json({ error: 'prospect_id required' });
  const result = await debriefService.submitDebrief({
    clientId,
    prospectId,
    taskId,
    aoOwnerId,
    debrief,
  });
  res.json(result);
}));

router.get('/api/v1/ao/debriefs', requireAuth, requireRole('admin', 'manager', 'ao'), wrap(async (req, res) => {
  const clientId = resolveClientId(req);
  if (!clientId) return res.status(400).json({ error: 'client_id required' });
  const aoOwnerId = effectiveAoOwnerId(req);
  const debriefs = await debriefService.listDebriefs({ clientId, aoOwnerId });
  res.json({ debriefs });
}));

router.get('/api/v1/ao/inspection/work-today', requireAuth, requireRole('admin', 'manager', 'ao'), wrap(async (req, res) => {
  const clientId = resolveClientId(req);
  if (!clientId) return res.status(400).json({ error: 'client_id required' });
  const aoOwnerId = effectiveAoOwnerId(req);
  const items = await inspection.prospectsToWorkToday({ clientId, aoOwnerId });
  res.json({ items });
}));

router.get('/api/v1/ao/inspection/assignment/:prospectId', requireAuth, requireRole('admin', 'manager', 'ao'), wrap(async (req, res) => {
  const clientId = resolveClientId(req);
  if (!clientId) return res.status(400).json({ error: 'client_id required' });
  const item = await inspection.explainAssignment(req.params.prospectId, clientId);
  if (!item) return res.status(404).json({ error: 'Prospect not found' });
  res.json(item);
}));

router.post('/api/v1/ao/inspection/ask', requireAuth, requireRole('admin', 'manager', 'ao'), wrap(async (req, res) => {
  const clientId = resolveClientId(req);
  if (!clientId) return res.status(400).json({ error: 'client_id required' });
  const { question, prospect_id: prospectId } = req.body || {};
  const aoOwnerId = effectiveAoOwnerId(req);
  const answer = await inspection.answerInspectionQuestion(question, {
    clientId,
    aoOwnerId,
    prospectId,
  });
  res.json(answer);
}));

module.exports = router;
