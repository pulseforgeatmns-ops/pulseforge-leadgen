const express = require('express');
const path = require('path');
const pool = require('../db');
const { requireAuth: sessionAuth, requireRole } = require('../middleware/auth');
const { normalizeClientId } = require('../utils/clientContext');
const { ensureAoFieldSchema } = require('../utils/aoFieldSchema');
const { TEMPLATES } = require('../utils/aoMessageTemplates');
const aoField = require('../services/aoFieldService');
const aoMax = require('../services/aoMaxFlow');

const router = express.Router();

function aoClientId(req) {
  if (req.user?.role === 'ao') {
    const assigned = Number(req.user.client_id);
    return Number.isInteger(assigned) && assigned > 0 ? assigned : null;
  }
  return normalizeClientId(req.session?.active_client_id || req.user?.client_id) || 10;
}

function requireAoRead(req, res, next) {
  return sessionAuth(req, res, err => {
    if (err) return next(err);
    return requireRole('admin', 'manager', 'ao')(req, res, next);
  });
}

function requireAoWrite(req, res, next) {
  return sessionAuth(req, res, err => {
    if (err) return next(err);
    return requireRole('admin', 'manager', 'ao')(req, res, next);
  });
}

function requireJakeRead(req, res, next) {
  return sessionAuth(req, res, err => {
    if (err) return next(err);
    return requireRole('admin', 'manager')(req, res, next);
  });
}

function effectiveAoOwnerId(req) {
  if (req.user.role === 'ao') return req.user.id;
  const override = Number(req.query.ao_owner_id || req.body?.ao_owner_id);
  return Number.isInteger(override) && override > 0 ? override : req.user.id;
}

async function refreshAoSession(req, _res, next) {
  if (req.user?.role !== 'ao' || !req.user?.id) return next();
  try {
    const profile = await aoField.getAoProfile(req.user.id);
    if (profile?.client_id && profile.client_id !== req.user.client_id) {
      req.user.client_id = profile.client_id;
      if (req.session?.user) req.session.user.client_id = profile.client_id;
    }
  } catch (err) {
    console.error('[ao] session refresh failed:', err.message);
  }
  return next();
}

function sessionProfile(req) {
  return {
    id: req.user.id,
    name: req.user.name,
    email: req.user.email,
    role: req.user.role,
    client_id: req.user.client_id || aoClientId(req),
    active: true,
  };
}

function wrapAoHandler(handler) {
  return async (req, res, next) => {
    try {
      await ensureAoFieldSchema();
      await handler(req, res, next);
    } catch (err) {
      console.error('[ao] route error:', req.method, req.originalUrl, err.message);
      if (!res.headersSent) {
        res.status(500).json({ error: err.message || 'AO request failed' });
      }
    }
  };
}

function requireAoClient(req, res) {
  const clientId = aoClientId(req);
  if (!clientId) {
    res.status(400).json({
      error: 'AO client not configured — assign client_id (e.g. 10 for Anchor Cleaning) in /admin/users',
    });
    return null;
  }
  return clientId;
}

router.get('/', requireAoRead, (_req, res) => {
  res.sendFile(path.join(__dirname, '..', 'public', 'ao-dashboard.html'));
});

router.get('/api/profile', requireAoRead, refreshAoSession, wrapAoHandler(async (req, res) => {
  const profile = await aoField.getAoProfile(req.user.id);
  if (profile) return res.json(profile);
  res.json(sessionProfile(req));
}));

router.get('/api/queue', requireAoRead, refreshAoSession, wrapAoHandler(async (req, res) => {
  const clientId = requireAoClient(req, res);
  if (!clientId) return;
  const aoOwnerId = effectiveAoOwnerId(req);
  const filter = String(req.query.filter || 'today');
  const tasks = await aoField.listQueue({ aoOwnerId, clientId, filter });
  res.json({ filter, tasks });
}));

router.get('/api/leads/:id', requireAoRead, wrapAoHandler(async (req, res) => {
  const aoOwnerId = effectiveAoOwnerId(req);
  const lead = await aoField.getLeadDetail(req.params.id, aoOwnerId);
  if (!lead) return res.status(404).json({ error: 'Lead not found' });
  res.json(lead);
}));

router.patch('/api/tasks/:id', requireAoWrite, wrapAoHandler(async (req, res) => {
  if (req.user.role !== 'ao' && req.user.role !== 'admin' && req.user.role !== 'manager') {
    return res.status(403).json({ error: 'Forbidden' });
  }
  const aoOwnerId = effectiveAoOwnerId(req);
  const task = await aoField.updateTask(req.params.id, aoOwnerId, req.body || {});
  if (!task) return res.status(404).json({ error: 'Task not found' });
  res.json(task);
}));

router.post('/api/tasks/:id/escalate', requireAoWrite, wrapAoHandler(async (req, res) => {
  const clientId = requireAoClient(req, res);
  if (!clientId) return;
  const aoOwnerId = effectiveAoOwnerId(req);
  const { reason = 'high_interest', summary } = req.body || {};
  const result = await aoField.escalateTask(req.params.id, aoOwnerId, {
    reason,
    summary: summary || 'AO requested Jake follow-up',
  });
  if (!result) return res.status(404).json({ error: 'Task not found' });

  await aoField.depositEscalationAction(
    result.escalation,
    { id: result.task.lead_id, business_name: result.task.business_name },
    clientId,
  );
  await aoField.notifyJakeEscalation(
    result.escalation,
    { business_name: result.task.business_name },
    req.user.name,
  ).catch(err => console.error('[ao] Jake notification failed:', err.message));

  res.json(result);
}));

router.get('/api/templates', requireAoRead, (_req, res) => {
  res.json({ templates: Object.values(TEMPLATES) });
});

router.post('/api/max/start', requireAoWrite, refreshAoSession, wrapAoHandler(async (req, res) => {
  const clientId = requireAoClient(req, res);
  if (!clientId) return;
  const aoOwnerId = effectiveAoOwnerId(req);
  const { mode, task_id: taskId } = req.body || {};
  if (!mode) return res.status(400).json({ error: 'mode required' });

  const result = await aoMax.startMode({
    aoOwnerId,
    clientId,
    mode,
    aoName: req.user.name,
    taskId,
  });
  if (result.status) return res.status(result.status).json({ error: result.error });
  res.json(result);
}));

router.post('/api/max/respond', requireAoWrite, refreshAoSession, wrapAoHandler(async (req, res) => {
  const clientId = requireAoClient(req, res);
  if (!clientId) return;
  const aoOwnerId = effectiveAoOwnerId(req);
  const { session_id: sessionId, message } = req.body || {};
  if (!sessionId || !message) return res.status(400).json({ error: 'session_id and message required' });

  const result = await aoMax.respondToSession({
    sessionId,
    aoOwnerId,
    clientId,
    aoName: req.user.name,
    message,
  });
  if (result.status) return res.status(result.status).json({ error: result.error });
  res.json(result);
}));

router.get('/api/escalations', requireJakeRead, wrapAoHandler(async (req, res) => {
  const clientId = aoClientId(req);
  const status = req.query.status ? String(req.query.status) : null;
  const escalations = await aoField.listEscalations({ clientId, status });
  res.json({ escalations });
}));

router.patch('/api/escalations/:id', requireJakeRead, wrapAoHandler(async (req, res) => {
  const updated = await aoField.updateEscalation(req.params.id, req.body || {});
  if (!updated) return res.status(404).json({ error: 'Escalation not found' });
  res.json(updated);
}));

router.get('/api/metrics/today', requireAoRead, refreshAoSession, wrapAoHandler(async (req, res) => {
  const aoOwnerId = effectiveAoOwnerId(req);
  const clientId = aoClientId(req);
  const today = new Date().toISOString().slice(0, 10);

  if (!clientId) {
    const profile = await aoField.getAoProfile(req.user.id);
    return res.json({
      visits_logged: 0,
      due_today: 0,
      overdue: 0,
      pending_escalations: 0,
      daily_goal: profile?.daily_goal || null,
      weekly_goal: profile?.weekly_goal || null,
      client_missing: true,
    });
  }

  const { rows } = await pool.query(`
    SELECT
      (SELECT COUNT(*)::int FROM ao_leads WHERE ao_owner_id = $1 AND client_id = $2 AND DATE(created_at) = $3) AS visits_logged,
      (SELECT COUNT(*)::int FROM ao_follow_up_tasks WHERE ao_owner_id = $1 AND status = 'open' AND due_date = $3) AS due_today,
      (SELECT COUNT(*)::int FROM ao_follow_up_tasks WHERE ao_owner_id = $1 AND status = 'open' AND due_date < $3) AS overdue,
      (SELECT COUNT(*)::int FROM ao_escalations WHERE ao_owner_id = $1 AND status = 'new') AS pending_escalations
  `, [aoOwnerId, clientId, today]);

  const profile = req.user.role === 'ao' ? await aoField.getAoProfile(req.user.id) : null;
  res.json({
    ...rows[0],
    daily_goal: profile?.daily_goal || null,
    weekly_goal: profile?.weekly_goal || null,
  });
}));

module.exports = router;
