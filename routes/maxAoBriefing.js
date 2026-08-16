'use strict';

const path = require('path');
const express = require('express');
const router = express.Router();
const { requireAuth, requireRole } = require('../middleware/auth');
const { getRequestClientId, normalizeClientId } = require('../utils/clientContext');
const { ensureAoFieldSchema } = require('../utils/aoFieldSchema');
const aoBriefing = require('../services/aoBriefingService');
const aoField = require('../services/aoFieldService');

const requireJakeRead = [requireAuth, requireRole('admin', 'manager')];

function resolveClientId(req) {
  if (req.query.client_id != null && req.query.client_id !== '') {
    const role = req.session?.user?.role;
    if (role === 'admin' || role === 'manager') {
      return normalizeClientId(req.query.client_id);
    }
  }
  return getRequestClientId(req);
}

function wrap(handler) {
  return async (req, res, next) => {
    try {
      await ensureAoFieldSchema();
      await handler(req, res, next);
    } catch (err) {
      console.error('[max-ao-briefing]', req.method, req.originalUrl, err.message);
      if (!res.headersSent) {
        res.status(500).json({ error: err.message || 'AO briefing request failed' });
      }
    }
  };
}

router.get('/max-briefing', requireJakeRead, (_req, res) => {
  res.sendFile(path.join(__dirname, '..', 'public', 'max-ao-briefing.html'));
});

router.use(
  '/max-briefing',
  express.static(path.join(__dirname, '..', 'public', 'max-ao-briefing'), {
    index: false,
    fallthrough: true,
  })
);

router.get('/api/v1/max/ao-briefing', requireJakeRead, wrap(async (req, res) => {
  const clientId = resolveClientId(req);
  if (!clientId) return res.status(400).json({ error: 'client_id required' });
  const asOf = req.query.asOf ? String(req.query.asOf).slice(0, 10) : undefined;
  const briefing = await aoBriefing.buildBriefing(clientId, { asOf });
  res.set('Cache-Control', 'no-store');
  res.json(briefing);
}));

router.get('/api/v1/max/ao-briefing/digest', requireJakeRead, wrap(async (req, res) => {
  const clientId = resolveClientId(req);
  if (!clientId) return res.status(400).json({ error: 'client_id required' });
  const briefing = await aoBriefing.buildBriefing(clientId);
  res.json(briefing.daily_digest);
}));

router.get('/api/v1/max/ao-escalations', requireJakeRead, wrap(async (req, res) => {
  const clientId = resolveClientId(req);
  if (!clientId) return res.status(400).json({ error: 'client_id required' });
  const status = req.query.status ? String(req.query.status) : null;
  const includeResolved = req.query.include_resolved === '1';
  const escalations = await aoBriefing.listEscalationInbox(clientId, { status, includeResolved });
  res.json({ escalations });
}));

router.patch('/api/v1/max/ao-escalations/:id', requireJakeRead, wrap(async (req, res) => {
  const { status } = req.body || {};
  if (!status) return res.status(400).json({ error: 'status required' });
  const allowed = ['new', 'seen', 'in_progress', 'resolved', 'ignored'];
  if (!allowed.includes(status)) return res.status(400).json({ error: 'invalid status' });
  const updated = await aoField.updateEscalation(req.params.id, { status });
  if (!updated) return res.status(404).json({ error: 'Escalation not found' });
  res.json(updated);
}));

router.post('/api/v1/max/ao-escalations/:id/assign-follow-up', requireJakeRead, wrap(async (req, res) => {
  const clientId = resolveClientId(req);
  if (!clientId) return res.status(400).json({ error: 'client_id required' });
  const task = await aoBriefing.assignEscalationFollowUp(req.params.id, clientId, req.body || {});
  if (!task) return res.status(404).json({ error: 'Escalation not found' });
  res.json({ task });
}));

router.post('/api/v1/max/ao-leads/:id/promote', requireJakeRead, wrap(async (req, res) => {
  const clientId = resolveClientId(req);
  if (!clientId) return res.status(400).json({ error: 'client_id required' });
  const result = await aoBriefing.promoteLeadToCrm(
    req.params.id,
    clientId,
    req.session?.user?.id || null
  );
  if (result.error) return res.status(result.status || 400).json(result);
  res.json(result);
}));

router.post('/api/v1/max/ao-briefing/ask', requireJakeRead, wrap(async (req, res) => {
  const clientId = resolveClientId(req);
  if (!clientId) return res.status(400).json({ error: 'client_id required' });
  const question = String(req.body?.question || '').trim();
  if (!question) return res.status(400).json({ error: 'question required' });
  const result = await aoBriefing.answerAoQuestion(clientId, question);
  res.json({ question, ...result });
}));

router.get('/api/v1/max/ao-briefing/campaign', requireJakeRead, wrap(async (req, res) => {
  const clientId = resolveClientId(req);
  if (!clientId) return res.status(400).json({ error: 'client_id required' });
  const progress = await aoBriefing.getCampaign001Progress(clientId);
  res.json(progress);
}));

module.exports = router;
