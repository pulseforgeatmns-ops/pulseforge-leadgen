'use strict';

const express = require('express');
const path = require('path');
const { requireAuth, requireRole } = require('../middleware/auth');
const { getAuthenticatedActor } = require('../utils/requestIdentity');
const {
  listImpersonationTargets,
  startImpersonation,
  stopImpersonation,
  publicImpersonationState,
} = require('../services/aoImpersonationService');
const { normalizeClientId } = require('../utils/clientContext');

const router = express.Router();
const adminImpersonation = [requireAuth, requireRole('admin', 'manager')];

router.get('/admin/ao-impersonation', ...adminImpersonation, (_req, res) => {
  res.sendFile(path.join(__dirname, '..', 'public', 'admin-ao-impersonation.html'));
});

router.get('/api/v1/admin/impersonation', ...adminImpersonation, (req, res) => {
  const auth = getAuthenticatedActor(req);
  res.json({
    ok: true,
    impersonation: publicImpersonationState(req.session, auth),
  });
});

router.get('/api/v1/admin/impersonation/targets', ...adminImpersonation, async (req, res) => {
  try {
    const clientId = normalizeClientId(req.query.client_id ?? req.session?.active_client_id);
    const result = await listImpersonationTargets({
      actor: getAuthenticatedActor(req),
      clientId,
    });
    if (result.error) {
      return res.status(result.status || 400).json({
        error: result.error,
        message: result.message,
      });
    }
    return res.json(result);
  } catch (err) {
    console.error('[ao-impersonation] list targets:', err.message);
    return res.status(500).json({ error: 'list_targets_failed' });
  }
});

router.post('/api/v1/admin/impersonation/start', ...adminImpersonation, async (req, res) => {
  try {
    const userId = Number(req.body?.user_id ?? req.body?.userId);
    const clientId = normalizeClientId(req.body?.client_id ?? req.body?.clientId ?? req.session?.active_client_id);
    if (!Number.isInteger(userId) || userId <= 0) {
      return res.status(400).json({ error: 'user_id_required' });
    }
    const result = await startImpersonation({
      actor: getAuthenticatedActor(req),
      userId,
      clientId,
      session: req.session,
    });
    if (result.error) {
      return res.status(result.status || 400).json({
        error: result.error,
        message: result.message,
      });
    }
    return res.json(result);
  } catch (err) {
    console.error('[ao-impersonation] start:', err.message);
    return res.status(500).json({ error: 'start_failed' });
  }
});

router.post('/api/v1/admin/impersonation/stop', ...adminImpersonation, async (req, res) => {
  try {
    const result = await stopImpersonation({
      actor: getAuthenticatedActor(req),
      session: req.session,
    });
    return res.json(result);
  } catch (err) {
    console.error('[ao-impersonation] stop:', err.message);
    return res.status(500).json({ error: 'stop_failed' });
  }
});

module.exports = router;
