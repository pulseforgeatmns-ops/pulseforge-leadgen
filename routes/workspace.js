'use strict';

// Phase A2 canonical Prospect Workspace routes.
//
//   GET  /api/prospects/:prospectId/workspace         — canonical read model
//   GET  /api/prospects/:prospectId/call-preparation  — deterministic call prep
//   POST /api/prospects/:prospectId/notes             — structured note write
//   POST /api/prospects/:prospectId/lifecycle         — canonical stage change
//
// Tenant scoping is enforced server-side: setter/sales/client roles are locked
// to their assigned client_id; operators resolve through the active client
// session exactly like the rest of the dashboard. Existing routes are
// untouched — these are additive.
//
// Exported as a factory (createWorkspaceRouter) so HTTP tests can compose the
// router against a disposable PostgreSQL pool and a stub session.

const express = require('express');
const defaultPool = require('../db');
const { requireAuth: sessionAuth, requireRole } = require('../middleware/auth');
const { normalizeClientId } = require('../utils/clientContext');
const { getProspectWorkspace, addProspectNote } = require('../services/prospectWorkspace');
const { getCallPreparation } = require('../services/callPreparation');
const {
  CANONICAL_STAGES,
  transitionProspectLifecycle,
} = require('../services/lifecycleService');

const READ_ROLES = ['admin', 'manager', 'viewer', 'setter', 'sales'];
const WRITE_ROLES = ['admin', 'manager', 'setter', 'sales'];

function workspaceClientId(req) {
  const role = req.user?.role;
  if (['setter', 'sales', 'client'].includes(role)) {
    const assigned = Number(req.user?.client_id);
    return Number.isInteger(assigned) && assigned > 0 ? assigned : null;
  }
  return normalizeClientId(
    req.query?.client_id ||
    req.session?.active_client_id ||
    req.user?.client_id
  );
}

function createWorkspaceRouter({ pool = defaultPool } = {}) {
  const router = express.Router();
  const requireRead = [sessionAuth, requireRole(...READ_ROLES)];
  const requireWrite = [sessionAuth, requireRole(...WRITE_ROLES)];

  router.get('/api/prospects/:prospectId/workspace', requireRead, async (req, res) => {
    try {
      const clientId = workspaceClientId(req);
      if (!clientId) return res.status(403).json({ error: 'No client assignment' });
      const workspace = await getProspectWorkspace({
        pool,
        clientId,
        prospectId: req.params.prospectId,
        user: req.user,
      });
      if (!workspace) return res.status(404).json({ error: 'Prospect not found' });
      res.json(workspace);
    } catch (err) {
      console.error('[workspace] read error:', err.message);
      res.status(err.status || 500).json({ error: err.status ? err.message : 'Unable to load prospect workspace' });
    }
  });

  router.get('/api/prospects/:prospectId/call-preparation', requireRead, async (req, res) => {
    try {
      const clientId = workspaceClientId(req);
      if (!clientId) return res.status(403).json({ error: 'No client assignment' });
      const clientRow = await pool.query('SELECT name FROM clients WHERE id = $1 LIMIT 1', [clientId]);
      const preparation = await getCallPreparation({
        pool,
        clientId,
        prospectId: req.params.prospectId,
        user: req.user,
        clientName: clientRow.rows[0]?.name || 'the client',
      });
      if (!preparation) return res.status(404).json({ error: 'Prospect not found' });
      res.json(preparation);
    } catch (err) {
      console.error('[workspace] call preparation error:', err.message);
      res.status(err.status || 500).json({ error: err.status ? err.message : 'Unable to build call preparation' });
    }
  });

  router.post('/api/prospects/:prospectId/notes', requireWrite, async (req, res) => {
    try {
      const clientId = workspaceClientId(req);
      if (!clientId) return res.status(403).json({ error: 'No client assignment' });
      const noteType = ['operator', 'call', 'research', 'system'].includes(req.body?.note_type)
        ? req.body.note_type
        : 'operator';
      const note = await addProspectNote({
        pool,
        clientId,
        prospectId: req.params.prospectId,
        noteType,
        text: req.body?.text,
        author: { id: req.user?.id, name: req.user?.name },
        source: String(req.body?.source || 'workspace').slice(0, 80),
      });
      res.status(201).json({ success: true, note });
    } catch (err) {
      if (!err.status) console.error('[workspace] note error:', err.message);
      res.status(err.status || 500).json({ error: err.status ? err.message : 'Unable to save note' });
    }
  });

  router.post('/api/prospects/:prospectId/lifecycle', requireWrite, async (req, res) => {
    try {
      const clientId = workspaceClientId(req);
      if (!clientId) return res.status(403).json({ error: 'No client assignment' });
      const targetStage = String(req.body?.target_stage || '').trim();
      if (!CANONICAL_STAGES.includes(targetStage)) {
        return res.status(400).json({ error: `target_stage must be one of: ${CANONICAL_STAGES.join(', ')}` });
      }
      const reason = String(req.body?.reason || '').trim().slice(0, 2000) || null;
      if (targetStage === 'dead' && !reason) {
        return res.status(400).json({ error: 'Dead transitions require a reason' });
      }
      if (targetStage === 'booked' && !reason) {
        return res.status(400).json({ error: 'Booked transitions require a handoff note' });
      }
      let callback;
      if (Object.prototype.hasOwnProperty.call(req.body || {}, 'callback_at')) {
        const at = req.body.callback_at ? new Date(req.body.callback_at) : null;
        if (at && Number.isNaN(at.getTime())) return res.status(400).json({ error: 'Invalid callback time' });
        callback = { at, mode: 'reschedule' };
      }
      const result = await transitionProspectLifecycle({
        pool,
        clientId,
        prospectId: req.params.prospectId,
        targetStage,
        reason,
        callback,
        handoffNote: targetStage === 'booked' ? reason : null,
        requireVisible: ['setter', 'sales'].includes(req.user?.role),
        actor: { type: 'user', id: req.user?.id, name: req.user?.name, role: req.user?.role },
        source: 'workspace_lifecycle_endpoint',
        idempotencyKey: String(req.body?.idempotency_key || req.get('Idempotency-Key') || '').trim().slice(0, 200) || null,
      });
      const workspace = await getProspectWorkspace({
        pool,
        clientId,
        prospectId: req.params.prospectId,
        user: req.user,
      });
      res.json({
        success: true,
        idempotent: result.idempotentReplay,
        transition: {
          from: result.event?.from_stage || null,
          to: result.event?.to_stage || targetStage,
        },
        workspace,
      });
    } catch (err) {
      if (!err.status) console.error('[workspace] lifecycle error:', err.message);
      res.status(err.status || 500).json({ error: err.status ? err.message : 'Unable to change lifecycle stage' });
    }
  });

  return router;
}

module.exports = createWorkspaceRouter();
module.exports.createWorkspaceRouter = createWorkspaceRouter;
module.exports.workspaceClientId = workspaceClientId;
