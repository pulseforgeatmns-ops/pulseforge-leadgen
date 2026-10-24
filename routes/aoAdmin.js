const express = require('express');
const path = require('path');
const { requireAuth: sessionAuth, requireRole } = require('../middleware/auth');
const { normalizeClientId } = require('../utils/clientContext');
const { ensureAoFieldSchema } = require('../utils/aoFieldSchema');
const aoField = require('../services/aoFieldService');

const router = express.Router();
const adminOnly = [sessionAuth, requireRole('admin', 'manager')];

function adminClientId(req) {
  return normalizeClientId(req.session?.active_client_id || req.user?.client_id) || 10;
}

router.get('/', ...adminOnly, (_req, res) => {
  res.sendFile(path.join(__dirname, '..', 'public', 'admin-field-visits.html'));
});

router.get('/api/visits', ...adminOnly, async (req, res) => {
  try {
    await ensureAoFieldSchema();
    const clientId = adminClientId(req);
    const escalatedOnly = String(req.query.escalated || '') === '1';
    const visits = await aoField.listAdminVisits({ clientId, escalatedOnly });
    res.json({ client_id: clientId, visits });
  } catch (err) {
    console.error('[ao-admin] list visits:', err.message);
    res.status(500).json({ error: err.message });
  }
});

router.get('/api/visits/:id', ...adminOnly, async (req, res) => {
  try {
    await ensureAoFieldSchema();
    const clientId = adminClientId(req);
    const visit = await aoField.getAdminVisit(req.params.id, clientId);
    if (!visit) return res.status(404).json({ error: 'Visit not found' });
    res.json(visit);
  } catch (err) {
    console.error('[ao-admin] get visit:', err.message);
    res.status(500).json({ error: err.message });
  }
});

router.patch('/api/escalations/:id', ...adminOnly, async (req, res) => {
  try {
    await ensureAoFieldSchema();
    const updated = await aoField.updateEscalation(req.params.id, req.body || {});
    if (!updated) return res.status(404).json({ error: 'Escalation not found' });
    res.json(updated);
  } catch (err) {
    console.error('[ao-admin] patch escalation:', err.message);
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
