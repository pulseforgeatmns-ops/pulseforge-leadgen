const express = require('express');
const path = require('path');
const pool = require('../db');
const { requireAuth: sessionAuth, requireRole } = require('../middleware/auth');
const { ensureCloserSchema } = require('../utils/closerSchema');

const router = express.Router();
const CLOSER_STATUSES = ['contacted', 'showed', 'lost'];

function clientId(req) {
  return Number(req.session?.active_client_id || req.query.client_id || 1);
}

function closerFilter(req, params) {
  if (['admin', 'manager'].includes(req.user?.role)) return '';
  params.push(req.user.id);
  return `AND p.closer_id = $${params.length}`;
}

function money(value) {
  return Number(value || 0);
}

router.get('/', sessionAuth, requireRole('admin', 'manager', 'closer', 'sales'), async (_req, res) => {
  await ensureCloserSchema().catch(err => console.error('[closer] schema error:', err.message));
  res.sendFile(path.join(__dirname, '..', 'public', 'closer-dashboard.html'));
});

router.get(['/api/metrics', '/metrics'], sessionAuth, requireRole('admin', 'manager', 'closer', 'sales'), async (req, res) => {
  try {
    await ensureCloserSchema();
    const params = [clientId(req)];
    const filter = closerFilter(req, params);
    const { rows } = await pool.query(`
      SELECT
        COUNT(*) FILTER (WHERE p.booked_at >= date_trunc('week', NOW()))::int AS booked_week,
        COUNT(*) FILTER (WHERE p.closer_status IN ('showed','closed') OR p.setter_status = 'closed')::int AS showed,
        COUNT(*) FILTER (WHERE p.booked_at IS NOT NULL)::int AS booked_total,
        COUNT(*) FILTER (WHERE p.setter_status = 'closed')::int AS closed,
        COALESCE(SUM(p.mrr_value) FILTER (WHERE p.setter_status = 'closed' AND p.closed_at >= date_trunc('month', NOW())), 0)::numeric AS mrr_month,
        COALESCE(SUM(c.commission_amt) FILTER (WHERE c.created_at >= date_trunc('month', NOW()) AND c.status <> 'void'), 0)::numeric AS commission_month
      FROM prospects p
      LEFT JOIN commissions c ON c.prospect_id = p.id
      WHERE p.client_id = $1
        ${filter}
    `, params);
    const m = rows[0] || {};
    const bookedTotal = Number(m.booked_total || 0);
    const showed = Number(m.showed || 0);
    const closed = Number(m.closed || 0);
    res.json({
      booked_week: Number(m.booked_week || 0),
      show_rate: bookedTotal ? +((showed / bookedTotal) * 100).toFixed(1) : 0,
      close_rate: showed ? +((closed / showed) * 100).toFixed(1) : 0,
      mrr_month: money(m.mrr_month),
      commission_month: money(m.commission_month),
    });
  } catch (err) {
    console.error('[closer] metrics error:', err.message);
    res.status(500).json({ error: 'Unable to load closer metrics' });
  }
});

router.get(['/api/pipeline', '/pipeline'], sessionAuth, requireRole('admin', 'manager', 'closer', 'sales'), async (req, res) => {
  try {
    await ensureCloserSchema();
    const params = [clientId(req)];
    const filter = closerFilter(req, params);
    const { rows } = await pool.query(`
      SELECT p.*, u.name AS closer_name
      FROM prospects p
      LEFT JOIN users u ON u.id = p.closer_id
      WHERE p.client_id = $1
        AND p.setter_status = 'booked'
        ${filter}
      ORDER BY p.booked_at DESC NULLS LAST, p.icp_score DESC NULLS LAST
      LIMIT 500
    `, params);
    res.json(rows.map(row => ({
      id: row.id,
      business_name: String(row.notes || '').split('\n\n--- setter notes ---\n')[0].split('—')[0].trim() || `${row.first_name || ''} ${row.last_name || ''}`.trim() || row.email || 'Unknown Lead',
      vertical: row.vertical || 'unknown',
      city: row.service_area_match || row.city || 'Providence RI',
      score: Number(row.icp_score || 0),
      booked_at: row.booked_at,
      setter_notes: String(row.notes || '').split('\n\n--- setter notes ---\n')[1] || '',
      phone: row.phone,
      email: row.email,
      contact_name: `${row.first_name || ''} ${row.last_name || ''}`.trim(),
      call_status: row.closer_status || 'booked',
      closer_name: row.closer_name,
    })));
  } catch (err) {
    console.error('[closer] pipeline error:', err.message);
    res.status(500).json({ error: 'Unable to load closer pipeline' });
  }
});

router.patch(['/api/prospects/:id/status', '/prospects/:id/status'], sessionAuth, requireRole('admin', 'manager', 'closer', 'sales'), async (req, res) => {
  const status = String(req.body.status || '');
  if (!CLOSER_STATUSES.includes(status)) return res.status(400).json({ error: 'Invalid closer status' });
  try {
    await ensureCloserSchema();
    const setterStatus = status === 'lost' ? 'dead' : 'booked';
    const params = [status, setterStatus, req.params.id, clientId(req)];
    let filter = '';
    if (!['admin', 'manager'].includes(req.user?.role)) {
      params.push(req.user.id);
      filter = `AND closer_id = $${params.length}`;
    }
    const { rows } = await pool.query(`
      UPDATE prospects
      SET closer_status = $1,
          setter_status = $2,
          setter_updated_at = NOW()
      WHERE id = $3 AND client_id = $4 ${filter}
      RETURNING *
    `, params);
    if (!rows.length) return res.status(404).json({ error: 'Prospect not found' });
    res.json({ success: true });
  } catch (err) {
    console.error('[closer] status error:', err.message);
    res.status(500).json({ error: 'Unable to update call status' });
  }
});

router.post(['/api/prospects/:id/close', '/prospects/:id/close'], sessionAuth, requireRole('admin', 'manager', 'closer', 'sales'), async (req, res) => {
  const mrr = Number(req.body.mrr_amount || 0);
  if (!Number.isFinite(mrr) || mrr <= 0) return res.status(400).json({ error: 'MRR amount is required' });
  try {
    await ensureCloserSchema();
    const params = [req.params.id, clientId(req)];
    let filter = '';
    if (!['admin', 'manager'].includes(req.user?.role)) {
      params.push(req.user.id);
      filter = `AND closer_id = $${params.length}`;
    }
    const prospect = await pool.query(`
      UPDATE prospects
      SET setter_status = 'closed',
          closer_status = 'closed',
          closed_at = COALESCE(closed_at, NOW()),
          mrr_value = $${params.length + 1},
          close_notes = $${params.length + 2},
          setter_updated_at = NOW()
      WHERE id = $1 AND client_id = $2 ${filter}
      RETURNING *
    `, [...params, mrr, String(req.body.notes || '').trim()]);
    if (!prospect.rows.length) return res.status(404).json({ error: 'Prospect not found' });
    const row = prospect.rows[0];
    const existing = await pool.query(`
      SELECT id FROM commissions
      WHERE prospect_id = $1 AND closer_id = $2 AND status <> 'void'
      LIMIT 1
    `, [row.id, row.closer_id]);
    let commission;
    if (existing.rows.length) {
      commission = await pool.query(`
        UPDATE commissions
        SET mrr_amount = $1, closed_at = $2, notes = $3
        WHERE id = $4
        RETURNING *
      `, [mrr, row.closed_at, row.close_notes, existing.rows[0].id]);
    } else {
      commission = await pool.query(`
        INSERT INTO commissions (closer_id, prospect_id, client_id, mrr_amount, closed_at, notes)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING *
      `, [row.closer_id, row.id, row.client_id || clientId(req), mrr, row.closed_at, row.close_notes]);
    }
    res.json({ success: true, commission: commission.rows[0] });
  } catch (err) {
    console.error('[closer] close error:', err.message);
    res.status(500).json({ error: 'Unable to close prospect' });
  }
});

router.get(['/api/commissions', '/commissions'], sessionAuth, requireRole('admin', 'manager', 'closer', 'sales'), async (req, res) => {
  try {
    await ensureCloserSchema();
    const params = [clientId(req)];
    let filter = '';
    if (!['admin', 'manager'].includes(req.user?.role)) {
      params.push(req.user.id);
      filter = `AND c.closer_id = $${params.length}`;
    }
    const { rows } = await pool.query(`
      SELECT c.*, p.notes, p.first_name, p.last_name, p.email, p.vertical
      FROM commissions c
      JOIN prospects p ON p.id = c.prospect_id
      WHERE c.client_id = $1 ${filter}
      ORDER BY c.closed_at DESC NULLS LAST, c.created_at DESC
    `, params);
    res.json(rows.map(row => ({
      id: row.id,
      business_name: String(row.notes || '').split('\n\n--- setter notes ---\n')[0].split('—')[0].trim() || `${row.first_name || ''} ${row.last_name || ''}`.trim() || row.email || 'Unknown Lead',
      mrr_amount: money(row.mrr_amount),
      commission_amt: money(row.commission_amt),
      commission_rate: Number(row.commission_rate || 0.15),
      status: row.status,
      closed_at: row.closed_at,
      paid_at: row.paid_at,
    })));
  } catch (err) {
    console.error('[closer] commissions error:', err.message);
    res.status(500).json({ error: 'Unable to load commissions' });
  }
});

router.patch(['/api/commissions/:id/pay', '/commissions/:id/pay'], sessionAuth, requireRole('admin', 'manager'), async (req, res) => {
  try {
    await ensureCloserSchema();
    const { rows } = await pool.query(`
      UPDATE commissions
      SET status = 'paid', paid_at = NOW()
      WHERE id = $1 AND client_id = $2
      RETURNING *
    `, [req.params.id, clientId(req)]);
    if (!rows.length) return res.status(404).json({ error: 'Commission not found' });
    res.json({ success: true, commission: rows[0] });
  } catch (err) {
    console.error('[closer] commission pay error:', err.message);
    res.status(500).json({ error: 'Unable to mark commission paid' });
  }
});

module.exports = router;
