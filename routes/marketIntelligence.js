'use strict';

/**
 * SPEC-065 — GET-only Market Intelligence query APIs.
 * Observational corpus. No recommendations, scoring, or Max wiring.
 *
 * GET /api/v1/market-intel/companies
 * GET /api/v1/market-intel/companies/:id
 * GET /api/v1/market-intel/companies/:id/timeline
 * GET /api/v1/market-intel/emails/:id
 * GET /api/v1/market-intel/cross-market/patterns?field=
 * GET /api/v1/market-intel/cross-market/sequence-stats
 */

const express = require('express');
const router = express.Router();
const { requireAuth, requireRole } = require('../middleware/auth');
const {
  crossMarketPatterns,
  crossMarketSequenceStats,
  diffTimelineField,
  getCompanyCampaignTimeline,
  getCompanyProfile,
  getEmailEvidence,
  listMarketCompanies,
  PATTERN_FIELDS,
} = require('../services/marketIntelligenceQuery');

const requireAdmin = [requireAuth, requireRole('admin', 'manager')];

function noStore(res) {
  res.set('Cache-Control', 'no-store');
}

router.get('/api/v1/market-intel/companies', requireAdmin, async (req, res) => {
  try {
    const companies = await listMarketCompanies({
      q: req.query.q,
      limit: req.query.limit,
    });
    noStore(res);
    return res.json({ companies, internal: true });
  } catch (err) {
    console.error('[market-intel] list companies', err);
    return res.status(500).json({
      error: 'market_intel_list_failed',
      message: err && err.message ? String(err.message) : 'failed',
    });
  }
});

router.get('/api/v1/market-intel/companies/:id', requireAdmin, async (req, res) => {
  try {
    const profile = await getCompanyProfile(req.params.id);
    if (!profile) {
      return res.status(404).json({ error: 'company_not_found' });
    }
    noStore(res);
    return res.json({ profile, internal: true });
  } catch (err) {
    console.error('[market-intel] company profile', err);
    return res.status(500).json({
      error: 'market_intel_profile_failed',
      message: err && err.message ? String(err.message) : 'failed',
    });
  }
});

router.get('/api/v1/market-intel/companies/:id/timeline', requireAdmin, async (req, res) => {
  try {
    const timeline = await getCompanyCampaignTimeline(req.params.id, {
      limit: req.query.limit,
    });
    const diffField = String(req.query.diff || '').trim();
    const diffs = diffField ? diffTimelineField(timeline, diffField) : undefined;
    noStore(res);
    return res.json({
      companyId: req.params.id,
      timeline,
      diffs,
      internal: true,
    });
  } catch (err) {
    console.error('[market-intel] timeline', err);
    return res.status(500).json({
      error: 'market_intel_timeline_failed',
      message: err && err.message ? String(err.message) : 'failed',
    });
  }
});

router.get('/api/v1/market-intel/emails/:id', requireAdmin, async (req, res) => {
  try {
    const email = await getEmailEvidence(req.params.id);
    if (!email) {
      return res.status(404).json({ error: 'email_not_found' });
    }
    noStore(res);
    return res.json({ email, internal: true });
  } catch (err) {
    console.error('[market-intel] email evidence', err);
    return res.status(500).json({
      error: 'market_intel_email_failed',
      message: err && err.message ? String(err.message) : 'failed',
    });
  }
});

router.get('/api/v1/market-intel/cross-market/patterns', requireAdmin, async (req, res) => {
  try {
    const field = String(req.query.field || 'cta').trim();
    if (field && !PATTERN_FIELDS.has(field)) {
      return res.status(400).json({
        error: 'invalid_field',
        allowed: [...PATTERN_FIELDS],
      });
    }
    const result = await crossMarketPatterns({
      field,
      limit: req.query.limit,
    });
    noStore(res);
    return res.json({ ...result, internal: true });
  } catch (err) {
    console.error('[market-intel] patterns', err);
    return res.status(500).json({
      error: 'market_intel_patterns_failed',
      message: err && err.message ? String(err.message) : 'failed',
    });
  }
});

router.get('/api/v1/market-intel/cross-market/sequence-stats', requireAdmin, async (req, res) => {
  try {
    const stats = await crossMarketSequenceStats();
    noStore(res);
    return res.json({ stats, internal: true });
  } catch (err) {
    console.error('[market-intel] sequence stats', err);
    return res.status(500).json({
      error: 'market_intel_sequence_stats_failed',
      message: err && err.message ? String(err.message) : 'failed',
    });
  }
});

module.exports = router;
