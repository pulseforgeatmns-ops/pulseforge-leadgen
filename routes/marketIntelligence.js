'use strict';

/**
 * SPEC-065 — GET-only Market Intelligence query APIs.
 * SPEC-067 — Operational readiness (GET /api/v1/market-intel/readiness).
 * SPEC-071 — Briefing / query surfaces (GET /api/v1/market-intel/briefing, …).
 * Observational corpus. No recommendations, scoring, or Max writes.
 *
 * GET /api/v1/market-intel/readiness
 * GET /api/v1/market-intel/briefing
 * GET /api/v1/market-intel/offers
 * GET /api/v1/market-intel/ctas
 * GET /api/v1/market-intel/companies/cadence
 * GET /api/v1/market-intel/themes
 * GET /api/v1/market-intel/changes
 * GET /api/v1/market-intel/import-intents
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
const { buildMarketIntelReadinessReport } = require('../services/marketIntelligenceReadiness');
const {
  getCompanyCadence,
  getMarketIntelligenceBriefing,
  getMessagingThemes,
  getObservationsByIntent,
  getRecentMessagingChanges,
  getTopCtas,
  getTopOffers,
} = require('../services/marketIntelligenceBriefing');

const requireAdmin = [requireAuth, requireRole('admin', 'manager')];

function noStore(res) {
  res.set('Cache-Control', 'no-store');
}

function briefingQueryOptions(query = {}) {
  return {
    days: query.days,
    limit: query.limit,
    importIntent: query.intent || query.importIntent || undefined,
    companyId: query.companyId || undefined,
    category: query.category || undefined,
    since: query.since || undefined,
    until: query.until || undefined,
  };
}

router.get('/api/v1/market-intel/readiness', requireAdmin, async (req, res) => {
  try {
    const report = await buildMarketIntelReadinessReport();
    noStore(res);
    return res.json(report);
  } catch (err) {
    console.error('[market-intel] readiness', err);
    return res.status(500).json({
      error: 'market_intel_readiness_failed',
      message: err && err.message ? String(err.message) : 'failed',
    });
  }
});

router.get('/api/v1/market-intel/briefing', requireAdmin, async (req, res) => {
  try {
    const briefing = await getMarketIntelligenceBriefing(briefingQueryOptions(req.query));
    noStore(res);
    return res.json(briefing);
  } catch (err) {
    console.error('[market-intel] briefing', err);
    return res.status(500).json({
      error: 'market_intel_briefing_failed',
      message: err && err.message ? String(err.message) : 'failed',
    });
  }
});

router.get('/api/v1/market-intel/offers', requireAdmin, async (req, res) => {
  try {
    const offers = await getTopOffers(briefingQueryOptions(req.query));
    noStore(res);
    return res.json({
      kind: 'market_intelligence_offers',
      isEvidence: false,
      offers,
      internal: true,
      observationalOnly: true,
    });
  } catch (err) {
    console.error('[market-intel] offers', err);
    return res.status(500).json({
      error: 'market_intel_offers_failed',
      message: err && err.message ? String(err.message) : 'failed',
    });
  }
});

router.get('/api/v1/market-intel/ctas', requireAdmin, async (req, res) => {
  try {
    const ctas = await getTopCtas(briefingQueryOptions(req.query));
    noStore(res);
    return res.json({
      kind: 'market_intelligence_ctas',
      isEvidence: false,
      ctas,
      internal: true,
      observationalOnly: true,
    });
  } catch (err) {
    console.error('[market-intel] ctas', err);
    return res.status(500).json({
      error: 'market_intel_ctas_failed',
      message: err && err.message ? String(err.message) : 'failed',
    });
  }
});

// Must be registered before /companies/:id so "cadence" is not treated as an id.
router.get('/api/v1/market-intel/companies/cadence', requireAdmin, async (req, res) => {
  try {
    const companies = await getCompanyCadence(briefingQueryOptions(req.query));
    noStore(res);
    return res.json({
      kind: 'market_intelligence_company_cadence',
      isEvidence: false,
      companies,
      internal: true,
      observationalOnly: true,
    });
  } catch (err) {
    console.error('[market-intel] company cadence', err);
    return res.status(500).json({
      error: 'market_intel_company_cadence_failed',
      message: err && err.message ? String(err.message) : 'failed',
    });
  }
});

router.get('/api/v1/market-intel/themes', requireAdmin, async (req, res) => {
  try {
    const result = await getMessagingThemes(briefingQueryOptions(req.query));
    noStore(res);
    return res.json({
      kind: 'market_intelligence_themes',
      isEvidence: false,
      themes: result.items,
      internal: true,
      observationalOnly: true,
    });
  } catch (err) {
    console.error('[market-intel] themes', err);
    return res.status(500).json({
      error: 'market_intel_themes_failed',
      message: err && err.message ? String(err.message) : 'failed',
    });
  }
});

router.get('/api/v1/market-intel/changes', requireAdmin, async (req, res) => {
  try {
    const result = await getRecentMessagingChanges(briefingQueryOptions(req.query));
    noStore(res);
    return res.json({
      kind: 'market_intelligence_changes',
      isEvidence: false,
      changes: result.items,
      caveats: result.caveats || [],
      internal: true,
      observationalOnly: true,
    });
  } catch (err) {
    console.error('[market-intel] changes', err);
    return res.status(500).json({
      error: 'market_intel_changes_failed',
      message: err && err.message ? String(err.message) : 'failed',
    });
  }
});

router.get('/api/v1/market-intel/import-intents', requireAdmin, async (req, res) => {
  try {
    const importIntents = await getObservationsByIntent(briefingQueryOptions(req.query));
    noStore(res);
    return res.json({
      kind: 'market_intelligence_import_intents',
      isEvidence: false,
      importIntents,
      internal: true,
      observationalOnly: true,
    });
  } catch (err) {
    console.error('[market-intel] import intents', err);
    return res.status(500).json({
      error: 'market_intel_import_intents_failed',
      message: err && err.message ? String(err.message) : 'failed',
    });
  }
});

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
