'use strict';

/**
 * SIGNAL-V1 operator + API surfaces (paper-only research engine).
 */

const express = require('express');
const path = require('path');
const { requireAuth, requireRole } = require('../middleware/auth');
const { SignalService, createShadowModeServiceFromStore } = require('../packages/signal-v1');
const { InMemorySignalStore } = require('../packages/signal-v1/storage/InMemorySignalStore');
const { createSignalStore } = require('../packages/signal-v1/storage/createSignalStore');
const { GeckoTerminalMarketDataProvider } = require('../packages/signal-v1/providers/GeckoTerminalMarketDataProvider');
const { runShadowSchedulerTick } = require('../packages/signal-v1/prospective/shadowScheduler');
const { seedFrontRunnersFixtures } = require('../packages/signal-v1/fixtures/seedFixtures');
const { resolveResearchWindow } = require('../packages/signal-v1/fixtures/researchWindows');
const { HISTORICAL_DATA_UNAVAILABLE } = require('../packages/signal-v1/ingestion/ingestHistoricalMarketData');

const router = express.Router();
const requireResearch = [requireAuth, requireRole('admin', 'manager')];

/** @type {Promise<SignalService>|null} */
let servicePromise = null;
/** @type {Promise<import('../packages/signal-v1/prospective/ShadowModeService').ShadowModeService>|null} */
let shadowPromise = null;

async function getStore() {
  if (process.env.DATABASE_URL) {
    const pool = require('../db');
    return createSignalStore(pool, { seedFixtures: true });
  }
  const store = new InMemorySignalStore();
  seedFrontRunnersFixtures(store);
  return store;
}

async function getService() {
  if (!servicePromise) {
    servicePromise = (async () => {
      const store = await getStore();
      return new SignalService(store, { seedFixtures: false });
    })();
  }
  return servicePromise;
}

async function getShadowService() {
  if (!shadowPromise) {
    shadowPromise = (async () => {
      const store = await getStore();
      const marketProvider = new GeckoTerminalMarketDataProvider();
      return createShadowModeServiceFromStore(store, {
        marketProvider,
        providerVersions: { market: marketProvider.providerId },
      });
    })();
  }
  return shadowPromise;
}

function noStore(res) {
  res.set('Cache-Control', 'no-store');
}

router.get('/signal-v1', requireResearch, (req, res) => {
  res.sendFile(path.join(__dirname, '../public/signal-v1.html'));
});

router.get('/api/v1/signal/research-cases', requireResearch, async (req, res) => {
  const service = await getService();
  noStore(res);
  return res.json({ cases: service.listResearchCases() });
});

router.get('/api/v1/signal/tokens/:tokenAddress', requireResearch, async (req, res) => {
  const service = await getService();
  const tokenAddress = req.params.tokenAddress;
  const token = await service.getToken(tokenAddress);
  if (!token) {
    return res.status(404).json({ error: 'token_not_found' });
  }
  const latest = await service.getLatestDecision(tokenAddress);
  const position = await service.getOpenPaperPosition(tokenAddress);
  noStore(res);
  return res.json({ token, decision: latest, paperPosition: position });
});

router.get('/api/v1/signal/tokens/:tokenAddress/timeline', requireResearch, async (req, res) => {
  const service = await getService();
  const { startTime, endTime } = req.query;
  const events = service.getTokenTimeline(req.params.tokenAddress, startTime, endTime);
  const resolved = events && typeof events.then === 'function' ? await events : events;
  noStore(res);
  return res.json({ events: resolved });
});

router.get('/api/v1/signal/tokens/:tokenAddress/market-history', requireResearch, async (req, res) => {
  const service = await getService();
  const { startTime, endTime } = req.query;
  const data = await service.getMarketHistory(req.params.tokenAddress, startTime, endTime);
  noStore(res);
  return res.json(data);
});

router.get('/api/v1/signal/tokens/:tokenAddress/outcomes', requireResearch, async (req, res) => {
  const service = await getService();
  const outcomes = await service.getOutcomes(req.params.tokenAddress);
  noStore(res);
  return res.json({ outcomes });
});

router.get('/api/v1/signal/tokens/:tokenAddress/replay-detail', requireResearch, async (req, res) => {
  const service = await getService();
  const detail = await service.getReplayDetail(req.params.tokenAddress);
  noStore(res);
  return res.json(detail);
});

router.post('/api/v1/signal/tokens/:tokenAddress/evaluate', requireResearch, async (req, res) => {
  const service = await getService();
  const evaluatedAt = req.body?.evaluatedAt || new Date().toISOString();
  try {
    const result = await service.evaluateAt(req.params.tokenAddress, evaluatedAt);
    noStore(res);
    return res.json(result);
  } catch (err) {
    return res.status(400).json({ error: 'evaluate_failed', message: String(err.message) });
  }
});

router.get('/api/v1/signal/research/cohorts', requireResearch, async (req, res) => {
  const service = await getService();
  noStore(res);
  const cohorts = service.listResearchCohorts();
  const resolved = cohorts && typeof cohorts.then === 'function' ? await cohorts : cohorts;
  return res.json({ cohorts: resolved });
});

router.get('/api/v1/signal/research/cohorts/:cohortId', requireResearch, async (req, res) => {
  const service = await getService();
  const cohort = await service.getResearchCohort(req.params.cohortId);
  if (!cohort) return res.status(404).json({ error: 'cohort_not_found' });
  noStore(res);
  return res.json({ cohort });
});

router.get('/api/v1/signal/research/cohorts/:cohortId/evaluation', requireResearch, async (req, res) => {
  const cohortId = req.params.cohortId;
  const delay = Number(req.query.executionDelaySeconds || 60);
  try {
    if (cohortId === 'cohort-signal-v1-prospective-001') {
      const shadow = await getShadowService();
      const evaluation = await shadow.evaluateProspectiveCohort(delay, {
        skipEmpiricalGuard: req.query.skipGuard === 'true',
      });
      noStore(res);
      return res.json(evaluation);
    }
    const service = await getService();
    const evaluation = await service.evaluateResearchCohort(cohortId, delay, {
      replayMembers: req.query.replayMembers !== 'false',
    });
    noStore(res);
    return res.json(evaluation);
  } catch (err) {
    const status = err.name === 'EmpiricalValidationError' ? 422 : 400;
    return res.status(status).json({
      error: 'evaluation_failed',
      message: String(err.message),
      details: err.details || null,
    });
  }
});

router.get('/api/v1/signal/research/cohorts/:cohortId/evaluation/export', requireResearch, async (req, res) => {
  const service = await getService();
  const delay = Number(req.query.executionDelaySeconds || 60);
  try {
    const artifact = await service.exportResearchCohortEvaluation(req.params.cohortId, {
      executionDelaySeconds: delay,
      replayMembers: false,
    });
    noStore(res);
    return res.json(artifact);
  } catch (err) {
    const status = err.name === 'EmpiricalValidationError' ? 422 : 400;
    return res.status(status).json({
      error: 'export_failed',
      message: String(err.message),
      details: err.details || null,
    });
  }
});

router.get('/api/v1/signal/tokens/:tokenAddress/research-observations', requireResearch, async (req, res) => {
  const service = await getService();
  const observations = service.getTokenResearchObservations(req.params.tokenAddress);
  noStore(res);
  return res.json({ observations });
});

router.get('/api/v1/signal/shadow/dashboard', requireResearch, async (req, res) => {
  const shadow = await getShadowService();
  noStore(res);
  return res.json(shadow.getShadowDashboard());
});

router.get('/api/v1/signal/shadow/sources', requireResearch, async (req, res) => {
  const shadow = await getShadowService();
  noStore(res);
  return res.json({ sources: shadow.listSourceRegistry() });
});

router.post('/api/v1/signal/shadow/unblind', requireResearch, async (req, res) => {
  const shadow = await getShadowService();
  try {
    const cohort = await shadow.explicitUnblind({
      unblindedBy: req.session?.user?.email || 'operator',
      evaluationVersion: req.body?.evaluationVersion || 'prospective-001-v1',
    });
    noStore(res);
    return res.json({ cohort });
  } catch (err) {
    return res.status(400).json({ error: 'unblind_failed', message: String(err.message) });
  }
});

router.post('/api/v1/signal/shadow/tick', requireResearch, async (req, res) => {
  const shadow = await getShadowService();
  const result = await runShadowSchedulerTick(shadow, req.body || {});
  noStore(res);
  return res.json(result);
});

router.post('/api/v1/signal/tokens/:tokenAddress/replay', requireResearch, async (req, res) => {
  const service = await getService();
  const body = req.body || {};
  try {
    const window = resolveResearchWindow(req.params.tokenAddress);
    const result = await service.replay({
      tokenAddress: req.params.tokenAddress,
      startTime: body.startTime || window.startTime,
      endTime: body.endTime || window.endTime,
      decisionAnchor: window.anchor,
      featureVersion: body.featureVersion,
      strategyVersion: body.strategyVersion,
      pricePath: body.pricePath,
      executionDelaysSeconds: body.executionDelaysSeconds,
    });
    noStore(res);
    if (result.skipped && result.replayStatus === HISTORICAL_DATA_UNAVAILABLE) {
      return res.status(422).json({
        ...result.unavailablePayload,
        replayStatus: result.replayStatus,
        coverage: result.coverage,
      });
    }
    return res.json(result);
  } catch (err) {
    return res.status(400).json({ error: 'replay_failed', message: String(err.message) });
  }
});

module.exports = router;
