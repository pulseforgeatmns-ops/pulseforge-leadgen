'use strict';

/**
 * SIGNAL-V1 operator + API surfaces (paper-only research engine).
 */

const express = require('express');
const path = require('path');
const { requireAuth, requireRole } = require('../middleware/auth');
const { SignalService } = require('../packages/signal-v1');
const { InMemorySignalStore } = require('../packages/signal-v1/storage/InMemorySignalStore');
const { createSignalStore } = require('../packages/signal-v1/storage/createSignalStore');
const { seedFrontRunnersFixtures } = require('../packages/signal-v1/fixtures/seedFixtures');
const { resolveResearchWindow } = require('../packages/signal-v1/fixtures/researchWindows');

const router = express.Router();
const requireResearch = [requireAuth, requireRole('admin', 'manager')];

/** @type {Promise<SignalService>|null} */
let servicePromise = null;

async function getService() {
  if (!servicePromise) {
    servicePromise = (async () => {
      if (process.env.DATABASE_URL) {
        const pool = require('../db');
        const store = await createSignalStore(pool, { seedFixtures: true });
        return new SignalService(store, { seedFixtures: false });
      }
      const store = new InMemorySignalStore();
      seedFrontRunnersFixtures(store);
      return new SignalService(store, { seedFixtures: false });
    })();
  }
  return servicePromise;
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

router.post('/api/v1/signal/tokens/:tokenAddress/ingest-history', requireResearch, async (req, res) => {
  const service = await getService();
  const body = req.body || {};
  try {
    const window = resolveResearchWindow(req.params.tokenAddress);
    const stats = await service.ingestHistory({
      tokenAddress: req.params.tokenAddress,
      startTime: body.startTime || window.startTime,
      endTime: body.endTime || window.endTime,
      resolutionSeconds: body.resolutionSeconds || window.resolutionSeconds,
    });
    noStore(res);
    return res.json(stats);
  } catch (err) {
    return res.status(400).json({ error: 'ingest_failed', message: String(err.message) });
  }
});

router.post('/api/v1/signal/tokens/:tokenAddress/replay', requireResearch, async (req, res) => {
  const service = await getService();
  const body = req.body || {};
  try {
    const result = await service.replay({
      tokenAddress: req.params.tokenAddress,
      startTime: body.startTime,
      endTime: body.endTime,
      featureVersion: body.featureVersion,
      strategyVersion: body.strategyVersion,
      pricePath: body.pricePath,
      executionDelaysSeconds: body.executionDelaysSeconds,
    });
    noStore(res);
    return res.json(result);
  } catch (err) {
    return res.status(400).json({ error: 'replay_failed', message: String(err.message) });
  }
});

module.exports = router;
