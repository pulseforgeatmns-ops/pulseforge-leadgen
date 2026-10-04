'use strict';

/**
 * SIGNAL-V1 operator + API surfaces (paper-only research engine).
 */

const express = require('express');
const path = require('path');
const { requireAuth, requireRole } = require('../middleware/auth');
const { SignalService } = require('../packages/signal-v1');

const router = express.Router();
const requireResearch = [requireAuth, requireRole('admin', 'manager')];

const service = new SignalService();

function noStore(res) {
  res.set('Cache-Control', 'no-store');
}

router.get('/signal-v1', requireResearch, (req, res) => {
  res.sendFile(path.join(__dirname, '../public/signal-v1.html'));
});

router.get('/api/v1/signal/research-cases', requireResearch, (req, res) => {
  noStore(res);
  return res.json({ cases: service.listResearchCases() });
});

router.get('/api/v1/signal/tokens/:tokenAddress', requireResearch, (req, res) => {
  const tokenAddress = req.params.tokenAddress;
  const token = service.getToken(tokenAddress);
  if (!token) {
    return res.status(404).json({ error: 'token_not_found' });
  }
  const latest = service.getLatestDecision(tokenAddress);
  const position = service.getOpenPaperPosition(tokenAddress);
  noStore(res);
  return res.json({ token, decision: latest, paperPosition: position });
});

router.get('/api/v1/signal/tokens/:tokenAddress/timeline', requireResearch, (req, res) => {
  const { startTime, endTime } = req.query;
  const events = service.getTokenTimeline(req.params.tokenAddress, startTime, endTime);
  noStore(res);
  return res.json({ events });
});

router.post('/api/v1/signal/tokens/:tokenAddress/evaluate', requireResearch, (req, res) => {
  const evaluatedAt = req.body?.evaluatedAt || new Date().toISOString();
  try {
    const result = service.evaluateAt(req.params.tokenAddress, evaluatedAt);
    noStore(res);
    return res.json(result);
  } catch (err) {
    return res.status(400).json({ error: 'evaluate_failed', message: String(err.message) });
  }
});

router.get('/api/v1/signal/research/cohorts', requireResearch, (req, res) => {
  noStore(res);
  return res.json({ cohorts: service.listResearchCohorts() });
});

router.get('/api/v1/signal/research/cohorts/:cohortId', requireResearch, (req, res) => {
  const cohort = service.getResearchCohort(req.params.cohortId);
  if (!cohort) return res.status(404).json({ error: 'cohort_not_found' });
  noStore(res);
  return res.json({ cohort });
});

router.get('/api/v1/signal/research/cohorts/:cohortId/evaluation', requireResearch, (req, res) => {
  const delay = Number(req.query.executionDelaySeconds || 60);
  try {
    const evaluation = service.evaluateResearchCohort(req.params.cohortId, delay);
    noStore(res);
    return res.json(evaluation);
  } catch (err) {
    return res.status(400).json({ error: 'evaluation_failed', message: String(err.message) });
  }
});

router.get('/api/v1/signal/tokens/:tokenAddress/research-observations', requireResearch, (req, res) => {
  const observations = service.getTokenResearchObservations(req.params.tokenAddress);
  noStore(res);
  return res.json({ observations });
});

router.post('/api/v1/signal/tokens/:tokenAddress/replay', requireResearch, (req, res) => {
  const body = req.body || {};
  try {
    const result = service.replay({
      tokenAddress: req.params.tokenAddress,
      startTime: body.startTime,
      endTime: body.endTime,
      featureVersion: body.featureVersion,
      strategyVersion: body.strategyVersion,
      pricePath: body.pricePath,
    });
    noStore(res);
    return res.json(result);
  } catch (err) {
    return res.status(400).json({ error: 'replay_failed', message: String(err.message) });
  }
});

module.exports = router;
