'use strict';

/**
 * SPEC-251 — Typed operator judgment commit API.
 *
 * POST /api/v1/operator-judgments/commit
 */

const express = require('express');
const router = express.Router();
const pool = require('../db');
const { requireAuth, requireRole } = require('../middleware/auth');
const {
  commitTypedOperatorJudgment,
  OperatorJudgmentCommitError,
} = require('../services/operatorJudgmentCommit');
const { inspectMission } = require('../services/acquisitionMission');

const requireActor = [requireAuth, requireRole('admin', 'manager', 'client')];

function noStore(res) {
  res.set('Cache-Control', 'no-store');
}

function fail(res, err, fallbackCode = 'operator_judgment_commit_failed', fallbackStatus = 500) {
  const code = (err && err.code) || fallbackCode;
  const status = (err && err.status) || fallbackStatus;
  return res.status(status).json({
    error: code,
    message: String((err && err.message) || err),
  });
}

router.post('/api/v1/operator-judgments/commit', requireActor, async (req, res) => {
  try {
    if (typeof req.body === 'string') {
      return fail(res, new OperatorJudgmentCommitError(
        'JUDGMENT_BODY_INVALID',
        'request body must be a structured judgment object'
      ));
    }
    const result = await commitTypedOperatorJudgment(
      { req, body: req.body, pool },
      { inspectMission, pool }
    );
    noStore(res);
    return res.status(result.replayed ? 200 : 201).json(result);
  } catch (err) {
    console.error('[operator-judgments] commit', err);
    if (err instanceof OperatorJudgmentCommitError) {
      return fail(res, err);
    }
    return fail(res, err);
  }
});

module.exports = router;
