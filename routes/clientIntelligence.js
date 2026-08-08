'use strict';

/**
 * SPEC-083 — Client Intelligence Engine APIs.
 * SPEC-084 — Interview experience (resume + understanding fields).
 * SPEC-085 — Executive Business Brief payload via executiveSummary.
 * SPEC-087 — Growth Infrastructure Readiness start/message.
 *
 * POST /api/v1/clients/:id/interview/start
 * POST /api/v1/interview/:id/message
 * POST /api/v1/interview/:id/resume
 * GET  /api/v1/interview/:id
 * GET  /api/v1/interview/:id/blueprint
 * POST /api/v1/blueprint/:id/revise
 * POST /api/v1/blueprint/:id/approve
 * POST /api/v1/interview/:id/growth/start
 * POST /api/v1/interview/:id/growth/message
 * POST /api/v1/interview/:id/readiness/start
 * POST /api/v1/interview/:id/readiness/message
 * POST /api/v1/interview/:id/readiness/dev/fixture  (dev/test only)
 * GET  /api/v1/clients/:id/blueprint
 * GET  /client-intel → UI
 */

const fs = require('fs');
const path = require('path');
const express = require('express');
const router = express.Router();
const { requireAuth, requireRole } = require('../middleware/auth');
const {
  ClientIntelligenceError,
  startClientInterview,
  postInterviewMessage,
  resumeInterview,
  getInterview,
  getInterviewBlueprint,
  getClientBlueprint,
  reviseBlueprint,
  approveBlueprint,
  startGrowthConversation,
  postGrowthMessage,
  startInfrastructureReadinessConversation,
  postInfrastructureReadinessMessage,
  applyInfrastructureReadinessFixture,
  isGrowthInfraDevFixturesEnabled,
  listGrowthInfraFixtures,
} = require('../services/clientIntelligenceInterview');

const requireOperator = [requireAuth, requireRole('admin', 'manager', 'client')];
const CLIENT_INTEL_HTML = path.join(__dirname, '..', 'public', 'client-intel.html');
const CIE_DEV_CONFIG_MARKER = '/*__CIE_DEV_CONFIG__*/';

function noStore(res) {
  res.set('Cache-Control', 'no-store');
}

function sendError(res, err) {
  if (err instanceof ClientIntelligenceError) {
    return res.status(err.status || 400).json({
      error: err.code,
      message: err.message,
    });
  }
  console.error('[client-intel]', err);
  return res.status(500).json({
    error: 'client_intel_failed',
    message: err && err.message ? String(err.message) : 'failed',
  });
}

function buildCieDevConfig() {
  const growthInfraFixtures = isGrowthInfraDevFixturesEnabled();
  return {
    growthInfraFixtures,
    fixtures: growthInfraFixtures ? listGrowthInfraFixtures() : [],
  };
}

router.get('/client-intel', requireOperator, (req, res) => {
  try {
    let html = fs.readFileSync(CLIENT_INTEL_HTML, 'utf8');
    const devConfig = buildCieDevConfig();
    const inject = `window.__CIE_DEV__ = ${JSON.stringify(devConfig)};`;
    if (html.includes(CIE_DEV_CONFIG_MARKER)) {
      html = html.replace(CIE_DEV_CONFIG_MARKER, inject);
    } else {
      html = html.replace('<script>', `<script>${inject}\n`);
    }
    // Production/client-facing: remove the shortcut markup entirely when gated off.
    if (!devConfig.growthInfraFixtures) {
      html = html.replace(
        /<div class="actions" id="readinessDevActions"[\s\S]*?<\/div>\s*/,
        ''
      );
    }
    noStore(res);
    res.type('html').send(html);
  } catch (err) {
    return sendError(res, err);
  }
});

router.post(
  '/api/v1/clients/:id/interview/start',
  requireOperator,
  async (req, res) => {
    try {
      const body = req.body || {};
      const result = await startClientInterview({
        clientId: req.params.id,
        notes: body.notes,
        source: body.source || 'api',
      });
      noStore(res);
      return res.status(201).json(result);
    } catch (err) {
      return sendError(res, err);
    }
  }
);

router.post('/api/v1/interview/:id/message', requireOperator, async (req, res) => {
  try {
    const message = req.body && req.body.message;
    if (message == null || String(message).trim() === '') {
      return res.status(400).json({
        error: 'empty_message',
        message: 'message is required',
      });
    }
    const result = await postInterviewMessage(req.params.id, message);
    noStore(res);
    return res.json(result);
  } catch (err) {
    return sendError(res, err);
  }
});

router.post('/api/v1/interview/:id/resume', requireOperator, async (req, res) => {
  try {
    const result = await resumeInterview(req.params.id);
    noStore(res);
    return res.json(result);
  } catch (err) {
    return sendError(res, err);
  }
});

router.get('/api/v1/interview/:id', requireOperator, async (req, res) => {
  try {
    const result = await getInterview(req.params.id);
    noStore(res);
    return res.json(result);
  } catch (err) {
    return sendError(res, err);
  }
});

router.get('/api/v1/interview/:id/blueprint', requireOperator, async (req, res) => {
  try {
    const result = await getInterviewBlueprint(req.params.id);
    noStore(res);
    return res.json(result);
  } catch (err) {
    return sendError(res, err);
  }
});

router.post('/api/v1/blueprint/:id/revise', requireOperator, async (req, res) => {
  try {
    const body = req.body || {};
    const result = await reviseBlueprint(req.params.id, body.sections || body);
    noStore(res);
    return res.json(result);
  } catch (err) {
    return sendError(res, err);
  }
});

router.post('/api/v1/blueprint/:id/approve', requireOperator, async (req, res) => {
  try {
    const result = await approveBlueprint(req.params.id);
    noStore(res);
    return res.json(result);
  } catch (err) {
    return sendError(res, err);
  }
});

router.post(
  '/api/v1/interview/:id/growth/start',
  requireOperator,
  async (req, res) => {
    try {
      const result = await startGrowthConversation(req.params.id);
      noStore(res);
      return res.json(result);
    } catch (err) {
      return sendError(res, err);
    }
  }
);

router.post(
  '/api/v1/interview/:id/growth/message',
  requireOperator,
  async (req, res) => {
    try {
      const message = req.body && req.body.message;
      if (message == null || String(message).trim() === '') {
        return res.status(400).json({
          error: 'empty_message',
          message: 'message is required',
        });
      }
      const result = await postGrowthMessage(req.params.id, message);
      noStore(res);
      return res.json(result);
    } catch (err) {
      return sendError(res, err);
    }
  }
);

router.post(
  '/api/v1/interview/:id/readiness/start',
  requireOperator,
  async (req, res) => {
    try {
      const result = await startInfrastructureReadinessConversation(req.params.id);
      noStore(res);
      return res.json(result);
    } catch (err) {
      return sendError(res, err);
    }
  }
);

router.post(
  '/api/v1/interview/:id/readiness/message',
  requireOperator,
  async (req, res) => {
    try {
      const message = req.body && req.body.message;
      if (message == null || String(message).trim() === '') {
        return res.status(400).json({
          error: 'empty_message',
          message: 'message is required',
        });
      }
      const result = await postInfrastructureReadinessMessage(req.params.id, message);
      noStore(res);
      return res.json(result);
    } catch (err) {
      return sendError(res, err);
    }
  }
);

/**
 * Dev/test only: apply sample fixture answers and generate the real readiness report.
 * Disabled in production unless CIE_GROWTH_INFRA_DEV_FIXTURES=1.
 */
router.post(
  '/api/v1/interview/:id/readiness/dev/fixture',
  requireOperator,
  async (req, res) => {
    try {
      if (!isGrowthInfraDevFixturesEnabled()) {
        return res.status(403).json({
          error: 'dev_fixtures_disabled',
          message:
            'Growth Infrastructure sample answers are disabled in this environment',
        });
      }
      const body = req.body || {};
      const fixtureId = body.fixture || body.fixtureId || 'anchor';
      const result = await applyInfrastructureReadinessFixture(
        req.params.id,
        fixtureId
      );
      noStore(res);
      return res.json(result);
    } catch (err) {
      return sendError(res, err);
    }
  }
);

router.get('/api/v1/clients/:id/blueprint', requireOperator, async (req, res) => {
  try {
    const result = await getClientBlueprint(req.params.id);
    noStore(res);
    return res.json(result);
  } catch (err) {
    return sendError(res, err);
  }
});

module.exports = router;
