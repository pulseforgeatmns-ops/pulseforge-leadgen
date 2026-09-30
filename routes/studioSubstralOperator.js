'use strict';

const express = require('express');
const pool = require('../db');
const { requireAuth, requireRole } = require('../middleware/auth');
const { getRequestClientId } = require('../utils/clientContext');
const { findStudioSubstralClient } = require('../utils/studioSubstralTenant');
const { buildStudioSubstralOperatorSnapshot } = require('../services/studioSubstralOperatorSnapshot');

const router = express.Router();

router.get(
  '/api/studio-substral/operator-snapshot',
  requireAuth,
  requireRole('admin', 'manager', 'viewer'),
  async (req, res) => {
    try {
      const clientId = getRequestClientId(req);
      const substral = await findStudioSubstralClient(pool);
      if (!substral) {
        return res.status(404).json({ error: 'Studio Substral tenant is not provisioned.' });
      }
      if (Number(clientId) !== Number(substral.id)) {
        return res.status(403).json({ error: 'Switch to the Studio Substral tenant to view this workspace.' });
      }
      const snapshot = await buildStudioSubstralOperatorSnapshot(pool, clientId);
      return res.json(snapshot);
    } catch (error) {
      const status = error.status || 500;
      return res.status(status).json({ error: error.message || 'Could not load Studio Substral operator snapshot.' });
    }
  }
);

module.exports = router;
