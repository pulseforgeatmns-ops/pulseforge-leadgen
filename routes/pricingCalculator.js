'use strict';

const express = require('express');
const path = require('path');
const { requireAuth, requireRole } = require('../middleware/auth');

const router = express.Router();
const requirePricingAccess = [
  requireAuth,
  requireRole('admin', 'manager', 'sales', 'closer'),
];

router.get('/pricing-calculator', requirePricingAccess, (_req, res) => {
  res.sendFile(path.join(__dirname, '..', 'public', 'pricing-calculator.html'));
});

module.exports = router;
