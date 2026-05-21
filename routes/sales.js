const express = require('express');
const path = require('path');
const { requireAuth: sessionAuth, requireRole } = require('../middleware/auth');

const router = express.Router();

router.get('/', sessionAuth, requireRole('sales'), (_req, res) => {
  res.sendFile(path.join(__dirname, '..', 'public', 'sales-dashboard.html'));
});

module.exports = router;
