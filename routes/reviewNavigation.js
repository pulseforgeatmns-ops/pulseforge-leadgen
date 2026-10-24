'use strict';
const express = require('express');
const { requireAuth, requireRole } = require('../middleware/auth');
const { reviewNavigationForRequest } = require('../services/reviewNavigation');
function createReviewNavigationRouter({ db } = {}) {
  const router = express.Router();
  router.post('/api/v1/max/workspace/review-navigation', requireAuth, requireRole('admin', 'manager', 'ao'), async (req, res) => {
    res.set('Cache-Control', 'no-store');
    try {
      const result = await reviewNavigationForRequest(req, db ? { db } : {});
      if (!result) return res.json({ handled: false });
      return res.json(result);
    } catch (error) {
      return res.status(error.statusCode || 503).json({ error: 'review_navigation_unavailable', message: 'The authorized review scope could not be verified. Nothing was opened.' });
    }
  });
  return router;
}
module.exports = { createReviewNavigationRouter };
