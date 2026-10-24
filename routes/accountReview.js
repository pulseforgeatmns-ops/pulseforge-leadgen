'use strict';
const express = require('express');
const { requireAuth, requireRole } = require('../middleware/auth');
const { resolveSpreadsheetScope } = require('../utils/maxSpreadsheetAuthorization');

function createAccountReviewRouter(deps = {}) {
  const router = express.Router();
  const access = [requireAuth, requireRole('admin', 'manager', 'ao')];
  const db = deps.db || require('../db');
  const scopeFor = deps.resolveScope || resolveSpreadsheetScope;
  function failure(res, error) {
    const status = error.statusCode || (/^[0-9A-Z]{5}$/.test(error.code || '') ? 503 : 500);
    return res.status(status).json({ ok: false, error: 'account_review_unavailable',
      message: status === 403 ? 'You cannot review accounts in this workspace or for this operator.' : 'Assigned accounts could not be loaded. Please try again.' });
  }
  router.get('/api/v1/max/account-review', ...access, async (req, res) => {
    res.set('Cache-Control', 'no-store');
    try {
      const scope = await scopeFor(req, db);
      const listAccounts = deps.listAccounts || require('../services/aoCrmService').listManagerAccounts;
      const result = await listAccounts({ clientId: scope.clientId, filters: { ao_owner_id: scope.aoId }, db });
      if (result.accounts.some(account => String(account.assigned_ao_id) !== String(scope.aoId))) throw new Error('account_scope_mismatch');
      return res.json({ ok: true, tenant_id: scope.clientId, ao_id: scope.aoId,
        ao_name: result.accounts[0]?.assigned_ao_name || null, date: result.date,
        accounts: result.accounts, summary: result.summary });
    } catch (error) { return failure(res, error); }
  });
  router.get('/api/v1/max/account-review/:prospectId', ...access, async (req, res) => {
    res.set('Cache-Control', 'no-store');
    try {
      const scope = await scopeFor(req, db);
      const getAccount = deps.getAccount || require('../services/aoCrmService').getAccountDetail;
      const account = await getAccount({ clientId: scope.clientId, aoUserId: scope.aoId, prospectId: req.params.prospectId, db });
      if (!account || String(account.summary?.assigned_ao_id) !== String(scope.aoId)) return res.status(404).json({ ok: false, error: 'account_not_found', message: 'This account is no longer assigned to this operator.' });
      return res.json({ ok: true, tenant_id: scope.clientId, ao_id: scope.aoId, account });
    } catch (error) { return failure(res, error); }
  });
  return router;
}

module.exports = { createAccountReviewRouter };
