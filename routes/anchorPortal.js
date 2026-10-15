'use strict';

const express = require('express');
const path = require('path');
function createPortalRouter({ requireAuth, requireRole, service, canonicalOrigin }) {
const {
  AnchorPortalError,
  assertLocationAccess,
  isOperator,
  isCleaner,
  isFacilityClient,
  listLocationsForUser,
  getLocationScope,
  replaceLocationScope,
  getVisitWithItems,
  listVisits,
  getTodayVisitForCleaner,
  startVisit,
  updateVisitItem,
  addVisitEvidence,
  completeVisit,
  createVisit,
  assertPortalUser,
  listIssues,
  createIssue,
  updateIssue,
  getClientHome,
  getOperatorSummary,
} = service;

const router = express.Router();

const PORTAL_ROLES = ['admin', 'manager', 'cleaner', 'facility_client'];
function expectedOrigin(req) {
  if (canonicalOrigin) return canonicalOrigin;
  if (process.env.APP_URL) {
    try {
      const url = new URL(process.env.APP_URL);
      if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password) return null;
      return url.origin;
    } catch (_) { return null; }
  }
  // Development harness only. Production must configure its canonical APP_URL;
  // never trust caller-supplied X-Forwarded-Proto to authorize a mutation.
  const host = req.get('host');
  return /^(127\.0\.0\.1|localhost|\[::1\])(?::[0-9]+)?$/.test(host || '') ? `${req.protocol}://${host}` : null;
}

const portalAuth = [requireAuth, requireRole(...PORTAL_ROLES), (req, res, next) => {
  try {
    assertPortalUser(req.user);
    if (req.session?.impersonation?.active) throw new AnchorPortalError('forbidden', 'Exit impersonation before using the service portal.', 403);
    res.set('Cache-Control', 'no-store');
    if (!['GET', 'HEAD', 'OPTIONS'].includes(req.method)) {
      const origin = req.get('origin');
      if (origin && origin !== expectedOrigin(req)) throw new AnchorPortalError('forbidden', 'Use this portal to save changes.', 403);
      if (req.body && !req.is('application/json')) throw new AnchorPortalError('invalid_content_type', 'Send service changes as JSON.', 415);
    }
    next();
  } catch (err) { handleError(res, err); }
}];

function handleError(res, err) {
  if (err instanceof AnchorPortalError) {
    return res.status(err.status || 400).json({ error: err.code, message: err.message });
  }
  console.error('[anchor-portal]', err);
  return res.status(500).json({ error: 'internal_error', message: 'Something went wrong.' });
}

router.get('/portal/anchor', ...portalAuth, (_req, res) => {
  res.sendFile(path.join(__dirname, '..', 'public', 'anchor-portal.html'));
});

router.get('/api/anchor-portal/me', ...portalAuth, async (req, res) => {
  try {
    const locations = await listLocationsForUser(req.user);
    return res.json({
      user: {
        id: req.user.id,
        name: req.user.name,
        email: req.user.email,
        role: req.user.role,
      },
      capabilities: {
        operator: isOperator(req.user),
        cleaner: isCleaner(req.user),
        facility_client: isFacilityClient(req.user),
      },
      locations,
    });
  } catch (err) {
    return handleError(res, err);
  }
});

router.get('/api/anchor-portal/home', ...portalAuth, async (req, res) => {
  try {
    const locationId = req.query.location_id;
    if (!locationId) {
      const locations = await listLocationsForUser(req.user);
      if (!locations.length) {
        return res.json({ empty: true, locations: [] });
      }
      if (isOperator(req.user) && !isFacilityClient(req.user)) {
        return res.json({ redirect: 'operator', locations });
      }
      const home = await getClientHome(req.user, locations[0].id);
      return res.json({ ...home, locations });
    }
    const home = await getClientHome(req.user, locationId);
    return res.json(home);
  } catch (err) {
    return handleError(res, err);
  }
});

router.get('/api/anchor-portal/operator/summary', ...portalAuth, async (req, res) => {
  try {
    const summary = await getOperatorSummary(req.user);
    return res.json(summary);
  } catch (err) {
    return handleError(res, err);
  }
});

router.get('/api/anchor-portal/locations', ...portalAuth, async (req, res) => {
  try {
    const locations = await listLocationsForUser(req.user);
    return res.json({ locations });
  } catch (err) {
    return handleError(res, err);
  }
});

router.get('/api/anchor-portal/locations/:id/scope', ...portalAuth, async (req, res) => {
  try {
    const locationId = await assertLocationAccess(req.user, req.params.id);
    const scope = await getLocationScope(locationId);
    return res.json({ location_id: locationId, scope });
  } catch (err) {
    return handleError(res, err);
  }
});

router.put('/api/anchor-portal/locations/:id/scope', ...portalAuth, async (req, res) => {
  try {
    if (!isOperator(req.user)) {
      return res.status(403).json({ error: 'forbidden', message: 'Operators only.' });
    }
    const locationId = await assertLocationAccess(req.user, req.params.id);
    const scope = await replaceLocationScope(locationId, req.body?.sections);
    return res.json({ location_id: locationId, scope });
  } catch (err) {
    return handleError(res, err);
  }
});

router.get('/api/anchor-portal/visits', ...portalAuth, async (req, res) => {
  try {
    const visits = await listVisits(req.user, {
      locationId: req.query.location_id,
      limit: req.query.limit,
    });
    return res.json({ visits });
  } catch (err) {
    return handleError(res, err);
  }
});

router.post('/api/anchor-portal/visits', ...portalAuth, async (req, res) => {
  try { return res.status(201).json({ visit: await createVisit(req.user, req.body || {}) }); }
  catch (err) { return handleError(res, err); }
});

router.get('/api/anchor-portal/visits/today', ...portalAuth, async (req, res) => {
  try {
    const visit = await getTodayVisitForCleaner(req.user);
    return res.json({ visit });
  } catch (err) {
    return handleError(res, err);
  }
});

router.get('/api/anchor-portal/visits/:id', ...portalAuth, async (req, res) => {
  try {
    const visit = await getVisitWithItems(req.params.id);
    if (!visit) return res.status(404).json({ error: 'not_found', message: 'Visit not found.' });
    await assertLocationAccess(req.user, visit.location_id);
    return res.json({ visit });
  } catch (err) {
    return handleError(res, err);
  }
});

router.post('/api/anchor-portal/visits/:id/start', ...portalAuth, async (req, res) => {
  try {
    const visit = await startVisit(req.user, req.params.id);
    return res.json({ visit });
  } catch (err) {
    return handleError(res, err);
  }
});

router.patch('/api/anchor-portal/visits/:visitId/items/:itemId', ...portalAuth, async (req, res) => {
  try {
    const visit = await updateVisitItem(req.user, req.params.visitId, req.params.itemId, req.body || {});
    return res.json({ visit });
  } catch (err) {
    return handleError(res, err);
  }
});

router.post('/api/anchor-portal/visits/:id/evidence', ...portalAuth, async (req, res) => {
  try {
    const visit = await addVisitEvidence(req.user, req.params.id, req.body || {});
    return res.json({ visit });
  } catch (err) {
    return handleError(res, err);
  }
});

router.post('/api/anchor-portal/visits/:id/complete', ...portalAuth, async (req, res) => {
  try {
    const visit = await completeVisit(req.user, req.params.id, req.body || {});
    return res.json({ visit });
  } catch (err) {
    return handleError(res, err);
  }
});

router.get('/api/anchor-portal/issues', ...portalAuth, async (req, res) => {
  try {
    const issues = await listIssues(req.user, {
      locationId: req.query.location_id,
      status: req.query.status,
      offset: req.query.offset,
    });
    return res.json({ issues });
  } catch (err) {
    return handleError(res, err);
  }
});

router.post('/api/anchor-portal/issues', ...portalAuth, async (req, res) => {
  try {
    const issue = await createIssue(req.user, req.body || {});
    return res.status(201).json({ issue });
  } catch (err) {
    return handleError(res, err);
  }
});

router.patch('/api/anchor-portal/issues/:id', ...portalAuth, async (req, res) => {
  try {
    const issue = await updateIssue(req.user, req.params.id, req.body || {});
    return res.json({ issue });
  } catch (err) {
    return handleError(res, err);
  }
});

router.expectedOrigin = expectedOrigin;
return router;
}
let defaultRouter;
function defaults() { return defaultRouter ||= createPortalRouter({ ...require('../middleware/auth'), service: require('../services/anchorPortal') }); }
module.exports = (req, res, next) => defaults()(req, res, next);
module.exports.createPortalRouter = createPortalRouter;
module.exports.expectedOrigin = req => defaults().expectedOrigin(req);
