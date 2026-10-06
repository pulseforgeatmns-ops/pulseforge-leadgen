'use strict';

const pool = require('../db');
const { snapshotUser } = require('../utils/requestIdentity');
const { assertAuthorizedClientSwitch } = require('../utils/tenantAuthorization');
const { normalizeClientId } = require('../utils/clientContext');
const { logAoAuditEvent } = require('../utils/aoAuditEvents');

const IMPERSONATION_ROLES = new Set(['admin', 'manager']);

function publicImpersonationState(session, authenticatedUser) {
  const imp = session?.impersonation;
  if (!imp?.active) {
    return { active: false };
  }
  const effective = imp.effectiveUser || null;
  return {
    active: true,
    authenticated_user: snapshotUser(authenticatedUser),
    effective_user: snapshotUser(effective),
    tenant_id: imp.tenantId,
    started_at: imp.startedAt,
    started_by_role: imp.startedByRole,
  };
}

async function loadAoUser(userId, clientId, db = pool) {
  const { rows } = await db.query(`
    SELECT id, name, email, role, client_id, active
    FROM users
    WHERE id = $1
    LIMIT 1
  `, [userId]);
  const user = rows[0];
  if (!user || !user.active) return { error: 'user_not_found', status: 404 };
  if (user.role !== 'ao') return { error: 'target_not_ao', status: 403 };
  const tenantId = normalizeClientId(clientId ?? user.client_id);
  if (tenantId == null) return { error: 'client_id_required', status: 400 };
  if (Number(normalizeClientId(user.client_id)) !== Number(tenantId)) {
    return { error: 'cross_tenant_target', status: 403 };
  }
  return { user, tenantId };
}

async function listImpersonationTargets({ actor, clientId, db = pool }) {
  if (!IMPERSONATION_ROLES.has(actor?.role)) {
    return { error: 'forbidden', status: 403 };
  }
  const tenantId = normalizeClientId(clientId);
  if (tenantId == null) return { error: 'client_id_required', status: 400 };
  const auth = assertAuthorizedClientSwitch(actor, tenantId);
  if (!auth.ok) return { error: auth.error, status: auth.status, message: auth.message };

  const { rows } = await db.query(`
    SELECT id, name, email, role, client_id, active
    FROM users
    WHERE role = 'ao'
      AND active = true
      AND client_id = $1
    ORDER BY name ASC, id ASC
  `, [tenantId]);

  return {
    ok: true,
    client_id: tenantId,
    targets: rows.map(r => ({
      id: r.id,
      name: r.name,
      email: r.email,
      client_id: r.client_id,
    })),
  };
}

async function logImpersonationAudit(event, payload, db = pool) {
  const clientId = payload.tenant_id ?? payload.clientId ?? null;
  await logAoAuditEvent({
    event,
    clientId: clientId || 1,
    aoUserId: payload.effective_user_id || payload.authenticated_user_id || null,
    payload: {
      ...payload,
      correlation_id: payload.correlation_id || null,
    },
    db,
  });
}

async function startImpersonation({ actor, userId, clientId, session, db = pool }) {
  if (!IMPERSONATION_ROLES.has(actor?.role)) {
    return { error: 'forbidden', status: 403 };
  }
  const loaded = await loadAoUser(userId, clientId, db);
  if (loaded.error) return loaded;

  const auth = assertAuthorizedClientSwitch(actor, loaded.tenantId);
  if (!auth.ok) return { error: auth.error, status: auth.status, message: auth.message };

  const effectiveSnapshot = snapshotUser(loaded.user);
  const startedAt = new Date().toISOString();
  session.impersonation = {
    active: true,
    authenticatedUserId: actor.id,
    effectiveUserId: loaded.user.id,
    tenantId: loaded.tenantId,
    startedAt,
    startedByRole: actor.role,
    effectiveUser: effectiveSnapshot,
  };
  session.active_client_id = loaded.tenantId;

  await logImpersonationAudit('ao_impersonation.started', {
    authenticated_user_id: actor.id,
    effective_user_id: loaded.user.id,
    tenant_id: loaded.tenantId,
    started_at: startedAt,
    started_by_role: actor.role,
  }, db);

  return {
    ok: true,
    impersonation: publicImpersonationState(session, actor),
  };
}

async function stopImpersonation({ actor, session, db = pool }) {
  const imp = session?.impersonation;
  if (!imp?.active) {
    return { ok: true, impersonation: { active: false } };
  }

  const endedAt = new Date().toISOString();
  const startedAt = imp.startedAt ? Date.parse(imp.startedAt) : null;
  const durationMs = startedAt ? Math.max(0, Date.parse(endedAt) - startedAt) : null;

  await logImpersonationAudit('ao_impersonation.ended', {
    authenticated_user_id: imp.authenticatedUserId ?? actor?.id,
    effective_user_id: imp.effectiveUserId,
    tenant_id: imp.tenantId,
    started_at: imp.startedAt,
    ended_at: endedAt,
    duration_ms: durationMs,
  }, db);

  delete session.impersonation;

  return {
    ok: true,
    impersonation: { active: false },
  };
}

async function logImpersonationAction(req, { route, action, extra = {} } = {}) {
  const imp = req.session?.impersonation;
  if (!imp?.active) return;
  const auth = req.authenticatedUser || req.session?.user;
  await logImpersonationAudit('ao_impersonation.action', {
    authenticated_user_id: auth?.id,
    effective_user_id: imp.effectiveUserId,
    tenant_id: imp.tenantId,
    route: route || req.originalUrl || req.path,
    action: action || null,
    ...extra,
  });
}

function validateActiveImpersonation(session, db) {
  const imp = session?.impersonation;
  if (!imp?.active) return Promise.resolve(null);
  return db.query(`
    SELECT id, name, email, role, client_id, active
    FROM users WHERE id = $1 LIMIT 1
  `, [imp.effectiveUserId]).then(({ rows }) => {
    const user = rows[0];
    if (!user || !user.active || user.role !== 'ao') {
      delete session.impersonation;
      return { cleared: true, reason: 'invalid_target' };
    }
    if (Number(user.client_id) !== Number(imp.tenantId)) {
      delete session.impersonation;
      return { cleared: true, reason: 'tenant_mismatch' };
    }
    session.impersonation.effectiveUser = snapshotUser(user);
    return null;
  }).catch(() => {
    delete session.impersonation;
    return { cleared: true, reason: 'validation_error' };
  });
}

module.exports = {
  IMPERSONATION_ROLES,
  publicImpersonationState,
  listImpersonationTargets,
  startImpersonation,
  stopImpersonation,
  logImpersonationAction,
  validateActiveImpersonation,
  loadAoUser,
};
