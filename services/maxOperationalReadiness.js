'use strict';

const { inspectShadowSchema } = require('../packages/decision-service/shadowSchema');
const { observerHealthSnapshot } = require('../packages/max/observability/ObserverHealth');
const { loadRecoveryState } = require('./anchorEmailRecovery');

async function tableCatalog(db) {
  const { rows } = await db.query(`SELECT
    to_regclass('public.decision_shadow_events') IS NOT NULL AS decision_shadow_events,
    to_regclass('public.decision_shadow_evidence') IS NOT NULL AS decision_shadow_evidence,
    to_regclass('public.max_interpretation_shadow') IS NOT NULL AS max_interpretation_shadow,
    to_regclass('public.max_conversation_context') IS NOT NULL AS max_conversation_context,
    to_regclass('public.anchor_email_recovery_control') IS NOT NULL AS anchor_email_recovery_control,
    to_regclass('public.acquisition_outbound_programs') IS NOT NULL AS acquisition_outbound_programs`);
  return rows[0] || {};
}

async function metric(db, present, sql) {
  if (!present) return { rows: 0, lastSuccessfulPersistenceAt: null };
  try {
    const { rows } = await db.query(sql);
    return { rows: rows[0]?.n || 0, lastSuccessfulPersistenceAt: rows[0]?.last_at || null };
  } catch (_) {
    return { rows: null, lastSuccessfulPersistenceAt: null, queryHealthy: false };
  }
}

function enabled(env, key) { return env[key] === 'true'; }

function authenticatedRecoveryCapability() {
  const { WorkspaceEngine } = require('../packages/max/workspace/WorkspaceEngine');
  return {
    implementedInDeployedCode: typeof WorkspaceEngine?.prototype?.recoverConversationSession === 'function',
    scope: 'deployed_code',
    configurationIndependent: true,
  };
}

function outboundGateRead(state, recoveryControl) {
  const stateVerified = state && state.reason !== 'recovery_state_unavailable'
    && state.reason !== 'recovery_catalog_unavailable';
  const persistedVerified = recoveryControl?.verified === true;
  return {
    verified: Boolean(stateVerified && persistedVerified),
    status: stateVerified && persistedVerified ? 'verified' : 'unknown',
    mode: stateVerified ? state.mode : null,
    reason: stateVerified ? (state.reason || null) : 'gate_state_unverified',
    recoveryKillSwitchEffective: stateVerified
      ? state.mode === 'maintenance' && state.reason === 'recovery_kill_switch'
      : null,
    persistedMaintenanceArmed: persistedVerified ? recoveryControl.value : null,
  };
}

async function getMaxOperationalReadiness({ db, env = process.env } = {}) {
  if (!db?.query) throw new Error('readiness_database_required');
  const catalog = await tableCatalog(db);
  let jevSchema;
  try { jevSchema = await inspectShadowSchema(db); }
  catch (_) { jevSchema = { ready: false, missing: null, queryHealthy: false }; }
  const [jevEvents, jevEvidence, semantic, continuity, outbound, recoveryControl, outboundProgram] = await Promise.all([
    metric(db, catalog.decision_shadow_events, 'SELECT COUNT(*)::int AS n, MAX(timestamp) AS last_at FROM decision_shadow_events'),
    metric(db, catalog.decision_shadow_evidence, 'SELECT COUNT(*)::int AS n, MAX(timestamp) AS last_at FROM decision_shadow_evidence'),
    metric(db, catalog.max_interpretation_shadow, 'SELECT COUNT(*)::int AS n, MAX(observed_at) AS last_at FROM max_interpretation_shadow'),
    metric(db, catalog.max_conversation_context, 'SELECT COUNT(*) FILTER (WHERE expires_at > NOW())::int AS n, MAX(expires_at) AS last_at FROM max_conversation_context'),
    loadRecoveryState(db, { env }),
    catalog.anchor_email_recovery_control
      ? db.query('SELECT maintenance_armed FROM anchor_email_recovery_control WHERE singleton = true LIMIT 1')
        .then(r => r.rows.length === 1
          ? { verified: true, value: r.rows[0].maintenance_armed }
          : { verified: false, reason: 'control_row_unavailable' })
        .catch(() => ({ verified: false, reason: 'control_read_failed' }))
      : { verified: false, reason: 'control_table_missing' },
    catalog.acquisition_outbound_programs
      ? db.query("SELECT mode, authorized_at, policy->>'expiresAt' AS expires_at FROM acquisition_outbound_programs WHERE tenant_id='10' AND mode<>'revoked' ORDER BY authorized_at DESC LIMIT 1").then(r => r.rows[0] || null).catch(() => null)
      : null,
  ]);
  return {
    kind: 'max_operational_readiness',
    generatedAt: new Date().toISOString(),
    readOnly: true,
    jev: {
      configured: enabled(env, 'DECISION_SHADOW_ENABLED'),
      providerConfigured: env.DECISION_PROVIDER === 'jev' && enabled(env, 'JEV_ENABLED'),
      persistenceConfigured: env.DECISION_SHADOW_PERSIST_ENABLED !== 'false',
      schema: jevSchema,
      events: jevEvents,
      evidence: jevEvidence,
    },
    semanticShadow: {
      configured: enabled(env, 'MAX_INTERPRETATION_SHADOW'),
      activeRouting: false,
      schemaReady: Boolean(catalog.max_interpretation_shadow),
      persistence: semantic,
    },
    conversationContext: {
      configured: enabled(env, 'MAX_CONVERSATION_CONTEXT'),
      schemaReady: Boolean(catalog.max_conversation_context),
      authenticatedRecoveryCapability: authenticatedRecoveryCapability(),
      activeRows: continuity.rows,
      latestExpiryAt: continuity.lastSuccessfulPersistenceAt,
    },
    observerHealth: observerHealthSnapshot(),
    outbound: {
      ...outboundGateRead(outbound, recoveryControl),
      authorization: outboundProgram ? {
        mode: outboundProgram.mode,
        authorizedAt: outboundProgram.authorized_at || null,
        expiresAt: outboundProgram.expires_at || null,
      } : null,
    },
  };
}

module.exports = { getMaxOperationalReadiness, tableCatalog };
