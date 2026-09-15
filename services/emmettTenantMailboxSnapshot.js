'use strict';

/**
 * SPEC-255 — build Emmett inbox snapshot from tenant mailbox canonical evidence.
 */

const { localDateOf } = require('../packages/emmett-outbound');
const { authenticationFromVerificationState } = require('../packages/emmett-outbound/AuthEvidence');
const { resolveInboxAge } = require('./emmettOutboundSnapshot');
const { resolveWarmupDailyCap } = require('../utils/sendWarmup');
const MAILBOX_STATUS = Object.freeze({ ACTIVE: 'active' });
const IDENTITY_STATUS = Object.freeze({ ACTIVE: 'active' });

function tenantMailboxStore(pool) {
  const { PostgresTenantMailboxStore } = require('./tenantMailbox');
  return new PostgresTenantMailboxStore(pool);
}

const DEFAULT_WARMUP_STAGES = Object.freeze([
  { afterSendDays: 0, dailyCap: 3 },
  { afterSendDays: 3, dailyCap: 5 },
  { afterSendDays: 7, dailyCap: 10 },
  { afterSendDays: 14, dailyCap: 20 },
]);

async function querySafe(pool, sql, params, fallback) {
  try {
    return await pool.query(sql, params);
  } catch (err) {
    if (err.code === '42P01' || err.code === '42703') return fallback;
    throw err;
  }
}

async function countTenantSends(pool, tenantId, sendingIdentityId, timeZone, localDate) {
  const todayRes = await querySafe(
    pool,
    `SELECT COUNT(*)::int AS sent_today
       FROM tenant_outreach_messages
      WHERE tenant_id = $1
        AND sending_identity_id = $2
        AND direction = 'OUTBOUND'
        AND status = 'sent'
        AND (sent_at AT TIME ZONE $4)::date = $3::date`,
    [String(tenantId), sendingIdentityId, localDate, timeZone],
    { rows: [{ sent_today: 0 }] }
  );

  const yesterdayRes = await querySafe(
    pool,
    `SELECT COUNT(*)::int AS sent_yesterday
       FROM tenant_outreach_messages
      WHERE tenant_id = $1
        AND sending_identity_id = $2
        AND direction = 'OUTBOUND'
        AND status = 'sent'
        AND (sent_at AT TIME ZONE $4)::date = ($3::date - INTERVAL '1 day')`,
    [String(tenantId), sendingIdentityId, localDate, timeZone],
    { rows: [{ sent_yesterday: 0 }] }
  );

  const totalRes = await querySafe(
    pool,
    `SELECT COUNT(*)::int AS total_sends
       FROM tenant_outreach_messages
      WHERE tenant_id = $1
        AND sending_identity_id = $2
        AND direction = 'OUTBOUND'
        AND status = 'sent'`,
    [String(tenantId), sendingIdentityId],
    { rows: [{ total_sends: 0 }] }
  );

  const activeDaysRes = await querySafe(
    pool,
    `SELECT COUNT(DISTINCT (sent_at AT TIME ZONE $3)::date)::int AS active_send_days
       FROM tenant_outreach_messages
      WHERE tenant_id = $1
        AND sending_identity_id = $2
        AND direction = 'OUTBOUND'
        AND status = 'sent'`,
    [String(tenantId), sendingIdentityId, timeZone],
    { rows: [{ active_send_days: 0 }] }
  );

  const firstSendRes = await querySafe(
    pool,
    `SELECT MIN(sent_at) AS first_sent_at
       FROM tenant_outreach_messages
      WHERE tenant_id = $1
        AND sending_identity_id = $2
        AND direction = 'OUTBOUND'
        AND status = 'sent'`,
    [String(tenantId), sendingIdentityId],
    { rows: [{ first_sent_at: null }] }
  );

  const scheduledRes = await querySafe(
    pool,
    `SELECT COUNT(*)::int AS scheduled_today
       FROM tenant_outreach_scheduled_sends
      WHERE tenant_id = $1
        AND sending_identity_id = $2
        AND status = 'SCHEDULED'
        AND (scheduled_for AT TIME ZONE $4)::date = $3::date`,
    [String(tenantId), sendingIdentityId, localDate, timeZone],
    { rows: [{ scheduled_today: 0 }] }
  );

  const bounceRes = await querySafe(
    pool,
    `SELECT COUNT(*)::int AS hard_bounces
       FROM tenant_outreach_messages
      WHERE tenant_id = $1
        AND sending_identity_id = $2
        AND status = 'failed'
        AND failure_code IN ('hard_bounce', 'bounce', '550', 'smtp_rejected')`,
    [String(tenantId), sendingIdentityId],
    { rows: [{ hard_bounces: 0 }] }
  );

  return {
    sentToday: Number(todayRes.rows[0]?.sent_today || 0),
    sentYesterday: Number(yesterdayRes.rows[0]?.sent_yesterday || 0),
    totalOperationalSends: Number(totalRes.rows[0]?.total_sends || 0),
    activeSendDays: Number(activeDaysRes.rows[0]?.active_send_days || 0),
    firstSentAt: firstSendRes.rows[0]?.first_sent_at || null,
    scheduledToday: Number(scheduledRes.rows[0]?.scheduled_today || 0),
    hardBounceCount: Number(bounceRes.rows[0]?.hard_bounces || 0),
  };
}

async function buildTenantMailboxInboxSnapshot(input = {}, opts = {}) {
  const tenantId = String(input.tenantId);
  const sendingIdentityId = input.sendingIdentityId;
  const mailboxIntegrationId = input.mailboxIntegrationId || null;
  const pool = opts.pool;
  const now = opts.now instanceof Date ? opts.now : new Date(opts.now || Date.now());
  const timeZone = opts.timeZone || 'America/New_York';
  const localDate = opts.localDate || localDateOf(now, timeZone);
  const store = opts.mailboxStore || (pool ? tenantMailboxStore(pool) : null);

  let integration = input.integration || null;
  let identity = input.identity || null;

  if (store && sendingIdentityId) {
    identity = identity || await store.getIdentity(tenantId, sendingIdentityId);
    if (identity) {
      integration = integration || await store.getIntegration(
        tenantId,
        mailboxIntegrationId || identity.mailboxIntegrationId
      );
    }
  }

  const verificationState = integration?.verificationState || integration?.verification_state || {};
  const authentication = input.authentication
    || authenticationFromVerificationState(verificationState);

  const warmupStages = opts.warmupStages || input.warmupStages || DEFAULT_WARMUP_STAGES;
  let sendStats = {
    sentToday: 0,
    sentYesterday: 0,
    totalOperationalSends: 0,
    activeSendDays: 0,
    firstSentAt: null,
    scheduledToday: 0,
    hardBounceCount: 0,
  };
  if (pool && sendingIdentityId) {
    sendStats = await countTenantSends(pool, tenantId, sendingIdentityId, timeZone, localDate);
  } else if (input.sendStats) {
    sendStats = { ...sendStats, ...input.sendStats };
  }

  const warmupCap = resolveWarmupDailyCap(warmupStages, sendStats.activeSendDays);
  let warmupStatus = 'warming';
  if (sendStats.activeSendDays >= 14 && warmupCap >= (opts.providerCeiling || 20)) {
    warmupStatus = 'healthy';
  } else if (sendStats.activeSendDays === 0 && !sendStats.firstSentAt) {
    warmupStatus = 'warming';
  }

  const { inboxAgeDays, inboxAgeSource, inboxAgeAnchor } = resolveInboxAge({
    firstSentAt: sendStats.firstSentAt,
    warmupStartDate: integration?.createdAt || identity?.createdAt || null,
    createdAt: integration?.createdAt || identity?.createdAt || null,
    now,
    fallbackAgeDays: 0,
  });

  const domain = (identity?.senderEmail || integration?.mailboxAddress || '').split('@')[1] || null;
  let suppressed = false;
  if (store && input.checkSuppression !== false) {
    // Suppression is recipient-scoped; snapshot only flags tenant-level hard bounce evidence.
    suppressed = false;
  }

  return {
    tenantId,
    clientId: Number.isFinite(Number(tenantId)) ? Number(tenantId) : null,
    sendingIdentityId,
    mailboxIntegrationId: integration?.id || mailboxIntegrationId,
    inboxId: identity?.senderEmail || sendingIdentityId,
    domain,
    localDate,
    timeZone,
    inboxAgeDays,
    inboxAgeSource,
    inboxAgeAnchor,
    providerCeiling: Number(opts.providerCeiling || warmupCap || 3),
    mailboxStatus: integration?.status || null,
    identityStatus: identity?.status || null,
    authentication,
    verificationState,
    warmup: {
      status: warmupStatus,
      dailyCap: warmupCap || warmupStages[0]?.dailyCap || 3,
      activeSendDays: sendStats.activeSendDays,
      reset: sendStats.activeSendDays === 0 && !sendStats.firstSentAt,
    },
    warmupStages,
    deliverabilityObservability: 'limited',
    bounceRate: sendStats.hardBounceCount > 0 && sendStats.totalOperationalSends > 0
      ? sendStats.hardBounceCount / sendStats.totalOperationalSends
      : 0,
    replyRate: null,
    openRate: null,
    complaintRate: 0,
    hardBounceCount: sendStats.hardBounceCount,
    blacklist: opts.blacklist || { listed: false, sources: [] },
    sentToday: sendStats.sentToday,
    sentYesterday: sendStats.sentYesterday,
    scheduledToday: sendStats.scheduledToday,
    historicalDailyAvg: sendStats.totalOperationalSends > 0
      ? sendStats.totalOperationalSends / Math.max(sendStats.activeSendDays, 1)
      : 0,
    recentSends: sendStats.totalOperationalSends,
    totalOperationalSends: sendStats.totalOperationalSends,
    suppressed,
    operatorOverride: opts.operatorOverride || null,
    replyByWeekday: {},
    bootstrapEnabled: opts.bootstrapEnabled !== false,
    businessHours: opts.businessHours || { startHour: 9, endHour: 16 },
  };
}

function isTenantMailboxOperational(integration, identity) {
  return integration?.status === MAILBOX_STATUS.ACTIVE
    && identity?.status === IDENTITY_STATUS.ACTIVE;
}

module.exports = {
  DEFAULT_WARMUP_STAGES,
  buildTenantMailboxInboxSnapshot,
  countTenantSends,
  isTenantMailboxOperational,
};
