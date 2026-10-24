'use strict';

/**
 * SPEC-254 — Tenant-mailbox outbound observability for Emmett.
 * Builds identity-scoped snapshots from tenant SMTP evidence.
 * SMTP provider acceptance remains distinct from delivery.
 */

const { localDateOf } = require('../packages/emmett-outbound');
const { authenticationFromVerificationState } = require('../packages/emmett-outbound/AuthEvidence');
const { resolveInboxAge } = require('./emmettOutboundSnapshot');
const {
  classifyOutreachContactType,
  isRoleContactType,
  OUTREACH_CONTACT_TYPE,
} = require('../utils/outreachContactType');

const EVIDENCE_UNKNOWN = 'UNKNOWN';

function tenantKey(value) {
  if (value == null || value === '') return '';
  return String(value);
}

function lower(value) {
  return typeof value === 'string' ? value.trim().toLowerCase() : '';
}

function extractDomain(email) {
  const normalized = lower(email);
  if (!normalized.includes('@')) return null;
  return normalized.split('@')[1] || null;
}

async function querySafe(pool, sql, params, fallback) {
  try {
    return await pool.query(sql, params);
  } catch (err) {
    if (err.code === '42P01' || err.code === '42703') return fallback;
    throw err;
  }
}

function pct(part, whole) {
  const w = Number(whole || 0);
  if (w <= 0) return null;
  const rate = Number(part || 0) / w;
  return Number.isFinite(rate) ? rate : null;
}

const TENANT_WARMUP_STAGES = Object.freeze([
  { afterSendDays: 0, dailyCap: 3 },
  { afterSendDays: 3, dailyCap: 5 },
  { afterSendDays: 7, dailyCap: 8 },
  { afterSendDays: 14, dailyCap: 12 },
  { afterSendDays: 21, dailyCap: 20 },
  { afterSendDays: 30, dailyCap: 35 },
]);

function resolveWarmupDailyCap(stages, activeSendDays) {
  let cap = stages[0]?.dailyCap || 3;
  for (const stage of stages) {
    if (activeSendDays >= stage.afterSendDays) cap = stage.dailyCap;
  }
  return cap;
}

function resolveWarmupStatus(activeSendDays, dailyCap, providerCeiling) {
  if (activeSendDays <= 0) return 'warming';
  if (dailyCap >= providerCeiling) return 'healthy';
  if (activeSendDays < 14) return 'warming';
  return 'healthy';
}

/**
 * Build tenant-mailbox snapshot scoped to one sending identity.
 */
async function buildTenantMailboxSnapshot(tenantId, sendingIdentityId, opts = {}) {
  const pool = opts.pool;
  const now = opts.now instanceof Date ? opts.now : new Date(opts.now || Date.now());
  const timeZone = opts.timeZone || 'America/New_York';
  const localDate = opts.localDate || localDateOf(now, timeZone);
  const tid = tenantKey(tenantId);
  const sid = tenantKey(sendingIdentityId);

  const identityRes = await querySafe(
    pool,
    `SELECT si.*, mi.mailbox_address, mi.status AS mailbox_status, mi.created_at AS mailbox_created_at,
            mi.verification_state AS mailbox_verification_state
       FROM tenant_sending_identities si
       JOIN tenant_mailbox_integrations mi ON mi.id = si.mailbox_integration_id AND mi.tenant_id = si.tenant_id
      WHERE si.tenant_id = $1 AND si.id = $2
      LIMIT 1`,
    [tid, sid],
    { rows: [] }
  );
  const row = identityRes.rows[0];
  if (!row) {
    throw Object.assign(new Error('Sending identity not found for tenant-mailbox snapshot.'), {
      code: 'sending_identity_not_found',
    });
  }

  const senderEmail = lower(row.sender_email);
  const sendingDomain = extractDomain(senderEmail);
  const mailboxIntegrationId = row.mailbox_integration_id;

  const messageStats = await querySafe(
    pool,
    `SELECT
        COUNT(*) FILTER (WHERE direction = 'OUTBOUND' AND status = 'sent')::int AS successful_sends,
        COUNT(*) FILTER (WHERE direction = 'OUTBOUND' AND status = 'failed')::int AS failed_sends,
        COUNT(*) FILTER (WHERE direction = 'INBOUND')::int AS inbound_messages,
        MIN(sent_at) FILTER (WHERE direction = 'OUTBOUND' AND status = 'sent') AS first_sent_at
       FROM tenant_outreach_messages
      WHERE tenant_id = $1 AND sending_identity_id = $2`,
    [tid, sid],
    { rows: [{ successful_sends: 0, failed_sends: 0, inbound_messages: 0, first_sent_at: null }] }
  );
  const msgStats = messageStats.rows[0] || {};

  const replyRes = await querySafe(
    pool,
    `SELECT COUNT(*)::int AS reply_count
       FROM tenant_outreach_events e
       JOIN tenant_outreach_messages m ON m.id = e.message_id AND m.tenant_id = e.tenant_id
      WHERE e.tenant_id = $1
        AND m.sending_identity_id = $2
        AND e.event_type = 'TENANT_OUTREACH_REPLY_RECEIVED'
        AND e.created_at >= NOW() - INTERVAL '7 days'`,
    [tid, sid],
    { rows: [{ reply_count: 0 }] }
  );

  const suppressionRes = await querySafe(
    pool,
    `SELECT COUNT(*)::int AS suppression_count,
        COUNT(*) FILTER (WHERE reason = 'bounce')::int AS hard_bounce_count
       FROM tenant_outreach_suppressions
      WHERE tenant_id = $1 AND revoked_at IS NULL`,
    [tid],
    { rows: [{ suppression_count: 0, hard_bounce_count: 0 }] }
  );

  const scheduledRes = await querySafe(
    pool,
    `SELECT COUNT(*)::int AS scheduled_count
       FROM tenant_outreach_scheduled_sends
      WHERE tenant_id = $1
        AND sending_identity_id = $2
        AND status IN ('SCHEDULED', 'EXECUTING')
        AND (scheduled_for AT TIME ZONE $4)::date = $3::date`,
    [tid, sid, localDate, timeZone],
    { rows: [{ scheduled_count: 0 }] }
  );

  const sentTodayRes = await querySafe(
    pool,
    `SELECT COUNT(*)::int AS sent_today
       FROM tenant_outreach_messages
      WHERE tenant_id = $1
        AND sending_identity_id = $2
        AND direction = 'OUTBOUND'
        AND status = 'sent'
        AND (sent_at AT TIME ZONE $4)::date = $3::date`,
    [tid, sid, localDate, timeZone],
    { rows: [{ sent_today: 0 }] }
  );

  const sentYesterdayRes = await querySafe(
    pool,
    `SELECT COUNT(*)::int AS sent_yesterday
       FROM tenant_outreach_messages
      WHERE tenant_id = $1
        AND sending_identity_id = $2
        AND direction = 'OUTBOUND'
        AND status = 'sent'
        AND (sent_at AT TIME ZONE $4)::date = ($3::date - INTERVAL '1 day')`,
    [tid, sid, localDate, timeZone],
    { rows: [{ sent_yesterday: 0 }] }
  );

  const avgRes = await querySafe(
    pool,
    `SELECT COALESCE(AVG(daily_count), 0)::float AS historical_daily_avg
       FROM (
         SELECT COUNT(*)::int AS daily_count
           FROM tenant_outreach_messages
          WHERE tenant_id = $1
            AND sending_identity_id = $2
            AND direction = 'OUTBOUND'
            AND status = 'sent'
            AND sent_at >= NOW() - INTERVAL '14 days'
          GROUP BY (sent_at AT TIME ZONE $3)::date
       ) days`,
    [tid, sid, timeZone],
    { rows: [{ historical_daily_avg: 0 }] }
  );

  const recentTimestampsRes = await querySafe(
    pool,
    `SELECT sent_at, recipients
       FROM tenant_outreach_messages
      WHERE tenant_id = $1
        AND sending_identity_id = $2
        AND direction = 'OUTBOUND'
        AND status = 'sent'
        AND sent_at >= NOW() - INTERVAL '7 days'
      ORDER BY sent_at DESC
      LIMIT 50`,
    [tid, sid],
    { rows: [] }
  );

  const activeSendDaysRes = await querySafe(
    pool,
    `SELECT COUNT(DISTINCT (sent_at AT TIME ZONE $3)::date)::int AS active_send_days
       FROM tenant_outreach_messages
      WHERE tenant_id = $1
        AND sending_identity_id = $2
        AND direction = 'OUTBOUND'
        AND status = 'sent'`,
    [tid, sid, timeZone],
    { rows: [{ active_send_days: 0 }] }
  );

  const successfulSends = Number(msgStats.successful_sends || 0);
  const failedSends = Number(msgStats.failed_sends || 0);
  const recentSends = successfulSends + failedSends;
  const hardBounces = Number(suppressionRes.rows[0]?.hard_bounce_count || 0);
  const replies = Number(replyRes.rows[0]?.reply_count || 0);

  let founderReplies = 0;
  let roleReplies = 0;
  for (const msgRow of recentTimestampsRes.rows) {
    const recipients = Array.isArray(msgRow.recipients) ? msgRow.recipients : [];
    for (const recipient of recipients) {
      const email = lower(recipient.email || recipient);
      if (!email) continue;
      const contactType = classifyOutreachContactType(email);
      if (contactType === OUTREACH_CONTACT_TYPE.VERIFIED_ROLE_EMAIL) roleReplies += 1;
      else if (contactType === OUTREACH_CONTACT_TYPE.VERIFIED_FOUNDER_EMAIL) founderReplies += 1;
    }
  }

  const { inboxAgeDays, inboxAgeSource, inboxAgeAnchor } = resolveInboxAge({
    firstSentAt: msgStats.first_sent_at || row.created_at || row.mailbox_created_at,
    warmupStartDate: null,
    createdAt: row.created_at || row.mailbox_created_at,
    now,
    fallbackAgeDays: 0,
  });

  const providerCeiling = Number(opts.providerCeiling || 35);
  const activeSendDays = Number(activeSendDaysRes.rows[0]?.active_send_days || 0);
  const warmupCap = resolveWarmupDailyCap(opts.warmupStages || TENANT_WARMUP_STAGES, activeSendDays);
  const warmupStatus = resolveWarmupStatus(activeSendDays, warmupCap, providerCeiling);

  const bounceRate = recentSends >= 5 && hardBounces > 0
    ? pct(hardBounces, recentSends)
    : null;
  const replyRate = successfulSends >= 3
    ? pct(replies, successfulSends)
    : null;
  const founderReplyRate = successfulSends >= 3 && founderReplies > 0
    ? pct(founderReplies, successfulSends)
    : null;

  const unknownEvidence = [];
  if (successfulSends < 5) unknownEvidence.push('delivery_rate');
  if (recentSends < 5) unknownEvidence.push('bounce_rate');
  if (successfulSends < 3) unknownEvidence.push('reply_rate');
  unknownEvidence.push('open_rate', 'complaint_rate', 'inbox_placement');

  return {
    channel: 'tenant_mailbox_smtp',
    spec: 'SPEC-254',
    tenantId: tid,
    sendingIdentityId: sid,
    mailboxIntegrationId,
    senderEmail,
    sendingDomain,
    inboxId: senderEmail,
    domain: sendingDomain,
    localDate,
    timeZone,
    mailboxStatus: row.mailbox_status || row.status,
    identityStatus: row.status,
    mailboxActivationAt: row.mailbox_created_at || row.created_at || null,
    inboxAgeDays,
    inboxAgeSource,
    inboxAgeAnchor,
    providerCeiling,
    authentication: row.mailbox_verification_state
      ? authenticationFromVerificationState(row.mailbox_verification_state)
      : {
        spf: EVIDENCE_UNKNOWN,
        dkim: EVIDENCE_UNKNOWN,
        dmarc: EVIDENCE_UNKNOWN,
      },
    deliverabilityObservability: 'limited',
    mailboxKind: 'tenant_smtp',
    warmup: {
      status: warmupStatus,
      dailyCap: warmupCap,
      activeSendDays,
      reset: activeSendDays <= 0,
      rampStage: activeSendDays < 7 ? 'early' : activeSendDays < 21 ? 'mid' : 'mature',
    },
    successfulSends,
    failedSends,
    scheduledSends: Number(scheduledRes.rows[0]?.scheduled_count || 0),
    suppressions: Number(suppressionRes.rows[0]?.suppression_count || 0),
    hardBounces,
    replies,
    founderReplies,
    roleReplies,
    bounceRate,
    replyRate,
    founderReplyRate,
    openRate: null,
    complaintRate: null,
    deliveryRate: null,
    unknownEvidence,
    blacklist: { listed: false, sources: [] },
    sentToday: Number(sentTodayRes.rows[0]?.sent_today || 0),
    sentYesterday: Number(sentYesterdayRes.rows[0]?.sent_yesterday || 0),
    historicalDailyAvg: Number(avgRes.rows[0]?.historical_daily_avg || 0),
    recentSends,
    recentSendTimestamps: recentTimestampsRes.rows.map((r) => r.sent_at).filter(Boolean),
    operatorOverride: opts.operatorOverride || null,
    contactTypeEvidence: {
      founderReplies,
      roleReplies,
      roleEmailsExcludedFromFounderEvidence: true,
    },
  };
}

async function buildTenantMailboxInboxSnapshot(input = {}, opts = {}) {
  if (input.integration && input.identity) {
    const verificationState = input.integration.verificationState
      || input.integration.verification_state
      || {};
    const authentication = input.authentication
      || authenticationFromVerificationState(verificationState);
    const createdAt = input.integration.createdAt || input.identity.createdAt || null;
    const now = opts.now instanceof Date ? opts.now : new Date(opts.now || Date.now());
    const timeZone = opts.timeZone || 'America/New_York';
    const sendStats = input.sendStats || {};
    const { inboxAgeDays, inboxAgeSource, inboxAgeAnchor } = resolveInboxAge({
      firstSentAt: sendStats.firstSentAt || null,
      warmupStartDate: createdAt,
      createdAt,
      now,
      fallbackAgeDays: 0,
    });
    return {
      tenantId: String(input.tenantId),
      clientId: Number.isFinite(Number(input.tenantId)) ? Number(input.tenantId) : null,
      sendingIdentityId: input.sendingIdentityId || input.identity.id,
      mailboxIntegrationId: input.integration.id,
      inboxId: input.identity.senderEmail,
      domain: extractDomain(input.identity.senderEmail || input.integration.mailboxAddress),
      localDate: opts.localDate || localDateOf(now, timeZone),
      timeZone,
      inboxAgeDays,
      inboxAgeSource,
      inboxAgeAnchor,
      providerCeiling: Number(opts.providerCeiling || 3),
      mailboxStatus: input.integration.status || null,
      identityStatus: input.identity.status || null,
      authentication,
      verificationState,
      warmup: {
        status: 'warming',
        dailyCap: 3,
        activeSendDays: Number(sendStats.activeSendDays || 0),
        reset: !sendStats.firstSentAt && !sendStats.activeSendDays,
      },
      deliverabilityObservability: 'limited',
      mailboxKind: 'tenant_smtp',
      bounceRate: 0,
      replyRate: null,
      openRate: null,
      complaintRate: 0,
      hardBounceCount: Number(sendStats.hardBounceCount || 0),
      blacklist: { listed: false, sources: [] },
      sentToday: Number(sendStats.sentToday || 0),
      sentYesterday: Number(sendStats.sentYesterday || 0),
      scheduledToday: Number(sendStats.scheduledToday || 0),
      recentSends: Number(sendStats.totalOperationalSends || 0),
      totalOperationalSends: Number(sendStats.totalOperationalSends || 0),
      bootstrapEnabled: opts.bootstrapEnabled !== false,
      businessHours: opts.businessHours || { startHour: 9, endHour: 16 },
    };
  }
  return buildTenantMailboxSnapshot(input.tenantId, input.sendingIdentityId, opts);
}

module.exports = {
  buildTenantMailboxSnapshot,
  buildTenantMailboxInboxSnapshot,
  TENANT_WARMUP_STAGES,
  EVIDENCE_UNKNOWN,
  isRoleContactType,
};
