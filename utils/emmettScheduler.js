'use strict';

/**
 * SPEC-189 — Emmett Infrastructure Scheduler Adapter
 *
 * Scheduled assessment of outbound infrastructure readiness.
 * Orchestrates shared EOI services only. Does not perform acquisition.
 *
 * Canonical acquisition execution remains owned by ExecutionRouter.
 *
 * This adapter assesses:
 * - Canonical sender identity
 * - Sender/domain health
 * - Inbox capacity and governor state
 * - Outbound readiness
 * - Infrastructure telemetry
 *
 * Does NOT:
 * - Select acquisition candidates
 * - Perform provider sends
 * - Mutate prospect lifecycle
 * - Advance sequence state
 */

const { getClientConfig, getRuntimeClientId } = require('./clientContext');
const { resolveCanonicalSenderIdentity } = require('./canonicalSenderIdentity');
const { normalizeRootDomain, rootDomainFromEmail } = require('./brevoEvents');
const { getWarmupProgress, resolveWarmupDailyCap } = require('./sendWarmup');
const { getBrevoState } = require('./sendingReadiness');
const { reportAgentRun } = require('./agentObservability');
const outboundIntel = require('../services/emmettOutbound');
const db = require('../dbClient');
const { randomUUID } = require('crypto');

const SCHEDULER_NAME = 'emmett_infrastructure';
const CLIENT_ID = getRuntimeClientId();

function makeRunId() {
  return `${SCHEDULER_NAME}-${CLIENT_ID || 'none'}-${new Date().toISOString()}-${randomUUID()}`;
}

function normalizeSendingDomain(value) {
  return normalizeRootDomain(value) || rootDomainFromEmail(value) || 'unknown.local';
}

/**
 * Assess sending domain health via 7-day bounce metrics.
 * Returns status { ok, paused, halted } with bounce rates and send counts.
 */
async function checkSendingDomainHealth(sendingDomain) {
  const normalizedDomain = normalizeSendingDomain(sendingDomain);
  const pool = require('../db');
  const res = await pool.query(`
    SELECT
      COUNT(*) FILTER (WHERE event_type IN ('hard_bounce','blocked')) AS bounces,
      COUNT(*) FILTER (WHERE event_type = 'sent') AS sends,
      ROUND(100.0 * COUNT(*) FILTER (WHERE event_type IN ('hard_bounce','blocked'))
            / NULLIF(COUNT(*) FILTER (WHERE event_type = 'sent'), 0), 2) AS bounce_pct
    FROM email_events
    WHERE event_at >= NOW() - INTERVAL '7 days'
      AND sending_domain = $1
  `, [normalizedDomain]);

  const row = res.rows[0] || {};
  const health = {
    status: 'ok',
    bouncePct: Number(row.bounce_pct || 0),
    sends: Number(row.sends || 0),
    bounces: Number(row.bounces || 0),
    sendingDomain: normalizedDomain,
  };

  if (health.sends < 20) return health;

  if (health.bouncePct >= 4.0) {
    health.status = 'halted';
    await logDomainHealthAlert('sending_domain_halted', health, 'failed');
    console.error(`[Emmett] Critical bounce alert for ${normalizedDomain}: ${health.bouncePct}% over ${health.sends} sends`);
    return health;
  }

  if (health.bouncePct >= 2.0) {
    health.status = 'paused';
    await logDomainHealthAlert('sending_domain_paused', health, 'skipped');
    console.warn(`[Emmett] Bounce warning for ${normalizedDomain}: ${health.bouncePct}% over ${health.sends} sends`);
  }

  return health;
}

async function logDomainHealthAlert(action, health, status = 'skipped') {
  await db.logAgentAction(
    SCHEDULER_NAME,
    action,
    null,
    null,
    {
      client_id: CLIENT_ID,
      sending_domain: health.sendingDomain,
      bounce_pct: health.bouncePct,
      sends: health.sends,
      bounces: health.bounces,
    },
    status
  );
}

/**
 * Get effective send config, accounting for warmup/ramp progression.
 */
async function getEffectiveSendConfig(baseConfig) {
  const pool = require('../db');
  if (baseConfig.warmup) {
    const progress = await getWarmupProgress(
      pool,
      CLIENT_ID,
      baseConfig.warmup.resetAfterDays
    );
    const warmupCap = resolveWarmupDailyCap(
      baseConfig.warmup.stages,
      progress.activeSendDays
    );
    const dailyCap = Math.min(baseConfig.dailyCap, warmupCap || baseConfig.dailyCap);
    return {
      ...baseConfig,
      dailyCap,
      ramped: dailyCap < baseConfig.dailyCap,
      warmupProgress: progress,
    };
  }

  if (!baseConfig.ramp || CLIENT_ID !== 5) return { ...baseConfig, ramped: false };

  const stats = await pool.query(`
    SELECT
      MIN(ran_at) AS first_sent_at,
      COUNT(*)::int AS total_sent
    FROM agent_log
    WHERE action = 'email_sent'
      AND client_id = $1
  `, [CLIENT_ID]);
  const firstSentAt = stats.rows[0]?.first_sent_at;
  const totalSent = Number(stats.rows[0]?.total_sent || 0);
  if (!firstSentAt || totalSent === 0) return { ...baseConfig, ramped: false };

  const bounceStats = await pool.query(`
    SELECT COUNT(*)::int AS bounced
    FROM touchpoints
    WHERE client_id = $1
      AND channel = 'email'
      AND action_type IN ('email_bounced', 'email_soft_bounce')
  `, [CLIENT_ID]);

  const bounced = Number(bounceStats.rows[0]?.bounced || 0);
  const bounceRate = totalSent ? bounced / totalSent : 0;
  const daysSinceFirstSend = (Date.now() - new Date(firstSentAt).getTime()) / (1000 * 60 * 60 * 24);
  const shouldRamp =
    daysSinceFirstSend >= baseConfig.ramp.afterDays &&
    bounceRate < baseConfig.ramp.bounceCeiling;

  if (!shouldRamp) return { ...baseConfig, ramped: false };

  const existingRampLog = await pool.query(`
    SELECT 1
    FROM agent_log
    WHERE agent_name = $1
      AND action = 'cap_ramped'
      AND client_id = $2
    LIMIT 1
  `, [SCHEDULER_NAME, CLIENT_ID]);

  if (!existingRampLog.rows.length) {
    await db.logAgentAction(
      SCHEDULER_NAME,
      'cap_ramped',
      null,
      null,
      {
        client_id: CLIENT_ID,
        previous_daily_cap: baseConfig.dailyCap,
        new_daily_cap: baseConfig.ramp.newDailyCap,
        days_since_first_send: Number(daysSinceFirstSend.toFixed(1)),
        bounce_rate: Number(bounceRate.toFixed(4)),
        bounced,
        total_sent: totalSent,
      },
      'success'
    );
  }

  return { ...baseConfig, dailyCap: baseConfig.ramp.newDailyCap, ramped: true };
}

/**
 * Get count of emails sent today.
 */
async function getEmailsSentToday() {
  const pool = require('../db');
  const res = await pool.query(`
    SELECT COUNT(*)::int AS count
    FROM agent_log
    WHERE action = 'email_sent'
      AND client_id = $1
      AND DATE(ran_at AT TIME ZONE 'America/New_York') =
          DATE(NOW() AT TIME ZONE 'America/New_York')
  `, [CLIENT_ID]);
  return res.rows[0]?.count || 0;
}

/**
 * Assess outbound capacity and governor state via shared EOI.
 * Does NOT initiate any sends or prospect selection.
 */
async function assessOutboundCapacity({ alreadySentToday, sendConfig }) {
  try {
    const pool = require('../db');
    await outboundIntel.hydrateTenant(String(CLIENT_ID), { pool });
    const snapshot = await outboundIntel.buildInboxSnapshot(CLIENT_ID, {
      pool,
      providerCeiling: sendConfig.dailyCap,
      warmupStages: sendConfig.warmup?.stages || [],
    });
    snapshot.sentToday = alreadySentToday;
    const assessed = outboundIntel.getEngine().assess({
      tenantId: String(CLIENT_ID),
      clientId: CLIENT_ID,
      snapshot,
    });
    const { governor, capacity, approvedPlan } = assessed;
    if (governor.halt) {
      return {
        allowRun: false,
        reason: governor.outcome,
        governor,
        capacity,
        approvedPlan,
        localDate: snapshot.localDate,
      };
    }
    if (!approvedPlan) {
      return {
        allowRun: false,
        reason: 'awaiting_operator_approval',
        governor,
        capacity,
        approvedPlan,
        localDate: snapshot.localDate,
      };
    }
    const reasonedCap = Number(approvedPlan.approvedCapacity || capacity.recommended || 0);
    const slowCap = governor.slowCap || reasonedCap;
    return {
      allowRun: true,
      reason: governor.outcome,
      governor,
      capacity,
      approvedPlan,
      localDate: snapshot.localDate,
      dailyCap: Math.max(0, Math.min(sendConfig.dailyCap, reasonedCap, slowCap)),
    };
  } catch (err) {
    console.error('[Emmett] Outbound intelligence assessment failed:', err.message);
    return { allowRun: false, reason: 'assessment_failed', error: err.message };
  }
}

async function reportSchedulerRun({ runId, status, infrastructure }) {
  try {
    return await reportAgentRun({
      agent: SCHEDULER_NAME,
      clientId: CLIENT_ID,
      runId,
      status,
      infrastructure,
    });
  } catch (err) {
    console.error('[Emmett] Scheduler observability report failed:', err.message);
    return null;
  }
}

/**
 * Client config defaults for Emmett infrastructure (capacity/warmup/ramp).
 */
const clientConfig = {
  1: {
    dailyCap: 100,
    verticalCap: 15,
    warmup: {
      resetAfterDays: 7,
      stages: [
        { afterSendDays: 0, dailyCap: 10 },
        { afterSendDays: 2, dailyCap: 15 },
        { afterSendDays: 4, dailyCap: 25 },
        { afterSendDays: 7, dailyCap: 40 },
        { afterSendDays: 11, dailyCap: 60 },
        { afterSendDays: 16, dailyCap: 80 },
        { afterSendDays: 22, dailyCap: 100 },
      ],
    },
  },
  2: { dailyCap: 40, verticalCap: 10 },
  5: { dailyCap: 30, verticalCap: 8, ramp: { afterDays: 14, bounceCeiling: 0.03, newDailyCap: 50 } },
  10: {
    dailyCap: 50,
    verticalCap: 10,
    warmup: {
      resetAfterDays: 7,
      stages: [
        { afterSendDays: 0, dailyCap: 5 },
        { afterSendDays: 3, dailyCap: 8 },
        { afterSendDays: 6, dailyCap: 12 },
        { afterSendDays: 10, dailyCap: 18 },
        { afterSendDays: 15, dailyCap: 25 },
        { afterSendDays: 21, dailyCap: 35 },
        { afterSendDays: 28, dailyCap: 50 },
      ],
    },
  },
};

function getEmmettClientConfig(clientId = CLIENT_ID) {
  return clientConfig[clientId] || clientConfig[1];
}

/**
 * Run scheduled Emmett infrastructure assessment.
 *
 * Returns { status, infrastructure } where:
 * - status: operational | halted | paused | capacity_limited | awaiting_approval | idle
 * - infrastructure: { sender, domain_health, capacity, governor, approvedPlan, ... }
 *
 * Does NOT perform any acquisition sends or prospect mutation.
 */
async function assessInfrastructure(context = {}) {
  const runId = makeRunId();

  try {
    console.log('\nEmmett infrastructure assessment...\n');

    // Resolve canonical sender identity
    const CLIENT_CONFIG = await getClientConfig(CLIENT_ID);
    if (!CLIENT_CONFIG) {
      await db.logAgentAction(
        SCHEDULER_NAME,
        'assessment',
        null,
        null,
        { client_id: CLIENT_ID, reason: 'client_not_found' },
        'failed'
      );
      return { status: 'failed', reason: 'client_not_found' };
    }

    const canonicalSender = await resolveCanonicalSenderIdentity({
      tenantId: CLIENT_ID,
      clientId: CLIENT_ID,
      client: CLIENT_CONFIG,
    });

    if (!canonicalSender.ok) {
      console.error(`[Emmett] Canonical sender blocked: ${canonicalSender.blockReason}`);
      await db.logAgentAction(
        SCHEDULER_NAME,
        'assessment',
        null,
        null,
        {
          client_id: CLIENT_ID,
          reason: canonicalSender.code,
          block_reason: canonicalSender.blockReason,
        },
        'failed'
      );
      return { status: 'blocked', reason: canonicalSender.code };
    }

    const senderEmail = canonicalSender.identity.senderEmail;
    const senderName = canonicalSender.identity.senderName;
    console.log(`Sender: ${senderName || '(missing)'} <${senderEmail || '(missing)'}>`);

    // Assess domain health
    const sendingDomain = normalizeSendingDomain(CLIENT_CONFIG.sending_domain);
    const domainHealth = await checkSendingDomainHealth(sendingDomain);

    if (domainHealth.status === 'halted') {
      console.error(`[Emmett] Infrastructure halted. Resolve the blocker before restarting.`);
      await db.logAgentAction(
        SCHEDULER_NAME,
        'assessment',
        null,
        null,
        {
          client_id: CLIENT_ID,
          sending_domain: sendingDomain,
          domain_health: domainHealth,
          reason: 'domain_halted',
        },
        'failed'
      );
      return {
        status: 'halted',
        infrastructure: {
          sender: senderEmail,
          domain_health: domainHealth,
        },
      };
    }

    if (domainHealth.status === 'paused') {
      console.warn(`[Emmett] Infrastructure paused. Bounce rate: ${domainHealth.bouncePct}% over ${domainHealth.sends} sends.`);
      await db.logAgentAction(
        SCHEDULER_NAME,
        'assessment',
        null,
        null,
        {
          client_id: CLIENT_ID,
          sending_domain: sendingDomain,
          domain_health: domainHealth,
          reason: 'domain_paused',
        },
        'skipped'
      );
      return {
        status: 'paused',
        infrastructure: {
          sender: senderEmail,
          domain_health: domainHealth,
        },
      };
    }

    // Get effective send config accounting for warmup/ramp
    let sendConfig = await getEffectiveSendConfig(getEmmettClientConfig(CLIENT_ID));
    const capOverride = Number(context.dailyCapOverride || 0);
    if (capOverride > 0) {
      sendConfig = { ...sendConfig, dailyCap: Math.min(getEmmettClientConfig(CLIENT_ID).dailyCap, capOverride) };
    }

    const alreadySentToday = await getEmailsSentToday();
    const capacityAssessment = await assessOutboundCapacity({ alreadySentToday, sendConfig });

    if (!capacityAssessment.allowRun) {
      console.warn(`[Emmett] Governor blocked assessment: ${capacityAssessment.reason}`);
      await db.logAgentAction(
        SCHEDULER_NAME,
        'assessment',
        null,
        null,
        {
          client_id: CLIENT_ID,
          already_sent_today: alreadySentToday,
          reason: capacityAssessment.reason,
          governor: capacityAssessment.governor || null,
          capacity: capacityAssessment.capacity || null,
        },
        capacityAssessment.reason === 'emergency' ? 'failed' : 'skipped'
      );
      return {
        status: capacityAssessment.reason === 'emergency' ? 'emergency' : 'awaiting_approval',
        infrastructure: {
          sender: senderEmail,
          domain_health: domainHealth,
          governor: capacityAssessment.governor,
          capacity: capacityAssessment.capacity,
          approvedPlan: capacityAssessment.approvedPlan,
        },
      };
    }

    const remainingCapacity = Math.max(0, capacityAssessment.dailyCap - alreadySentToday);
    const warmupLabel = sendConfig.warmupProgress
      ? ` (warmup send-day ${sendConfig.warmupProgress.activeSendDays},${sendConfig.warmupProgress.reset ? ' reset,' : ''} ceiling ${getEmmettClientConfig(CLIENT_ID).dailyCap})`
      : sendConfig.ramped ? ' (ramped)' : '';
    console.log(`Daily cap: ${capacityAssessment.dailyCap}${warmupLabel}; already sent: ${alreadySentToday}; remaining: ${remainingCapacity}`);

    if (remainingCapacity <= 0) {
      console.log('Daily send limit reached.');
      await db.logAgentAction(
        SCHEDULER_NAME,
        'assessment',
        null,
        null,
        {
          client_id: CLIENT_ID,
          daily_cap: capacityAssessment.dailyCap,
          already_sent_today: alreadySentToday,
          reason: 'daily_cap_reached',
        },
        'success'
      );
      return {
        status: 'capacity_limited',
        infrastructure: {
          sender: senderEmail,
          domain_health: domainHealth,
          governor: capacityAssessment.governor,
          capacity: capacityAssessment.capacity,
          approvedPlan: capacityAssessment.approvedPlan,
          remainingCapacity,
        },
      };
    }

    // Infrastructure ready
    console.log('[Emmett] Infrastructure assessment complete. Ready for canonical acquisition execution.');
    await db.logAgentAction(
      SCHEDULER_NAME,
      'assessment',
      null,
      null,
      {
        client_id: CLIENT_ID,
        sender: senderEmail,
        daily_cap: capacityAssessment.dailyCap,
        already_sent_today: alreadySentToday,
        remaining_capacity: remainingCapacity,
        infrastructure_status: 'operational',
      },
      'success'
    );

    return {
      status: 'operational',
      infrastructure: {
        sender: senderEmail,
        domain_health: domainHealth,
        governor: capacityAssessment.governor,
        capacity: capacityAssessment.capacity,
        approvedPlan: capacityAssessment.approvedPlan,
        dailyCap: capacityAssessment.dailyCap,
        remainingCapacity,
        warmupProgress: sendConfig.warmupProgress,
        ramped: sendConfig.ramped,
      },
    };
  } catch (err) {
    console.error('[Emmett] Assessment failed:', err.message);
    await db.logAgentAction(
      SCHEDULER_NAME,
      'assessment',
      null,
      null,
      { client_id: CLIENT_ID },
      'failed',
      err.message
    );
    return { status: 'failed', error: err.message };
  }
}

module.exports = {
  assessInfrastructure,
  checkSendingDomainHealth,
  assessOutboundCapacity,
  SCHEDULER_NAME,
};
