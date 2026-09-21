'use strict';

/**
 * SPEC-248 IPv4 SMTP connectivity verification (no send).
 *
 * Verifies that GENERIC_SMTP_IMAP transport can reach the provider SMTP endpoint
 * over IPv4 while preserving TLS servername on the configured hostname.
 *
 * Usage:
 *   DATABASE_URL=... BABRUN_MAILBOX_SMTP_PASSWORD=... \
 *     node scripts/verifyGenericSmtpIpv4Connectivity.js --tenant-id=13 --integration-id=tmi_13_babrun_hello
 *
 * Optional:
 *   --schedule-kaylee-canary   After connectivity passes, authorize a new Babrun Kaylee schedule.
 */

require('dotenv').config();

const pool = require('../db');
const { resolveIpv4Addresses } = require('../utils/mailNetwork');
const {
  PostgresTenantMailboxStore,
  createSmtpTransport,
  resolveSecretRef,
} = require('../services/tenantMailbox');
const {
  authorizeScheduledOutreachSend,
  evaluateSchedulingEligibility,
  PostgresScheduleStore,
  PAST_DUE_POLICY,
} = require('../services/tenantOutreachScheduler');
const { resolveOutreachAssetMessage } = require('../packages/acquisition-knowledge/resolveOutreachAssetMessage');

const BABRUN = Object.freeze({
  tenantId: '13',
  prospectId: '12264293-ba4d-494d-a1e7-f93ed1f62a2c',
  akObjectId: 'ak_babrun_prospect_p024',
  outreachAssetId: 'ak_babrun_outreach_final_05',
  sendingIdentityId: 'tsi_13_babrun_fedir',
  recipientEmail: 'kaylee@kbpainting.com',
});

const BUSINESS_TZ = 'America/New_York';
const BUSINESS_START_HOUR = 9;
const BUSINESS_END_HOUR = 16;
const MIN_LEAD_MINUTES = 10;

function parseArgs(argv = process.argv.slice(2)) {
  const args = { scheduleKayleeCanary: false };
  for (const arg of argv) {
    if (arg === '--schedule-kaylee-canary') args.scheduleKayleeCanary = true;
    else if (arg.startsWith('--tenant-id=')) args.tenantId = arg.split('=')[1];
    else if (arg.startsWith('--integration-id=')) args.integrationId = arg.split('=')[1];
  }
  return args;
}

function zonedParts(date, timeZone) {
  const fmt = new Intl.DateTimeFormat('en-US', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  });
  const parts = Object.fromEntries(fmt.formatToParts(date).filter((p) => p.type !== 'literal').map((p) => [p.type, p.value]));
  return {
    year: Number(parts.year),
    month: Number(parts.month),
    day: Number(parts.day),
    hour: Number(parts.hour),
    minute: Number(parts.minute),
  };
}

function offsetMinutesForTimeZone(date, timeZone) {
  const parts = zonedParts(date, timeZone);
  const asUtc = Date.UTC(parts.year, parts.month - 1, parts.day, parts.hour, parts.minute, 0, 0);
  return Math.round((asUtc - date.getTime()) / 60000);
}

function zonedDateTimeToUtc({ year, month, day, hour, minute }, timeZone) {
  const guess = new Date(Date.UTC(year, month - 1, day, hour, minute, 0, 0));
  const offset = offsetMinutesForTimeZone(guess, timeZone);
  return new Date(guess.getTime() - offset * 60000);
}

function nextSuitableBusinessWindow(now = new Date(), timeZone = BUSINESS_TZ) {
  let candidate = new Date(now.getTime() + MIN_LEAD_MINUTES * 60000);
  candidate = new Date(Math.ceil(candidate.getTime() / (15 * 60000)) * (15 * 60000));

  for (let i = 0; i < 96; i += 1) {
    const parts = zonedParts(candidate, timeZone);
    const nowParts = zonedParts(now, timeZone);
    const sameDay = parts.year === nowParts.year && parts.month === nowParts.month && parts.day === nowParts.day;
    if (sameDay && parts.hour >= BUSINESS_START_HOUR && parts.hour < BUSINESS_END_HOUR) {
      const scheduledUtc = zonedDateTimeToUtc({
        year: parts.year,
        month: parts.month,
        day: parts.day,
        hour: parts.hour,
        minute: parts.minute,
      }, timeZone);
      return {
        scheduledUtc,
        scheduledForIso: scheduledUtc.toISOString(),
        localLabel: new Intl.DateTimeFormat('en-US', {
          timeZone,
          weekday: 'short',
          month: 'short',
          day: 'numeric',
          hour: 'numeric',
          minute: '2-digit',
          timeZoneName: 'shortGeneric',
        }).format(scheduledUtc),
      };
    }
    candidate = new Date(candidate.getTime() + 15 * 60000);
  }
  throw new Error('No suitable business-hours window found today.');
}

async function loadOutreachAsset(client, tenantId, assetId) {
  const res = await client.query(
    `SELECT id, object_type, lifecycle_state, validation_state, status, channel, content, version, updated_at
     FROM acquisition_knowledge_objects
     WHERE tenant_id = $1 AND id = $2 AND object_type = 'outreach_asset'
     LIMIT 1`,
    [tenantId, assetId]
  );
  const row = res.rows[0];
  if (!row) throw Object.assign(new Error(`Outreach asset not found: ${assetId}`), { code: 'outreach_asset_not_found' });
  const resolved = resolveOutreachAssetMessage(row, { requireStakeholderValidated: true });
  return {
    outreachAssetVersion: resolved.revision,
    subject: resolved.subject,
    body: resolved.body,
    copySource: resolved.source,
  };
}

async function verifySmtpConnectivity(integration, opts = {}) {
  const secret = resolveSecretRef(integration.smtpSecretRef || integration.sharedSecretRef, opts);
  const ipv4Addresses = await resolveIpv4Addresses(integration.smtpHost, opts);
  const transport = createSmtpTransport(integration, secret, opts);
  const startedAt = Date.now();
  await transport.verify();
  return {
    hostname: integration.smtpHost,
    port: integration.smtpPort,
    tlsServername: integration.smtpHost,
    ipv4Addresses,
    verifyMs: Date.now() - startedAt,
  };
}

async function main() {
  const args = parseArgs();
  const tenantId = args.tenantId || BABRUN.tenantId;
  const store = new PostgresTenantMailboxStore(pool);
  const integration = await store.getIntegration(tenantId, args.integrationId || `tmi_${tenantId}_babrun_hello`);
  if (!integration) {
    throw Object.assign(new Error(`Mailbox integration not found for tenant ${tenantId}`), { code: 'mailbox_integration_not_found' });
  }

  const report = {
    generatedAt: new Date().toISOString(),
    tenantId,
    integrationId: integration.id,
    smtpHost: integration.smtpHost,
    smtpPort: integration.smtpPort,
    connectivity: null,
    schedule: null,
    verdict: null,
  };

  try {
    report.connectivity = await verifySmtpConnectivity(integration, { store });
    report.verdict = 'SMTP_IPV4_PATH_VERIFIED';
  } catch (err) {
    report.connectivity = {
      errorCode: err.code || 'smtp_connectivity_failed',
      errorMessage: String(err.message || err),
    };
    report.verdict = 'SMTP_NETWORK_PATH_STILL_BLOCKED';
    process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
    process.exitCode = 2;
    return;
  }

  if (!args.scheduleKayleeCanary) {
    process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
    return;
  }

  const window = nextSuitableBusinessWindow(new Date(), BUSINESS_TZ);
  const asset = await loadOutreachAsset(pool, BABRUN.tenantId, BABRUN.outreachAssetId);
  const scheduleStore = new PostgresScheduleStore(pool);
  const mailboxStore = new PostgresTenantMailboxStore(pool);
  const eligibility = await evaluateSchedulingEligibility({
    tenantId: BABRUN.tenantId,
    prospectId: BABRUN.prospectId,
    outreachAssetId: BABRUN.outreachAssetId,
    outreachAssetVersion: asset.outreachAssetVersion,
    sendingIdentityId: BABRUN.sendingIdentityId,
    recipientEmail: BABRUN.recipientEmail,
    sequenceStep: 1,
    scheduledFor: window.scheduledForIso,
    timezone: BUSINESS_TZ,
    pastDuePolicy: PAST_DUE_POLICY.EXECUTE_WITHIN_WINDOW,
    maxLatenessMinutes: 30,
    authorizationSnapshot: {
      recipientEmail: BABRUN.recipientEmail,
      outreachAssetId: BABRUN.outreachAssetId,
      outreachAssetVersion: asset.outreachAssetVersion,
      sendingIdentityId: BABRUN.sendingIdentityId,
      scheduledFor: window.scheduledForIso,
      timezone: BUSINESS_TZ,
      sequenceStep: 1,
      subject: asset.subject,
      body: asset.body,
    },
  }, { scheduleStore, mailboxStore });

  report.schedulingEligibility = eligibility;
  if (!eligibility.eligible) {
    report.verdict = 'SCHEDULING_ELIGIBILITY_FAILED';
    process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
    process.exitCode = 3;
    return;
  }

  const idempotencyKey = `babrun_canary_ipv4_002:${window.scheduledForIso}`;
  const auth = await authorizeScheduledOutreachSend({
    tenantId: BABRUN.tenantId,
    prospectId: BABRUN.prospectId,
    acquisitionKnowledgeObjectId: BABRUN.akObjectId,
    outreachAssetId: BABRUN.outreachAssetId,
    outreachAssetVersion: asset.outreachAssetVersion,
    sendingIdentityId: BABRUN.sendingIdentityId,
    recipientEmail: BABRUN.recipientEmail,
    sequenceStep: 1,
    scheduledFor: window.scheduledForIso,
    timezone: BUSINESS_TZ,
    subject: asset.subject,
    body: asset.body,
    authorizationSource: 'spec_248_ipv4_connectivity_canary',
    authorizedBy: 'jacob@gopulseforge.com',
    pastDuePolicy: PAST_DUE_POLICY.EXECUTE_WITHIN_WINDOW,
    maxLatenessMinutes: 30,
    idempotencyKey,
  }, { pool });

  report.schedule = {
    id: auth.schedule.id,
    scheduledFor: auth.schedule.scheduledFor,
    scheduledLocalEt: window.localLabel,
    status: auth.schedule.status,
    idempotencyKey: auth.schedule.idempotencyKey,
    created: auth.created,
    duplicate: auth.duplicate,
  };
  report.verdict = 'SMTP_IPV4_PATH_VERIFIED — NEW_KAYLEE_CANARY_SCHEDULED';
  process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
}

if (require.main === module) {
  main().catch(async (err) => {
    process.stderr.write(`${err.stack || err.message}\n`);
    process.exitCode = 1;
  }).finally(async () => {
    await pool.end().catch(() => {});
  });
}

module.exports = {
  BABRUN,
  nextSuitableBusinessWindow,
  verifySmtpConnectivity,
};
