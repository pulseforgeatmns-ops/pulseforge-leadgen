#!/usr/bin/env node
'use strict';

/**
 * Read-only audit: why CAPACITY queue items fail executeAnchorOneOutbound pre-send selection.
 * Never sends mail. Never mutates state.
 *
 * Railway:
 *   node scripts/auditAnchorCapacitySendability.js --confirm-production
 */

require('dotenv').config();

const amo = require('../packages/acquisition-mission');
const {
  SPECIALISTS,
  CONTRIBUTION_KINDS,
  validateProspectMessageBindings,
} = amo;
const pool = require('../db');
const { unwrapContributionPayload } = require('./validateAnchorCanonicalMission');
const { sendableQueueItems } = require('./executeAnchorOneOutbound');
const { invalidOutreachEmailReason } = require('../utils/emailGuard');

const TENANT_ID = '10';
const CLIENT_ID = 10;
const MISSION_ID = 'mission_ad7753b0-6def-441d-bb1a-3764656f5750';
const CAPACITY_ID = 'contrib_55e11312-3837-4485-b95a-58134dcd7601';
const VERIFIED_EMAIL_STATUSES = new Set(['valid', 'verified']);

const SENDABLE_PREDICATE = Object.freeze({
  script: 'executeAnchorOneOutbound.sendableQueueItems',
  rules: [
    'item must exist',
    'item.sendable !== false',
    'item.dnc !== true',
    'Boolean(String(item.email || "").trim()) — email must be present on the CAPACITY queue item',
  ],
  note: 'Does not consult CRM, resolveProspectAttributes, email_verified, or paige.sendable.',
});

const EXECUTE_PREDICATE = Object.freeze({
  script: 'OutboundExecution.buildExecutionBundle',
  rules: [
    'executionApproved + valid approval revision + governor not paused',
    'prospectId in approved queue',
    'Paige variant resolvable from VARIANTS contribution',
    'email on item OR resolveProspectAttributes(prospectId) when provided',
    'item.sendable !== false && item.dnc !== true',
  ],
  note: 'executeAnchorOneOutbound does not pass resolveProspectAttributes; email must be on item for both gates.',
});

function parseArgs(argv = process.argv.slice(2)) {
  const confirmProduction = argv.includes('--confirm-production');
  if (!confirmProduction) {
    throw Object.assign(new Error('Refusing without --confirm-production.'), { code: 'confirm_production_required' });
  }
  return { confirmProduction };
}

function spec212ItemValid(item, index, validation) {
  const itemId = String(item.id || item.candidateId || item.prospectId || item.companyId || '');
  const violation = (validation.violations || []).find((v) => v.index === index);
  if (violation) return { valid: false, reason: violation.reason || violation.message };
  if (!item.paige?.candidateId) return { valid: false, reason: 'missing_message_binding' };
  if (String(item.paige.candidateId) !== itemId) {
    return { valid: false, reason: 'candidate_id_mismatch' };
  }
  return { valid: true, reason: null };
}

function scriptRejectReason(item) {
  if (!item) return 'missing_item';
  if (item.sendable === false) return 'item.sendable=false';
  if (item.dnc === true) return 'item.dnc=true';
  const email = String(item.email || '').trim();
  if (!email) return 'missing_recipient_email_on_queue_item';
  return null;
}

async function loadProspect(db, prospectId) {
  if (!prospectId) return null;
  const { rows } = await db.query(
    `SELECT p.id, p.email, p.email_status, p.email_verified, p.do_not_contact,
            p.first_name, p.last_name, p.phone, p.vertical,
            c.id AS company_id, c.name AS company_name
       FROM prospects p
       LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
      WHERE p.client_id = $1
        AND p.id::text = $2
      LIMIT 1`,
    [CLIENT_ID, String(prospectId)]
  );
  return rows[0] || null;
}

async function run(options = {}) {
  parseArgs();
  if (!process.env.DATABASE_URL) {
    throw Object.assign(new Error('Missing DATABASE_URL'), { code: 'runtime_env_missing' });
  }

  const db = options.pool || pool;
  const cap = await db.query(
    `SELECT id, payload, at
       FROM acquisition_mission_contributions
      WHERE id = $1 AND tenant_id = $2 AND mission_id = $3
        AND specialist = 'emmett' AND kind = 'capacity'`,
    [CAPACITY_ID, TENANT_ID, MISSION_ID]
  );
  if (!cap.rows.length) {
    throw Object.assign(new Error(`CAPACITY ${CAPACITY_ID} not found.`), { code: 'capacity_not_found' });
  }

  const body = unwrapContributionPayload(cap.rows[0].payload) || {};
  const items = Array.isArray(body.queue?.items) ? body.queue.items : [];
  const spec212 = validateProspectMessageBindings(body);
  const sendable = sendableQueueItems(cap.rows[0].payload);

  const rows = [];
  for (let i = 0; i < items.length; i += 1) {
    const item = items[i];
    const prospectId = String(item.prospectId || item.id || '').trim() || null;
    const crm = await loadProspect(db, prospectId);
    const emailOnItem = String(item.email || '').trim() || null;
    const emailGuard = emailOnItem ? invalidOutreachEmailReason(emailOnItem) : 'missing_email';
    const crmEmail = crm?.email ? String(crm.email).trim() : null;
    const crmVerified = crm
      && crm.do_not_contact !== true
      && VERIFIED_EMAIL_STATUSES.has(String(crm.email_status || '').toLowerCase())
      && crm.email_verified === true
      && Boolean(crmEmail)
      && !invalidOutreachEmailReason(crmEmail);
    const reject = scriptRejectReason(item);
    rows.push({
      index: i,
      prospectId,
      company: item.company || item.name || crm?.company_name || null,
      itemSendable: item.sendable,
      paigeSendable: item.paige?.sendable ?? null,
      emailOnQueueItem: emailOnItem,
      emailGuardOnItem: emailGuard,
      dnc: item.dnc === true || crm?.do_not_contact === true,
      spec212: spec212ItemValid(item, i, spec212),
      crmEmailPresent: Boolean(crmEmail),
      crmEmailVerified: crmVerified,
      crmEmailStatus: crm?.email_status || null,
      scriptRejectReason: reject,
      sendableByScript: reject == null,
    });
  }

  const firstBlocker = (() => {
    if (!items.length) return 'empty_queue';
    if (sendable.length) return null;
    const reasons = rows.map((r) => r.scriptRejectReason).filter(Boolean);
    const counts = reasons.reduce((acc, r) => { acc[r] = (acc[r] || 0) + 1; return acc; }, {});
    const top = Object.entries(counts).sort((a, b) => b[1] - a[1])[0];
    return top ? top[0] : 'unknown';
  })();

  const upstreamExists = rows.some((r) => r.crmEmailVerified === true);
  const projected = rows.some((r) => r.emailOnQueueItem);

  return {
    missionId: MISSION_ID,
    capacityContributionId: CAPACITY_ID,
    sendablePredicate: SENDABLE_PREDICATE,
    executePredicate: EXECUTE_PREDICATE,
    queueItemCount: items.length,
    sendableCount: sendable.length,
    spec212: { valid: spec212.valid === true, violationCount: (spec212.violations || []).length },
    queueItems: rows,
    firstBlocker,
    rootCause:
      firstBlocker === 'missing_recipient_email_on_queue_item'
        ? 'CAPACITY queue items were materialized without item.email. buildMissionBoundCandidates copies email only from the frozen Scout DISCOVERY contribution (prospect?.email), not from live CRM. executeAnchorOneOutbound requires email on the queue item and does not pass resolveProspectAttributes.'
        : firstBlocker === 'item.sendable=false'
          ? 'Emmett queue cognition marked every item sendable=false at PREPARE (buildTodayQueue requires paige subject/body on the candidate at assess time).'
          : firstBlocker === 'item.dnc=true'
            ? 'Every queue item is DNC-flagged.'
            : 'See per-item reject reasons.',
    upstreamVerifiedEmailExists: upstreamExists,
    upstreamProjectedIntoCapacity: projected,
    smallestCanonicalFix:
      firstBlocker === 'missing_recipient_email_on_queue_item' && upstreamExists
        ? 'At PREPARE/REVISE, project verified CRM recipient email onto each mission-bound queue item (prospectId join to prospects where email_status in valid|verified, email_verified=true, do_not_contact=false). Then REVISE_PREPARED_OUTREACH to regenerate CAPACITY. Do not weaken sendable predicate or bypass verification.'
        : firstBlocker === 'missing_recipient_email_on_queue_item'
          ? 'Enrich mission-bound prospects to verified email in CRM, ensure Scout DISCOVERY snapshot includes email on matched prospects, then REVISE_PREPARED_OUTREACH to regenerate CAPACITY with projected emails.'
          : 'Regenerate CAPACITY after fixing the firstBlocker condition at PREPARE inputs.',
    scriptRerunUnchanged:
      firstBlocker === 'missing_recipient_email_on_queue_item'
        ? 'No — regenerate CAPACITY (REVISE_PREPARED_OUTREACH) after email projection fix; then executeAnchorOneOutbound.js can rerun unchanged.'
        : 'Fix PREPARE inputs, regenerate CAPACITY if needed, then rerun executeAnchorOneOutbound.js unchanged.',
    completedAt: new Date().toISOString(),
  };
}

module.exports = { run, SENDABLE_PREDICATE, scriptRejectReason };

if (require.main === module) {
  run()
    .then((report) => {
      console.log(JSON.stringify(report, null, 2));
      process.exitCode = report.sendableCount > 0 ? 0 : 2;
    })
    .catch((err) => {
      console.log(JSON.stringify({ error: { code: err.code, message: err.message } }, null, 2));
      process.exitCode = 1;
    })
    .finally(() => pool.end());
}
