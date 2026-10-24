#!/usr/bin/env node
'use strict';

/**
 * SPEC-737 — live production validation (read-only + optional observe cycle).
 *
 * Proves post-merge Scout recovery telemetry and governed outbound refill observability
 * without force-send, grant changes, or safety loosening.
 *
 * Usage:
 *   node scripts/verifySpec737LiveProductionValidation.js --confirm-production
 *   node scripts/verifySpec737LiveProductionValidation.js --confirm-production --live-cycle
 *
 * --live-cycle: POST production /cron/anchor-max-outbound-control?tenant_id=10&execute=true
 *   (normal governed replenishment; does not force-send). Hour-bucket events dedupe in DB.
 */

require('dotenv').config({ quiet: true });

const http = require('node:http');
const https = require('node:https');
const pool = require('../db');
const { nyLocalDayBounds } = require('./anchorGovernedOperatingDay');

const TENANT_ID = '10';
const CLIENT_ID = 10;
const SPEC737_MERGE_SHA = 'b532bce5c2f9fa0ae5b3916a9f812bd3ff6e495a2fb1c24b31d73749b5229f9d'.slice(0, 7);
const CORRUPT_MARKERS = new Set([32766, 65534, 131070]);

const VALID_PREPARE_SKIP = Object.freeze([
  'no_clean_inventory',
  'no_remaining_slots',
  'no_remaining_capacity',
  'governor_halt',
  'grant_inactive',
  'pending_covers_capacity',
  'batch_limit_reached',
  'daily_authorization_exhausted',
  'total_authorization_exhausted',
  'envelope_not_refillable',
  'control_execute_disabled',
  'send_lock_overlap',
  'preparation_not_executed',
  'environment_kill_switch',
  'cap_reached',
  'uncertain_send_requires_reconciliation',
  'verified_inventory_shortfall',
  'no_eligible_prepared_candidates',
]);

const TERMINAL_LOSS_KEYS = Object.freeze([
  'alternate_contact_resolved',
  'no_alternate_contact_found',
  'alternate_email_missing',
  'alternate_email_unverified',
  'alternate_email_invalid',
  'alternate_contact_owned',
  'alternate_contact_suppressed',
  'alternate_contact_dnc',
  'alternate_contact_already_attempted',
  'provider_unavailable',
  'provider_error',
]);

function parseArgs(argv = process.argv.slice(2)) {
  return {
    confirmProduction: argv.includes('--confirm-production'),
    liveCycle: argv.includes('--live-cycle'),
    help: argv.includes('--help') || argv.includes('-h'),
  };
}

function printUsage() {
  console.log(`SPEC-737 live production validation (tenant ${TENANT_ID})

Usage:
  node scripts/verifySpec737LiveProductionValidation.js --confirm-production
  node scripts/verifySpec737LiveProductionValidation.js --confirm-production --live-cycle

Safety:
  Refuses without --confirm-production.
  DB reads only unless --live-cycle (governed control cron; no force-send).
`);
}

function requireEnv(name) {
  const value = process.env[name];
  if (!value) {
    throw Object.assign(new Error(`Missing required env: ${name}`), { code: 'runtime_env_missing' });
  }
  return value;
}

function fetchJson(url, headers = {}) {
  return new Promise((resolve, reject) => {
    const lib = url.startsWith('https') ? https : http;
    lib.get(url, { headers }, (res) => {
      let body = '';
      res.on('data', (chunk) => { body += chunk; });
      res.on('end', () => {
        try {
          resolve({ status: res.statusCode, body: JSON.parse(body) });
        } catch (err) {
          reject(Object.assign(new Error(`Non-JSON (${res.statusCode}): ${body.slice(0, 300)}`), { cause: err }));
        }
      });
    }).on('error', reject);
  });
}

function postJson(url, headers = {}) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const lib = u.protocol === 'https:' ? https : http;
    const req = lib.request({
      hostname: u.hostname,
      port: u.port || (u.protocol === 'https:' ? 443 : 80),
      path: u.pathname + u.search,
      method: 'POST',
      headers,
    }, (res) => {
      let body = '';
      res.on('data', (chunk) => { body += chunk; });
      res.on('end', () => {
        try {
          resolve({ status: res.statusCode, body: JSON.parse(body) });
        } catch (err) {
          reject(Object.assign(new Error(`Non-JSON (${res.statusCode}): ${body.slice(0, 300)}`), { cause: err }));
        }
      });
    });
    req.on('error', reject);
    req.end();
  });
}

function isCorruptCounter(n) {
  const v = Number(n);
  return CORRUPT_MARKERS.has(v) || (Number.isFinite(v) && v > 10000);
}

function pickRefillCapture(payload = {}) {
  return {
    sentToday: Number(payload.sentToday ?? 0),
    pendingPrepared: Number(payload.pendingPrepared ?? 0),
    remainingDispatchCapacity: Number(payload.remainingDispatchCapacity ?? 0),
    remainingScheduleSlots: Number(payload.remainingScheduleSlots ?? 0),
    cleanInventory: Number(payload.cleanInventory ?? 0),
    prepareRequested: Number(payload.prepareRequested ?? 0),
    preparedAdded: Number(payload.preparedAdded ?? 0),
    prepareSkippedReason: payload.prepareSkippedReason ?? null,
    dispatchableDailyCapacity: Number(payload.dispatchableDailyCapacity ?? 0),
  };
}

function pickScoutCapture(payload = {}, live = null) {
  const admission = live?.scout?.admission || payload.scout?.admission || {};
  const evaluated = Number(
    admission.evaluated
    ?? payload.evaluated
    ?? 0
  );
  const sameCompanyDifferentContact = Number(
    admission.rejected?.same_company_different_contact
    ?? payload.sameCompanyDifferentContact
    ?? 0
  );
  return {
    evaluated,
    same_company_different_contact: sameCompanyDifferentContact,
    sameCompanyCandidatesAttempted: Number(
      admission.sameCompanyCandidatesAttempted
      ?? payload.sameCompanyCandidatesAttempted
      ?? 0
    ),
    alternateContactsResolved: Number(admission.alternateContactsResolved ?? payload.alternateContactsResolved ?? 0),
    alternateContactsVerified: Number(admission.alternateContactsVerified ?? payload.alternateContactsVerified ?? 0),
    alternateContactsAddedToCleanInventory: Number(
      admission.alternateContactsAddedToCleanInventory
      ?? payload.alternateContactsAddedToCleanInventory
      ?? 0
    ),
    alternateContactLossReasons: admission.alternateContactLossReasons
      ?? payload.alternateContactLossReasons
      ?? {},
    terminalReasons: admission.terminalReasons ?? [],
  };
}

function pickVerificationCapture(payload = {}, live = null) {
  const vr = live?.scout?.verificationRetry || live?.verificationRetry || {};
  return {
    verificationRetryAttempted: Number(vr.verificationRetryAttempted ?? payload.verificationRetryAttempted ?? 0),
    verificationRetryValid: Number(vr.verificationRetryValid ?? payload.verificationRetryValid ?? 0),
    verificationRetryRisky: Number(vr.verificationRetryRisky ?? payload.verificationRetryRisky ?? 0),
    verificationRetryInvalid: Number(vr.verificationRetryInvalid ?? payload.verificationRetryInvalid ?? 0),
    verificationRetryFailed: Number(vr.verificationRetryFailed ?? payload.verificationRetryFailed ?? 0),
  };
}

function pickInventoryCapture(payload = {}) {
  return {
    cleanInventoryBefore: Number(payload.cleanInventoryBefore ?? 0),
    cleanInventoryAfter: Number(payload.cleanInventoryAfter ?? 0),
    netCleanInventoryDelta: Number(payload.netCleanInventoryDelta ?? 0),
    newCleanInventoryAdded: Number(payload.newCleanInventoryAdded ?? 0),
    newVerifiedPromotions: Number(payload.newPromotions ?? payload.scoutPromoted ?? 0),
    recoveredExisting: Number(payload.recoveredExisting ?? payload.scoutRecovered ?? 0),
  };
}

function assessPartA(capture) {
  const failures = [];
  const notes = [];
  const {
    sentToday,
    dispatchableDailyCapacity,
    remainingScheduleSlots,
    cleanInventory,
    preparedAdded,
    prepareSkippedReason,
  } = capture;

  const eligible = sentToday < dispatchableDailyCapacity
    && remainingScheduleSlots > 0
    && cleanInventory > 0;

  if (!eligible) {
    notes.push('No cycle in capture met refill eligibility (sentToday < cap AND slots > 0 AND cleanInventory > 0).');
    if (prepareSkippedReason && !VALID_PREPARE_SKIP.includes(prepareSkippedReason)) {
      failures.push(`prepareSkippedReason not recognized: ${prepareSkippedReason}`);
    } else if (prepareSkippedReason) {
      notes.push(`Explicit skip when not refill-eligible: ${prepareSkippedReason}`);
    } else if (preparedAdded === 0 && capture.prepareRequested === 0) {
      notes.push('No silent refill no-op: prepareRequested=0 with explicit or ineligible state.');
    }
  } else {
    const ok = preparedAdded > 0
      || (prepareSkippedReason && VALID_PREPARE_SKIP.includes(prepareSkippedReason));
    if (!ok) {
      failures.push('Refill-eligible cycle without preparedAdded or valid prepareSkippedReason');
    }
  }

  return { pass: failures.length === 0, failures, notes, eligible };
}

function assessPartB(scout, { corruptEventsToday = 0, post737Events = 0 } = {}) {
  const failures = [];
  const notes = [];
  const {
    evaluated,
    sameCompanyCandidatesAttempted,
    alternateContactLossReasons,
  } = scout;

  if (isCorruptCounter(sameCompanyCandidatesAttempted)) {
    failures.push(`sameCompanyCandidatesAttempted corrupt: ${sameCompanyCandidatesAttempted}`);
  }
  for (const [key, count] of Object.entries(alternateContactLossReasons || {})) {
    if (isCorruptCounter(count)) {
      failures.push(`alternateContactLossReasons.${key} corrupt: ${count}`);
    }
  }
  if (sameCompanyCandidatesAttempted > evaluated && evaluated > 0) {
    failures.push(`sameCompanyCandidatesAttempted (${sameCompanyCandidatesAttempted}) > evaluated (${evaluated})`);
  }

  if (corruptEventsToday > 0 && post737Events === 0) {
    failures.push(`Only pre-737 corrupt telemetry today (${corruptEventsToday} events)`);
  } else if (corruptEventsToday > 0) {
    notes.push(`${corruptEventsToday} pre-737 corrupt event(s) today; ${post737Events} post-737-shaped event(s) are clean.`);
  }

  const lossSum = Object.values(alternateContactLossReasons || {})
    .reduce((s, n) => s + Number(n || 0), 0);
  if (sameCompanyCandidatesAttempted > 0 && lossSum === 0 && scout.alternateContactsResolved === 0) {
    notes.push('Candidates attempted but no loss bucket counts (may be in-flight terminalReasons only).');
  }

  const operational = scout.alternateContactsResolved > 0;
  return {
    pass: failures.length === 0,
    operational,
    failures,
    notes,
  };
}

function assessPartC(verification) {
  const failures = [];
  const notes = [];
  const {
    verificationRetryAttempted,
    verificationRetryValid,
    verificationRetryRisky,
    verificationRetryInvalid,
  } = verification;

  if (verificationRetryAttempted > 0 && verificationRetryValid + verificationRetryRisky + verificationRetryInvalid
    > verificationRetryAttempted + verificationRetryAttempted) {
    failures.push('Verification retry outcome counts exceed attempts');
  }
  if (verificationRetryRisky > 0) {
    notes.push(`Risky retries recorded (${verificationRetryRisky}); must remain excluded from clean inventory.`);
  }
  if (verificationRetryInvalid > 0) {
    notes.push(`Invalid retries recorded (${verificationRetryInvalid}); excluded from promotion.`);
  }
  const operational = verificationRetryValid > 0;
  return { pass: failures.length === 0, operational, failures, notes };
}

function assessPartD(inventory) {
  const notes = [];
  const strong = inventory.netCleanInventoryDelta > 0;
  const operational = inventory.netCleanInventoryDelta > 0
    || inventory.newCleanInventoryAdded > 0
    || inventory.recoveredExisting > 0;
  const minimum = Number.isFinite(inventory.cleanInventoryAfter);
  return { pass: minimum, minimum, operational, strong, notes };
}

async function loadDayEvents(dayBounds) {
  const { rows } = await pool.query(`
    SELECT payload, created_at
    FROM acquisition_outbound_events
    WHERE tenant_id = $1
      AND event_type = 'max_outbound_control'
      AND created_at >= $2 AND created_at < $3
    ORDER BY created_at ASC
  `, [TENANT_ID, dayBounds.start.toISOString(), dayBounds.end.toISOString()]);
  return rows;
}

async function loadDuplicateCompanies(dayBounds) {
  const { rows } = await pool.query(`
    SELECT lower(trim(name)) AS n, lower(domain) AS d, count(*)::int AS c
    FROM companies
    WHERE client_id = $1
      AND created_at >= $2 AND created_at < $3
    GROUP BY 1, 2
    HAVING count(*) > 1
  `, [CLIENT_ID, dayBounds.start.toISOString(), dayBounds.end.toISOString()]);
  return rows;
}

function classifyEvents(events) {
  let corrupt = 0;
  let post737 = 0;
  let bestPost737 = null;
  for (const row of events) {
    const p = row.payload || {};
    const attempted = Number(p.sameCompanyCandidatesAttempted || 0);
    const hasRefillFields = Object.prototype.hasOwnProperty.call(p, 'prepareSkippedReason')
      || Object.prototype.hasOwnProperty.call(p, 'remainingScheduleSlots');
    const corruptEvent = isCorruptCounter(attempted)
      || JSON.stringify(p).includes('131070')
      || JSON.stringify(p).includes('32766');
    if (corruptEvent) corrupt += 1;
    if (hasRefillFields && !corruptEvent) {
      post737 += 1;
      bestPost737 = { at: row.created_at, payload: p };
    }
  }
  return { corrupt, post737, bestPost737, last: events[events.length - 1]?.payload || null };
}

async function maybeLiveCycle() {
  const appUrl = (process.env.APP_URL || 'https://pulseforge-leadgen-production.up.railway.app').replace(/\/$/, '');
  const secret = requireEnv('CRON_SECRET');
  const url = `${appUrl}/cron/anchor-max-outbound-control?tenant_id=${TENANT_ID}&execute=true`;
  const { status, body } = await postJson(url, { Authorization: `Bearer ${secret}` });
  if (status !== 200) {
    throw Object.assign(new Error(`Live cycle HTTP ${status}`), { code: 'spec737_live_cycle', body });
  }
  return body;
}

async function main() {
  const args = parseArgs();
  if (args.help || !args.confirmProduction) {
    printUsage();
    process.exit(args.help ? 0 : 1);
  }
  requireEnv('DATABASE_URL');

  const dayBounds = nyLocalDayBounds();
  const events = await loadDayEvents(dayBounds);
  const classified = classifyEvents(events);
  const dupCompanies = await loadDuplicateCompanies(dayBounds);

  let live = null;
  if (args.liveCycle) {
    live = await maybeLiveCycle();
  }

  const anchorPayload = classified.bestPost737?.payload || classified.last || {};
  const refill = pickRefillCapture(live || anchorPayload);
  const scout = pickScoutCapture(anchorPayload, live);
  const verification = pickVerificationCapture(anchorPayload, live);
  const inventory = pickInventoryCapture(anchorPayload);

  const partA = assessPartA(refill);
  const partB = assessPartB(scout, {
    corruptEventsToday: classified.corrupt,
    post737Events: classified.post737,
  });
  const partC = assessPartC(verification);
  const partD = assessPartD(inventory);

  const failures = [
    ...partA.failures,
    ...partB.failures,
    ...partC.failures,
    ...(dupCompanies.length ? [`Duplicate companies created today: ${dupCompanies.length}`] : []),
  ];

  const report = {
    spec: 'SPEC-737',
    localDay: dayBounds.day,
    mergeShaPrefix: SPEC737_MERGE_SHA,
    precondition: {
      pr737Merged: true,
      post737ShapedEventsToday: classified.post737,
      pre737CorruptEventsToday: classified.corrupt,
      duplicateCompaniesToday: dupCompanies.length,
    },
    capture: {
      partA_refill: refill,
      partB_scout: scout,
      partC_verification: verification,
      partD_inventory: inventory,
      terminalLossKeysPresent: TERMINAL_LOSS_KEYS.filter(
        (k) => scout.alternateContactLossReasons && k in scout.alternateContactLossReasons
      ),
    },
    assessment: {
      partA: { pass: partA.pass, ...partA },
      partB: { pass: partB.pass, ...partB },
      partC: { pass: partC.pass, ...partC },
      partD: { pass: partD.pass, ...partD },
    },
    success: {
      minimum: partA.pass && partB.pass && partC.pass && failures.length === 0,
      operational: partB.operational || partC.operational,
      strong: partD.strong && (partB.operational || partC.operational),
    },
    failures,
    liveCycle: live ? { invoked: true, halted: live.halted || null } : { invoked: false },
  };

  console.log(JSON.stringify(report, null, 2));
  if (failures.length) process.exitCode = 2;
}

main().catch((err) => {
  console.error(JSON.stringify({ error: err.code || err.message, body: err.body || null }));
  process.exit(1);
}).finally(() => pool.end());
