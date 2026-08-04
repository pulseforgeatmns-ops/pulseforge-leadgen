'use strict';

/**
 * SPEC-068 — Market Intelligence ingestion preflight (read-only).
 * Verifies Gmail credentials, label existence, and labeled-message discoverability
 * before dry-run or real import. No DB writes.
 */

const { buildLabelQuery } = require('../utils/marketEmailParse');
const gmailClient = require('../utils/gmailClient');

function emptyCheck(ok = false) {
  return { ok };
}

/**
 * Probe Gmail auth + MARKET_INTEL (or custom) label + discovery for lookback window.
 *
 * @param {object} [options]
 * @param {string} [options.label='MARKET_INTEL']
 * @param {number} [options.days=365]
 * @param {number} [options.limit=1000]
 * @param {boolean} [options.requireMessages=false] — when true, zero discovered messages fails
 * @param {object} [options.deps] — inject Gmail helpers for tests
 */
async function preflightMarketIntelIngestion(options = {}) {
  const label = String(options.label || 'MARKET_INTEL').trim() || 'MARKET_INTEL';
  const days = Math.max(1, Number(options.days) || 365);
  const limit = Math.max(1, Number(options.limit) || 1000);
  const requireMessages = Boolean(options.requireMessages);
  const deps = options.deps || {};

  const loadOAuthCredentials = deps.loadOAuthCredentials || gmailClient.loadOAuthCredentials;
  const createGmailClient = deps.createGmailClient || gmailClient.createGmailClient;
  const listGmailLabels = deps.listGmailLabels || gmailClient.listGmailLabels;
  const findLabelByName = deps.findLabelByName || gmailClient.findLabelByName;
  const countMatchingMessages = deps.countMatchingMessages || gmailClient.countMatchingMessages;

  const checks = {
    credentials: emptyCheck(false),
    auth: emptyCheck(false),
    label: emptyCheck(false),
    discovery: emptyCheck(false),
  };
  const blockers = [];
  const warnings = [];
  const nextActions = [];
  const query = buildLabelQuery({ label, days });

  try {
    loadOAuthCredentials();
    checks.credentials = {
      ok: true,
      detail: 'OAuth client JSON / GOOGLE_CLIENT_ID+SECRET available',
    };
  } catch (err) {
    const message = err && err.message ? String(err.message) : 'credentials_unavailable';
    checks.credentials = { ok: false, error: message };
    blockers.push(`gmail_credentials_unavailable: ${message}`);
    nextActions.push(
      'Set GMAIL_CREDENTIALS (full OAuth client JSON starting with {"web": or {"installed":) or GOOGLE_CLIENT_ID + GOOGLE_CLIENT_SECRET'
    );
    return finalize({ label, days, limit, query, checks, blockers, warnings, nextActions });
  }

  let gmail;
  try {
    gmail = await createGmailClient();
    checks.auth = { ok: true, detail: 'Gmail API client authenticated (readonly)' };
  } catch (err) {
    const message = err && err.message ? String(err.message) : 'auth_failed';
    checks.auth = { ok: false, error: message };
    blockers.push(`gmail_auth_unavailable: ${message}`);
    nextActions.push(
      'Set GMAIL_TOKEN (token JSON) or RILEY_ACCESS_TOKEN + RILEY_REFRESH_TOKEN, then re-run preflight'
    );
    return finalize({ label, days, limit, query, checks, blockers, warnings, nextActions });
  }

  try {
    const labels = await listGmailLabels(gmail);
    const match = findLabelByName(labels, label);
    if (!match) {
      const sample = labels
        .map((row) => row.name)
        .filter(Boolean)
        .slice(0, 12);
      checks.label = {
        ok: false,
        label,
        error: `label_not_found:${label}`,
        availableLabelsSample: sample,
      };
      blockers.push(`gmail_label_missing: ${label}`);
      nextActions.push(
        `Create a Gmail label named exactly "${label}", apply it to competitor/vendor marketing emails, then re-run preflight`
      );
      return finalize({ label, days, limit, query, checks, blockers, warnings, nextActions });
    }
    checks.label = {
      ok: true,
      label,
      labelId: match.id || null,
      labelType: match.type || null,
    };
  } catch (err) {
    const message = err && err.message ? String(err.message) : 'label_list_failed';
    checks.label = { ok: false, label, error: message };
    blockers.push(`gmail_label_check_failed: ${message}`);
    nextActions.push('Confirm Gmail readonly scope and mailbox access, then re-run preflight');
    return finalize({ label, days, limit, query, checks, blockers, warnings, nextActions });
  }

  try {
    const discovery = await countMatchingMessages({ query, limit, gmail });
    const discoveredCount = Number(discovery.discoveredCount || 0);
    const discoveryOk = discoveredCount > 0 || !requireMessages;
    checks.discovery = {
      ok: discoveryOk,
      query,
      discoveredCount,
      cappedByLimit: Boolean(discovery.cappedByLimit),
      sampleIds: discovery.sampleIds || [],
    };
    if (discoveredCount <= 0) {
      const emptyMsg = 'gmail_label_empty: no labeled messages in lookback window';
      if (requireMessages) {
        blockers.push(emptyMsg);
      } else {
        warnings.push(emptyMsg);
      }
      nextActions.push(
        `Apply label "${label}" to marketing emails from the last ${days} days (or widen --days), then re-run preflight`
      );
    }
  } catch (err) {
    const message = err && err.message ? String(err.message) : 'discovery_failed';
    checks.discovery = { ok: false, query, error: message };
    blockers.push(`gmail_discovery_failed: ${message}`);
    nextActions.push('Fix Gmail list permissions / query, then re-run preflight');
  }

  return finalize({ label, days, limit, query, checks, blockers, warnings, nextActions });
}

function finalize({ label, days, limit, query, checks, blockers, warnings, nextActions }) {
  const ok =
    Boolean(checks.credentials.ok)
    && Boolean(checks.auth.ok)
    && Boolean(checks.label.ok)
    && Boolean(checks.discovery.ok)
    && blockers.length === 0;

  if (ok && nextActions.length === 0) {
    nextActions.push(
      'Preflight passed. Run npm run market:intel:import -- --days=365 --label=MARKET_INTEL --limit=1000 --dry-run'
    );
  }

  return {
    ok,
    observationalOnly: true,
    internal: true,
    label,
    days,
    limit,
    query,
    checks,
    blockers,
    warnings,
    nextActions,
    generatedAt: new Date().toISOString(),
  };
}

function formatPreflightReport(report) {
  const lines = [
    'Market Intelligence Ingestion Preflight (SPEC-068)',
    `Status: ${report.ok ? 'pass' : 'fail'}`,
    `Generated: ${report.generatedAt}`,
    `Label: ${report.label}`,
    `Lookback days: ${report.days}`,
    `Query: ${report.query}`,
    '',
    'Checks:',
    `  credentials: ${report.checks.credentials.ok ? 'OK' : 'FAIL'}${
      report.checks.credentials.error ? ` — ${report.checks.credentials.error}` : ''
    }`,
    `  auth: ${report.checks.auth.ok ? 'OK' : 'FAIL'}${
      report.checks.auth.error ? ` — ${report.checks.auth.error}` : ''
    }`,
    `  label: ${report.checks.label.ok ? 'OK' : 'FAIL'}${
      report.checks.label.error ? ` — ${report.checks.label.error}` : ''
    }`,
    `  discovery: ${
      report.checks.discovery.ok
        ? `OK (${Number(report.checks.discovery.discoveredCount || 0).toLocaleString('en-US')} messages)`
        : `FAIL${report.checks.discovery.error ? ` — ${report.checks.discovery.error}` : ''}`
    }`,
  ];

  if (report.blockers && report.blockers.length) {
    lines.push('', 'Blockers:');
    for (const b of report.blockers) lines.push(`  - ${b}`);
  }
  if (report.warnings && report.warnings.length) {
    lines.push('', 'Warnings:');
    for (const w of report.warnings) lines.push(`  - ${w}`);
  }
  if (report.nextActions && report.nextActions.length) {
    lines.push('', 'Next actions:');
    for (const a of report.nextActions) lines.push(`  - ${a}`);
  }

  return lines.join('\n');
}

module.exports = {
  formatPreflightReport,
  preflightMarketIntelIngestion,
};
