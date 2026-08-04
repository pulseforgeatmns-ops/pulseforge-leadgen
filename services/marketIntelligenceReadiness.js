'use strict';

/**
 * SPEC-067 — Market Intelligence Operational Acceptance (read-only).
 * Reports whether Phase 1 MI corpus can be trusted — no scoring, recommendations,
 * CRM writes, or Max side effects.
 */

const defaultPool = require('../db');

const REQUIRED_TABLES = [
  'market_companies',
  'market_emails',
  'market_observations',
  'market_company_profiles',
  'market_intel_sync_state',
];

const EMAIL_EXTRACTION_READY_FLOOR = 50;
const PROFILE_REBUILD_READY_FLOOR = 50;

function roundPct(numerator, denominator) {
  if (!denominator || denominator <= 0) return 0;
  return Math.round((Number(numerator) / Number(denominator)) * 1000) / 10;
}

function emptyMetrics() {
  return {
    totalEmails: 0,
    totalObservations: 0,
    emailsWithObservations: 0,
    emailExtractionCoveragePct: 0,
    companiesObserved: 0,
    companiesWithObservations: 0,
    companyExtractionCoveragePct: 0,
    companiesWithProfiles: 0,
    profileRebuildCoveragePct: 0,
    unknownCompanyPresent: false,
    emailsAssignedToUnknown: 0,
    lastSyncState: null,
  };
}

async function tablePresent(db, tableName) {
  const result = await db.query(`SELECT to_regclass($1) AS name`, [`public.${tableName}`]);
  return Boolean(result.rows[0] && result.rows[0].name);
}

async function checkTableReadiness(db) {
  const tables = {};
  const missing = [];
  for (const name of REQUIRED_TABLES) {
    const present = await tablePresent(db, name);
    tables[name] = present;
    if (!present) missing.push(name);
  }
  return { tables, missing, allPresent: missing.length === 0 };
}

async function loadCorpusMetrics(db) {
  const metrics = emptyMetrics();

  const counts = await db.query(`
    SELECT
      (SELECT COUNT(*)::int FROM market_emails) AS total_emails,
      (SELECT COUNT(*)::int FROM market_observations) AS total_observations,
      (SELECT COUNT(DISTINCT email_id)::int FROM market_observations) AS emails_with_observations,
      (SELECT COUNT(DISTINCT company_id)::int FROM market_emails) AS companies_observed,
      (SELECT COUNT(DISTINCT company_id)::int FROM market_observations) AS companies_with_observations,
      (
        SELECT COUNT(*)::int
          FROM (
            SELECT DISTINCT e.company_id
              FROM market_emails e
              JOIN market_company_profiles p ON p.company_id = e.company_id
          ) profiled
      ) AS companies_with_profiles,
      (
        SELECT EXISTS(
          SELECT 1 FROM market_companies WHERE is_unknown = TRUE
        )
      ) AS unknown_company_present,
      (
        SELECT COUNT(*)::int
          FROM market_emails e
          JOIN market_companies c ON c.id = e.company_id
         WHERE c.is_unknown = TRUE
      ) AS emails_assigned_to_unknown
  `);

  const row = counts.rows[0] || {};
  metrics.totalEmails = Number(row.total_emails || 0);
  metrics.totalObservations = Number(row.total_observations || 0);
  metrics.emailsWithObservations = Number(row.emails_with_observations || 0);
  metrics.companiesObserved = Number(row.companies_observed || 0);
  metrics.companiesWithObservations = Number(row.companies_with_observations || 0);
  metrics.companiesWithProfiles = Number(row.companies_with_profiles || 0);
  metrics.unknownCompanyPresent = Boolean(row.unknown_company_present);
  metrics.emailsAssignedToUnknown = Number(row.emails_assigned_to_unknown || 0);
  metrics.emailExtractionCoveragePct = roundPct(
    metrics.emailsWithObservations,
    metrics.totalEmails
  );
  metrics.companyExtractionCoveragePct = roundPct(
    metrics.companiesWithObservations,
    metrics.companiesObserved
  );
  metrics.profileRebuildCoveragePct = roundPct(
    metrics.companiesWithProfiles,
    metrics.companiesObserved
  );

  const sync = await db.query(
    `SELECT id, label, days, import_intent, last_synced_at, last_run_stats, updated_at
       FROM market_intel_sync_state
      WHERE id = 'default'
      LIMIT 1`
  );
  if (sync.rows[0]) {
    const s = sync.rows[0];
    metrics.lastSyncState = {
      id: s.id,
      label: s.label,
      days: s.days,
      importIntent: s.import_intent || null,
      sourceIntent: s.import_intent || null,
      lastSyncedAt: s.last_synced_at ? new Date(s.last_synced_at).toISOString() : null,
      lastRunStats: s.last_run_stats || {},
      updatedAt: s.updated_at ? new Date(s.updated_at).toISOString() : null,
    };
  }

  return metrics;
}

/**
 * Pure status derivation — exported for unit tests.
 */
function deriveReadinessStatus({ tableReadiness, metrics, queryError = null }) {
  const blockers = [];
  const nextActions = [];

  if (queryError) {
    blockers.push(`corpus_query_failed: ${queryError}`);
    nextActions.push('Fix database connectivity / permissions, then re-run readiness');
    return { status: 'blocked', blockers, nextActions };
  }

  if (!tableReadiness.allPresent) {
    for (const table of tableReadiness.missing) {
      blockers.push(`missing_table:${table}`);
    }
    nextActions.push(
      'Apply migrations/2026-08-01-market-intelligence-ingestion.sql and migrations/2026-08-03-market-intelligence-foundation.sql'
    );
    return { status: 'blocked', blockers, nextActions };
  }

  if (!metrics.totalEmails || metrics.totalEmails <= 0) {
    blockers.push('market_email_corpus_empty');
    nextActions.push('Run npm run market:intel:import to ingest labeled marketing emails');
    return { status: 'blocked', blockers, nextActions };
  }

  const lastSyncedAt = metrics.lastSyncState && metrics.lastSyncState.lastSyncedAt;
  let status = 'ready';

  if (metrics.emailExtractionCoveragePct < EMAIL_EXTRACTION_READY_FLOOR) {
    status = 'partial';
    blockers.push(
      `email_extraction_coverage_below_floor: ${metrics.emailExtractionCoveragePct}% < ${EMAIL_EXTRACTION_READY_FLOOR}%`
    );
    nextActions.push('Run npm run market:intel:extract to populate market_observations');
  }

  if (metrics.profileRebuildCoveragePct < PROFILE_REBUILD_READY_FLOOR) {
    status = 'partial';
    blockers.push(
      `profile_rebuild_coverage_below_floor: ${metrics.profileRebuildCoveragePct}% < ${PROFILE_REBUILD_READY_FLOOR}%`
    );
    nextActions.push(
      'Run npm run market:intel:extract (profile rebuild is on by default) or rebuild profiles for observed companies'
    );
  }

  if (!lastSyncedAt) {
    status = 'partial';
    blockers.push('sync_state_missing_last_synced_at');
    nextActions.push('Confirm import wrote market_intel_sync_state, or re-run npm run market:intel:import');
  }

  if (status === 'ready') {
    nextActions.push('Phase 1 corpus looks trustworthy for observational query / Max read-only use');
  } else if (nextActions.length === 0) {
    nextActions.push('Review blockers and re-run npm run market:intel:readiness');
  }

  return { status, blockers, nextActions };
}

/**
 * Build full operational readiness report.
 *
 * @param {object} [options]
 * @param {object} [options.pool]
 * @param {object|null} [options.gmailPreflight] — optional SPEC-068 probe result to merge
 */
async function buildMarketIntelReadinessReport({ pool = defaultPool, gmailPreflight = null } = {}) {
  const generatedAt = new Date().toISOString();
  let tableReadiness;
  try {
    tableReadiness = await checkTableReadiness(pool);
  } catch (err) {
    const message = err && err.message ? String(err.message) : 'table_check_failed';
    const derived = deriveReadinessStatus({
      tableReadiness: { tables: {}, missing: REQUIRED_TABLES.slice(), allPresent: false },
      metrics: emptyMetrics(),
      queryError: message,
    });
    return {
      ok: true,
      generatedAt,
      status: derived.status,
      thresholds: {
        emailExtractionReadyFloor: EMAIL_EXTRACTION_READY_FLOOR,
        profileRebuildReadyFloor: PROFILE_REBUILD_READY_FLOOR,
      },
      tableReadiness: {
        tables: Object.fromEntries(REQUIRED_TABLES.map((t) => [t, false])),
        missing: REQUIRED_TABLES.slice(),
        allPresent: false,
      },
      metrics: emptyMetrics(),
      blockers: derived.blockers,
      nextActions: derived.nextActions,
      gmailPreflight: gmailPreflight || null,
      internal: true,
      observationalOnly: true,
    };
  }

  let metrics = emptyMetrics();
  let queryError = null;
  if (tableReadiness.allPresent) {
    try {
      metrics = await loadCorpusMetrics(pool);
    } catch (err) {
      queryError = err && err.message ? String(err.message) : 'corpus_query_failed';
    }
  }

  const derived = deriveReadinessStatus({ tableReadiness, metrics, queryError });
  const blockers = derived.blockers.slice();
  const nextActions = derived.nextActions.slice();
  let status = derived.status;

  if (gmailPreflight && gmailPreflight.ok === false) {
    status = 'blocked';
    for (const b of gmailPreflight.blockers || []) {
      if (!blockers.includes(b)) blockers.push(b);
    }
    for (const a of gmailPreflight.nextActions || []) {
      if (!nextActions.includes(a)) nextActions.push(a);
    }
    if (!(gmailPreflight.blockers || []).length) {
      blockers.push('gmail_ingestion_path_unavailable');
      nextActions.push('Run npm run market:intel:preflight and fix Gmail auth/label blockers');
    }
  }

  return {
    ok: true,
    generatedAt,
    status,
    thresholds: {
      emailExtractionReadyFloor: EMAIL_EXTRACTION_READY_FLOOR,
      profileRebuildReadyFloor: PROFILE_REBUILD_READY_FLOOR,
    },
    tableReadiness,
    metrics,
    blockers,
    nextActions,
    gmailPreflight: gmailPreflight || null,
    internal: true,
    observationalOnly: true,
  };
}

function formatReadinessReport(report) {
  const m = report.metrics || emptyMetrics();
  const lines = [
    `Market Intelligence Readiness (SPEC-067)`,
    `Status: ${report.status}`,
    `Generated: ${report.generatedAt}`,
    '',
    'Tables:',
    ...REQUIRED_TABLES.map((name) => {
      const present = report.tableReadiness && report.tableReadiness.tables
        ? report.tableReadiness.tables[name]
        : false;
      return `  ${present ? 'OK' : 'MISSING'}  ${name}`;
    }),
    '',
    `Imported emails: ${Number(m.totalEmails || 0).toLocaleString('en-US')}`,
    `Observations: ${Number(m.totalObservations || 0).toLocaleString('en-US')}`,
    `Email extraction coverage: ${m.emailExtractionCoveragePct}% (${m.emailsWithObservations}/${m.totalEmails})`,
    `Company extraction coverage: ${m.companyExtractionCoveragePct}% (${m.companiesWithObservations}/${m.companiesObserved})`,
    `Companies observed: ${Number(m.companiesObserved || 0).toLocaleString('en-US')}`,
    `Profile rebuild coverage: ${m.profileRebuildCoveragePct}% (${m.companiesWithProfiles}/${m.companiesObserved})`,
    `Unknown company present: ${m.unknownCompanyPresent ? 'yes' : 'no'}`,
    `Emails assigned to unknown: ${Number(m.emailsAssignedToUnknown || 0).toLocaleString('en-US')}`,
    `Last synced at: ${
      m.lastSyncState && m.lastSyncState.lastSyncedAt
        ? m.lastSyncState.lastSyncedAt
        : '(none)'
    }`,
    `Last sync import intent: ${
      m.lastSyncState && m.lastSyncState.importIntent
        ? m.lastSyncState.importIntent
        : '(none)'
    }`,
  ];

  if (report.blockers && report.blockers.length) {
    lines.push('', 'Blockers:');
    for (const b of report.blockers) lines.push(`  - ${b}`);
  }
  if (report.nextActions && report.nextActions.length) {
    lines.push('', 'Next actions:');
    for (const a of report.nextActions) lines.push(`  - ${a}`);
  }

  return lines.join('\n');
}

module.exports = {
  REQUIRED_TABLES,
  EMAIL_EXTRACTION_READY_FLOOR,
  PROFILE_REBUILD_READY_FLOOR,
  buildMarketIntelReadinessReport,
  checkTableReadiness,
  deriveReadinessStatus,
  emptyMetrics,
  formatReadinessReport,
  loadCorpusMetrics,
  roundPct,
  tablePresent,
};
