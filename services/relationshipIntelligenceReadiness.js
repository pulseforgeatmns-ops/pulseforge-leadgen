'use strict';

/**
 * SPEC-064 — Relationship Intelligence operational readiness / acceptance.
 * Read-only report by default. Optional --accept exercises notes→summarize→commit
 * against RI tables only (never CRM / opportunities).
 */

const defaultPool = require('../db');
const {
  INTERACTION_TYPES,
  INSIGHT_KINDS,
  createMemoryStore,
  createPostgresStore,
  startRelationshipInterview,
  summarizeRelationshipInterview,
  commitRelationshipInterview,
  getInteraction,
  listInteractions,
  assertAllowedSql,
} = require('./relationshipIntelligenceInterview');

const REQUIRED_TABLES = Object.freeze([
  'relationship_interactions',
  'relationship_interaction_insights',
]);

const CRM_WATCH_TABLES = Object.freeze([
  'prospects',
  'companies',
  'opportunities',
  'customers',
  'jobs',
  'activity_log',
  'touchpoints',
  'commissions',
]);

const ACCEPTANCE_SOURCE = 'readiness_acceptance';
const ACCEPTANCE_NOTES =
  'Readiness acceptance fixture. Discovery-style debrief with the office manager. ' +
  'Main pain is inconsistent night cleaning. Goal is reliable coverage. ' +
  'Objection was switching cost. Budget around 2k/month. Timeline next month. ' +
  'Decision maker is the owner. Next step is a walkthrough Friday. ' +
  'We promised references and a written estimate. Prefer email before phone.';

function emptyMetrics() {
  return {
    totalInteractions: 0,
    draftCount: 0,
    reviewedCount: 0,
    committedCount: 0,
    insightsCount: 0,
    missingSummaryCount: 0,
    committedWithoutInsightsCount: 0,
    latestInteractionAt: null,
    commitFlowExercised: false,
    readyCommittedCount: 0,
  };
}

function emptyConstraintValidation() {
  return {
    interactionTypes: {
      expected: INTERACTION_TYPES.slice(),
      installed: [],
      missing: INTERACTION_TYPES.slice(),
      unexpected: [],
      ok: false,
    },
    insightKinds: {
      expected: INSIGHT_KINDS.slice(),
      installed: [],
      missing: INSIGHT_KINDS.slice(),
      unexpected: [],
      ok: false,
    },
    ok: false,
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

/**
 * Parse CHECK (... IN (...)) constraint text into sorted unique string values.
 * @param {string} def
 */
function parseCheckInValues(def) {
  const text = String(def || '');
  const match = text.match(/\bIN\s*\(([^)]+)\)/i);
  if (!match) return [];
  const values = [];
  const re = /'([^']*)'/g;
  let m;
  while ((m = re.exec(match[1]))) {
    values.push(m[1]);
  }
  return [...new Set(values)].sort();
}

function compareEnumSets(expected, installed) {
  const expectedSet = new Set(expected);
  const installedSet = new Set(installed);
  const missing = expected.filter((v) => !installedSet.has(v));
  const unexpected = installed.filter((v) => !expectedSet.has(v));
  return {
    expected: expected.slice(),
    installed: installed.slice(),
    missing,
    unexpected,
    ok: missing.length === 0 && unexpected.length === 0 && installed.length > 0,
  };
}

async function loadConstraintValidation(db) {
  const validation = emptyConstraintValidation();
  if (!(await tablePresent(db, 'relationship_interactions'))) {
    return validation;
  }

  const result = await db.query(`
    SELECT c.conrelid::regclass::text AS table_name,
           c.conname,
           pg_get_constraintdef(c.oid) AS definition
      FROM pg_constraint c
     WHERE c.contype = 'c'
       AND c.conrelid = ANY (
         ARRAY[
           to_regclass('public.relationship_interactions'),
           to_regclass('public.relationship_interaction_insights')
         ]::regclass[]
       )
  `);

  let typeInstalled = [];
  let kindInstalled = [];
  for (const row of result.rows) {
    const def = String(row.definition || '');
    const values = parseCheckInValues(def);
    if (/interaction_type/i.test(def) || /interaction_type/i.test(row.conname || '')) {
      typeInstalled = values;
    }
    if (/\bkind\b/i.test(def) || /insight.*kind|kind/i.test(row.conname || '')) {
      // Prefer the insights table kind constraint when both match loosely.
      if (String(row.table_name).includes('insight') || /\bkind\b/i.test(def)) {
        kindInstalled = values.length ? values : kindInstalled;
      }
    }
  }

  // Fallback: if kind constraint not found via name heuristics, pick non-type IN lists on insights table.
  if (!kindInstalled.length) {
    for (const row of result.rows) {
      if (!String(row.table_name).includes('insight')) continue;
      const values = parseCheckInValues(row.definition);
      if (values.includes('pain') || values.includes('context')) {
        kindInstalled = values;
        break;
      }
    }
  }

  validation.interactionTypes = compareEnumSets(INTERACTION_TYPES, typeInstalled);
  validation.insightKinds = compareEnumSets(INSIGHT_KINDS, kindInstalled);
  validation.ok = validation.interactionTypes.ok && validation.insightKinds.ok;
  return validation;
}

async function loadCorpusMetrics(db) {
  const metrics = emptyMetrics();
  const counts = await db.query(`
    SELECT
      (SELECT COUNT(*)::int FROM relationship_interactions) AS total_interactions,
      (SELECT COUNT(*)::int FROM relationship_interactions WHERE status = 'draft') AS draft_count,
      (SELECT COUNT(*)::int FROM relationship_interactions WHERE status = 'reviewed') AS reviewed_count,
      (SELECT COUNT(*)::int FROM relationship_interactions WHERE status = 'committed') AS committed_count,
      (SELECT COUNT(*)::int FROM relationship_interaction_insights) AS insights_count,
      (
        SELECT COUNT(*)::int FROM relationship_interactions
         WHERE raw_summary IS NULL
            OR BTRIM(COALESCE(raw_summary, '')) = ''
            OR structured_summary IS NULL
            OR structured_summary = '{}'::jsonb
      ) AS missing_summary_count,
      (
        SELECT COUNT(*)::int
          FROM relationship_interactions i
         WHERE i.status = 'committed'
           AND NOT EXISTS (
             SELECT 1 FROM relationship_interaction_insights x
              WHERE x.interaction_id = i.id
           )
      ) AS committed_without_insights_count,
      (
        SELECT MAX(occurred_at) FROM relationship_interactions
      ) AS latest_interaction_at,
      (
        SELECT COUNT(*)::int
          FROM relationship_interactions i
         WHERE i.status = 'committed'
           AND i.raw_summary IS NOT NULL
           AND BTRIM(i.raw_summary) <> ''
           AND i.structured_summary IS NOT NULL
           AND i.structured_summary <> '{}'::jsonb
           AND EXISTS (
             SELECT 1 FROM relationship_interaction_insights x
              WHERE x.interaction_id = i.id
           )
      ) AS ready_committed_count
  `);

  const row = counts.rows[0] || {};
  metrics.totalInteractions = Number(row.total_interactions || 0);
  metrics.draftCount = Number(row.draft_count || 0);
  metrics.reviewedCount = Number(row.reviewed_count || 0);
  metrics.committedCount = Number(row.committed_count || 0);
  metrics.insightsCount = Number(row.insights_count || 0);
  metrics.missingSummaryCount = Number(row.missing_summary_count || 0);
  metrics.committedWithoutInsightsCount = Number(row.committed_without_insights_count || 0);
  metrics.readyCommittedCount = Number(row.ready_committed_count || 0);
  metrics.latestInteractionAt = row.latest_interaction_at
    ? new Date(row.latest_interaction_at).toISOString()
    : null;
  metrics.commitFlowExercised = metrics.committedCount > 0;
  return metrics;
}

async function snapshotCrmCounts(db) {
  const counts = {};
  for (const table of CRM_WATCH_TABLES) {
    const present = await tablePresent(db, table);
    if (!present) {
      counts[table] = { present: false, count: null };
      continue;
    }
    // Read-only COUNT — never mutate CRM.
    const result = await db.query(`SELECT COUNT(*)::int AS n FROM ${table}`);
    counts[table] = { present: true, count: Number(result.rows[0].n || 0) };
  }
  return counts;
}

function diffCrmCounts(before, after) {
  const mutated = [];
  const tables = {};
  for (const table of CRM_WATCH_TABLES) {
    const b = before && before[table];
    const a = after && after[table];
    const changed =
      b &&
      a &&
      b.present &&
      a.present &&
      Number(b.count) !== Number(a.count);
    tables[table] = {
      before: b ? b.count : null,
      after: a ? a.count : null,
      present: Boolean(a && a.present),
      changed: Boolean(changed),
    };
    if (changed) mutated.push(table);
  }
  return {
    detectable: true,
    mutated: mutated.length > 0,
    mutatedTables: mutated,
    tables,
  };
}

/**
 * Pure status derivation — exported for unit tests.
 */
function deriveReadinessStatus({
  tableReadiness,
  constraintValidation,
  metrics,
  queryError = null,
  crmMutation = null,
} = {}) {
  const blockers = [];
  const nextActions = [];

  if (queryError) {
    blockers.push(`corpus_query_failed: ${queryError}`);
    nextActions.push('Fix database connectivity / permissions, then re-run readiness');
    return { status: 'blocked', blockers, nextActions };
  }

  if (!tableReadiness || !tableReadiness.allPresent) {
    for (const table of (tableReadiness && tableReadiness.missing) || REQUIRED_TABLES) {
      blockers.push(`missing_table:${table}`);
    }
    nextActions.push(
      'Apply migrations/2026-08-04-relationship-intelligence-interview.sql'
    );
    return { status: 'blocked', blockers, nextActions };
  }

  if (constraintValidation && constraintValidation.ok === false) {
    if (constraintValidation.interactionTypes && !constraintValidation.interactionTypes.ok) {
      blockers.push('interaction_types_constraint_mismatch');
      if (constraintValidation.interactionTypes.missing.length) {
        blockers.push(
          `interaction_types_missing:${constraintValidation.interactionTypes.missing.join(',')}`
        );
      }
    }
    if (constraintValidation.insightKinds && !constraintValidation.insightKinds.ok) {
      blockers.push('insight_kinds_constraint_mismatch');
      if (constraintValidation.insightKinds.missing.length) {
        blockers.push(
          `insight_kinds_missing:${constraintValidation.insightKinds.missing.join(',')}`
        );
      }
    }
    nextActions.push(
      'Re-apply migrations/2026-08-04-relationship-intelligence-interview.sql and verify CHECK constraints'
    );
    return { status: 'blocked', blockers, nextActions };
  }

  if (crmMutation && crmMutation.mutated) {
    blockers.push(
      `crm_mutation_detected:${(crmMutation.mutatedTables || []).join(',') || 'unknown'}`
    );
    nextActions.push(
      'Investigate Relationship Intelligence write path — CRM/opportunity tables must not change'
    );
    return { status: 'blocked', blockers, nextActions };
  }

  const m = metrics || emptyMetrics();
  if (!m.commitFlowExercised || m.readyCommittedCount <= 0) {
    nextActions.push(
      'Run npm run relationship:intel:readiness -- --accept to create a safe notes-mode committed fixture'
    );
    nextActions.push(
      'Or: npm run relationship:intel:interview -- --type=discovery_call --notes="..." --commit'
    );
    return {
      status: 'partial',
      blockers: ['no_committed_interaction_with_summary_and_insights'],
      nextActions,
    };
  }

  if (m.committedWithoutInsightsCount > 0) {
    blockers.push(
      `committed_without_insights:${m.committedWithoutInsightsCount}`
    );
  }
  if (m.missingSummaryCount > 0) {
    blockers.push(`interactions_missing_summary:${m.missingSummaryCount}`);
  }

  return {
    status: 'ready',
    blockers,
    nextActions: [
      'Relationship Intelligence corpus has at least one committed interaction with summary + insights',
    ],
  };
}

/**
 * Safe acceptance fixture: notes → summarize → commit → query.
 * Uses injectable store (memory in unit tests; postgres in CLI --accept).
 *
 * @param {object} [options]
 */
async function runRelationshipIntelligenceAcceptance(options = {}) {
  const store = options.store || createMemoryStore();
  const opts = { store };
  const sqlLogBefore = Array.isArray(store.sqlLog) ? store.sqlLog.length : null;

  let crmBefore = null;
  let crmAfter = null;
  if (options.pool && typeof options.pool.query === 'function' && !options.skipCrmSnapshot) {
    crmBefore = await snapshotCrmCounts(options.pool);
  }

  const started = await startRelationshipInterview(
    {
      interactionType: options.interactionType || 'discovery_call',
      companyId: options.companyId || 'readiness-demo-company',
      contactId: options.contactId || null,
      opportunityId: options.opportunityId || null,
      clientId: options.clientId != null ? options.clientId : null,
      notes: options.notes || ACCEPTANCE_NOTES,
      source: ACCEPTANCE_SOURCE,
      occurredAt: options.occurredAt || new Date().toISOString(),
    },
    opts
  );

  const draft = await summarizeRelationshipInterview(started.interviewId, opts);
  const committed = await commitRelationshipInterview(started.interviewId, opts);
  const queried = await getInteraction(started.interviewId, opts);
  const listed = await listInteractions({ status: 'committed' }, opts);

  if (crmBefore) {
    crmAfter = await snapshotCrmCounts(options.pool);
  }

  const sqlLog = Array.isArray(store.sqlLog) ? store.sqlLog.slice(sqlLogBefore || 0) : [];
  const sqlTables = [...new Set(sqlLog.map((e) => e.table).filter(Boolean))];
  const forbiddenSql = sqlTables.filter((t) => CRM_WATCH_TABLES.includes(t));

  let crmMutation = {
    detectable: Boolean(crmBefore && crmAfter) || sqlLog.length > 0,
    mutated: forbiddenSql.length > 0,
    mutatedTables: forbiddenSql.slice(),
    tables: {},
  };
  if (crmBefore && crmAfter) {
    const diff = diffCrmCounts(crmBefore, crmAfter);
    crmMutation = {
      detectable: true,
      mutated: diff.mutated || forbiddenSql.length > 0,
      mutatedTables: [...new Set([...(diff.mutatedTables || []), ...forbiddenSql])],
      tables: diff.tables,
    };
  }

  const queryable =
    queried &&
    queried.status === 'committed' &&
    Array.isArray(queried.insights) &&
    queried.insights.length > 0 &&
    listed.some((row) => row.id === started.interviewId);

  return {
    ok: queryable && !crmMutation.mutated,
    interviewId: started.interviewId,
    draftStatus: draft.status,
    committedStatus: committed.status,
    insightCount: (committed.insights || []).length,
    queryable,
    crmMutation,
    sqlTables,
    payload: committed,
  };
}

/**
 * @param {object} [options]
 * @param {object} [options.pool]
 * @param {object|null} [options.acceptance] — result of runRelationshipIntelligenceAcceptance
 * @param {object|null} [options.crmMutation]
 */
async function buildRelationshipIntelReadinessReport({
  pool = defaultPool,
  acceptance = null,
  crmMutation = null,
} = {}) {
  const generatedAt = new Date().toISOString();

  let tableReadiness;
  try {
    tableReadiness = await checkTableReadiness(pool);
  } catch (err) {
    const message = err && err.message ? String(err.message) : 'table_check_failed';
    const derived = deriveReadinessStatus({
      tableReadiness: { tables: {}, missing: REQUIRED_TABLES.slice(), allPresent: false },
      constraintValidation: emptyConstraintValidation(),
      metrics: emptyMetrics(),
      queryError: message,
    });
    return {
      ok: true,
      kind: 'relationship_intelligence_readiness',
      generatedAt,
      status: derived.status,
      tableReadiness: {
        tables: Object.fromEntries(REQUIRED_TABLES.map((t) => [t, false])),
        missing: REQUIRED_TABLES.slice(),
        allPresent: false,
      },
      constraintValidation: emptyConstraintValidation(),
      metrics: emptyMetrics(),
      crmMutation: crmMutation || { detectable: false, mutated: false, mutatedTables: [], tables: {} },
      acceptance: acceptance || null,
      blockers: derived.blockers,
      nextActions: derived.nextActions,
      internal: true,
    };
  }

  let constraintValidation = emptyConstraintValidation();
  let metrics = emptyMetrics();
  let queryError = null;

  if (tableReadiness.allPresent) {
    try {
      constraintValidation = await loadConstraintValidation(pool);
      metrics = await loadCorpusMetrics(pool);
    } catch (err) {
      queryError = err && err.message ? String(err.message) : 'corpus_query_failed';
    }
  }

  const resolvedCrm =
    crmMutation ||
    (acceptance && acceptance.crmMutation) || {
      detectable: false,
      mutated: false,
      mutatedTables: [],
      tables: {},
      note: 'No CRM snapshot taken (read-only readiness). Use --accept to compare CRM counts.',
    };

  const derived = deriveReadinessStatus({
    tableReadiness,
    constraintValidation,
    metrics,
    queryError,
    crmMutation: resolvedCrm.mutated ? resolvedCrm : null,
  });

  // Code-level CRM guard still present.
  let crmGuardOk = true;
  try {
    assertAllowedSql('UPDATE prospects SET status = $1 WHERE id = $2');
    crmGuardOk = false;
  } catch (_) {
    crmGuardOk = true;
  }

  const blockers = derived.blockers.slice();
  const nextActions = derived.nextActions.slice();
  if (!crmGuardOk) {
    blockers.push('crm_sql_allowlist_inactive');
    nextActions.push('Restore assertAllowedSql CRM deny list in relationshipIntelligenceInterview');
  }

  return {
    ok: true,
    kind: 'relationship_intelligence_readiness',
    generatedAt,
    status: crmGuardOk ? derived.status : 'blocked',
    tableReadiness,
    constraintValidation,
    metrics,
    crmMutation: resolvedCrm,
    crmGuardOk,
    acceptance: acceptance || null,
    blockers: crmGuardOk ? blockers : [...blockers],
    nextActions,
    enums: {
      interactionTypes: INTERACTION_TYPES.slice(),
      insightKinds: INSIGHT_KINDS.slice(),
    },
    internal: true,
  };
}

function formatReadinessReport(report) {
  const m = report.metrics || emptyMetrics();
  const cv = report.constraintValidation || emptyConstraintValidation();
  const lines = [
    'Relationship Intelligence Readiness (SPEC-064)',
    `Status: ${report.status}`,
    `Generated: ${report.generatedAt}`,
    '',
    'Tables:',
    ...REQUIRED_TABLES.map((name) => {
      const present =
        report.tableReadiness && report.tableReadiness.tables
          ? report.tableReadiness.tables[name]
          : false;
      return `  ${present ? 'OK' : 'MISSING'}  ${name}`;
    }),
    '',
    `Interaction types constraint: ${cv.interactionTypes && cv.interactionTypes.ok ? 'OK' : 'FAIL'}`,
    `Insight kinds constraint: ${cv.insightKinds && cv.insightKinds.ok ? 'OK' : 'FAIL'}`,
    '',
    `Total interactions: ${Number(m.totalInteractions || 0).toLocaleString('en-US')}`,
    `Draft / reviewed / committed: ${m.draftCount} / ${m.reviewedCount} / ${m.committedCount}`,
    `Insights: ${Number(m.insightsCount || 0).toLocaleString('en-US')}`,
    `Missing summary: ${m.missingSummaryCount}`,
    `Committed with zero insights: ${m.committedWithoutInsightsCount}`,
    `Ready committed (summary + insights): ${m.readyCommittedCount}`,
    `Latest interaction: ${m.latestInteractionAt || '(none)'}`,
    `Commit flow exercised: ${m.commitFlowExercised ? 'yes' : 'no'}`,
    `CRM SQL guard active: ${report.crmGuardOk === false ? 'no' : 'yes'}`,
    `CRM mutation detected: ${
      report.crmMutation && report.crmMutation.detectable
        ? report.crmMutation.mutated
          ? `yes (${(report.crmMutation.mutatedTables || []).join(', ')})`
          : 'no'
        : 'not checked'
    }`,
  ];

  if (report.acceptance) {
    lines.push(
      '',
      `Acceptance fixture: ${report.acceptance.ok ? 'pass' : 'fail'}`,
      `  interviewId: ${report.acceptance.interviewId || '(none)'}`,
      `  queryable: ${report.acceptance.queryable ? 'yes' : 'no'}`,
      `  insights: ${report.acceptance.insightCount || 0}`
    );
  }

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

/**
 * Metrics from an in-memory store (unit tests / offline acceptance).
 */
function metricsFromMemoryStore(store) {
  // Memory store keeps private Maps; re-query via public list APIs is enough for tests
  // that build reports after acceptance. For pure unit tests of derive*, use emptyMetrics.
  void store;
  return emptyMetrics();
}

module.exports = {
  REQUIRED_TABLES,
  CRM_WATCH_TABLES,
  ACCEPTANCE_SOURCE,
  ACCEPTANCE_NOTES,
  emptyMetrics,
  emptyConstraintValidation,
  tablePresent,
  checkTableReadiness,
  parseCheckInValues,
  compareEnumSets,
  loadConstraintValidation,
  loadCorpusMetrics,
  snapshotCrmCounts,
  diffCrmCounts,
  deriveReadinessStatus,
  runRelationshipIntelligenceAcceptance,
  buildRelationshipIntelReadinessReport,
  formatReadinessReport,
  metricsFromMemoryStore,
  createMemoryStore,
  createPostgresStore,
};
