'use strict';

/**
 * AUDIT-063 — Google Places API cost attribution + cognitive context.
 */

const { AsyncLocalStorage } = require('async_hooks');
const pool = require('../db');

const placesContext = new AsyncLocalStorage();

const PLACES_FEATURES = Object.freeze({
  LEADGEN: 'Leadgen',
  DISCOVERY: 'Discovery',
  CANDIDATE_REFRESH: 'Candidate Refresh',
  WARM_ROUTING: 'Warm Routing',
  DIAGNOSTIC: 'Diagnostic',
  GEOCODE: 'Geocode',
  SCRIPT: 'Script',
});

const PLACES_ENDPOINTS = Object.freeze({
  TEXT_SEARCH: 'text_search',
  PLACE_DETAILS: 'place_details',
  NEARBY_SEARCH: 'nearby_search',
  AUTOCOMPLETE: 'autocomplete',
  FIND_PLACE: 'find_place',
  SEARCH_TEXT_V1: 'search_text',
  PLACE_DETAILS_V1: 'place_details_v1',
  GEOCODE: 'geocode',
});

const TRIGGER_MODES = Object.freeze({
  SCHEDULER: 'scheduler',
  MANUAL: 'manual',
  OPERATOR: 'operator',
  CRON: 'cron',
  UNKNOWN: 'unknown',
});

const FEATURE_REPORT_LABELS = Object.freeze({
  [PLACES_FEATURES.LEADGEN]: 'leadgen.js',
  [PLACES_FEATURES.DISCOVERY]: 'Scout Discovery',
  [PLACES_FEATURES.CANDIDATE_REFRESH]: 'Candidate Refresh',
  [PLACES_FEATURES.WARM_ROUTING]: 'Warm Routing',
  [PLACES_FEATURES.DIAGNOSTIC]: 'Diagnostic',
  [PLACES_FEATURES.GEOCODE]: 'Geocode',
  [PLACES_FEATURES.SCRIPT]: 'Script',
});

const EVIDENCE_LABELS = Object.freeze({
  identity: 'Identity',
  portfolio_evidence: 'Portfolio',
  decision_makers: 'Decision Makers',
  growth_signals: 'Growth Signals',
  cleaning_signals: 'Cleaning Signals',
  reviews: 'Reviews',
  licensing: 'Licensing',
  social: 'Social',
  contact_path: 'Contact Path',
  buying_signals: 'Buying Signals',
});

const INVESTIGATION_TASK_LABELS = Object.freeze({
  identity: 'Collect business identities',
  portfolio_evidence: 'Collect property portfolio evidence',
  decision_makers: 'Collect organizational roles and decision makers',
  growth_signals: 'Collect growth signals',
  cleaning_signals: 'Collect cleaning responsibility signals',
  reviews: 'Collect customer reviews and service feedback',
  licensing: 'Collect licensing and registry records',
  social: 'Collect social presence',
  contact_path: 'Collect contact paths',
  buying_signals: 'Collect buying signals',
});

const ESTIMATED_COST_USD_BY_ENDPOINT = Object.freeze({
  [PLACES_ENDPOINTS.TEXT_SEARCH]: 0.032,
  [PLACES_ENDPOINTS.PLACE_DETAILS]: 0.017,
  [PLACES_ENDPOINTS.NEARBY_SEARCH]: 0.032,
  [PLACES_ENDPOINTS.AUTOCOMPLETE]: 0.00283,
  [PLACES_ENDPOINTS.FIND_PLACE]: 0.017,
  [PLACES_ENDPOINTS.SEARCH_TEXT_V1]: 0.032,
  [PLACES_ENDPOINTS.PLACE_DETAILS_V1]: 0.017,
  [PLACES_ENDPOINTS.GEOCODE]: 0.005,
});

const TEXT_ENDPOINTS = new Set([
  PLACES_ENDPOINTS.TEXT_SEARCH,
  PLACES_ENDPOINTS.SEARCH_TEXT_V1,
  PLACES_ENDPOINTS.NEARBY_SEARCH,
  PLACES_ENDPOINTS.FIND_PLACE,
]);

const DETAILS_ENDPOINTS = new Set([
  PLACES_ENDPOINTS.PLACE_DETAILS,
  PLACES_ENDPOINTS.PLACE_DETAILS_V1,
]);

const COST_CLASS_BY_ENDPOINT = Object.freeze({
  [PLACES_ENDPOINTS.TEXT_SEARCH]: 'Place Search — Text Search',
  [PLACES_ENDPOINTS.PLACE_DETAILS]: 'Place Details',
  [PLACES_ENDPOINTS.NEARBY_SEARCH]: 'Place Search — Nearby Search',
  [PLACES_ENDPOINTS.AUTOCOMPLETE]: 'Place Autocomplete',
  [PLACES_ENDPOINTS.FIND_PLACE]: 'Place Search — Find Place',
  [PLACES_ENDPOINTS.SEARCH_TEXT_V1]: 'Places API (New) — Text Search',
  [PLACES_ENDPOINTS.PLACE_DETAILS_V1]: 'Places API (New) — Place Details',
  [PLACES_ENDPOINTS.GEOCODE]: 'Geocoding API',
});

const SCHEMA_COLUMNS = [
  ['mission_stage', 'TEXT'],
  ['operator_id', 'TEXT'],
  ['hypothesis_id', 'TEXT'],
  ['hypothesis_label', 'TEXT'],
  ['evidence_requirement', 'TEXT'],
  ['investigation_task', 'TEXT'],
  ['provider_id', 'TEXT'],
  ['cache_hit', 'BOOLEAN'],
  ['cache_miss', 'BOOLEAN'],
  ['cache_age', 'INTEGER'],
  ['cache_key', 'TEXT'],
  ['cache_strategy', 'TEXT'],
  ['original_query', 'TEXT'],
  ['normalized_query', 'TEXT'],
  ['businesses_returned', 'INTEGER'],
  ['businesses_accepted', 'INTEGER'],
  ['candidates_created', 'INTEGER'],
  ['qualified_candidates', 'INTEGER'],
];

let schemaReady = null;
const schemaReadyByDb = new WeakMap();

function schemaCacheFor(db) {
  if (db && typeof db.query === 'function') {
    if (schemaReadyByDb.has(db)) return schemaReadyByDb.get(db);
    return null;
  }
  return schemaReady;
}

function setSchemaCache(db, ready) {
  if (db && typeof db.query === 'function') {
    schemaReadyByDb.set(db, ready);
    return;
  }
  schemaReady = ready;
}

function getPlacesContext() {
  return placesContext.getStore() || {};
}

function withPlacesContext(ctx, fn) {
  const parent = getPlacesContext();
  return placesContext.run({ ...parent, ...ctx }, fn);
}

function resolveCostClass(endpoint) {
  return COST_CLASS_BY_ENDPOINT[endpoint] || 'Unknown Places SKU';
}

function endpointFlags(endpoint) {
  return {
    isAutocomplete: endpoint === PLACES_ENDPOINTS.AUTOCOMPLETE,
    isNearbySearch: endpoint === PLACES_ENDPOINTS.NEARBY_SEARCH,
    isFindPlace: endpoint === PLACES_ENDPOINTS.FIND_PLACE,
  };
}

function normalizeTriggerMode(value) {
  const mode = String(value || '').trim().toLowerCase();
  if (Object.values(TRIGGER_MODES).includes(mode)) return mode;
  if (mode === 'scheduled') return TRIGGER_MODES.SCHEDULER;
  return TRIGGER_MODES.UNKNOWN;
}

function normalizePlacesQuery(query) {
  return String(query || '')
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\b(commercial|professional|local|near me|nearby|best|top)\b/g, ' ')
    .replace(/\b(nh|ma|vt|me|ct|ri|ny|nj|pa|wv|tn|tx|ca|fl|ga|nc|sc|va|md|de|oh|in|il|mi|wi|mn|ia|mo|ks|ne|sd|nd|ok|ar|la|ms|al|ky|co|ut|az|nm|nv|or|wa|id|mt|wy|ak|hi|dc)\b/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function evidenceRequirementLabel(value) {
  const key = String(value || '').trim();
  if (!key) return null;
  return EVIDENCE_LABELS[key] || key.replace(/_/g, ' ');
}

function investigationTaskLabel(evidenceType, taskId) {
  const key = String(evidenceType || '').trim();
  if (INVESTIGATION_TASK_LABELS[key]) return INVESTIGATION_TASK_LABELS[key];
  const id = String(taskId || '').trim();
  if (id.startsWith('task:')) {
    const fromId = id.slice(5);
    if (INVESTIGATION_TASK_LABELS[fromId]) return INVESTIGATION_TASK_LABELS[fromId];
  }
  if (key) return `Collect ${evidenceRequirementLabel(key) || key}`;
  return null;
}

function pickContextValue(record, ctx, ...keys) {
  for (const key of keys) {
    if (record[key] != null && record[key] !== '') return record[key];
    if (ctx[key] != null && ctx[key] !== '') return ctx[key];
  }
  return null;
}

function buildPlacesContextFromDiscovery(input = {}) {
  const searchDefinition = input.searchDefinition || {};
  const assignment = input.assignment || {};
  const task = input.task || {};
  const mission = input.mission || {};
  const evidenceRequest = searchDefinition.evidenceRequest || {};
  const evidenceType =
    assignment.evidenceType ||
    task.evidenceType ||
    evidenceRequest.evidenceType ||
    searchDefinition._evidenceType ||
    null;
  const taskId =
    task.id ||
    evidenceRequest.investigationTaskId ||
    searchDefinition._investigationTask ||
    (evidenceType ? `task:${evidenceType}` : null);
  const hypotheses = mission.hypotheses || searchDefinition.hypotheses || [];
  const hypothesisId =
    assignment.hypothesisId ||
    evidenceRequest.hypothesisId ||
    searchDefinition.hypothesisId ||
    (hypotheses[0] && hypotheses[0].id) ||
    null;
  const hypothesis =
    hypotheses.find((row) => row.id === hypothesisId) ||
    hypotheses[0] ||
    null;

  return {
    caller: input.caller || 'DiscoveryAdapters',
    feature: PLACES_FEATURES.DISCOVERY,
    tenantId:
      searchDefinition.tenantId ||
      searchDefinition.clientId ||
      mission.tenantId ||
      mission.clientId ||
      null,
    missionId: searchDefinition.missionId || searchDefinition.mission_id || mission.id || null,
    missionStage:
      searchDefinition.missionStage ||
      searchDefinition.mission_stage ||
      mission.stage ||
      mission.currentStage ||
      null,
    executionId:
      searchDefinition.executionId ||
      searchDefinition.execution_id ||
      mission.executionId ||
      null,
    operatorId:
      searchDefinition.operatorId ||
      searchDefinition.operator_id ||
      mission.operatorId ||
      mission.createdBy ||
      null,
    triggerMode:
      searchDefinition.triggerMode ||
      searchDefinition.trigger_mode ||
      TRIGGER_MODES.MANUAL,
    hypothesisId,
    hypothesisLabel: hypothesis?.text || hypothesis?.label || null,
    evidenceRequirement: evidenceType,
    investigationTask: investigationTaskLabel(evidenceType, taskId),
    providerId:
      assignment.providerId ||
      (Array.isArray(evidenceRequest.providerIds) && evidenceRequest.providerIds[0]) ||
      'google_places',
  };
}

function resolveEstimatedCostUsd(endpoint) {
  try {
    const raw = process.env.PLACES_ESTIMATED_COST_USD_JSON;
    if (raw) {
      const parsed = JSON.parse(raw);
      if (parsed && parsed[endpoint] != null) return Number(parsed[endpoint]);
    }
  } catch (_err) {
    /* defaults */
  }
  return ESTIMATED_COST_USD_BY_ENDPOINT[endpoint] || 0;
}

function countFromResult(endpoint, data) {
  if (!data || typeof data !== 'object') return 0;
  if (endpoint === PLACES_ENDPOINTS.TEXT_SEARCH) return Array.isArray(data.results) ? data.results.length : 0;
  if (endpoint === PLACES_ENDPOINTS.SEARCH_TEXT_V1) return Array.isArray(data.places) ? data.places.length : 0;
  if (DETAILS_ENDPOINTS.has(endpoint)) return data.result || data.displayName || data.id ? 1 : 0;
  if (endpoint === PLACES_ENDPOINTS.GEOCODE) return Array.isArray(data.results) ? data.results.length : 0;
  return 0;
}

function buildAttributionRow(record = {}, ctx = {}) {
  const endpoint = record.endpoint || PLACES_ENDPOINTS.TEXT_SEARCH;
  const flags = endpointFlags(endpoint);
  const originalQuery = pickContextValue(record, ctx, 'originalQuery', 'original_query');
  const normalizedQuery =
    pickContextValue(record, ctx, 'normalizedQuery', 'normalized_query') ||
    (originalQuery ? normalizePlacesQuery(originalQuery) : null);
  const evidenceRequirement = pickContextValue(
    record,
    ctx,
    'evidenceRequirement',
    'evidence_requirement'
  );
  const investigationTask =
    pickContextValue(record, ctx, 'investigationTask', 'investigation_task') ||
    investigationTaskLabel(
      evidenceRequirement,
      pickContextValue(record, ctx, 'investigationTaskId', 'investigation_task_id')
    );

  return {
    caller: record.caller || ctx.caller || 'unknown',
    feature: record.feature || ctx.feature || PLACES_FEATURES.SCRIPT,
    missionId: pickContextValue(record, ctx, 'missionId', 'mission_id'),
    missionStage: pickContextValue(record, ctx, 'missionStage', 'mission_stage'),
    tenantId: pickContextValue(record, ctx, 'tenantId', 'tenant_id', 'clientId', 'client_id'),
    executionId: pickContextValue(record, ctx, 'executionId', 'execution_id'),
    operatorId: pickContextValue(record, ctx, 'operatorId', 'operator_id'),
    triggerMode: normalizeTriggerMode(
      pickContextValue(record, ctx, 'triggerMode', 'trigger_mode')
    ),
    hypothesisId: pickContextValue(record, ctx, 'hypothesisId', 'hypothesis_id'),
    hypothesisLabel: pickContextValue(record, ctx, 'hypothesisLabel', 'hypothesis_label'),
    evidenceRequirement,
    investigationTask,
    providerId: pickContextValue(record, ctx, 'providerId', 'provider_id') || 'google_places',
    endpoint,
    isAutocomplete: record.isAutocomplete ?? record.is_autocomplete ?? flags.isAutocomplete,
    isNearbySearch: record.isNearbySearch ?? record.is_nearby_search ?? flags.isNearbySearch,
    isFindPlace: record.isFindPlace ?? record.is_find_place ?? flags.isFindPlace,
    costClass: record.costClass || record.cost_class || resolveCostClass(endpoint),
    httpStatus: record.httpStatus ?? record.http_status ?? null,
    googleStatus: record.googleStatus ?? record.google_status ?? null,
    latencyMs: record.latencyMs ?? record.latency_ms ?? null,
    cacheHit: record.cacheHit ?? record.cache_hit ?? null,
    cacheMiss: record.cacheMiss ?? record.cache_miss ?? null,
    cacheAge: record.cacheAge ?? record.cache_age ?? null,
    cacheKey: pickContextValue(record, ctx, 'cacheKey', 'cache_key'),
    cacheStrategy: pickContextValue(record, ctx, 'cacheStrategy', 'cache_strategy'),
    originalQuery,
    normalizedQuery,
    businessesReturned: record.businessesReturned ?? record.businesses_returned ?? null,
    businessesAccepted: record.businessesAccepted ?? record.businesses_accepted ?? null,
    candidatesCreated: record.candidatesCreated ?? record.candidates_created ?? null,
    qualifiedCandidates: record.qualifiedCandidates ?? record.qualified_candidates ?? null,
  };
}

async function ensurePlacesAttributionSchema(db = pool) {
  const cached = schemaCacheFor(db);
  if (cached) return cached;

  const ready = (async () => {
    await db.query(`
      CREATE TABLE IF NOT EXISTS places_api_requests (
        id BIGSERIAL PRIMARY KEY,
        caller TEXT NOT NULL,
        feature TEXT NOT NULL,
        mission_id TEXT,
        tenant_id INTEGER,
        execution_id TEXT,
        trigger_mode TEXT NOT NULL DEFAULT 'unknown',
        endpoint TEXT NOT NULL,
        is_autocomplete BOOLEAN NOT NULL DEFAULT false,
        is_nearby_search BOOLEAN NOT NULL DEFAULT false,
        is_find_place BOOLEAN NOT NULL DEFAULT false,
        cost_class TEXT NOT NULL,
        http_status INTEGER,
        google_status TEXT,
        latency_ms INTEGER,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );

      CREATE INDEX IF NOT EXISTS places_api_requests_created_at_idx
        ON places_api_requests (created_at DESC);
      CREATE INDEX IF NOT EXISTS places_api_requests_feature_idx
        ON places_api_requests (feature);
      CREATE INDEX IF NOT EXISTS places_api_requests_endpoint_idx
        ON places_api_requests (endpoint);
    `);

    for (const [column, type] of SCHEMA_COLUMNS) {
      await db.query(
        `ALTER TABLE places_api_requests ADD COLUMN IF NOT EXISTS ${column} ${type}`
      );
    }

    await db.query(`
      CREATE INDEX IF NOT EXISTS places_api_requests_mission_id_idx
        ON places_api_requests (mission_id);
      CREATE INDEX IF NOT EXISTS places_api_requests_normalized_query_idx
        ON places_api_requests (normalized_query);
      CREATE INDEX IF NOT EXISTS places_api_requests_hypothesis_id_idx
        ON places_api_requests (hypothesis_id);
    `);
  })();

  setSchemaCache(db, ready);
  return ready;
}

async function recordPlacesRequest(record = {}, deps = {}) {
  const ctx = getPlacesContext();
  const db = deps.db || pool;
  const persist = deps.persist !== false;
  const row = buildAttributionRow(record, ctx);

  if (!persist) return row;

  await ensurePlacesAttributionSchema(db);
  await db.query(
    `
      INSERT INTO places_api_requests (
        caller, feature, mission_id, mission_stage, tenant_id, execution_id, operator_id,
        trigger_mode, endpoint, is_autocomplete, is_nearby_search, is_find_place, cost_class,
        http_status, google_status, latency_ms,
        hypothesis_id, hypothesis_label, evidence_requirement, investigation_task, provider_id,
        cache_hit, cache_miss, cache_age, cache_key, cache_strategy,
        original_query, normalized_query,
        businesses_returned, businesses_accepted, candidates_created, qualified_candidates
      ) VALUES (
        $1, $2, $3, $4, $5, $6, $7,
        $8, $9, $10, $11, $12, $13,
        $14, $15, $16,
        $17, $18, $19, $20, $21,
        $22, $23, $24, $25, $26,
        $27, $28,
        $29, $30, $31, $32
      )
    `,
    [
      row.caller,
      row.feature,
      row.missionId,
      row.missionStage,
      row.tenantId != null ? Number(row.tenantId) : null,
      row.executionId,
      row.operatorId,
      row.triggerMode,
      row.endpoint,
      row.isAutocomplete,
      row.isNearbySearch,
      row.isFindPlace,
      row.costClass,
      row.httpStatus,
      row.googleStatus,
      row.latencyMs,
      row.hypothesisId,
      row.hypothesisLabel,
      row.evidenceRequirement,
      row.investigationTask,
      row.providerId,
      row.cacheHit,
      row.cacheMiss,
      row.cacheAge,
      row.cacheKey,
      row.cacheStrategy,
      row.originalQuery,
      row.normalizedQuery,
      row.businessesReturned,
      row.businessesAccepted,
      row.candidatesCreated,
      row.qualifiedCandidates,
    ]
  );

  return row;
}

function endpointDisplayLabel(endpoint) {
  switch (endpoint) {
    case PLACES_ENDPOINTS.TEXT_SEARCH:
    case PLACES_ENDPOINTS.SEARCH_TEXT_V1:
      return 'Text Search';
    case PLACES_ENDPOINTS.PLACE_DETAILS:
    case PLACES_ENDPOINTS.PLACE_DETAILS_V1:
      return 'Place Details';
    case PLACES_ENDPOINTS.NEARBY_SEARCH:
      return 'Nearby Search';
    case PLACES_ENDPOINTS.AUTOCOMPLETE:
      return 'Autocomplete';
    case PLACES_ENDPOINTS.FIND_PLACE:
      return 'Find Place';
    case PLACES_ENDPOINTS.GEOCODE:
      return 'Geocode';
    default:
      return endpoint;
  }
}

function pct(part, total) {
  if (!total) return '0%';
  return `${Math.round((part / total) * 1000) / 10}%`;
}

function ratioText(details, text) {
  if (!text) return details ? `${details}:0` : '0:0';
  if (!details) return `0:${text}`;
  return `${details}:${text}`;
}

function sum(rows, field) {
  return rows.reduce((acc, row) => acc + (Number(row[field]) || 0), 0);
}

function incrementMap(map, key, amount = 1) {
  if (!key) return;
  map.set(key, (map.get(key) || 0) + amount);
}

function topRows(map, labelKey = 'label', limit = 15) {
  return [...map.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, limit)
    .map(([label, calls]) => ({ [labelKey]: label, calls }));
}

function buildPlacesCostReport(rows = []) {
  const total = rows.length;
  const byCaller = new Map();
  const byFeature = new Map();
  const byEndpoint = new Map();
  const featureEndpoint = new Map();
  const byHypothesis = new Map();
  const byEvidence = new Map();
  const byTask = new Map();
  const normalizedQueryCounts = new Map();
  let cacheHits = 0;
  let cacheMisses = 0;
  let cacheChecked = 0;
  let estimatedSpendUsd = 0;
  let apiCalls = 0;

  for (const row of rows) {
    const feature = row.feature || PLACES_FEATURES.SCRIPT;
    const callerLabel = FEATURE_REPORT_LABELS[feature] || row.caller || feature;
    incrementMap(byCaller, callerLabel);
    incrementMap(byFeature, feature);

    const endpoint = row.endpoint || 'unknown';
    incrementMap(byEndpoint, endpointDisplayLabel(endpoint));

    if (!featureEndpoint.has(feature)) featureEndpoint.set(feature, { details: 0, text: 0 });
    const bucket = featureEndpoint.get(feature);
    if (DETAILS_ENDPOINTS.has(endpoint)) bucket.details += 1;
    if (TEXT_ENDPOINTS.has(endpoint)) bucket.text += 1;

    incrementMap(byHypothesis, row.hypothesis_label || row.hypothesis_id);
    incrementMap(byEvidence, evidenceRequirementLabel(row.evidence_requirement) || row.evidence_requirement);
    incrementMap(byTask, row.investigation_task);

    if (row.normalized_query) incrementMap(normalizedQueryCounts, row.normalized_query);

    if (row.cache_hit === true || row.cache_miss === true) {
      cacheChecked += 1;
      if (row.cache_hit) cacheHits += 1;
      if (row.cache_miss) cacheMisses += 1;
    }

    if (row.cache_hit !== true) {
      apiCalls += 1;
      estimatedSpendUsd += resolveEstimatedCostUsd(endpoint);
    }
  }

  const duplicateNormalizedQueries = [...normalizedQueryCounts.entries()]
    .filter(([, count]) => count > 1)
    .sort((a, b) => b[1] - a[1])
    .map(([query, count]) => ({ query, calls: count }));

  const businessesReturned = sum(rows, 'businesses_returned');
  const businessesAccepted = sum(rows, 'businesses_accepted');
  const candidatesCreated = sum(rows, 'candidates_created');
  const qualifiedCandidates = sum(rows, 'qualified_candidates');

  const callerRows = [...byCaller.entries()]
    .sort((a, b) => b[1] - a[1])
    .map(([caller, calls]) => ({
      caller,
      calls,
      pctOfTotal: pct(calls, total),
      estimatedSpendUsd:
        Math.round(
          rows
            .filter(
              (row) =>
                (FEATURE_REPORT_LABELS[row.feature] || row.caller) === caller &&
                row.cache_hit !== true
            )
            .reduce((acc, row) => acc + resolveEstimatedCostUsd(row.endpoint), 0) * 100
        ) / 100,
    }));

  const featureRows = [...byFeature.entries()]
    .sort((a, b) => b[1] - a[1])
    .map(([feature, calls]) => ({
      feature: FEATURE_REPORT_LABELS[feature] || feature,
      featureKey: feature,
      calls,
      pctOfTotal: pct(calls, total),
    }));

  const endpointRows = [...byEndpoint.entries()]
    .sort((a, b) => b[1] - a[1])
    .map(([endpoint, calls]) => ({ endpoint, calls }));

  const ratioRows = [...featureEndpoint.entries()]
    .filter(([feature]) => Object.values(PLACES_FEATURES).includes(feature))
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([feature, counts]) => ({
      feature: FEATURE_REPORT_LABELS[feature] || feature,
      featureKey: feature,
      detailsTextRatio: ratioText(counts.details, counts.text),
      details: counts.details,
      text: counts.text,
    }));

  const missionMap = new Map();
  for (const row of rows) {
    if (!row.mission_id) continue;
    const label = `${row.mission_id}${row.mission_stage ? ` (${row.mission_stage})` : ''}`;
    incrementMap(missionMap, label);
  }

  return {
    total,
    apiCalls,
    estimatedSpendUsd: Math.round(estimatedSpendUsd * 100) / 100,
    callerBreakdown: callerRows,
    featureBreakdown: featureRows,
    endpointBreakdown: endpointRows,
    featureRatioBreakdown: ratioRows,
    cache: {
      checked: cacheChecked,
      hits: cacheHits,
      misses: cacheMisses,
      hitRate: pct(cacheHits, cacheChecked),
      missRate: pct(cacheMisses, cacheChecked),
    },
    duplicateNormalizedQueries,
    cognitive: {
      hypothesisBreakdown: topRows(byHypothesis, 'hypothesis'),
      evidenceBreakdown: topRows(byEvidence, 'evidenceRequirement'),
      taskBreakdown: topRows(byTask, 'investigationTask'),
    },
    missionBreakdown: topRows(missionMap, 'mission', 20),
    efficiency: {
      calls: apiCalls,
      businessesReturned,
      businessesAccepted,
      candidatesCreated,
      qualifiedCandidates,
      costPerCallUsd: apiCalls ? Math.round((estimatedSpendUsd / apiCalls) * 10000) / 10000 : 0,
      costPerCandidateUsd: candidatesCreated
        ? Math.round((estimatedSpendUsd / candidatesCreated) * 100) / 100
        : null,
      costPerQualifiedCandidateUsd: qualifiedCandidates
        ? Math.round((estimatedSpendUsd / qualifiedCandidates) * 100) / 100
        : null,
    },
  };
}

async function queryPlacesCostReport(opts = {}) {
  const db = opts.db || pool;
  await ensurePlacesAttributionSchema(db);

  const params = [];
  const clauses = [];

  if (opts.since) {
    params.push(opts.since);
    clauses.push(`created_at >= $${params.length}`);
  }
  if (opts.until) {
    params.push(opts.until);
    clauses.push(`created_at < $${params.length}`);
  }
  if (opts.tenantId != null) {
    params.push(Number(opts.tenantId));
    clauses.push(`tenant_id = $${params.length}`);
  }
  if (opts.missionId) {
    params.push(String(opts.missionId));
    clauses.push(`mission_id = $${params.length}`);
  }

  const where = clauses.length ? `WHERE ${clauses.join(' AND ')}` : '';
  const { rows } = await db.query(
    `SELECT * FROM places_api_requests ${where} ORDER BY created_at ASC`,
    params
  );

  return buildPlacesCostReport(rows);
}

function formatUsd(value) {
  if (value == null || Number.isNaN(Number(value))) return '—';
  return `$${Number(value).toFixed(2)}`;
}

function formatPlacesCostReportMarkdown(report, opts = {}) {
  const lines = [];
  lines.push('# Places API Cost Attribution (AUDIT-063)');
  lines.push('');
  for (const line of opts.headerLines || []) {
    lines.push(line);
    lines.push('');
  }
  lines.push(`Total ledger rows: **${report.total.toLocaleString()}**`);
  lines.push(`Billable API calls (excl. cache hits): **${report.apiCalls.toLocaleString()}**`);
  lines.push(`Estimated spend: **${formatUsd(report.estimatedSpendUsd)}**`);
  lines.push('');
  lines.push('## Spend by caller');
  lines.push('');
  lines.push('| Caller | Calls | % of Total | Est. Spend |');
  lines.push('| --- | ---: | ---: | ---: |');
  for (const row of report.callerBreakdown) {
    lines.push(
      `| ${row.caller} | ${row.calls.toLocaleString()} | ${row.pctOfTotal} | ${formatUsd(row.estimatedSpendUsd)} |`
    );
  }
  lines.push('');
  lines.push('## Spend by feature');
  lines.push('');
  lines.push('| Feature | Calls | % |');
  lines.push('| --- | ---: | ---: |');
  for (const row of report.featureBreakdown) {
    lines.push(`| ${row.feature} | ${row.calls.toLocaleString()} | ${row.pctOfTotal} |`);
  }
  lines.push('');
  lines.push('## Spend by endpoint');
  lines.push('');
  lines.push('| Endpoint | Calls |');
  lines.push('| --- | ---: |');
  for (const row of report.endpointBreakdown) {
    lines.push(`| ${row.endpoint} | ${row.calls.toLocaleString()} |`);
  }
  lines.push('');
  lines.push('## Cache');
  lines.push('');
  lines.push(`- Hit rate: **${report.cache.hitRate}** (${report.cache.hits} hits)`);
  lines.push(`- Miss rate: **${report.cache.missRate}** (${report.cache.misses} misses)`);
  lines.push(`- Duplicate normalized queries: **${report.duplicateNormalizedQueries.length}**`);
  if (report.duplicateNormalizedQueries.length) {
    lines.push('');
    lines.push('| Normalized Query | Calls |');
    lines.push('| --- | ---: |');
    for (const row of report.duplicateNormalizedQueries.slice(0, 10)) {
      lines.push(`| ${row.query} | ${row.calls} |`);
    }
  }
  lines.push('');
  lines.push('## Cognitive breakdown');
  lines.push('');
  lines.push('### Hypothesis');
  lines.push('| Hypothesis | Calls |');
  lines.push('| --- | ---: |');
  for (const row of report.cognitive.hypothesisBreakdown) {
    lines.push(`| ${row.hypothesis || '—'} | ${row.calls.toLocaleString()} |`);
  }
  lines.push('');
  lines.push('### Evidence requirement');
  lines.push('| Evidence Requirement | Calls |');
  lines.push('| --- | ---: |');
  for (const row of report.cognitive.evidenceBreakdown) {
    lines.push(`| ${row.evidenceRequirement || '—'} | ${row.calls.toLocaleString()} |`);
  }
  lines.push('');
  lines.push('### Investigation task');
  lines.push('| Investigation Task | Calls |');
  lines.push('| --- | ---: |');
  for (const row of report.cognitive.taskBreakdown) {
    lines.push(`| ${row.investigationTask || '—'} | ${row.calls.toLocaleString()} |`);
  }
  lines.push('');
  if (report.missionBreakdown.length) {
    lines.push('## Spend by mission');
    lines.push('| Mission | Calls |');
    lines.push('| --- | ---: |');
    for (const row of report.missionBreakdown) {
      lines.push(`| ${row.mission} | ${row.calls.toLocaleString()} |`);
    }
    lines.push('');
  }
  lines.push('## Efficiency funnel');
  lines.push('```text');
  lines.push(`Google Calls          ${report.efficiency.calls.toLocaleString()}`);
  lines.push('↓');
  lines.push(`Businesses Returned   ${report.efficiency.businessesReturned.toLocaleString()}`);
  lines.push('↓');
  lines.push(`Businesses Accepted   ${report.efficiency.businessesAccepted.toLocaleString()}`);
  lines.push('↓');
  lines.push(`Candidates Created    ${report.efficiency.candidatesCreated.toLocaleString()}`);
  lines.push('↓');
  lines.push(`Qualified Candidates  ${report.efficiency.qualifiedCandidates.toLocaleString()}`);
  lines.push('```');
  lines.push('');
  lines.push(`Cost per call: **${formatUsd(report.efficiency.costPerCallUsd)}**`);
  lines.push(`Cost per candidate: **${formatUsd(report.efficiency.costPerCandidateUsd)}**`);
  lines.push(`Cost per qualified candidate: **${formatUsd(report.efficiency.costPerQualifiedCandidateUsd)}**`);
  lines.push('');
  lines.push('## Feature — Details/Text ratio');
  lines.push('| Feature | Details/Text ratio |');
  lines.push('| --- | --- |');
  for (const row of report.featureRatioBreakdown) {
    lines.push(`| ${row.feature} | ${row.detailsTextRatio} |`);
  }
  lines.push('');
  return lines.join('\n');
}

module.exports = {
  PLACES_FEATURES,
  PLACES_ENDPOINTS,
  TRIGGER_MODES,
  FEATURE_REPORT_LABELS,
  EVIDENCE_LABELS,
  INVESTIGATION_TASK_LABELS,
  ESTIMATED_COST_USD_BY_ENDPOINT,
  getPlacesContext,
  withPlacesContext,
  resolveCostClass,
  normalizePlacesQuery,
  evidenceRequirementLabel,
  investigationTaskLabel,
  buildPlacesContextFromDiscovery,
  buildAttributionRow,
  countFromResult,
  resolveEstimatedCostUsd,
  ensurePlacesAttributionSchema,
  recordPlacesRequest,
  buildPlacesCostReport,
  queryPlacesCostReport,
  formatPlacesCostReportMarkdown,
  endpointDisplayLabel,
};
