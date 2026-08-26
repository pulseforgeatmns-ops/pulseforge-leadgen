'use strict';

/**
 * AUDIT-063 — Google Places API cost attribution.
 * Records every Places request with caller, feature, tenant, mission, and endpoint metadata.
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

/** Report row labels (feature → display caller column). */
const FEATURE_REPORT_LABELS = Object.freeze({
  [PLACES_FEATURES.LEADGEN]: 'leadgen.js',
  [PLACES_FEATURES.DISCOVERY]: 'Scout Discovery',
  [PLACES_FEATURES.CANDIDATE_REFRESH]: 'Candidate Refresh',
  [PLACES_FEATURES.WARM_ROUTING]: 'Warm Routing',
  [PLACES_FEATURES.DIAGNOSTIC]: 'Diagnostic',
  [PLACES_FEATURES.GEOCODE]: 'Geocode',
  [PLACES_FEATURES.SCRIPT]: 'Script',
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

async function readFetchJson(res) {
  if (res && typeof res.json === 'function') return res.json();
  if (res && typeof res.text === 'function') {
    const text = await res.text();
    return text ? JSON.parse(text) : null;
  }
  return null;
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

async function ensurePlacesAttributionSchema(db = pool) {
  const cached = schemaCacheFor(db);
  if (cached) return cached;
  const ready = db.query(`
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
  setSchemaCache(db, ready);
  return ready;
}

/**
 * Persist one Places API request. Merges AsyncLocalStorage context with explicit overrides.
 *
 * @param {object} record
 * @param {object} [deps]
 * @param {import('pg').Pool} [deps.db]
 * @param {boolean} [deps.persist=true]
 */
async function recordPlacesRequest(record = {}, deps = {}) {
  const ctx = getPlacesContext();
  const db = deps.db || pool;
  const persist = deps.persist !== false;
  const endpoint = record.endpoint || PLACES_ENDPOINTS.TEXT_SEARCH;
  const flags = endpointFlags(endpoint);

  const row = {
    caller: record.caller || ctx.caller || 'unknown',
    feature: record.feature || ctx.feature || PLACES_FEATURES.SCRIPT,
    missionId: record.missionId || record.mission_id || ctx.missionId || ctx.mission_id || null,
    tenantId: record.tenantId ?? record.tenant_id ?? ctx.tenantId ?? ctx.tenant_id ?? ctx.clientId ?? ctx.client_id ?? null,
    executionId: record.executionId || record.execution_id || ctx.executionId || ctx.execution_id || null,
    triggerMode: normalizeTriggerMode(record.triggerMode || record.trigger_mode || ctx.triggerMode || ctx.trigger_mode),
    endpoint,
    isAutocomplete: record.isAutocomplete ?? record.is_autocomplete ?? flags.isAutocomplete,
    isNearbySearch: record.isNearbySearch ?? record.is_nearby_search ?? flags.isNearbySearch,
    isFindPlace: record.isFindPlace ?? record.is_find_place ?? flags.isFindPlace,
    costClass: record.costClass || record.cost_class || resolveCostClass(endpoint),
    httpStatus: record.httpStatus ?? record.http_status ?? null,
    googleStatus: record.googleStatus ?? record.google_status ?? null,
    latencyMs: record.latencyMs ?? record.latency_ms ?? null,
  };

  if (!persist) return row;

  await ensurePlacesAttributionSchema(db);
  await db.query(
    `
      INSERT INTO places_api_requests (
        caller, feature, mission_id, tenant_id, execution_id, trigger_mode,
        endpoint, is_autocomplete, is_nearby_search, is_find_place, cost_class,
        http_status, google_status, latency_ms
      ) VALUES (
        $1, $2, $3, $4, $5, $6,
        $7, $8, $9, $10, $11,
        $12, $13, $14
      )
    `,
    [
      row.caller,
      row.feature,
      row.missionId,
      row.tenantId != null ? Number(row.tenantId) : null,
      row.executionId,
      row.triggerMode,
      row.endpoint,
      row.isAutocomplete,
      row.isNearbySearch,
      row.isFindPlace,
      row.costClass,
      row.httpStatus,
      row.googleStatus,
      row.latencyMs,
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

/**
 * Build AUDIT-063 summary tables from persisted rows.
 *
 * @param {object[]} rows
 * @returns {object}
 */
function buildPlacesCostReport(rows = []) {
  const total = rows.length;

  const byFeature = new Map();
  const byEndpoint = new Map();
  const featureEndpoint = new Map();

  for (const row of rows) {
    const feature = row.feature || PLACES_FEATURES.SCRIPT;
    byFeature.set(feature, (byFeature.get(feature) || 0) + 1);

    const endpoint = row.endpoint || 'unknown';
    const endpointLabel = endpointDisplayLabel(endpoint);
    byEndpoint.set(endpointLabel, (byEndpoint.get(endpointLabel) || 0) + 1);

    if (!featureEndpoint.has(feature)) {
      featureEndpoint.set(feature, { details: 0, text: 0 });
    }
    const bucket = featureEndpoint.get(feature);
    if (DETAILS_ENDPOINTS.has(endpoint)) bucket.details += 1;
    if (TEXT_ENDPOINTS.has(endpoint)) bucket.text += 1;
  }

  const callerRows = [...byFeature.entries()]
    .sort((a, b) => b[1] - a[1])
    .map(([feature, calls]) => ({
      caller: FEATURE_REPORT_LABELS[feature] || feature,
      feature,
      calls,
      pctOfTotal: pct(calls, total),
    }));

  const endpointRows = [...byEndpoint.entries()]
    .sort((a, b) => b[1] - a[1])
    .map(([endpoint, calls]) => ({
      endpoint,
      calls,
    }));

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

  return {
    total,
    callerBreakdown: callerRows,
    endpointBreakdown: endpointRows,
    featureRatioBreakdown: ratioRows,
  };
}

/**
 * Query DB and return formatted report.
 *
 * @param {object} [opts]
 * @param {import('pg').Pool} [opts.db]
 * @param {Date|string} [opts.since]
 * @param {Date|string} [opts.until]
 * @param {number} [opts.tenantId]
 */
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

  const where = clauses.length ? `WHERE ${clauses.join(' AND ')}` : '';
  const { rows } = await db.query(
    `SELECT * FROM places_api_requests ${where} ORDER BY created_at ASC`,
    params
  );

  return buildPlacesCostReport(rows);
}

function formatPlacesCostReportMarkdown(report) {
  const lines = [];
  lines.push('# Places API Cost Attribution (AUDIT-063)');
  lines.push('');
  lines.push(`Total requests: **${report.total}**`);
  lines.push('');
  lines.push('## By caller');
  lines.push('');
  lines.push('| Caller | Calls | % of Total |');
  lines.push('| --- | ---: | ---: |');
  for (const row of report.callerBreakdown) {
    lines.push(`| ${row.caller} | ${row.calls.toLocaleString()} | ${row.pctOfTotal} |`);
  }
  lines.push('');
  lines.push('## By endpoint');
  lines.push('');
  lines.push('| Endpoint | Calls |');
  lines.push('| --- | ---: |');
  for (const row of report.endpointBreakdown) {
    lines.push(`| ${row.endpoint} | ${row.calls.toLocaleString()} |`);
  }
  lines.push('');
  lines.push('## Feature — Details/Text ratio');
  lines.push('');
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
  getPlacesContext,
  withPlacesContext,
  resolveCostClass,
  ensurePlacesAttributionSchema,
  recordPlacesRequest,
  buildPlacesCostReport,
  queryPlacesCostReport,
  formatPlacesCostReportMarkdown,
  endpointDisplayLabel,
};
