'use strict';

/**
 * Safe Scout Places connectivity diagnostic.
 *
 * Probes the exact same legacy Places Text Search client/path Scout sourcing uses.
 * Never logs the full API key. Never writes CRM / outreach / placeholders.
 */

const {
  SCOUT_PLACES_ENDPOINT_FAMILY,
  SCOUT_PLACES_AUTH_STYLE,
  SCOUT_PLACES_TEXTSEARCH_URL,
  buildScoutPlacesTextSearchUrl,
  scoutPlacesUrlHostPath,
} = require('./scoutPublicSourcing');
const { PLACES_FEATURES, TRIGGER_MODES } = require('../utils/placesCostAttribution');
const { legacyTextSearch } = require('../utils/placesApi');

const PLACES_NEW_ENDPOINT_FAMILY = 'places_api_new_search_text';
const PLACES_NEW_AUTH_STYLE = 'header_x_goog_api_key';
const PLACES_NEW_SEARCH_TEXT_URL =
  'https://places.googleapis.com/v1/places:searchText';

const DEFAULT_PROBE_QUERY = 'coffee shop Manchester NH';

function fingerprintApiKey(key) {
  const raw = String(key || '');
  if (!raw) return null;
  if (raw.length < 8) return `${raw.slice(0, 1)}…${raw.slice(-1)}`;
  return `${raw.slice(0, 4)}…${raw.slice(-4)}`;
}

function railwayRuntimeIdentity() {
  const service =
    process.env.RAILWAY_SERVICE_NAME ||
    process.env.RAILWAY_SERVICE ||
    null;
  const environment =
    process.env.RAILWAY_ENVIRONMENT_NAME ||
    process.env.RAILWAY_ENVIRONMENT ||
    null;
  return {
    service,
    environment,
    environmentId: process.env.RAILWAY_ENVIRONMENT_ID || null,
    projectId: process.env.RAILWAY_PROJECT_ID || null,
    deploymentId: process.env.RAILWAY_DEPLOYMENT_ID || null,
    replicaId: process.env.RAILWAY_REPLICA_ID || null,
    publicDomain: process.env.RAILWAY_PUBLIC_DOMAIN || null,
    gitCommit: process.env.RAILWAY_GIT_COMMIT_SHA || null,
    nodeEnv: process.env.NODE_ENV || null,
    testedLabel: [service, environment].filter(Boolean).join(' / ') || 'local_or_unknown',
  };
}

function baseGuardrails() {
  return {
    crmWritesMade: false,
    outreachCopyGenerated: false,
    placeholdersCreated: false,
    fullKeyLogged: false,
  };
}

async function readJsonSafe(res) {
  const text = await res.text();
  if (!text) return { data: null, parseError: null };
  try {
    return { data: JSON.parse(text), parseError: null };
  } catch (err) {
    return {
      data: null,
      parseError: err && err.message ? String(err.message) : 'json_parse_failed',
    };
  }
}

/**
 * Probe Places API (New) for differential diagnosis only.
 * Scout sourcing does NOT use this path — reported separately.
 */
async function probePlacesApiNew(apiKey, fetchImpl, probeQuery) {
  const hostPath = scoutPlacesUrlHostPath(PLACES_NEW_SEARCH_TEXT_URL);
  const startedAt = new Date().toISOString();
  try {
    const res = await fetchImpl(PLACES_NEW_SEARCH_TEXT_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Goog-Api-Key': apiKey,
        'X-Goog-FieldMask': 'places.id,places.displayName',
      },
      body: JSON.stringify({
        textQuery: probeQuery,
        maxResultCount: 1,
      }),
    });
    const { data, parseError } = await readJsonSafe(res);
    const googleError =
      (data && (data.error?.message || data.error_message)) || null;
    const googleStatus =
      (data && (data.error?.status || data.status)) ||
      (res.ok ? 'OK_OR_EMPTY' : null);
    return {
      endpointFamily: PLACES_NEW_ENDPOINT_FAMILY,
      authStyle: PLACES_NEW_AUTH_STYLE,
      request: hostPath,
      httpStatus: res.status,
      googleStatus,
      googleErrorMessage: googleError,
      parseError,
      resultCount: Array.isArray(data?.places) ? data.places.length : null,
      probedAt: startedAt,
      note: 'Comparison only — Scout sourcing does not use Places API (New).',
    };
  } catch (err) {
    return {
      endpointFamily: PLACES_NEW_ENDPOINT_FAMILY,
      authStyle: PLACES_NEW_AUTH_STYLE,
      request: hostPath,
      httpStatus: null,
      googleStatus: null,
      googleErrorMessage: err && err.message ? String(err.message) : 'fetch_failed',
      parseError: null,
      resultCount: null,
      probedAt: startedAt,
      note: 'Comparison only — Scout sourcing does not use Places API (New).',
    };
  }
}

/**
 * Run a single Text Search against the Scout Places path.
 *
 * @param {object} [opts]
 * @param {string} [opts.apiKey]
 * @param {typeof fetch} [opts.fetchImpl]
 * @param {string} [opts.query]
 * @param {boolean} [opts.comparePlacesNew=true]
 * @returns {Promise<object>}
 */
async function diagnoseScoutPlaces(opts = {}) {
  const apiKey = opts.apiKey != null ? String(opts.apiKey) : String(process.env.GOOGLE_PLACES_KEY || '');
  const keyPresent = Boolean(apiKey && apiKey.trim());
  const fetchImpl =
    opts.fetchImpl ||
    (typeof fetch === 'function' ? fetch.bind(globalThis) : null);
  const probeQuery = String(opts.query || DEFAULT_PROBE_QUERY).trim() || DEFAULT_PROBE_QUERY;
  const comparePlacesNew = opts.comparePlacesNew !== false;
  const railway = railwayRuntimeIdentity();

  const textSearchUrl = buildScoutPlacesTextSearchUrl({
    query: probeQuery,
    apiKey: keyPresent ? apiKey : 'REDACTED',
  });
  const hostPath = scoutPlacesUrlHostPath(textSearchUrl);

  const report = {
    ok: false,
    diagnostic: 'scout_places',
    endpointFamily: SCOUT_PLACES_ENDPOINT_FAMILY,
    authStyle: SCOUT_PLACES_AUTH_STYLE,
    request: hostPath,
    requestUrlConstant: SCOUT_PLACES_TEXTSEARCH_URL,
    httpStatus: null,
    googleStatus: null,
    googleErrorMessage: null,
    keyFingerprint: fingerprintApiKey(apiKey),
    keyPresent,
    railwayService: railway.service,
    railwayEnvironment: railway.environment,
    railway: railway,
    probeQueryLabel: 'fixed_diagnostic_query',
    resultCount: null,
    error: null,
    likelyCauseHints: [],
    placesApiNewComparison: null,
    ...baseGuardrails(),
    diagnosedAt: new Date().toISOString(),
  };

  if (!keyPresent) {
    report.error = 'GOOGLE_PLACES_KEY_missing';
    report.likelyCauseHints.push(
      'GOOGLE_PLACES_KEY is not present in this runtime — check Railway service variables and redeploy if recently changed.'
    );
    return report;
  }

  if (!fetchImpl) {
    report.error = 'fetch_unavailable';
    report.likelyCauseHints.push('No fetch implementation available in this runtime.');
    return report;
  }

  try {
    const traced = await legacyTextSearch({
      query: probeQuery,
      apiKey,
      fetchImpl,
      record: {
        caller: 'scoutPlacesDiagnostic.js',
        feature: PLACES_FEATURES.DIAGNOSTIC,
        triggerMode: TRIGGER_MODES.MANUAL,
      },
    });
    report.httpStatus = traced.httpStatus;
    const data = traced.data;
    if (!data || typeof data !== 'object') {
      report.error = 'google_response_empty';
    } else {
      report.googleStatus = data.status || null;
      report.googleErrorMessage = data.error_message || null;
      report.resultCount = Array.isArray(data.results) ? data.results.length : null;
      if (data.status === 'OK' || data.status === 'ZERO_RESULTS') {
        report.ok = true;
      } else {
        report.error = `google_places_status_${data.status || 'unknown'}`;
      }
    }

    if (!traced.ok && !report.error) {
      report.error = `google_places_http_${traced.httpStatus}`;
    }
  } catch (err) {
    report.error = 'google_places_fetch_failed';
    report.googleErrorMessage = err && err.message ? String(err.message) : 'fetch_failed';
  }

  if (report.googleStatus === 'REQUEST_DENIED') {
    report.likelyCauseHints.push(
      'Scout uses legacy Places Text Search (maps.googleapis.com) with ?key= auth — not Places API (New).'
    );
    report.likelyCauseHints.push(
      'Enable the legacy Places API (sometimes labeled "Places API" under Maps) for this key/project, or migrate Scout to Places API (New).'
    );
    report.likelyCauseHints.push(
      'If only Places API (New) is enabled, legacy Text Search returns REQUEST_DENIED even when billing and key restrictions look correct.'
    );
    report.likelyCauseHints.push(
      'Confirm this Railway service/environment picked up the edited key (redeploy after env change).'
    );
  } else if (!report.keyPresent) {
    // already hinted
  } else if (!report.ok && report.httpStatus === 403) {
    report.likelyCauseHints.push(
      'HTTP 403 on legacy Places — check API restrictions and whether legacy Places API is enabled.'
    );
  }

  if (comparePlacesNew) {
    report.placesApiNewComparison = await probePlacesApiNew(
      apiKey,
      fetchImpl,
      probeQuery
    );
    const newOk =
      report.placesApiNewComparison &&
      report.placesApiNewComparison.httpStatus >= 200 &&
      report.placesApiNewComparison.httpStatus < 300 &&
      !report.placesApiNewComparison.googleErrorMessage;
    if (!report.ok && newOk) {
      report.likelyCauseHints.push(
        'Places API (New) probe succeeded while Scout legacy Text Search failed — strongest signal that the key/project is New-only or legacy Places is disabled.'
      );
    }
  }

  return report;
}

function formatScoutPlacesDiagnostic(report) {
  const lines = [];
  lines.push('Scout Places diagnostic');
  lines.push('=======================');
  lines.push(`ok: ${report.ok}`);
  lines.push(`endpointFamily: ${report.endpointFamily}`);
  lines.push(`authStyle: ${report.authStyle}`);
  lines.push(
    `request: ${report.request?.host || '?'}${report.request?.path || ''}`
  );
  lines.push(`httpStatus: ${report.httpStatus}`);
  lines.push(`googleStatus: ${report.googleStatus}`);
  lines.push(`googleErrorMessage: ${report.googleErrorMessage || '(none)'}`);
  lines.push(`keyFingerprint: ${report.keyFingerprint || '(none)'}`);
  lines.push(`keyPresent: ${report.keyPresent}`);
  lines.push(
    `railwayService/environment: ${report.railway?.testedLabel || 'local_or_unknown'}`
  );
  if (report.error) lines.push(`error: ${report.error}`);
  if (report.likelyCauseHints?.length) {
    lines.push('hints:');
    for (const hint of report.likelyCauseHints) {
      lines.push(`  - ${hint}`);
    }
  }
  if (report.placesApiNewComparison) {
    const c = report.placesApiNewComparison;
    lines.push('placesApiNewComparison:');
    lines.push(`  endpointFamily: ${c.endpointFamily}`);
    lines.push(`  request: ${c.request?.host || '?'}${c.request?.path || ''}`);
    lines.push(`  httpStatus: ${c.httpStatus}`);
    lines.push(`  googleStatus: ${c.googleStatus}`);
    lines.push(`  googleErrorMessage: ${c.googleErrorMessage || '(none)'}`);
  }
  lines.push('guardrails: no CRM writes, no outreach, no placeholders, no full key');
  return lines.join('\n');
}

module.exports = {
  DEFAULT_PROBE_QUERY,
  fingerprintApiKey,
  railwayRuntimeIdentity,
  diagnoseScoutPlaces,
  formatScoutPlacesDiagnostic,
  PLACES_NEW_SEARCH_TEXT_URL,
  PLACES_NEW_ENDPOINT_FAMILY,
};
