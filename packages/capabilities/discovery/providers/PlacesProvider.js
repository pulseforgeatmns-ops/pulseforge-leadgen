'use strict';
const { withResearchProviderScope, assertResearchProviderAllowed, rethrowResearchProviderControl } = require('../../../../utils/anchorResearchProviderPause');

/**
 * Google Places discovery provider (SPEC-024).
 * SPEC-181 — evidence-native execution: query generation lives here, not in Scout.
 * Capability-layer adapter — no Scout/agent module imports.
 */

const {
  resolveMarketHypothesisBySegmentKey,
  expandSearchStrategies,
  SEARCH_SOURCES,
} = require('../../../scout/hypothesis/MarketHypothesisRegistry');
const { INVESTIGATIVE_EVIDENCE } = require('../../../scout/coverage/EvidenceRequirements');
const { PLACES_FEATURES } = require('../../../../utils/placesCostAttribution');
const { legacyTextSearch, legacyPlaceDetails } = require('../../../../utils/placesApi');

function isValidEvidenceRequest(value) {
  return (
    value &&
    typeof value === 'object' &&
    typeof value.segment === 'string' &&
    typeof value.evidenceType === 'string' &&
    value.geography &&
    Array.isArray(value.geography.cities)
  );
}

const DEFAULT_LIMIT = 20;
const MAX_RETRIES = 2;
const RETRYABLE_GOOGLE_STATUS = new Set(['OVER_QUERY_LIMIT', 'UNKNOWN_ERROR']);
const SUCCESS_GOOGLE_STATUS = new Set(['OK', 'ZERO_RESULTS']);

function mergeTicketRefs(existing, incoming) {
  const refs = [...(existing?._scoutTicketRefs || []), ...(incoming?._scoutTicketRefs || [])];
  if (refs.length) existing._scoutTicketRefs = [...new Map(refs.map(ref => [JSON.stringify(ref), ref])).values()];
}

function createProviderExecution(partial = {}) {
  return {
    providerId: 'google_places',
    executed: partial.executed === true,
    abortReason: partial.abortReason || null,
    queries: Array.isArray(partial.queries) ? partial.queries : [],
    totals: partial.totals || {
      queries: 0,
      results: 0,
      retries: 0,
      latencyMs: 0,
    },
    errors: Array.isArray(partial.errors) ? partial.errors : [],
    quota: partial.quota || null,
  };
}

/**
 * @param {object} [deps]
 * @param {string} [deps.apiKey]
 * @param {typeof fetch} [deps.fetchImpl]
 */
function createPlacesProvider(deps = {}) {
  const apiKey = deps.apiKey || process.env.GOOGLE_PLACES_KEY || '';
  const fetchImpl =
    deps.fetchImpl ||
    (typeof fetch === 'function'
      ? fetch.bind(globalThis)
      : null);

  const provider = {
    id: 'google_places',
    lastExecution: null,
    available() {
      return !!(apiKey && fetchImpl);
    },

    /**
     * SPEC-181 — collect evidence from a structured request.
     * Provider owns query generation, pagination, retries, and localization.
     *
     * @param {object} evidenceRequest
     * @param {string} evidenceRequest.segment
     * @param {string} evidenceRequest.evidenceType
     * @param {object} evidenceRequest.geography
     * @param {string[]} evidenceRequest.geography.cities
     * @param {string|null} [evidenceRequest.geography.state]
     * @returns {Promise<object[]>}
     */
    async collectEvidence(evidenceRequest) {
      if (!this.available()) {
        this.lastExecution = createProviderExecution({
          executed: false,
          abortReason: 'provider_unavailable',
          errors: [{ code: 'provider_unavailable', message: 'Google Places provider unavailable.' }],
        });
        return [];
      }
      if (!isValidEvidenceRequest(evidenceRequest)) {
        this.lastExecution = createProviderExecution({
          executed: false,
          abortReason: 'invalid_evidence_request',
          errors: [{ code: 'invalid_evidence_request', message: 'Evidence request missing segment, evidenceType, or geography.cities.' }],
        });
        return [];
      }

      const { segment, evidenceType, geography = {}, entity = null, businessName = null } = evidenceRequest;
      const cities = geography.cities || [];
      const state = geography.state || null;
      if (!cities.length && !entity?.name) {
        this.lastExecution = createProviderExecution({
          executed: false,
          abortReason: 'empty_geography',
          errors: [{ code: 'empty_geography', message: 'Evidence request geography.cities is empty — Places was not called.' }],
        });
        return [];
      }

      const queries = buildQueriesForEvidence({
        segment,
        evidenceType,
        cities,
        state,
        entityName: entity?.name || businessName || null,
        website: entity?.website || evidenceRequest.website || null,
        discoveryConcept: evidenceRequest.discoveryConcept || null,
      });
      const requireWebsite = evidenceType !== INVESTIGATIVE_EVIDENCE.IDENTITY;
      const limit = DEFAULT_LIMIT;
      const out = [];
      const seenPlaceIds = new Set();
      const knownHits = new Map();
      const queryRecords = [];
      const errors = [];
      let totalRetries = 0;
      let totalLatency = 0;
      let quota = null;

      const checkpoint = !entity?.name && !businessName && evidenceRequest.scope !== 'entity'
        ? deps.queryCheckpointSession : null;
      if (checkpoint) checkpoint.assertTenant(evidenceRequest.tenantId);
      const tenantMemory = checkpoint ? deps.tenantDiscoveryMemory : null;
      if (tenantMemory && String(evidenceRequest.tenantId)!==tenantMemory.tenantId)
        throw new Error('Scout tenant memory request tenant mismatch');
      let checkpointSkipped = 0;
      let checkpointReplayed = 0;
      let continuationStatus = null;
      for (const query of queries) {
        const prior = checkpoint ? await checkpoint.lookup(query, requireWebsite) : null;
        if (prior?.handled) { checkpointSkipped += 1; continuationStatus ||= prior.continuationStatus; continue; }
        if (prior && Array.isArray(prior.candidates)) {
          checkpointReplayed += 1;
          for (const hit of prior.candidates) {
            const existing = out.find(row => row.placeId === hit.placeId);
            if (existing) mergeTicketRefs(existing, hit);
            else { seenPlaceIds.add(hit.placeId); out.push(hit); }
            knownHits.set(hit.placeId, existing || hit);
          }
          continue;
        }
        // Isolate query output until its durable replay payload is confirmed.
        const queryOut = checkpoint ? [] : out;
        const querySeen = checkpoint ? new Set(seenPlaceIds) : seenPlaceIds;
        const searchResult = await searchWithRetry(
          { industry: query, location: '', limit },
          apiKey,
          fetchImpl,
          { requireWebsite, seenPlaceIds: querySeen, out: queryOut, knownHits: checkpoint ? knownHits : null,
            singlePage: prior?.singlePage === true, pageToken: prior?.pageToken || null,
            apiDeps: deps.placesApiDeps, useCache: deps.useCache, tenantMemory }
        );
        if (checkpoint) {
          const rows = !searchResult.error
            ? await checkpoint.stage(prior.key, queryOut, prior.singlePage ? {
              nextPageToken: searchResult.nextPageToken, rawResults: searchResult.rawResults,
            } : null) : prior.singlePage ? [] : queryOut;
          if (searchResult.error && typeof checkpoint.fail === 'function') await checkpoint.fail(prior.key, { ...searchResult.error, rawResults: searchResult.rawResults });
          for (const hit of rows) {
            const existing = out.find(row => row.placeId === hit.placeId);
            if (existing) mergeTicketRefs(existing, hit);
            else { seenPlaceIds.add(hit.placeId); out.push(hit); }
            knownHits.set(hit.placeId, existing || hit);
          }
        }
        queryRecords.push(...(searchResult.attempts || []));
        totalRetries += searchResult.retries || 0;
        totalLatency += searchResult.latencyMs || 0;
        if (searchResult.quota) quota = searchResult.quota;
        if (searchResult.error) errors.push(searchResult.error);
        // Authorization failure applies to this provider invocation, not one business.
        if (searchResult.error?.googleStatus === 'REQUEST_DENIED') break;
      }

      this.lastExecution = createProviderExecution({
        executed: queryRecords.some(
          (row) => row.httpStatus != null || row.googleStatus != null || row.googleError
        ),
        abortReason: checkpointSkipped === queries.length && queries.length
          ? (continuationStatus || 'approved_query_set_handled') : queryRecords.length || out.length || checkpointReplayed ? null : 'no_queries_generated',
        queries: queryRecords,
        totals: {
          queries: queryRecords.length,
          results: queryRecords.reduce((sum, row) => sum + (row.resultCount || 0), 0),
          retries: totalRetries,
          latencyMs: totalLatency,
        },
        errors,
        quota,
      });

      return out;
    },

    /**
     * Legacy keyword search — retained for cron/backward compatibility until Phase 3 migration.
     * @param {object} query
     * @param {string} query.industry
     * @param {string} query.location
     * @param {number} [query.limit]
     * @returns {Promise<object[]>}
     */
    async search(query) {
      if (!this.available()) {
        this.lastExecution = createProviderExecution({
          executed: false,
          abortReason: 'provider_unavailable',
          errors: [{ code: 'provider_unavailable', message: 'Google Places provider unavailable.' }],
        });
        return [];
      }
      const limit = Math.min(Number(query.limit) || DEFAULT_LIMIT, DEFAULT_LIMIT);
      const q = `${query.industry || ''} ${query.location || ''}`.trim();
      if (!q) {
        this.lastExecution = createProviderExecution({
          executed: false,
          abortReason: 'empty_query',
          errors: [{ code: 'empty_query', message: 'Legacy Places search had no industry/location.' }],
        });
        return [];
      }

      const out = [];
      const seenPlaceIds = new Set();
      const searchResult = await searchWithRetry(
        { industry: q, location: '', limit },
        apiKey,
        fetchImpl,
        { requireWebsite: true, seenPlaceIds, out, apiDeps: deps.placesApiDeps, useCache: deps.useCache }
      );
      this.lastExecution = createProviderExecution({
        executed: true,
        queries: searchResult.attempts || [],
        totals: {
          queries: (searchResult.attempts || []).length,
          results: (searchResult.attempts || []).reduce((sum, row) => sum + (row.resultCount || 0), 0),
          retries: searchResult.retries || 0,
          latencyMs: searchResult.latencyMs || 0,
        },
        errors: searchResult.error ? [searchResult.error] : [],
        quota: searchResult.quota || null,
      });
      return out;
    },
  };
  // Capture request authority before any checkpoint, retry, or provider await.
  // Attribution/global configuration cannot substitute for this bound scope.
  for (const method of ['collectEvidence', 'search']) {
    const execute = provider[method];
    provider[method] = async function (request = {}) {
      const tenants = [request.tenantId, request.clientId, request.client_id,
        deps.tenantId, deps.clientId, deps.client_id].filter(value => value != null);
      return withResearchProviderScope(tenants[0], () => {
        for (const tenant of tenants) assertResearchProviderAllowed('google_places', tenant);
        return execute.call(this, request);
      });
    };
  }
  return provider;
}

/**
 * Derive Places query strings from segment + evidence type.
 * Query templates live in the market hypothesis registry — not in Scout execution.
 */
function buildQueriesForEvidence({ segment, evidenceType, cities, state, entityName = null, website = null, discoveryConcept = null }) {
  if (entityName) {
    const loc = cities.length ? (state ? `${cities[0]} ${state}` : cities[0]) : '';
    const base = `${entityName}${loc ? ` ${loc}` : ''}`.trim();
    if (website) return [base, website];
    return [base];
  }

  if (discoveryConcept) {
    return cities.map(city => `${discoveryConcept} ${city}${state ? ` ${state}` : ''}`.trim());
  }

  const hypothesis = resolveMarketHypothesisBySegmentKey(segment);
  const geoRows = cities.map((city) => ({ city, state: state || '' }));

  if (hypothesis) {
    const sourceFilter =
      evidenceType === INVESTIGATIVE_EVIDENCE.IDENTITY
        ? [SEARCH_SOURCES.GOOGLE_PLACES, SEARCH_SOURCES.PUBLIC_BUSINESS_DATA]
        : [SEARCH_SOURCES.GOOGLE_PLACES];

    const seen = new Set();
    const queries = [];
    for (const geo of geoRows) {
      const workloads = expandSearchStrategies(hypothesis, geo, { sources: sourceFilter });
      for (const row of workloads) {
        if (!row.query || seen.has(row.query)) continue;
        seen.add(row.query);
        queries.push(row.query);
      }
    }
    if (queries.length) return queries;
  }

  // Fallback when no registry hypothesis — provider still owns query shape.
  const segmentLabel = String(segment || 'business').replace(/_/g, ' ');
  return geoRows.map((geo) => {
    const loc = geo.state ? `${geo.city} ${geo.state}` : geo.city;
    return `${segmentLabel} ${loc}`.trim();
  });
}

async function searchWithRetry(querySpec, apiKey, fetchImpl, opts = {}) {
  const { requireWebsite = true, seenPlaceIds, out, knownHits = null } = opts;
  const captured = new Set();
  const attempts = [];
  let retries = 0;
  let latencyMs = 0;
  let quota = null;
  let error = null;
  let nextPageToken = opts.pageToken || null;
  let rawPageResults = [];
  let attempt = 0;
  let acceptedForQuery = 0;
  const limit = Math.min(Number(querySpec.limit) || DEFAULT_LIMIT, DEFAULT_LIMIT);

  while (attempt <= MAX_RETRIES) {
    attempt += 1;
    let pageAccepted = 0;
    const page = await fetchPlacesPage(querySpec, apiKey, fetchImpl, nextPageToken, opts);
    attempts.push(page.attempt);
    latencyMs += page.attempt.latencyMs || 0;
    if (page.attempt.quota) quota = page.attempt.quota;

    if (page.failed) {
      error = {
        code: page.attempt.googleStatus
          ? `google_places_status_${page.attempt.googleStatus}`
          : 'google_places_fetch_failed',
        message: page.attempt.googleError || page.attempt.googleStatus || 'Places Text Search failed',
        query: page.attempt.query,
        httpStatus: page.attempt.httpStatus,
        googleStatus: page.attempt.googleStatus,
        noNetwork: page.attempt.noNetwork === true && attempts.every(row => row.noNetwork === true),
      };
      const retryable = RETRYABLE_GOOGLE_STATUS.has(page.attempt.googleStatus);
      if (retryable && attempt <= MAX_RETRIES) {
        retries += 1;
        continue;
      }
      break;
    }

    // Single-page mode has not processed any earlier successful page. A prior
    // error here can only be a failed search retry; do not poison its recovery.
    if (opts.singlePage) error = null;
    if (opts.singlePage && page.attempt.resultCount > DEFAULT_LIMIT) {
      error = { code: 'unexpected_page_size', message: 'Provider page exceeds approved result bound' };
      break;
    }
    rawPageResults = page.results || [];
    for (const hit of page.results || []) {
      if (!hit.place_id) continue;
      if (seenPlaceIds.has(hit.place_id)) {
        // A staged query is a self-contained replay unit, not a delta dependent
        // on earlier query order. Reuse the known hit without another detail call.
        if (knownHits?.has(hit.place_id) && !captured.has(hit.place_id)) {
          const { _scoutTicketRefs, ...rawHit } = knownHits.get(hit.place_id);
          out.push({ ...rawHit, industry: querySpec.industry || null, discoveryQuery: querySpec.industry || null }); captured.add(hit.place_id);
        }
        continue;
      }
      seenPlaceIds.add(hit.place_id);
      let detailResult;
      let detailClaim = null;
      if (opts.tenantMemory) {
        detailClaim = await opts.tenantMemory.claimPlace(hit.place_id);
        if (detailClaim.kind==='reuse') {
          const raw = detailClaim.candidate;
          if (!(requireWebsite && !raw.website)) {
            const reused = { ...raw, industry:querySpec.industry || null, discoveryQuery:querySpec.industry || null,
              detailsReused:true, detailObservedAt:detailClaim.observedAt };
            out.push(reused); captured.add(hit.place_id); knownHits?.set(hit.place_id,reused);
            pageAccepted++; acceptedForQuery++;
          }
          if (acceptedForQuery>=limit) break;
          continue;
        }
      }
      try {
        detailResult = await fetchPlaceDetails(hit.place_id, apiKey, fetchImpl, { requireWebsite, apiDeps: opts.apiDeps,
          useCache:detailClaim ? false : opts.useCache });
      } catch (failure) {
        if (detailClaim) await opts.tenantMemory.finishPlace(detailClaim,null);
        rethrowResearchProviderControl(failure);
        if (!opts.singlePage) throw failure;
        error = { code: 'google_places_details_failed', message: 'Place details request failed', noNetwork: false };
        break;
      }
      if (detailResult.error && detailClaim) {
        await opts.tenantMemory.finishPlace(detailClaim,null);
        detailClaim = null;
      }
      if (detailResult.error && !error) error = detailResult.error;
      if (detailResult.error?.googleStatus === 'REQUEST_DENIED') {
        error = detailResult.error;
        break;
      }
      const details = detailResult.result;
      if (detailClaim && (!details || details.place_id!==hit.place_id)) {
        await opts.tenantMemory.finishPlace(detailClaim,null);
        throw Object.assign(new Error('scout_tenant_memory_detail_identity_unconfirmed'),{retryAllowed:false});
      }
      const website = details?.website || null;
      const accepted = !(requireWebsite && !website);

      const acceptedHit = {
        companyName: details?.name || hit.name || 'Unknown',
        website: website ? normalizeDomain(website) : null,
        phone: details?.formatted_phone_number || null,
        address: details?.formatted_address || hit.formatted_address || '',
        placeId: details?.place_id || hit.place_id,
        placeTypes: details?.types || hit.types || [],
        googleRating: details?.rating ?? hit.rating ?? null,
        source: 'google_places',
        industry: querySpec.industry || null,
        discoveryQuery: querySpec.industry || null,
        snippet: '',
      };
      // Persist the observed business immediately, before page/ticket admission.
      // A later crash cannot authorize another paid detail request on a new query.
      if (detailClaim) await opts.tenantMemory.finishPlace(detailClaim,acceptedHit);
      if (!accepted) continue;
      out.push(acceptedHit);
      captured.add(hit.place_id);
      if (knownHits) knownHits.set(hit.place_id, acceptedHit);
      pageAccepted += 1;
      acceptedForQuery += 1;
      if (acceptedForQuery >= limit) break;
    }

    if (typeof page.commitRecord === 'function') {
      await page.commitRecord({
        businessesAccepted: pageAccepted,
        candidatesCreated: opts.singlePage && error ? 0 : pageAccepted,
      });
    }

    if (error?.googleStatus === 'REQUEST_DENIED') break;
    nextPageToken = page.nextPageToken || null;
    if (opts.singlePage || !nextPageToken || acceptedForQuery >= limit) break;
  }

  return { attempts, retries, latencyMs, quota, error, nextPageToken, rawResults: rawPageResults };
}

// Search already returns rating. Re-requesting it adds the Atmosphere SKU.
// Legacy request field is "type"; the returned JSON field remains "types".
const PLACE_DETAILS_FIELDS =
  'name,formatted_address,formatted_phone_number,website,place_id,type,address_component,business_status';

async function fetchPlacesPage(querySpec, apiKey, fetchImpl, pageToken = null, opts = {}) {
  const q = `${querySpec.industry || ''} ${querySpec.location || ''}`.trim();
  const started = Date.now();
  if (!q) {
    return {
      results: [],
      nextPageToken: null,
      failed: true,
      attempt: {
        query: '',
        httpStatus: null,
        googleStatus: null,
        googleError: 'empty_query',
        resultCount: 0,
        latencyMs: 0,
        retries: 0,
        quota: null,
      },
    };
  }

  try {
    const traced = await legacyTextSearch({
      query: q,
      apiKey,
      pageToken,
      fetchImpl,
      useCache: opts.useCache,
      deferRecord: true,
      record: {
        caller: 'PlacesProvider',
        feature: PLACES_FEATURES.DISCOVERY,
        providerId: 'google_places',
      },
    }, opts.apiDeps);
    const latencyMs = Date.now() - started;
    const httpStatus = traced.httpStatus;
    if (!traced.ok) {
      const failureStatus = traced.applicationFailure === true ? traced.googleStatus : null;
      if (typeof traced.commitRecord === 'function') {
        await traced.commitRecord({
          businessesReturned: 0,
          businessesAccepted: 0,
          candidatesCreated: 0,
        });
      }
      return {
        results: [],
        nextPageToken: null,
        failed: true,
        attempt: {
          query: q,
          httpStatus,
          cacheHit: traced.cacheHit, cacheStrategy: traced.cacheStrategy,
          cacheAgeMs: traced.cacheAgeMs, cacheExpiresInMs: traced.cacheExpiresInMs,
          googleStatus: failureStatus,
          googleError: failureStatus || `http_${httpStatus}`,
          resultCount: 0,
          latencyMs,
          retries: 0,
          quota: failureStatus === 'OVER_QUERY_LIMIT' ? { status: failureStatus } : null,
        },
      };
    }

    const data = traced.data || {};
    const googleStatus = data.status || null;
    // Raw provider prose can contain credentials or personal data, even on success.
    const googleError = null;
    const ok = SUCCESS_GOOGLE_STATUS.has(googleStatus);
    const limit = Math.min(Number(querySpec.limit) || DEFAULT_LIMIT, DEFAULT_LIMIT);
    const results = ok ? (data.results || []).slice(0, limit) : [];
    return {
      results,
      nextPageToken: ok ? data.next_page_token || null : null,
      failed: !ok,
      commitRecord: traced.commitRecord,
      attempt: {
        query: q,
        httpStatus,
        cacheHit: traced.cacheHit, cacheStrategy: traced.cacheStrategy,
        cacheAgeMs: traced.cacheAgeMs, cacheExpiresInMs: traced.cacheExpiresInMs,
        googleStatus,
        googleError,
        resultCount: Array.isArray(data.results) ? data.results.length : 0,
        latencyMs,
        retries: 0,
        quota: googleStatus === 'OVER_QUERY_LIMIT' ? { status: googleStatus } : null,
      },
    };
  } catch (err) {
    rethrowResearchProviderControl(err);
    return {
      results: [],
      nextPageToken: null,
      failed: true,
      attempt: {
        query: q,
        httpStatus: null,
        googleStatus: null,
        googleError: err.apiUsageNoNetwork ? 'provider_budget_denied' : 'google_places_transport_failed',
        noNetwork: err.apiUsageNoNetwork === true,
        resultCount: 0,
        latencyMs: Date.now() - started,
        retries: 0,
        quota: null,
      },
    };
  }
}

async function fetchPlaceDetails(placeId, apiKey, fetchImpl, opts = {}) {
  if (!placeId) return { result: null, error: null };
  const requireWebsite = opts.requireWebsite !== false;
  const traced = await legacyPlaceDetails({
    placeId,
    fields: PLACE_DETAILS_FIELDS,
    apiKey,
    fetchImpl,
    useCache: opts.useCache,
    deferRecord: true,
    record: {
      caller: 'PlacesProvider',
      feature: PLACES_FEATURES.DISCOVERY,
      providerId: 'google_places',
    },
  }, opts.apiDeps);
  if (!traced.ok) {
    if (typeof traced.commitRecord === 'function') {
      await traced.commitRecord({ businessesReturned: 0, businessesAccepted: 0 });
    }
    // A detail failure is not evidence that the business has no website/contact.
    // Keep the sanitized provider status visible without retrying or admitting it.
    const googleStatus = traced.applicationFailure === true ? traced.googleStatus : null;
    return { result: null, error: {
      code: googleStatus ? `google_places_status_${googleStatus}` : 'google_places_details_failed',
      message: googleStatus || `http_${traced.httpStatus}`,
      endpoint: 'place_details',
      httpStatus: traced.httpStatus,
      googleStatus,
    } };
  }
  const result = traced.data?.result || null;
  const website = result?.website || null;
  const accepted = !(requireWebsite && !website);
  if (typeof traced.commitRecord === 'function') {
    await traced.commitRecord({
      businessesReturned: result ? 1 : 0,
      businessesAccepted: accepted ? 1 : 0,
      candidatesCreated: accepted ? 1 : 0,
    });
  }
  return { result, error: null };
}

function normalizeDomain(website) {
  try {
    const u = new URL(
      String(website).startsWith('http') ? website : `https://${website}`
    );
    return u.hostname.replace(/^www\./, '');
  } catch {
    return String(website)
      .replace(/^https?:\/\//, '')
      .replace(/^www\./, '')
      .split('/')[0];
  }
}

module.exports = {
  createPlacesProvider,
  normalizeDomain,
  buildQueriesForEvidence,
  createProviderExecution,
};

