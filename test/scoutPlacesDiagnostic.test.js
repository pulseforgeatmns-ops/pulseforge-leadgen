'use strict';

/**
 * Scout Places diagnostic unit tests — no live Google calls.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  SCOUT_PLACES_ENDPOINT_FAMILY,
  SCOUT_PLACES_AUTH_STYLE,
  SCOUT_PLACES_TEXTSEARCH_URL,
  buildScoutPlacesTextSearchUrl,
  scoutPlacesUrlHostPath,
} = require('../services/scoutPublicSourcing');
const {
  fingerprintApiKey,
  railwayRuntimeIdentity,
  diagnoseScoutPlaces,
  formatScoutPlacesDiagnostic,
  PLACES_NEW_SEARCH_TEXT_URL,
} = require('../services/scoutPlacesDiagnostic');

describe('scoutPlacesDiagnostic', () => {
  it('fingerprints keys without exposing the full secret', () => {
    assert.equal(fingerprintApiKey(''), null);
    assert.equal(fingerprintApiKey('abcdefghijklmnop'), 'abcd…mnop');
    const printed = formatScoutPlacesDiagnostic({
      ok: false,
      endpointFamily: SCOUT_PLACES_ENDPOINT_FAMILY,
      authStyle: SCOUT_PLACES_AUTH_STYLE,
      request: { host: 'maps.googleapis.com', path: '/maps/api/place/textsearch/json' },
      httpStatus: 200,
      googleStatus: 'REQUEST_DENIED',
      googleErrorMessage: 'This API project is not authorized to use this API.',
      keyFingerprint: 'abcd…mnop',
      keyPresent: true,
      railway: { testedLabel: 'web / production' },
      error: 'google_places_status_REQUEST_DENIED',
      likelyCauseHints: ['legacy vs new'],
    });
    assert.doesNotMatch(printed, /abcdefghijklmnop/);
    assert.match(printed, /abcd…mnop/);
    assert.match(printed, /REQUEST_DENIED/);
  });

  it('Scout text-search builder uses legacy host/path and query-key auth', () => {
    assert.equal(SCOUT_PLACES_ENDPOINT_FAMILY, 'legacy_places_text_search_details');
    assert.equal(SCOUT_PLACES_AUTH_STYLE, 'query_param_key');
    const url = buildScoutPlacesTextSearchUrl({
      query: 'coffee shop Manchester NH',
      apiKey: 'secretKEY123456',
    });
    assert.equal(url.origin + url.pathname, SCOUT_PLACES_TEXTSEARCH_URL);
    assert.equal(url.searchParams.get('key'), 'secretKEY123456');
    const hostPath = scoutPlacesUrlHostPath(url);
    assert.equal(hostPath.host, 'maps.googleapis.com');
    assert.equal(hostPath.path, '/maps/api/place/textsearch/json');
    assert.equal(hostPath.path.includes('key'), false);
  });

  it('reports missing key without calling Google', async () => {
    let called = false;
    const report = await diagnoseScoutPlaces({
      apiKey: '',
      comparePlacesNew: false,
      fetchImpl: async () => {
        called = true;
        throw new Error('should not fetch');
      },
    });
    assert.equal(called, false);
    assert.equal(report.ok, false);
    assert.equal(report.keyPresent, false);
    assert.equal(report.error, 'GOOGLE_PLACES_KEY_missing');
    assert.equal(report.crmWritesMade, false);
    assert.equal(report.outreachCopyGenerated, false);
    assert.equal(report.placeholdersCreated, false);
    assert.equal(report.fullKeyLogged, false);
    assert.equal(report.endpointFamily, 'legacy_places_text_search_details');
    assert.equal(report.request.host, 'maps.googleapis.com');
    assert.equal(report.request.path, '/maps/api/place/textsearch/json');
  });

  it('surfaces REQUEST_DENIED with Google error_message from Scout path', async () => {
    const secret = 'AIzaSyTESTKEY1234567890ABCD';
    let requestedUrl = '';
    const report = await diagnoseScoutPlaces({
      apiKey: secret,
      comparePlacesNew: false,
      fetchImpl: async (url) => {
        requestedUrl = String(url);
        assert.match(requestedUrl, /maps\.googleapis\.com\/maps\/api\/place\/textsearch\/json/);
        return {
          ok: true,
          status: 200,
          async text() {
            return JSON.stringify({
              status: 'REQUEST_DENIED',
              error_message:
                'This API key is not authorized to use this service or API.',
            });
          },
        };
      },
    });

    assert.equal(report.ok, false);
    assert.equal(report.httpStatus, 200);
    assert.equal(report.googleStatus, 'REQUEST_DENIED');
    assert.match(report.googleErrorMessage, /not authorized/i);
    assert.equal(report.keyFingerprint, 'AIza…ABCD');
    assert.equal(report.keyPresent, true);
    assert.equal(report.error, 'google_places_status_REQUEST_DENIED');
    assert.ok(report.likelyCauseHints.length >= 1);
    assert.equal(report.crmWritesMade, false);
    const serialized = JSON.stringify(report);
    assert.doesNotMatch(serialized, new RegExp(secret));
    assert.doesNotMatch(serialized, /"key":/);
    assert.match(requestedUrl, /key=/);
  });

  it('marks ok on ZERO_RESULTS from legacy Text Search', async () => {
    const report = await diagnoseScoutPlaces({
      apiKey: 'AIzaSyOKKEY1234567890WXYZ',
      comparePlacesNew: false,
      fetchImpl: async () => ({
        ok: true,
        status: 200,
        async text() {
          return JSON.stringify({ status: 'ZERO_RESULTS', results: [] });
        },
      }),
    });
    assert.equal(report.ok, true);
    assert.equal(report.googleStatus, 'ZERO_RESULTS');
    assert.equal(report.resultCount, 0);
    assert.equal(report.error, null);
  });

  it('compares Places API New when requested and flags New-only success', async () => {
    const calls = [];
    const report = await diagnoseScoutPlaces({
      apiKey: 'AIzaSyCMPKEY1234567890ZZZZ',
      comparePlacesNew: true,
      fetchImpl: async (url, init) => {
        calls.push({ url: String(url), method: init?.method || 'GET' });
        if (String(url).includes('places.googleapis.com')) {
          assert.equal(init.method, 'POST');
          assert.equal(init.headers['X-Goog-Api-Key'], 'AIzaSyCMPKEY1234567890ZZZZ');
          return {
            ok: true,
            status: 200,
            async text() {
              return JSON.stringify({
                places: [{ id: 'places/abc', displayName: { text: 'Cafe' } }],
              });
            },
          };
        }
        return {
          ok: true,
          status: 200,
          async text() {
            return JSON.stringify({
              status: 'REQUEST_DENIED',
              error_message: 'legacy denied',
            });
          },
        };
      },
    });

    assert.equal(report.ok, false);
    assert.equal(report.googleStatus, 'REQUEST_DENIED');
    assert.ok(report.placesApiNewComparison);
    assert.equal(
      report.placesApiNewComparison.endpointFamily,
      'places_api_new_search_text'
    );
    assert.equal(report.placesApiNewComparison.request.host, 'places.googleapis.com');
    assert.equal(report.placesApiNewComparison.request.path, '/v1/places:searchText');
    assert.equal(report.placesApiNewComparison.httpStatus, 200);
    assert.ok(
      report.likelyCauseHints.some((h) => /Places API \(New\) probe succeeded/i.test(h))
    );
    assert.equal(calls.length, 2);
    assert.equal(PLACES_NEW_SEARCH_TEXT_URL.includes('places.googleapis.com'), true);
  });

  it('exposes railway runtime identity fields', () => {
    const prev = {
      RAILWAY_SERVICE_NAME: process.env.RAILWAY_SERVICE_NAME,
      RAILWAY_ENVIRONMENT_NAME: process.env.RAILWAY_ENVIRONMENT_NAME,
    };
    process.env.RAILWAY_SERVICE_NAME = 'pulseforge-web';
    process.env.RAILWAY_ENVIRONMENT_NAME = 'production';
    try {
      const identity = railwayRuntimeIdentity();
      assert.equal(identity.service, 'pulseforge-web');
      assert.equal(identity.environment, 'production');
      assert.equal(identity.testedLabel, 'pulseforge-web / production');
    } finally {
      if (prev.RAILWAY_SERVICE_NAME == null) delete process.env.RAILWAY_SERVICE_NAME;
      else process.env.RAILWAY_SERVICE_NAME = prev.RAILWAY_SERVICE_NAME;
      if (prev.RAILWAY_ENVIRONMENT_NAME == null) {
        delete process.env.RAILWAY_ENVIRONMENT_NAME;
      } else {
        process.env.RAILWAY_ENVIRONMENT_NAME = prev.RAILWAY_ENVIRONMENT_NAME;
      }
    }
  });
});
