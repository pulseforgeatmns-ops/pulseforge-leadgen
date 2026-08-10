'use strict';

/**
 * Cron dispatcher registration for Scout Places diagnostic.
 * Ensures GET /cron/scout-places-diagnostic never returns Unknown agent.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const express = require('express');

const cronSourcePath = path.join(__dirname, '..', 'routes', 'cron.js');
const cronSource = fs.readFileSync(cronSourcePath, 'utf8');

const {
  isScoutPlacesDiagnosticAgent,
  CRON_SPECIAL_HANDLERS,
} = require('../routes/cron');
const cronRouter = require('../routes/cron');

function listen(app) {
  const server = app.listen(0, '127.0.0.1');
  return new Promise((resolve) => {
    server.on('listening', () => {
      const { port } = server.address();
      resolve({
        server,
        base: `http://127.0.0.1:${port}`,
        async close() {
          await new Promise((r) => server.close(r));
        },
      });
    });
  });
}

describe('scout places diagnostic cron routing', () => {
  it('registers scout-places-diagnostic in the special handler dispatcher map', () => {
    assert.equal(
      CRON_SPECIAL_HANDLERS['scout-places-diagnostic'],
      'scoutPlacesDiagnostic'
    );
    assert.equal(
      CRON_SPECIAL_HANDLERS.scout_places_diagnostic,
      'scoutPlacesDiagnostic'
    );
    assert.equal(isScoutPlacesDiagnosticAgent('scout-places-diagnostic'), true);
    assert.equal(isScoutPlacesDiagnosticAgent('scout_places_diagnostic'), true);
    assert.equal(isScoutPlacesDiagnosticAgent('scout'), false);
  });

  it('keeps dedicated + :agent dispatcher wiring in routes/cron.js', () => {
    assert.match(cronSource, /isScoutPlacesDiagnosticAgent\(agent\)/);
    assert.match(
      cronSource,
      /router\.get\('\/cron\/scout-places-diagnostic',\s*handleScoutPlacesDiagnostic\)/
    );
    assert.match(
      cronSource,
      /if \(isScoutPlacesDiagnosticAgent\(agent\)\) \{\s*return handleScoutPlacesDiagnostic/
    );
    assert.doesNotMatch(
      cronSource,
      /CRON_MODULES\s*=\s*\{[^}]*scout-places-diagnostic/
    );
  });

  it('GET /cron/scout-places-diagnostic returns diagnostic JSON not Unknown agent', async () => {
    const prevSecret = process.env.CRON_SECRET;
    const prevKey = process.env.GOOGLE_PLACES_KEY;
    process.env.CRON_SECRET = 'route-test-secret';
    process.env.GOOGLE_PLACES_KEY = 'AIzaSyROUTEDIAGKEY1234ZZZZ';

    const originalFetch = globalThis.fetch;
    globalThis.fetch = async (url, init) => {
      const href = String(url);
      if (
        href.includes('maps.googleapis.com') ||
        href.includes('places.googleapis.com')
      ) {
        if (href.includes('places.googleapis.com')) {
          return {
            ok: true,
            status: 200,
            async text() {
              return JSON.stringify({ places: [{ id: 'places/1' }] });
            },
          };
        }
        return {
          ok: true,
          status: 200,
          async text() {
            return JSON.stringify({
              status: 'REQUEST_DENIED',
              error_message:
                'This API project is not authorized to use this API.',
            });
          },
        };
      }
      return originalFetch(url, init);
    };

    const app = express();
    app.use(express.json());
    app.use('/', cronRouter);
    const { base, close } = await listen(app);

    try {
      const res = await fetch(
        `${base}/cron/scout-places-diagnostic?secret=${encodeURIComponent('route-test-secret')}`
      );
      const body = await res.json();
      assert.notEqual(res.status, 400);
      assert.notEqual(body.error, 'Unknown agent: scout-places-diagnostic');
      assert.equal(body.diagnostic, 'scout_places');
      assert.equal(body.endpointFamily, 'legacy_places_text_search_details');
      assert.equal(body.request.host, 'maps.googleapis.com');
      assert.equal(body.request.path, '/maps/api/place/textsearch/json');
      assert.equal(body.googleStatus, 'REQUEST_DENIED');
      assert.ok(body.placesApiNewComparison);
      assert.equal(
        body.placesApiNewComparison.endpointFamily,
        'places_api_new_search_text'
      );
      assert.equal(body.keyPresent, true);
      assert.equal(body.keyFingerprint, 'AIza…ZZZZ');
      assert.equal(body.fullKeyLogged, false);
      assert.equal(body.crmWritesMade, false);
      const serialized = JSON.stringify(body);
      assert.doesNotMatch(serialized, /AIzaSyROUTEDIAGKEY1234ZZZZ/);

      const res2 = await fetch(
        `${base}/cron/scout_places_diagnostic?secret=${encodeURIComponent('route-test-secret')}`
      );
      const body2 = await res2.json();
      assert.notEqual(body2.error, 'Unknown agent: scout_places_diagnostic');
      assert.equal(body2.diagnostic, 'scout_places');
    } finally {
      globalThis.fetch = originalFetch;
      await close();
      if (prevSecret == null) delete process.env.CRON_SECRET;
      else process.env.CRON_SECRET = prevSecret;
      if (prevKey == null) delete process.env.GOOGLE_PLACES_KEY;
      else process.env.GOOGLE_PLACES_KEY = prevKey;
    }
  });
});
