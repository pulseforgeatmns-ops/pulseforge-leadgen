'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const express = require('express');

const routePath = path.join(__dirname, '..', 'routes', 'contentOutcomeIntelligence.js');
const serverPath = path.join(__dirname, '..', 'server.js');
const packagePath = path.join(__dirname, '..', 'package.json');
const cliPath = path.join(__dirname, '..', 'scripts', 'contentOutcome.js');
const uiPath = path.join(__dirname, '..', 'public', 'content-outcomes.html');

const source = fs.readFileSync(routePath, 'utf8');
const serverSource = fs.readFileSync(serverPath, 'utf8');
const packageJson = JSON.parse(fs.readFileSync(packagePath, 'utf8'));
const cliSource = fs.readFileSync(cliPath, 'utf8');
const uiSource = fs.readFileSync(uiPath, 'utf8');

const {
  createMemoryStore,
  createContentPublication,
  ContentOutcomeError,
} = require('../services/contentOutcomeIntelligence');

// Route module uses default postgres pool; for HTTP tests we exercise static
// wiring and a thin in-process router that delegates to the service memory store.

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

async function request(base, method, urlPath, body) {
  const url = new URL(urlPath, base);
  const res = await fetch(url, {
    method,
    headers: {
      Accept: 'application/json',
      ...(body == null ? {} : { 'Content-Type': 'application/json' }),
    },
    body: body == null ? undefined : JSON.stringify(body),
  });
  const text = await res.text();
  let json = null;
  try {
    json = text ? JSON.parse(text) : null;
  } catch (_) {
    json = null;
  }
  return { status: res.status, headers: res.headers, text, json };
}

describe('contentOutcomeIntelligence routes (static)', () => {
  it('registers content outcome endpoints with auth roles', () => {
    assert.match(source, /requireRole\('admin', 'manager'\)/);
    assert.match(source, /Cache-Control',\s*'no-store'/);

    const expected = [
      '/api/content-publications',
      '/api/content-publications/:id',
      '/api/content-publications/:id/performance',
      '/api/content-publications/:id/outcomes',
      '/api/content-publications/:id/signals',
      '/api/content-publications/:id/timeline',
      '/api/content-outcomes',
      '/api/content-outcomes/compare',
      '/api/content-outcomes/recent',
      '/content-outcomes',
    ];
    for (const route of expected) {
      assert.match(source, new RegExp(route.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')));
    }
  });

  it('is mounted from server.js', () => {
    assert.match(serverSource, /require\('\.\/routes\/contentOutcomeIntelligence'\)/);
  });

  it('registers npm CLI script', () => {
    assert.equal(packageJson.scripts['content:outcome'], 'node scripts/contentOutcome.js');
  });

  it('ships CLI commands and operator UI', () => {
    for (const cmd of [
      'publish',
      'performance',
      'add-outcome',
      'add-signal',
      'show',
      'list',
      'compare',
    ]) {
      assert.match(cliSource, new RegExp(`case '${cmd}'`));
    }
    assert.match(uiSource, /Record Content Outcome/);
    assert.match(uiSource, /\/api\/content-publications/);
    assert.doesNotMatch(uiSource, /post_score|vanity/i);
  });
});

describe('contentOutcomeIntelligence service-backed HTTP slice', () => {
  it('creates publication and retrieves outcomes through a thin router', async () => {
    const store = createMemoryStore();
    const {
      createContentPublication: createPub,
      addPerformanceSnapshot,
      addBusinessOutcome,
      addQualitativeSignal,
      getPublicationOutcome,
      listContentOutcomes,
      compareContentOutcomes,
    } = require('../services/contentOutcomeIntelligence');

    const app = express();
    app.use(express.json());

    app.post('/api/content-publications', async (req, res) => {
      try {
        const created = await createPub(req.body, { store });
        res.status(201).json(created);
      } catch (err) {
        res.status(err.status || 400).json({ error: err.code, message: err.message });
      }
    });
    app.post('/api/content-publications/:id/performance', async (req, res) => {
      try {
        const snap = await addPerformanceSnapshot(req.params.id, req.body, { store });
        res.status(201).json(snap);
      } catch (err) {
        res.status(err.status || 400).json({ error: err.code, message: err.message });
      }
    });
    app.post('/api/content-publications/:id/outcomes', async (req, res) => {
      try {
        const out = await addBusinessOutcome(req.params.id, req.body, { store });
        res.status(201).json(out);
      } catch (err) {
        res.status(err.status || 400).json({ error: err.code, message: err.message });
      }
    });
    app.post('/api/content-publications/:id/signals', async (req, res) => {
      try {
        const sig = await addQualitativeSignal(req.params.id, req.body, { store });
        res.status(201).json(sig);
      } catch (err) {
        res.status(err.status || 400).json({ error: err.code, message: err.message });
      }
    });
    app.get('/api/content-publications/:id/outcomes', async (req, res) => {
      try {
        const full = await getPublicationOutcome(req.params.id, {
          store,
          clientId: Number(req.query.client_id),
        });
        res.json(full);
      } catch (err) {
        res.status(err.status || 400).json({ error: err.code, message: err.message });
      }
    });
    app.get('/api/content-outcomes', async (req, res) => {
      try {
        const items = await listContentOutcomes(
          { clientId: Number(req.query.client_id) },
          { store }
        );
        res.json({ items });
      } catch (err) {
        res.status(err.status || 400).json({ error: err.code, message: err.message });
      }
    });
    app.get('/api/content-outcomes/compare', async (req, res) => {
      try {
        const cmp = await compareContentOutcomes(
          { clientId: Number(req.query.client_id), groupBy: 'objective' },
          { store }
        );
        res.json(cmp);
      } catch (err) {
        res.status(err.status || 400).json({ error: err.code, message: err.message });
      }
    });

    const { base, close } = await listen(app);
    try {
      const created = await request(base, 'POST', '/api/content-publications', {
        clientId: 1,
        contentArtifactId: 'http-artifact',
        objective: 'launch_runway',
        channel: 'linkedin',
      });
      assert.equal(created.status, 201);
      const id = created.json.id;

      const perf = await request(base, 'POST', `/api/content-publications/${id}/performance`, {
        clientId: 1,
        impressions: 1200,
        comments: 4,
      });
      assert.equal(perf.status, 201);
      assert.equal(perf.json.impressions, 1200);

      const outcome = await request(base, 'POST', `/api/content-publications/${id}/outcomes`, {
        clientId: 1,
        outcomeType: 'builder_connection',
        attribution: 'direct',
        evidenceId: 'ev-1',
      });
      assert.equal(outcome.status, 201);

      const signal = await request(base, 'POST', `/api/content-publications/${id}/signals`, {
        clientId: 1,
        signalType: 'message_resonance',
        description: 'Confidence/correction generated discussion.',
      });
      assert.equal(signal.status, 201);

      const history = await request(
        base,
        'GET',
        `/api/content-publications/${id}/outcomes?client_id=1`
      );
      assert.equal(history.status, 200);
      assert.equal(history.json.performanceSnapshots.length, 1);
      assert.equal(history.json.businessOutcomes.length, 1);
      assert.equal(history.json.qualitativeSignals.length, 1);

      const isolated = await request(
        base,
        'GET',
        `/api/content-publications/${id}/outcomes?client_id=99`
      );
      assert.equal(isolated.status, 404);

      const list = await request(base, 'GET', '/api/content-outcomes?client_id=1');
      assert.equal(list.status, 200);
      assert.equal(list.json.items.length, 1);

      const cmp = await request(base, 'GET', '/api/content-outcomes/compare?client_id=1');
      assert.equal(cmp.status, 200);
      assert.equal(cmp.json.totalPublications, 1);
      assert.equal(cmp.json.vanityScore, null);
    } finally {
      await close();
    }
  });

  it('rejects invalid outcome types', async () => {
    const store = createMemoryStore();
    await assert.rejects(
      () =>
        createContentPublication(
          { clientId: 1, contentArtifactId: 'x', objective: 'not_real' },
          { store }
        ),
      (err) => err instanceof ContentOutcomeError
    );
  });
});
