'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const express = require('express');

const routePath = path.join(__dirname, '..', 'routes', 'contentLearning.js');
const serverPath = path.join(__dirname, '..', 'server.js');
const uiPath = path.join(__dirname, '..', 'public', 'content-outcomes.html');

const source = fs.readFileSync(routePath, 'utf8');
const serverSource = fs.readFileSync(serverPath, 'utf8');
const uiSource = fs.readFileSync(uiPath, 'utf8');

const {
  createContentPublication,
  addPerformanceSnapshot,
  addBusinessOutcome,
  createMemoryStore,
} = require('../services/contentOutcomeIntelligence');
const {
  createMemoryStore: createLearningMemoryStore,
  evaluateContentPublication,
  listContentLearnings,
  generateContentRecommendation,
  ContentLearningError,
} = require('../services/contentLearning');

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
  return { status: res.status, json, text };
}

describe('contentLearning routes (static)', () => {
  it('registers learning endpoints with auth roles', () => {
    assert.match(source, /requireRole\('admin', 'manager'\)/);
    assert.match(source, /Cache-Control',\s*'no-store'/);
    for (const route of [
      '/api/content-learning/evaluate/:publicationId',
      '/api/content-learnings',
      '/api/content-learnings/:id',
      '/api/content-learnings/recompute',
      '/api/paige/content-recommendation',
    ]) {
      assert.match(
        source,
        new RegExp(route.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'))
      );
    }
  });

  it('is mounted from server.js', () => {
    assert.match(serverSource, /require\('\.\/routes\/contentLearning'\)/);
  });

  it('extends content-outcomes UI with learnings and recommendation panels', () => {
    assert.match(uiSource, /Content learnings/);
    assert.match(uiSource, /Paige recommends/);
    assert.match(uiSource, /\/api\/content-learning\/evaluate/);
    assert.match(uiSource, /\/api\/paige\/content-recommendation/);
    assert.doesNotMatch(uiSource, /post_score|vanity score/i);
  });

  it('registers npm CLI script', () => {
    const packageJson = JSON.parse(
      fs.readFileSync(path.join(__dirname, '..', 'package.json'), 'utf8')
    );
    assert.equal(
      packageJson.scripts['content:learning'],
      'node scripts/contentLearning.js'
    );
  });
});

describe('contentLearning service-backed HTTP slice', () => {
  it('evaluates, lists learnings, and recommends through a thin router', async () => {
    const outcomeStore = createMemoryStore();
    const learningStore = createLearningMemoryStore();
    const opts = { store: outcomeStore, outcomeStore, learningStore };

    const app = express();
    app.use(express.json());

    app.post('/api/content-learning/evaluate/:publicationId', async (req, res) => {
      try {
        const result = await evaluateContentPublication(req.params.publicationId, {
          ...opts,
          clientId: Number(req.body.clientId),
        });
        res.json(result);
      } catch (err) {
        res.status(err.status || 500).json({
          error: err.code || 'failed',
          message: err.message,
        });
      }
    });

    app.get('/api/content-learnings', async (req, res) => {
      const items = await listContentLearnings(
        { clientId: Number(req.query.client_id) },
        opts
      );
      res.json({ items, count: items.length });
    });

    app.post('/api/paige/content-recommendation', async (req, res) => {
      const rec = await generateContentRecommendation(
        { ...req.body, clientId: Number(req.body.clientId) },
        opts
      );
      res.json(rec);
    });

    const { base, close } = await listen(app);
    try {
      const pub = await createContentPublication(
        {
          clientId: 1,
          contentArtifactId: 'http-1',
          objective: 'category_creation',
          topic: 'Software should learn you',
          channel: 'linkedin',
        },
        opts
      );
      await addPerformanceSnapshot(
        pub.id,
        {
          clientId: 1,
          impressions: 18750,
          membersReached: 12645,
          metadata: { outOfNetworkPct: 97 },
        },
        opts
      );
      await addBusinessOutcome(
        pub.id,
        {
          clientId: 1,
          outcomeType: 'partner_conversation',
          attribution: 'direct',
        },
        opts
      );

      const evaluated = await request(
        base,
        'POST',
        `/api/content-learning/evaluate/${pub.id}`,
        { clientId: 1 }
      );
      assert.equal(evaluated.status, 200);
      assert.ok(evaluated.json.learnings.length >= 1);
      assert.equal(evaluated.json.learnings[0].status, 'signal');

      const listed = await request(base, 'GET', '/api/content-learnings?client_id=1');
      assert.equal(listed.status, 200);
      assert.ok(listed.json.count >= 1);

      const rec = await request(base, 'POST', '/api/paige/content-recommendation', {
        clientId: 1,
        learningObjective: 'category_creation',
        channel: 'linkedin',
      });
      assert.equal(rec.status, 200);
      assert.ok(rec.json.supporting_learning_ids.length >= 1);
      assert.equal(rec.json.autonomousPublish, false);
    } finally {
      await close();
    }
  });

  it('maps ContentLearningError for missing learning', async () => {
    const learningStore = createLearningMemoryStore();
    await assert.rejects(
      () =>
        require('../services/contentLearning').getContentLearning('missing', {
          learningStore,
          clientId: 1,
        }),
      (err) => err instanceof ContentLearningError && err.code === 'learning_not_found'
    );
  });
});
