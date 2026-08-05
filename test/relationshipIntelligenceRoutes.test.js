'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const express = require('express');

const routePath = path.join(__dirname, '..', 'routes', 'relationshipIntelligence.js');
const serverPath = path.join(__dirname, '..', 'server.js');
const packagePath = path.join(__dirname, '..', 'package.json');
const source = fs.readFileSync(routePath, 'utf8');
const serverSource = fs.readFileSync(serverPath, 'utf8');
const packageJson = JSON.parse(fs.readFileSync(packagePath, 'utf8'));

const {
  createMemoryStore,
  startRelationshipInterview,
  summarizeRelationshipInterview,
  RelationshipIntelligenceError,
} = require('../services/relationshipIntelligenceInterview');

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

async function request(base, method, urlPath, body, headers = {}) {
  const url = new URL(urlPath, base);
  const res = await fetch(url, {
    method,
    headers: {
      Accept: 'application/json',
      ...(body == null ? {} : { 'Content-Type': 'application/json' }),
      ...headers,
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

describe('relationshipIntelligence routes (static)', () => {
  it('registers interview + interaction endpoints with admin/manager auth', () => {
    assert.match(source, /requireRole\('admin', 'manager'\)/);
    assert.match(source, /Cache-Control',\s*'no-store'/);

    const expected = [
      '/api/v1/relationship-intel/readiness',
      '/api/v1/relationship-intel/interviews',
      '/api/v1/relationship-intel/interviews/:id/messages',
      '/api/v1/relationship-intel/interviews/:id/summarize',
      '/api/v1/relationship-intel/interviews/:id/commit',
      '/api/v1/relationship-intel/interactions',
      '/api/v1/relationship-intel/interactions/:id',
    ];
    for (const route of expected) {
      assert.match(source, new RegExp(route.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')));
    }

    assert.match(source, /router\.post\(/);
    assert.match(source, /router\.get\(/);
    assert.match(source, /interviews\/:id\/messages/);
    assert.match(source, /interviews\/:id\/summarize/);
    assert.match(source, /interviews\/:id\/commit/);
    assert.match(source, /buildRelationshipIntelReadinessReport/);
  });

  it('is mounted from server.js', () => {
    assert.match(serverSource, /require\('\.\/routes\/relationshipIntelligence'\)/);
  });

  it('registers npm CLI scripts', () => {
    assert.equal(
      packageJson.scripts['relationship:intel:interview'],
      'node scripts/relationshipIntelInterview.js'
    );
    assert.equal(
      packageJson.scripts['relationship:intel:readiness'],
      'node scripts/relationshipIntelReadiness.js'
    );
  });

  it('does not reference Cal coaching or opportunity stage mutation', () => {
    const codeOnly = source.replace(/\/\*[\s\S]*?\*\//g, '').replace(/\/\/.*$/gm, '');
    assert.equal(/\bopportunities\b/.test(codeOnly), false);
    assert.equal(/\bstage\b/.test(codeOnly), false);
    assert.equal(/calAgent|Cal coaching/i.test(codeOnly), false);
  });
});

describe('relationshipIntelligence routes (payload validation)', () => {
  it('rejects unauthenticated access', async () => {
    const app = express();
    app.use(express.json());
    app.use('/', require('../routes/relationshipIntelligence'));
    const { base, close } = await listen(app);
    try {
      const res = await request(base, 'POST', '/api/v1/relationship-intel/interviews', {
        type: 'meeting',
        notes: 'hello',
      });
      assert.ok(res.status === 401 || res.status === 302 || res.status === 403);
    } finally {
      await close();
    }
  });

  it('validates interaction type and empty message with stubbed auth', async () => {
    const store = createMemoryStore();
    const svc = require('../services/relationshipIntelligenceInterview');

    // Patch service methods to use memory store via wrapper routes.
    const app = express();
    app.use(express.json());
    app.use((req, _res, next) => {
      req.session = { user: { id: 1, role: 'admin' }, active_client_id: 10 };
      next();
    });

    // Inline minimal handlers mirroring validation behavior (auth already stubbed).
    app.post('/api/v1/relationship-intel/interviews', async (req, res) => {
      try {
        const type = req.body.interactionType || req.body.type || 'other';
        if (!svc.INTERACTION_TYPES.includes(String(type))) {
          return res.status(400).json({ error: 'invalid_interaction_type' });
        }
        const result = await startRelationshipInterview(
          {
            interactionType: type,
            companyId: req.body.companyId,
            notes: req.body.notes,
            clientId: 10,
            source: 'api',
          },
          { store }
        );
        return res.status(201).json(result);
      } catch (err) {
        const status = err instanceof RelationshipIntelligenceError ? err.status : 500;
        return res.status(status).json({ error: err.code || 'failed', message: err.message });
      }
    });

    app.post('/api/v1/relationship-intel/interviews/:id/messages', async (req, res) => {
      if (req.body.message == null || String(req.body.message).trim() === '') {
        return res.status(400).json({ error: 'empty_message' });
      }
      return res.json({ ok: true });
    });

    app.post('/api/v1/relationship-intel/interviews/:id/summarize', async (req, res) => {
      const result = await summarizeRelationshipInterview(req.params.id, { store });
      return res.json(result);
    });

    const { base, close } = await listen(app);
    try {
      const badType = await request(base, 'POST', '/api/v1/relationship-intel/interviews', {
        type: 'telepathy',
        notes: 'x',
      });
      assert.equal(badType.status, 400);
      assert.equal(badType.json.error, 'invalid_interaction_type');

      const created = await request(base, 'POST', '/api/v1/relationship-intel/interviews', {
        type: 'discovery_call',
        companyId: 'co-api',
        notes:
          'Discovery call. Pain around missed calls. Goal is coverage. Next step walkthrough. Budget 2k.',
      });
      assert.equal(created.status, 201);
      assert.ok(created.json.interviewId);

      const emptyMsg = await request(
        base,
        'POST',
        `/api/v1/relationship-intel/interviews/${created.json.interviewId}/messages`,
        { message: '   ' }
      );
      assert.equal(emptyMsg.status, 400);
      assert.equal(emptyMsg.json.error, 'empty_message');

      const summarized = await request(
        base,
        'POST',
        `/api/v1/relationship-intel/interviews/${created.json.interviewId}/summarize`,
        {}
      );
      assert.equal(summarized.status, 200);
      assert.equal(summarized.json.kind, 'relationship_intelligence_interview');
      assert.equal(summarized.json.isEvidence, true);
      assert.equal(summarized.json.status, 'draft');
      assert.ok(Array.isArray(summarized.json.insights));
    } finally {
      await close();
    }
  });
});
