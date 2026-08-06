'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const express = require('express');

const routePath = path.join(__dirname, '..', 'routes', 'clientIntelligence.js');
const serverPath = path.join(__dirname, '..', 'server.js');
const packagePath = path.join(__dirname, '..', 'package.json');
const uiPath = path.join(__dirname, '..', 'public', 'client-intel.html');
const source = fs.readFileSync(routePath, 'utf8');
const serverSource = fs.readFileSync(serverPath, 'utf8');
const packageJson = JSON.parse(fs.readFileSync(packagePath, 'utf8'));
const uiSource = fs.readFileSync(uiPath, 'utf8');

const {
  createMemoryStore,
  startClientInterview,
  ClientIntelligenceError,
} = require('../services/clientIntelligenceInterview');

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

describe('clientIntelligence routes (static)', () => {
  it('registers CIE endpoints with auth roles', () => {
    assert.match(source, /requireRole\('admin', 'manager', 'client'\)/);
    assert.match(source, /Cache-Control',\s*'no-store'/);

    const expected = [
      '/api/v1/clients/:id/interview/start',
      '/api/v1/interview/:id/message',
      '/api/v1/interview/:id',
      '/api/v1/interview/:id/blueprint',
      '/api/v1/blueprint/:id/revise',
      '/api/v1/blueprint/:id/approve',
      '/api/v1/clients/:id/blueprint',
      '/client-intel',
    ];
    for (const route of expected) {
      assert.match(source, new RegExp(route.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')));
    }
  });

  it('is mounted from server.js and registers npm script', () => {
    assert.match(serverSource, /require\('\.\/routes\/clientIntelligence'\)/);
    assert.equal(
      packageJson.scripts['client:intel:interview'],
      'node scripts/clientIntelInterview.js'
    );
  });

  it('ships client-intel UI with chat + blueprint approve', () => {
    assert.match(uiSource, /Interview/);
    assert.match(uiSource, /Business Blueprint/);
    assert.match(uiSource, /Approve/);
    assert.match(uiSource, /\/api\/v1\/clients\//);
    assert.match(uiSource, /overflow:\s*hidden/);
    assert.match(uiSource, /stickToBottom/);
    assert.match(uiSource, /Business Understanding/);
    assert.match(uiSource, /progress-fill/);
    assert.match(uiSource, /Unknowns/);
  });
});

describe('clientIntelligence routes (http smoke)', () => {
  it('start endpoint returns interview payload when wired with memory store', async () => {
    const store = createMemoryStore();
    const app = express();
    app.use(express.json());
    app.post('/api/v1/clients/:id/interview/start', async (req, res) => {
      try {
        const result = await startClientInterview(
          { clientId: req.params.id, notes: req.body && req.body.notes },
          { store, useMemoryPlaybookStore: true }
        );
        res.status(201).json(result);
      } catch (err) {
        const status = err instanceof ClientIntelligenceError ? err.status : 500;
        res.status(status).json({ error: err.code || 'failed', message: err.message });
      }
    });
    const { base, close } = await listen(app);
    try {
      const res = await request(base, 'POST', '/api/v1/clients/55/interview/start', {});
      assert.equal(res.status, 201);
      assert.ok(res.json.interviewId);
      assert.equal(res.json.status, 'DISCOVERY');
      assert.equal(res.json.question.id, 'identity');
    } finally {
      await close();
    }
  });
});
