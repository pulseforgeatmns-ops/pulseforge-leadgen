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
  postInterviewMessage,
  approveBlueprint,
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
      '/api/v1/interview/:id/resume',
      '/api/v1/interview/:id',
      '/api/v1/interview/:id/blueprint',
      '/api/v1/blueprint/:id/revise',
      '/api/v1/blueprint/:id/approve',
      '/api/v1/interview/:id/growth/start',
      '/api/v1/interview/:id/growth/message',
      '/api/v1/interview/:id/readiness/start',
      '/api/v1/interview/:id/readiness/message',
      '/api/v1/clients/:id/blueprint',
      '/api/v1/client-intel/sessions',
      '/api/v1/client-intel/sessions/:id/resume',
      '/api/v1/client-intel/fixtures/anchor-blueprint',
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

  it('ships client-intel UI with SPEC-085 Executive Business Brief markers', () => {
    assert.match(uiSource, /Conversation/);
    assert.match(uiSource, /Business Blueprint/);
    assert.match(uiSource, /Approve Blueprint/);
    assert.match(uiSource, /\/api\/v1\/clients\//);
    assert.match(uiSource, /\/resume/);
    assert.match(uiSource, /overflow:\s*hidden/);
    assert.match(uiSource, /stickToBottom/);
    assert.match(uiSource, /Business Understanding/);
    assert.match(uiSource, /progress-fill/);
    assert.match(uiSource, /Executive Business Brief/);
    assert.match(uiSource, /Prepared by Max/);
    assert.match(uiSource, /Connecting themes/);
    assert.match(uiSource, /PREMIUM_LOAD_MS\s*=\s*3000/);
    assert.match(uiSource, /Yes, this reflects my business/);
    assert.match(uiSource, /I'd like to refine this/);
    assert.match(uiSource, /Let's keep talking/);
    assert.match(uiSource, /earned your trust/);
    assert.match(uiSource, /foundation Pulseforge will use/);
    assert.match(uiSource, /Return to Dashboard/);
    assert.match(uiSource, /Resume Growth Plan/);
    assert.match(uiSource, /growth_workspace|Growth Workspace/);
    assert.match(uiSource, /Check Growth Infrastructure/);
    assert.match(uiSource, /Initial Growth Direction/);
    assert.match(uiSource, /assessment-stars/);
    assert.match(uiSource, /2500/);
  });

  it('handles blueprint approval post-state without stuck loading or red APPROVED error', () => {
    assert.match(uiSource, /Blueprint approved/);
    assert.match(uiSource, /Client Playbook ready/);
    assert.match(uiSource, /Initial Growth Direction/);
    assert.match(uiSource, /Resume Growth Plan/);
    assert.match(uiSource, /applyApprovedState/);
    assert.match(uiSource, /approveInFlight/);
    assert.match(uiSource, /already_approved|alreadyApproved/);
    assert.match(uiSource, /sessionStatus\s*===\s*'APPROVED'/);
    assert.match(uiSource, /playbook_prep/);
    assert.match(uiSource, /approvalSuccess|Client Playbook ready/);
    assert.match(uiSource, /was APPROVED/);
    assert.match(uiSource, /startGrowthConversation|\/growth\/start/);
    assert.doesNotMatch(uiSource, /Session must be CLIENT_REVIEW to approve/);
    // After Ready checklist, UI must land on complete / approved outcome.
    assert.match(uiSource, /setPhase\('complete'\)/);
    assert.match(uiSource, /renderCompletion/);
    // Approve action must not stay actionable after approval.
    assert.match(uiSource, /Blueprint approved/);
    assert.match(uiSource, /els\.blueprintActions\.hidden\s*=\s*true/);
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

  it('approve endpoint is idempotent for already APPROVED sessions', async () => {
    const store = createMemoryStore();
    const opts = { store, useMemoryPlaybookStore: true };
    const answers = [
      'Acme Cleaning — premium commercial cleaning.',
      'Recurring office cleans and deep cleans.',
      'Property managers and professional offices.',
      'Bargain hunters and one-off cheap jobs.',
      'Manchester NH and nearby towns.',
      'Reliable crews and clear communication.',
      'Warm professional voice.',
      'Book qualified walkthroughs in 90 days.',
      'Walkthroughs booked and close rate.',
    ];
    const started = await startClientInterview({ clientId: 77 }, opts);
    let turn = started;
    for (const answer of answers) {
      turn = await postInterviewMessage(started.interviewId, answer, opts);
    }
    assert.ok(turn.blueprint, 'expected blueprint before approve');
    assert.equal(turn.status, 'CLIENT_REVIEW');

    const app = express();
    app.use(express.json());
    app.post('/api/v1/blueprint/:id/approve', async (req, res) => {
      try {
        const result = await approveBlueprint(req.params.id, opts);
        res.json(result);
      } catch (err) {
        const status = err instanceof ClientIntelligenceError ? err.status || 400 : 500;
        res.status(status).json({
          error: err.code || 'failed',
          message: err.message,
        });
      }
    });

    const { base, close } = await listen(app);
    try {
      const first = await request(
        base,
        'POST',
        '/api/v1/blueprint/' + encodeURIComponent(turn.blueprint.id) + '/approve',
        {}
      );
      assert.equal(first.status, 200);
      assert.equal(first.json.ok, true);
      assert.equal(first.json.status, 'APPROVED');
      assert.equal(first.json.alreadyApproved, false);
      assert.equal(first.json.blueprint.status, 'approved');

      const second = await request(
        base,
        'POST',
        '/api/v1/blueprint/' + encodeURIComponent(turn.blueprint.id) + '/approve',
        {}
      );
      assert.equal(second.status, 200);
      assert.equal(second.json.ok, true);
      assert.equal(second.json.status, 'APPROVED');
      assert.equal(second.json.message, 'already_approved');
      assert.equal(second.json.alreadyApproved, true);
      assert.doesNotMatch(String(second.json.message || ''), /was APPROVED/);
    } finally {
      await close();
    }
  });
});
