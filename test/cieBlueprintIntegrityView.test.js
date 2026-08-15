'use strict';

/**
 * CIE Blueprint Approval Integrity & Blueprint Rendering regressions.
 * Covers approval lifecycle + View Blueprint / BLUEPRINT tab document rendering.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('path');
const express = require('express');

const uiPath = path.join(__dirname, '..', 'public', 'client-intel.html');
const routePath = path.join(__dirname, '..', 'routes', 'clientIntelligence.js');
const uiSource = fs.readFileSync(uiPath, 'utf8');
const routeSource = fs.readFileSync(routePath, 'utf8');

const {
  createMemoryStore,
  startClientInterview,
  postInterviewMessage,
  approveBlueprint,
  getInterview,
  getResumePayload,
  resolveClientOnboardingState,
  getApprovedClientBlueprint,
  getClientBlueprint,
  getBlueprintRecord,
  ClientIntelligenceError,
} = require('../services/clientIntelligenceInterview');
const {
  resolveCieClientId,
  assertCieClientAccess,
} = require('../utils/cieAuth');

const AS_CLEANING_ID = 11;
const ANCHOR_ID = 10;

const AS_ANSWERS = [
  'AS Cleaning Co. — residential and light commercial cleaning.',
  'Weekly home cleans and office refreshes.',
  'Busy homeowners and small offices that want reliable crews.',
  'Lowest-price bargain hunters.',
  'Greater Manchester New Hampshire.',
  'Consistent quality without chasing the team.',
  'Warm professional reliable voice.',
  'Grow recurring cleaning routes in Manchester.',
  'Booked recurring clients and clearer weekly capacity in 90 days.',
];

async function runInterviewToBlueprint(store, clientId) {
  const opts = { store, useMemoryPlaybookStore: true };
  const started = await startClientInterview({ clientId, forceNew: true }, opts);
  let turn = started;
  for (const answer of AS_ANSWERS) {
    turn = await postInterviewMessage(started.interviewId, answer, opts);
  }
  assert.ok(turn.blueprint, 'expected blueprint after interview');
  return {
    opts,
    started,
    turn,
    interviewId: started.interviewId,
    blueprint: turn.blueprint,
  };
}

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
  const res = await fetch(base + urlPath, {
    method,
    headers: body ? { 'Content-Type': 'application/json' } : undefined,
    body: body ? JSON.stringify(body) : undefined,
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

describe('CIE Blueprint integrity — UI markers', () => {
  it('View Blueprint and BLUEPRINT tab share showCurrentBlueprintView / renderBlueprint', () => {
    assert.match(uiSource, /function showCurrentBlueprintView/);
    assert.match(uiSource, /function renderBlueprint/);
    assert.match(uiSource, /data-role="blueprint-document"/);
    assert.match(uiSource, /Business Blueprint/);
    assert.match(uiSource, /Awaiting approval/);
    assert.match(uiSource, /mode === 'view'/);
    assert.match(uiSource, /showCurrentBlueprintView\(/);
    assert.match(uiSource, /data-tab="blueprint"/);
    assert.match(uiSource, /View Blueprint/);
    // Must not route View Blueprint through applyApprovedState.
    const viewBlock = uiSource.slice(
      uiSource.indexOf("if (mode === 'view')"),
      uiSource.indexOf("if (mode === 'view')") + 900
    );
    assert.match(viewBlock, /showCurrentBlueprintView/);
    assert.doesNotMatch(viewBlock, /applyApprovedState/);
    assert.doesNotMatch(viewBlock, /\/approve/);
  });

  it('renderBlueprint never replaces document with approval ceremony', () => {
    const start = uiSource.indexOf('function renderBlueprint');
    const end = uiSource.indexOf('function showCurrentBlueprintView', start);
    const body = uiSource.slice(start, end);
    assert.doesNotMatch(body, /renderCompletion\(/);
    assert.match(body, /blueprint-document/);
    assert.match(body, /data-blueprint-id/);
    assert.match(body, /data-blueprint-version/);
  });

  it('blueprint_review resume does not open Growth Workspace by default', () => {
    assert.match(uiSource, /target === 'blueprint_review'/);
    const idx = uiSource.indexOf("if (target === 'blueprint_review')");
    const block = uiSource.slice(idx, idx + 700);
    assert.match(block, /setPhase\('blueprint'\)/);
    assert.doesNotMatch(block, /openGrowthWorkspace/);
  });

  it('Playbook ready is gated on an actual playbook id', () => {
    assert.match(uiSource, /playbookReady/);
    assert.match(uiSource, /Client Playbook handoff pending/);
  });
});

describe('A — Interview completion leaves Blueprint in_review', () => {
  it('generated Blueprint remains in_review until explicit approval', async () => {
    const store = createMemoryStore();
    const { blueprint, interviewId, opts } = await runInterviewToBlueprint(
      store,
      AS_CLEANING_ID
    );
    assert.equal(String(blueprint.status).toLowerCase(), 'in_review');
    const detail = await getInterview(interviewId, opts);
    assert.equal(detail.status, 'CLIENT_REVIEW');
    assert.equal(String(detail.blueprint.status).toLowerCase(), 'in_review');
    assert.equal(detail.blueprint.playbookId, null);
  });
});

describe('B — Executive Brief / viewing does not approve', () => {
  it('resume view action does not mutate Blueprint status', async () => {
    const store = createMemoryStore();
    const { blueprint, interviewId, opts } = await runInterviewToBlueprint(
      store,
      AS_CLEANING_ID
    );
    const before = await store.getBlueprint(blueprint.id, blueprint.version);
    assert.equal(String(before.status).toLowerCase(), 'in_review');

    const resume = await getResumePayload(interviewId, {
      ...opts,
      action: 'view',
    });
    assert.equal(resume.action, 'view');
    assert.equal(String(resume.blueprint.status).toLowerCase(), 'in_review');

    const after = await store.getBlueprint(blueprint.id, blueprint.version);
    assert.equal(String(after.status).toLowerCase(), 'in_review');
    assert.equal(after.playbook_id, null);
  });
});

describe('C — Recovery does not mutate Blueprint status', () => {
  it('onboarding recovery keeps in_review Blueprint', async () => {
    const store = createMemoryStore();
    const { blueprint, opts } = await runInterviewToBlueprint(
      store,
      AS_CLEANING_ID
    );
    const recovered = await resolveClientOnboardingState(AS_CLEANING_ID, opts);
    assert.equal(recovered.onboardingState, 'blueprint_review');
    assert.equal(recovered.status, 'CLIENT_REVIEW');
    assert.equal(String(recovered.blueprint.status).toLowerCase(), 'in_review');

    const row = await store.getBlueprint(blueprint.id, blueprint.version);
    assert.equal(String(row.status).toLowerCase(), 'in_review');
  });
});

describe('D/E — View Blueprint + BLUEPRINT tab same document', () => {
  it('resume view returns the same Blueprint id/version/content as interview detail', async () => {
    const store = createMemoryStore();
    const { blueprint, interviewId, opts } = await runInterviewToBlueprint(
      store,
      AS_CLEANING_ID
    );
    const detail = await getInterview(interviewId, opts);
    const viewed = await getResumePayload(interviewId, {
      ...opts,
      action: 'view',
    });
    assert.equal(viewed.blueprint.id, detail.blueprint.id);
    assert.equal(viewed.blueprint.version, detail.blueprint.version);
    assert.equal(viewed.blueprint.id, blueprint.id);
    assert.deepEqual(
      viewed.blueprint.sections.identity &&
        viewed.blueprint.sections.identity.summary,
      detail.blueprint.sections.identity &&
        detail.blueprint.sections.identity.summary
    );
    assert.ok(viewed.blueprint.sections);
    assert.ok(Object.keys(viewed.blueprint.sections).length > 0);
  });
});

describe('F — Explicit approval transitions and Playbook handoff', () => {
  it('approve endpoint transitions in_review → approved and creates Playbook', async () => {
    const store = createMemoryStore();
    const { blueprint, interviewId, opts } = await runInterviewToBlueprint(
      store,
      AS_CLEANING_ID
    );
    const result = await approveBlueprint(blueprint.id, opts);
    assert.equal(result.status, 'APPROVED');
    assert.equal(result.alreadyApproved, false);
    assert.equal(String(result.blueprint.status).toLowerCase(), 'approved');
    assert.ok(result.playbook && result.playbook.id);

    const detail = await getInterview(interviewId, opts);
    assert.equal(detail.status, 'APPROVED');
    assert.ok(detail.approvedAt);
    assert.equal(String(detail.blueprint.status).toLowerCase(), 'approved');
    assert.ok(detail.blueprint.playbookId);
    assert.equal(
      detail.blueprint.playbookId,
      result.playbook.id
    );
  });
});

describe('G — No false Playbook readiness before approval', () => {
  it('in_review Blueprint has no playbook and UI gates ready copy', async () => {
    const store = createMemoryStore();
    const { interviewId, opts } = await runInterviewToBlueprint(
      store,
      AS_CLEANING_ID
    );
    const interview = await getInterview(interviewId, opts);
    assert.equal(String(interview.blueprint.status).toLowerCase(), 'in_review');
    assert.equal(interview.blueprint.playbookId, null);
    assert.match(uiSource, /playbookReady/);
    const completionFn = uiSource.slice(
      uiSource.indexOf('function renderCompletion'),
      uiSource.indexOf('function applyApprovedState')
    );
    assert.match(completionFn, /playbookReady/);
    assert.match(completionFn, /Client Playbook handoff pending/);
  });
});

describe('H — Tenant isolation (SPEC-096)', () => {
  it('AS Cleaning cannot load Anchor Blueprint', async () => {
    const store = createMemoryStore();
    const anchor = await runInterviewToBlueprint(store, ANCHOR_ID);
    const asCleaning = await runInterviewToBlueprint(store, AS_CLEANING_ID);

    const aji = {
      id: 9100,
      role: 'client',
      client_id: AS_CLEANING_ID,
    };
    const req = { user: aji, session: { user: aji, active_client_id: AS_CLEANING_ID } };
    assert.equal(resolveCieClientId(req), AS_CLEANING_ID);
    assert.throws(
      () => assertCieClientAccess(req, ANCHOR_ID),
      (err) => err.status === 403
    );
    assert.doesNotThrow(() =>
      assertCieClientAccess(req, asCleaning.blueprint.clientId)
    );

    const app = express();
    app.use(express.json());
    app.use((req2, _res, next) => {
      req2.user = aji;
      req2.session = { user: aji, active_client_id: AS_CLEANING_ID };
      next();
    });
    app.get('/api/v1/blueprint/:id', async (req2, res) => {
      try {
        const bp = await getBlueprintRecord(req2.params.id, {
          store,
          useMemoryPlaybookStore: true,
        });
        assertCieClientAccess(req2, bp.clientId);
        return res.json(bp);
      } catch (err) {
        return res.status(err.status || 400).json({
          error: err.code || 'error',
          message: err.message,
        });
      }
    });

    const { base, close } = await listen(app);
    try {
      const blocked = await request(
        base,
        'GET',
        '/api/v1/blueprint/' + encodeURIComponent(anchor.blueprint.id)
      );
      assert.equal(blocked.status, 403);

      const allowed = await request(
        base,
        'GET',
        '/api/v1/blueprint/' + encodeURIComponent(asCleaning.blueprint.id)
      );
      assert.equal(allowed.status, 200);
      assert.equal(Number(allowed.json.clientId), AS_CLEANING_ID);
    } finally {
      await close();
    }
  });
});

describe('I — Active Blueprint resolution prefers current in_review over superseded', () => {
  it('superseded Blueprint is skipped by getClientBlueprint current selection', async () => {
    const store = createMemoryStore();
    const first = await runInterviewToBlueprint(store, AS_CLEANING_ID);
    // Explicit restart supersedes unfinished unapproved onboarding.
    const restarted = await startClientInterview(
      { clientId: AS_CLEANING_ID, restart: true },
      first.opts
    );
    assert.ok(restarted.interviewId);
    assert.notEqual(restarted.interviewId, first.interviewId);

    const superseded = await store.getBlueprint(
      first.blueprint.id,
      first.blueprint.version
    );
    assert.equal(String(superseded.status).toLowerCase(), 'superseded');

    await assert.rejects(
      () => getApprovedClientBlueprint(AS_CLEANING_ID, first.opts),
      (err) =>
        err instanceof ClientIntelligenceError && err.code === 'not_found'
    );

    // Complete second interview — its in_review blueprint is current, not superseded.
    let turn = restarted;
    for (const answer of AS_ANSWERS) {
      turn = await postInterviewMessage(restarted.interviewId, answer, first.opts);
    }
    assert.ok(turn.blueprint);
    assert.notEqual(turn.blueprint.id, first.blueprint.id);
    assert.equal(String(turn.blueprint.status).toLowerCase(), 'in_review');

    const recovered = await resolveClientOnboardingState(
      AS_CLEANING_ID,
      first.opts
    );
    assert.equal(recovered.interviewId, restarted.interviewId);
    assert.equal(recovered.blueprint.id, turn.blueprint.id);
    assert.notEqual(recovered.blueprint.id, first.blueprint.id);

    const current = await getClientBlueprint(AS_CLEANING_ID, first.opts);
    assert.equal(current.id, turn.blueprint.id);
    assert.equal(String(current.status).toLowerCase(), 'in_review');
  });

  it('Interview A → supersede → Interview B: recovery/View/tab resolve B; A stays historical', async () => {
    const store = createMemoryStore();
    const opts = { store, useMemoryPlaybookStore: true };

    // Interview A → Blueprint A in_review
    const first = await runInterviewToBlueprint(store, AS_CLEANING_ID);
    const interviewA = first.interviewId;
    const blueprintA = first.blueprint;
    assert.equal(String(blueprintA.status).toLowerCase(), 'in_review');

    // Restart → A superseded
    const restarted = await startClientInterview(
      { clientId: AS_CLEANING_ID, restart: true },
      opts
    );
    assert.notEqual(restarted.interviewId, interviewA);
    const rawA = await store.getSession(interviewA);
    assert.equal(rawA.interview_state.lifecycle, 'superseded');
    const bpAAfter = await store.getBlueprint(blueprintA.id, blueprintA.version);
    assert.equal(String(bpAAfter.status).toLowerCase(), 'superseded');

    // Interview B → Blueprint B in_review
    let turnB = restarted;
    for (const answer of AS_ANSWERS) {
      turnB = await postInterviewMessage(restarted.interviewId, answer, opts);
    }
    const blueprintB = turnB.blueprint;
    assert.ok(blueprintB);
    assert.notEqual(blueprintB.id, blueprintA.id);
    assert.equal(String(blueprintB.status).toLowerCase(), 'in_review');

    // Recovery resolves B
    const recovered = await resolveClientOnboardingState(AS_CLEANING_ID, opts);
    assert.equal(recovered.interviewId, restarted.interviewId);
    assert.equal(recovered.onboardingState, 'blueprint_review');
    assert.equal(recovered.blueprint.id, blueprintB.id);
    assert.equal(String(recovered.blueprint.status).toLowerCase(), 'in_review');
    assert.notEqual(recovered.resumeTarget, 'blueprint_historical');

    // View Blueprint (resume view) resolves B
    const viewed = await getResumePayload(recovered.interviewId, {
      ...opts,
      action: 'view',
    });
    assert.equal(viewed.blueprint.id, blueprintB.id);
    assert.equal(String(viewed.blueprint.status).toLowerCase(), 'in_review');

    // Client current Blueprint endpoint / BLUEPRINT tab source resolves B
    const current = await getClientBlueprint(AS_CLEANING_ID, opts);
    assert.equal(current.id, blueprintB.id);
    assert.equal(String(current.status).toLowerCase(), 'in_review');

    // B can be explicitly approved; Playbook handoff once
    const approved = await approveBlueprint(blueprintB.id, opts);
    assert.equal(approved.status, 'APPROVED');
    assert.equal(approved.alreadyApproved, false);
    assert.equal(String(approved.blueprint.status).toLowerCase(), 'approved');
    assert.ok(approved.playbook && approved.playbook.id);
    const playbookId = approved.playbook.id;

    const again = await approveBlueprint(blueprintB.id, opts);
    assert.equal(again.alreadyApproved, true);
    assert.equal(again.playbook && again.playbook.id, playbookId);

    // A remains superseded; History can still expose A
    const histA = await getInterview(interviewA, opts);
    assert.equal(String(histA.blueprint.status).toLowerCase(), 'superseded');
    assert.equal(histA.resumeTarget, 'blueprint_historical');
    const histView = await getResumePayload(interviewA, {
      ...opts,
      action: 'view',
    });
    assert.equal(histView.blueprint.id, blueprintA.id);
    assert.equal(String(histView.blueprint.status).toLowerCase(), 'superseded');

    // Superseded A is not approvable
    await assert.rejects(
      () => approveBlueprint(blueprintA.id, opts),
      (err) =>
        err instanceof ClientIntelligenceError && err.code === 'invalid_status'
    );

    // Fresh Max continuity uses only the approved current Blueprint
    const continuity = await getApprovedClientBlueprint(AS_CLEANING_ID, opts);
    assert.equal(continuity.id, blueprintB.id);
    assert.equal(String(continuity.status).toLowerCase(), 'approved');
    assert.notEqual(continuity.id, blueprintA.id);

    // Tenant isolation remains intact — Anchor untouched
    const anchor = await runInterviewToBlueprint(store, ANCHOR_ID);
    const asCurrent = await getClientBlueprint(AS_CLEANING_ID, opts);
    assert.equal(Number(asCurrent.clientId), AS_CLEANING_ID);
    assert.equal(asCurrent.id, blueprintB.id);
    assert.equal(Number(anchor.blueprint.clientId), ANCHOR_ID);
    assert.notEqual(anchor.blueprint.id, blueprintB.id);
  });

  it('orphan superseded Blueprint session cannot hijack recovery over newer in_review', async () => {
    const store = createMemoryStore();
    const opts = { store, useMemoryPlaybookStore: true };

    const first = await runInterviewToBlueprint(store, AS_CLEANING_ID);
    // Partial supersession: Blueprint A marked superseded but session lifecycle missed.
    await store.updateBlueprint(first.blueprint.id, first.blueprint.version, {
      status: 'superseded',
    });

    const second = await startClientInterview(
      { clientId: AS_CLEANING_ID, forceNew: true },
      opts
    );
    let turnB = second;
    for (const answer of AS_ANSWERS) {
      turnB = await postInterviewMessage(second.interviewId, answer, opts);
    }
    assert.equal(String(turnB.blueprint.status).toLowerCase(), 'in_review');

    // Touch Interview A so it would sort first if orphan sessions were recoverable.
    const rawA = await store.getSession(first.interviewId);
    await store.updateSession(first.interviewId, {
      interview_state: { ...(rawA.interview_state || {}) },
      summary: (rawA.summary || '') + ' touched',
    });

    const recovered = await resolveClientOnboardingState(AS_CLEANING_ID, opts);
    assert.equal(recovered.interviewId, second.interviewId);
    assert.equal(recovered.blueprint.id, turnB.blueprint.id);
    assert.equal(String(recovered.blueprint.status).toLowerCase(), 'in_review');
    assert.notEqual(recovered.blueprint.id, first.blueprint.id);

    const current = await getClientBlueprint(AS_CLEANING_ID, opts);
    assert.equal(current.id, turnB.blueprint.id);
  });
});

describe('Approval route remains POST-only', () => {
  it('approve is a POST route and GET markers do not approve', () => {
    assert.match(
      routeSource,
      /router\.post\(\s*'\/api\/v1\/blueprint\/:id\/approve'/
    );
    assert.doesNotMatch(
      routeSource,
      /router\.get\(\s*'\/api\/v1\/blueprint\/:id\/approve'/
    );
  });
});

describe('UI status consistency markers', () => {
  it('gates ready-for-review copy and approve controls on durable status', () => {
    assert.match(uiSource, /isBlueprintAwaitingApproval/);
    assert.match(uiSource, /blueprint_historical/);
    assert.match(uiSource, /historical superseded Blueprint/);
    assert.match(uiSource, /status === 'superseded'/);
  });
});
