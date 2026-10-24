'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const {
  createMemoryStore,
  startClientInterview,
  postInterviewMessage,
  approveBlueprint,
  getInterview,
  listApprovedBlueprintSessions,
  getResumePayload,
  loadAnchorSampleBlueprint,
  resolveResumeTarget,
  startGrowthConversation,
  reviseBlueprint,
} = require('../services/clientIntelligenceInterview');
const {
  ANCHOR_SAMPLE_CLIENT_ID,
  ANCHOR_FIXTURE_KEY,
  fixturesAllowed,
} = require('../services/clientIntelligenceFixtures');

const ANSWERS = [
  'Anchor Cleaning — commercial cleaning for professional offices.',
  'Recurring commercial cleaning and weekly office cleans.',
  'Property managers, facility managers, and professional offices.',
  'Lowest-price bargain hunters.',
  'Greater Manchester including Bedford and Hooksett.',
  'Reliable crews that do the work right without chasing.',
  'Calm professional reliable voice.',
  'Grow commercial cleaning in Greater Manchester.',
  'Clearer path to commercial opportunities in 90 days.',
];

async function approveFreshInterview(opts, clientId = 42) {
  const started = await startClientInterview({ clientId }, opts);
  let turn = started;
  for (const answer of ANSWERS) {
    turn = await postInterviewMessage(started.interviewId, answer, opts);
  }
  assert.ok(turn.blueprint, 'expected blueprint');
  const approved = await approveBlueprint(turn.blueprint.id, opts);
  assert.equal(approved.status, 'APPROVED');
  return { started, approved, interviewId: started.interviewId };
}

describe('CIE saved Blueprint sessions', () => {
  it('allows fixtures outside production', () => {
    assert.equal(fixturesAllowed({ NODE_ENV: 'development' }), true);
    assert.equal(fixturesAllowed({ NODE_ENV: 'test' }), true);
    assert.equal(fixturesAllowed({ NODE_ENV: 'production' }), false);
    assert.equal(
      fixturesAllowed({ NODE_ENV: 'production', ALLOW_CIE_FIXTURES: '1' }),
      true
    );
  });

  it('persists approved Blueprint summary fields across getInterview', async () => {
    const store = createMemoryStore();
    const opts = { store, useMemoryPlaybookStore: true };
    const { interviewId, approved } = await approveFreshInterview(opts, 42);

    const detail = await getInterview(interviewId, opts);
    assert.equal(detail.status, 'APPROVED');
    assert.equal(detail.blueprint.status, 'approved');
    assert.equal(detail.blueprint.version, approved.blueprint.version);
    assert.ok(detail.businessName);
    assert.match(detail.businessName, /Anchor/i);
    assert.equal(detail.resumeTarget, 'growth_workspace');
    assert.equal(detail.resumePhase, 'growth_workspace');
    assert.ok(detail.initialGrowthDirection);
    assert.ok(detail.growthPlan);
    assert.equal(detail.isSample, false);

    const listed = await listApprovedBlueprintSessions({
      ...opts,
      clientId: 42,
      includeSamples: false,
    });
    assert.equal(listed.count, 1);
    const row = listed.sessions[0];
    assert.equal(row.sessionId, interviewId);
    assert.equal(row.clientId, 42);
    assert.match(row.label, /Growth Plan/i);
    assert.equal(row.blueprintVersion, approved.blueprint.version);
    assert.ok(row.approvedBlueprint);
    assert.equal(row.approvedBlueprint.id, approved.blueprint.id);
    assert.equal(row.resumeTarget, 'growth_workspace');
    assert.ok(row.growthPlan);
    assert.equal(row.isSample, false);
  });

  it('routes Resume Growth Plan to Growth Workspace without restarting interview', async () => {
    const store = createMemoryStore();
    const opts = { store, useMemoryPlaybookStore: true };
    const { interviewId } = await approveFreshInterview(opts, 43);

    let resume = await getResumePayload(interviewId, { ...opts, action: 'continue' });
    assert.equal(resume.resumeTarget, 'growth_workspace');
    assert.equal(resume.resumePhase, 'growth_workspace');
    assert.doesNotMatch(JSON.stringify(resume.question || {}), /What is the name/);

    await startGrowthConversation(interviewId, opts);
    resume = await getResumePayload(interviewId, opts);
    assert.equal(resume.resumeTarget, 'growth_workspace');
    assert.equal(resume.resumePhase, 'growth_workspace');
    assert.ok(resume.growthConversation);
    assert.ok(Array.isArray(resume.growthConversation.turns));
    assert.ok(resume.growthConversation.turns.length >= 1);
    assert.ok(resume.growthPlan);
    assert.equal(
      resume.growthPlan.currentTask.id,
      'milestone:growth_conversation'
    );

    const session = await store.getSession(interviewId);
    assert.equal(resolveResumeTarget(session), 'growth_workspace');
  });

  it('does not overwrite an approved Blueprint unless revise creates a new version', async () => {
    const store = createMemoryStore();
    const opts = { store, useMemoryPlaybookStore: true };
    const { approved } = await approveFreshInterview(opts, 44);
    const blueprintId = approved.blueprint.id;
    const version = approved.blueprint.version;

    const again = await approveBlueprint(blueprintId, opts);
    assert.equal(again.alreadyApproved, true);
    assert.equal(again.blueprint.version, version);
    assert.equal(again.blueprint.status, 'approved');

    const revised = await reviseBlueprint(
      blueprintId,
      { brandVoice: { summary: 'Warm, clear, and dependable.' } },
      opts
    );
    assert.notEqual(revised.version, version);
    assert.equal(revised.status, 'in_review');

    const original = await store.getBlueprint(blueprintId, version);
    assert.equal(original.status, 'approved');
    assert.match(original.sections.brandVoice.summary, /Calm|professional|reliable/i);
    assert.doesNotMatch(original.sections.brandVoice.summary, /Warm, clear, and dependable/i);
  });

  it('keeps Anchor sample fixture separate from real client sessions', async () => {
    const store = createMemoryStore();
    const opts = {
      store,
      useMemoryPlaybookStore: true,
      env: { NODE_ENV: 'test', ALLOW_CIE_FIXTURES: '1' },
    };

    const real = await approveFreshInterview(opts, 99);
    const fixture = await loadAnchorSampleBlueprint(opts);
    assert.equal(fixture.ok, true);
    assert.equal(fixture.isSample, true);
    assert.equal(fixture.fixtureKey, ANCHOR_FIXTURE_KEY);
    assert.equal(fixture.clientId, ANCHOR_SAMPLE_CLIENT_ID);
    assert.match(String(fixture.message || ''), /sample|dev/i);
    assert.equal(fixture.resumeTarget, 'growth_workspace');

    const realOnly = await listApprovedBlueprintSessions({
      ...opts,
      clientId: 99,
      includeSamples: false,
    });
    assert.equal(realOnly.count, 1);
    assert.equal(realOnly.sessions[0].sessionId, real.interviewId);
    assert.equal(realOnly.sessions[0].isSample, false);

    const withSamples = await listApprovedBlueprintSessions({
      ...opts,
      clientId: 99,
      includeSamples: true,
    });
    assert.ok(withSamples.sessions.some((s) => s.sessionId === real.interviewId));
    assert.ok(withSamples.sessions.some((s) => s.isSample && s.fixtureKey === ANCHOR_FIXTURE_KEY));

    const reused = await loadAnchorSampleBlueprint(opts);
    assert.equal(reused.created, false);
    assert.equal(reused.interviewId, fixture.interviewId);
  });

  it('rejects fixtures in production without override', async () => {
    const store = createMemoryStore();
    await assert.rejects(
      () =>
        loadAnchorSampleBlueprint({
          store,
          useMemoryPlaybookStore: true,
          env: { NODE_ENV: 'production' },
        }),
      /fixtures are disabled|fixtures_disabled/i
    );
  });
});

describe('CIE saved sessions UI/routes markers', () => {
  it('exposes list/resume/fixture routes and dashboard entry points', () => {
    const routeSource = fs.readFileSync(
      path.join(__dirname, '..', 'routes', 'clientIntelligence.js'),
      'utf8'
    );
    const uiSource = fs.readFileSync(
      path.join(__dirname, '..', 'public', 'client-intel.html'),
      'utf8'
    );
    const dashSource = fs.readFileSync(
      path.join(__dirname, '..', 'public', 'dashboard.html'),
      'utf8'
    );
    const shellSource = fs.readFileSync(
      path.join(__dirname, '..', 'public', 'shared', 'shell.js'),
      'utf8'
    );

    assert.match(routeSource, /\/api\/v1\/client-intel\/sessions/);
    assert.match(routeSource, /\/api\/v1\/client-intel\/sessions\/:id\/resume/);
    assert.match(routeSource, /\/api\/v1\/client-intel\/fixtures\/anchor-blueprint/);
    assert.match(routeSource, /listApprovedBlueprintSessions/);
    assert.match(routeSource, /loadAnchorSampleBlueprint/);

    assert.match(uiSource, /Resume Growth Plan/);
    assert.match(uiSource, /View Blueprint/);
    assert.match(uiSource, /Start New Interview/);
    assert.match(uiSource, /Load Anchor sample Blueprint/);
    assert.match(uiSource, /resumeSession/);
    assert.match(uiSource, /applyResumeState/);
    assert.match(uiSource, /growth_workspace/);
    assert.match(uiSource, /SAMPLE \/ DEV DATA/);

    assert.match(dashSource, /cieSessionsPanel|CLIENT INTELLIGENCE/);
    assert.match(dashSource, /loadCieSessions/);
    assert.match(dashSource, /Resume Growth Plan/);
    assert.match(dashSource, /Previous Plans/);
    assert.match(shellSource, /client-intel/);
    assert.match(shellSource, /Client Intel/);
  });
});
