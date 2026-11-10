'use strict';

/**
 * P0 #1110 — Operational reads outrank generic Blueprint reasoning.
 *
 * End-to-end through WorkspaceEngine.ask() with an APPROVED Business Blueprint
 * present (the condition PR #1111's unit tests never exercised). Operating
 * evidence loaders are injected; no database, network, delegation, or writes.
 *
 * Baseline (503c0f2): both 2026-11-10 production turns were answered by the
 * generic Client Intelligence handler (pipeline ClientIntelligence) with
 * Blueprint-derived guidance and no record data.
 */

const { describe, it, afterEach } = require('node:test');
const assert = require('node:assert/strict');

const { createHermeticWorkspaceEngine: createWorkspaceEngine } = require('./workspaceTestHarness');
const {
  createMemoryStore,
  startClientInterview,
  postInterviewMessage,
  approveBlueprint,
} = require('../../../../services/clientIntelligenceInterview');

const LEVI_TENANT = 21;
const OTHER_TENANT = 11;

// Exact production utterances (tenant 21, session 0dac5683…, 2026-11-10).
const FAILURE_A =
  "Brief me on my prospects. Identify the top five opportunities, explain why they're ranked that way, and recommend the next action for each. Use actual workspace data and cite the supporting evidence.";
const FAILURE_B =
  'List the actual prospect records currently stored in this workspace. For each, return its name, record ID, prospect type, current status, and source. Do not use the Business Blueprint as a substitute for prospect data. If no records are available, explicitly state which data source was queried and why it returned no records.';
const RANK_FOLLOW_UP = 'Why is the top opportunity ranked first?';

// Synthetic private-lending Blueprint (shape of the tenant-21 incident; no real data).
const LENDING_ANSWERS = [
  'Private lending for active real-estate investors.',
  'Asset-backed bridge and rehab loans.',
  'Active real-estate operators and borrowers.',
  'Speculators without collateral.',
  'Charleston WV and surrounding counties.',
  'Fast, clear, asset-backed decisions.',
  'Direct and professional.',
  'Grow a qualified borrower pipeline.',
  'Funded loans with a credible repayment or exit plan.',
];

async function approveTenant(store, clientId) {
  const opts = { store };
  const started = await startClientInterview({ clientId, forceNew: true }, opts);
  let turn = started;
  for (const answer of LENDING_ANSWERS) {
    turn = await postInterviewMessage(started.interviewId, answer, opts);
  }
  await approveBlueprint(turn.blueprint.id, opts);
}

const PROSPECT_COUNTS = {
  [LEVI_TENANT]: { total: 10, qualified: 3, cold: 10, warm: 0, hot: 0, setter_visible: 0, booked: 0, closed: 0 },
  [OTHER_TENANT]: { total: 77, qualified: 40, cold: 50, warm: 20, hot: 7, setter_visible: 30, booked: 4, closed: 1 },
};

function operatingLoaders({ prospects } = {}) {
  const calls = [];
  const nr = async () => ({ available: false, reason: 'unit_fixture_not_recorded' });
  const loadProspects =
    prospects ||
    (async (input) => {
      calls.push(Number(input.clientId));
      const counts = PROSPECT_COUNTS[Number(input.clientId)];
      return counts
        ? { available: true, counts, segments: [{ vertical: 'private_lending', count: counts.total }] }
        : { available: true, counts: { total: 0 }, segments: [] };
    });
  return {
    calls,
    opts: {
      loadCampaignAo: nr, loadScout: nr, loadMissions: nr, loadObjectives: nr,
      loadActivity: nr, loadOutcomes: nr, loadOperatorAttested: nr, loadCapability: nr,
      loadProspects,
    },
  };
}

async function openTenant({ tenantId = LEVI_TENANT, loaders, engineOptions = {} } = {}) {
  const store = createMemoryStore();
  await approveTenant(store, LEVI_TENANT);
  await approveTenant(store, OTHER_TENANT);
  const ev = loaders || operatingLoaders();
  const engine = createWorkspaceEngine({
    disableLlm: true,
    missionsEnabled: true,
    clientIntelligenceOpts: { store },
    operatingEvidenceOpts: ev.opts,
    ...engineOptions,
  });
  const opened = engine.open({ tenantId: String(tenantId), page: 'command-deck' });
  const ask = (question) => engine.ask({ sessionId: opened.sessionId, question });
  return { engine, ask, loaders: ev, sessionId: opened.sessionId };
}

const BLUEPRINT_LEAK_RE = /Approved Business Blueprint|detailed_answer|exit is credible/i;

function assertOperationalRead(result, label) {
  const owner = result.workspaceOwnership || {};
  assert.equal(owner.owner, 'knowledge_retrieval', `${label}: owner ${owner.owner}/${owner.reason}`);
  assert.match(String(owner.reason), /^operational_read/, `${label}: reason ${owner.reason}`);
  assert.equal(result.routingTrace && result.routingTrace.pipeline, 'OperatingEvidence', `${label}: pipeline`);
  assert.notEqual(result.routingTrace && result.routingTrace.pipeline, 'ClientIntelligence', label);
  assert.equal(result.clientIntelligence, undefined, `${label}: CIE attachment`);
  for (const ev of result.structured.supportingEvidence || []) {
    assert.doesNotMatch(String(ev.label), /Approved Business Blueprint/, `${label}: Blueprint used as evidence`);
  }
  assert.doesNotMatch(result.prose, BLUEPRINT_LEAK_RE, `${label}: Blueprint/raw-token leak`);
  // Read-only: nothing executed, delegated, or created.
  assert.equal(result.mission, null, `${label}: mission`);
  assert.notEqual(result.structured.metadata.executed, true, `${label}: executed`);
  assert.notEqual(result.structured.metadata.scoutDelegated, true, `${label}: scout`);
}

function isBlueprintTurn(result) {
  return /client_intelligence/.test(String(result.domainDecision && result.domainDecision.reason));
}

afterEach(() => {
  delete process.env.MAX_OPERATIONAL_READ_PRECEDENCE;
});

describe('P0 #1110 — 2026-11-10 Levi failures', () => {
  it('Failure A (ranked prospect briefing) is answered from operating records, not Blueprint', async () => {
    const { ask, loaders } = await openTenant();
    const result = await ask(FAILURE_A);
    assertOperationalRead(result, 'Failure A');
    assert.equal(result.workspaceOwnership.reason, 'operational_read');
    assert.match(result.prose, /10 existing prospects, 3 at or above qualification threshold/);
    assert.match(result.prose, /prospects table/);
    assert.match(result.prose, /Record-level listing and ranking are not available/);
    assert.equal(result.retrieval.retrievalStatus, 'found');
    assert.deepEqual([...new Set(loaders.calls)], [LEVI_TENANT]);
  });

  it('Failure B (direct record retrieval) recovers after mission inspection declines', async () => {
    const { ask } = await openTenant();
    const result = await ask(FAILURE_B);
    assertOperationalRead(result, 'Failure B');
    assert.match(result.prose, /prospects table/);
    assert.match(result.prose, /Record-level listing and ranking are not available/);
    assert.equal(result.retrieval.operationalRead.wantsRecordList, true);
    assert.equal(result.retrieval.retrievalStatus, 'found');
  });

  it('ranking follow-up after Failure A says no ranking exists instead of fabricating one', async () => {
    const { ask } = await openTenant();
    await ask(FAILURE_A);
    const follow = await ask(RANK_FOLLOW_UP);
    assertOperationalRead(follow, 'rank follow-up');
    assert.match(follow.prose, /No prospect ranking was produced earlier in this conversation/);
  });

  it('pre-#1111 sequence: "brief me on my prospects" then the ranking follow-up', async () => {
    const { ask } = await openTenant();
    const first = await ask('brief me on my prospects');
    assertOperationalRead(first, 'brief me');
    const follow = await ask(RANK_FOLLOW_UP);
    assertOperationalRead(follow, 'rank follow-up');
    assert.match(follow.prose, /No prospect ranking was produced/);
  });

  it('rollback switch restores the pre-fix routing (documents the baseline)', async () => {
    process.env.MAX_OPERATIONAL_READ_PRECEDENCE = 'false';
    const { ask } = await openTenant();
    const result = await ask(FAILURE_A);
    assert.equal(result.routingTrace.pipeline, 'ClientIntelligence');
    assert.ok(isBlueprintTurn(result));
  });
});

describe('P0 #1110 — declined owner recovery', () => {
  it('a Scout decline lands on operating evidence, not the generic Blueprint handler', async () => {
    const { ask } = await openTenant({
      engineOptions: {
        resolveWorkspaceOwner: async () => ({
          owner: 'specialist_scout',
          reason: 'scout_command',
          confidence: 0.9,
          specialist: 'scout',
        }),
      },
    });
    const result = await ask(FAILURE_A);
    assertOperationalRead(result, 'scout decline');
    assert.equal(result.workspaceOwnership.reason, 'operational_read_recovery');
    assert.deepEqual(result.workspaceOwnership.recoveredFrom, ['specialist_scout']);
  });
});

describe('P0 #1110 — operating-data requests after a Blueprint turn', () => {
  const cases = [
    ['How many prospects do we have?', 'prospects'],
    ['show my leads', 'prospects'],
    ['what tasks are due?', 'tasks'],
    ['What follow-ups are due today?', 'tasks'],
    ['how many emails went out?', 'emails'],
    ['Who replied to our emails?', 'emails'],
    ['pipeline status?', 'pipeline'],
    ["What's in the pipeline today?", 'pipeline'],
  ];
  for (const [question, object] of cases) {
    it(`${question}`, async () => {
      const { ask } = await openTenant();
      const advice = await ask('What should we focus on first?');
      assert.ok(isBlueprintTurn(advice), 'setup: Blueprint answered the advisory turn');
      const result = await ask(question);
      assertOperationalRead(result, question);
      assert.ok(result.retrieval.operationalRead.objects.includes(object));
    });
  }
});

describe('P0 #1110 — short follow-ups', () => {
  it('"Tell me more" right after a Blueprint turn stays with Blueprint', async () => {
    const { ask } = await openTenant();
    await ask('What should we focus on first?');
    const more = await ask('Tell me more');
    assert.ok(isBlueprintTurn(more), String(more.domainDecision && more.domainDecision.reason));
  });

  it('the eight-word rule no longer extends a Blueprint turn across intervening turns', async () => {
    const { ask } = await openTenant();
    await ask('What should we focus on first?');
    await ask('show my leads');
    const more = await ask('Tell me more');
    assert.equal(isBlueprintTurn(more), false, String(more.domainDecision && more.domainDecision.reason));
  });
});

describe('P0 #1110 — retrieval status', () => {
  it('empty tenant: zero rows, stated as such', async () => {
    const loaders = operatingLoaders({ prospects: async () => ({ available: true, counts: { total: 0 } }) });
    const { ask } = await openTenant({ loaders });
    const result = await ask('How many prospects do we have?');
    assertOperationalRead(result, 'empty');
    assert.equal(result.retrieval.retrievalStatus, 'empty');
    assert.match(result.prose, /0 rows/);
  });

  it('failed query: reported as a retrieval failure, never as "nothing there"', async () => {
    const loaders = operatingLoaders({
      prospects: async () => {
        throw Object.assign(new Error('connection terminated'), { code: 'ECONNRESET' });
      },
    });
    const { ask } = await openTenant({ loaders });
    const result = await ask(FAILURE_B);
    assertOperationalRead(result, 'failed');
    assert.equal(result.retrieval.retrievalStatus, 'failed');
    assert.match(result.prose, /retrieval failure, not an empty result/);
    assert.doesNotMatch(result.prose, /No prospects are currently recorded/);
  });

  it('unavailable store: distinct from failed and from empty', async () => {
    const loaders = operatingLoaders({
      prospects: async () => ({ available: false, reason: 'prospect_store_unavailable' }),
    });
    const { ask } = await openTenant({ loaders });
    const result = await ask('How many prospects do we have?');
    assertOperationalRead(result, 'unavailable');
    assert.equal(result.retrieval.retrievalStatus, 'unavailable');
    assert.match(result.prose, /not an empty result/);
  });
});

describe('P0 #1110 — tenant isolation', () => {
  it('each tenant reads only its own records', async () => {
    const levi = await openTenant({ tenantId: LEVI_TENANT });
    const leviResult = await levi.ask('How many prospects do we have?');
    assert.deepEqual([...new Set(levi.loaders.calls)], [LEVI_TENANT]);
    assert.match(leviResult.prose, /10 existing prospects/);
    assert.doesNotMatch(leviResult.prose, /77 existing prospects/);

    const other = await openTenant({ tenantId: OTHER_TENANT });
    const otherResult = await other.ask('How many prospects do we have?');
    assert.deepEqual([...new Set(other.loaders.calls)], [OTHER_TENANT]);
    assert.match(otherResult.prose, /77 existing prospects/);
    assert.doesNotMatch(otherResult.prose, /10 existing prospects/);
  });
});

describe('P0 #1110 — Blueprint advisory behaviour is preserved', () => {
  for (const question of [
    'What should we focus on first?',
    'What campaign would you recommend?',
    'What do you think our biggest opportunity is?',
  ]) {
    it(`${question} stays a Blueprint advisory turn`, async () => {
      const { ask } = await openTenant();
      const result = await ask(question);
      assert.ok(isBlueprintTurn(result), `${question} → ${result.domainDecision && result.domainDecision.reason}`);
      assert.doesNotMatch(String(result.workspaceOwnership && result.workspaceOwnership.reason), /operational_read/);
    });
  }

  it('Who are our ideal customers? is not taken as an operational read', async () => {
    const { ask } = await openTenant();
    const result = await ask('Who are our ideal customers?');
    assert.doesNotMatch(String(result.workspaceOwnership && result.workspaceOwnership.reason), /operational_read/);
    assert.notEqual(result.routingTrace && result.routingTrace.pipeline, 'OperatingEvidence');
  });
});
