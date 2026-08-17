'use strict';

/**
 * SPEC-104 — Persistent Operator Context tests.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const {
  createMemoryStore,
  assembleOperatorContext,
  rebuildOperatorContext,
  loadOperatorContext,
  generateSessionBrief,
  deriveRecommendedActions,
  REBUILD_TRIGGERS,
} = require('../../../../services/operatorContext');
const { loadOperatorContextForSession } = require('../OperatorContextLoader');
const { createWorkspaceEngine } = require('../index');
const {
  createMemoryStore: createObjectiveStore,
  createOperatorObjective,
} = require('../../../../services/operatorObjectives');
const {
  createMemoryStore: createCieStore,
  startClientInterview,
  postInterviewMessage,
  approveBlueprint,
} = require('../../../../services/clientIntelligenceInterview');
const { buildOpeningState } = require('../OpeningStateBuilder');
const { PAGE_TYPES } = require('../WorkspaceTypes');

const CLIENT_ID = 10;

const ANCHOR_ANSWERS = [
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

async function seedAnchorBlueprint(cieStore) {
  const opts = { store: cieStore };
  const started = await startClientInterview(
    { clientId: CLIENT_ID, forceNew: true },
    opts
  );
  let turn = started;
  for (const answer of ANCHOR_ANSWERS) {
    turn = await postInterviewMessage(started.interviewId, answer, opts);
  }
  await approveBlueprint(turn.blueprint.id, opts);
  return opts;
}

describe('SPEC-104 operator context service', () => {
  /** @type {ReturnType<typeof createMemoryStore>} */
  let contextStore;
  /** @type {ReturnType<typeof createObjectiveStore>} */
  let objectiveStore;
  /** @type {ReturnType<typeof createCieStore>} */
  let cieStore;

  beforeEach(() => {
    contextStore = createMemoryStore();
    objectiveStore = createObjectiveStore();
    cieStore = createCieStore();
  });

  function baseOpts(extra = {}) {
    return {
      store: contextStore,
      objectiveOpts: { store: objectiveStore },
      cieOpts: { store: cieStore },
      listMissions: async () => [
        {
          id: 'msn_1',
          title: 'Campaign 002',
          status: 'running',
          objectiveText: 'Direct mail follow-up',
          updatedAt: new Date().toISOString(),
        },
      ],
      loadOutcomes: async () => [
        {
          kind: 'content_outcome',
          summary: 'One 5-star review.',
          occurredAt: new Date().toISOString(),
        },
      ],
      ...extra,
    };
  }

  it('assembles operator context from blueprint, objectives, missions, and outcomes', async () => {
    await seedAnchorBlueprint(cieStore);
    await createOperatorObjective(
      {
        tenantId: String(CLIENT_ID),
        scope: 'client',
        clientId: CLIENT_ID,
        title: 'Land first recurring commercial account',
        objectiveText: 'Validate outbound before scaling.',
      },
      { store: objectiveStore }
    );

    const doc = await assembleOperatorContext({
      tenantId: String(CLIENT_ID),
      clientId: CLIENT_ID,
      ...baseOpts(),
    });

    assert.equal(doc.identity.companyName, 'Anchor Cleaning');
    assert.match(doc.identity.serviceArea, /Manchester/i);
    assert.ok(doc.currentPriorities.length >= 1);
    assert.equal(doc.activeMissions.length, 1);
    assert.equal(doc.activeMissions[0].title, 'Campaign 002');
    assert.ok(doc.recentOutcomes.length >= 1);
    assert.ok(doc.sources.blueprintApproved);
  });

  it('persists and versions operator context on rebuild', async () => {
    await seedAnchorBlueprint(cieStore);

    const first = await rebuildOperatorContext({
      tenantId: String(CLIENT_ID),
      clientId: CLIENT_ID,
      trigger: REBUILD_TRIGGERS.INITIAL,
      ...baseOpts(),
    });
    assert.equal(first.version, 1);
    assert.equal(first.lastRebuildTrigger, REBUILD_TRIGGERS.INITIAL);

    const second = await rebuildOperatorContext({
      tenantId: String(CLIENT_ID),
      clientId: CLIENT_ID,
      trigger: REBUILD_TRIGGERS.BLUEPRINT_APPROVED,
      ...baseOpts(),
    });
    assert.equal(second.version, 2);
    assert.equal(second.lastRebuildTrigger, REBUILD_TRIGGERS.BLUEPRINT_APPROVED);

    const events = await contextStore.listRebuildEvents({ clientId: CLIENT_ID });
    assert.equal(events.length, 2);
  });

  it('loads missing context with initial rebuild', async () => {
    await seedAnchorBlueprint(cieStore);

    const row = await loadOperatorContext({
      tenantId: String(CLIENT_ID),
      clientId: CLIENT_ID,
      rebuildIfMissing: true,
      ...baseOpts(),
    });

    assert.ok(row);
    assert.ok(row.context.identity.companyName);
  });

  it('generates session brief without storing recommendations in context', async () => {
    await seedAnchorBlueprint(cieStore);
    const row = await rebuildOperatorContext({
      tenantId: String(CLIENT_ID),
      clientId: CLIENT_ID,
      trigger: REBUILD_TRIGGERS.INITIAL,
      ...baseOpts(),
    });

    const brief = generateSessionBrief(row, { hour: 9 });
    assert.match(brief.greeting, /Good morning/i);
    assert.match(brief.fullText, /reviewed Anchor Cleaning before you arrived/i);
    assert.equal(brief.reviewedBeforeArrival, true);
    assert.ok(brief.recommendations.length >= 1);
    assert.equal(row.context.recommendations, undefined);
  });

  it('deriveRecommendedActions returns deterministic next steps', () => {
    const actions = deriveRecommendedActions({
      activeMissions: [{ id: 'm1', title: 'Campaign 002' }],
      currentPriorities: ['Follow up with Aji'],
      recentOutcomes: [{ summary: 'Job won' }],
      openQuestions: ['Does direct mail outperform email?'],
    });
    assert.ok(actions.some((a) => /Campaign 002/i.test(a.label)));
  });
});

describe('SPEC-104 startup loader and workspace open', () => {
  /** @type {ReturnType<typeof createMemoryStore>} */
  let contextStore;
  /** @type {ReturnType<typeof createCieStore>} */
  let cieStore;

  beforeEach(() => {
    contextStore = createMemoryStore();
    cieStore = createCieStore();
  });

  it('loadOperatorContextForSession returns brief attachment', async () => {
    await seedAnchorBlueprint(cieStore);
    await rebuildOperatorContext({
      tenantId: String(CLIENT_ID),
      clientId: CLIENT_ID,
      trigger: REBUILD_TRIGGERS.INITIAL,
      store: contextStore,
      cieOpts: { store: cieStore },
      listMissions: async () => [],
      loadOutcomes: async () => [],
    });

    const attachment = await loadOperatorContextForSession({
      tenantId: String(CLIENT_ID),
      clientId: CLIENT_ID,
      operatorContextOpts: {
        store: contextStore,
        cieOpts: { store: cieStore },
        listMissions: async () => [],
        loadOutcomes: async () => [],
      },
    });

    assert.equal(attachment.reviewedBeforeArrival, true);
    assert.ok(attachment.sessionBrief);
    assert.ok(attachment.operatorContext);
  });

  it('workspace open uses reviewed-before-arrival opening', async () => {
    await seedAnchorBlueprint(cieStore);
    await rebuildOperatorContext({
      tenantId: String(CLIENT_ID),
      clientId: CLIENT_ID,
      trigger: REBUILD_TRIGGERS.INITIAL,
      store: contextStore,
      cieOpts: { store: cieStore },
      listMissions: async () => [],
      loadOutcomes: async () => [],
    });

    const workspace = createWorkspaceEngine({
      disableLlm: true,
      loadOperatorContext: true,
      operatorContextOpts: {
        store: contextStore,
        cieOpts: { store: cieStore },
        listMissions: async () => [],
        loadOutcomes: async () => [],
      },
    });

    const opened = await workspace.open({
      tenantId: String(CLIENT_ID),
      page: 'command-deck',
    });

    assert.equal(opened.reviewedBeforeArrival, true);
    assert.ok(opened.opening.reviewedBeforeArrival);
    assert.match(opened.opening.fullText, /reviewed Anchor Cleaning before you arrived/i);
  });

  it('OpeningStateBuilder prefers sessionBrief on command-deck', () => {
    const opening = buildOpeningState(
      {
        page: PAGE_TYPES.COMMAND_DECK,
        tenantId: '10',
        sessionBrief: {
          reviewedBeforeArrival: true,
          greeting: 'Good morning.',
          body: ['I reviewed Anchor before you arrived.'],
          prompt: "I'd recommend we review new leads first.",
          fullText:
            'Good morning.\n\nI reviewed Anchor before you arrived.\n\nI\'d recommend we review new leads first.',
        },
      },
      { hour: 9 }
    );

    assert.equal(opening.reviewedBeforeArrival, true);
    assert.match(opening.fullText, /reviewed Anchor before you arrived/i);
  });
});
