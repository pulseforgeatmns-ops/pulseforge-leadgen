'use strict';

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const {
  maybeHandleScoutAcquisitionTurn,
  shouldHandleScoutAcquisition,
} = require('../ScoutAcquisitionContext');
const {
  createMemoryStore,
  createSpecialistDelegationService,
} = require('../../../../services/specialistDelegation');
const { createMemoryAcquisitionState } = require('../../../../services/scoutAcquisitionIntelligence');

const ANCHOR_QUESTION =
  'Max, where should we be looking for commercial cleaning opportunities right now?';

function anchorCompanies() {
  return [
    {
      id: 'co-granite',
      tenantId: '10',
      name: 'Granite State Property Management',
      industry: 'property_management',
      location: 'Manchester, NH',
      signals: [
        {
          type: 'portfolio_growth',
          observedAt: '2026-07-12T00:00:00.000Z',
          label: 'Company website lists 37 managed properties.',
          observation: 'Company website lists 37 managed properties.',
        },
      ],
    },
    {
      id: 'co-queen',
      tenantId: '10',
      name: 'Queen City Residences',
      industry: 'property_management',
      location: 'Manchester, NH',
      signals: [
        {
          type: 'portfolio_growth',
          observedAt: '2026-07-20T00:00:00.000Z',
          label: 'Added three buildings to the downtown portfolio.',
        },
      ],
    },
  ];
}

describe('SPEC-100 Max workspace Scout acquisition loop', () => {
  let store;
  let service;
  let aoStore;
  let session;

  beforeEach((t) => {
    // These scenario fixtures describe July 2026 evidence. Keep the scenario
    // clock fixed rather than letting its signals expire as CI wall time moves.
    t.mock.timers.enable({ apis: ['Date'], now: Date.parse('2026-08-01T00:00:00.000Z') });
    store = createMemoryStore();
    service = createSpecialistDelegationService({ store });
    aoStore = createMemoryAcquisitionState();
    session = {
      id: 'sess-anchor',
      context: {
        tenantId: '10',
        page: 'command-deck',
        domainId: 'acquisition',
      },
    };
  });

  async function handle(question, extras = {}) {
    return maybeHandleScoutAcquisitionTurn({
      question,
      session,
      context: {
        tenantId: '10',
        page: 'command-deck',
        domainId: 'acquisition',
        applyPriority: extras.applyPriority === true,
        businessContext: {
          serviceGeography: 'Manchester, NH',
          preferredSegments: ['property_management'],
          commercialCapability: 'commercial_cleaning',
        },
        targetContext: {
          geography: 'Manchester, NH',
          segments: ['property_management'],
        },
        ...extras.context,
      },
      action: extras.action,
      delegationService: service,
      delegationOpts: { store },
      aoStore,
      companies: extras.companies || anchorCompanies(),
      people: extras.people || [
        {
          id: 'p-1',
          tenantId: '10',
          companyId: 'co-granite',
          name: 'Pat Riley',
          jobTitle: 'Director of Operations',
          decisionMaker: true,
        },
      ],
      priorityApplier: async () => ({ applied: true }),
    });
  }

  it('runs the Anchor acquisition question through Max → Scout → evaluation', async () => {
    const turn = await handle(ANCHOR_QUESTION, { applyPriority: true });
    assert.ok(turn);
    assert.match(turn.prose, /Scout|opportunit|property/i);
    assert.equal(turn.loop.delegated, true);
    assert.equal(turn.loop.evaluation.acceptedAsGroundTruth, false);
    assert.equal(session.context.acquisitionLoop, true);
    assert.ok(session.context.lastScoutEvaluation);
  });

  it('preserves acquisition context for Discuss with Max follow-ups', async () => {
    await handle(ANCHOR_QUESTION, { applyPriority: true });
    const follow = await handle('Which four?', { action: 'discuss_with_max' });
    assert.ok(follow);
    assert.equal(follow.loop.delegated, false);
    assert.match(follow.prose, /Granite State|Queen City/i);
  });

  it('keeps the freshest named opportunity at the inclusive 90-day boundary', async (t) => {
    t.mock.timers.setTime(Date.parse('2026-10-18T00:00:00.000Z'));
    await handle(ANCHOR_QUESTION, { applyPriority: true });
    const follow = await handle('Which four?', { action: 'discuss_with_max' });
    assert.equal(follow.loop.delegated, false);
    assert.match(follow.prose, /Queen City/i);
  });

  it('does not retain named opportunities once fixture evidence exceeds 90 days', async (t) => {
    // The freshest fixture is July 20; the inclusive 90-day boundary is Oct 18.
    t.mock.timers.setTime(Date.parse('2026-10-18T00:00:00.001Z'));
    await handle(ANCHOR_QUESTION, { applyPriority: true });
    const follow = await handle('Which four?', { action: 'discuss_with_max' });
    assert.ok(follow);
    assert.equal(follow.loop.delegated, false);
    assert.doesNotMatch(follow.prose, /Granite State|Queen City/i);
    assert.match(follow.prose, /do not have named opportunities/i);
  });

  it('answers coverage questions from durable investigation provenance', async () => {
    await handle(ANCHOR_QUESTION);
    const inspect = await handle('How thorough was the search?', {
      action: 'discuss_with_max',
    });
    assert.ok(inspect);
    assert.equal(inspect.loop.delegated, false);
    assert.match(inspect.prose, /evaluated|coverage/i);
    assert.ok(inspect.structured.investigation || session.context.lastScoutInvestigation);
    assert.equal(
      inspect.structured.confidenceContributors.includes('scout_acquisition'),
      false
    );
  });

  it('does not claim CIE strategy questions', () => {
    assert.equal(
      shouldHandleScoutAcquisition({
        question: 'What do you think our biggest opportunity is?',
        context: { tenantId: '10' },
      }),
      false
    );
    assert.equal(
      shouldHandleScoutAcquisition({
        question: ANCHOR_QUESTION,
        context: { tenantId: '10' },
      }),
      true
    );
  });
});

describe('SPEC-100 wiring markers', () => {
  it('WorkspaceEngine includes the Scout acquisition seam', () => {
    const engineSrc = fs.readFileSync(
      path.join(__dirname, '..', 'WorkspaceEngine.js'),
      'utf8'
    );
    const indexSrc = fs.readFileSync(path.join(__dirname, '..', 'index.js'), 'utf8');
    assert.match(engineSrc, /maybeHandleScoutAcquisitionTurn/);
    assert.match(engineSrc, /scoutAcquisitionOpts/);
    assert.match(indexSrc, /maybeHandleScoutAcquisitionTurn/);
  });
});
