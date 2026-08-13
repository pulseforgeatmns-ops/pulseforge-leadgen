'use strict';

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const { createWorkspaceEngine } = require('../index');
const { selectExecutionDomain } = require('../ExecutionDomain');
const {
  createMemoryStore,
  createOperatorObjective,
  ensurePublicMaxLaunchObjective,
  getActiveObjectives,
} = require('../../../../services/operatorObjectives');
const delegation = require('../../../../services/maxPaigeCampaignDelegation');

describe('SPEC-095 durable operator objectives in WorkspaceEngine', () => {
  /** @type {ReturnType<typeof createMemoryStore>} */
  let store;

  beforeEach(() => {
    store = createMemoryStore();
  });

  function engine(extra = {}) {
    return createWorkspaceEngine({
      disableLlm: true,
      missionsEnabled: false,
      operatorObjectiveOpts: { store },
      ...extra,
    });
  }

  it('establishes Public Max Launch from explicit operator language', async () => {
    const workspace = engine();
    const result = await workspace.ask({
      question: [
        "We're preparing for your public launch over roughly the next three weeks,",
        'but the launch should be evidence-gated rather than forced by the date.',
        'The objective is to build qualified attention around the ideas behind Pulseforge,',
        "progressively expose the problems we're solving, then reveal Max and convert",
        'that attention into qualified demos.',
        'I want you to own the overall objective. Paige should handle content strategy.',
      ].join(' '),
      context: { tenantId: '1', page: 'command-deck' },
    });

    assert.equal(result.mission, null);
    assert.equal(result.domainDecision.reason, 'operator_objective_established');
    assert.match(result.prose, /Public Max Launch/i);
    assert.match(result.prose, /saved/i);
    assert.ok(result.resolvedObjective);
    assert.equal(result.resolvedObjective.scope, 'operator');
    assert.equal(result.resolvedObjective.status, 'active');
  });

  it('recovers the launch in a fresh session without prior messages', async () => {
    await ensurePublicMaxLaunchObjective({ store, tenantId: '1' });

    const workspace = engine();
    const result = await workspace.ask({
      question: 'Where are we with the launch?',
      context: { tenantId: '1', page: 'command-deck' },
    });

    assert.equal(result.mission, null);
    assert.equal(result.domainDecision.reason, 'operator_objective_status');
    assert.match(result.prose, /Public Max Launch/i);
    assert.match(result.prose, /Thesis/i);
    assert.equal(result.resolvedObjective.title, 'Public Max Launch');
    assert.notEqual(result.domainDecision.reason, 'understood_campaign_creation');
  });

  it('does not route Max launch campaign status into Mission / cleaning campaign', async () => {
    await ensurePublicMaxLaunchObjective({ store, tenantId: '1' });
    let missionCreated = false;
    const workspace = createWorkspaceEngine({
      disableLlm: true,
      missionsEnabled: true,
      operatorObjectiveOpts: { store },
      missionEngine: {
        createFromObjective: async () => {
          missionCreated = true;
          return {
            id: 'msn_bad',
            title: 'Commercial Cleaning - Manchester',
            type: 'campaign_creation',
            status: 'requested',
          };
        },
        activeMissionResolver: null,
      },
    });

    const result = await workspace.ask({
      question: 'Where are we with the Max launch campaign?',
      context: { tenantId: '1', page: 'command-deck' },
    });

    assert.equal(missionCreated, false);
    assert.equal(result.mission, null);
    assert.equal(result.domainDecision.reason, 'operator_objective_status');
    assert.match(result.prose, /Public Max Launch/i);
    assert.doesNotMatch(result.prose, /Commercial Cleaning/i);
  });

  it('still routes explicit new campaign creation to Mission Engine', async () => {
    await ensurePublicMaxLaunchObjective({ store, tenantId: '1' });
    let createdObjective = null;
    const workspace = createWorkspaceEngine({
      disableLlm: true,
      missionsEnabled: true,
      resolverEnabled: false,
      operatorObjectiveOpts: { store },
      missionEngine: {
        createFromObjective: async (input) => {
          createdObjective = input.objective;
          return {
            id: 'msn_ok',
            title: 'Commercial Cleaning Boston',
            type: 'campaign_creation',
            status: 'review_required',
            objective_text: input.objective,
          };
        },
        toCard: (mission) => ({
          id: mission.id,
          title: mission.title,
          status: mission.status,
          type: mission.type,
        }),
        activeMissionResolver: null,
      },
    });

    const result = await workspace.ask({
      question:
        'Launch a commercial cleaning campaign targeting law firms in Boston.',
      context: { tenantId: '1', page: 'command-deck' },
    });

    assert.ok(createdObjective);
    assert.match(String(createdObjective), /commercial cleaning/i);
    assert.ok(result.mission);
    assert.equal(result.mission.id, 'msn_ok');
    assert.notEqual(result.domainDecision.reason, 'operator_objective_status');
  });

  it('delegates content asks to Paige with recovered objective context', async () => {
    await ensurePublicMaxLaunchObjective({ store, tenantId: '1' });
    let seenContext = null;
    const workspace = createWorkspaceEngine({
      disableLlm: true,
      missionsEnabled: false,
      operatorObjectiveOpts: { store },
      paigeCampaignDelegationService: {
        shouldDelegateToPaige: delegation.shouldDelegateToPaige,
        delegateCampaignContentRecommendation: async (input) => {
          seenContext = input.context;
          const recommendation = delegation.normalizePaigeRecommendation(
            {
              objective: input.context.objective,
              recommended_direction:
                'AI systems should understand uncertainty before acting.',
              reason:
                'Tests whether operator-centered AI framing continues to produce qualified out-of-network discovery.',
              confidence: 0.55,
              uncertainties: ['Repeatability unknown.'],
              supporting_learning_ids: ['learn-1'],
              supporting_publication_ids: ['pub-1'],
              autonomousPublish: false,
            },
            {
              clientId: 1,
              tenantId: 1,
              objective: input.context.objective,
              channel: 'linkedin',
            }
          );
          return {
            ok: true,
            skipped: false,
            recommendation,
            structured:
              delegation.composeMaxPaigeCampaignStructuredResponse(recommendation),
            prose: delegation.formatMaxPaigeCampaignRecommendation(recommendation),
          };
        },
      },
    });

    const result = await workspace.ask({
      question: 'What should we publish next for the launch?',
      context: { tenantId: '1', page: 'command-deck' },
    });

    assert.equal(result.domainDecision.reason, 'paige_campaign_content_delegation');
    assert.ok(seenContext);
    assert.ok(seenContext.resolvedObjective);
    assert.equal(seenContext.resolvedObjective.title, 'Public Max Launch');
    assert.match(String(seenContext.objective), /qualified attention/i);
    assert.match(result.prose, /PUBLIC MAX LAUNCH/i);
    assert.match(result.prose, /Thesis|phase/i);
    assert.match(result.prose, /uncertainty before acting/i);
    assert.equal(result.mission, null);
  });

  it('asks for clarification when launch reference is ambiguous', async () => {
    await createOperatorObjective(
      {
        tenantId: '1',
        scope: 'operator',
        title: 'Public Max Launch',
        objectiveText: 'a',
        aliases: ['the launch', 'launch'],
      },
      { store }
    );
    await createOperatorObjective(
      {
        tenantId: '1',
        scope: 'operator',
        title: 'Boston Market Launch',
        objectiveText: 'b',
        aliases: ['the launch', 'market launch'],
      },
      { store }
    );

    const workspace = engine();
    const result = await workspace.ask({
      question: 'Where are we with the launch?',
      context: { tenantId: '1', page: 'command-deck' },
    });

    assert.equal(result.domainDecision.reason, 'operator_objective_ambiguous');
    assert.match(result.prose, /more than one/i);
    assert.match(result.prose, /Public Max Launch/);
    assert.match(result.prose, /Boston Market Launch/);
    assert.equal(result.mission, null);
  });

  it('survives process-local SessionStore reset (conversation independence)', async () => {
    await ensurePublicMaxLaunchObjective({ store, tenantId: '1' });

    const first = engine();
    await first.ask({
      question: 'Where are we with the launch?',
      context: { tenantId: '1', page: 'command-deck' },
    });

    // Brand-new engine + session — no prior messages, new SessionStore
    const second = engine();
    const result = await second.ask({
      question: 'Where are we with the launch?',
      context: { tenantId: '1', page: 'command-deck' },
    });

    assert.equal(result.resolvedObjective.title, 'Public Max Launch');
    assert.equal(result.domainDecision.reason, 'operator_objective_status');
  });

  it('selectExecutionDomain suppresses mission only with resolved objective flag', () => {
    const warm = selectExecutionDomain(
      'Where are we with the Max launch campaign?',
      {
        resolvedObjective: {
          id: '1',
          title: 'Public Max Launch',
        },
        suppressMissionForObjective: true,
      }
    );
    assert.equal(warm.domain, 'workspace');
    assert.equal(warm.reason, 'resolved_operator_objective');

    const explicit = selectExecutionDomain(
      'Launch a commercial cleaning campaign targeting law firms in Boston.',
      {
        resolvedObjective: {
          id: '1',
          title: 'Public Max Launch',
        },
        suppressMissionForObjective: false,
      }
    );
    assert.equal(explicit.domain, 'mission_execution');
  });

  it('does not create objectives from weak language via workspace', async () => {
    const workspace = engine();
    const result = await workspace.ask({
      question: 'Boston could be interesting someday.',
      context: { tenantId: '1', page: 'command-deck' },
    });
    assert.notEqual(result.domainDecision.reason, 'operator_objective_established');
    const active = await getActiveObjectives({ tenantId: '1' }, { store });
    assert.equal(active.length, 0);
  });

  it('enforces tenant isolation on objective recovery', async () => {
    await ensurePublicMaxLaunchObjective({ store, tenantId: '1' });
    const workspace = engine();
    const result = await workspace.ask({
      question: 'Where are we with the launch?',
      context: { tenantId: '2', page: 'command-deck' },
    });
    assert.notEqual(result.domainDecision.reason, 'operator_objective_status');
    if (result.resolvedObjective) {
      assert.notEqual(result.resolvedObjective.tenantId, '1');
    }
  });
});
