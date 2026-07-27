'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');
const {
  routeIntent,
  createMissionEngine,
  MISSION_TYPES,
  MISSION_STATUS,
  ROUTE_KINDS,
  TYPE_CAPABILITY_CHAINS,
} = require('..');
const { BUILTIN_IDS, createBuiltinRegistry } = require('../../capabilities');

function testEngine() {
  return createMissionEngine({
    registry: createBuiltinRegistry({ discovery: { useFixture: true } }),
  });
}

describe('SPEC-022 IntentRouter', () => {
  it('routes Build Campaign 001 to mission / campaign_creation', () => {
    const d = routeIntent('Build Campaign 001 for Anchor Cleaning.');
    assert.equal(d.kind, ROUTE_KINDS.MISSION);
    assert.equal(d.missionType, MISSION_TYPES.CAMPAIGN_CREATION);
  });

  it('routes prospect discovery objectives to mission', () => {
    const d = routeIntent(
      'Find the best commercial cleaning prospects in Manchester.'
    );
    assert.equal(d.kind, ROUTE_KINDS.MISSION);
    assert.equal(d.missionType, MISSION_TYPES.PROSPECT_DISCOVERY);
  });

  it('routes Monitor Microsoft to intelligence', () => {
    const d = routeIntent('Monitor Microsoft.');
    assert.equal(d.kind, ROUTE_KINDS.INTELLIGENCE);
    assert.equal(d.missionType, null);
  });

  it('routes Summarize Nvidia to intelligence', () => {
    const d = routeIntent('Summarize Nvidia.');
    assert.equal(d.kind, ROUTE_KINDS.INTELLIGENCE);
  });

  it('routes Show competitor changes to intelligence', () => {
    const d = routeIntent('Show competitor changes.');
    assert.equal(d.kind, ROUTE_KINDS.INTELLIGENCE);
  });

  it('routes overflow partner search to mission', () => {
    const d = routeIntent('Run an overflow partner search in Manchester.');
    assert.equal(d.kind, ROUTE_KINDS.MISSION);
    assert.equal(d.missionType, MISSION_TYPES.OVERFLOW_PARTNER_SEARCH);
  });

  it('routes proposal generation to mission (SPEC-027B)', () => {
    const d = routeIntent('Generate proposal for AS Cleaning Co.');
    assert.equal(d.kind, ROUTE_KINDS.MISSION);
    assert.equal(d.missionType, MISSION_TYPES.PROPOSAL_GENERATION);
  });

  it('routes mail package generation to mission (SPEC-033)', () => {
    const d = routeIntent('Generate mail packages for Campaign 001');
    assert.equal(d.kind, ROUTE_KINDS.MISSION);
    assert.equal(d.missionType, MISSION_TYPES.MAIL_PACKAGE_GENERATION);
  });

  it('routes campaign review to mission (SPEC-034)', () => {
    const d = routeIntent('Review Campaign 001');
    assert.equal(d.kind, ROUTE_KINDS.MISSION);
    assert.equal(d.missionType, MISSION_TYPES.CAMPAIGN_REVIEW);
  });

  it('routes direct mail execution to mission (SPEC-035)', () => {
    const d = routeIntent('Execute direct mail for Campaign 001');
    assert.equal(d.kind, ROUTE_KINDS.MISSION);
    assert.equal(d.missionType, MISSION_TYPES.DIRECT_MAIL_EXECUTION);
  });
});

describe('SPEC-022 MissionPlanner + Executor', () => {
  it('plans Campaign Creation capability chain', () => {
    const engine = testEngine();
    const draft = engine.planner.plan({
      objective: 'Build Campaign 001 for Anchor Cleaning.',
      tenantId: '10',
      clientId: 10,
    });
    assert.equal(draft.type, MISSION_TYPES.CAMPAIGN_CREATION);
    assert.equal(draft.title, 'Campaign 001');
    const ids = draft.plan.steps.map((s) => s.capabilityId);
    assert.deepEqual(ids, TYPE_CAPABILITY_CHAINS[MISSION_TYPES.CAMPAIGN_CREATION]);
    assert.equal(ids[0], BUILTIN_IDS.PROSPECT_DISCOVERY);
    assert.equal(ids[ids.length - 1], BUILTIN_IDS.CAMPAIGN_BUILDER);
  });

  it('selects Commercial Cleaning - Manchester Discovery Profile', () => {
    const engine = testEngine();
    const draft = engine.planner.plan({
      objective: 'Build Campaign 001 for Anchor Cleaning.',
      tenantId: '10',
      clientId: 10,
    });
    assert.ok(draft.discoveryProfile);
    assert.equal(draft.discoveryProfile.name, 'Commercial Cleaning - Manchester');
    assert.equal(draft.constraints.discoveryProfileId, 'dp_commercial_cleaning_manchester');
    assert.equal(draft.constraints.discoveryProfile.version, '1.0');
    assert.match(draft.discoveryProfile.message, /Commercial Cleaning/);
  });

  it('pins Anchor Client Playbook on campaign missions', () => {
    const engine = testEngine();
    const draft = engine.planner.plan({
      objective: 'Build Campaign 001 for Anchor Cleaning.',
      tenantId: '10',
      clientId: 10,
    });
    assert.ok(draft.clientPlaybook);
    assert.equal(draft.clientPlaybook.id, 'pb_anchor_cleaning');
    assert.equal(draft.constraints.clientPlaybookId, 'pb_anchor_cleaning');
    assert.equal(draft.constraints.clientPlaybookVersion, '1.0');
    assert.ok(draft.constraints.clientPlaybook.outreachSequence.length >= 1);
  });

  it('executes to review_required with verified prospects and no outbound', async () => {
    const engine = testEngine();
    const mission = await engine.createFromObjective({
      objective: 'Build Campaign 001 for Anchor Cleaning.',
      tenantId: '10',
      clientId: 10,
      constraints: { targetCount: 50 },
    });
    assert.equal(mission.status, MISSION_STATUS.REVIEW_REQUIRED);
    assert.equal(mission.deliverables.outboundBlocked, true);
    assert.ok(mission.deliverables.campaign);
    assert.equal(mission.deliverables.campaign.name, 'Campaign 001');
    assert.ok(Array.isArray(mission.deliverables.prospects));
    assert.ok(mission.deliverables.prospects.length >= 1);
    assert.ok(mission.deliverables.reviewPackage);
    assert.ok(mission.deliverables.discoveryProfile);
    assert.ok(mission.deliverables.clientPlaybook || mission.constraints.clientPlaybook);
    assert.ok(mission.deliverables.campaign.playbook);
    assert.equal(mission.deliverables.campaign.playbook.playbookId, 'pb_anchor_cleaning');
    assert.equal(mission.progress.percent, 100);
    assert.match(mission.progress.currentStage, /review/i);
  });

  it('approve records review without sending', async () => {
    const engine = testEngine();
    const mission = await engine.createFromObjective({
      objective: 'Build Campaign 001',
      tenantId: '10',
      clientId: 10,
    });
    const reviewed = await engine.review({
      missionId: mission.id,
      action: 'approve',
      actor: 'jacob',
    });
    assert.equal(reviewed.status, MISSION_STATUS.REVIEWED);
    assert.equal(reviewed.review.outboundSent, false);
  });

  it('plans and executes Proposal Generation (SPEC-027B)', async () => {
    const engine = testEngine();
    const draft = engine.planner.plan({
      objective: 'Generate proposal for AS Cleaning Co.',
      tenantId: '1',
      clientId: 1,
      constraints: {
        discoverySummary: {
          companyName: 'AS Cleaning Co.',
          geography: 'Greater Toronto Area',
          companyStage: 'Four months in business',
          icp: ['Medical Offices', 'Dental Practices'],
          goals: ['Commercial growth'],
          challenges: ['Manual follow-up'],
          growthVision: 'Hire subcontractors',
          currentProcess: 'Manual follow-up today',
        },
        pricingPackageId: 'pilot',
      },
    });
    assert.equal(draft.type, MISSION_TYPES.PROPOSAL_GENERATION);
    assert.equal(draft.title, 'Proposal — AS Cleaning Co');
    assert.deepEqual(
      draft.plan.steps.map((s) => s.capabilityId),
      [BUILTIN_IDS.PROPOSAL_GENERATOR]
    );

    const mission = await engine.createFromObjective({
      objective: 'Generate proposal for AS Cleaning Co.',
      tenantId: '1',
      clientId: 1,
      constraints: draft.constraints,
    });
    assert.equal(mission.status, MISSION_STATUS.REVIEW_REQUIRED);
    assert.ok(mission.deliverables.proposal || mission.deliverables.document);
    assert.ok(mission.deliverables.reviewPackage);
    assert.ok(
      mission.deliverables.clientPlaybookId === 'pb_as_cleaning_co' ||
        (mission.constraints &&
          mission.constraints.clientPlaybookId === 'pb_as_cleaning_co')
    );
  });

  it('plans Mail Package Generation (SPEC-033)', () => {
    const engine = testEngine();
    const draft = engine.planner.plan({
      objective: 'Generate mail packages for Campaign 001',
      tenantId: '10',
      clientId: 10,
    });
    assert.equal(draft.type, MISSION_TYPES.MAIL_PACKAGE_GENERATION);
    assert.equal(draft.title, 'Mail Packages — Campaign 001');
    assert.deepEqual(
      draft.plan.steps.map((s) => s.capabilityId),
      [BUILTIN_IDS.MAIL_PACKAGE_GENERATOR]
    );
    assert.ok(draft.clientPlaybook);
    assert.equal(draft.clientPlaybook.id, 'pb_anchor_cleaning');
  });

  it('plans Campaign Review (SPEC-034)', () => {
    const engine = testEngine();
    const draft = engine.planner.plan({
      objective: 'Review Campaign 001',
      tenantId: '10',
      clientId: 10,
    });
    assert.equal(draft.type, MISSION_TYPES.CAMPAIGN_REVIEW);
    assert.equal(draft.title, 'Campaign Review — Campaign 001');
    assert.deepEqual(
      draft.plan.steps.map((s) => s.capabilityId),
      [BUILTIN_IDS.CAMPAIGN_REVIEW]
    );
    assert.ok(draft.clientPlaybook);
    assert.equal(draft.clientPlaybook.id, 'pb_anchor_cleaning');
  });

  it('plans Direct Mail Execution (SPEC-035)', () => {
    const engine = testEngine();
    const draft = engine.planner.plan({
      objective: 'Execute direct mail for Campaign 001',
      tenantId: '10',
      clientId: 10,
    });
    assert.equal(draft.type, MISSION_TYPES.DIRECT_MAIL_EXECUTION);
    assert.equal(draft.title, 'Direct Mail Execution — Campaign 001');
    assert.deepEqual(
      draft.plan.steps.map((s) => s.capabilityId),
      [BUILTIN_IDS.DIRECT_MAIL_EXECUTION]
    );
    assert.ok(draft.clientPlaybook);
    assert.equal(draft.clientPlaybook.id, 'pb_anchor_cleaning');
  });
});

describe('SPEC-022 architecture — no agent leakage', () => {
  it('planner and executor source do not import agent modules', () => {
    const root = path.join(__dirname, '..');
    const files = [
      'MissionPlanner.js',
      'MissionExecutor.js',
      'MissionEngine.js',
      'IntentRouter.js',
    ];
    for (const file of files) {
      const src = fs.readFileSync(path.join(root, file), 'utf8');
      assert.doesNotMatch(src, /leadgen\.js/);
      assert.doesNotMatch(src, /Agent\.js/);
      assert.doesNotMatch(src, /require\(['"][^'"]*Agent/);
    }
  });
});
