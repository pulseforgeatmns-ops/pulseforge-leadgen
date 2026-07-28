'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  createMissionEngine,
  createArtifactBus,
  ArtifactRegistry,
  ARTIFACT_EVENTS,
  AUDIT_KINDS,
  MISSION_STATUS,
  ARTIFACT_VALIDATION_STATUS,
} = require('..');
const {
  createBuiltinRegistry,
  BUILTIN_IDS,
} = require('../../capabilities');

const {
  ARTIFACT_TYPES,
  resolveArtifactType,
  draftsFromCapabilityOutputs,
} = ArtifactRegistry;

function testEngine() {
  return createMissionEngine({
    registry: createBuiltinRegistry({ discovery: { useFixture: true } }),
  });
}

describe('SPEC-042 ArtifactRegistry', () => {
  it('resolves Stage Library aliases to typed names', () => {
    assert.equal(resolveArtifactType('prospect_list'), ARTIFACT_TYPES.PROSPECT_LIST);
    assert.equal(resolveArtifactType('ranked_prospects'), ARTIFACT_TYPES.OPPORTUNITY_RANKING);
    assert.equal(resolveArtifactType('enriched_list'), ARTIFACT_TYPES.COMPANY_INTELLIGENCE);
    assert.equal(resolveArtifactType('ProspectList'), ARTIFACT_TYPES.PROSPECT_LIST);
    assert.equal(resolveArtifactType('mail_packages'), ARTIFACT_TYPES.MAIL_PACKAGE);
  });

  it('builds drafts from capability outputs', () => {
    const drafts = draftsFromCapabilityOutputs(['prospect_list'], {
      prospects: [{ id: 1 }, { id: 2 }],
      prospectCount: 2,
      targetCount: 20,
    });
    assert.equal(drafts.length, 1);
    assert.equal(drafts[0].artifactType, ARTIFACT_TYPES.PROSPECT_LIST);
    assert.equal(drafts[0].payload.prospectCount, 2);
  });
});

describe('SPEC-042 ArtifactBus', () => {
  it('publishes immutable versioned artifacts', () => {
    const bus = createArtifactBus();
    const v1 = bus.publishArtifact({
      missionId: 'msn_1',
      stageId: 'prospect_discovery',
      artifactType: 'ProspectList',
      producer: BUILTIN_IDS.PROSPECT_DISCOVERY,
      payload: {
        prospects: [{ id: 'p1' }],
        prospectCount: 1,
        targetCount: 1,
      },
      validationStatus: ARTIFACT_VALIDATION_STATUS.VALID,
      skipRegistryValidation: true,
    });
    assert.equal(v1.revision, 1);
    assert.throws(() => {
      v1.payload.prospectCount = 99;
    });

    const v2 = bus.publishArtifact({
      missionId: 'msn_1',
      stageId: 'prospect_discovery',
      artifactType: 'prospect_list',
      producer: BUILTIN_IDS.PROSPECT_DISCOVERY,
      payload: {
        prospects: [{ id: 'p1' }, { id: 'p2' }],
        prospectCount: 2,
        targetCount: 2,
      },
      validationStatus: ARTIFACT_VALIDATION_STATUS.VALID,
      skipRegistryValidation: true,
    });
    assert.equal(v2.revision, 2);
    assert.equal(bus.getLatestArtifact('msn_1', 'ProspectList').id, v2.id);
    assert.equal(bus.getArtifactHistory('msn_1', 'ProspectList').length, 2);
    assert.equal(bus.getArtifact(v1.id).supersededBy, v2.id);
  });

  it('hides quarantined artifacts from getLatestArtifact', () => {
    const bus = createArtifactBus();
    bus.publishArtifact({
      missionId: 'msn_q',
      artifactType: 'ProspectList',
      payload: { prospects: [{ id: 1 }], prospectCount: 1 },
      validationStatus: ARTIFACT_VALIDATION_STATUS.VALID,
      skipRegistryValidation: true,
    });
    bus.publishArtifact({
      missionId: 'msn_q',
      artifactType: 'ProspectList',
      payload: { prospects: [], prospectCount: 0 },
      validationStatus: ARTIFACT_VALIDATION_STATUS.QUARANTINED,
      skipRegistryValidation: true,
    });
    const latest = bus.getLatestArtifact('msn_q', 'ProspectList');
    assert.ok(latest);
    assert.equal(latest.revision, 1);
    assert.equal(latest.validationStatus, ARTIFACT_VALIDATION_STATUS.VALID);
    assert.equal(
      bus.consumeArtifact('msn_q', 'ProspectList').revision,
      1
    );
  });

  it('validates before publication when not skipped', () => {
    const bus = createArtifactBus();
    const art = bus.publishArtifact({
      missionId: 'msn_v',
      artifactType: 'ProspectList',
      payload: { prospects: [], prospectCount: 0 },
    });
    assert.equal(art.validationStatus, ARTIFACT_VALIDATION_STATUS.QUARANTINED);
    assert.equal(bus.getLatestArtifact('msn_v', 'ProspectList'), null);
  });

  it('compares revisions and builds replay plans', () => {
    const bus = createArtifactBus();
    const list = bus.publishArtifact({
      missionId: 'msn_r',
      stageId: 'prospect_discovery',
      artifactType: 'ProspectList',
      payload: { prospects: [{ id: 1 }], prospectCount: 1 },
      validationStatus: ARTIFACT_VALIDATION_STATUS.VALID,
      skipRegistryValidation: true,
    });
    const ranked = bus.publishArtifact({
      missionId: 'msn_r',
      stageId: 'opportunity_ranking',
      artifactType: 'OpportunityRanking',
      payload: { prospects: [{ id: 1, rank: 1 }], rankedCount: 1 },
      dependencies: [
        { artifactType: 'ProspectList', artifactId: list.id, revision: 1 },
      ],
      validationStatus: ARTIFACT_VALIDATION_STATUS.VALID,
      skipRegistryValidation: true,
    });
    const campaign = bus.publishArtifact({
      missionId: 'msn_r',
      stageId: 'campaign_builder',
      artifactType: 'Campaign',
      payload: {
        campaign: { name: 'C1', prospectCount: 1, prospects: [{ id: 1 }] },
      },
      dependencies: [
        {
          artifactType: 'OpportunityRanking',
          artifactId: ranked.id,
          revision: 1,
        },
      ],
      validationStatus: ARTIFACT_VALIDATION_STATUS.VALID,
      skipRegistryValidation: true,
    });

    const campaign2 = bus.publishArtifact({
      missionId: 'msn_r',
      stageId: 'campaign_builder',
      artifactType: 'Campaign',
      payload: {
        campaign: {
          name: 'C2',
          prospectCount: 1,
          prospects: [{ id: 1 }],
          mailMerge: [{ personalizationSentence: 'Hello' }],
        },
      },
      validationStatus: ARTIFACT_VALIDATION_STATUS.VALID,
      skipRegistryValidation: true,
    });

    const cmp = bus.compareArtifacts(campaign.id, campaign2.id);
    assert.equal(cmp.ok, true);
    assert.equal(cmp.sameType, true);
    assert.ok(cmp.changedKeys.includes('campaign'));
    assert.equal(cmp.highlights.changedPersonalization, true);

    const replay = bus.replayFromArtifact('msn_r', campaign2.id, {
      planStageIds: [
        'prospect_discovery',
        'company_enrichment',
        'opportunity_ranking',
        'campaign_builder',
      ],
    });
    assert.equal(replay.ok, true);
    assert.equal(replay.startStageId, 'campaign_builder');
    assert.deepEqual(replay.skipStageIds, [
      'prospect_discovery',
      'company_enrichment',
      'opportunity_ranking',
    ]);
    assert.ok(replay.reuseArtifactIds.includes(list.id));
  });

  it('round-trips via toJSON / fromJSON', () => {
    const bus = createArtifactBus();
    bus.publishArtifact({
      missionId: 'msn_s',
      artifactType: 'ProspectList',
      payload: { prospects: [{ id: 1 }], prospectCount: 1 },
      validationStatus: ARTIFACT_VALIDATION_STATUS.VALID,
      skipRegistryValidation: true,
    });
    const restored = createArtifactBus({ snapshot: bus.toJSON() });
    assert.equal(
      restored.getLatestArtifact('msn_s', 'ProspectList').payload.prospectCount,
      1
    );
    assert.ok(
      restored.events('msn_s').some((e) => e.type === ARTIFACT_EVENTS.PUBLISHED)
    );
  });
});

describe('SPEC-042 MissionExecutor Artifact Bus integration', () => {
  it('publishes typed artifacts through a successful campaign mission', async () => {
    const engine = testEngine();
    const mission = await engine.createFromObjective({
      objective: 'Build a campaign for Anchor Cleaning in Manchester NH',
      tenantId: '10',
      clientId: 10,
      constraints: { targetCount: 5 },
      execute: true,
    });

    assert.ok(
      [MISSION_STATUS.REVIEW_REQUIRED, MISSION_STATUS.WAITING].includes(
        mission.status
      )
    );

    if (mission.status === MISSION_STATUS.REVIEW_REQUIRED) {
      assert.ok(mission.deliverables);
      assert.ok(mission.deliverables.artifactBus);
      const bus = createArtifactBus({
        snapshot: mission.deliverables.artifactBus,
      });
      const prospectList = bus.getLatestArtifact(mission.id, 'ProspectList');
      assert.ok(prospectList, 'ProspectList should be published');
      assert.ok(prospectList.revision >= 1);
      assert.ok(
        [
          ARTIFACT_VALIDATION_STATUS.VALID,
          ARTIFACT_VALIDATION_STATUS.VALID_WITH_WARNINGS,
        ].includes(prospectList.validationStatus)
      );

      const ranking = bus.getLatestArtifact(mission.id, 'OpportunityRanking');
      assert.ok(ranking, 'OpportunityRanking should be published');

      const campaign = bus.getLatestArtifact(mission.id, 'Campaign');
      assert.ok(campaign, 'Campaign should be published');

      const workspace = await engine.getWorkspace(mission.id);
      assert.ok(Array.isArray(workspace.artifacts));
      assert.ok(workspace.artifacts.length >= 1);
      assert.ok(workspace.artifactGraph);
      assert.ok(
        workspace.artifacts.some((a) => a.artifactType === 'ProspectList')
      );

      const audit = await engine.listAudit(mission.id);
      assert.ok(
        audit.some((e) => e.kind === AUDIT_KINDS.ARTIFACT_PUBLISHED),
        'audit should include artifact_published'
      );
      assert.ok(
        audit.some((e) => e.kind === AUDIT_KINDS.ARTIFACT_CONSUMED),
        'audit should include artifact_consumed'
      );
    }
  });

  it('does not expose quarantined empty discovery to consumers', async () => {
    const emptyProvider = {
      id: 'empty',
      available: () => true,
      async search() {
        return [];
      },
    };
    const engine = createMissionEngine({
      registry: createBuiltinRegistry({
        discovery: { searchProviders: [emptyProvider] },
      }),
    });
    const mission = await engine.createFromObjective({
      objective: 'Build Campaign 001 for Anchor Cleaning',
      tenantId: '10',
      clientId: 10,
      constraints: { targetCount: 20 },
      execute: true,
    });
    assert.equal(mission.status, MISSION_STATUS.WAITING);
    assert.ok(mission.deliverables && mission.deliverables.artifactBus);
    const bus = createArtifactBus({
      snapshot: mission.deliverables.artifactBus,
    });
    assert.equal(bus.getLatestArtifact(mission.id, 'ProspectList'), null);
    const history = bus.getArtifactHistory(mission.id, 'ProspectList');
    assert.ok(history.length >= 1);
    assert.equal(
      history[history.length - 1].validationStatus,
      ARTIFACT_VALIDATION_STATUS.QUARANTINED
    );
    const enrichment = (mission.plan.steps || []).find(
      (s) => s.capabilityId === BUILTIN_IDS.COMPANY_ENRICHMENT
    );
    assert.ok(enrichment);
    assert.notEqual(enrichment.status, 'completed');
  });
});
