'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  createMissionEngine,
  evaluatePipelineGate,
  STAGE_OUTCOMES,
  AUDIT_KINDS,
  MISSION_STATUS,
} = require('..');
const {
  createBuiltinRegistry,
  CAPABILITY_RESULT_STATUS,
  BUILTIN_IDS,
} = require('../../capabilities');
const discovery = require('../../capabilities/discovery');

function testEngine() {
  return createMissionEngine({
    registry: createBuiltinRegistry({ discovery: { useFixture: true } }),
  });
}

describe('SPEC-040 DiscoveryProfileResolver', () => {
  it('resolves pinned client profile with report (Anchor → Manchester)', () => {
    const resolver = discovery.createDiscoveryProfileResolver();
    const report = resolver.resolve({
      objective: 'Build Campaign 001 for Anchor Cleaning',
      clientId: 10,
      tenantId: '10',
    });
    assert.equal(report.blocked, false);
    assert.equal(report.profile.name, 'Commercial Cleaning - Manchester');
    assert.equal(report.selection, 'pinned_client_profile');
    assert.equal(report.reason, 'Pinned client profile');
    assert.ok(report.confidence >= 0.8);
    assert.ok(report.geography);
    assert.match(String(report.geography.label || ''), /Manchester/i);
  });

  it('never hops to Boston when client has Manchester profiles', () => {
    const resolver = discovery.createDiscoveryProfileResolver();
    const report = resolver.resolve({
      objective: 'Build Campaign 001 for Boston market for Anchor Cleaning',
      clientId: 10,
      tenantId: '10',
    });
    assert.equal(report.blocked, false);
    assert.ok(report.profile.clientIds.some((c) => String(c) === '10'));
    assert.match(String(report.profile.geography.label || ''), /Manchester/i);
    assert.ok(!/Boston/i.test(String(report.profile.name || '')));
  });

  it('honors explicit operator override profile id', () => {
    const resolver = discovery.createDiscoveryProfileResolver();
    const report = resolver.resolve({
      objective: 'Build Campaign 001',
      clientId: 10,
      constraints: {
        operatorOverrideProfileId: 'dp_overflow_cleaning_partners',
      },
    });
    assert.equal(report.selection, 'explicit_operator_override');
    assert.equal(report.profile.id, 'dp_overflow_cleaning_partners');
    assert.equal(report.confidence, 1);
    assert.ok(report.overridesApplied.length >= 1);
  });

  it('blocks when override profile id is missing', () => {
    const resolver = discovery.createDiscoveryProfileResolver();
    const report = resolver.resolve({
      objective: 'Build Campaign 001',
      clientId: 10,
      constraints: {
        operatorOverrideProfileId: 'dp_does_not_exist',
      },
    });
    assert.equal(report.blocked, true);
    assert.equal(report.profile, null);
    assert.ok(report.blockingIssues.length >= 1);
  });
});

describe('SPEC-040 PipelineGate', () => {
  it('blocks empty discovery artifacts', () => {
    const gate = evaluatePipelineGate({
      capabilityId: BUILTIN_IDS.PROSPECT_DISCOVERY,
      runResult: {
        result: {
          status: CAPABILITY_RESULT_STATUS.COMPLETED,
          outputs: {
            prospectCount: 0,
            prospects: [],
            targetCount: 20,
            discoveryProfile: {
              id: 'dp_x',
              name: 'Test',
              version: '1.0',
              geography: { label: 'Manchester, NH', cities: ['Manchester'] },
            },
            summary: { discovered: 0, verified: 0, rejected: 0 },
          },
          artifacts: [{ type: 'prospect_list', count: 0 }],
          warnings: [],
          errors: [],
        },
      },
      context: { constraints: { targetCount: 20 } },
    });
    assert.equal(gate.outcome, STAGE_OUTCOMES.BLOCKED);
    assert.equal(gate.advance, false);
    assert.equal(gate.publishOutputs, false);
    assert.ok(
      gate.blockingIssues.some((i) => /zero verified/i.test(i))
    );
    assert.ok(gate.quarantinedArtifacts.length >= 1);
  });

  it('warns on yield shortfall', () => {
    const prospects = Array.from({ length: 17 }, (_, i) => ({
      id: `p${i}`,
      companyName: `Co ${i}`,
    }));
    const gate = evaluatePipelineGate({
      capabilityId: BUILTIN_IDS.PROSPECT_DISCOVERY,
      runResult: {
        result: {
          status: CAPABILITY_RESULT_STATUS.COMPLETED,
          outputs: {
            prospectCount: 17,
            prospects,
            targetCount: 20,
            discoveryProfile: {
              id: 'dp_x',
              name: 'Test',
              version: '1.0',
              geography: { label: 'Manchester, NH', cities: ['Manchester'] },
            },
          },
          artifacts: [{ type: 'prospect_list', count: 17 }],
          warnings: ['Requested 20 prospects; confidently verified 17.'],
          errors: [],
        },
      },
      context: { constraints: { targetCount: 20 } },
    });
    assert.equal(gate.outcome, STAGE_OUTCOMES.COMPLETED_WITH_WARNINGS);
    assert.equal(gate.advance, true);
    assert.equal(gate.publishOutputs, true);
    assert.match(
      gate.reviewSummary.label || '',
      /17 of 20/
    );
  });

  it('blocks campaign with zero prospects', () => {
    const gate = evaluatePipelineGate({
      capabilityId: BUILTIN_IDS.CAMPAIGN_BUILDER,
      runResult: {
        result: {
          status: CAPABILITY_RESULT_STATUS.COMPLETED,
          outputs: {
            campaign: {
              name: 'Campaign Empty',
              prospectCount: 0,
              prospects: [],
              mailMerge: [],
            },
          },
          artifacts: [{ type: 'campaign_draft', prospectCount: 0 }],
          warnings: [],
          errors: [],
        },
      },
    });
    assert.equal(gate.outcome, STAGE_OUTCOMES.BLOCKED);
    assert.equal(gate.advance, false);
  });
});

describe('SPEC-040 MissionExecutor gate', () => {
  it('pauses mission when discovery yields zero (no enrichment)', async () => {
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
    assert.ok(
      (mission.blockingIssues || []).some((i) => /zero verified/i.test(i)) ||
        /Blocked/i.test(mission.progress.currentStage || '')
    );
    const discoveryStep = (mission.plan.steps || []).find(
      (s) => s.capabilityId === BUILTIN_IDS.PROSPECT_DISCOVERY
    );
    assert.ok(discoveryStep);
    assert.equal(discoveryStep.status, 'blocked');
    assert.equal(discoveryStep.outcome, STAGE_OUTCOMES.BLOCKED);
    const enrichment = (mission.plan.steps || []).find(
      (s) => s.capabilityId === BUILTIN_IDS.COMPANY_ENRICHMENT
    );
    assert.ok(enrichment);
    assert.notEqual(enrichment.status, 'completed');
    assert.notEqual(enrichment.status, 'running');
    const audits = await engine.listAudit(mission.id);
    assert.ok(
      audits.some((a) => a.kind === AUDIT_KINDS.STAGE_BLOCKED)
    );
  });

  it('completes fixture discovery through review when yield is positive', async () => {
    const engine = testEngine();
    const mission = await engine.createFromObjective({
      objective: 'Build Campaign 001 for Anchor Cleaning',
      tenantId: '10',
      clientId: 10,
      constraints: { targetCount: 5 },
      execute: true,
    });
    assert.equal(mission.status, MISSION_STATUS.REVIEW_REQUIRED);
    assert.ok(mission.discoveryProfile);
    assert.equal(
      mission.discoveryProfile.reason || 'Pinned client profile',
      'Pinned client profile'
    );
    assert.ok(
      (mission.deliverables.stepResults || []).every(
        (s) =>
          s.outcome === STAGE_OUTCOMES.COMPLETED ||
          s.outcome === STAGE_OUTCOMES.COMPLETED_WITH_WARNINGS
      )
    );
  });

  it('flag-off advances empty discovery (legacy technical complete)', async () => {
    const prev = process.env.MISSION_ARTIFACT_VALIDATION;
    process.env.MISSION_ARTIFACT_VALIDATION = '0';
    try {
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
      // Without gate, empty discovery still completes and pipeline continues
      assert.notEqual(
        (mission.plan.steps || []).find(
          (s) => s.capabilityId === BUILTIN_IDS.PROSPECT_DISCOVERY
        )?.status,
        'blocked'
      );
    } finally {
      if (prev == null) delete process.env.MISSION_ARTIFACT_VALIDATION;
      else process.env.MISSION_ARTIFACT_VALIDATION = prev;
    }
  });
});
