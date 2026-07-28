'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  createMissionPlanner,
  parseIntent,
  resolveExecutionRequest,
  resolveArtifacts,
  resolveCompatibleProducer,
  buildMissingProducerDiagnostic,
  formatDiagnosticMessage,
  ARTIFACT_SOURCES,
} = require('..');
const {
  createBuiltinRegistry,
  createCapabilityRegistry,
  BUILTIN_IDS,
  withArtifactContracts,
  CAPABILITY_CATEGORIES,
  buildCapabilityEstimate,
  buildCapabilityResult,
} = require('../../capabilities');

function testRegistry() {
  return createBuiltinRegistry({ discovery: { useFixture: true } });
}

describe('SPEC-054 Capability Registry queries', () => {
  it('lists producers and consumers for Campaign', () => {
    const registry = testRegistry();
    const producers = registry.producersOf('campaign');
    assert.ok(producers.some((p) => p.id === BUILTIN_IDS.CAMPAIGN_BUILDER));
    const consumers = registry.consumersOf('campaign');
    assert.ok(
      consumers.some(
        (c) =>
          c.id === BUILTIN_IDS.MAIL_PACKAGE_GENERATOR ||
          c.id === BUILTIN_IDS.CAMPAIGN_REVIEW
      )
    );
  });

  it('resolves mission aliases from the registry', () => {
    const registry = testRegistry();
    const match = registry.resolveAlias('Build Business Intelligence');
    assert.equal(match.known, true);
    assert.equal(match.capabilityId, BUILTIN_IDS.BUSINESS_INTELLIGENCE);
    assert.ok(match.confidence >= 0.9);
  });

  it('suggests matches for unknown mission text', () => {
    const registry = testRegistry();
    const suggestions = registry.suggestMatches('Completely unknown phrase', {
      limit: 5,
    });
    assert.ok(suggestions.length >= 1);
    assert.ok(
      suggestions.some(
        (s) =>
          /campaign|mail|review|intelligence|discovery/i.test(s.name) ||
          /campaign|mail|review/i.test(s.id)
      )
    );
  });

  it('explains why a disabled producer was not selected', () => {
    const registry = createCapabilityRegistry();
    registry.register(
      withArtifactContracts({
        id: 'ghost_mail',
        name: 'Ghost Mail Generator',
        description: 'test',
        category: CAPABILITY_CATEGORIES.CAMPAIGN,
        enabled: false,
        produces: ['mail_packages'],
        requires: [],
        canRun: () => true,
        estimate: () => buildCapabilityEstimate(),
        execute: async () => buildCapabilityResult(),
      })
    );
    const explanation = registry.explainSelection('ghost_mail', {
      artifactType: 'mail_packages',
    });
    assert.equal(explanation.selected, false);
    assert.match(explanation.reason, /disabled/i);
    assert.match(explanation.recommendedAction, /Enable/i);
  });

  it('attaches version, enabled, missionAliases on builtins', () => {
    const cap = testRegistry().get(BUILTIN_IDS.BUSINESS_INTELLIGENCE);
    assert.equal(cap.enabled, true);
    assert.equal(cap.version, 1);
    assert.ok(
      (cap.missionAliases || []).some((a) =>
        /business intelligence/i.test(a)
      )
    );
  });
});

describe('SPEC-054 Compatibility Resolver + missing producer diagnostics', () => {
  it('ranks Campaign producers from the registry', () => {
    const registry = testRegistry();
    const result = resolveCompatibleProducer('Campaign', { registry });
    assert.equal(result.ok, true);
    assert.equal(result.chosen.capabilityId, BUILTIN_IDS.CAMPAIGN_BUILDER);
  });

  it('emits deterministic diagnostic when no producer is registered', () => {
    const empty = createCapabilityRegistry();
    const result = resolveCompatibleProducer('ExecutionPackage', {
      registry: empty,
    });
    assert.equal(result.ok, false);
    assert.ok(result.diagnostic);
    assert.equal(result.diagnostic.status, 'Blocked');
    assert.equal(result.diagnostic.artifact, 'ExecutionPackage');
    assert.match(result.diagnostic.recommendedAction, /Register a capability/i);
    const msg = formatDiagnosticMessage(result.diagnostic);
    assert.doesNotMatch(msg, /Acquire via unavailable/i);
    assert.doesNotMatch(msg, /^Unknown capability/i);
    assert.match(msg, /Recommended Action:/i);
  });

  it('ArtifactResolver unavailable path includes recommended action', () => {
    const empty = createCapabilityRegistry();
    const resolution = resolveArtifacts({
      required: ['ExecutionPackage'],
      registry: empty,
      availableArtifacts: [],
    });
    const blocked = resolution.acquisitions.find(
      (a) => a.strategy === 'unavailable'
    );
    assert.ok(blocked);
    assert.ok(blocked.recommendedAction);
    assert.match(blocked.reason, /Recommended Action:/i);
    assert.doesNotMatch(blocked.reason, /Acquire via unavailable/i);
    assert.ok(
      resolution.summary.some((line) => /blocked|register/i.test(line))
    );
  });
});

describe('SPEC-054 unknown mission text diagnostics', () => {
  it('stores suggested matches in Notes — never invents execution nodes', () => {
    const registry = testRegistry();
    const plan = parseIntent(
      'Build Campaign 001. Teleport the prospects into hyperspace.',
      { registry }
    );
    assert.ok(
      plan.notes.some(
        (n) =>
          /no matching mission alias|hyperspace|suggested/i.test(n)
      )
    );
    assert.ok(
      !plan.execution.some((e) =>
        /hyperspace|teleport/i.test(String(e.stageId))
      )
    );
    assert.ok(!plan.notes.some((n) => /^Unknown capability\.?$/i.test(n)));
  });

  it('resolveExecutionRequest returns structured diagnostic', () => {
    const registry = testRegistry();
    const resolved = resolveExecutionRequest('Completely unknown phrase', {
      registry,
    });
    assert.equal(resolved.known, false);
    assert.ok(resolved.diagnostic);
    assert.equal(resolved.diagnostic.status, 'No matching mission alias');
    assert.ok(
      (resolved.diagnostic.suggestedMatches || []).length >= 1
    );
    assert.match(resolved.note, /Suggested|Rephrase|matching mission alias/i);
  });

  it('resolves Execute Campaign via registry aliases', () => {
    const registry = testRegistry();
    const resolved = resolveExecutionRequest('Execute Campaign 001', {
      registry,
    });
    assert.equal(resolved.known, true);
    assert.equal(resolved.capabilityId, BUILTIN_IDS.CAMPAIGN_BUILDER);
  });
});

describe('SPEC-054 MissionPlanner Planning Diagnostics', () => {
  it('attaches planningDiagnostics with selected capabilities', () => {
    const planner = createMissionPlanner({ registry: testRegistry() });
    const mission = planner.plan({
      objective:
        'Build Campaign 001 for Anchor Cleaning using the current ProspectList.',
      tenantId: '10',
      clientId: 10,
    });
    const diag = mission.plan.planningDiagnostics;
    assert.ok(diag);
    assert.ok(Array.isArray(diag.decisions));
    assert.ok(
      diag.decisions.some(
        (d) =>
          d.selected &&
          /business intelligence|sales intelligence|campaign/i.test(d.name)
      )
    );
  });

  it('records missing producer diagnostics when registry lacks a producer', () => {
    const registry = createCapabilityRegistry();
    // Minimal stubs so planning can form a graph that needs mail packages
    for (const id of [
      BUILTIN_IDS.PROSPECT_DISCOVERY,
      BUILTIN_IDS.COMPANY_ENRICHMENT,
      BUILTIN_IDS.KNOWLEDGE_UPDATE,
      BUILTIN_IDS.OPPORTUNITY_RANKING,
      BUILTIN_IDS.BUSINESS_INTELLIGENCE,
      BUILTIN_IDS.SALES_INTELLIGENCE,
      BUILTIN_IDS.CAMPAIGN_BUILDER,
    ]) {
      registry.register(
        withArtifactContracts({
          id,
          name: id.replace(/_/g, ' '),
          description: 'test',
          category: CAPABILITY_CATEGORIES.CAMPAIGN,
          canRun: () => true,
          estimate: () => buildCapabilityEstimate({ durationMs: 1 }),
          execute: async () => buildCapabilityResult(),
        })
      );
    }

    const diagnostic = buildMissingProducerDiagnostic({
      artifact: 'MailPackage',
      expectedProducer: 'Mail Package Generator',
    });
    assert.equal(diagnostic.status, 'Blocked');
    assert.match(diagnostic.recommendedAction, /Mail Package Generator/i);

    // Direct registry query: no mail package producer
    const producers = registry.producersOf('mail_packages');
    assert.equal(producers.length, 0);
    const compat = resolveCompatibleProducer('MailPackage', { registry });
    assert.equal(compat.ok, false);
    assert.match(compat.diagnostic.recommendedAction, /Register/i);
  });

  it('does not emit bare Unknown capability in planner notes', () => {
    const planner = createMissionPlanner({ registry: testRegistry() });
    const mission = planner.plan({
      objective: 'Build Campaign 001. Warp drive engagement sequence.',
      tenantId: '10',
      clientId: 10,
    });
    const notes = (mission.missionPlan && mission.missionPlan.notes) || [];
    const blob = JSON.stringify(mission.plan.planningDiagnostics || {});
    assert.ok(
      !notes.some((n) => /^Unknown capability\.?\s*$/i.test(String(n).trim()))
    );
    assert.doesNotMatch(blob, /Acquire via unavailable/i);
  });
});

describe('SPEC-054 ArtifactResolver registry producers', () => {
  it('prefers registry producers when resolving ProspectList acquisition', () => {
    const registry = testRegistry();
    const resolution = resolveArtifacts({
      required: ['ProspectList'],
      registry,
      availableArtifacts: [],
    });
    const acquire = resolution.acquisitions.find(
      (a) => a.strategy === 'capability_acquisition'
    );
    assert.ok(acquire);
    assert.equal(acquire.capabilityId, BUILTIN_IDS.PROSPECT_DISCOVERY);
    assert.match(acquire.reason, /acquire via/i);
    assert.ok(acquire.stageName);
  });

  it('skips acquisition when compatible artifact exists', () => {
    const registry = testRegistry();
    const resolution = resolveArtifacts({
      required: ['ProspectList'],
      registry,
      availableArtifacts: [
        {
          type: 'ProspectList',
          source: ARTIFACT_SOURCES.OPERATOR_IMPORT,
          confidence: 'High',
          compatible: true,
        },
      ],
    });
    assert.ok(resolution.resolved.some((r) => r.type === 'ProspectList'));
    assert.ok(
      resolution.acquisitions.every((a) => a.strategy === 'use_existing')
    );
  });
});
