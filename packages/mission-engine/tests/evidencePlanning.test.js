'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  understandIntent,
  planFromIntent,
  planFromOperatorText,
  planEvidence,
  INTENT_CATEGORIES,
  EVIDENCE_TYPES,
  summarizeEvidencePlan,
  summarizeMissionIntent,
  createMissionEngine,
} = require('..');
const {
  BUILTIN_IDS: CAP_IDS,
  createBuiltinRegistry,
  createCapabilityRegistry,
  withArtifactContracts,
  createDiscoveryDiagnosticsCapability,
} = require('../../capabilities');

function testEngine() {
  return createMissionEngine({
    registry: createBuiltinRegistry({
      discovery: { useFixture: true },
    }),
  });
}

describe('SPEC-056 MissionIntent requiresEvidence', () => {
  it('declares evidence requirements for Campaign Diagnostics', () => {
    const intent = understandIntent(
      'Why did Campaign 001 fail?'
    );
    assert.equal(intent.matchedIntent, INTENT_CATEGORIES.CAMPAIGN_DIAGNOSTICS);
    assert.ok(Array.isArray(intent.requiresEvidence));
    assert.ok(intent.requiresEvidence.includes(EVIDENCE_TYPES.DISCOVERY_TRACE));
    assert.ok(
      intent.requiresEvidence.includes(EVIDENCE_TYPES.DISCOVERY_DIAGNOSTICS)
    );
    assert.ok(intent.requiresEvidence.includes(EVIDENCE_TYPES.MISSION_STATE));
    const summary = summarizeMissionIntent(intent);
    assert.ok(summary.requiresEvidence.length >= 3);
  });

  it('declares discovery evidence for Discovery Investigation', () => {
    const intent = understandIntent("Why isn't Discovery finding anyone?");
    assert.equal(
      intent.matchedIntent,
      INTENT_CATEGORIES.DISCOVERY_INVESTIGATION
    );
    assert.ok(
      intent.requiresEvidence.includes(EVIDENCE_TYPES.PROVIDER_SELECTION)
    );
    assert.ok(
      intent.requiresEvidence.includes(EVIDENCE_TYPES.CANDIDATE_COUNTS)
    );
    assert.ok(
      intent.requiresEvidence.includes(EVIDENCE_TYPES.VERIFICATION_RESULTS)
    );
  });
});

describe('SPEC-056 Evidence Planning', () => {
  it('marks MissionState available and DiscoveryTrace missing by default', () => {
    const intent = understandIntent(
      'Run an end-to-end execution audit for Campaign 001.'
    );
    const evidence = planEvidence(intent, {
      registry: createBuiltinRegistry({ discovery: { useFixture: true } }),
    });
    assert.ok(evidence.available.includes(EVIDENCE_TYPES.MISSION_STATE));
    assert.ok(evidence.missing.includes(EVIDENCE_TYPES.DISCOVERY_TRACE));
    assert.ok(evidence.missing.includes(EVIDENCE_TYPES.DISCOVERY_DIAGNOSTICS));
    assert.equal(evidence.unableToAnswer, false);
    assert.ok(
      evidence.acquisitions.some(
        (a) => a.capabilityId === CAP_IDS.DISCOVERY_DIAGNOSTICS
      )
    );
  });

  it('does not schedule producers when evidence already in catalog', () => {
    const intent = understandIntent(
      'Run an end-to-end execution audit for Campaign 001.'
    );
    const evidence = planEvidence(intent, {
      availableArtifacts: [
        EVIDENCE_TYPES.MISSION_STATE,
        EVIDENCE_TYPES.DISCOVERY_EXECUTION,
        EVIDENCE_TYPES.DISCOVERY_TRACE,
        EVIDENCE_TYPES.DISCOVERY_DIAGNOSTICS,
      ],
      registry: createBuiltinRegistry({ discovery: { useFixture: true } }),
    });
    assert.equal(evidence.missing.length, 0);
    assert.equal(evidence.acquisitions.length, 0);
    assert.equal(evidence.unableToAnswer, false);
  });

  it('blocks with Unable to answer when producer is not registered', () => {
    const intent = understandIntent(
      'Run an end-to-end execution audit for Campaign 001.'
    );
    // Empty registry — Discovery Diagnostics not registered
    const empty = createCapabilityRegistry();
    const evidence = planEvidence(intent, { registry: empty });
    assert.equal(evidence.unableToAnswer, true);
    assert.ok(evidence.blocked.length > 0);
    assert.match(evidence.reason || '', /Unable to answer/i);
    assert.ok(
      evidence.blocked.some((b) => /No registered producer/i.test(b.reason))
    );
  });
});

describe('SPEC-056 Capability Planning after Evidence', () => {
  it('schedules Discovery Diagnostics before Campaign Review for campaign diagnostics', () => {
    const result = planFromOperatorText(
      'Why did Campaign 001 fail?',
      {
        registry: createBuiltinRegistry({ discovery: { useFixture: true } }),
      }
    );
    assert.equal(result.unableToAnswer, false);
    assert.equal(result.clarification, false);
    const stages = result.missionPlan.execution.map((e) => e.stageId);
    assert.equal(stages[0], 'discovery_diagnostics');
    assert.ok(stages.includes('campaign_review'));
    assert.ok(stages.includes('outcome_intelligence'));
    assert.ok(result.evidencePlan);
    assert.ok(result.evidencePlanSummary);
  });

  it('schedules Discovery Diagnostics → Campaign Review for discovery investigation', () => {
    const result = planFromOperatorText("Why isn't Discovery finding anyone?", {
      registry: createBuiltinRegistry({ discovery: { useFixture: true } }),
    });
    assert.equal(result.unableToAnswer, false);
    const stages = result.missionPlan.execution.map((e) => e.stageId);
    assert.ok(stages.includes('discovery_diagnostics'));
    assert.ok(stages.includes('campaign_review'));
    // Must not answer by re-running Discovery as a substitute for evidence
    assert.ok(!stages.includes('prospect_discovery'));
  });

  it('returns unableToAnswer instead of inventing a diagnostic plan', () => {
    const intent = understandIntent('Why did Campaign 001 fail?');
    const result = planFromIntent(intent, {
      registry: createCapabilityRegistry(),
    });
    assert.equal(result.unableToAnswer, true);
    assert.equal(result.missionPlan, null);
    assert.match(result.reason || '', /Unable to answer/i);
  });

  it('leaves non-diagnostic Campaign Execution unchanged', () => {
    const result = planFromOperatorText('Run the campaign for Campaign 001.', {
      registry: createBuiltinRegistry({ discovery: { useFixture: true } }),
    });
    assert.equal(result.unableToAnswer, false);
    const stages = result.missionPlan.execution.map((e) => e.stageId);
    assert.deepEqual(stages, ['direct_mail_execution']);
  });
});

describe('SPEC-056 Discovery Diagnostics capability', () => {
  it('is read-only and produces typed diagnostic artifacts', async () => {
    const cap = withArtifactContracts(createDiscoveryDiagnosticsCapability());
    assert.equal(cap.readOnly, true);
    assert.equal(cap.mutatesBusinessState, false);
    assert.ok(cap.produces.includes('DiscoveryTrace'));
    assert.ok(cap.produces.includes('DiscoveryDiagnostics'));

    const result = await cap.execute({
      constraints: {
        discoveryDiagnosticsSeed: {
          provider: 'Google Maps',
          rawCandidateCount: 83,
          verifiedCount: 61,
          rejectedByConfidence: 61,
          confidenceThreshold: 0.7,
          minimumRequired: 70,
          blocked: true,
        },
      },
      inputs: {},
    });
    assert.equal(result.outputs.mutatesBusinessState, false);
    assert.equal(result.outputs.readOnly, true);
    assert.match(
      result.outputs.explanation,
      /Google Maps/
    );
    assert.match(result.outputs.explanation, /83 raw/);
    assert.match(result.outputs.explanation, /61 verified/);
    assert.ok(
      result.artifacts.some((a) => a.type === 'DiscoveryDiagnostics' && a.readOnly)
    );
  });
});

describe('SPEC-056 MissionPlanner three-stage planning', () => {
  it('plans Why did Campaign fail with Discovery Diagnostics first', () => {
    const engine = testEngine();
    const draft = engine.planner.plan({
      objective: 'Why did Campaign 001 fail?',
      tenantId: '10',
      clientId: 10,
    });
    assert.equal(draft.unableToAnswer, undefined);
    assert.ok(draft.evidencePlan);
    assert.ok(draft.evidencePlanSummary);
    assert.ok(draft.plan.evidencePlanSummary);
    const stepIds = draft.plan.steps.map((s) => s.capabilityId);
    assert.ok(stepIds.includes(CAP_IDS.DISCOVERY_DIAGNOSTICS));
    assert.ok(stepIds.includes(CAP_IDS.CAMPAIGN_REVIEW));
    const diagIndex = stepIds.indexOf(CAP_IDS.DISCOVERY_DIAGNOSTICS);
    const reviewIndex = stepIds.indexOf(CAP_IDS.CAMPAIGN_REVIEW);
    assert.ok(diagIndex >= 0 && reviewIndex > diagIndex);
  });

  it('returns Unable to answer draft when producer missing', () => {
    // Empty registry — Discovery Diagnostics not registered
    const engine = createMissionEngine({
      registry: createCapabilityRegistry(),
    });
    const draft = engine.planner.plan({
      objective: 'Why did Campaign 001 fail?',
      tenantId: '10',
      clientId: 10,
    });
    assert.equal(draft.unableToAnswer, true);
    assert.equal(draft.missionPlan, null);
    assert.ok(draft.plan.unableToAnswer);
    assert.match(String(draft.blockingIssues[0] || ''), /Unable to answer/i);
    const summary = summarizeEvidencePlan(draft.evidencePlan);
    assert.equal(summary.unableToAnswer, true);
    assert.ok(summary.items.some((i) => i.status === 'blocked'));
  });
});
