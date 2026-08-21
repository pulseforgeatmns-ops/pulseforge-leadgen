'use strict';

/**
 * SPEC-132 — Specialist Execution Contract.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const amo = require('../index');
const {
  EXECUTION_STATUSES,
  RECOMMENDATION_TIERS,
  buildExecutionInput,
  createExecutionResult,
  blockedExecutionResult,
  failedExecutionResult,
  validateExecutionResult,
  executeSpecialist,
  fromScoutLegacyOutput,
  executionResultFromStageOutput,
  normalizeConfidence,
  normalizeEvidence,
  assertExecutionResult,
  SPECIALISTS,
  createAcquisitionMissionEngine,
} = amo;

const OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester NH for law firms.';

function missionWithPlan(engine) {
  return engine.create({
    tenantId: '10',
    objective: OBJECTIVE,
    targetSegment: 'Law Firms',
    planApproved: true,
  });
}

function scoutContributions() {
  return {
    companies: [{ id: 'c1', name: 'Harbor Law Group' }],
    prospects: [{ id: 'p1', name: 'Alex Morgan', title: 'Office Manager' }],
    buyingSignals: [{ label: 'Hiring office coordinator', source: 'linkedin' }],
    evidence: [{
      label: 'LinkedIn hiring post',
      source: 'linkedin',
      confidence: 0.8,
      timestamp: '2026-07-28T00:00:00.000Z',
      provenance: { kind: 'observed', source: 'linkedin' },
    }],
    confidence: 0.82,
  };
}

describe('SPEC-132 — Specialist Execution Contract', () => {
  it('buildExecutionInput assembles mission plan, context, and specialist input', () => {
    const engine = createAcquisitionMissionEngine();
    const mission = missionWithPlan(engine);
    const input = buildExecutionInput({
      mission,
      specialist: SPECIALISTS.SCOUT,
      transactionId: 'tme_test_1',
    });

    assert.equal(input.spec, 'SPEC-132');
    assert.equal(input.transactionId, 'tme_test_1');
    assert.equal(input.specialist, SPECIALISTS.SCOUT);
    assert.ok(input.missionPlan);
    assert.equal(input.missionPlan.immutable, true);
    assert.ok(input.executionContext.missionId);
    assert.ok(input.workspaceContext.mission);
    assert.ok(input.evidencePolicy);
    assert.equal(input.structuredOnly, true);
    assert.equal(input.specialistInput.segment, input.missionPlan.market.segment);
    assert.equal(input.specialistInput.missionBound, true);
  });

  it('normalizes structured confidence — never a bare float in results', () => {
    const structured = normalizeConfidence(0.75);
    assert.equal(structured.overall, 0.75);
    assert.equal(structured.evidence, 0.75);
    assert.equal(structured.fit, 0.75);
    assert.equal(structured.completeness, 0.75);

    const decomposed = normalizeConfidence({
      overall: 0.8,
      evidence: 0.9,
      fit: 0.7,
      completeness: 0.6,
    });
    assert.equal(decomposed.overall, 0.8);
    assert.equal(decomposed.evidence, 0.9);
    assert.equal(decomposed.fit, 0.7);
    assert.equal(decomposed.completeness, 0.6);
  });

  it('requires provenance on every evidence claim', () => {
    const evidence = normalizeEvidence([
      { label: 'Job post', source: 'job_board', confidence: 0.7 },
    ]);
    assert.equal(evidence.length, 1);
    assert.ok(evidence[0].provenance);
    assert.ok(evidence[0].timestamp);
    assert.equal(evidence[0].source, 'job_board');
  });

  it('createExecutionResult enforces SUCCESS status and explainability', () => {
    const result = createExecutionResult({
      specialist: SPECIALISTS.SCOUT,
      transactionId: 'sec_1',
      status: EXECUTION_STATUSES.SUCCESS,
      confidence: { overall: 0.8, evidence: 0.85, fit: 0.75, completeness: 0.7 },
      evidence: [{
        label: 'Website mentions expansion',
        source: 'website',
        confidence: 0.7,
        timestamp: '2026-08-01T00:00:00.000Z',
        provenance: { kind: 'observed', source: 'website' },
      }],
      contributions: scoutContributions(),
      recommendations: [
        { tier: RECOMMENDATION_TIERS.REQUIRED, text: 'Approve top prospects for outreach.' },
        { tier: RECOMMENDATION_TIERS.OPTIONAL, text: 'Expand to Bedford.' },
      ],
      unknowns: [{
        unknown: 'Property manager not identified.',
        reason: 'No public ownership data.',
      }],
      nextActions: [{ kind: 'generate_outreach', label: 'Generate outreach' }],
    });

    assert.equal(result.status, EXECUTION_STATUSES.SUCCESS);
    assert.equal(result.confidence.overall, 0.8);
    assert.equal(result.evidence.length, 1);
    assert.ok(result.explainability.whyRecommended.length);
    assert.ok(result.explainability.remainsUnknown.length);
    assert.match(result.explainability.remainsUnknown[0], /Property manager/);
    assert.equal(result.audit.transactionId, 'sec_1');
  });

  it('blocked results never throw — they return BLOCKED with preconditions', () => {
    const result = blockedExecutionResult({
      specialist: SPECIALISTS.SCOUT,
      transactionId: 'sec_blocked',
      reason: 'Insufficient evidence for prioritization.',
      requiredPrecondition: 'minimum_buying_signals',
      recommendedAction: 'Expand geography or lower evidence threshold.',
    });

    assert.equal(result.status, EXECUTION_STATUSES.BLOCKED);
    assert.equal(result.blocked.reason, 'Insufficient evidence for prioritization.');
    assert.equal(result.blocked.requiredPrecondition, 'minimum_buying_signals');
    assert.ok(result.nextActions.length);
    assert.ok(result.explainability.whyNotRecommended.length);
  });

  it('validateExecutionResult rejects missing evidence on SUCCESS', () => {
    assert.throws(
      () => validateExecutionResult(createExecutionResult({
        specialist: SPECIALISTS.SCOUT,
        transactionId: 'sec_bad',
        status: EXECUTION_STATUSES.SUCCESS,
        confidence: 0.5,
        evidence: [],
        contributions: scoutContributions(),
      }), { specialist: SPECIALISTS.SCOUT }),
      (err) => err.code === 'sec_evidence_missing'
    );
  });

  it('validateExecutionResult rejects bare confidence in final result', () => {
    const result = createExecutionResult({
      specialist: SPECIALISTS.SCOUT,
      transactionId: 'sec_conf',
      status: EXECUTION_STATUSES.SUCCESS,
      confidence: 0.5,
      evidence: [{
        label: 'Signal',
        source: 'test',
        timestamp: '2026-08-01T00:00:00.000Z',
        provenance: { kind: 'test' },
      }],
      contributions: scoutContributions(),
    });
    assert.equal(typeof result.confidence, 'object');
    assert.equal(result.confidence.overall, 0.5);
    assert.doesNotThrow(() => validateExecutionResult(result, { specialist: SPECIALISTS.SCOUT }));
  });

  it('executeSpecialist wraps thrown errors into FAILED results', async () => {
    const engine = createAcquisitionMissionEngine();
    const mission = missionWithPlan(engine);
    const result = await executeSpecialist({
      specialist: SPECIALISTS.SCOUT,
      mission,
      run: async () => {
        throw new Error('network timeout');
      },
    });
    assert.equal(result.status, EXECUTION_STATUSES.FAILED);
    assert.ok(result.nextActions.some((a) => a.kind === 'retry'));
  });

  it('executeSpecialist can treat errors as BLOCKED', async () => {
    const engine = createAcquisitionMissionEngine();
    const mission = missionWithPlan(engine);
    const result = await executeSpecialist({
      specialist: SPECIALISTS.SCOUT,
      mission,
      treatErrorsAsBlocked: true,
      run: async () => {
        throw new Error('policy conflict');
      },
    });
    assert.equal(result.status, EXECUTION_STATUSES.BLOCKED);
    assert.match(result.blocked.reason, /policy conflict/);
  });

  it('fromScoutLegacyOutput maps discovery payloads to SEC envelope', () => {
    const legacy = {
      status: 'completed',
      confidence: 0.72,
      payload: {
        companies: [{ id: 'c1', name: 'Harbor Law' }],
        prospects: [{ id: 'p1', name: 'Alex' }],
        buyingSignals: ['Hiring'],
        evidence: [{ label: 'LinkedIn', source: 'linkedin' }],
        qualifiedCount: 2,
        confidenceBreakdown: {
          overall: 0.72,
          evidence: 0.8,
          fit: 0.7,
          completeness: 0.65,
        },
      },
    };
    const result = fromScoutLegacyOutput(legacy, { transactionId: 'sec_scout' });
    assert.equal(result.status, EXECUTION_STATUSES.SUCCESS);
    assert.equal(result.confidence.overall, 0.72);
    assert.equal(result.confidence.evidence, 0.8);
    assert.ok(result.contributions.companies.length);
    assert.equal(result.audit.transactionId, 'sec_scout');
  });

  it('assertExecutionResult integrates with TME validation errors', () => {
    assert.throws(
      () => assertExecutionResult({
        status: 'MAYBE',
        confidence: { overall: 0.5, evidence: 0.5, fit: 0.5, completeness: 0.5 },
        evidence: [],
        contributions: {},
        recommendations: [],
        unknowns: [],
        nextActions: [],
        audit: { transactionId: 'x' },
        explainability: {},
      }),
      (err) => err.tmeClass === 'validation'
    );
  });

  it('builds emmett and vera input contracts from mission plan', () => {
    const engine = createAcquisitionMissionEngine();
    const mission = missionWithPlan(engine);

    const emmett = buildExecutionInput({
      mission,
      specialist: SPECIALISTS.EMMETT,
      transactionId: 'sec_emmett',
    });
    assert.ok(emmett.specialistInput.deliverabilityPolicy);
    assert.ok(emmett.specialistInput.constraints);

    const vera = buildExecutionInput({
      mission,
      specialist: SPECIALISTS.VERA,
      companies: [{ name: 'Summit Law' }],
      transactionId: 'sec_vera',
    });
    assert.equal(vera.specialistInput.companies.length, 1);
    assert.ok(vera.specialistInput.reviewPolicy);
  });
});

describe('SPEC-132 — TME integration', () => {
  beforeEach(() => {
    amo.clearExecutionAudit();
  });

  it('discovery stage output validates through SEC before commit', async () => {
    const engine = createAcquisitionMissionEngine();
    const mission = missionWithPlan(engine);

    const result = await amo.executeMissionStage({
      engine,
      missionId: mission.id,
      tenantId: '10',
      specialist: SPECIALISTS.SCOUT,
      stage: amo.STAGES.DISCOVER,
      execute: async ({ transactionId }) => {
        const discoveryPayload = {
          companies: [{ id: 'c1', name: 'Harbor Law' }],
          prospects: [{ id: 'p1', name: 'Alex' }],
          buyingSignals: [{ label: 'Hiring', source: 'linkedin' }],
          evidence: [{
            label: 'LinkedIn post',
            source: 'linkedin',
            timestamp: '2026-08-01T00:00:00.000Z',
            provenance: { kind: 'observed', source: 'linkedin' },
          }],
          confidence: 0.8,
          qualifiedCount: 1,
        };
        const executionResult = executionResultFromStageOutput(
          { discoveryPayload },
          { specialist: SPECIALISTS.SCOUT, transactionId }
        );
        return { discoveryPayload, executionResult };
      },
      validateOutput: (output, ctx) => {
        amo.assertExecutionResult(output.executionResult, {
          specialist: SPECIALISTS.SCOUT,
          transactionId: ctx.transactionId,
        });
      },
      commit: ({ engine: amoEngine, mission: current, output, transactionId }) => {
        amo.bumpMissionVersion(current, transactionId);
        amoEngine.store.putMission(current);
        amoEngine.contribute(current.id, {
          specialist: SPECIALISTS.SCOUT,
          kind: amo.CONTRIBUTION_KINDS.DISCOVERY,
          payload: output.discoveryPayload,
        }, { tenantId: '10' });
        return { ok: true };
      },
    });

    assert.equal(result.committed, true);
    assert.equal(result.output.executionResult.status, EXECUTION_STATUSES.SUCCESS);
  });
});
