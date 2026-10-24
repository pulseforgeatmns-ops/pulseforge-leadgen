'use strict';

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const {
  createBoundedDelegation,
  executeBoundedDelegation,
  consumeSpecialistResult,
  explainSpecialistTrail,
} = require('../SpecialistDelegationContext');
const {
  createMemoryStore,
  createSpecialistDelegationService,
  CONTRACT_OBJECTIVE,
} = require('../../../../services/specialistDelegation');

describe('SPEC-098 Max specialist delegation workspace interfaces', () => {
  let store;
  let service;

  beforeEach(() => {
    store = createMemoryStore();
    service = createSpecialistDelegationService({ store });
  });

  function sessionFor(tenantId) {
    return {
      id: 'sess-1',
      context: { tenantId, page: 'command-deck' },
    };
  }

  it('lets Max create, execute, evaluate, and explain through workspace interfaces', async () => {
    const session = sessionFor('1');

    const delegation = await createBoundedDelegation({
      session,
      context: { tenantId: '1' },
      delegationService: service,
      delegationOpts: { store },
      specialist: 'test_intelligence',
      capability: 'acquisition_assessment',
      objective: CONTRACT_OBJECTIVE,
      reason: 'Operator asked whether Acquisition has meaningful opportunity.',
      authority: 'observe',
      expectedReturn: {
        type: 'acquisition_intelligence',
        requireEvidence: true,
        requireConfidence: true,
        requireRecommendation: true,
      },
    });

    const result = await executeBoundedDelegation({
      session,
      context: { tenantId: '1' },
      delegationService: service,
      delegationOpts: { store },
      delegationId: delegation.id,
    });
    assert.equal(result.status, 'completed');

    const evaluation = await consumeSpecialistResult({
      session,
      context: { tenantId: '1' },
      delegationService: service,
      delegationOpts: { store },
      resultId: result.id,
    });
    assert.equal(evaluation.acceptedAsGroundTruth, false);
    assert.equal(evaluation.priorityApplied, false);
    assert.equal(session.context.lastSpecialistEvaluation.id, evaluation.id);

    const trail = await explainSpecialistTrail({
      session,
      context: { tenantId: '1' },
      delegationService: service,
      delegationOpts: { store },
      evaluationId: evaluation.id,
    });
    assert.match(trail.narrative, /Acquisition/i);
    assert.ok(trail.chain.evidence.length);
  });

  it('refuses to create a delegation when workspace tenant is missing', async () => {
    await assert.rejects(
      () =>
        createBoundedDelegation({
          delegationService: service,
          specialist: 'test_intelligence',
          capability: 'acquisition_assessment',
          objective: CONTRACT_OBJECTIVE,
          reason: 'No tenant.',
          authority: 'observe',
        }),
      (err) => err.code === 'tenant_required'
    );
  });
});

describe('SPEC-098 wiring markers', () => {
  it('WorkspaceEngine and ContextEnvelope include SPEC-098 delegation seams', () => {
    const engineSrc = fs.readFileSync(
      path.join(__dirname, '..', 'WorkspaceEngine.js'),
      'utf8'
    );
    const envelopeSrc = fs.readFileSync(
      path.join(__dirname, '..', 'ContextEnvelope.js'),
      'utf8'
    );
    const indexSrc = fs.readFileSync(path.join(__dirname, '..', 'index.js'), 'utf8');
    assert.match(engineSrc, /specialistDelegationService/);
    assert.match(envelopeSrc, /lastSpecialistEvaluation/);
    assert.match(indexSrc, /createBoundedDelegation/);
  });
});
