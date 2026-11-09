'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  validateTurnFrame,
  unavailableFrame,
  TURN_FRAME_OUTPUT_SCHEMA,
} = require('../TurnFrame');

const CTX = {
  tenantId: '44',
  candidates: [
    { id: 'm_a', kind: 'mission', label: 'A' },
    { id: 'm_b', kind: 'mission', label: 'B' },
    { id: 'pd_1', kind: 'pending_decision', label: 'Execution approval' },
  ],
  pendingDecisionId: 'pd_1',
  producer: 'model',
};

function raw(over = {}) {
  return {
    intent: 'question',
    subjectKind: 'mission',
    relationToPrior: 'none',
    referents: [{ candidateId: 'm_a', role: 'primary' }],
    evidenceNeeds: ['mission_lifecycle'],
    facets: ['cause'],
    requestedOutcome: 'explanation',
    requestedAction: { verb: 'none', targetCandidateId: null, polarity: 'none' },
    scope: 'focused',
    foreignTenantReference: false,
    ambiguities: [],
    confidence: 0.9,
    ...over,
  };
}

const approve = (over = {}) => raw({
  intent: 'authorization',
  subjectKind: 'pending_decision',
  referents: [],
  requestedOutcome: 'action',
  requestedAction: { verb: 'approve', targetCandidateId: 'pd_1', polarity: 'affirmative' },
  ...over,
});

describe('TurnFrame contract', () => {
  it('accepts a valid question frame and never grants authority', () => {
    const f = validateTurnFrame(raw(), CTX);
    assert.equal(f.intent, 'question');
    assert.equal(f.tenantId, '44');
    assert.equal(f.executionCandidate, false);
    assert.deepEqual(f.authority, { grantsAuthority: false });
    assert.ok(Object.isFrozen(f) && Object.isFrozen(f.authority));
  });

  it('the output schema forbids extra fields at every level', () => {
    assert.equal(TURN_FRAME_OUTPUT_SCHEMA.additionalProperties, false);
    assert.equal(TURN_FRAME_OUTPUT_SCHEMA.properties.requestedAction.additionalProperties, false);
    assert.equal(TURN_FRAME_OUTPUT_SCHEMA.properties.referents.items.additionalProperties, false);
  });

  it('rejects unknown fields, including a producer-chosen tenant', () => {
    assert.throws(() => validateTurnFrame({ ...raw(), tenantId: '10' }, CTX), /frame\.unexpected/);
    assert.throws(() => validateTurnFrame({ ...raw(), grantsAuthority: true }, CTX), /frame\.unexpected/);
  });

  it('rejects out-of-enum values without echoing them', () => {
    assert.throws(() => validateTurnFrame(raw({ intent: 'execute_now' }), CTX), (e) => e.code === 'invalid_turn_frame' && !/execute_now/.test(e.message));
    assert.throws(() => validateTurnFrame(raw({ confidence: 1.5 }), CTX), /confidence/);
  });

  it('requires the session tenant in context', () => {
    assert.throws(() => validateTurnFrame(raw(), { ...CTX, tenantId: null }), /context\.tenantId/);
  });

  it('drops referents that are not supplied candidates and forces clarification', () => {
    const f = validateTurnFrame(raw({ referents: [{ candidateId: 'm_other_tenant', role: 'primary' }] }), CTX);
    assert.deepEqual(f.referents, []);
    assert.equal(f.clarificationRequired, true);
    assert.ok(f.repairs.includes('referent_not_in_candidates'));
    assert.ok(f.confidence <= 0.5);
  });

  it('a bound, affirmative, confident approval is only an execution CANDIDATE', () => {
    const f = validateTurnFrame(approve(), CTX);
    assert.equal(f.executionCandidate, true);
    assert.equal(f.authority.grantsAuthority, false);
  });

  it('authorization without a bound pending decision becomes ambiguous', () => {
    const f = validateTurnFrame(approve(), { ...CTX, pendingDecisionId: null });
    assert.equal(f.intent, 'ambiguous');
    assert.equal(f.executionCandidate, false);
    assert.equal(f.clarificationRequired, true);
  });

  it('authorization targeting a different decision than the bound one becomes ambiguous', () => {
    const ctx = { ...CTX, candidates: [...CTX.candidates, { id: 'pd_2', kind: 'pending_decision' }] };
    const f = validateTurnFrame(approve({ requestedAction: { verb: 'approve', targetCandidateId: 'pd_2', polarity: 'affirmative' } }), ctx);
    assert.equal(f.intent, 'ambiguous');
    assert.equal(f.executionCandidate, false);
  });

  it('a negated action is a prohibition and never an execution candidate', () => {
    const f = validateTurnFrame(approve({ requestedAction: { verb: 'approve', targetCandidateId: 'pd_1', polarity: 'negated' } }), CTX);
    assert.equal(f.intent, 'prohibition');
    assert.equal(f.executionCandidate, false);
  });

  it('a prohibition carrying an affirmative action is contradictory: ambiguous', () => {
    const f = validateTurnFrame(approve({ intent: 'prohibition' }), CTX);
    assert.equal(f.intent, 'ambiguous');
    assert.equal(f.executionCandidate, false);
  });

  it('hypothetical actions never become execution candidates', () => {
    const f = validateTurnFrame(approve({ intent: 'hypothetical', requestedAction: { verb: 'approve', targetCandidateId: 'pd_1', polarity: 'hypothetical' } }), CTX);
    assert.equal(f.executionCandidate, false);
  });

  it('foreign-tenant references and low confidence block execution candidacy', () => {
    assert.equal(validateTurnFrame(approve({ foreignTenantReference: true }), CTX).executionCandidate, false);
    assert.equal(validateTurnFrame(approve({ confidence: 0.6 }), CTX).executionCandidate, false);
  });

  it('a degraded (lexical fallback) frame never nominates execution', () => {
    const f = validateTurnFrame(approve(), { ...CTX, producer: 'model_degraded_to_lexical' });
    assert.equal(f.executionCandidate, false);
  });

  it('an action target outside the candidates is dropped and becomes an action ambiguity', () => {
    const f = validateTurnFrame(raw({ intent: 'instruction', requestedAction: { verb: 'cancel', targetCandidateId: 'm_ghost', polarity: 'affirmative' } }), CTX);
    assert.equal(f.requestedAction.targetCandidateId, null);
    assert.equal(f.executionCandidate, false);
    assert.ok(f.ambiguities.some((a) => a.kind === 'action'));
  });

  it('unavailableFrame is ambiguous and inert', () => {
    const f = unavailableFrame({ tenantId: '1' });
    assert.equal(f.intent, 'ambiguous');
    assert.equal(f.executionCandidate, false);
    assert.equal(f.clarificationRequired, true);
  });
});
