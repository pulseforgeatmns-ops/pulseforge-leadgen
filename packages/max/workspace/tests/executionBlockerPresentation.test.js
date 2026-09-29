'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  resolveExecutionBlocker,
  isGenericBlockerMessage,
  codeToBlockerMessage,
} = require('../ExecutionBlockerPresentation');
const { buildExecutionMissionResponse } = require('../AcquisitionMissionExecution');

describe('Execution blocker presentation', () => {
  it('detects generic blocker messages', () => {
    assert.equal(isGenericBlockerMessage('Execution blocked.'), true);
    assert.equal(isGenericBlockerMessage('Scout discovery blocked.'), true);
    assert.equal(
      isGenericBlockerMessage('Scout contribution did not attach canonical evidence.'),
      false
    );
  });

  it('maps tme_evidence_missing to actionable message', () => {
    assert.equal(
      codeToBlockerMessage('tme_evidence_missing'),
      'Scout contribution did not attach canonical evidence.'
    );
  });

  it('prefers specific rollback reason over generic fallback', () => {
    const blocker = resolveExecutionBlocker({
      error: { code: 'tme_evidence_missing', message: 'Execution blocked.' },
      rollbackReason: 'Execution blocked.',
      stageName: 'Discovery',
    });
    assert.equal(
      blocker.message,
      'Scout contribution did not attach canonical evidence.'
    );
    assert.match(blocker.waitingOn, /tme_evidence_missing|canonical evidence/i);
    assert.match(blocker.nextStep, /Resolve:/);
  });

  it('surfaces scout payload summary for committed blocked discovery', () => {
    const blocker = resolveExecutionBlocker({
      scoutPayload: {
        summary: 'Google Places returned zero qualifying offices for this segment.',
        blocked: { reason: 'Execution blocked.' },
      },
      stageName: 'Discovery',
    });
    assert.equal(
      blocker.message,
      'Google Places returned zero qualifying offices for this segment.'
    );
  });

  it('buildExecutionMissionResponse rolledBack preserves specific evidence status', () => {
    const { comm, prose } = buildExecutionMissionResponse({
      mission: {
        id: 'm1',
        title: 'STR — Greater Manchester',
        objective: 'Acquire STR client',
        stage: 'discover',
      },
      snapshot: { workspace: {} },
      action: 'discovery_approved',
      question: 'Approve discovery',
      executionResult: {
        rolledBack: true,
        rollbackReason: 'Execution blocked.',
        error: {
          code: 'tme_evidence_missing',
          message: 'Execution blocked.',
          tmeClass: 'validation',
        },
        transactionId: 'tx-1',
      },
    });

    assert.match(comm.evidenceStatus, /canonical evidence/i);
    assert.match(prose, /canonical evidence/i);
    assert.doesNotMatch(comm.evidenceStatus, /^Execution blocked\.$/);
    assert.match(comm.waitingOn, /canonical evidence|tme_evidence_missing/i);
  });

  it('buildExecutionMissionResponse discovery_approved blocked uses scout summary', () => {
    const { comm } = buildExecutionMissionResponse({
      mission: {
        id: 'm2',
        title: 'Commercial',
        objective: 'Acquire commercial client',
        stage: 'discover',
      },
      snapshot: { workspace: {} },
      action: 'discovery_approved',
      question: 'Approve discovery',
      executionResult: {
        executionOutcome: 'blocked',
        discovery: {
          payload: {
            summary: 'Coverage incomplete: no property-management operators found.',
            blocked: true,
            evidence: [],
          },
        },
      },
    });

    assert.match(comm.evidenceStatus, /Coverage incomplete/i);
    assert.match(comm.waitingOn, /Coverage incomplete/i);
    assert.doesNotMatch(comm.evidenceStatus, /^Mission state$/);
  });
});
