'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');
const {
  STR_OBJECTIVE,
  isStrOutboundObjective,
  isLawFirmObjective,
  pickCanonicalStrMission,
  chooseNextRecoveryIntent,
  classifyQueueItems,
  assertNotSendingIntent,
  FORBIDDEN_SEND_INTENTS,
} = require('../scripts/lib/anchorCanonicalOutbound');
const {
  parseArgs: parseInspectArgs,
  run: runInspect,
} = require('../scripts/inspectAnchorCanonicalOutbound');
const {
  parseArgs: parseRecoverArgs,
  run: runRecover,
} = require('../scripts/recoverAnchorCanonicalOutbound');

const RECOVER_SRC = fs.readFileSync(
  path.join(__dirname, '../scripts/recoverAnchorCanonicalOutbound.js'),
  'utf8'
);
const INSPECT_SRC = fs.readFileSync(
  path.join(__dirname, '../scripts/inspectAnchorCanonicalOutbound.js'),
  'utf8'
);
const LIB_SRC = fs.readFileSync(
  path.join(__dirname, '../scripts/lib/anchorCanonicalOutbound.js'),
  'utf8'
);

describe('Anchor STR canonical outbound recovery', () => {
  it('matches the operator STR objective and rejects the law-firm mission', () => {
    assert.equal(isStrOutboundObjective(STR_OBJECTIVE), true);
    assert.equal(
      isStrOutboundObjective(
        'Acquire one recurring commercial cleaning client from a short-term rental operator in Greater Manchester.'
      ),
      true
    );
    assert.equal(
      isStrOutboundObjective(
        'Acquire one recurring commercial cleaning client from a short-term rental operator in the Greater Manchester area.'
      ),
      true
    );
    assert.equal(
      isLawFirmObjective(
        'Acquire recurring commercial cleaning customers from law firms in Greater Manchester, NH.'
      ),
      true
    );
    assert.equal(
      isStrOutboundObjective(
        'Acquire recurring commercial cleaning customers from law firms in Greater Manchester, NH.'
      ),
      false
    );
  });

  it('picks the STR mission and does not silently prefer the law-firm or excluded READY mission', () => {
    const canonical = pickCanonicalStrMission([
      {
        id: 'mission_law',
        isStrObjective: false,
        isLawFirmObjective: true,
        stage: 'ready',
        scoutCandidateCount: 5,
        status: 'active',
      },
      {
        id: 'mission_30b36f10-20ce-4e41-8780-c3d8822e2c8e',
        isStrObjective: true,
        isLawFirmObjective: false,
        stage: 'ready',
        scoutCandidateCount: 8,
        status: 'active',
      },
      {
        id: 'mission_82e8102f-249c-4f44-b88e-2de76b13898e',
        isStrObjective: true,
        isLawFirmObjective: false,
        stage: 'understand',
        scoutCandidateCount: 0,
        status: 'Understanding',
      },
    ]);
    assert.equal(canonical.id, 'mission_82e8102f-249c-4f44-b88e-2de76b13898e');
    assert.equal(pickCanonicalStrMission([{ id: 'law', isStrObjective: false }]), null);
  });

  it('never selects APPROVE_EXECUTION or EXECUTE_OUTBOUND as a recovery intent', () => {
    const ready = chooseNextRecoveryIntent({
      stage: 'ready',
      pendingIntent: 'APPROVE_EXECUTION',
      sendableCount: 3,
      scoutCandidateCount: 12,
      contributions: { scout: {}, max: {}, paige: {}, emmett: {} },
    });
    assert.equal(ready.intent, null);
    assert.equal(ready.stop, true);
    assert.equal(ready.reason, 'ready_awaiting_execution_approval');
    assert.match(ready.operatorAction, /APPROVE_EXECUTION/);

    assert.throws(
      () => assertNotSendingIntent('APPROVE_EXECUTION'),
      (err) => err.code === 'send_intent_forbidden'
    );
    assert.throws(
      () => assertNotSendingIntent('EXECUTE_OUTBOUND'),
      (err) => err.code === 'send_intent_forbidden'
    );
    assert.deepEqual(FORBIDDEN_SEND_INTENTS, ['APPROVE_EXECUTION', 'EXECUTE_OUTBOUND']);
  });

  it('issues APPROVE_DISCOVERY only when discovery approval is absent', () => {
    const chosen = chooseNextRecoveryIntent({
      stage: 'discover',
      pendingIntent: 'APPROVE_DISCOVERY',
      scoutCandidateCount: 0,
      sendableCount: 0,
      discoveryApproved: false,
      contributions: { scout: null },
    });
    assert.equal(chosen.intent, 'APPROVE_DISCOVERY');
    assert.equal(chosen.reason, 'pending_operator_decision');
    assert.equal(chosen.stop, false);
  });

  it('approved discovery + empty candidate set executes Scout continuation, not another approval', () => {
    const summary = {
      stage: 'understand',
      pendingIntent: null,
      scoutCandidateCount: 0,
      sendableCount: 0,
      discoveryApproved: true,
      contributions: { scout: { candidateCount: 0 } },
    };
    const first = chooseNextRecoveryIntent(summary);
    assert.equal(first.intent, 'CONTINUE_INVESTIGATION');
    assert.equal(first.reason, 'approved_empty_discovery');
    assert.notEqual(first.intent, 'APPROVE_DISCOVERY');
    const second = chooseNextRecoveryIntent(summary);
    assert.equal(second.intent, 'CONTINUE_INVESTIGATION');
    assert.notEqual(second.intent, 'APPROVE_DISCOVERY');
  });

  it('does not re-issue APPROVE_DISCOVERY when pending approval is stale and Scout is empty', () => {
    const chosen = chooseNextRecoveryIntent({
      stage: 'understand',
      pendingIntent: 'APPROVE_DISCOVERY',
      scoutCandidateCount: 0,
      sendableCount: 0,
      discoveryApproved: true,
      contributions: { scout: { candidateCount: 0 } },
    });
    assert.equal(chosen.intent, 'CONTINUE_INVESTIGATION');
    assert.equal(chosen.reason, 'approved_empty_discovery');
  });

  it('refuses destructive Scout continuation when a healthy candidate set already exists', () => {
    const chosen = chooseNextRecoveryIntent({
      stage: 'discover',
      pendingIntent: 'CONTINUE_INVESTIGATION',
      scoutCandidateCount: 24,
      sendableCount: 0,
      discoveryApproved: true,
      contributions: { scout: { candidateCount: 24 } },
    });
    assert.notEqual(chosen.intent, 'CONTINUE_INVESTIGATION');
    assert.equal(chosen.reason, 'skip_destructive_continuation');
    assert.equal(chosen.intent, 'APPROVE_PRIORITIZATION');
    assert.equal(chosen.stop, false);
  });

  it('does not continue investigation when healthy candidates exist without a pending investigation', () => {
    const chosen = chooseNextRecoveryIntent({
      stage: 'understand',
      pendingIntent: null,
      scoutCandidateCount: 12,
      sendableCount: 0,
      discoveryApproved: true,
      contributions: { scout: { candidateCount: 12 } },
    });
    assert.notEqual(chosen.intent, 'CONTINUE_INVESTIGATION');
    assert.equal(chosen.intent, 'APPROVE_PRIORITIZATION');
  });

  it('dispatches GENERATE_CAPACITY when Paige is complete and Emmett is not', () => {
    const chosen = chooseNextRecoveryIntent({
      stage: 'prepare',
      pendingIntent: null,
      scoutCandidateCount: 8,
      sendableCount: 0,
      contributions: {
        scout: { candidateCount: 8 },
        max: { rankedCount: 8 },
        approach: { selected: 'outbound' },
        paige: { variantCount: 8 },
        emmett: null,
      },
    });
    assert.equal(chosen.intent, 'GENERATE_CAPACITY');
    assert.equal(chosen.stop, false);
  });

  it('classifies sendable vs missing-email queue items without inventing recipients', () => {
    const classified = classifyQueueItems({
      queue: {
        items: [
          { prospectId: 'a', email: 'a@example.com', sendable: true, paige: { subject: 'Hi', body: 'Hello' } },
          { prospectId: 'b', email: '', sendable: true, paige: { subject: 'Hi', body: 'Hello' } },
          { prospectId: 'c', dnc: true, email: 'c@example.com' },
        ],
      },
    });
    assert.equal(classified.queueCount, 3);
    assert.equal(classified.sendableCount, 1);
    assert.equal(classified.sendable[0].prospectId, 'a');
    assert.ok(classified.blocked.some((row) => row.reasons.includes('missing_recipient_email_on_queue_item')));
  });

  it('inspect and recover CLIs refuse without --confirm-production and never enable autosend', async () => {
    assert.equal(parseInspectArgs([]).confirmProduction, false);
    assert.equal(parseRecoverArgs(['--confirm-production']).confirmProduction, true);
    await assert.rejects(
      () => runInspect({ confirmProduction: false }),
      (err) => err.code === 'confirm_production_required'
    );
    await assert.rejects(
      () => runRecover({ confirmProduction: false }),
      (err) => err.code === 'confirm_production_required'
    );
    assert.doesNotMatch(RECOVER_SRC, /SET\s+autosend_enabled/i);
    assert.doesNotMatch(INSPECT_SRC, /SET\s+autosend_enabled/i);
    assert.doesNotMatch(RECOVER_SRC, /intent:\s*['"]EXECUTE_OUTBOUND['"]/);
    assert.doesNotMatch(RECOVER_SRC, /intent:\s*EXECUTION_INTENTS\.EXECUTE_OUTBOUND/);
    assert.match(RECOVER_SRC, /assertNotSendingIntent/);
    assert.match(RECOVER_SRC, /allowFixtureFallback:\s*false/);
    assert.match(LIB_SRC, /skip_destructive_continuation/);
    assert.match(LIB_SRC, /approved_empty_discovery/);
    assert.match(LIB_SRC, /CONTINUE_INVESTIGATION/);
    assert.doesNotMatch(LIB_SRC, /attachEmmettCapacity/);
    const cronSrc = fs.readFileSync(path.join(__dirname, '../routes/cron.js'), 'utf8');
    assert.match(cronSrc, /inspect-anchor-canonical-outbound/);
    assert.match(cronSrc, /recover-anchor-canonical-outbound/);
    assert.match(cronSrc, /inspectOnly: true/);
  });
});
