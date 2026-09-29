'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');
const {
  EXECUTION_INTENTS,
  OPERATOR_DECISION_KINDS,
} = require('../packages/acquisition-mission');
const { evaluatePrioritizationReadiness } = require('../packages/acquisition-mission/DecisionReadiness');
const { normalizeScoutDiscoveryPayload } = require('../packages/acquisition-mission/DiscoveryPayload');
const {
  STR_OBJECTIVE,
  isStrOutboundObjective,
  isLawFirmObjective,
  pickCanonicalStrMission,
  chooseNextRecoveryIntent,
  canIssuePrioritizationApproval,
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

  it('does not call APPROVE_PRIORITIZATION when investigation is pending and readiness is insufficient', () => {
    const incompletePayload = {
      summary: '15 qualified STR operators found with partial city coverage.',
      discoveryStatus: 'incomplete',
      qualifiedCount: 15,
      rankedProspects: Array.from({ length: 15 }, (_, index) => ({
        rank: index + 1,
        name: `Summit STR ${index + 1}`,
        readinessState: 'unknown',
      })),
      buyingSignals: [{
        label: 'Hiring cleaning operations coordinator',
        type: 'hiring',
      }],
      evidence: [{
        label: 'Google Places search result',
        source: 'google_places',
      }],
      coverage: {
        complete: false,
        cities: { searched: 1, planned: 6 },
        warnings: ['Only 1 / 6 cities searched.', 'Discovery coverage is incomplete.'],
      },
    };
    const readiness = evaluatePrioritizationReadiness(incompletePayload);
    assert.equal(readiness.sufficient, false);

    const chosen = chooseNextRecoveryIntent({
      stage: 'discover',
      pendingIntent: EXECUTION_INTENTS.CONTINUE_INVESTIGATION,
      pendingOperatorDecision: {
        kind: OPERATOR_DECISION_KINDS.DISCOVERY_INVESTIGATION,
        prompt: 'Continue investigation?',
        reason: 'Discovery coverage is incomplete.',
      },
      scoutCandidateCount: 15,
      prioritizationReady: false,
      discoveryReadiness: {
        sufficient: readiness.sufficient,
        primaryBlocker: readiness.primaryBlocker,
      },
      sendableCount: 0,
      discoveryApproved: true,
      waitingReason: 'Discovery coverage is incomplete.',
      contributions: { scout: { candidateCount: 15, payload: incompletePayload } },
    });

    assert.notEqual(chosen.intent, EXECUTION_INTENTS.CONTINUE_INVESTIGATION);
    assert.notEqual(chosen.intent, EXECUTION_INTENTS.APPROVE_PRIORITIZATION);
    assert.equal(chosen.stop, true);
    assert.equal(chosen.reason, 'discovery_investigation_required');
    assert.match(String(chosen.operatorAction), /coverage is incomplete|Discovery coverage is incomplete/i);
  });

  it('invokes APPROVE_PRIORITIZATION only after prioritization approval is legitimately pending', () => {
    const strongPayload = normalizeScoutDiscoveryPayload({
      status: 'completed',
      summary: '1 prospect matches mission objective.',
      discoveryStatus: 'complete',
      payload: {
        opportunities: [{
          companyId: 'co-1',
          name: 'Summit STR Management',
          fit: 0.84,
          timing: 0.72,
          confidence: 0.81,
          signals: [{
            type: 'hiring',
            label: 'Hiring cleaning operations coordinator',
            source: 'job_board',
          }],
          evidenceRefs: [{
            label: 'Job posting: cleaning operations coordinator',
            snapshot: { source: 'job_board', companyName: 'Summit STR Management' },
          }],
        }],
        qualifiedCount: 1,
      },
    }, { missionObjective: STR_OBJECTIVE });
    const readiness = evaluatePrioritizationReadiness(strongPayload);
    assert.equal(readiness.sufficient, true);

    const chosen = chooseNextRecoveryIntent({
      stage: 'discover',
      pendingIntent: EXECUTION_INTENTS.APPROVE_PRIORITIZATION,
      pendingOperatorDecision: {
        kind: OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL,
        prompt: 'Approve prioritization?',
      },
      scoutCandidateCount: 1,
      prioritizationReady: true,
      discoveryReadiness: {
        sufficient: readiness.sufficient,
        primaryBlocker: readiness.primaryBlocker,
      },
      sendableCount: 0,
      discoveryApproved: true,
      contributions: { scout: { candidateCount: 1, payload: strongPayload } },
    });

    assert.equal(chosen.intent, EXECUTION_INTENTS.APPROVE_PRIORITIZATION);
    assert.equal(chosen.reason, 'pending_operator_decision');
    assert.equal(chosen.stop, false);
    assert.equal(canIssuePrioritizationApproval({
      scoutCandidateCount: 1,
      prioritizationReady: true,
      pendingIntent: EXECUTION_INTENTS.APPROVE_PRIORITIZATION,
      pendingOperatorDecision: { kind: OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL },
      contributions: {},
    }), true);
  });

  it('does not continue investigation or force Max when healthy candidates exist without prioritization pending', () => {
    const chosen = chooseNextRecoveryIntent({
      stage: 'discover',
      pendingIntent: EXECUTION_INTENTS.CONTINUE_INVESTIGATION,
      scoutCandidateCount: 15,
      prioritizationReady: false,
      discoveryReadiness: {
        sufficient: false,
        primaryBlocker: {
          code: 'coverage_incomplete',
          reason: 'Discovery coverage is incomplete.',
        },
      },
      sendableCount: 0,
      discoveryApproved: true,
      contributions: { scout: { candidateCount: 15 } },
    });
    assert.notEqual(chosen.intent, EXECUTION_INTENTS.CONTINUE_INVESTIGATION);
    assert.notEqual(chosen.intent, EXECUTION_INTENTS.APPROVE_PRIORITIZATION);
    assert.equal(chosen.stop, true);
  });

  it('does not enter an APPROVE_PRIORITIZATION intent loop after a failed prioritization attempt', () => {
    const summary = {
      stage: 'discover',
      pendingIntent: EXECUTION_INTENTS.CONTINUE_INVESTIGATION,
      scoutCandidateCount: 15,
      prioritizationReady: false,
      discoveryReadiness: {
        sufficient: false,
        primaryBlocker: { code: 'coverage_incomplete', reason: 'Discovery coverage is incomplete.' },
      },
      sendableCount: 0,
      discoveryApproved: true,
      contributions: { scout: { candidateCount: 15 } },
    };
    const first = chooseNextRecoveryIntent(summary);
    const second = chooseNextRecoveryIntent(summary);
    assert.equal(first.intent, null);
    assert.equal(second.intent, null);
    assert.notEqual(first.intent, EXECUTION_INTENTS.APPROVE_PRIORITIZATION);
    assert.notEqual(second.intent, EXECUTION_INTENTS.APPROVE_PRIORITIZATION);
  });

  it('preserves healthy candidate counts in the recovery summary contract', () => {
    const summary = {
      stage: 'discover',
      pendingIntent: EXECUTION_INTENTS.CONTINUE_INVESTIGATION,
      scoutCandidateCount: 15,
      prioritizationReady: false,
      discoveryReadiness: {
        sufficient: false,
        primaryBlocker: { reason: 'Discovery coverage is incomplete.' },
      },
      contributions: { scout: { candidateCount: 15 } },
    };
    assert.equal(summary.scoutCandidateCount, 15);
    assert.equal(summary.contributions.scout.candidateCount, 15);
    const chosen = chooseNextRecoveryIntent(summary);
    assert.equal(chosen.stop, true);
    assert.notEqual(chosen.intent, EXECUTION_INTENTS.CONTINUE_INVESTIGATION);
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

  it('does not request execution approval when sendableCount is 0', () => {
    const chosen = chooseNextRecoveryIntent({
      stage: 'ready',
      pendingIntent: 'APPROVE_EXECUTION',
      sendableCount: 0,
      capacityItemCount: 5,
      scoutCandidateCount: 15,
      contributions: { scout: {}, max: {}, paige: {}, emmett: {} },
    });
    assert.equal(chosen.reason, 'capacity_queue_blocked');
    assert.notEqual(chosen.intent, 'APPROVE_EXECUTION');
    assert.notEqual(chosen.intent, 'EXECUTE_OUTBOUND');
    assert.match(chosen.operatorAction, /Resolve blocked recipient\/copy requirements/);
  });

  it('classifies sanitized CAPACITY items as having Paige copy via variant join', () => {
    const classified = classifyQueueItems(
      {
        queue: {
          items: [{
            prospectId: 'ChIJ43Z_V2dP4okRCRcDHefV8OU',
            candidateId: 'ChIJ43Z_V2dP4okRCRcDHefV8OU',
            email: '',
            sendable: true,
            paige: {
              candidateId: 'ChIJ43Z_V2dP4okRCRcDHefV8OU',
              variantLabel: 'Primary - Blue Door Living Property Management',
            },
          }],
        },
      },
      {
        variants: [{
          candidateId: 'ChIJ43Z_V2dP4okRCRcDHefV8OU',
          subject: 'Walkthrough for Blue Door',
          body: 'Prepared copy',
        }],
      }
    );
    assert.equal(classified.sendableCount, 0);
    assert.equal(classified.blocked[0].reasons.includes('missing_recipient_email_on_queue_item'), true);
    assert.equal(classified.blocked[0].reasons.includes('missing_paige_copy'), false);
    assert.equal(classified.blocked[0].candidateId, 'ChIJ43Z_V2dP4okRCRcDHefV8OU');
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
    assert.match(LIB_SRC, /discovery_investigation_required/);
    assert.match(LIB_SRC, /prioritizationApprovalPending/);
    assert.doesNotMatch(LIB_SRC, /skip_destructive_continuation/);
    assert.match(LIB_SRC, /approved_empty_discovery/);
    assert.match(LIB_SRC, /CONTINUE_INVESTIGATION/);
    assert.doesNotMatch(LIB_SRC, /attachEmmettCapacity/);
    assert.match(LIB_SRC, /capacity_queue_blocked/);
    assert.doesNotMatch(LIB_SRC, /execution_approval_without_sendable_queue/);
    const workflowSrc = fs.readFileSync(
      path.join(__dirname, '../.github/workflows/anchor-canonical-outbound.yml'),
      'utf8'
    );
    assert.doesNotMatch(workflowSrc, /environment:\s*charming-trust/);
    assert.match(workflowSrc, /github.event_name == 'workflow_dispatch'/);
    const cronSrc = fs.readFileSync(path.join(__dirname, '../routes/cron.js'), 'utf8');
    assert.match(cronSrc, /inspect-anchor-canonical-outbound/);
    assert.match(cronSrc, /recover-anchor-canonical-outbound/);
    assert.match(cronSrc, /inspectOnly: true/);
  });
});
