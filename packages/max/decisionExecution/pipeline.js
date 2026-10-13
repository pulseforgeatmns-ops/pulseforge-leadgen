'use strict';

const { DECISION_TRIGGER, EXECUTION_STATUS, SUBJECT_TYPE } = require('./types');
const { buildUnderstanding, isOverdue } = require('./understand');
const { baseCandidates } = require('./candidates');
const { selectAction, prioritizeDecisions } = require('./select');
const { authorizeExecution } = require('./authority');
const { executeActionIntent } = require('./execute');
const { verifyActionIntent } = require('./verify');
const { buildDecisionReceipt } = require('./receipt');
const { decisionIdempotencyKey, newDecisionId } = require('./fingerprints');
const { createTelemetryCounters, bump } = require('./telemetry');
const { markOverdueExpectations } = require('../stateIngestion/expectations');
const { evaluateAgentOutput } = require('./delegation');
const { expectationScope, matchesExpectation } = require('./expectationScope');

async function reverifyResolutionReplay({ decision, snapshot, stateStore, decisionStore }) {
  let intent = null;
  try {
    intent = await decisionStore.findActionIntentByIdempotency?.(decision.client_id, decision.idempotency_key);
  } catch {
    // An unavailable receipt cannot establish that a previous action succeeded.
  }
  const boundIntent = intent && intent.decision_id === decision.id
    && String(intent.client_id) === String(decision.client_id)
    && intent.idempotency_key === decision.idempotency_key
    && intent.action_type === 'RESOLVE_EXPECTATION';
  const originalScope = expectationScope(decision.client_id, decision.canonical_state_snapshot);
  const currentScope = expectationScope(decision.client_id, snapshot);
  if (boundIntent) {
    const previous = { ...intent };
    if (!originalScope || !currentScope || decision.canonical_state_snapshot?.expectation?.id !== decision.subject_id
      || !matchesExpectation(snapshot.expectation, decision.subject_id, originalScope)) {
      intent = { ...intent, execution_status: EXECUTION_STATUS.ACTION_VERIFICATION_FAILED,
        verification_status: 'VERIFICATION_FAILED', error_code: 'EXPECTATION_REPLAY_SCOPE_INVALID' };
    } else {
      // Readback only: never execute an action again to repair an uncertain receipt.
      intent = await verifyActionIntent({ decisionStore, stateStore, intent: { ...intent }, snapshot });
      if (intent.verification_status !== 'VERIFIED'
        && ![EXECUTION_STATUS.EXECUTION_FAILURE, EXECUTION_STATUS.ACTION_VERIFICATION_FAILED].includes(intent.execution_status)) {
        intent = { ...intent, execution_status: EXECUTION_STATUS.ACTION_VERIFICATION_FAILED,
          error_code: 'EXPECTATION_REPLAY_UNCONFIRMED' };
      }
    }
    if (intent.execution_status !== previous.execution_status
      || intent.verification_status !== previous.verification_status || intent.error_code !== previous.error_code) {
      await decisionStore.persistActionIntent(intent);
    }
  } else {
    intent = null;
  }
  const executionStatus = intent?.execution_status || EXECUTION_STATUS.ACTION_VERIFICATION_FAILED;
  const verificationStatus = intent?.verification_status || 'VERIFICATION_FAILED';
  if (decision.execution_status !== executionStatus || decision.verification_status !== verificationStatus) {
    decision = { ...decision, execution_status: executionStatus, verification_status: verificationStatus };
    await decisionStore.persistDecision(decision);
  }
  return { decision, intent };
}

function computePriority({ snapshot, trigger }) {
  const rel = snapshot.relationship || {};
  return {
    importance: rel.relationship_active ? 8 : 4,
    urgency: trigger?.type === DECISION_TRIGGER.EXPECTATION_OVERDUE ? 9 : 3,
    mission_relevance: rel.relationship_active ? 7 : 2,
    relationship_sensitivity: rel.relationship_active ? 9 : 1,
    expected_value: rel.relationship_active ? 8 : 3,
    dependency: 0,
  };
}

async function evaluateOperationalDecision({
  clientId,
  trigger,
  stateStore,
  decisionStore,
  attentionStore = null,
  expectation = null,
  prospect = null,
  now = new Date(),
  policy = {},
  telemetry: telemetryInput,
}) {
  const telemetry = telemetryInput || createTelemetryCounters();
  const { snapshot, supportingEvidence, prospect: resolvedProspect, expectation: resolvedExp, owner } =
    await buildUnderstanding({ stateStore, trigger, expectation, prospect });

  const subjectType = resolvedExp ? SUBJECT_TYPE.EXPECTATION : SUBJECT_TYPE.PROSPECT;
  const subjectId = resolvedExp?.id || resolvedProspect?.id || trigger?.payload?.subject_id || 'unknown';

  const candidates = baseCandidates({ snapshot, trigger, now });
  const selection = selectAction({ snapshot, candidates, trigger, now });
  const selected = selection.selected;
  const authority = authorizeExecution({ selected, snapshot, policy });

  const triggerState = {
    expectation_status: snapshot.expectation?.status,
    overdue: snapshot.expectation ? isOverdue(snapshot.expectation, now) : false,
    window_open: snapshot.expectation?.expected_window || {},
  };

  const idempotencyKey = decisionIdempotencyKey({
    clientId,
    triggerType: trigger.type,
    subjectType,
    subjectId,
    actionType: selected?.action_type,
    triggerState,
  });

  const existing = await decisionStore.findDecisionByIdempotency(clientId, idempotencyKey);
  if (existing) {
    bump(telemetry, 'duplicate_evaluations_resolved');
    let replay = { decision: existing, intent: null };
    if (existing.selected_action?.action_type === 'RESOLVE_EXPECTATION'
      && existing.execution_status !== EXECUTION_STATUS.SUPERSEDED) {
      replay = await reverifyResolutionReplay({ decision: existing, snapshot, stateStore, decisionStore });
      if (attentionStore && replay.decision.verification_status !== 'VERIFIED') {
        const { syncAttentionFromDecision } = require('../attention/syncFromDecision');
        await syncAttentionFromDecision({ attentionStore, decision: replay.decision, snapshot, now });
      }
    }
    return {
      duplicate: true,
      ...replay,
      receipt: buildDecisionReceipt({ decision: replay.decision, intent: replay.intent, snapshot }),
      telemetry,
      snapshot,
    };
  }

  const activeDup = await decisionStore.findActiveDecision({
    clientId,
    subjectType,
    subjectId,
    actionType: selected?.action_type,
  });
  if (activeDup && ['ASK_AO_STATUS', 'CREATE_AO_TASK'].includes(selected?.action_type)) {
    bump(telemetry, 'duplicate_evaluations_resolved');
    return {
      duplicate: true,
      decision: activeDup,
      execution_status: EXECUTION_STATUS.ACTION_ALREADY_ACTIVE,
      receipt: buildDecisionReceipt({ decision: activeDup, intent: null, snapshot }),
      telemetry,
    };
  }

  let execution_status = selection.outcome;
  if (!authority.authorized) {
    execution_status = authority.execution_status;
    bump(telemetry, 'actions_blocked');
  }

  const decision = {
    id: newDecisionId(),
    client_id: clientId,
    trigger,
    triggering_evidence: trigger.payload?.evidence || [],
    canonical_state_snapshot: snapshot,
    subject_type: subjectType,
    subject_id: subjectId,
    decision_type: trigger.type,
    candidate_actions: candidates,
    selected_action: selected,
    not_selected: selection.notSelected,
    rationale: selection.rationale,
    supporting_evidence: supportingEvidence,
    assumptions: selection.assumptions || [],
    confidence: selection.confidence,
    priority: computePriority({ snapshot, trigger }),
    owner: owner?.name || snapshot.account?.owner_name || null,
    authority_class: authority.authority_class,
    execution_status,
    verification_status: null,
    idempotency_key: idempotencyKey,
    reevaluate_after: selection.reevaluate_after || null,
    created_at: now.toISOString(),
    resolved_at: null,
  };

  bump(telemetry, 'decisions_created');
  if (selected?.action_type === 'NO_ACTION') bump(telemetry, 'no_action_decisions');
  else bump(telemetry, 'actions_selected');

  await decisionStore.persistDecision(decision);

  let intent = null;
  if (authority.authorized && execution_status === EXECUTION_STATUS.AUTHORIZED) {
    const execResult = await executeActionIntent({
      decisionStore,
      stateStore,
      decision,
      selected,
      snapshot,
    });
    intent = execResult.intent;
    decision.execution_status = intent.execution_status;
    if (intent.execution_status === EXECUTION_STATUS.EXECUTED) bump(telemetry, 'actions_executed');
    if (selected?.action_type === 'DELEGATE_AGENT') bump(telemetry, 'agent_delegations');
    if (selected?.action_type === 'ESCALATE_OPERATOR') bump(telemetry, 'human_escalations');
    if (selected?.action_type === 'ABSTAIN_INSUFFICIENT_EVIDENCE') {
      bump(telemetry, 'insufficient_evidence_abstentions');
    }

    intent = await verifyActionIntent({
      decisionStore,
      stateStore,
      intent,
      snapshot,
      options: {
        simulateVerificationFailure: decisionStore.simulateVerificationFailure,
      },
    });
    await decisionStore.persistActionIntent(intent);
    decision.execution_status = intent.execution_status;
    decision.verification_status = intent.verification_status;
    if (selection.alsoSupersede && intent.execution_status === EXECUTION_STATUS.EXECUTED
      && intent.verification_status === 'VERIFIED') {
      await decisionStore.supersedeActiveDecisions({
        clientId,
        subjectType,
        subjectId,
        supersededBy: decision.id,
      });
      bump(telemetry, 'decisions_superseded');
    }
    if (intent.verification_status === 'VERIFIED') bump(telemetry, 'actions_verified');
    if (intent.execution_status === EXECUTION_STATUS.ACTION_VERIFICATION_FAILED) {
      bump(telemetry, 'verification_failures');
    }
  } else if (execution_status === EXECUTION_STATUS.INSUFFICIENT_EVIDENCE) {
    bump(telemetry, 'insufficient_evidence_abstentions');
  }

  await decisionStore.persistDecision(decision);
  const receipt = buildDecisionReceipt({ decision, intent, snapshot });

  if (attentionStore) {
    const { syncAttentionFromDecision } = require('../attention/syncFromDecision');
    await syncAttentionFromDecision({
      attentionStore,
      decision,
      snapshot,
      now,
    });
  }

  return { decision, intent, receipt, telemetry, snapshot };
}

async function scanExpectationTriggers({ clientId, stateStore, decisionStore, now = new Date() }) {
  const open = await stateStore.listOpenExpectations({ clientId });
  const marked = markOverdueExpectations(open, now);
  for (const exp of marked) {
    const local = stateStore.expectations?.find(e => e.id === exp.id);
    if (local && exp.status === 'OVERDUE') local.status = 'OVERDUE';
  }
  const results = [];
  for (const exp of open) {
    const overdue = isOverdue(exp, now) || exp.status === 'OVERDUE';
    const trigger = {
      type: overdue ? DECISION_TRIGGER.EXPECTATION_OVERDUE : DECISION_TRIGGER.SCHEDULED_REVIEW,
      payload: { expectation_id: exp.id, prospect_id: exp.prospect_id },
    };
    if (!overdue) {
      trigger.type = DECISION_TRIGGER.EXPECTATION_OPEN;
    }
    results.push(await evaluateOperationalDecision({
      clientId,
      trigger,
      stateStore,
      decisionStore,
      expectation: exp,
      now,
    }));
  }
  return prioritizeDecisions(results.map(r => r.decision)).map(d =>
    results.find(r => r.decision.id === d.id)
  );
}

async function reevaluateOnIngestion({
  clientId,
  stateStore,
  decisionStore,
  attentionStore = null,
  ingestionResult,
  now = new Date(),
}) {
  if (!ingestionResult) return null;
  if (ingestionResult.commit_blocked || ingestionResult.clarification_required) {
    return {
      skipped: true,
      reason: 'understanding_blocked',
      clarification_required: ingestionResult.clarification_required || null,
    };
  }
  if (attentionStore && ingestionResult) {
    const { wakeAttentionForIngestion } = require('../attention/wake');
    await wakeAttentionForIngestion({
      attentionStore,
      clientId,
      ingestionResult,
      stateStore,
      now,
    });
  }
  let prospect = ingestionResult.prospect || null;
  const openExp = (stateStore.expectations || []).find(e =>
    ['OPEN', 'WAITING', 'OVERDUE'].includes(e.status)
  );
  if (!prospect && openExp) {
    prospect = await stateStore.readProspect(openExp.prospect_id);
  }
  if (!prospect) return null;
  const trigger = {
    type: DECISION_TRIGGER.NEW_EVIDENCE,
    payload: {
      ingestion_id: ingestionResult.ingestion_id,
      prospect_id: prospect.id,
      evidence: ingestionResult.telemetry,
    },
  };
  const scopedExp = (stateStore.expectations || []).find(e =>
    e.prospect_id === prospect.id && ['OPEN', 'WAITING', 'OVERDUE'].includes(e.status)
  );
  const telemetry = createTelemetryCounters();
  if (ingestionResult.operatorCorrection) {
    bump(telemetry, 'human_corrections');
  }
  const result = await evaluateOperationalDecision({
    clientId,
    trigger,
    stateStore,
    decisionStore,
    attentionStore,
    prospect,
    expectation: scopedExp || openExp,
    now,
    telemetry,
  });
  return result;
}

function rejectAgentDelegationOutput(delegation, output) {
  const evaluation = evaluateAgentOutput({ delegation, output });
  if (!evaluation.accepted) {
    return { ...evaluation, rejected: true };
  }
  return evaluation;
}

module.exports = {
  evaluateOperationalDecision,
  scanExpectationTriggers,
  reevaluateOnIngestion,
  rejectAgentDelegationOutput,
  computePriority,
};
