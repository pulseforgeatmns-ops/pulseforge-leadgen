'use strict';

const { ACTION_TYPE, DECISION_TRIGGER } = require('./types');
const { isOverdue, expectationStillOpen } = require('./understand');

function baseCandidates({ snapshot, trigger, now }) {
  const rel = snapshot.relationship || {};
  const candidates = [];

  candidates.push({
    action_type: ACTION_TYPE.NO_ACTION,
    label: 'Deliberately do nothing',
    authority_class: 'A',
    rationale_hint: 'No operational change required',
  });

  if (snapshot.expectation && isOverdue(snapshot.expectation, now)) {
    candidates.push({
      action_type: ACTION_TYPE.ASK_AO_STATUS,
      label: 'Ask AO whether expected event occurred',
      authority_class: 'A',
      rationale_hint: 'Expected window expired without resolving activity',
    });
    candidates.push({
      action_type: ACTION_TYPE.CREATE_AO_TASK,
      label: 'Create AO follow-up task',
      authority_class: 'A',
      rationale_hint: 'Close the loop on overdue expectation',
    });
    candidates.push({
      action_type: ACTION_TYPE.ESCALATE_OPERATOR,
      label: 'Escalate to operator (Jake)',
      authority_class: 'C',
      rationale_hint: 'Material ambiguity or policy exception',
    });
  }

  if (snapshot.expectation && expectationStillOpen(snapshot.expectation, now)) {
    candidates.push({
      action_type: ACTION_TYPE.NO_ACTION,
      label: 'Wait — expectation window still open',
      authority_class: 'A',
      rationale_hint: 'Expected event window remains open',
      reevaluate_after: snapshot.expectation.expected_window?.ends_at
        || snapshot.expectation.expected_window?.end,
    });
  }

  if (!rel.suppress_cold_outreach && !rel.relationship_active) {
    candidates.push({
      action_type: ACTION_TYPE.RETURN_TO_COLD_OUTREACH,
      label: 'Return account to automated cold outreach',
      authority_class: 'B',
      rationale_hint: 'No active relationship — outbound may be appropriate',
    });
  } else {
    candidates.push({
      action_type: ACTION_TYPE.RETURN_TO_COLD_OUTREACH,
      label: 'Return account to automated cold outreach',
      authority_class: 'D',
      rationale_hint: 'Existing relationship — cold outreach prohibited',
      prohibited: true,
    });
  }

  candidates.push({
    action_type: ACTION_TYPE.DELEGATE_AGENT,
    label: 'Delegate research to Scout',
    authority_class: 'A',
    agent: 'scout',
    rationale_hint: 'Need more canonical evidence before acting',
  });

  candidates.push({
    action_type: ACTION_TYPE.ABSTAIN_INSUFFICIENT_EVIDENCE,
    label: 'Abstain — insufficient evidence',
    authority_class: 'A',
    rationale_hint: 'Required evidence missing',
  });

  if (trigger?.type === DECISION_TRIGGER.NEW_EVIDENCE) {
    candidates.push({
      action_type: ACTION_TYPE.RESOLVE_EXPECTATION,
      label: 'Mark expectation resolved from new evidence',
      authority_class: 'A',
    });
    candidates.push({
      action_type: ACTION_TYPE.SUPERSEDE_PRIOR,
      label: 'Supersede stale follow-up decision',
      authority_class: 'A',
    });
  }

  return candidates;
}

module.exports = {
  baseCandidates,
};
