'use strict';

const { ACTION_TYPE, EXECUTION_STATUS, EPISTEMIC, DECISION_TRIGGER } = require('./types');
const { expectationStillOpen, isOverdue } = require('./understand');

function selectAction({ snapshot, candidates, trigger, now = new Date() }) {
  const notSelected = [];
  const assumptions = [];

  const conflicting = Object.values(snapshot.epistemic || {}).includes(EPISTEMIC.CONFLICTING);
  if (conflicting) {
    const escalate = candidates.find(c => c.action_type === ACTION_TYPE.ESCALATE_OPERATOR);
    if (escalate) {
      for (const c of candidates) {
        if (c.action_type !== ACTION_TYPE.ESCALATE_OPERATOR) notSelected.push({ ...c, reason: 'Conflicting canonical evidence' });
      }
      return {
        selected: escalate,
        notSelected,
        rationale: 'Conflicting evidence blocks autonomous material action; escalating to operator.',
        confidence: 0.85,
        outcome: EXECUTION_STATUS.EXECUTION_BLOCKED,
        assumptions,
      };
    }
  }

  const missingOwner = snapshot.epistemic?.account_owner === EPISTEMIC.UNKNOWN;
  const missingWindow = snapshot.expectation && snapshot.epistemic?.expectation_window === EPISTEMIC.UNKNOWN;
  if (missingOwner || (snapshot.expectation && missingWindow && trigger?.type !== DECISION_TRIGGER.NEW_EVIDENCE)) {
    const abstain = candidates.find(c => c.action_type === ACTION_TYPE.ABSTAIN_INSUFFICIENT_EVIDENCE);
    for (const c of candidates) {
      if (c.action_type !== ACTION_TYPE.ABSTAIN_INSUFFICIENT_EVIDENCE) {
        notSelected.push({ ...c, reason: 'INSUFFICIENT_EVIDENCE' });
      }
    }
    return {
      selected: abstain,
      notSelected,
      rationale: 'Required evidence is missing; abstaining rather than inventing certainty.',
      confidence: 0.9,
      outcome: EXECUTION_STATUS.INSUFFICIENT_EVIDENCE,
      assumptions: ['Ownership or expectation window not canonically known'],
    };
  }

  if (trigger?.type === DECISION_TRIGGER.NEW_EVIDENCE && snapshot.has_resolving_activity) {
    const resolve = candidates.find(c => c.action_type === ACTION_TYPE.RESOLVE_EXPECTATION);
    const supersede = candidates.find(c => c.action_type === ACTION_TYPE.SUPERSEDE_PRIOR);
    for (const c of candidates) {
      if (![ACTION_TYPE.RESOLVE_EXPECTATION, ACTION_TYPE.SUPERSEDE_PRIOR].includes(c.action_type)) {
        notSelected.push({ ...c, reason: 'New evidence resolves prior expectation' });
      }
    }
    return {
      selected: resolve || supersede,
      notSelected,
      rationale: 'New evidence resolves the open expectation; superseding stale follow-up.',
      confidence: 0.92,
      outcome: EXECUTION_STATUS.AUTHORIZED,
      assumptions,
      alsoSupersede: true,
    };
  }

  if (snapshot.expectation && expectationStillOpen(snapshot.expectation, now)) {
    const wait = candidates.find(c =>
      c.action_type === ACTION_TYPE.NO_ACTION && c.rationale_hint?.includes('window')
    ) || candidates.find(c => c.action_type === ACTION_TYPE.NO_ACTION);
    for (const c of candidates) {
      if (c !== wait) notSelected.push({ ...c, reason: 'Expected window still open' });
    }
    return {
      selected: wait,
      notSelected,
      rationale: 'Expected event window remains open; intentional no-action until reevaluation.',
      confidence: 0.95,
      outcome: EXECUTION_STATUS.NO_ACTION_RECORDED,
      assumptions,
      reevaluate_after: wait?.reevaluate_after || snapshot.expectation.expected_window?.ends_at,
    };
  }

  if (snapshot.expectation && isOverdue(snapshot.expectation, now)) {
    const ask = candidates.find(c => c.action_type === ACTION_TYPE.ASK_AO_STATUS)
      || candidates.find(c => c.action_type === ACTION_TYPE.CREATE_AO_TASK);
    for (const c of candidates) {
      if (c.action_type === ACTION_TYPE.RETURN_TO_COLD_OUTREACH) {
        notSelected.push({ ...c, reason: 'Existing relationship makes automated cold outreach inappropriate' });
      } else if (c.action_type === ACTION_TYPE.ESCALATE_OPERATOR) {
        notSelected.push({ ...c, reason: 'Routine AO follow-up — not escalating to operator' });
      } else if (c.action_type === ACTION_TYPE.NO_ACTION) {
        notSelected.push({ ...c, reason: 'Unresolved expectation requires closure' });
      } else if (c !== ask) {
        notSelected.push({ ...c, reason: 'Lower priority alternative' });
      }
    }
    assumptions.push('Tony owns relationship (canonical ownership)');
    return {
      selected: ask,
      notSelected,
      rationale: 'Expected inbound event expired with no subsequent resolving activity; AO owns the relationship — ask for status rather than cold outreach.',
      confidence: 0.88,
      outcome: EXECUTION_STATUS.AUTHORIZED,
      assumptions,
    };
  }

  const noop = candidates.find(c => c.action_type === ACTION_TYPE.NO_ACTION);
  for (const c of candidates) {
    if (c !== noop) notSelected.push({ ...c, reason: 'No actionable trigger state' });
  }
  return {
    selected: noop,
    notSelected,
    rationale: 'No material operational change required.',
    confidence: 0.7,
    outcome: EXECUTION_STATUS.NO_ACTION_RECORDED,
    assumptions,
  };
}

function prioritizeDecisions(decisions = []) {
  return [...decisions].sort((a, b) => {
    const score = (d) => {
      const p = d.priority || {};
      return (p.mission_relevance || 0) * 10
        + (p.urgency || 0) * 5
        + (p.importance || 0)
        + (p.relationship_sensitivity || 0) * 3;
    };
    return score(b) - score(a);
  });
}

module.exports = {
  selectAction,
  prioritizeDecisions,
};
