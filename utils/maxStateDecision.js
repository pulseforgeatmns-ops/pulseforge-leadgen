const VALID_LIFECYCLE_STATES = Object.freeze([
  'cold', 'heating', 'warm', 'hot', 'engaged', 'nurture', 'recycle', 'disqualified', 'null',
]);
const TERMINAL_STATES = new Set(['disqualified', 'null']);

const DIRECT_TRANSITIONS = Object.freeze({
  email_positive_reply: { state: 'engaged', code: 'POSITIVE_REPLY_RECEIVED' },
  email_meaningful_reply: { state: 'engaged', code: 'MEANINGFUL_REPLY_RECEIVED' },
  operator_marked_hot: { state: 'hot', code: 'OPERATOR_MARKED_HOT' },
  operator_marked_warm: { state: 'warm', code: 'OPERATOR_MARKED_WARM' },
  email_unsubscribed: { state: 'disqualified', code: 'EMAIL_UNSUBSCRIBED' },
  operator_disqualified: { state: 'disqualified', code: 'OPERATOR_DISQUALIFIED' },
  operator_nulled: { state: 'null', code: 'OPERATOR_NULLED' },
  contact_invalid: { state: 'null', code: 'CONTACT_CONFIRMED_INVALID' },
  email_hard_bounced_confirmed_invalid: { state: 'null', code: 'CONFIRMED_INVALID_HARD_BOUNCE' },
});

function normalizeLifecycleState(value) {
  const state = String(value || 'cold').trim().toLowerCase();
  if (!VALID_LIFECYCLE_STATES.includes(state)) throw new Error(`Invalid lifecycle state: ${value}`);
  return state;
}

function hoursSince(value, now) {
  if (!value) return 0;
  const at = normalizeEventTimestamp(value, { source: 'max_state_decision', field: 'downgrade_candidate_since' });
  return Math.max(0, (now.getTime() - at.getTime()) / 3600000);
}

function upwardState(score, thresholds) {
  if (score >= thresholds.hot) return 'hot';
  if (score >= thresholds.warm) return 'warm';
  if (score >= thresholds.heating) return 'heating';
  return 'cold';
}

function actionPolicy(recommendedState, prospect) {
  if (TERMINAL_STATES.has(recommendedState)) {
    return { next: 'suppress_outreach', operator: false, priority: 'high', actions: ['stop_automated_sequences'] };
  }
  if (recommendedState === 'engaged') {
    return { next: 'operator_review_reply', operator: true, priority: 'high', actions: ['stop_automated_sequences', 'create_reply_required_task'] };
  }
  if (recommendedState === 'hot') {
    return { next: 'operator_review', operator: true, priority: 'urgent', actions: ['pause_automated_outreach', 'create_hot_prospect_review_task'] };
  }
  if (recommendedState === 'warm') {
    if (!prospect.email || prospect.email_verified !== true) {
      return { next: 'prioritized_enrichment', operator: false, priority: 'normal', actions: ['pause_cold_sequence', 'retry_enrichment'] };
    }
    const actions = ['pause_cold_sequence', 'start_warm_sequence'];
    if (prospect.phone) actions.push('create_call_task');
    return { next: 'start_warm_sequence', operator: false, priority: 'normal', actions };
  }
  if (recommendedState === 'heating') {
    return { next: 'monitor_signals', operator: false, priority: 'low', actions: [] };
  }
  if (recommendedState === 'recycle') {
    return { next: 'schedule_recycle', operator: false, priority: 'low', actions: ['schedule_recycle'] };
  }
  return { next: 'maintain_cold_sequence', operator: false, priority: 'low', actions: [] };
}

function determineStateDecision({ prospect = {}, scoreResult, signals = [], config, now = new Date() }) {
  const currentState = normalizeLifecycleState(prospect.lifecycle_state || 'cold');
  const score = Number(scoreResult?.score || 0);
  const reasonCodes = [];
  let recommendedState = currentState;
  let downgradeCandidateSince = null;
  const direct = DIRECT_TRANSITIONS[scoreResult?.direct_state_event];
  const operatorRestore = ['operator_marked_hot', 'operator_marked_warm'].includes(scoreResult?.direct_state_event);

  if (TERMINAL_STATES.has(currentState) && !operatorRestore) {
    reasonCodes.push('TERMINAL_STATE_PROTECTED');
  } else if (direct) {
    recommendedState = direct.state;
    reasonCodes.push(direct.code, 'DIRECT_STATE_EVENT');
  } else {
    const upward = upwardState(score, config.thresholds);
    const rank = { cold: 0, heating: 1, warm: 2, hot: 3 };
    if (rank[upward] > (rank[currentState] ?? 0) && ['cold', 'heating', 'warm', 'hot'].includes(currentState)) {
      recommendedState = upward;
      reasonCodes.push(`${upward.toUpperCase()}_THRESHOLD_CROSSED`);
    } else {
      const downgrade = {
        hot: { threshold: config.downgrade_thresholds.hot_to_warm, state: 'warm' },
        warm: { threshold: config.downgrade_thresholds.warm_to_heating, state: 'heating' },
        heating: { threshold: config.downgrade_thresholds.heating_to_cold, state: 'cold' },
      }[currentState];
      if (downgrade && score < downgrade.threshold) {
        if (prospect.downgrade_candidate_since && hoursSince(prospect.downgrade_candidate_since, now) >= Number(config.downgrade_stabilization_hours)) {
          recommendedState = downgrade.state;
          reasonCodes.push('DOWNGRADE_STABILIZED', `${currentState.toUpperCase()}_COOLED`);
        } else {
          downgradeCandidateSince = prospect.downgrade_candidate_since || now.toISOString();
          reasonCodes.push('DOWNGRADE_STABILIZING');
        }
      } else {
        reasonCodes.push('STATE_RETAINED');
      }
    }
  }

  const policy = actionPolicy(recommendedState, prospect);
  const componentCodes = (scoreResult?.components || []).filter(c => c.points > 0).map(c => c.code);
  reasonCodes.push(...componentCodes.slice(0, 4));
  const changed = recommendedState !== currentState;
  const reasonSummary = changed
    ? `Recommend ${currentState} → ${recommendedState} with warmth score ${score}. ${reasonCodes.slice(0, 3).join(', ')}.`
    : `Retain ${currentState} with warmth score ${score}. ${reasonCodes.slice(0, 3).join(', ')}.`;

  return {
    prospect_id: prospect.id,
    current_state: currentState,
    recommended_state: recommendedState,
    transition_recommended: changed,
    next_best_action: policy.next,
    operator_required: policy.operator,
    operator_priority: policy.priority,
    operator_reason: policy.operator ? reasonSummary : null,
    reason_codes: [...new Set(reasonCodes)],
    reason_summary: reasonSummary,
    actions: policy.actions,
    downgrade_candidate_since: downgradeCandidateSince,
    decision_version: config.decision_version,
  };
}

module.exports = {
  DIRECT_TRANSITIONS,
  TERMINAL_STATES,
  VALID_LIFECYCLE_STATES,
  determineStateDecision,
  normalizeLifecycleState,
  upwardState,
};
const { normalizeEventTimestamp } = require('./maxTimestamp');
