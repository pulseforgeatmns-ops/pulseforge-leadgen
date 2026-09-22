'use strict';

const {
  DEBRIEF_NEXT_ACTIONS,
  OPPORTUNITY_TYPES,
  OPPORTUNITY_STRENGTHS,
} = require('../utils/aoProspectRoutingConstants');

function normalize(value) {
  return String(value || '').trim();
}

function hasText(value) {
  return normalize(value).length > 0;
}

function diagnosisPresent(debrief) {
  return hasText(debrief.problem_or_risk)
    || hasText(debrief.opportunity_type)
    || hasText(debrief.opportunity_timing);
}

function discoverPresent(debrief) {
  return hasText(debrief.person_spoken_to)
    || hasText(debrief.decision_maker)
    || hasText(debrief.current_cleaning_solution)
    || hasText(debrief.stated_context);
}

function assessDebriefCompleteness(debrief) {
  const missing = [];
  if (!discoverPresent(debrief)) missing.push('discover');
  if (!diagnosisPresent(debrief)) missing.push('diagnose');
  if (!hasText(debrief.recommended_next_step)) missing.push('advise_next_step');
  if (debrief.specific_dated_next_step !== true && !debrief.follow_up_due_at) {
    missing.push('dated_next_step');
  }
  if (debrief.real_reason_to_continue !== true && !hasText(debrief.problem_or_risk)) {
    missing.push('reason_to_continue');
  }
  return missing;
}

function mapOpportunityType(raw) {
  const value = normalize(raw).toLowerCase().replace(/\s+/g, '_');
  if (OPPORTUNITY_TYPES.includes(value)) return value;
  if (/backup|overflow/.test(value)) return 'backup_overflow';
  if (/turnover/.test(value)) return 'turnover_support';
  if (/deep/.test(value)) return 'one_time_deep_clean';
  if (/common/.test(value)) return 'common_area_cleaning';
  if (/recurr/.test(value)) return 'recurring_cleaning';
  if (/not/.test(value)) return 'not_a_fit';
  return value || null;
}

function mapOpportunityStrength(raw) {
  const value = normalize(raw).toLowerCase();
  return OPPORTUNITY_STRENGTHS.includes(value) ? value : 'unclear';
}

function shouldBookAssessment(debrief) {
  const timing = normalize(debrief.opportunity_timing).toLowerCase();
  const strength = mapOpportunityStrength(debrief.opportunity_strength);
  const type = mapOpportunityType(debrief.opportunity_type);
  if (type === 'not_a_fit') return false;
  if (timing === 'not_at_all') return false;
  if (strength === 'weak' || strength === 'unclear') return false;
  const text = [
    debrief.problem_or_risk,
    debrief.stated_context,
    debrief.current_cleaning_solution,
    debrief.blocker,
  ].join(' ').toLowerCase();
  const activeNeed = /need|dissatisf|miss|gap|problem|turnover|backup|overload|unhappy|switch|urgent|now/.test(text);
  const vendorRisk = /backup|overflow|miss|unreliable|coverage|turnover/.test(text);
  return (timing === 'now' && (activeNeed || vendorRisk)) || strength === 'strong';
}

function shouldEscalateJake(debrief) {
  const strength = mapOpportunityStrength(debrief.opportunity_strength);
  const timing = normalize(debrief.opportunity_timing).toLowerCase();
  const text = [debrief.problem_or_risk, debrief.stated_context].join(' ').toLowerCase();
  const quoteReady = /quote|proposal|assessment|walkthrough|pricing|bid/.test(text);
  return strength === 'strong' && timing === 'now' && quoteReady;
}

function classifyNextAction(debrief) {
  const type = mapOpportunityType(debrief.opportunity_type);
  const timing = normalize(debrief.opportunity_timing).toLowerCase();
  const strength = mapOpportunityStrength(debrief.opportunity_strength);
  const text = [
    debrief.problem_or_risk,
    debrief.stated_context,
    debrief.current_cleaning_solution,
    debrief.recommended_next_step,
  ].join(' ').toLowerCase();

  if (type === 'not_a_fit' || /not a fit|no need|already covered|all set/.test(text)) {
    return {
      next_action: 'SUPPRESS',
      reason: 'Account is not a fit or has no commercial cleaning need.',
    };
  }
  const decisionMakerUnknown = !hasText(debrief.decision_maker);
  const proposingQuote = /quote|proposal|assessment|walkthrough|pricing|bid|send proposal/.test(text);
  if (decisionMakerUnknown && proposingQuote) {
    return {
      next_action: 'NEEDS_RESEARCH',
      reason: 'Decision-maker is still unknown — identify before quoting or proposing.',
    };
  }
  if (decisionMakerUnknown && !hasText(debrief.person_spoken_to)) {
    return {
      next_action: 'NEEDS_RESEARCH',
      reason: 'Decision-maker is still unknown — identify before quoting or proposing.',
    };
  }
  if (shouldEscalateJake(debrief)) {
    return {
      next_action: 'JAKE_REVIEW',
      reason: 'Strong active opportunity may be ready for owner review or facilities assessment.',
    };
  }
  if (shouldBookAssessment(debrief)) {
    return {
      next_action: 'BOOK_ASSESSMENT',
      reason: 'Debrief shows a real current need, vendor risk, or active timing.',
    };
  }
  if (/backup|overflow|later|month|follow up|check back/.test(text) || timing === 'later') {
    return {
      next_action: 'AO_FOLLOW_UP',
      reason: 'Prospect has future or backup need but no active quote yet.',
    };
  }
  if (/send info|vendor info|leave-behind|email/.test(text)) {
    return {
      next_action: 'SEND_INFO',
      reason: 'Next step is to send information before another live touch.',
    };
  }
  if (timing === 'later' || strength === 'weak') {
    return {
      next_action: 'NURTURE',
      reason: 'Real account but timing is not active.',
    };
  }
  return {
    next_action: 'NEEDS_RESEARCH',
    reason: 'More discovery is needed before prescribing the next commercial step.',
  };
}

function buildCoachingFeedback(debrief, evaluation) {
  const notes = [];
  if (debrief.prescribed_before_diagnosing === true) {
    notes.push('You recommended a next step before the debrief showed enough diagnosis. Next time, ask who handles vendor decisions and what they use today before offering to send information or book an assessment.');
  }
  if (!diagnosisPresent(debrief)) {
    notes.push('This debrief does not show enough diagnosis yet. Before recommending a facilities assessment, we need to know whether there is a current problem, upcoming need, or dissatisfaction with the existing cleaner.');
  }
  if (!hasText(debrief.decision_maker)) {
    notes.push('Next time, capture who handles cleaning decisions before moving to a quote or proposal step.');
  }
  if (evaluation.next_action === 'AO_FOLLOW_UP' && /backup|overflow/.test(normalize(debrief.problem_or_risk).toLowerCase())) {
    notes.push('Good debrief. You identified the current vendor situation and uncovered a backup opportunity. Next time, ask who handles vendor decisions before offering to send information.');
  }
  if (evaluation.next_action === 'BOOK_ASSESSMENT' && diagnosisPresent(debrief)) {
    notes.push('Good debrief. You diagnosed a real need and the next step is appropriately specific.');
  }
  if (!notes.length) {
    notes.push('Good debrief structure. Keep using Discover → Diagnose → Advise before prescribing the next step.');
  }
  return notes.join(' ');
}

function evaluateDebrief(debrief) {
  const missing = assessDebriefCompleteness(debrief);
  const classification = classifyNextAction(debrief);
  const opportunity_type = mapOpportunityType(debrief.opportunity_type);
  const opportunity_strength = mapOpportunityStrength(debrief.opportunity_strength);

  let debrief_quality = 'complete';
  if (missing.length >= 3 || debrief.prescribed_before_diagnosing === true) debrief_quality = 'weak';
  else if (missing.length > 0) debrief_quality = 'incomplete';

  const incomplete = missing.length > 0 || debrief.prescribed_before_diagnosing === true;
  const next_action_owner = normalize(debrief.next_owner)
    || (classification.next_action === 'JAKE_REVIEW' ? 'jake' : 'ao');

  const evaluation = {
    next_action: classification.next_action,
    opportunity_type,
    opportunity_strength,
    next_action_owner,
    follow_up_due_at: debrief.follow_up_due_at || null,
    coaching_feedback: buildCoachingFeedback(debrief, classification),
    debrief_quality,
    incomplete,
    missing_sections: missing,
    classification_reason: classification.reason,
    suggested_message: debrief.recommended_message || null,
  };

  if (!DEBRIEF_NEXT_ACTIONS.includes(evaluation.next_action)) {
    evaluation.next_action = 'NEEDS_RESEARCH';
  }

  return evaluation;
}

module.exports = {
  assessDebriefCompleteness,
  classifyNextAction,
  evaluateDebrief,
  shouldBookAssessment,
  diagnosisPresent,
};
