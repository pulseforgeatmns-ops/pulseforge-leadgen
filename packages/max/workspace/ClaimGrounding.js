'use strict';

/**
 * SPEC-108 — claim grounding as a transferable reasoning competency.
 *
 * Recommendations may only depend on supported operating-state claims.
 * Every operating-state claim is classified as supported, partially
 * supported, or unsupported before recommendation generation.
 *
 * Distinctions that must be preserved:
 *   planned != completed
 *   inventory != execution
 *   goals != operating state
 *   historical != current
 *   mission != execution
 *
 * No persistent memory. Session working-model correction only.
 */

const SUPPORT = Object.freeze({
  SUPPORTED: 'supported',
  PARTIALLY_SUPPORTED: 'partially_supported',
  UNSUPPORTED: 'unsupported',
});

const EVIDENCE_KIND = Object.freeze({
  OBSERVED: 'observed',
  INFERRED: 'inferred',
  PLANNED: 'planned',
  ASSUMPTION: 'assumption',
  OBJECTIVE: 'objective',
  INVENTORY: 'inventory',
  HISTORICAL: 'historical',
});

const VERDICT = Object.freeze({
  CONFIRM: 'confirmed',
  QUALIFY: 'qualified',
  RETRACT: 'retract',
});

const TOPICS = Object.freeze({
  EMAIL_MOTION: 'email_motion',
  FOLLOW_UP: 'follow_up',
  OUTREACH_BEGUN: 'outreach_begun',
  INVENTORY: 'inventory',
  COMMERCIAL_EXPANSION: 'commercial_expansion',
  OBJECTIVE: 'objective',
  MAIL: 'mail',
  CAMPAIGN_COMPLETED: 'campaign_completed',
});

function present(text) {
  return String(text || '')
    .replace(/\s{2,}/g, ' ')
    .trim();
}

function claimText(claim) {
  return present(claim && (claim.text || claim.claim || claim.assertedText));
}

function formatDay(value) {
  if (!value) return null;
  const d = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(d.getTime())) return present(value);
  return d.toISOString().slice(0, 10);
}

function claimAssertsCompletion(text) {
  return /\b(occurred|completed|already (?:happened|done|begun|executed|active|running)|has (?:occurred|begun|happened|started)|is (?:already )?(?:active|running|executing))\b/i.test(
    present(text)
  );
}

function evaluationResult({
  support,
  verdict,
  epistemic,
  detail,
  distinction = null,
  evidence = null,
}) {
  return {
    support,
    verdict,
    epistemic,
    detail,
    distinction,
    evidence,
  };
}

function inventoryEvidence(state = {}) {
  const total = Number((state.prospects && state.prospects.total) || 0);
  const scout = Number((state.scout && state.scout.matched) || 0);
  if (total <= 0 && scout <= 0) return null;
  return { kind: EVIDENCE_KIND.INVENTORY, total, scout };
}

function objectiveEvidence(state = {}) {
  const rows = Array.isArray(state.objectives) ? state.objectives : [];
  const texts = rows
    .map((row) => present(row && (row.title || row.objectiveText || row.statement || row.goal)))
    .filter(Boolean);
  if (state.commercialPreference) texts.push('commercial preference recorded on the Blueprint');
  if (!texts.length) return null;
  return { kind: EVIDENCE_KIND.OBJECTIVE, texts };
}

function plannedFollowUp(state = {}) {
  const follow = state.followUp || {};
  return (
    follow.kind === 'planned' ||
    follow.kind === 'planned_future' ||
    follow.kind === 'planned_due'
  );
}

function currentOutreach(state = {}) {
  if (state.emailMotion && state.emailMotion.current === true) {
    return { kind: EVIDENCE_KIND.OBSERVED, source: 'email_motion' };
  }
  if (state.followUp && state.followUp.kind === 'completed' && state.followUp.executed) {
    return { kind: EVIDENCE_KIND.OBSERVED, source: 'follow_up' };
  }
  return null;
}

function deliveryLogs(state = {}) {
  const items = Array.isArray(state.items) ? state.items : [];
  const fromItems = items.filter((item) =>
    /deliver|mailed|postage|execution log/i.test(String((item && (item.claim || item.provenance)) || ''))
  );
  const activity = [
    ...(((state.activity && state.activity.touchpoints) || [])),
    ...(((state.activity && state.activity.activity) || [])),
  ];
  const fromActivity = activity.filter((row) =>
    /mail|deliver/i.test(String((row && (row.channel || row.action_type || row.status)) || ''))
  );
  return { items: fromItems, activity: fromActivity, present: fromItems.length + fromActivity.length > 0 };
}

function evaluateEmailMotionClaim(claim, state = {}) {
  const motion = state.emailMotion || {};
  if (motion.kind === 'active' && motion.current === true) {
    return evaluationResult({
      support: SUPPORT.SUPPORTED,
      verdict: VERDICT.CONFIRM,
      epistemic: EVIDENCE_KIND.OBSERVED,
      detail:
        'Current execution evidence records an outbound email motion in progress. That is distinct from historical sends or a planned mission.',
      evidence: motion,
    });
  }
  if (motion.kind === 'historical') {
    return evaluationResult({
      support: SUPPORT.UNSUPPORTED,
      verdict: VERDICT.RETRACT,
      epistemic: EVIDENCE_KIND.HISTORICAL,
      detail:
        'Historical email touchpoints exist, but they do not establish that email outbound is currently active.',
      distinction: 'I treated historical activity as current execution.',
      evidence: motion,
    });
  }
  if (motion.kind === 'planned') {
    return evaluationResult({
      support: SUPPORT.UNSUPPORTED,
      verdict: VERDICT.RETRACT,
      epistemic: EVIDENCE_KIND.PLANNED,
      detail: 'An email-related mission is on file as planned/intent work. A mission is not execution.',
      distinction: 'I treated planned work as completed.',
      evidence: motion,
    });
  }
  if (motion.kind === 'disabled') {
    return evaluationResult({
      support: SUPPORT.UNSUPPORTED,
      verdict: VERDICT.RETRACT,
      epistemic: 'exists_disabled',
      detail: 'Emmett exists but is disabled for this tenant. Disabled is not active.',
      distinction: 'I treated a disabled capability as active execution.',
      evidence: motion,
    });
  }
  return evaluationResult({
    support: SUPPORT.UNSUPPORTED,
    verdict: VERDICT.RETRACT,
    epistemic: 'not_recorded',
    detail:
      'The evidence I retrieved establishes prospect inventory and historical activity, but neither proves current email execution.',
    distinction: 'I treated inventory as execution.',
    evidence: motion,
  });
}

function evaluateFollowUpClaim(claim, state = {}) {
  const follow = state.followUp || {};
  const text = claimText(claim);
  const wantsCompleted = claimAssertsCompletion(text) || /already occurred|has occurred/i.test(text);
  const campaign = present(state.campaignName) || 'the campaign';

  if (follow.kind === 'completed') {
    return evaluationResult({
      support: SUPPORT.SUPPORTED,
      verdict: VERDICT.CONFIRM,
      epistemic: EVIDENCE_KIND.OBSERVED,
      detail: `Follow-up on ${campaign} has recorded execution evidence.`,
      evidence: follow,
    });
  }

  if (wantsCompleted) {
    const when = follow.expectedAt ? formatDay(follow.expectedAt) : null;
    return evaluationResult({
      support: SUPPORT.UNSUPPORTED,
      verdict: VERDICT.RETRACT,
      epistemic: EVIDENCE_KIND.PLANNED,
      detail: when
        ? `I know a follow-up has been scheduled for ${when}. I do not currently have evidence that it has occurred.`
        : 'I do not currently have evidence that follow-up has occurred.',
      distinction: 'I treated planned work as completed.',
      evidence: follow,
    });
  }

  if (follow.expectedAt || plannedFollowUp(state)) {
    const when = follow.expectedAt ? formatDay(follow.expectedAt) : 'the reported date';
    return evaluationResult({
      support: SUPPORT.PARTIALLY_SUPPORTED,
      verdict: VERDICT.QUALIFY,
      epistemic: EVIDENCE_KIND.PLANNED,
      detail: `I know a follow-up has been scheduled${follow.expectedAt ? ` for ${when}` : ''}. I do not currently have evidence that it has occurred.`,
      distinction: 'Planned or expected is not completed execution.',
      evidence: follow,
    });
  }

  return evaluationResult({
    support: SUPPORT.UNSUPPORTED,
    verdict: VERDICT.RETRACT,
    epistemic: 'not_recorded',
    detail: 'I cannot verify that follow-up is underway.',
    evidence: follow,
  });
}

function evaluateOutreachClaim(_claim, state = {}) {
  const current = currentOutreach(state);
  if (current) {
    return evaluationResult({
      support: SUPPORT.SUPPORTED,
      verdict: VERDICT.CONFIRM,
      epistemic: EVIDENCE_KIND.OBSERVED,
      detail: 'Current execution evidence records that outreach is underway.',
      evidence: current,
    });
  }

  const inventory = inventoryEvidence(state);
  const planned = plannedFollowUp(state) || (state.emailMotion && state.emailMotion.kind === 'planned');

  if (inventory) {
    return evaluationResult({
      support: SUPPORT.UNSUPPORTED,
      verdict: VERDICT.RETRACT,
      epistemic: EVIDENCE_KIND.INVENTORY,
      detail: `Prospect inventory (${inventory.total || inventory.scout} discovered) is not execution. Discovery is not outreach.`,
      distinction: 'I treated inventory as execution.',
      evidence: inventory,
    });
  }

  if (planned) {
    return evaluationResult({
      support: SUPPORT.UNSUPPORTED,
      verdict: VERDICT.RETRACT,
      epistemic: EVIDENCE_KIND.PLANNED,
      detail: 'I have evidence that outreach is planned, but not that it has occurred.',
      distinction: 'I treated planned work as completed.',
      evidence: state.followUp || state.emailMotion,
    });
  }

  return evaluationResult({
    support: SUPPORT.UNSUPPORTED,
    verdict: VERDICT.RETRACT,
    epistemic: 'not_recorded',
    detail: 'I cannot verify that outreach has begun.',
    distinction: 'I treated inventory as execution.',
  });
}

function evaluateInventoryClaim(_claim, state = {}) {
  const inventory = inventoryEvidence(state);
  if (inventory) {
    return evaluationResult({
      support: SUPPORT.SUPPORTED,
      verdict: VERDICT.CONFIRM,
      epistemic: EVIDENCE_KIND.INVENTORY,
      detail: `${inventory.total || inventory.scout} prospect${(inventory.total || inventory.scout) === 1 ? '' : 's'} are recorded. That is inventory, not outreach.`,
      evidence: inventory,
    });
  }
  return evaluationResult({
    support: SUPPORT.UNSUPPORTED,
    verdict: VERDICT.RETRACT,
    epistemic: 'not_recorded',
    detail: 'I cannot verify recorded prospect inventory.',
  });
}

function evaluateCommercialExpansionClaim(_claim, state = {}) {
  const jobs = Number((state.outcomes && state.outcomes.jobs) || 0);
  const payments = Number((state.outcomes && state.outcomes.payments) || 0);
  if (jobs > 0 || payments > 0) {
    return evaluationResult({
      support: SUPPORT.SUPPORTED,
      verdict: VERDICT.CONFIRM,
      epistemic: EVIDENCE_KIND.OBSERVED,
      detail: 'Recorded jobs or payments support that commercial work is underway.',
      evidence: state.outcomes,
    });
  }

  const objectives = objectiveEvidence(state);
  if (objectives) {
    const quoted = objectives.texts[0];
    return evaluationResult({
      support: SUPPORT.PARTIALLY_SUPPORTED,
      verdict: VERDICT.QUALIFY,
      epistemic: EVIDENCE_KIND.OBJECTIVE,
      detail: `"${quoted}" is a stated objective, not observed operating evidence that expansion is underway.`,
      distinction: 'I treated a stated objective as observed operating state.',
      evidence: objectives,
    });
  }

  return evaluationResult({
    support: SUPPORT.UNSUPPORTED,
    verdict: VERDICT.RETRACT,
    epistemic: 'not_recorded',
    detail: 'I cannot verify that the commercial business is expanding.',
    distinction: 'I treated a stated objective as observed operating state.',
  });
}

function evaluateMailClaim(_claim, state = {}) {
  const campaign = present(state.campaignName) || 'the campaign';
  const logs = deliveryLogs(state);
  if (state.mailAttestedAt) {
    return evaluationResult({
      support: SUPPORT.SUPPORTED,
      verdict: VERDICT.CONFIRM,
      epistemic: 'operator_attested',
      detail: `${campaign} was operator-reported as physically mailed on ${formatDay(state.mailAttestedAt)}. Provenance: operator report (SPEC-106). That is operator-attested, not independently system-observed.`,
      evidence: { occurredAt: state.mailAttestedAt, deliveryLogs: logs },
    });
  }
  if (state.mailExecuted) {
    return evaluationResult({
      support: SUPPORT.SUPPORTED,
      verdict: VERDICT.CONFIRM,
      epistemic: EVIDENCE_KIND.OBSERVED,
      detail: `System-verified evidence records that ${campaign} was mailed.${
        logs.present ? ' Delivery logs are on file.' : ''
      }`,
      evidence: { mailExecuted: true, deliveryLogs: logs },
    });
  }
  return evaluationResult({
    support: SUPPORT.UNSUPPORTED,
    verdict: VERDICT.RETRACT,
    epistemic: 'not_recorded',
    detail: `I cannot verify that ${campaign} was mailed.`,
  });
}

function evaluateGenericClaim(claim, state = {}) {
  const text = claimText(claim);
  if (claimAssertsCompletion(text) && plannedFollowUp(state)) {
    return evaluateFollowUpClaim({ ...claim, topic: TOPICS.FOLLOW_UP, text }, state);
  }
  if (/outreach|execution/i.test(text) && inventoryEvidence(state)) {
    return evaluateOutreachClaim(claim, state);
  }
  if (/expanding|objective|goal/i.test(text)) {
    return evaluateCommercialExpansionClaim(claim, state);
  }
  return evaluationResult({
    support: SUPPORT.UNSUPPORTED,
    verdict: VERDICT.RETRACT,
    epistemic: 'not_recorded',
    detail: 'I cannot verify that claim from retrieved operating evidence.',
  });
}

const TOPIC_EVALUATORS = Object.freeze({
  [TOPICS.EMAIL_MOTION]: evaluateEmailMotionClaim,
  [TOPICS.FOLLOW_UP]: evaluateFollowUpClaim,
  follow_up_completed: evaluateFollowUpClaim,
  [TOPICS.OUTREACH_BEGUN]: evaluateOutreachClaim,
  [TOPICS.INVENTORY]: evaluateInventoryClaim,
  [TOPICS.COMMERCIAL_EXPANSION]: evaluateCommercialExpansionClaim,
  [TOPICS.OBJECTIVE]: evaluateCommercialExpansionClaim,
  [TOPICS.MAIL]: evaluateMailClaim,
  [TOPICS.CAMPAIGN_COMPLETED]: evaluateMailClaim,
});

function evaluateOperatingStateClaim(claim, state = {}) {
  const topic = claim && claim.topic;
  const evaluator = TOPIC_EVALUATORS[topic] || evaluateGenericClaim;
  return evaluator(claim, state);
}

function collectOperatingStateClaims(state = {}) {
  const claims = [];
  const email = state.emailMotion || { kind: 'not_recorded', current: false };
  claims.push({
    id: TOPICS.EMAIL_MOTION,
    topic: TOPICS.EMAIL_MOTION,
    text: email.current
      ? 'An outbound email motion is already active.'
      : 'A currently active outbound email motion is not verified.',
    assertedText: 'An outbound email motion is already active.',
    kind: email.current ? EVIDENCE_KIND.OBSERVED : email.kind,
  });

  if (state.mailAttestedAt || state.mailExecuted) {
    claims.push({
      id: TOPICS.MAIL,
      topic: TOPICS.MAIL,
      text: state.mailAttestedAt
        ? `${present(state.campaignName) || 'Campaign'} was operator-reported as mailed ${formatDay(state.mailAttestedAt)}.`
        : `${present(state.campaignName) || 'Campaign'} was mailed.`,
      kind: state.mailAttestedAt ? 'operator_attested' : EVIDENCE_KIND.OBSERVED,
    });
    claims.push({
      id: TOPICS.CAMPAIGN_COMPLETED,
      topic: TOPICS.CAMPAIGN_COMPLETED,
      text: `${present(state.campaignName) || 'Campaign'} is complete and delivery evidence is recorded.`,
      kind: EVIDENCE_KIND.OBSERVED,
    });
  }

  if (state.followUp && state.followUp.kind !== 'none') {
    claims.push({
      id: TOPICS.FOLLOW_UP,
      topic: TOPICS.FOLLOW_UP,
      text:
        state.followUp.kind === 'completed'
          ? `${present(state.campaignName) || 'Campaign'} follow-up has recorded execution.`
          : `${present(state.campaignName) || 'Campaign'} follow-up is planned and owned; completion is not yet recorded.`,
      assertedText: 'Follow-up occurred.',
      kind: state.followUp.kind === 'completed' ? EVIDENCE_KIND.OBSERVED : EVIDENCE_KIND.PLANNED,
    });
  }

  const inventory = inventoryEvidence(state);
  if (inventory) {
    claims.push({
      id: TOPICS.INVENTORY,
      topic: TOPICS.INVENTORY,
      text: `${inventory.total || inventory.scout} prospects are recorded as inventory.`,
      kind: EVIDENCE_KIND.INVENTORY,
    });
    claims.push({
      id: TOPICS.OUTREACH_BEGUN,
      topic: TOPICS.OUTREACH_BEGUN,
      text: 'Outreach has begun.',
      kind: EVIDENCE_KIND.INFERRED,
    });
  }

  if (objectiveEvidence(state)) {
    claims.push({
      id: TOPICS.COMMERCIAL_EXPANSION,
      topic: TOPICS.COMMERCIAL_EXPANSION,
      text: 'You are expanding your commercial business.',
      kind: EVIDENCE_KIND.OBJECTIVE,
    });
  }

  return claims;
}

function evaluateAllOperatingStateClaims(state = {}) {
  return collectOperatingStateClaims(state).map((claim) => {
    const evaluation = evaluateOperatingStateClaim(claim, state);
    return {
      ...claim,
      support: evaluation.support,
      status: evaluation.support,
      epistemic: evaluation.epistemic,
      evaluation,
    };
  });
}

function applyClaimEvaluations(state = {}, evaluations = []) {
  const next = { ...state, claimEvaluations: evaluations };
  const byTopic = {};
  for (const row of evaluations) {
    if (row && row.topic) byTopic[row.topic] = row;
  }

  const email = byTopic[TOPICS.EMAIL_MOTION];
  if (email && email.evaluation && email.evaluation.support !== SUPPORT.SUPPORTED) {
    if (next.emailMotion && next.emailMotion.current === true && email.evaluation.verdict === VERDICT.RETRACT) {
      next.emailMotion = { ...next.emailMotion, current: false };
      next.emailMotionActive = false;
    }
  }

  const expansion = byTopic[TOPICS.COMMERCIAL_EXPANSION];
  next.commercialExpansionObserved = Boolean(
    expansion && expansion.evaluation && expansion.evaluation.support === SUPPORT.SUPPORTED
  );

  const outreach = byTopic[TOPICS.OUTREACH_BEGUN];
  next.outreachBegun =
    Boolean(outreach && outreach.evaluation && outreach.evaluation.support === SUPPORT.SUPPORTED);

  return next;
}

function identifyClaimTopic(question) {
  const q = present(question).toLowerCase();
  if (!q) return null;
  if (/follow[- ]up/i.test(q)) return TOPICS.FOLLOW_UP;
  if (
    /outreach (?:has )?(?:already )?(?:begun|started)|already reaching out|outreach is (?:already )?(?:underway|active)/i.test(
      q
    )
  ) {
    return TOPICS.OUTREACH_BEGUN;
  }
  if (/expanding|commercial business|twenty commercial|acquire twenty/i.test(q)) {
    return TOPICS.COMMERCIAL_EXPANSION;
  }
  if (/email|outbound|emmett|autosend|sending/i.test(q)) return TOPICS.EMAIL_MOTION;
  if (/mail(?:ed)?|august|postage|delivery logs?|campaign complete/i.test(q) && !/email/i.test(q)) {
    return TOPICS.MAIL;
  }
  return null;
}

function assertedTextForTopic(topic, question) {
  const q = present(question);
  if (topic === TOPICS.FOLLOW_UP && claimAssertsCompletion(q)) return 'Follow-up occurred.';
  if (topic === TOPICS.OUTREACH_BEGUN) return 'Outreach has begun.';
  if (topic === TOPICS.COMMERCIAL_EXPANSION) return 'You are expanding your commercial business.';
  if (topic === TOPICS.EMAIL_MOTION && /already active|currently active|is active|is running|is executing/i.test(q)) {
    return 'An outbound email motion is already active.';
  }
  if (topic === TOPICS.MAIL) return 'Campaign 001 was mailed.';
  return null;
}

function formatChallengeResponse({ claim, evaluation, revised, correction }) {
  const text = claimText(claim) || 'that statement';
  const parts = [];

  if (correction) {
    parts.push(
      'I accept that as an operator-attested correction of my working model. I will not continue asserting that outbound email is currently active. This does not invent a new durable operating event.'
    );
  }

  parts.push(`The challenged claim was: ${text}`);

  if (evaluation.verdict === VERDICT.CONFIRM) {
    parts.push('Based on the available evidence I still believe this claim is supported.');
    parts.push('Evidence:');
    parts.push(evaluation.detail);
    parts.push('I am confirming that claim from retrieved evidence, not from my prior wording.');
  } else if (evaluation.verdict === VERDICT.QUALIFY) {
    parts.push("You're right that I overstated the current state.");
    parts.push(evaluation.detail);
    parts.push('I am qualifying that claim: planned/expected is not completed execution.');
  } else {
    parts.push("You're right.");
    if (evaluation.distinction) parts.push(evaluation.distinction);
    if (/email|outbound/i.test(text) && claim && claim.topic === TOPICS.EMAIL_MOTION) {
      parts.push(
        `I can't verify that an outbound email motion is currently active. ${evaluation.detail} I retract that statement.`
      );
    } else {
      parts.push(`I can't verify that claim. ${evaluation.detail}`);
      parts.push('I no longer believe that claim is supported.');
      parts.push('I retract that statement.');
    }
  }

  if (revised && (evaluation.verdict === VERDICT.RETRACT || correction)) {
    parts.push('');
    parts.push('REVISED RECOMMENDATION');
    parts.push(revised.prose);
  }

  return parts.join('\n');
}

module.exports = {
  SUPPORT,
  EVIDENCE_KIND,
  VERDICT,
  TOPICS,
  evaluateOperatingStateClaim,
  evaluateAllOperatingStateClaims,
  collectOperatingStateClaims,
  applyClaimEvaluations,
  identifyClaimTopic,
  assertedTextForTopic,
  claimAssertsCompletion,
  formatChallengeResponse,
  inventoryEvidence,
  objectiveEvidence,
};
