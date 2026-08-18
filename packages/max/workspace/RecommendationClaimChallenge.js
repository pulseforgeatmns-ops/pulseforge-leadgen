'use strict';

/**
 * SPEC-107A — targeted claim challenge, retraction, and working-model correction.
 *
 * Does not persist Max-generated statements as operating facts.
 * Does not create a new memory store. Session working model only.
 */

const CLAIM_CHALLENGE_RE = new RegExp(
  [
    String.raw`\bwhat evidence (?:supports|tells you|backs|justifies|proves|in (?:pulseforge|the system))\b`,
    String.raw`\bwhat evidence .{0,40}\btells you\b`,
    String.raw`\bwhere did you get that\b`,
    String.raw`\bhow do you know (?:that|this|it|email)\b`,
    String.raw`\bcan you verify (?:that|this|it)\b`,
    String.raw`\byou said\b.{0,120}\bwhat evidence\b`,
    String.raw`\bwhy do you (?:think|believe|say)\b`,
    String.raw`\bthat(?:'s| is) not right\b`,
    String.raw`\bthat(?:'s| is) (?:wrong|incorrect|unsupported)\b`,
  ].join('|'),
  'i'
);

const CLAIM_CORRECTION_RE = new RegExp(
  [
    String.raw`\bno,?\s+(?:email |outbound )?(?:email )?outbound isn'?t\b`,
    String.raw`\bemail outbound isn'?t (?:running|active)`,
    String.raw`\b(?:email|outbound(?: email)?) (?:is |are )?(?:not|isn'?t) (?:currently )?(?:active|running|executing)`,
  ].join('|'),
  'i'
);

const INVENTORY_EVIDENCE_RE =
  /\bwhat evidence (?:do we|have we) already\b|\bevidence[- ]based inventory\b|\bwhat evidence is (?:already )?(?:recorded|on file)\b/i;

function present(text) {
  return String(text || '')
    .replace(/\s{2,}/g, ' ')
    .trim();
}

function isClaimChallenge(question) {
  const q = present(question);
  if (!q) return false;
  if (INVENTORY_EVIDENCE_RE.test(q)) return false;
  return CLAIM_CHALLENGE_RE.test(q);
}

function isOperatorClaimCorrection(question) {
  const q = present(question);
  if (!q) return false;
  if (isClaimChallenge(q) && !CLAIM_CORRECTION_RE.test(q)) return false;
  return CLAIM_CORRECTION_RE.test(q);
}

function sessionContext(input = {}) {
  const session = input.session || {};
  return session.context && typeof session.context === 'object' ? session.context : {};
}

function lastRecommendationFrom(input = {}) {
  if (input.lastRecommendation && typeof input.lastRecommendation === 'object') {
    return input.lastRecommendation;
  }
  const ctx = sessionContext(input);
  if (ctx.lastRecommendation && typeof ctx.lastRecommendation === 'object') {
    return ctx.lastRecommendation;
  }
  const envelope = input.context && typeof input.context === 'object' ? input.context : {};
  return envelope.lastRecommendation || null;
}

function retractedIdsFrom(input = {}) {
  const ctx = sessionContext(input);
  const fromSession = Array.isArray(ctx.retractedPremises) ? ctx.retractedPremises : [];
  const fromInput = Array.isArray(input.retractedPremises) ? input.retractedPremises : [];
  return Array.from(new Set([...fromSession, ...fromInput].map(String)));
}

function defaultEmailClaim() {
  return {
    id: 'email_motion',
    topic: 'email_motion',
    text: 'An outbound email motion is already active.',
    kind: 'inferred',
    support: 'unknown',
  };
}

function looksLikeAssertedActiveEmail(premise) {
  if (!premise || premise.topic !== 'email_motion') return false;
  const text = String(premise.text || premise.claim || '');
  if (/cannot verify|not verify|do not establish|not currently active|not verified|disabled|planned or intended/i.test(text)) {
    return false;
  }
  return (
    premise.support === 'supported' ||
    /already active|currently active|is executing|is running/i.test(text)
  );
}

function identifyChallengedClaim(question, lastRecommendation = null) {
  const q = present(question).toLowerCase();
  const premises = (lastRecommendation && lastRecommendation.premises) || [];

  if (/email|outbound|emmett|autosend|sending/i.test(q)) {
    const found = premises.find((p) => p.topic === 'email_motion');
    if (/already active|currently active|is active|is running|is executing/i.test(q)) {
      return {
        ...(found || defaultEmailClaim()),
        text: 'An outbound email motion is already active.',
        kind: (found && found.kind) || 'inferred',
        support: (found && found.support) || 'unknown',
      };
    }
    return found || defaultEmailClaim();
  }
  if (/mail(?:ed)?|august|postage/i.test(q) && !/email/i.test(q)) {
    return (
      premises.find((p) => p.topic === 'mail') || {
        id: 'mail',
        topic: 'mail',
        text: 'Campaign 001 was mailed.',
      }
    );
  }
  if (/follow[- ]up/i.test(q)) {
    return premises.find((p) => p.topic === 'follow_up') || { id: 'follow_up', topic: 'follow_up' };
  }

  if (lastRecommendation && lastRecommendation.lastClaim) {
    return lastRecommendation.lastClaim;
  }
  const assertedActive = premises.find(looksLikeAssertedActiveEmail);
  if (assertedActive) return assertedActive;
  return premises.find((p) => p.topic && p.topic !== 'next_constraint') || premises[0] || defaultEmailClaim();
}

function evaluateEmailClaim(state) {
  const motion = state.emailMotion || {};
  if (motion.kind === 'active' && motion.current === true) {
    return {
      supported: true,
      verdict: 'confirmed',
      epistemic: 'verified',
      detail:
        'Current execution evidence records an outbound email motion in progress. That is distinct from historical sends or a planned mission.',
    };
  }
  if (motion.kind === 'historical') {
    return {
      supported: false,
      verdict: 'retract',
      epistemic: 'verified',
      detail:
        'Historical email touchpoints exist, but they do not establish that email outbound is currently active.',
    };
  }
  if (motion.kind === 'planned') {
    return {
      supported: false,
      verdict: 'retract',
      epistemic: 'verified',
      detail:
        'An email-related mission is on file as planned/intent work. A mission is not execution.',
    };
  }
  if (motion.kind === 'disabled') {
    return {
      supported: false,
      verdict: 'retract',
      epistemic: 'verified',
      detail:
        'Emmett exists but is disabled for this tenant. Disabled is not active.',
    };
  }
  return {
    supported: false,
    verdict: 'retract',
    epistemic: 'not_recorded',
    detail:
      'The evidence I retrieved establishes prospect inventory and historical activity, but neither proves current email execution.',
  };
}

function evaluateMailClaim(state) {
  if (state.mailAttestedAt) {
    return {
      supported: true,
      verdict: 'confirmed',
      epistemic: 'operator_attested',
      detail: `${state.campaignName} was operator-reported as physically mailed on ${state.mailAttestedAt}. Provenance: operator report (SPEC-106). That is operator-attested, not independently system-observed.`,
    };
  }
  if (state.mailExecuted) {
    return {
      supported: true,
      verdict: 'confirmed',
      epistemic: 'verified',
      detail: `System-verified evidence records that ${state.campaignName} was mailed.`,
    };
  }
  return {
    supported: false,
    verdict: 'retract',
    epistemic: 'not_recorded',
    detail: `I cannot verify that ${state.campaignName} was mailed.`,
  };
}

function evaluateFollowUpClaim(state) {
  const follow = state.followUp || {};
  if (follow.kind === 'completed') {
    return {
      supported: true,
      verdict: 'confirmed',
      epistemic: 'verified',
      detail: `Follow-up on ${state.campaignName} has recorded execution evidence.`,
    };
  }
  if (follow.expectedAt) {
    return {
      supported: true,
      verdict: 'qualified',
      epistemic: 'planned',
      detail: `Follow-up was planned/expected ${follow.expectedAt}. That is not recorded execution.`,
    };
  }
  return {
    supported: false,
    verdict: 'retract',
    epistemic: 'not_recorded',
    detail: 'I cannot verify that follow-up is underway.',
  };
}

function evaluateClaim(claim, state) {
  const topic = claim && claim.topic;
  if (topic === 'mail') return evaluateMailClaim(state);
  if (topic === 'follow_up') return evaluateFollowUpClaim(state);
  return evaluateEmailClaim(state);
}

function composeChallengeAnswer({ claim, evaluation, revised, correction }) {
  const claimText = present(claim && claim.text) || 'that statement';
  const parts = [];

  if (correction) {
    parts.push(
      'I accept that as an operator-attested correction of my working model. I will not continue asserting that outbound email is currently active. This does not invent a new durable operating event.'
    );
  }

  if (evaluation.verdict === 'confirmed') {
    parts.push(`The challenged claim was: ${claimText}`);
    parts.push(evaluation.detail);
    parts.push('I am confirming that claim from retrieved evidence, not from my prior wording.');
  } else if (evaluation.verdict === 'qualified') {
    parts.push(`The challenged claim was: ${claimText}`);
    parts.push(evaluation.detail);
    parts.push('I am qualifying that claim: planned/expected is not completed execution.');
  } else {
    parts.push(`The challenged claim was: ${claimText}`);
    if (/email|outbound/i.test(claimText) && (claim && claim.topic === 'email_motion')) {
      parts.push(
        `I can't verify that an outbound email motion is currently active. ${evaluation.detail} I retract that statement.`
      );
    } else {
      parts.push(`I can't verify that claim. ${evaluation.detail}`);
      parts.push('I retract that statement.');
    }
  }

  if (revised && evaluation.verdict === 'retract') {
    parts.push('');
    parts.push('REVISED RECOMMENDATION');
    parts.push(revised.prose);
  }

  return parts.join('\n');
}

function recordWorkingModel(sessionCtx, extras = {}) {
  if (!sessionCtx || typeof sessionCtx !== 'object') return;
  if (extras.lastRecommendation) {
    sessionCtx.lastRecommendation = extras.lastRecommendation;
  }
  if (Array.isArray(extras.retractedPremises)) {
    sessionCtx.retractedPremises = extras.retractedPremises;
  }
  if (extras.operatorDeniedEmailActive === true) {
    sessionCtx.operatorDeniedEmailActive = true;
  }
}

function recommendationRecord(composed) {
  if (!composed) return null;
  const premises = Array.isArray(composed.premises) ? composed.premises : [];
  return {
    premises,
    lastClaim: composed.lastClaim || premises.find((p) => p.topic !== 'next_constraint') || premises[0] || null,
    recommendation: composed.decision && (composed.decision.focus || composed.decision.recommendation),
    prose: composed.prose,
    decision: composed.decision || null,
  };
}

function handleClaimChallenge({
  question,
  state,
  lastRecommendation,
  correction,
  revised,
}) {
  const claim = identifyChallengedClaim(question, lastRecommendation) || defaultEmailClaim();
  const evaluation = evaluateClaim(claim, state);
  const prose = composeChallengeAnswer({
    claim,
    evaluation,
    revised: evaluation.verdict === 'retract' || correction ? revised : null,
    correction,
  });
  return { claim, evaluation, prose };
}

module.exports = {
  CLAIM_CHALLENGE_RE,
  isClaimChallenge,
  isOperatorClaimCorrection,
  lastRecommendationFrom,
  retractedIdsFrom,
  identifyChallengedClaim,
  evaluateClaim,
  composeChallengeAnswer,
  recordWorkingModel,
  recommendationRecord,
  handleClaimChallenge,
  defaultEmailClaim,
};
