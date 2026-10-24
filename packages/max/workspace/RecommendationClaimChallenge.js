'use strict';

/**
 * SPEC-107A / SPEC-108 — targeted claim challenge, retraction, and
 * working-model correction.
 *
 * Challenge handling is domain-general: identify the operating-state
 * claim, evaluate it against retrieved evidence, then confirm, qualify,
 * retract, or revise. Does not persist Max-generated statements as
 * operating facts. Session working model only.
 */

const {
  TOPICS,
  evaluateOperatingStateClaim,
  identifyClaimTopic,
  assertedTextForTopic,
  formatChallengeResponse,
} = require('./ClaimGrounding');

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
    String.raw`\bthat(?:'s| is) (?:wrong|incorrect|unsupported|not true)\b`,
    String.raw`\bthat isn'?t true\b`,
    String.raw`\bthat isn'?t (?:right|correct|accurate)\b`,
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
    id: TOPICS.EMAIL_MOTION,
    topic: TOPICS.EMAIL_MOTION,
    text: 'An outbound email motion is already active.',
    kind: 'inferred',
    support: 'unsupported',
  };
}

function looksLikeAssertedActiveEmail(premise) {
  if (!premise || premise.topic !== TOPICS.EMAIL_MOTION) return false;
  const text = String(premise.text || premise.claim || '');
  if (/cannot verify|not verify|do not establish|not currently active|not verified|disabled|planned or intended/i.test(text)) {
    return false;
  }
  return (
    premise.support === 'supported' ||
    /already active|currently active|is executing|is running/i.test(text)
  );
}

function premiseForTopic(premises, topic) {
  return (premises || []).find((p) => p && p.topic === topic);
}

function identifyChallengedClaim(question, lastRecommendation = null) {
  const premises = (lastRecommendation && lastRecommendation.premises) || [];
  const topic = identifyClaimTopic(question);
  const asserted = assertedTextForTopic(topic, question);

  if (topic) {
    const found = premiseForTopic(premises, topic);
    if (found) {
      return {
        ...found,
        text: asserted || found.text || found.claim,
      };
    }
    return {
      id: topic,
      topic,
      text: asserted || (topic === TOPICS.EMAIL_MOTION ? defaultEmailClaim().text : topic),
      kind: 'inferred',
      support: 'unsupported',
    };
  }

  if (lastRecommendation && lastRecommendation.lastClaim) {
    return lastRecommendation.lastClaim;
  }
  const assertedActive = premises.find(looksLikeAssertedActiveEmail);
  if (assertedActive) return assertedActive;
  return premises.find((p) => p.topic && p.topic !== 'next_constraint') || premises[0] || defaultEmailClaim();
}

function evaluateClaim(claim, state) {
  return evaluateOperatingStateClaim(claim, state);
}

function composeChallengeAnswer({ claim, evaluation, revised, correction }) {
  return formatChallengeResponse({ claim, evaluation, revised, correction });
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
