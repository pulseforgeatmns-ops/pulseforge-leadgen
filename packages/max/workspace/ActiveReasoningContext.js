'use strict';

/**
 * SPEC-154 — Active Reasoning Context (ARC).
 *
 * Tracks the current chain of reasoning across conversation turns.
 * Follow-up questions bind to the active primaryClaim — not a fresh retrieval.
 */

const { REASONING_GOALS } = require('../reasoning/ConceptGraph/ConceptPlanner');
const { CONVERSATION_SUBJECTS } = require('./ConversationSubject');
const { OPERATING_MODEL, getRelationship } = require('../identity/OperatingModel');
const { joinSentences } = require('../reasoning/ConceptGraph/ConceptReasoner');

const REASONING_CHAIN_NODES = Object.freeze({
  IDENTITY: 'identity',
  PURPOSE: 'purpose',
  SPECIALIZATION: 'specialization',
  GOVERNANCE: 'governance',
  AUTHORITY: 'authority',
  BOUNDARIES: 'boundaries',
  RELATIONSHIPS: 'relationships',
  FAILURE_MODES: 'failure_modes',
  COMPARISON: 'comparison',
});

const FOLLOW_UP_TYPES = Object.freeze({
  WHY: 'why',
  HOW: 'how',
  COMPARE: 'compare',
  CHALLENGE: 'challenge',
  WHY_NOT: 'why_not',
  THEN_WHAT: 'then_what',
  EXPLAIN: 'explain',
});

const GOAL_TO_CLAIM = Object.freeze({
  [REASONING_GOALS.EXPLAIN_IDENTITY]:
    'Max coordinates specialists as the business operating system.',
  [REASONING_GOALS.EXPLAIN_AUTHORITY]:
    'The operator retains final authority.',
  [REASONING_GOALS.COMPARE_ROLES]:
    'Max integrates whole-business outcomes; specialists optimize within their domain.',
  [REASONING_GOALS.RESOLVE_CONFLICT]:
    'When specialists disagree, neither wins by default — the operator retains final authority.',
  [REASONING_GOALS.EXPLAIN_BOUNDARIES]:
    'Certain responsibilities never belong to Max.',
  [REASONING_GOALS.EXPLAIN_FAILURE_MODES]:
    'Operator judgment overrides Max when evidence is thin or priorities conflict.',
  [REASONING_GOALS.EXPLAIN_RELATIONSHIPS]:
    'Specialists serve different layers of the operating system; Max coordinates both.',
  [REASONING_GOALS.EXPLAIN_SPECIALIZATION]:
    'No single specialist sees the entire business.',
  [REASONING_GOALS.EXPLAIN_DEPENDENCY]:
    'Discovery and communication depend on each other through Max.',
});

const GOAL_TO_CHAIN_NODE = Object.freeze({
  [REASONING_GOALS.EXPLAIN_IDENTITY]: REASONING_CHAIN_NODES.IDENTITY,
  [REASONING_GOALS.EXPLAIN_AUTHORITY]: REASONING_CHAIN_NODES.AUTHORITY,
  [REASONING_GOALS.COMPARE_ROLES]: REASONING_CHAIN_NODES.COMPARISON,
  [REASONING_GOALS.RESOLVE_CONFLICT]: REASONING_CHAIN_NODES.GOVERNANCE,
  [REASONING_GOALS.EXPLAIN_BOUNDARIES]: REASONING_CHAIN_NODES.BOUNDARIES,
  [REASONING_GOALS.EXPLAIN_FAILURE_MODES]: REASONING_CHAIN_NODES.FAILURE_MODES,
  [REASONING_GOALS.EXPLAIN_RELATIONSHIPS]: REASONING_CHAIN_NODES.RELATIONSHIPS,
  [REASONING_GOALS.EXPLAIN_SPECIALIZATION]: REASONING_CHAIN_NODES.SPECIALIZATION,
  [REASONING_GOALS.EXPLAIN_DEPENDENCY]: REASONING_CHAIN_NODES.RELATIONSHIPS,
});

const DEFAULT_ASSUMPTIONS = Object.freeze([
  'Specialists optimize local objectives within their domain.',
  'Business optimization requires integration across competing priorities.',
  'The operator owns business accountability and final decisions.',
]);

const GOAL_CHANGE_RES = [
  /\b(?:enough about|stop talking about|let'?s stop talking about)\b/i,
  /\b(?:let'?s talk about|switch to|change subject to|move on to)\b/i,
  /\b(?:different topic|new topic|unrelated topic)\b/i,
  /\bback to\b/i,
];

const ARC_FOLLOWUP_RES = Object.freeze({
  [FOLLOW_UP_TYPES.WHY]: [/^why\b/i, /\bwhy (?:that|this|it)\b/i],
  [FOLLOW_UP_TYPES.HOW]: [/^how\b/i, /\bhow (?:does|do|would|is|are)\b/i],
  [FOLLOW_UP_TYPES.COMPARE]: [
    /\b(?:different from|differs from|compare(?:d)? to|compared to|vs\.?|versus)\b/i,
    /\bhow is (?:that|this|it) different\b/i,
  ],
  [FOLLOW_UP_TYPES.CHALLENGE]: [
    /\bwhat if\b/i,
    /\bsuppose\b/i,
    /\bif .* (?:disagree|became|were smarter)\b/i,
  ],
  [FOLLOW_UP_TYPES.WHY_NOT]: [/\bwhy not\b/i, /\bwhy shouldn'?t\b/i],
  [FOLLOW_UP_TYPES.THEN_WHAT]: [/\bthen what\b/i, /\bwhat happens next\b/i],
  [FOLLOW_UP_TYPES.EXPLAIN]: [/^explain\b/i, /^tell me more\b/i, /^go on\b/i],
});

function normalizeText(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function capitalizeFirst(text) {
  const s = normalizeText(text);
  if (!s) return s;
  return s.charAt(0).toUpperCase() + s.slice(1);
}

function matchesAny(text, patterns) {
  return patterns.some((re) => re.test(text));
}

function getActiveReasoningContext(session) {
  if (session && session.activeReasoningContext && typeof session.activeReasoningContext === 'object') {
    return session.activeReasoningContext;
  }
  const ctx = session && session.context && typeof session.context === 'object' ? session.context : null;
  const arc = ctx && ctx.activeReasoningContext;
  if (!arc || typeof arc !== 'object') return null;
  return arc;
}

function setActiveReasoningContext(session, arc) {
  if (!session || typeof session !== 'object') return;
  session.activeReasoningContext = arc;
  if (session.context && typeof session.context === 'object') {
    session.context.activeReasoningContext = arc;
  }
}

function getArchivedReasoningContexts(session) {
  const ctx = session && session.context && typeof session.context === 'object' ? session.context : null;
  if (!ctx || !ctx.archivedReasoningContexts || typeof ctx.archivedReasoningContexts !== 'object') {
    return {};
  }
  return ctx.archivedReasoningContexts;
}

function archiveReasoningContext(session, arc) {
  if (!session || !session.context || !arc || !arc.conversationGoal) return;
  const archived = getArchivedReasoningContexts(session);
  archived[arc.conversationGoal] = { ...arc, archivedAt: new Date().toISOString() };
  session.context.archivedReasoningContexts = archived;
}

function inferConversationGoal(subject, goal) {
  if (subject === CONVERSATION_SUBJECTS.IDENTITY) {
    if (goal === REASONING_GOALS.RESOLVE_CONFLICT) {
      return 'Understand specialist governance and conflict resolution.';
    }
    if (goal === REASONING_GOALS.EXPLAIN_AUTHORITY) {
      return 'Understand authority and decision ownership.';
    }
    return "Understand Max's operating model.";
  }
  return `Understand ${subject || 'the current topic'}.`;
}

function supportingClaimsForGoal(goal, specialists = []) {
  const claims = [];
  switch (goal) {
    case REASONING_GOALS.EXPLAIN_IDENTITY:
      claims.push(
        capitalizeFirst(OPERATING_MODEL.why[0]),
        capitalizeFirst(OPERATING_MODEL.why[2])
      );
      break;
    case REASONING_GOALS.COMPARE_ROLES:
      for (const name of specialists.slice(0, 2)) {
        const rel = getRelationship(name);
        if (rel) claims.push(`${capitalizeFirst(name)} specializes in ${rel.owns.toLowerCase()}`);
      }
      claims.push('Max synthesizes competing evidence across the whole business.');
      break;
    case REASONING_GOALS.RESOLVE_CONFLICT:
      for (const name of (specialists.length ? specialists : ['scout', 'paige']).slice(0, 2)) {
        const rel = getRelationship(name);
        if (rel) claims.push(`${capitalizeFirst(name)} optimizes ${rel.optimizes.toLowerCase()}`);
      }
      claims.push('Max synthesizes domain evidence against mission priorities.');
      claims.push('No specialist owns whole-business context.');
      break;
    case REASONING_GOALS.EXPLAIN_AUTHORITY:
      claims.push('Specialists execute within their domain.');
      claims.push('Max advises and synthesizes — the operator decides.');
      break;
    case REASONING_GOALS.EXPLAIN_SPECIALIZATION:
      claims.push(...OPERATING_MODEL.why.slice(0, 3).map(capitalizeFirst));
      break;
    default:
      if (OPERATING_MODEL.principles[1]) {
        claims.push(capitalizeFirst(OPERATING_MODEL.principles[1]));
      }
  }
  return claims.filter(Boolean);
}

function openQuestionsForGoal(goal) {
  switch (goal) {
    case REASONING_GOALS.RESOLVE_CONFLICT:
      return ['What happens during specialist conflict?', 'Should Max ever override the operator?'];
    case REASONING_GOALS.EXPLAIN_AUTHORITY:
      return ['When would Max disagree with the operator?'];
    case REASONING_GOALS.COMPARE_ROLES:
      return ['Why should specialists remain separate from Max?'];
    default:
      return [];
  }
}

function claimForGoal(goal, specialists = []) {
  return GOAL_TO_CLAIM[goal] || GOAL_TO_CLAIM[REASONING_GOALS.EXPLAIN_IDENTITY];
}

function chainNodeForGoal(goal) {
  return GOAL_TO_CHAIN_NODE[goal] || REASONING_CHAIN_NODES.IDENTITY;
}

function createInitialArc(input = {}) {
  const goal = input.goal || REASONING_GOALS.EXPLAIN_IDENTITY;
  const subject = input.subject || CONVERSATION_SUBJECTS.IDENTITY;
  const specialists = input.specialists || [];
  const now = new Date().toISOString();

  return {
    primaryClaim: input.primaryClaim || claimForGoal(goal, specialists),
    supportingClaims: input.supportingClaims || supportingClaimsForGoal(goal, specialists),
    assumptions: [...DEFAULT_ASSUMPTIONS],
    openQuestions: openQuestionsForGoal(goal),
    conversationGoal: input.conversationGoal || inferConversationGoal(subject, goal),
    reasoningChain: [chainNodeForGoal(goal)],
    confidence: input.confidence || 0.9,
    createdAt: now,
    updatedAt: now,
    goal,
    subject,
  };
}

function detectConversationGoalChange(question, priorArc, priorState) {
  const q = normalizeText(question);
  if (!q) return { changed: false };

  if (/\bback to\b/i.test(q) && /\b(?:max|operating model|identity)\b/i.test(q)) {
    return { changed: true, restore: true, newGoal: "Understand Max's operating model." };
  }

  if (!matchesAny(q, GOAL_CHANGE_RES)) {
    return { changed: false };
  }

  if (/\b(?:anchor|hiring|scout|paige|emmett|mission|client)\b/i.test(q)) {
    let newGoal = 'Understand the new topic.';
    if (/\banchor\b/i.test(q)) newGoal = 'Understand Anchor Cleaning.';
    if (/\bhiring\b/i.test(q)) newGoal = 'Understand hiring decisions.';
    if (/\bscout\b/i.test(q)) newGoal = "Understand Scout's role.";
    return { changed: true, reset: true, newGoal };
  }

  if (priorState && priorArc && priorState.subject && priorState.subject !== priorArc.subject) {
    return { changed: true, reset: true, newGoal: inferConversationGoal(priorState.subject) };
  }

  return { changed: true, reset: true, newGoal: null };
}

function classifyArcFollowUp(question) {
  const q = normalizeText(question);
  if (!q) return null;

  // Order matters — why_not before why.
  const orderedTypes = [
    FOLLOW_UP_TYPES.WHY_NOT,
    FOLLOW_UP_TYPES.CHALLENGE,
    FOLLOW_UP_TYPES.THEN_WHAT,
    FOLLOW_UP_TYPES.COMPARE,
    FOLLOW_UP_TYPES.HOW,
    FOLLOW_UP_TYPES.WHY,
    FOLLOW_UP_TYPES.EXPLAIN,
  ];

  for (const type of orderedTypes) {
    const patterns = ARC_FOLLOWUP_RES[type];
    if (patterns && matchesAny(q, patterns)) {
      return { type, question: q };
    }
  }

  return null;
}

function shouldBindFollowUpToArc(question, continuityApplied) {
  const q = normalizeText(question);
  const followUp = classifyArcFollowUp(question);
  if (!followUp) return false;

  const tokenCount = q.split(/\s+/).filter(Boolean).length;

  // Substantive compare advances the claim — concept graph owns the turn.
  if (
    followUp.type === FOLLOW_UP_TYPES.COMPARE &&
    /\b(?:scout|paige|emmett|rex|sam|riley|cal|vera|max)\b/i.test(q) &&
    tokenCount > 4
  ) {
    return false;
  }

  // Bare follow-ups always bind to the active proposition.
  if (tokenCount <= 4) return true;

  // Governance challenges bind even when longer.
  if (followUp.type === FOLLOW_UP_TYPES.WHY_NOT && /\b(?:scout|paige|decide|approve)\b/i.test(q)) {
    if (/\b(?:do your job|replace you|replace max|merge scout)\b/i.test(q)) {
      return false;
    }
    return true;
  }

  return Boolean(continuityApplied && tokenCount <= 6);
}

function isArcFollowUpQuestion(question) {
  return Boolean(classifyArcFollowUp(question));
}

function parseArcResolvedQuestion(resolvedQuestion) {
  const rq = normalizeText(resolvedQuestion);
  const claimWhy = rq.match(/^claim_why\(([^)]+)\)$/i);
  if (claimWhy) {
    return { kind: 'claim_why', node: claimWhy[1].toLowerCase() };
  }
  const claimHow = rq.match(/^claim_how\(([^)]+)\)$/i);
  if (claimHow) {
    return { kind: 'claim_how', node: claimHow[1].toLowerCase() };
  }
  const claimCompare = rq.match(/^claim_compare\(([^)]+)\)$/i);
  if (claimCompare) {
    return {
      kind: 'claim_compare',
      objects: claimCompare[1].split(',').map((o) => o.trim().toLowerCase()).filter(Boolean),
    };
  }
  const claimChallenge = rq.match(/^claim_challenge\(([^)]+)\)$/i);
  if (claimChallenge) {
    return { kind: 'claim_challenge', node: claimChallenge[1].toLowerCase() };
  }
  const claimWhyNot = rq.match(/^claim_why_not\(([^)]+)\)$/i);
  if (claimWhyNot) {
    return { kind: 'claim_why_not', node: claimWhyNot[1].toLowerCase() };
  }
  return null;
}

function buildResolvedQuestionFromArc(question, arc, followUp) {
  const node =
    (arc.reasoningChain && arc.reasoningChain[arc.reasoningChain.length - 1]) ||
    REASONING_CHAIN_NODES.IDENTITY;

  switch (followUp.type) {
    case FOLLOW_UP_TYPES.WHY:
      return `claim_why(${node})`;
    case FOLLOW_UP_TYPES.HOW:
      return `claim_how(${node})`;
    case FOLLOW_UP_TYPES.COMPARE:
      return `claim_compare(${node},specialist)`;
    case FOLLOW_UP_TYPES.CHALLENGE:
      return `claim_challenge(${node})`;
    case FOLLOW_UP_TYPES.WHY_NOT:
      return `claim_why_not(${node})`;
    case FOLLOW_UP_TYPES.THEN_WHAT:
      return `claim_how(${node})`;
    case FOLLOW_UP_TYPES.EXPLAIN:
      return `claim_why(${node})`;
    default:
      return `claim_why(${node})`;
  }
}

/**
 * Apply ARC binding after conversational continuity.
 * Follow-ups bind to primaryClaim before conversation topic.
 */
function applyActiveReasoningContinuity(input = {}) {
  const question = normalizeText(input.question);
  const session = input.session || null;
  const priorArc = getActiveReasoningContext(session);
  const priorState = input.priorState || null;
  const continuityApplied = Boolean(input.continuityApplied);
  const conversationContract = input.conversationContract || null;
  const contractGoal =
    conversationContract && conversationContract.conversationGoal
      ? conversationContract.conversationGoal
      : null;

  const goalChange = detectConversationGoalChange(question, priorArc, priorState);
  if (goalChange.changed) {
    if (goalChange.restore && session) {
      const archived = getArchivedReasoningContexts(session);
      const restored = archived["Understand Max's operating model."];
      if (restored) {
        setActiveReasoningContext(session, restored);
        return {
          applied: false,
          goalChanged: true,
          restored: true,
          activeReasoningContext: restored,
        };
      }
    }
    if (goalChange.reset && priorArc && session) {
      archiveReasoningContext(session, priorArc);
      setActiveReasoningContext(session, null);
    }
    return {
      applied: false,
      goalChanged: true,
      reset: true,
      activeReasoningContext: null,
      newConversationGoal: goalChange.newGoal,
    };
  }

  const followUp = classifyArcFollowUp(question);
  if (!followUp || !priorArc || !priorArc.primaryClaim) {
    return {
      applied: false,
      activeReasoningContext: priorArc,
    };
  }

  const shouldBind = shouldBindFollowUpToArc(question, continuityApplied);

  if (!shouldBind) {
    return {
      applied: false,
      activeReasoningContext: priorArc,
      conversationGoal: contractGoal || (priorArc && priorArc.conversationGoal),
    };
  }

  const resolvedQuestion = buildResolvedQuestionFromArc(question, priorArc, followUp);

  return {
    applied: true,
    activeReasoningContext: priorArc,
    arcFollowUp: followUp,
    resolvedQuestion,
    bindToClaim: priorArc.primaryClaim,
    conversationGoal: contractGoal || priorArc.conversationGoal,
  };
}

function computeArcDelta(input = {}) {
  const priorArc = input.priorArc || null;
  const goal = input.goal || REASONING_GOALS.EXPLAIN_IDENTITY;
  const specialists = input.specialists || [];
  const subject = input.subject || CONVERSATION_SUBJECTS.IDENTITY;
  const followUp = input.arcFollowUp || null;
  const question = normalizeText(input.question);

  const newClaim = claimForGoal(goal, specialists);
  const chainNode = chainNodeForGoal(goal);
  const supporting = supportingClaimsForGoal(goal, specialists);

  if (!priorArc) {
    return {
      primaryClaim: newClaim,
      supportingClaims: supporting,
      assumptions: [...DEFAULT_ASSUMPTIONS],
      openQuestions: openQuestionsForGoal(goal),
      conversationGoal: inferConversationGoal(subject, goal),
      reasoningChain: [chainNode],
      confidence: 0.9,
      goal,
      subject,
      questionsResolved: [],
      questionsOpened: openQuestionsForGoal(goal),
    };
  }

  const delta = {
    primaryClaim: newClaim,
    supportingClaims: supporting,
    questionsResolved: [],
    questionsOpened: [],
    confidence: Math.min(0.98, (priorArc.confidence || 0.85) + 0.02),
    reasoningChainNode: chainNode,
    goal,
    subject,
  };

  if (followUp && followUp.type === FOLLOW_UP_TYPES.WHY) {
    delta.questionsResolved.push(`Why: ${priorArc.primaryClaim}`);
    delta.primaryClaim = priorArc.primaryClaim;
    delta.reasoningChainNode = null;
  } else if (followUp && followUp.type === FOLLOW_UP_TYPES.COMPARE) {
    delta.primaryClaim = newClaim;
    delta.supportingClaims = mergeSupporting(priorArc.primaryClaim, supporting);
    delta.priorClaimDemoted = priorArc.primaryClaim;
  } else if (followUp && (followUp.type === FOLLOW_UP_TYPES.CHALLENGE || followUp.type === FOLLOW_UP_TYPES.WHY_NOT)) {
    delta.primaryClaim = priorArc.primaryClaim;
    delta.reasoningChainNode = REASONING_CHAIN_NODES.GOVERNANCE;
  } else if (newClaim !== priorArc.primaryClaim) {
    delta.priorClaimDemoted = priorArc.primaryClaim;
  }

  if (/\bwho decides\b/i.test(question)) {
    delta.primaryClaim = GOAL_TO_CLAIM[REASONING_GOALS.EXPLAIN_AUTHORITY];
    delta.reasoningChainNode = REASONING_CHAIN_NODES.AUTHORITY;
    delta.questionsResolved.push('Who decides?');
  }

  if (/\bwhen would you disagree\b/i.test(question)) {
    delta.primaryClaim = GOAL_TO_CLAIM[REASONING_GOALS.EXPLAIN_FAILURE_MODES];
    delta.reasoningChainNode = REASONING_CHAIN_NODES.FAILURE_MODES;
    delta.openQuestions = ['When would Max disagree with the operator?'];
  }

  return delta;
}

function mergeSupporting(priorClaim, nextSupporting) {
  const merged = [priorClaim, ...(nextSupporting || [])];
  const seen = new Set();
  return merged.filter((claim) => {
    const key = normalizeText(claim).toLowerCase();
    if (!key || seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function applyArcDelta(priorArc, delta) {
  if (!delta) return priorArc;

  if (!priorArc) {
    return createInitialArc(delta);
  }

  const reasoningChain = [...(priorArc.reasoningChain || [])];
  if (delta.reasoningChainNode && !reasoningChain.includes(delta.reasoningChainNode)) {
    reasoningChain.push(delta.reasoningChainNode);
  }

  const supportingClaims = mergeSupporting(
    delta.priorClaimDemoted || null,
    delta.supportingClaims || priorArc.supportingClaims
  );

  const openQuestions = [...(priorArc.openQuestions || [])];
  for (const resolved of delta.questionsResolved || []) {
    const idx = openQuestions.findIndex((q) => normalizeText(q).toLowerCase().includes(normalizeText(resolved).toLowerCase().slice(0, 20)));
    if (idx >= 0) openQuestions.splice(idx, 1);
  }
  for (const opened of delta.questionsOpened || delta.openQuestions || []) {
    if (!openQuestions.includes(opened)) openQuestions.push(opened);
  }

  return {
    ...priorArc,
    primaryClaim: delta.primaryClaim || priorArc.primaryClaim,
    supportingClaims,
    assumptions: priorArc.assumptions || [...DEFAULT_ASSUMPTIONS],
    openQuestions,
    conversationGoal: delta.conversationGoal || priorArc.conversationGoal,
    reasoningChain,
    confidence: delta.confidence || priorArc.confidence,
    goal: delta.goal || priorArc.goal,
    subject: delta.subject || priorArc.subject,
    updatedAt: new Date().toISOString(),
  };
}

function synthesizeWhyFromArc(arc) {
  const claim = arc.primaryClaim;
  const parts = [claim];

  if (claim.includes('operator retains final authority')) {
    parts.push(
      'Neither Scout nor Paige has visibility across the entire business — each optimizes one domain.',
      'Allowing either to make final decisions independently would optimize toward one objective instead of balancing competing objectives.',
      'My role is to surface those tradeoffs, but because the business belongs to you, final authority remains yours.'
    );
  } else if (claim.includes('integrates') || claim.includes('specializ')) {
    parts.push(
      capitalizeFirst(OPERATING_MODEL.why[0]),
      capitalizeFirst(OPERATING_MODEL.why[2]),
      capitalizeFirst(OPERATING_MODEL.principles.find((p) => p.startsWith('Delegate expertise')))
    );
  } else if (claim.includes('operating system')) {
    parts.push(
      ...OPERATING_MODEL.why.slice(0, 4).map(capitalizeFirst),
      'My purpose is to ' +
        OPERATING_MODEL.purpose.slice(0, 3).join(', ').toLowerCase().replace(/,\s([^,]+)$/, ', and $1') +
        '.'
    );
  } else if (claim.includes('never belong')) {
    parts.push(...OPERATING_MODEL.boundaries.slice(0, 3).map(capitalizeFirst));
  } else if (claim.includes('judgment overrides')) {
    parts.push(...OPERATING_MODEL.failureModes.map(capitalizeFirst));
  } else {
    for (const supporting of (arc.supportingClaims || []).slice(0, 3)) {
      parts.push(supporting);
    }
    parts.push(
      capitalizeFirst(OPERATING_MODEL.principles.find((p) => p.startsWith('Operator retains')))
    );
  }

  return joinSentences(parts);
}

function synthesizeHowFromArc(arc) {
  const parts = [arc.primaryClaim];
  for (const supporting of (arc.supportingClaims || []).slice(0, 4)) {
    parts.push(supporting);
  }
  parts.push(
    'Max synthesizes domain evidence, explains tradeoffs, and presents options — the operator chooses.'
  );
  return joinSentences(parts);
}

function synthesizeCompareFromArc(arc, question) {
  const q = normalizeText(question).toLowerCase();
  const specialistMatch = q.match(/\b(scout|paige|emmett|rex|sam|riley|cal|vera)\b/i);
  const specialist = specialistMatch ? specialistMatch[1].toLowerCase() : 'scout';
  const rel = getRelationship(specialist);
  const parts = [arc.primaryClaim];

  if (rel) {
    parts.push(
      `${capitalizeFirst(specialist)} owns ${rel.owns.charAt(0).toLowerCase()}${rel.owns.slice(1)}`,
      capitalizeFirst(rel.reasoning),
      `Max optimizes ${OPERATING_MODEL.relationships.max.optimizes.charAt(0).toLowerCase()}${OPERATING_MODEL.relationships.max.optimizes.slice(1)}`
    );
  }

  return joinSentences(parts);
}

function synthesizeChallengeFromArc(arc, question) {
  const q = normalizeText(question).toLowerCase();
  const parts = [arc.primaryClaim];

  if (/\bscout.*(?:smarter|disagree|became)\b/i.test(q) || /\bdisagree\b/i.test(q)) {
    parts.push(
      'Even if a specialist produced stronger domain evidence, whole-business integration still requires balancing competing objectives.',
      'Max would synthesize the stronger signal, explain the tradeoff, and defer the final call to you.',
      capitalizeFirst(OPERATING_MODEL.principles.find((p) => p.startsWith('Operator retains')))
    );
  } else {
    parts.push(
      'Counterfactuals are evaluated against the same governance frame — specialists advise within their domain; you decide.',
      capitalizeFirst(OPERATING_MODEL.principles.find((p) => p.startsWith('Fail closed')))
    );
  }

  return joinSentences(parts);
}

function synthesizeWhyNotFromArc(arc) {
  return joinSentences([
    arc.primaryClaim,
    'Letting a specialist decide would optimize one domain at the expense of the whole business.',
    capitalizeFirst(OPERATING_MODEL.why[0]),
    capitalizeFirst(OPERATING_MODEL.principles.find((p) => p.startsWith('Operator retains'))),
  ]);
}

/**
 * Synthesize prose from the active proposition — not a fresh identity retrieval.
 */
function synthesizeFromArc(arc, followUpType, question = '') {
  if (!arc || !arc.primaryClaim) return null;

  switch (followUpType) {
    case FOLLOW_UP_TYPES.WHY:
    case FOLLOW_UP_TYPES.EXPLAIN:
      return synthesizeWhyFromArc(arc);
    case FOLLOW_UP_TYPES.HOW:
    case FOLLOW_UP_TYPES.THEN_WHAT:
      return synthesizeHowFromArc(arc);
    case FOLLOW_UP_TYPES.COMPARE:
      return synthesizeCompareFromArc(arc, question);
    case FOLLOW_UP_TYPES.CHALLENGE:
      return synthesizeChallengeFromArc(arc, question);
    case FOLLOW_UP_TYPES.WHY_NOT:
      return synthesizeWhyNotFromArc(arc);
    default:
      return synthesizeWhyFromArc(arc);
  }
}

function advanceActiveReasoningContext(session, turn = {}) {
  if (!session) return null;

  const priorArc = getActiveReasoningContext(session);
  const meta = (turn.structured && turn.structured.metadata) || null;
  const omr = meta && meta.operatingModelReasoning;

  const goal =
    (omr && omr.goal) ||
    (meta && meta.goal) ||
    (turn.activeReasoningDelta && turn.activeReasoningDelta.goal) ||
    (turn.conversationSubject &&
    turn.conversationSubject.subject === CONVERSATION_SUBJECTS.IDENTITY
      ? REASONING_GOALS.EXPLAIN_IDENTITY
      : REASONING_GOALS.EXPLAIN_IDENTITY);

  const conceptSource =
    (omr && omr.concepts) ||
    (meta && meta.activeConcepts) ||
    (meta && meta.concepts) ||
    [];

  const specialists = conceptSource.filter((c) =>
    ['scout', 'paige', 'emmett', 'rex', 'sam', 'riley', 'cal', 'vera'].includes(c)
  );

  const delta = computeArcDelta({
    priorArc,
    goal,
    specialists,
    subject: turn.conversationSubject && turn.conversationSubject.subject,
    arcFollowUp: turn.arcFollowUp || null,
    question: turn.question,
  });

  const nextArc = applyArcDelta(priorArc, delta);
  setActiveReasoningContext(session, nextArc);
  return nextArc;
}

module.exports = {
  REASONING_CHAIN_NODES,
  FOLLOW_UP_TYPES,
  GOAL_TO_CLAIM,
  getActiveReasoningContext,
  setActiveReasoningContext,
  createInitialArc,
  detectConversationGoalChange,
  classifyArcFollowUp,
  isArcFollowUpQuestion,
  parseArcResolvedQuestion,
  buildResolvedQuestionFromArc,
  applyActiveReasoningContinuity,
  computeArcDelta,
  applyArcDelta,
  synthesizeFromArc,
  advanceActiveReasoningContext,
  claimForGoal,
  chainNodeForGoal,
  inferConversationGoal,
};
