'use strict';

/**
 * SPEC-156 — Reasoning Operator Engine (ROE).
 *
 * Selects and executes cognitive operations over Active Reasoning Context.
 * Reasoning is a transformation — not retrieval. Every response passes through
 * exactly one explicit operator before presentation (ADR-063).
 */

const { OPERATING_MODEL, getRelationship } = require('../identity/OperatingModel');
const { joinSentences } = require('../reasoning/ConceptGraph/ConceptReasoner');
const {
  FOLLOW_UP_TYPES,
  classifyArcFollowUp,
  getActiveReasoningContext,
} = require('./ActiveReasoningContext');

const OPERATOR_IDS = Object.freeze({
  EXPLAIN: 'explain',
  JUSTIFY: 'justify',
  SURFACE_ASSUMPTIONS: 'surface_assumptions',
  CHALLENGE: 'challenge',
  COUNTERFACTUAL: 'counterfactual',
  COMPARE: 'compare',
  CONTRAST: 'contrast',
  GENERALIZE: 'generalize',
  SPECIALIZE: 'specialize',
  EVALUATE: 'evaluate',
  SUMMARIZE: 'summarize',
  SYNTHESIZE: 'synthesize',
  REVISE: 'revise',
  REFLECT: 'reflect',
});

const OPERATOR_CATEGORIES = Object.freeze({
  ELABORATION: 'elaboration',
  CRITICAL: 'critical',
  STRUCTURAL: 'structural',
  META: 'meta',
});

const EXPLICIT_OPERATOR_PATTERNS = Object.freeze([
  { operator: OPERATOR_IDS.SURFACE_ASSUMPTIONS, patterns: [
    /\bwhat assumption/i,
    /\bwhich assumption/i,
    /\bunderlying assumption/i,
    /\bassumptions? (?:is|are|that|this) (?:based|built)/i,
  ]},
  { operator: OPERATOR_IDS.COUNTERFACTUAL, patterns: [
    /\bsuppose (?:that'?s|this|it) (?:wrong|failed|fails)/i,
    /\bwhat if (?:that|this|it) (?:failed|were wrong|is wrong)/i,
    /\bcould (?:that|it|this) (?:assumption )?fail/i,
    /\bif (?:that|it) failed/i,
  ]},
  { operator: OPERATOR_IDS.REVISE, patterns: [
    /\bdoes that change your conclusion/i,
    /\bwould that change your conclusion/i,
    /\bchange your (?:conclusion|view|position)/i,
    /\bdo you still (?:believe|think|hold)/i,
    /\brevise (?:your|the) (?:conclusion|claim|view)/i,
  ]},
  { operator: OPERATOR_IDS.REFLECT, patterns: [
    /\bearlier you said/i,
    /\bpreviously you (?:said|claimed|stated)/i,
    /\bhow has your reasoning/i,
    /\bexplain (?:the )?evolution/i,
  ]},
  { operator: OPERATOR_IDS.SUMMARIZE, patterns: [
    /\bsummarize (?:how|your|the)/i,
    /\bsum up (?:how|your|the)/i,
    /\bcompress (?:your|the) reasoning/i,
  ]},
  { operator: OPERATOR_IDS.GENERALIZE, patterns: [
    /\bgeneralize/i,
    /\bin general\b/i,
    /\bhigher (?:level|abstraction)/i,
    /\bwhat(?:'s| is) the broader (?:pattern|principle)/i,
  ]},
  { operator: OPERATOR_IDS.SPECIALIZE, patterns: [
    /\bgive (?:me )?(?:an )?example/i,
    /\bconcrete example/i,
    /\bbe more specific/i,
    /\bwhat would that look like/i,
  ]},
  { operator: OPERATOR_IDS.EVALUATE, patterns: [
    /\bevaluate (?:the|this|your)/i,
    /\bstrengths and weaknesses/i,
    /\bassess (?:the|this|your)/i,
  ]},
  { operator: OPERATOR_IDS.CHALLENGE, patterns: [
    /\bwhat(?:'s| is) wrong with/i,
    /\bweakness/i,
    /\bcounterargument/i,
    /\bchallenge (?:that|this|your)/i,
  ]},
  { operator: OPERATOR_IDS.COMPARE, patterns: [
    /\bcompared to\b/i,
    /\bcompare (?:to|with|against)\b/i,
    /\bvs\.?\b/i,
    /\bversus\b/i,
    /\bdifferent from\b/i,
  ]},
  { operator: OPERATOR_IDS.CONTRAST, patterns: [
    /\bcontrast (?:with|to|against)\b/i,
    /\bhow (?:is|are) (?:they|these) different\b/i,
  ]},
  { operator: OPERATOR_IDS.SYNTHESIZE, patterns: [
    /\bsynthesize/i,
    /\bmerge (?:these|the) (?:claims|points)/i,
    /\bcombine (?:these|the) (?:claims|points)/i,
  ]},
]);

const FOLLOWUP_TO_OPERATOR = Object.freeze({
  [FOLLOW_UP_TYPES.WHY]: OPERATOR_IDS.JUSTIFY,
  [FOLLOW_UP_TYPES.HOW]: OPERATOR_IDS.EXPLAIN,
  [FOLLOW_UP_TYPES.COMPARE]: OPERATOR_IDS.COMPARE,
  [FOLLOW_UP_TYPES.CHALLENGE]: OPERATOR_IDS.COUNTERFACTUAL,
  [FOLLOW_UP_TYPES.WHY_NOT]: OPERATOR_IDS.CHALLENGE,
  [FOLLOW_UP_TYPES.THEN_WHAT]: OPERATOR_IDS.EXPLAIN,
  [FOLLOW_UP_TYPES.EXPLAIN]: OPERATOR_IDS.EXPLAIN,
});

const CONTINUITY_OPERATOR_CHAIN = Object.freeze({
  [OPERATOR_IDS.EXPLAIN]: OPERATOR_IDS.JUSTIFY,
  [OPERATOR_IDS.JUSTIFY]: OPERATOR_IDS.JUSTIFY,
  [OPERATOR_IDS.SURFACE_ASSUMPTIONS]: OPERATOR_IDS.COUNTERFACTUAL,
  [OPERATOR_IDS.COUNTERFACTUAL]: OPERATOR_IDS.REVISE,
  [OPERATOR_IDS.COMPARE]: OPERATOR_IDS.GENERALIZE,
  [OPERATOR_IDS.GENERALIZE]: OPERATOR_IDS.SUMMARIZE,
  [OPERATOR_IDS.REFLECT]: OPERATOR_IDS.REVISE,
  [OPERATOR_IDS.CHALLENGE]: OPERATOR_IDS.COUNTERFACTUAL,
});

function normalizeText(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function capitalizeFirst(text) {
  const s = normalizeText(text);
  if (!s) return s;
  return s.charAt(0).toUpperCase() + s.slice(1);
}

function getReasoningHistory(session) {
  if (session && Array.isArray(session.reasoningHistory)) {
    return session.reasoningHistory;
  }
  if (session && session.context && Array.isArray(session.context.reasoningHistory)) {
    return session.context.reasoningHistory;
  }
  return [];
}

function appendReasoningHistory(session, entry) {
  if (!session) return;
  if (!Array.isArray(session.reasoningHistory)) {
    session.reasoningHistory = getReasoningHistory(session);
  }
  session.reasoningHistory.push({
    ...entry,
    timestamp: new Date().toISOString(),
  });
  if (session.context && typeof session.context === 'object') {
    session.context.reasoningHistory = session.reasoningHistory;
  }
}

function computeReasoningDepth(session, operatorId) {
  const history = getReasoningHistory(session);
  if (!history.length) {
    return operatorId === OPERATOR_IDS.EXPLAIN ? 0 : 1;
  }
  const last = history[history.length - 1];
  const lastDepth = typeof last.depth === 'number' ? last.depth : 0;

  if (operatorId === OPERATOR_IDS.EXPLAIN && lastDepth === 0) return 0;
  if (operatorId === OPERATOR_IDS.JUSTIFY) return lastDepth + 1;
  if (operatorId === OPERATOR_IDS.SURFACE_ASSUMPTIONS) return Math.max(lastDepth + 1, 3);
  if (operatorId === OPERATOR_IDS.COUNTERFACTUAL) return Math.max(lastDepth + 1, 4);
  if (operatorId === OPERATOR_IDS.REVISE) return Math.max(lastDepth + 1, 5);
  if (operatorId === OPERATOR_IDS.REFLECT) return lastDepth;
  return lastDepth + 1;
}

function getPriorTransformedClaim(session, arc) {
  const history = getReasoningHistory(session);
  if (history.length) {
    return history[history.length - 1].transformedClaim;
  }
  return arc && arc.primaryClaim ? arc.primaryClaim : null;
}

function detectExplicitOperator(question) {
  const q = normalizeText(question);
  for (const entry of EXPLICIT_OPERATOR_PATTERNS) {
    if (entry.patterns.some((re) => re.test(q))) {
      return entry.operator;
    }
  }
  return null;
}

function inferOperatorFromContinuity(session) {
  const history = getReasoningHistory(session);
  if (!history.length) return null;
  const lastOperator = history[history.length - 1].operator;
  return CONTINUITY_OPERATOR_CHAIN[lastOperator] || null;
}

/**
 * Dispatch the cognitive operator for this turn.
 * Priority: explicit operator → conversation continuity → ARC follow-up → default Explain.
 *
 * @param {object} input
 * @returns {{ operatorId: string, source: string, confidence: number }}
 */
function dispatchReasoningOperator(input = {}) {
  const question = normalizeText(input.question);
  const arcFollowUp = input.arcFollowUp || classifyArcFollowUp(question);
  const session = input.session || null;

  const explicit = detectExplicitOperator(question);
  if (explicit) {
    return { operatorId: explicit, source: 'explicit_operator', confidence: 0.95 };
  }

  const continuity = inferOperatorFromContinuity(session);
  if (continuity && arcFollowUp) {
    return { operatorId: continuity, source: 'conversation_continuity', confidence: 0.88 };
  }

  if (arcFollowUp && FOLLOWUP_TO_OPERATOR[arcFollowUp.type]) {
    return {
      operatorId: FOLLOWUP_TO_OPERATOR[arcFollowUp.type],
      source: 'arc_follow_up',
      confidence: 0.9,
    };
  }

  if (/^why\b/i.test(question)) {
    return { operatorId: OPERATOR_IDS.JUSTIFY, source: 'question_pattern', confidence: 0.85 };
  }

  return { operatorId: OPERATOR_IDS.EXPLAIN, source: 'default', confidence: 0.75 };
}

function buildOperatorMeta(operatorId) {
  const categoryMap = {
    [OPERATOR_IDS.EXPLAIN]: OPERATOR_CATEGORIES.ELABORATION,
    [OPERATOR_IDS.JUSTIFY]: OPERATOR_CATEGORIES.ELABORATION,
    [OPERATOR_IDS.SPECIALIZE]: OPERATOR_CATEGORIES.ELABORATION,
    [OPERATOR_IDS.GENERALIZE]: OPERATOR_CATEGORIES.ELABORATION,
    [OPERATOR_IDS.SURFACE_ASSUMPTIONS]: OPERATOR_CATEGORIES.CRITICAL,
    [OPERATOR_IDS.CHALLENGE]: OPERATOR_CATEGORIES.CRITICAL,
    [OPERATOR_IDS.COUNTERFACTUAL]: OPERATOR_CATEGORIES.CRITICAL,
    [OPERATOR_IDS.EVALUATE]: OPERATOR_CATEGORIES.CRITICAL,
    [OPERATOR_IDS.COMPARE]: OPERATOR_CATEGORIES.STRUCTURAL,
    [OPERATOR_IDS.CONTRAST]: OPERATOR_CATEGORIES.STRUCTURAL,
    [OPERATOR_IDS.SYNTHESIZE]: OPERATOR_CATEGORIES.STRUCTURAL,
    [OPERATOR_IDS.SUMMARIZE]: OPERATOR_CATEGORIES.STRUCTURAL,
    [OPERATOR_IDS.REVISE]: OPERATOR_CATEGORIES.META,
    [OPERATOR_IDS.REFLECT]: OPERATOR_CATEGORIES.META,
  };
  return {
    id: operatorId,
    category: categoryMap[operatorId] || OPERATOR_CATEGORIES.ELABORATION,
  };
}

function executeExplain(ctx) {
  const claim = ctx.inputClaim;
  const parts = [claim];
  for (const supporting of (ctx.arc.supportingClaims || []).slice(0, 3)) {
    parts.push(supporting);
  }
  if (ctx.depth === 0) {
    parts.push(
      capitalizeFirst(OPERATING_MODEL.why[0]),
      capitalizeFirst(OPERATING_MODEL.principles.find((p) => p.startsWith('Delegate expertise')))
    );
  } else {
    parts.push('This follows from the operating model design — specialists own domains; Max integrates.');
  }
  return {
    transformedClaim: joinSentences(parts),
    evidenceUsed: [claim, ...(ctx.arc.supportingClaims || []).slice(0, 2)],
    assumptions: ctx.arc.assumptions || [],
    confidence: 0.9,
  };
}

function executeJustify(ctx) {
  const claim = ctx.inputClaim;
  const parts = [];

  if (ctx.depth <= 1) {
    parts.push(`The claim "${claim}" holds because:`);
    if (claim.includes('operator retains final authority') || claim.includes('final authority')) {
      parts.push(
        'Neither Scout nor Paige has visibility across the entire business — each optimizes one domain.',
        'Allowing either to make final decisions independently would optimize toward one objective instead of balancing competing objectives.',
        'My role is to surface those tradeoffs, but because the business belongs to you, final authority remains yours.'
      );
    } else if (claim.includes('operating system') || claim.includes('coordinates specialists')) {
      parts.push(
        ...OPERATING_MODEL.why.slice(0, 4).map(capitalizeFirst),
        'My purpose is to ' +
          OPERATING_MODEL.purpose.slice(0, 3).join(', ').toLowerCase().replace(/,\s([^,]+)$/, ', and $1') +
          '.'
      );
    } else if (claim.includes('integrates') || claim.includes('specializ')) {
      parts.push(
        capitalizeFirst(OPERATING_MODEL.why[0]),
        capitalizeFirst(OPERATING_MODEL.why[2]),
        capitalizeFirst(OPERATING_MODEL.principles.find((p) => p.startsWith('Delegate expertise')))
      );
    } else {
      for (const supporting of (ctx.arc.supportingClaims || []).slice(0, 3)) {
        parts.push(supporting);
      }
      parts.push(
        capitalizeFirst(OPERATING_MODEL.principles.find((p) => p.startsWith('Operator retains')))
      );
    }
  } else {
    parts.push(`That justification is necessary because at depth ${ctx.depth}:`);
    parts.push(
      capitalizeFirst(OPERATING_MODEL.why[4] || OPERATING_MODEL.why[3]),
      'Without this layer, the reasoning would collapse to domain-local optimization.',
      capitalizeFirst(OPERATING_MODEL.principles.find((p) => p.startsWith('Business first')))
    );
  }

  return {
    transformedClaim: joinSentences(parts),
    evidenceUsed: [claim, ...(ctx.arc.supportingClaims || []).slice(0, 2)],
    assumptions: ctx.arc.assumptions || [],
    confidence: Math.min(0.95, 0.85 + ctx.depth * 0.02),
  };
}

function executeSurfaceAssumptions(ctx) {
  const claim = ctx.inputClaim;
  const assumptions = ctx.arc.assumptions && ctx.arc.assumptions.length
    ? [...ctx.arc.assumptions]
    : [
        'Specialists optimize local objectives within their domain.',
        'Business optimization requires integration across competing priorities.',
        'The operator owns business accountability and final decisions.',
      ];

  const parts = [
    `The claim "${normalizeText(claim).slice(0, 80)}${claim.length > 80 ? '…' : ''}" rests on these assumptions:`,
    ...assumptions.map((a, i) => `${i + 1}. ${capitalizeFirst(a)}`),
    'If any of these assumptions fail, the conclusion would need re-evaluation.',
  ];

  return {
    transformedClaim: joinSentences(parts),
    evidenceUsed: [claim],
    assumptions,
    confidence: 0.92,
  };
}

function executeChallenge(ctx) {
  const claim = ctx.inputClaim;
  const parts = [
    claim,
    'Potential weaknesses in this reasoning:',
    'Domain specialists may produce stronger local evidence that conflicts with whole-business integration.',
    'The operator may have context Max cannot observe — operator judgment can override synthesis.',
    capitalizeFirst(OPERATING_MODEL.principles.find((p) => p.startsWith('Fail closed'))),
  ];
  return {
    transformedClaim: joinSentences(parts),
    evidenceUsed: [claim],
    assumptions: ctx.arc.assumptions || [],
    confidence: 0.88,
  };
}

function executeCounterfactual(ctx) {
  const claim = ctx.inputClaim;
  const assumptions = ctx.arc.assumptions || [];
  const targetAssumption = assumptions[assumptions.length - 1] || 'The operator owns final authority.';

  const parts = [
    `Suppose this assumption fails: "${targetAssumption}"`,
    'If that assumption were wrong:',
    'Specialists might need to make cross-domain tradeoffs without an integrator.',
    'Competing priorities could go unresolved without an operator decision frame.',
    'The original claim would weaken — ' + normalizeText(claim).slice(0, 100),
    'Counterfactuals are evaluated against the same governance frame — specialists advise within their domain; you decide.',
  ];

  return {
    transformedClaim: joinSentences(parts),
    evidenceUsed: [claim, targetAssumption],
    assumptions: [targetAssumption],
    confidence: 0.87,
  };
}

function executeCompare(ctx) {
  const claim = ctx.inputClaim;
  const q = normalizeText(ctx.question).toLowerCase();
  const specialistMatch = q.match(/\b(scout|paige|emmett|rex|sam|riley|cal|vera)\b/i);
  const specialist = specialistMatch ? specialistMatch[1].toLowerCase() : 'scout';
  const rel = getRelationship(specialist);
  const parts = [claim];

  if (rel) {
    parts.push(
      `${capitalizeFirst(specialist)} owns ${rel.owns.charAt(0).toLowerCase()}${rel.owns.slice(1)}`,
      capitalizeFirst(rel.reasoning),
      `Max optimizes ${OPERATING_MODEL.relationships.max.optimizes.charAt(0).toLowerCase()}${OPERATING_MODEL.relationships.max.optimizes.slice(1)}`
    );
  } else {
    parts.push(
      'Max integrates whole-business outcomes; specialists optimize within their domain.',
      capitalizeFirst(OPERATING_MODEL.why[2])
    );
  }

  return {
    transformedClaim: joinSentences(parts),
    evidenceUsed: [claim, specialist],
    assumptions: ctx.arc.assumptions || [],
    confidence: 0.91,
  };
}

function executeContrast(ctx) {
  const result = executeCompare(ctx);
  result.transformedClaim = joinSentences([
    'Key differences:',
    result.transformedClaim,
    'The contrast is structural — integration vs. domain depth — not a ranking of importance.',
  ]);
  return result;
}

function executeGeneralize(ctx) {
  const claim = ctx.inputClaim;
  const parts = [
    'At a higher level of abstraction:',
    claim,
    capitalizeFirst(OPERATING_MODEL.principles.find((p) => p.startsWith('Delegate expertise'))),
    'This pattern applies whenever local optimization must be balanced against whole-business outcomes.',
    capitalizeFirst(OPERATING_MODEL.why[5] || OPERATING_MODEL.why[4]),
  ];
  return {
    transformedClaim: joinSentences(parts),
    evidenceUsed: [claim],
    assumptions: ctx.arc.assumptions || [],
    confidence: 0.89,
  };
}

function executeSpecialize(ctx) {
  const claim = ctx.inputClaim;
  const parts = [
    claim,
    'Concretely: if Scout and Paige disagreed on outreach timing, Max would surface the tradeoff — pipeline velocity vs. deliverability — and defer the call to you.',
    capitalizeFirst(OPERATING_MODEL.relationships.max.reasoning),
  ];
  return {
    transformedClaim: joinSentences(parts),
    evidenceUsed: [claim, 'scout', 'paige'],
    assumptions: ctx.arc.assumptions || [],
    confidence: 0.9,
  };
}

function executeEvaluate(ctx) {
  const claim = ctx.inputClaim;
  const parts = [
    `Evaluating: "${normalizeText(claim).slice(0, 80)}"`,
    'Strengths: grounded in operating-model separation; preserves operator authority; scales across specialists.',
    'Weaknesses: depends on operator availability for final calls; synthesis quality varies with evidence completeness.',
    capitalizeFirst(OPERATING_MODEL.principles.find((p) => p.startsWith('Evidence before opinion'))),
  ];
  return {
    transformedClaim: joinSentences(parts),
    evidenceUsed: [claim],
    assumptions: ctx.arc.assumptions || [],
    confidence: 0.86,
  };
}

function executeSummarize(ctx) {
  const history = getReasoningHistory(ctx.session);
  const parts = ['Summary of the reasoning chain:'];

  if (history.length) {
    for (const entry of history.slice(-5)) {
      parts.push(`[${entry.operator} @ depth ${entry.depth}] ${normalizeText(entry.transformedClaim).slice(0, 120)}`);
    }
  } else {
    parts.push(ctx.inputClaim);
    for (const supporting of (ctx.arc.supportingClaims || []).slice(0, 2)) {
      parts.push(supporting);
    }
  }

  parts.push('The reasoning progressed from claim through justification toward explicit assumptions and counterfactuals.');
  return {
    transformedClaim: joinSentences(parts),
    evidenceUsed: history.map((h) => h.transformedClaim).slice(-5),
    assumptions: ctx.arc.assumptions || [],
    confidence: 0.93,
  };
}

function executeSynthesize(ctx) {
  const claims = [
    ctx.inputClaim,
    ...(ctx.arc.supportingClaims || []).slice(0, 3),
  ];
  const parts = [
    'Synthesized view:',
    ...claims.map((c, i) => `${i + 1}. ${c}`),
    capitalizeFirst(OPERATING_MODEL.principles.find((p) => p.startsWith('Optimize outcomes'))),
  ];
  return {
    transformedClaim: joinSentences(parts),
    evidenceUsed: claims,
    assumptions: ctx.arc.assumptions || [],
    confidence: 0.9,
  };
}

function executeRevise(ctx) {
  const claim = ctx.inputClaim;
  const history = getReasoningHistory(ctx.session);
  const parts = [
    'Revised conclusion after counterfactual analysis:',
  ];

  if (history.some((h) => h.operator === OPERATOR_IDS.COUNTERFACTUAL)) {
    parts.push(
      'The core governance frame holds — operator authority remains necessary — but the confidence in automatic synthesis decreases when assumptions fail.',
      'Revised claim: Max should surface assumption failures explicitly and request operator re-evaluation rather than proceeding on weakened premises.'
    );
  } else {
    parts.push(
      claim,
      'No counterfactual has been explored yet — the conclusion stands but should be tested against assumption failure.'
    );
  }

  return {
    transformedClaim: joinSentences(parts),
    evidenceUsed: history.map((h) => h.transformedClaim).slice(-3),
    assumptions: ctx.arc.assumptions || [],
    confidence: 0.88,
    revisedClaim: parts[parts.length - 1],
  };
}

function executeReflect(ctx) {
  const history = getReasoningHistory(ctx.session);
  const parts = ['Evolution of reasoning across this conversation:'];

  if (history.length) {
    for (let i = 0; i < history.length; i++) {
      const entry = history[i];
      parts.push(
        `Step ${i + 1} (${entry.operator}, depth ${entry.depth}): ${normalizeText(entry.transformedClaim).slice(0, 100)}`
      );
    }
  } else {
    parts.push(`Started with: ${ctx.inputClaim}`);
  }

  parts.push('Each step transformed the prior proposition rather than repeating it.');
  return {
    transformedClaim: joinSentences(parts),
    evidenceUsed: history.map((h) => h.transformedClaim),
    assumptions: ctx.arc.assumptions || [],
    confidence: 0.91,
  };
}

const OPERATOR_EXECUTORS = Object.freeze({
  [OPERATOR_IDS.EXPLAIN]: executeExplain,
  [OPERATOR_IDS.JUSTIFY]: executeJustify,
  [OPERATOR_IDS.SURFACE_ASSUMPTIONS]: executeSurfaceAssumptions,
  [OPERATOR_IDS.CHALLENGE]: executeChallenge,
  [OPERATOR_IDS.COUNTERFACTUAL]: executeCounterfactual,
  [OPERATOR_IDS.COMPARE]: executeCompare,
  [OPERATOR_IDS.CONTRAST]: executeContrast,
  [OPERATOR_IDS.GENERALIZE]: executeGeneralize,
  [OPERATOR_IDS.SPECIALIZE]: executeSpecialize,
  [OPERATOR_IDS.EVALUATE]: executeEvaluate,
  [OPERATOR_IDS.SUMMARIZE]: executeSummarize,
  [OPERATOR_IDS.SYNTHESIZE]: executeSynthesize,
  [OPERATOR_IDS.REVISE]: executeRevise,
  [OPERATOR_IDS.REFLECT]: executeReflect,
});

/**
 * Build ARC delta from a reasoning result. Operators never mutate ARC directly.
 *
 * @param {object} input
 * @returns {object}
 */
function buildReasoningArcDelta(input = {}) {
  const { arc, reasoningResult, operatorId, depth } = input;
  if (!arc || !reasoningResult) return null;

  const delta = {
    questionsResolved: [],
    questionsOpened: [],
    confidence: reasoningResult.confidence,
  };

  if (operatorId === OPERATOR_IDS.REVISE && reasoningResult.revisedClaim) {
    delta.newPrimaryClaim = reasoningResult.revisedClaim;
    delta.priorClaimDemoted = arc.primaryClaim;
  } else if (operatorId === OPERATOR_IDS.JUSTIFY && depth > 1) {
    delta.newSupportingClaims = [reasoningResult.transformedClaim];
    delta.primaryClaim = arc.primaryClaim;
  } else if (operatorId === OPERATOR_IDS.SURFACE_ASSUMPTIONS) {
    delta.assumptions = reasoningResult.assumptions;
  } else if (operatorId === OPERATOR_IDS.SUMMARIZE || operatorId === OPERATOR_IDS.SYNTHESIZE) {
    delta.newPrimaryClaim = reasoningResult.transformedClaim;
  }

  if (operatorId === OPERATOR_IDS.COUNTERFACTUAL) {
    delta.questionsOpened = ['Does that change your conclusion?'];
  }
  if (operatorId === OPERATOR_IDS.SURFACE_ASSUMPTIONS) {
    delta.questionsOpened = ['Could that assumption fail?', 'What would change if it did?'];
  }

  return delta;
}

/**
 * Execute a reasoning operator over ARC.
 *
 * @param {object} input
 * @returns {object|null} ReasoningResult envelope
 */
function executeReasoningOperator(input = {}) {
  const arc = input.arc || input.activeReasoningContext || null;
  if (!arc || !arc.primaryClaim) return null;

  const dispatch = input.dispatch || dispatchReasoningOperator(input);
  const operatorId = input.operatorId || dispatch.operatorId;
  const operatorMeta = buildOperatorMeta(operatorId);
  const session = input.session || null;
  const depth = typeof input.depth === 'number'
    ? input.depth
    : computeReasoningDepth(session, operatorId);

  const inputClaim = getPriorTransformedClaim(session, arc);

  const executor = OPERATOR_EXECUTORS[operatorId];
  if (!executor) return null;

  const ctx = {
    arc,
    session,
    depth,
    inputClaim,
    question: input.question || '',
    operatorIntent: input.operatorIntent || null,
    conversationContract: input.conversationContract || null,
  };

  const execution = executor(ctx);

  const reasoningResult = {
    operator: {
      ...operatorMeta,
      dispatchSource: dispatch.source,
      dispatchConfidence: dispatch.confidence,
    },
    transformedClaim: execution.transformedClaim,
    evidenceUsed: execution.evidenceUsed || [],
    assumptions: execution.assumptions || [],
    confidence: execution.confidence || 0.85,
    depth,
    revisedClaim: execution.revisedClaim || null,
  };

  const arcDelta = buildReasoningArcDelta({
    arc,
    reasoningResult,
    operatorId,
    depth,
  });

  if (session) {
    appendReasoningHistory(session, {
      operator: operatorId,
      transformedClaim: reasoningResult.transformedClaim,
      depth,
      dispatchSource: dispatch.source,
    });
  }

  return {
    ...reasoningResult,
    arcDelta,
    dispatch,
  };
}

/**
 * Main ROE entry — dispatch, execute, return result ready for presentation.
 * ARC is read-only input; workspace applies arcDelta separately.
 *
 * @param {object} input
 * @returns {object|null}
 */
function executeReasoning(input = {}) {
  const session = input.session || null;
  const arc =
    input.arc ||
    input.activeReasoningContext ||
    getActiveReasoningContext(session);

  if (!arc || !arc.primaryClaim) return null;

  const dispatch = dispatchReasoningOperator({
    ...input,
    arc,
    session,
  });

  return executeReasoningOperator({
    ...input,
    arc,
    session,
    dispatch,
    operatorId: dispatch.operatorId,
  });
}

/**
 * Verbalize a ReasoningResult for presentation. Presentation never reasons.
 *
 * @param {object} reasoningResult
 * @returns {string|null}
 */
function verbalizeReasoningResult(reasoningResult) {
  if (!reasoningResult || !reasoningResult.transformedClaim) return null;
  return reasoningResult.transformedClaim;
}

function clearReasoningHistory(session) {
  if (!session) return;
  session.reasoningHistory = [];
  if (session.context && typeof session.context === 'object') {
    session.context.reasoningHistory = [];
  }
}

module.exports = {
  OPERATOR_IDS,
  OPERATOR_CATEGORIES,
  dispatchReasoningOperator,
  executeReasoningOperator,
  executeReasoning,
  verbalizeReasoningResult,
  buildReasoningArcDelta,
  computeReasoningDepth,
  detectExplicitOperator,
  getReasoningHistory,
  appendReasoningHistory,
  clearReasoningHistory,
};
