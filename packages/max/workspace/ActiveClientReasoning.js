'use strict';

/**
 * SPEC-103C — Session-scoped active conversational reasoning continuity.
 *
 * Preserves the active recommendation/plan across natural follow-ups within
 * a Workspace session. Not durable memory — tenant + session scoped only.
 *
 * BLUEPRINT FACT ≠ ACTIVE MAX THOUGHT ≠ OBSERVED EVIDENCE
 */

const WORD_TO_INDEX = Object.freeze({
  one: 0,
  two: 1,
  three: 2,
  four: 3,
  five: 4,
  six: 5,
  seven: 6,
  eight: 7,
});

/** Semantic operation families — stems, not phrase allowlists. */
const ACTIVE_THOUGHT_OP_STEMS = Object.freeze({
  advance: ['then', 'after', 'next', 'following', 'subsequent', 'later'],
  select: ['first', 'start', 'begin', 'initial'],
  deepen: ['exact', 'exactly', 'detail', 'mean', 'how', 'procedure', 'specific'],
  critique: ['wrong', 'risk', 'fail', 'break', 'weak', 'downside', 'concern', 'assuming'],
  capability: ['pulseforge', 'handle', 'available', 'capable'],
  operator: ['myself', 'human', 'operator'],
  recover: ['back', 'return', 'resume'],
  subject_change: ['separate', 'unrelated', 'different'],
  explain: ['why'],
  falsify: ['change', 'mind', 'falsif', 'revise', 'reconsider'],
  compare: ['instead', 'rather', 'versus'],
});

function normalizeClientUtterance(question) {
  return String(question || '')
    .normalize('NFKC')
    .replace(/[\u2018\u2019\u2032\u00B4]/g, "'")
    .replace(/[\u201C\u201D]/g, '"')
    .toLowerCase()
    .replace(/([a-z])\1{2,}/g, '$1$1')
    .replace(/\s+/g, ' ')
    .trim();
}

function tokenize(normalized) {
  return String(normalized || '')
    .replace(/[^a-z0-9'\s]/g, ' ')
    .split(/\s+/)
    .filter(Boolean);
}

function lightStem(token) {
  let t = String(token || '');
  if (t.length <= 3) return t;
  t = t.replace(/'s$/, '');
  if (t.endsWith('ies') && t.length > 4) return `${t.slice(0, -3)}y`;
  if (t.endsWith('ing') && t.length > 5) return t.slice(0, -3);
  if (t.endsWith('ed') && t.length > 4) return t.slice(0, -2);
  if (t.endsWith('es') && t.length > 4) return t.slice(0, -2);
  if (t.endsWith('s') && t.length > 3 && !t.endsWith('ss')) return t.slice(0, -1);
  return t;
}

function stemHits(tokens, stems) {
  const normalized = tokens.map(lightStem);
  let hits = 0;
  for (const prefix of stems) {
    if (
      normalized.some((s) => {
        if (s.startsWith(prefix)) return true;
        if (s.length >= 3 && prefix.startsWith(s)) return true;
        return false;
      })
    ) {
      hits += 1;
    }
  }
  return hits;
}

function midSentencePhrase(text) {
  const s = String(text || '').trim();
  if (!s) return '';
  if (/^Greater\s+/i.test(s)) return s;
  return s.charAt(0).toLowerCase() + s.slice(1);
}

function getActiveClientReasoning(session) {
  const ctx =
    session && session.context && typeof session.context === 'object'
      ? session.context
      : null;
  const active = ctx && ctx.activeClientReasoning;
  if (!active || typeof active !== 'object') return null;
  return active;
}

function setActiveClientReasoning(session, active) {
  if (!session || !session.context || typeof session.context !== 'object') return;
  session.context.activeClientReasoning = active;
}

/**
 * Build ordered plan steps (shared by decompose prose + active state).
 */
function buildDecomposePlanSteps(summary, opts = {}) {
  const prior = opts.prior || null;
  const audience = summary.idealCustomers || 'your target accounts';
  const market = summary.geography || summary.targetMarkets || null;
  const where = market ? ` in ${market}` : '';
  const outcome =
    summary.successMetrics ||
    summary.campaignGoals ||
    'walkthroughs and recurring revenue';

  const steps = [
    `Define qualification criteria for ${audience}${where} — geography, building type, and decision-maker role.`,
    `Build a short account set that fits those criteria (roughly 15–25 targets, not a city-wide list).`,
    `Identify the decision-maker at each account (owner, property manager, or facility manager).`,
    `Verify contact paths — email and phone where available; record gaps instead of guessing.`,
    `Draft a concise outreach message tied to walkthrough outcomes, not a broad pitch blast.`,
    `Review the list and messages before any send — advisory only until you authorize execution.`,
    `Run a controlled outreach batch and log conversations → walkthroughs → ${midSentencePhrase(outcome)}.`,
    `Measure results after one cycle; widen or revise the motion only with evidence.`,
  ];

  const focus =
    (prior && prior.recommendationFocus) ||
    summary.idealCustomers ||
    summary.campaignGoals ||
    'the active plan';

  return {
    focus,
    audience,
    market,
    outcome,
    steps,
    recommendation:
      `Prove a repeatable commercial acquisition motion around ${audience}${where}.`,
  };
}

function parseStepNumber(q) {
  const m = String(q || '').match(/\bstep (\d+|one|two|three|four|five|six|seven|eight)\b/);
  if (!m) return null;
  const raw = m[1];
  if (/^\d+$/.test(raw)) return Math.max(0, Number(raw) - 1);
  return WORD_TO_INDEX[raw] != null ? WORD_TO_INDEX[raw] : null;
}

/**
 * Semantic follow-up classification against active reasoning state.
 * Returns null when message should use standard SPEC-103 routing.
 */
function classifyActiveThoughtFollowUp(question, session) {
  const active = getActiveClientReasoning(session);
  if (!active) return null;

  const q = normalizeClientUtterance(question);
  const tokens = tokenize(q);
  const hasPlan = Array.isArray(active.planSteps) && active.planSteps.length > 0;

  if (
    /\b(separate question|different topic|unrelated question)\b/.test(q) ||
    (hasPlan &&
      /\b(raise|raising|price|pric)\b/.test(q) &&
      /\bresidential\b/.test(q) &&
      !/\b(commercial|plan|step|acquisition)\b/.test(q))
  ) {
    return { op: 'subject_change', confidence: 0.9 };
  }

  if (/\bgo back\b/.test(q) && /\b(plan|commercial|step)\b/.test(q)) {
    const idx = parseStepNumber(q);
    return {
      op: idx != null ? 'select_step' : 'recover_plan',
      stepIndex: idx,
      confidence: 0.88,
    };
  }

  const scores = {};
  for (const [op, stems] of Object.entries(ACTIVE_THOUGHT_OP_STEMS)) {
    scores[op] = stemHits(tokens, stems);
  }

  if (hasPlan) {
    if (/\b(and|ok|okay|fine|suppose|once)\b/.test(q) && /\b(then|after|next)\b/.test(q)) {
      scores.advance += 3;
    }
    if (
      (/\b(first|initial|very first)\b/.test(q) &&
        /\b(thing|step|move|action|do)\b/.test(q)) ||
      /\bwhat('s| is) first\b/.test(q)
    ) {
      scores.select += 4;
    }
    if (
      /\b(what can|which part can|can pulseforge|can you|could pulseforge)\b/.test(q) ||
      (/\b(handle|do for me)\b/.test(q) && /\b(part|step|those|these)\b/.test(q))
    ) {
      scores.capability += 4;
    }
    if (
      /\b(what do i need|what must i|myself|on me|human|operator)\b/.test(q)
    ) {
      scores.operator += 3;
    }
    if (
      /\b(could go wrong|go wrong|weak spot|downside|where does this break|what are you assuming)\b/.test(
        q
      )
    ) {
      scores.critique += 4;
    }
    if (
      /\bhow exactly\b/.test(q) ||
      (/\bhow\b/.test(q) && tokens.length <= 4) ||
      /\bwhat does that mean\b/.test(q)
    ) {
      scores.deepen += 3;
    }
    const explicitStep = parseStepNumber(q);
    if (explicitStep != null) {
      return { op: 'select_step', stepIndex: explicitStep, confidence: 0.9 };
    }
  }

  if (/\bwhy\b/.test(q) && tokens.length <= 6) {
    scores.explain += 3;
  }
  if (/\b(change your mind|falsif|revise|reconsider)\b/.test(q)) {
    scores.falsify += 3;
  }
  if (/\b(instead of|rather than|versus)\b/.test(q) && /\bresidential\b/.test(q)) {
    scores.compare += 3;
  }

  const ranked = Object.entries(scores)
    .filter(([, score]) => score > 0)
    .sort((a, b) => b[1] - a[1]);
  if (!ranked.length || ranked[0][1] < 2) return null;

  const op = ranked[0][0];
  if (op === 'subject_change') return { op, confidence: ranked[0][1] / 5 };
  if (!hasPlan && !['explain', 'falsify', 'compare'].includes(op)) return null;

  const result = { op, confidence: Math.min(0.95, ranked[0][1] / 5) };
  if (op === 'advance') {
    const base =
      active.conversationalFocusIndex != null
        ? active.conversationalFocusIndex
        : 0;
    result.stepIndex = Math.min(base + 1, (active.planSteps || []).length - 1);
  }
  if (op === 'select') {
    result.stepIndex = 0;
  }
  return result;
}

function resolveTenantCapabilities(session, summary) {
  const ctx =
    session && session.context && typeof session.context === 'object'
      ? session.context
      : {};
  const hasBlueprint = Boolean(summary && summary.approved);
  const hasMarketIntel = Boolean(ctx.marketIntelligence);
  return {
    blueprintTargeting: hasBlueprint,
    planDecomposition: hasBlueprint,
    marketIntelligenceRanking: hasMarketIntel,
    scoutProspectingDesigned: true,
    scoutCallableFromWorkspace: false,
    autonomousOutreach: false,
    executionReviewPath: true,
  };
}

function formatStepFocus(stepText, stepNumber, total) {
  return (
    `Step ${stepNumber} of ${total}: ${stepText.replace(/^\d+\.\s*/, '')} ` +
    `This is the current conversational focus — not a claim that earlier steps are already complete.`
  );
}

function formatPlanSelectResponse(active, stepIndex = 0) {
  const steps = active.planSteps || [];
  const idx = Math.max(0, Math.min(stepIndex, steps.length - 1));
  const step = steps[idx];
  const n = idx + 1;
  let prose = `The ${n === 1 ? 'very first' : `next relevant`} thing I'd do is:\n\n`;
  prose += formatStepFocus(step, n, steps.length);
  if (n === 1) {
    prose +=
      '\n\nBefore looking for companies, decide what qualifies as a useful prospect — geography, property type, and decision-maker role.';
  }
  return prose;
}

function formatPlanAdvanceResponse(active, stepIndex) {
  const steps = active.planSteps || [];
  const idx =
    stepIndex != null
      ? Math.max(0, Math.min(stepIndex, steps.length - 1))
      : Math.min((active.conversationalFocusIndex || 0) + 1, steps.length - 1);
  const step = steps[idx];
  const n = idx + 1;
  const prev = idx > 0 ? idx : null;
  let prose = `After step ${prev || 1}, the next move is:\n\n`;
  prose += formatStepFocus(step, n, steps.length);
  prose +=
    '\n\nMoving to the next step here is conversational progression — it does not mean the prior step is finished or executed.';
  return prose;
}

function formatPlanDeepenResponse(active, summary) {
  const idx =
    active.conversationalFocusIndex != null ? active.conversationalFocusIndex : 0;
  const step = (active.planSteps || [])[idx] || '';
  const audience = summary.idealCustomers || 'your target accounts';
  const market = summary.geography || summary.targetMarkets || '';
  const where = market ? ` in ${market}` : '';

  const deepeners = [
    `Write down explicit criteria: ${audience}${where}, commercial or multifamily properties, and a reachable property or facility manager.`,
    `Use those criteria to compile 15–25 named targets from your network, referrals, or permitted prospect sources — not a random city-wide scrape.`,
    `For each account, name the likely decision-maker title and one verified contact path or an explicit "contact unknown" flag.`,
    `Validate email/phone via your existing tools; do not guess titles or direct lines.`,
    `Draft one short message focused on booking a walkthrough — outcome, not a generic pitch deck.`,
    `You review every name and message before anything sends; I stay advisory until you authorize execution.`,
    `Track outreach → replies → walkthroughs booked in one simple log for this batch only.`,
    `After one cycle, compare walkthrough quality and conversion before widening the audience.`,
  ];

  const detail = deepeners[idx] || deepeners[0];
  return (
    `On the current step (${idx + 1}), I'd get more concrete this way:\n\n${detail}\n\n` +
    `This deepens the active plan — it does not restart from the high-level recommendation.`
  );
}

function formatPlanCritiqueResponse(active, summary) {
  const audience = summary.idealCustomers || 'your target accounts';
  const market = summary.geography || summary.targetMarkets || '';
  const where = market ? ` in ${market}` : '';
  const risks = [
    `Qualification criteria could stay too broad and pull in ${audience}${where} that will never convert.`,
    'A 15–25 account sample could be too small to learn from or accidentally biased toward easy-but-weak targets.',
    'Missing or stale contact data could make a viable ICP look like a dead market.',
    'Weak messaging could be mistaken for weak market fit before we have enough conversations.',
    'Insufficient follow-up could suppress response rates independent of targeting quality.',
    'Walkthrough quantity could look good while walkthrough quality or close rate stays poor.',
    'Property managers and facility managers may behave differently — treating them as one homogeneous segment could blur the signal.',
    'You might win opportunities that are hard to service operationally if criteria ignore capacity constraints.',
    'Conclusions could be drawn from one short batch before enough observations exist.',
  ];
  return (
    `Critiquing the active plan — these are reasoning hypotheses, not observed problems:\n\n` +
    risks.map((r, i) => `${i + 1}. ${r}`).join('\n') +
    `\n\nI'm not repeating the high-level recommendation; I'm stress-testing the plan we're already discussing.`
  );
}

function formatPlanCapabilityResponse(active, session, summary) {
  const caps = resolveTenantCapabilities(session, summary);
  const lines = [];
  lines.push(
    'Mapping Pulseforge capabilities onto the active plan (truthful for this workspace — not a marketing list):'
  );

  const pfNow = [];
  const operator = [];
  const unavailable = [];

  if (caps.blueprintTargeting) {
    pfNow.push('use your approved Blueprint to define targeting criteria and keep reasoning tenant-scoped');
  }
  if (caps.planDecomposition) {
    pfNow.push('decompose the recommendation into ordered operator steps and stay inside that thread');
  }
  pfNow.push('preserve campaign objectives and track interaction/outcome context where wired');

  operator.push('approve targeting criteria and account scope');
  operator.push('make human calls, conduct walkthroughs, quote jobs, and close contracts');
  operator.push('report outcomes the system does not automatically capture');

  if (!caps.marketIntelligenceRanking) {
    unavailable.push(
      'evidence-ranked live company lists for this tenant without sufficient Market Intelligence'
    );
  }
  if (!caps.scoutCallableFromWorkspace) {
    unavailable.push(
      'initiating Scout prospect discovery directly from this Max workspace conversation (capability exists in the product, but is not callable on this path yet)'
    );
  }
  if (!caps.autonomousOutreach) {
    unavailable.push('unsupported autonomous outreach or send-from-chat');
  }

  lines.push('\nPulseforge can help now:\n' + pfNow.map((x) => `• ${x}`).join('\n'));
  lines.push('\nRequires you (operator):\n' + operator.map((x) => `• ${x}`).join('\n'));
  lines.push(
    '\nNot currently available here:\n' + unavailable.map((x) => `• ${x}`).join('\n')
  );
  lines.push(
    '\nThis maps capabilities to the same plan — it does not execute anything and does not create a Mission.'
  );
  return lines.join('\n');
}

function formatPlanOperatorResponse(active) {
  const operatorOwned = (active.planSteps || []).map((step, i) => {
    const n = i + 1;
    if (n === 1) return `${n}. Finalize qualification criteria judgments.`;
    if (n === 2) return `${n}. Approve the account set scope and sources.`;
    if (n === 3 || n === 4) return `${n}. Human verification of decision-makers and contact paths where automation is thin.`;
    if (n === 5 || n === 6) return `${n}. Review and approve outreach copy before any send.`;
    if (n === 7) return `${n}. Conduct calls, walkthroughs, and on-site qualification.`;
    return `${n}. Judge results and decide whether to widen the motion.`;
  });
  return (
    `Relative to the active plan, these are operator-owned actions:\n\n` +
    operatorOwned.join('\n') +
    `\n\nPulseforge can advise and organize; the human steps above stay on you unless an existing review-controlled execution path is explicitly authorized.`
  );
}

function formatPlanEvidenceContinuation(active) {
  const steps = active.planSteps || [];
  return (
    `I do not yet have enough live tenant-scoped evidence to name specific companies without inventing them. ` +
    `That is the missing piece between step 1 and step 2 of the active plan: once qualification criteria are set, we need a verified account set. ` +
    `I can stay inside the plan and fail closed on names — step 1 is still: ${String(steps[0] || 'define qualification criteria').replace(/^\d+\.\s*/, '')}.`
  );
}

function composeActiveThoughtResponse(summary, question, opts, followUp) {
  const session = opts.session || null;
  const active = getActiveClientReasoning(session);
  if (!active) return null;

  const op = followUp.op;
  let prose = null;
  let kind = 'plan_continuity';
  let stepIndex = followUp.stepIndex;

  if (op === 'select' || op === 'select_step') {
    stepIndex = op === 'select_step' ? followUp.stepIndex : 0;
    prose = formatPlanSelectResponse(active, stepIndex);
    kind = 'plan_select';
  } else if (op === 'advance') {
    stepIndex =
      followUp.stepIndex != null
        ? followUp.stepIndex
        : Math.min((active.conversationalFocusIndex || 0) + 1, active.planSteps.length - 1);
    prose = formatPlanAdvanceResponse(active, stepIndex);
    kind = 'plan_advance';
  } else if (op === 'deepen') {
    stepIndex = active.conversationalFocusIndex != null ? active.conversationalFocusIndex : 0;
    prose = formatPlanDeepenResponse(active, summary);
    kind = 'plan_deepen';
  } else if (op === 'critique') {
    prose = formatPlanCritiqueResponse(active, summary);
    kind = 'plan_critique';
  } else if (op === 'capability') {
    prose = formatPlanCapabilityResponse(active, session, summary);
    kind = 'plan_capability';
  } else if (op === 'operator') {
    prose = formatPlanOperatorResponse(active);
    kind = 'plan_operator';
  } else if (op === 'recover_plan' || op === 'recover_step') {
    stepIndex =
      op === 'recover_step' && followUp.stepIndex != null ? followUp.stepIndex : 1;
    prose = formatPlanAdvanceResponse(active, stepIndex);
    kind = 'plan_recover';
  } else {
    return null;
  }

  return {
    prose,
    kind,
    confidenceLabel: 'moderate',
    confidence: 0.78,
    recommendationFocus: active.recommendationFocus || active.focus || null,
    conversationalFocusIndex: stepIndex != null ? stepIndex : active.conversationalFocusIndex,
    planSteps: active.planSteps,
  };
}

function updateActiveReasoningFromTurn(session, turn, summary, prose) {
  if (!session || !turn) return;
  const prior = getActiveClientReasoning(session) || {};
  const kind = turn.kind || turn.turnKind || 'reasoning';
  const next = {
    ...prior,
    executionStatus: 'advisory_only',
    updatedAt: new Date().toISOString(),
    tenantId:
      (session.context && session.context.tenantId) ||
      (summary && summary.clientId) ||
      prior.tenantId ||
      null,
  };

  if (summary && summary.businessName) {
    next.subject = `${summary.businessName} commercial acquisition`;
  }

  if (turn.recommendationFocus) {
    next.recommendationFocus = turn.recommendationFocus;
  }

  if (kind === 'decompose') {
    const built = buildDecomposePlanSteps(summary || {}, {
      prior: { recommendationFocus: turn.recommendationFocus },
    });
    next.planSteps = turn.planSteps || built.steps;
    next.recommendation = built.recommendation;
    next.focus = built.focus;
    next.conversationalFocusIndex =
      prior.conversationalFocusIndex != null ? prior.conversationalFocusIndex : null;
    next.kind = 'plan';
  } else if (
    kind === 'plan_select' ||
    kind === 'plan_advance' ||
    kind === 'plan_deepen' ||
    kind === 'plan_recover'
  ) {
    if (turn.conversationalFocusIndex != null) {
      next.conversationalFocusIndex = turn.conversationalFocusIndex;
    }
    next.kind = 'plan';
  } else if (kind === 'follow_up' && prose) {
    next.rationale = String(prose).slice(0, 2000);
    if (!next.recommendation) {
      const built = buildDecomposePlanSteps(summary || {}, {
        prior: { recommendationFocus: turn.recommendationFocus },
      });
      next.recommendation = built.recommendation;
    }
    next.kind = next.planSteps ? 'plan' : 'recommendation';
  } else if (kind === 'challenge') {
    next.falsificationConditions = String(prose || '').slice(0, 1500);
    next.kind = next.planSteps ? 'plan' : 'recommendation';
  } else if (
    kind === 'plan_critique' ||
    kind === 'plan_capability' ||
    kind === 'plan_operator' ||
    kind === 'plan_continuity'
  ) {
    next.kind = 'plan';
  } else if (
    ['reasoning', 'focus', 'opportunity', 'approach', 'targeting'].includes(kind)
  ) {
    const built = buildDecomposePlanSteps(summary || {}, {
      prior: { recommendationFocus: turn.recommendationFocus },
    });
    next.recommendation = built.recommendation;
    next.focus = built.focus;
    next.kind = 'recommendation';
  }

  if (summary && Array.isArray(summary.unknowns)) {
    next.unknowns = summary.unknowns.slice(0, 6);
  }

  setActiveClientReasoning(session, next);
}

module.exports = {
  getActiveClientReasoning,
  setActiveClientReasoning,
  buildDecomposePlanSteps,
  classifyActiveThoughtFollowUp,
  composeActiveThoughtResponse,
  updateActiveReasoningFromTurn,
  resolveTenantCapabilities,
  formatPlanEvidenceContinuation,
};
