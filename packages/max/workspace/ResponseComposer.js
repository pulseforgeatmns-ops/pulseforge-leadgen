'use strict';

const { ACTION_TYPES } = require('../commandDeck/CommandDeckTypes');
const {
  buildStructuredResponse,
  PAGE_TYPES,
} = require('./WorkspaceTypes');
const { contextFocusLabel } = require('./ContextEnvelope');
const { assembleEvidence } = require('./EvidenceAssembler');
const { buildSuggestions, topCompanyName } = require('./SuggestionEngine');

/**
 * Build a grounded StructuredResponseObject from MaxContext + question.
 * Fail-closed: never invent evidence, scores, or confidence.
 *
 * @param {object} input
 * @param {object} input.context - normalized MaxContext
 * @param {string} input.question
 * @param {object} [input.session]
 */
function composeResponse(input) {
  const context = input.context;
  const question = String(input.question || '').trim();
  const evidence = assembleEvidence(context);
  const intent = classifyIntent(question, context);
  const focus = contextFocusLabel(context);

  const { answer, reasoning, unavailableExtra } = answerForIntent({
    intent,
    context,
    evidence,
    focus,
    question,
  });

  // A missing opposing signal is only relevant when the operator asked about
  // risk or contradiction. Showing it on every answer makes a normal lack of
  // counter-evidence look like a data failure.
  const unavailable = [
    ...evidence.unavailable.filter(
      (item) => item !== 'contradicting_evidence' || intent === 'contradictions'
    ),
    ...(unavailableExtra || []),
  ].filter((v, i, a) => a.indexOf(v) === i);

  const nextInvestigations = buildSuggestions(context).filter(
    (s) => s.toLowerCase() !== question.toLowerCase()
  );

  return buildStructuredResponse({
    answer,
    reasoning,
    supportingEvidence: evidence.supportingEvidence,
    contradictingEvidence: evidence.contradictingEvidence,
    confidence: evidence.confidence,
    nextInvestigations: nextInvestigations.slice(0, 4),
    recommendedActions: buildRecommendedActions(context, intent),
    confidenceContributors: evidence.confidenceContributors,
    timelineReferences: evidence.timelineReferences,
    relatedEntities: evidence.relatedEntities,
    metadata: {
      sourcesUsed: evidence.sourcesUsed,
      evidenceCount:
        evidence.supportingEvidence.length +
        evidence.contradictingEvidence.length,
      asOf: evidence.asOf,
      unavailable,
    },
  });
}

function classifyIntent(question, context) {
  const q = question.toLowerCase();
  if (
    /(?:highest|top|biggest|largest).*(?:call|calls|move|moves|movement)|(?:call|calls|move|moves).*(?:highest|top|biggest|largest)/.test(
      q
    )
  ) return 'activity';
  if (/contradict|oppos|against|why not|risk/.test(q)) return 'contradictions';
  if (/policy|approval|block|allow|wait/.test(q)) return 'policy';
  if (/compar/.test(q)) return 'compare';
  if (/overnight|changed|change|movement|shift/.test(q)) return 'changes';
  if (/watch|alert/.test(q)) return 'watches';
  if (/#1|ranked|top|why is/.test(q)) return 'rank';
  if (/confidence|signal|evidence|explain|reason/.test(q)) return 'explain';
  if (/history|relationship|timeline/.test(q)) return 'history';
  if (context.page === PAGE_TYPES.RECOMMENDATION) return 'explain';
  if (context.page === PAGE_TYPES.COMPANY) return 'explain';
  return 'general';
}

function answerForIntent({ intent, context, evidence, focus, question }) {
  const corpus = context._answerCorpus || null;
  const brief =
    corpus === 'briefing' || corpus == null
      ? context.briefing || (context.deck && context.deck.morningBrief) || {}
      : {};
  // Mission / market / general domains must not fall through to briefing copy.
  const allowBriefingAnswer =
    corpus === 'briefing' ||
    (corpus == null &&
      (!context.executionDomain ||
        context.executionDomain === 'morning_briefing'));
  const hla =
    (context.deck && context.deck.highestLeverageAction) ||
    findHlaPayload(context);
  const unavailableExtra = [];

  if (intent === 'rank') {
    return rankAnswer({ context, evidence, hla, focus, brief });
  }
  if (intent === 'changes') {
    return changesAnswer({ brief, evidence, context });
  }
  if (intent === 'activity') {
    return activityAnswer({ context, question });
  }
  if (intent === 'watches') {
    return watchesAnswer({ brief, context, evidence });
  }
  if (intent === 'contradictions') {
    return contradictionsAnswer({ evidence, focus });
  }
  if (intent === 'policy') {
    return policyAnswer({ hla, context, evidence });
  }
  if (intent === 'compare') {
    return compareAnswer({ context, evidence });
  }
  if (intent === 'history') {
    return historyAnswer({ evidence, focus });
  }
  if (intent === 'explain') {
    return explainAnswer({ hla, context, evidence, focus });
  }

  // general
  const parts = [];
  if (allowBriefingAnswer && brief.headline) parts.push(brief.headline);
  else if (allowBriefingAnswer && brief.summary) parts.push(brief.summary);
  else if (hla && hla.recommendation) {
    parts.push(
      `Current focus: ${hla.recommendation.companyName || focus} (${hla.recommendation.recommendedAction || 'review'}).`
    );
  } else if (corpus === 'market') {
    parts.push(
      `I can investigate ${focus} using Market Intelligence already in this context.`
    );
    unavailableExtra.push('detailed_answer');
  } else {
    parts.push(
      `I can investigate ${focus} using only the intelligence already in this context.`
    );
    unavailableExtra.push('detailed_answer');
  }

  const reasoning = [];
  if (allowBriefingAnswer && brief.summary) reasoning.push(String(brief.summary));
  if (evidence.supportingEvidence[0]) {
    const summary = evidence.supportingEvidence[0].summary;
    if (!reasoning.includes(summary)) reasoning.push(summary);
  }
  if (!reasoning.length) {
    reasoning.push(
      'No additional verified facts were present in the context envelope for this question.'
    );
    unavailableExtra.push('reasoning_detail');
  }

  return {
    answer: parts.join(' '),
    reasoning,
    unavailableExtra,
  };
}

function rankAnswer({ context, evidence, hla, focus, brief }) {
  const name = topCompanyName(context) || focus;
  const reasoning = [];
  let answer;

  if (hla && hla.recommendation) {
    const opp =
      hla.opportunity != null ? `Opportunity ${hla.opportunity}` : null;
    const conf =
      hla.confidence != null ? `Confidence ${hla.confidence}` : null;
    answer = `${hla.recommendation.companyName || name} is the highest-leverage item in the current briefing${opp || conf ? ` (${[opp, conf].filter(Boolean).join(' · ')})` : ''}.`;
    if (hla.recommendation.recommendedAction) {
      reasoning.push(
        `Recommended action: ${hla.recommendation.recommendedAction}.`
      );
    }
    for (const s of (hla.supportingSignals || []).slice(0, 3)) {
      reasoning.push(typeof s === 'string' ? s : s.summary || String(s));
    }
  } else {
    answer = `The context envelope does not include a highest-leverage ranking explanation for ${name}.`;
    reasoning.push(
      'Open Max from the Highest Leverage Action or Priority Queue after the deck has assembled priorities.'
    );
  }

  if (brief.headline) reasoning.push(String(brief.headline));
  if (!reasoning.length && evidence.supportingEvidence[0]) {
    reasoning.push(evidence.supportingEvidence[0].summary);
  }

  return {
    answer,
    reasoning: reasoning.filter(Boolean),
    unavailableExtra: hla ? [] : ['rank_detail'],
  };
}

function changesAnswer({ brief, evidence, context }) {
  const marketChanges = Number(brief.marketChanges);
  const reasoning = [];
  let answer;

  if (Number.isFinite(marketChanges)) {
    answer =
      marketChanges === 0
        ? 'No overnight market changes are recorded in the current briefing.'
        : `${marketChanges} market change${marketChanges === 1 ? '' : 's'} are recorded in the current briefing.`;
    if (brief.summary) reasoning.push(String(brief.summary));
  } else {
    answer =
      'Overnight change counts are not available in the current context envelope.';
    reasoning.push('The briefing snapshot did not include marketChanges.');
  }

  for (const ref of evidence.timelineReferences.slice(0, 4)) {
    reasoning.push(ref.summary);
  }

  const queue = priorityQueueItems(context);
  for (const item of queue.slice(0, 3)) {
    if (item.movement || item.trend) {
      reasoning.push(
        `${item.companyName || item.title}: ${item.movement || item.trend}`
      );
    }
  }

  return {
    answer,
    reasoning: reasoning.length
      ? reasoning
      : ['No movement indicators were present on visible priority items.'],
    unavailableExtra: Number.isFinite(marketChanges) ? [] : ['market_changes'],
  };
}

function activityAnswer({ context, question }) {
  const queue = priorityQueueItems(context);
  const moves = queue.filter(
    (item) =>
      (item.movement && item.movement !== '—') ||
      (item.scoreDelta != null && Number(item.scoreDelta) !== 0)
  );
  const asksForCalls = /\bcalls?\b/i.test(question);
  const asksForMoves = /\b(?:moves?|movement)\b/i.test(question);
  const reasoning = [];

  if (moves.length) {
    const ranked = moves.slice(0, 3);
    const answer = `The largest recorded moves in this briefing are ${ranked
      .map((item) => `${item.companyName || item.company || item.title || 'Unnamed priority'} (${item.movement || formatScoreDelta(item.scoreDelta)})`)
      .join(', ')}.`;
    if (asksForCalls) {
      reasoning.push('Call activity metrics are not included in the current context.');
    }
    reasoning.push(
      ...ranked.map(
        (item) =>
          `${item.companyName || item.company || item.title || 'Unnamed priority'}: ${item.summary || item.movement || formatScoreDelta(item.scoreDelta)}`
      )
    );
    return {
      answer,
      reasoning,
      unavailableExtra: asksForCalls ? ['call_activity'] : [],
    };
  }

  const gaps = [];
  if (asksForCalls) gaps.push('call_activity');
  if (asksForMoves || !asksForCalls) gaps.push('market_movement');
  return {
    answer:
      "I can’t rank today’s calls or moves from this briefing because it contains neither call activity metrics nor recorded movement in the current window.",
    reasoning: [
      'Historical comparison data is not attached to the current Max context.',
    ],
    unavailableExtra: gaps,
  };
}

function watchesAnswer({ brief, context, evidence }) {
  const count = Number(brief.watchAlertCount);
  const alerts =
    (context.deck && context.deck.watchAlerts) ||
    (context.visibleCards || []).filter((c) => c.type === 'watch_alert');
  const reasoning = alerts.slice(0, 5).map(
    (a) => a.summary || a.title || String(a.id)
  );

  let answer;
  if (Number.isFinite(count) && count >= 0) {
    answer =
      count === 0
        ? 'No watch alerts are flagged in the current briefing.'
        : `${count} watch alert${count === 1 ? '' : 's'} require attention.`;
  } else if (alerts.length) {
    answer = `${alerts.length} watch alert${alerts.length === 1 ? '' : 's'} are visible in context.`;
  } else {
    answer = 'Watch alert details are not available in the current context.';
  }

  if (!reasoning.length && evidence.supportingEvidence[0]) {
    reasoning.push(evidence.supportingEvidence[0].summary);
  }

  return {
    answer,
    reasoning: reasoning.length
      ? reasoning
      : ['No watch alert cards were included in the envelope.'],
    unavailableExtra: alerts.length || Number.isFinite(count) ? [] : ['watches'],
  };
}

function contradictionsAnswer({ evidence, focus }) {
  const list = evidence.contradictingEvidence;
  if (!list.length) {
    return {
      answer: `No contradicting evidence is present in the current context for ${focus}.`,
      reasoning: [
        'The envelope did not include opposing signals for the focused entity.',
      ],
      unavailableExtra: ['contradicting_evidence'],
    };
  }
  return {
    answer: `${list.length} contradicting signal${list.length === 1 ? '' : 's'} are recorded for ${focus}.`,
    reasoning: list.slice(0, 5).map((e) => e.summary),
    unavailableExtra: [],
  };
}

function policyAnswer({ hla, context, evidence }) {
  const policy =
    (hla && hla.policy) ||
    findPolicyOnCards(context);
  if (!policy || (policy.outcome == null && policy.allowed == null)) {
    return {
      answer:
        'Policy evaluation is not available in the current context envelope.',
      reasoning: [
        'Open Max from a recommendation that includes an attached policy decision.',
      ],
      unavailableExtra: ['policy'],
    };
  }
  const outcome = policy.outcome || (policy.blocked ? 'block' : policy.requiresApproval ? 'requireApproval' : policy.allowed ? 'allow' : 'unknown');
  const reasoning = [];
  if (policy.reason) reasoning.push(String(policy.reason));
  if (policy.severity) reasoning.push(`Severity: ${policy.severity}.`);
  reasoning.push(`Outcome: ${outcome}.`);
  if (outcome === 'allow') {
    reasoning.push('Waiting is optional; policy currently allows progress.');
  } else if (outcome === 'requireApproval' || policy.requiresApproval) {
    reasoning.push('Waiting without approval leaves the recommendation pending.');
  } else if (outcome === 'block' || policy.blocked) {
    reasoning.push('Policy blocks autonomous progress until conditions change.');
  }

  return {
    answer: `Policy evaluation for the focused recommendation: ${outcome}.`,
    reasoning,
    unavailableExtra: [],
  };
}

function compareAnswer({ context, evidence }) {
  const queue = priorityQueueItems(context);
  const items = queue.slice(0, 3);
  if (items.length < 2) {
    const names = evidence.relatedEntities.slice(0, 3).map((e) => e.name);
    if (names.length < 2) {
      return {
        answer:
          'Fewer than two opportunities are available in the current context to compare.',
        reasoning: [
          'Priority queue comparison requires multiple ranked items in the envelope.',
        ],
        unavailableExtra: ['comparison_set'],
      };
    }
    return {
      answer: `Related entities in context: ${names.join(', ')}.`,
      reasoning: names.map((n) => `Entity: ${n}`),
      unavailableExtra: [],
    };
  }
  return {
    answer: `Top opportunities in context: ${items
      .map((i) => i.companyName || i.title)
      .join(', ')}.`,
    reasoning: items.map((i) => {
      const bits = [
        i.companyName || i.title,
        i.opportunity != null || i.score != null
          ? `opportunity ${i.opportunity != null ? i.opportunity : i.score}`
          : null,
        i.confidence != null ? `confidence ${i.confidence}`
          : null,
        i.movement || i.trend || null,
      ].filter(Boolean);
      return bits.join(' · ');
    }),
    unavailableExtra: [],
  };
}

function priorityQueueItems(context) {
  const queue = context.deck && context.deck.priorityQueue;
  return Array.isArray(queue) ? queue : (queue && queue.items) || [];
}

function formatScoreDelta(value) {
  const delta = Number(value) || 0;
  return delta > 0 ? `↑${delta}` : delta < 0 ? `↓${Math.abs(delta)}` : '—';
}

function historyAnswer({ evidence, focus }) {
  const refs = evidence.timelineReferences;
  if (!refs.length) {
    return {
      answer: `No timeline references are present in the current context for ${focus}.`,
      reasoning: [
        'Timeline history is not included in this envelope. A dedicated timeline page will supply richer history later.',
      ],
      unavailableExtra: ['timeline'],
    };
  }
  return {
    answer: `${refs.length} timeline reference${refs.length === 1 ? '' : 's'} are available for ${focus}.`,
    reasoning: refs.slice(0, 5).map((r) => r.summary),
    unavailableExtra: [],
  };
}

function explainAnswer({ hla, context, evidence, focus }) {
  const reasoning = [];
  let answer;

  if (hla && hla.recommendation) {
    answer = `${hla.recommendation.companyName || focus} is recommended for ${hla.recommendation.recommendedAction || 'review'}${hla.opportunity != null ? ` with opportunity ${hla.opportunity}` : ''}${hla.confidence != null ? ` and confidence ${hla.confidence}` : ''}.`;
    for (const s of (hla.supportingSignals || []).slice(0, 4)) {
      reasoning.push(typeof s === 'string' ? s : s.summary || String(s));
    }
  } else {
    const card = (context.visibleCards || [])[0];
    if (card) {
      answer = card.summary || card.title || `Focused on ${focus}.`;
      if (card.summary) reasoning.push(String(card.summary));
    } else {
      answer = `Verified explanation detail for ${focus} is limited in the current envelope.`;
      reasoning.push(
        'Provide a recommendation or company card when opening Max for richer explainability.'
      );
    }
  }

  for (const e of evidence.supportingEvidence.slice(0, 3)) {
    if (!reasoning.includes(e.summary)) reasoning.push(e.summary);
  }

  if (!reasoning.length) {
    reasoning.push('No supporting signals were attached to the focused item.');
  }

  return {
    answer,
    reasoning,
    unavailableExtra: hla || (context.visibleCards || []).length ? [] : ['explanation'],
  };
}

function findHlaPayload(context) {
  for (const card of context.visibleCards || []) {
    if (card.type === 'highest_leverage' && card.payload) return card.payload;
  }
  return null;
}

function findPolicyOnCards(context) {
  for (const card of context.visibleCards || []) {
    if (card.payload && card.payload.policy) return card.payload.policy;
  }
  return null;
}

function buildRecommendedActions(context, intent) {
  const actions = [];
  const recId = context.recommendationId;
  const companyId = context.companyId;
  const hla =
    (context.deck && context.deck.highestLeverageAction) ||
    findHlaPayload(context);
  const resolvedRec =
    recId ||
    (hla && hla.recommendation && hla.recommendation.id) ||
    null;
  const resolvedCompany =
    companyId ||
    (hla && hla.recommendation && hla.recommendation.companyId) ||
    null;

  if (resolvedRec) {
    actions.push({
      id: 'review_recommendation',
      type: ACTION_TYPES.REVIEW_RECOMMENDATION,
      label: 'Review Recommendation',
      payload: { recommendationId: resolvedRec },
    });
  }
  if (resolvedCompany) {
    actions.push({
      id: 'open_company',
      type: ACTION_TYPES.OPEN_COMPANY,
      label: 'Open Company Context',
      payload: { companyId: resolvedCompany },
    });
  }
  actions.push({
    id: 'view_evidence',
    type: ACTION_TYPES.ASK_MAX,
    label: 'View Supporting Evidence',
    payload: { prompt: 'Explain supporting signals.', context: context.page },
  });
  if (intent === 'compare') {
    actions.push({
      id: 'compare',
      type: ACTION_TYPES.ASK_MAX,
      label: 'Compare Companies',
      payload: {
        prompt: "Compare today's top opportunities.",
        context: context.page,
      },
    });
  }
  return actions.slice(0, 4);
}

module.exports = {
  composeResponse,
  classifyIntent,
};
