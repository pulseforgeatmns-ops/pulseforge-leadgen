'use strict';

/**
 * SPEC-164 — Opportunity Intelligence Engine.
 * ADR-084 — Businesses grow by pursuing opportunities.
 *
 * Transforms market understanding and business judgment into ranked,
 * explainable business opportunities. No lead scores — multidimensional reasoning.
 *
 * Pipeline: Understanding → Business Judgment → Opportunity Intelligence → Decision
 */

const {
  VALUE_LEVELS,
  URGENCY_LEVELS,
  OPPORTUNITY_CATEGORIES,
  OPPORTUNITY_TIMELINE_STAGES,
  CATEGORY_RANK,
  buildOpportunity,
  buildOpportunityDimension,
} = require('./types');

const LEVEL_ORDER = Object.freeze({ high: 3, medium: 2, low: 1 });

function asText(value) {
  if (value == null) return '';
  return String(value).trim();
}

function observationTexts(understanding = {}) {
  const texts = [];
  for (const assertion of understanding.assertions || []) {
    texts.push(asText(assertion));
  }
  for (const evidence of [
    ...(understanding.supportingEvidence || []),
    ...(understanding.contradictoryEvidence || []),
  ]) {
    texts.push(asText(evidence.observation || evidence.label || evidence.text));
  }
  return texts.filter(Boolean);
}

function matchesAny(texts = [], patterns = []) {
  const hits = [];
  for (const pattern of patterns) {
    for (const text of texts) {
      if (typeof pattern === 'string' ? text.toLowerCase().includes(pattern.toLowerCase()) : pattern.test(text)) {
        hits.push(text);
        break;
      }
    }
  }
  return hits;
}

function levelFromScore(score) {
  if (score >= 0.72) return VALUE_LEVELS.HIGH;
  if (score >= 0.45) return VALUE_LEVELS.MEDIUM;
  return VALUE_LEVELS.LOW;
}

function entityKey(entity = {}) {
  const name = asText(entity.name || entity.entity).toLowerCase();
  if (name) return `name:${name}`;
  const id = asText(entity.entityId || entity.id).toLowerCase();
  if (id) return `id:${id}`;
  return '';
}

function findJudgmentForEntity(judgmentResult = {}, entityName) {
  const target = asText(entityName).toLowerCase();
  const perEntity = judgmentResult.perEntity || [];
  return (
    perEntity.find((row) => asText(row.entity).toLowerCase() === target) || {
      activatedHeuristics: (judgmentResult.activatedHeuristics || []).filter(
        (h) => !h.entity || asText(h.entity).toLowerCase() === target
      ),
      contradictions: judgmentResult.contradictions || [],
      overallJudgment: judgmentResult.overallJudgment || null,
    }
  );
}

function findCandidate(candidates = [], entityName, entityId) {
  const targetName = asText(entityName).toLowerCase();
  const targetId = asText(entityId).toLowerCase();
  return (
    candidates.find(
      (c) =>
        asText(c.name).toLowerCase() === targetName ||
        asText(c.id).toLowerCase() === targetId ||
        asText(c.companyId).toLowerCase() === targetId
    ) || null
  );
}

function findAcquisitionOpportunity(acquisitionOpportunities = [], entityName, entityId) {
  const targetName = asText(entityName).toLowerCase();
  const targetId = asText(entityId).toLowerCase();
  return (
    acquisitionOpportunities.find(
      (o) =>
        asText(o.name).toLowerCase() === targetName ||
        asText(o.companyId).toLowerCase() === targetId
    ) || null
  );
}

function collectEntities(input = {}) {
  const byKey = new Map();
  const add = (entity) => {
    const key = entityKey(entity);
    if (!key) return;
    const existing = byKey.get(key);
    byKey.set(key, {
      ...(existing || {}),
      ...entity,
      name: entity.name || existing?.name || entity.entity,
      entityId: entity.entityId || entity.id || existing?.entityId,
      understanding: entity.understanding || existing?.understanding,
      candidate: entity.candidate || existing?.candidate,
      acquisitionOpportunity: entity.acquisitionOpportunity || existing?.acquisitionOpportunity,
    });
  };

  for (const understanding of input.businessUnderstandings || []) {
    add({
      name: understanding.entity,
      entityId: understanding.entityId,
      kind: understanding.kind,
      understanding,
    });
  }

  for (const candidate of input.candidates || []) {
    add({
      name: candidate.name,
      entityId: candidate.id || candidate.companyId,
      candidate,
    });
  }

  for (const opp of input.acquisitionOpportunities || []) {
    add({
      name: opp.name,
      entityId: opp.companyId,
      acquisitionOpportunity: opp,
    });
  }

  return [...byKey.values()];
}

function evaluateBusinessValue(context = {}) {
  const texts = context.texts || [];
  const heuristics = context.activatedHeuristics || [];
  const reasoning = [];

  const recurringSignals = matchesAny(texts, [
    /property management/i,
    /portfolio/i,
    /multi.?family/i,
    /commercial/i,
    /recurring/i,
    /contract/i,
  ]);
  if (recurringSignals.length) {
    reasoning.push('High recurring revenue potential');
  }

  const strategicSignals = matchesAny(texts, [
    /beachhead/i,
    /reference customer/i,
    /first customer/i,
    /ideal customer/i,
  ]);
  if (strategicSignals.length) {
    reasoning.push('Strategic customer or reference potential');
  }

  for (const heuristic of heuristics) {
    if (heuristic.category === 'market_growth') {
      reasoning.push('Market growth increases expansion value');
    }
    if (heuristic.category === 'vendor_replacement') {
      reasoning.push('Vendor replacement window may unlock recurring contract');
    }
    if (/expansion|portfolio growth|managed properties/i.test((heuristic.implications || []).join(' '))) {
      reasoning.push('Portfolio expansion increases lifetime value');
    }
  }

  if (!reasoning.length && context.understanding?.kind === 'growth') {
    reasoning.push('Growth signals suggest expanding business value');
  }

  let score = 0.35;
  if (recurringSignals.length) score += 0.25;
  if (strategicSignals.length) score += 0.15;
  if (heuristics.some((h) => h.category === 'vendor_replacement')) score += 0.2;
  if (heuristics.some((h) => h.category === 'market_growth')) score += 0.1;
  score = Math.min(0.98, score);

  return {
    dimension: buildOpportunityDimension({
      level: levelFromScore(score),
      reasoning: reasoning.length ? reasoning : ['Business value not yet established — continue investigation'],
    }),
    score,
  };
}

function evaluateTiming(context = {}) {
  const texts = context.texts || [];
  const heuristics = context.activatedHeuristics || [];
  const reasoning = [];

  const timingPatterns = [
    { re: /hiring|hired|recruiting/i, label: 'Hiring activity suggests operational change' },
    { re: /operations?\s+manager|leadership change|new ownership/i, label: 'Leadership change often precedes vendor evaluation' },
    { re: /funding|investment|capital raise/i, label: 'Funding may accelerate growth and vendor decisions' },
    { re: /expansion|expanding|new location|portfolio growth|managed properties/i, label: 'Expansion increases service needs' },
    { re: /cleanliness|negative review|complaint|dissatisfaction/i, label: 'Service complaints create evaluation urgency' },
    { re: /seasonal|spring|year.?end/i, label: 'Seasonal timing may affect buying window' },
  ];

  for (const { re, label } of timingPatterns) {
    if (matchesAny(texts, [re]).length) reasoning.push(label);
  }

  if (context.acquisitionOpportunity?.timing >= 0.7) {
    reasoning.push('Recent timely signals detected in acquisition intelligence');
  }

  if (heuristics.some((h) => h.heuristicId === 'vendor_instability')) {
    reasoning.push('Vendor instability signals elevate timing urgency');
  }
  if (heuristics.some((h) => h.heuristicId === 'buying_readiness')) {
    reasoning.push('Buying readiness signals suggest near-term evaluation window');
  }

  let score = 0.25;
  score += Math.min(0.45, reasoning.length * 0.12);
  if (context.acquisitionOpportunity?.timing >= 0.7) score += 0.15;
  if (heuristics.some((h) => h.heuristicId === 'vendor_instability')) score += 0.15;
  score = Math.min(0.98, score);

  return {
    dimension: buildOpportunityDimension({
      level: levelFromScore(score),
      reasoning: reasoning.length ? reasoning : ['Timing signals not yet strong — monitor for changes'],
    }),
    score,
  };
}

function evaluateStrategicFit(context = {}, mission = {}) {
  const reasoning = [];
  const objectives = (mission.objectives || mission.goals || []).map(asText).filter(Boolean);
  const beachhead = asText(mission.beachhead || mission.targetSegment);
  const texts = context.texts || [];

  if (beachhead && texts.some((t) => t.toLowerCase().includes(beachhead.toLowerCase()))) {
    reasoning.push(`Matches beachhead segment: ${beachhead}`);
  }

  if (context.understanding?.kind === 'service_need' || context.understanding?.kind === 'buying_signal') {
    reasoning.push('Ideal customer profile signals present');
  }

  for (const objective of objectives) {
    if (texts.some((t) => t.toLowerCase().includes(objective.toLowerCase()))) {
      reasoning.push(`Advances mission objective: ${objective}`);
    }
  }

  if (context.candidate?.icpScore >= 70 || context.acquisitionOpportunity?.fit >= 0.7) {
    reasoning.push('Strong fit with current mission ICP criteria');
  }

  let score = 0.3;
  if (reasoning.length) score += Math.min(0.5, reasoning.length * 0.15);
  if (context.candidate?.icpScore >= 70) score += 0.15;
  score = Math.min(0.98, score);

  return {
    dimension: buildOpportunityDimension({
      level: levelFromScore(score),
      reasoning: reasoning.length ? reasoning : ['Strategic fit requires further mission alignment validation'],
    }),
    score,
  };
}

function evaluateReachability(context = {}) {
  const reasoning = [];
  const candidate = context.candidate || {};
  const acquisition = context.acquisitionOpportunity || {};
  const people = candidate.people || [];
  const decisionMakers = people.filter(
    (p) =>
      p.decisionMaker === true ||
      /\b(owner|principal|partner|operations|office manager|director|president|founder)\b/i.test(
        String(p.jobTitle || '')
      )
  );

  if (decisionMakers.length) {
    reasoning.push(`Decision maker identified: ${decisionMakers.map((p) => p.name).join(', ')}`);
  } else if (acquisition.signals?.some((s) => s.type === 'decision_maker')) {
    reasoning.push('Operations decision-maker identified via signals');
  }

  if (candidate.email || acquisition.email) reasoning.push('Email contact available');
  if (candidate.phone || acquisition.phone) reasoning.push('Phone contact available');
  if (context.warmIntroduction) reasoning.push('Warm introduction available');

  let score = 0.2;
  if (decisionMakers.length) score += 0.35;
  if (candidate.email || acquisition.email) score += 0.2;
  if (candidate.phone || acquisition.phone) score += 0.15;
  if (context.warmIntroduction) score += 0.2;
  score = Math.min(0.98, score);

  return {
    dimension: buildOpportunityDimension({
      level: levelFromScore(score),
      reasoning: reasoning.length ? reasoning : ['Reachability limited — identify decision maker before outreach'],
    }),
    score,
  };
}

function evaluateProbability(context = {}) {
  const heuristics = context.activatedHeuristics || [];
  const contradictions = context.contradictions || [];
  const reasoning = [];

  if (heuristics.some((h) => h.heuristicId === 'vendor_instability')) {
    reasoning.push('Vendor instability increases probability of desired outcome (evaluation/walkthrough)');
  }
  if (heuristics.some((h) => h.heuristicId === 'buying_readiness')) {
    reasoning.push('Buying readiness signals increase outcome probability');
  }
  if (heuristics.some((h) => h.heuristicId === 'vendor_stability')) {
    reasoning.push('Vendor stability reduces near-term switching probability');
  }

  if (context.understanding?.confidence >= 0.7) {
    reasoning.push('Strong supporting understanding confidence');
  }

  let score = heuristics.length
    ? heuristics.reduce((sum, h) => sum + (h.score || 0), 0) / heuristics.length
    : context.understanding?.confidence || 0.35;

  score -= contradictions.length * 0.1;
  score = Math.max(0.1, Math.min(0.98, score));

  if (!reasoning.length) {
    reasoning.push('Outcome probability depends on further validation of buying window');
  }

  return {
    dimension: buildOpportunityDimension({
      level: levelFromScore(score),
      reasoning,
    }),
    score,
  };
}

function evaluateLearningValue(context = {}, mission = {}) {
  const reasoning = [];
  const isFirstInVertical = mission.firstInVertical === true || mission.learningPriority === 'high';
  const texts = context.texts || [];
  const entityName = asText(context.understanding?.entity || context.candidate?.name).toLowerCase();

  const strSignals = matchesAny(texts, [/first str|short.?term rental|str property|str customer|pilot/i]);
  if (strSignals.length) {
    reasoning.push('Landing first customer in new segment teaches market patterns');
  }

  if (isFirstInVertical) {
    reasoning.push('First customer in vertical — high learning value even if revenue is smaller');
  }

  if (context.understanding?.kind === 'growth' && !isFirstInVertical) {
    reasoning.push('Growth case may validate beachhead assumptions');
  }

  let score = 0.25;
  if (strSignals.length) score += 0.45;
  if (isFirstInVertical && (strSignals.length || /str|pilot|first/i.test(entityName))) score += 0.35;
  else if (isFirstInVertical) score += 0.15;
  if (context.understanding?.kind === 'growth') score += 0.1;
  score = Math.min(0.98, score);

  return {
    dimension: buildOpportunityDimension({
      level: levelFromScore(score),
      reasoning: reasoning.length ? reasoning : ['Standard learning value — not a strategic learning priority'],
    }),
    score,
  };
}

function evaluateDifficulty(context = {}) {
  const reasoning = [];
  const heuristics = context.activatedHeuristics || [];

  if (heuristics.some((h) => h.heuristicId === 'vendor_stability')) {
    reasoning.push('Entrenched vendor relationships increase engagement difficulty');
  }
  if (context.reachability?.level === VALUE_LEVELS.LOW) {
    reasoning.push('Decision maker not yet identified');
  }
  if ((context.contradictions || []).length) {
    reasoning.push('Contradictory signals increase evaluation complexity');
  }

  let score = 0.35;
  if (heuristics.some((h) => h.heuristicId === 'vendor_stability')) score += 0.25;
  if (context.reachability?.level === VALUE_LEVELS.LOW) score += 0.2;
  if ((context.contradictions || []).length) score += 0.15;
  score = Math.min(0.98, score);

  return {
    dimension: buildOpportunityDimension({
      level: levelFromScore(score),
      reasoning: reasoning.length ? reasoning : ['Standard engagement difficulty expected'],
    }),
    score,
  };
}

function assignCategory(dimensions = {}, heuristics = [], contradictions = []) {
  const { businessValue, timing, probability, strategicFit, reachability, learningValue } = dimensions;

  if (heuristics.some((h) => h.heuristicId === 'vendor_stability') && timing.level === VALUE_LEVELS.LOW) {
    return OPPORTUNITY_CATEGORIES.DECLINE;
  }

  if (
    timing.level === VALUE_LEVELS.HIGH &&
    probability.level !== VALUE_LEVELS.LOW &&
    businessValue.level !== VALUE_LEVELS.LOW &&
    reachability.level !== VALUE_LEVELS.LOW
  ) {
    return OPPORTUNITY_CATEGORIES.IMMEDIATE;
  }

  if (
    timing.level === VALUE_LEVELS.HIGH &&
    (probability.level === VALUE_LEVELS.MEDIUM || learningValue.level === VALUE_LEVELS.HIGH)
  ) {
    return OPPORTUNITY_CATEGORIES.IMMEDIATE;
  }

  if (
    (businessValue.level === VALUE_LEVELS.HIGH || strategicFit.level === VALUE_LEVELS.HIGH) &&
    timing.level === VALUE_LEVELS.MEDIUM
  ) {
    return OPPORTUNITY_CATEGORIES.DEVELOPING;
  }

  if (timing.level === VALUE_LEVELS.MEDIUM || probability.level === VALUE_LEVELS.MEDIUM) {
    return OPPORTUNITY_CATEGORIES.DEVELOPING;
  }

  if (contradictions.length) {
    return OPPORTUNITY_CATEGORIES.MONITOR;
  }

  if (strategicFit.level === VALUE_LEVELS.HIGH && timing.level === VALUE_LEVELS.LOW) {
    return OPPORTUNITY_CATEGORIES.LONG_TERM;
  }

  if (businessValue.level === VALUE_LEVELS.LOW && timing.level === VALUE_LEVELS.LOW) {
    return OPPORTUNITY_CATEGORIES.WATCH;
  }

  return OPPORTUNITY_CATEGORIES.MONITOR;
}

function categoryToTimelineStage(category) {
  const map = {
    [OPPORTUNITY_CATEGORIES.IMMEDIATE]: OPPORTUNITY_TIMELINE_STAGES.IMMEDIATE,
    [OPPORTUNITY_CATEGORIES.DEVELOPING]: OPPORTUNITY_TIMELINE_STAGES.DEVELOPING,
    [OPPORTUNITY_CATEGORIES.MONITOR]: OPPORTUNITY_TIMELINE_STAGES.MONITOR,
    [OPPORTUNITY_CATEGORIES.LONG_TERM]: OPPORTUNITY_TIMELINE_STAGES.MONITOR,
    [OPPORTUNITY_CATEGORIES.WATCH]: OPPORTUNITY_TIMELINE_STAGES.MONITOR,
    [OPPORTUNITY_CATEGORIES.DECLINE]: OPPORTUNITY_TIMELINE_STAGES.MONITOR,
  };
  return map[category] || OPPORTUNITY_TIMELINE_STAGES.MONITOR;
}

function assignUrgency(category, timingLevel) {
  if (category === OPPORTUNITY_CATEGORIES.IMMEDIATE) return URGENCY_LEVELS.IMMEDIATE;
  if (category === OPPORTUNITY_CATEGORIES.DEVELOPING && timingLevel === VALUE_LEVELS.HIGH) {
    return URGENCY_LEVELS.SOON;
  }
  if (category === OPPORTUNITY_CATEGORIES.DECLINE) return URGENCY_LEVELS.DEFER;
  if (timingLevel === VALUE_LEVELS.HIGH) return URGENCY_LEVELS.SOON;
  return URGENCY_LEVELS.ROUTINE;
}

function buildRecommendedAction(category, reachabilityLevel) {
  if (category === OPPORTUNITY_CATEGORIES.IMMEDIATE) {
    return reachabilityLevel === VALUE_LEVELS.LOW
      ? 'Identify decision maker, then call today.'
      : 'Call today.';
  }
  if (category === OPPORTUNITY_CATEGORIES.DEVELOPING) return 'Schedule outreach this week.';
  if (category === OPPORTUNITY_CATEGORIES.MONITOR) return 'Monitor for timing signals; set review in 7 days.';
  if (category === OPPORTUNITY_CATEGORIES.LONG_TERM) return 'Add to long-term nurture; revisit quarterly.';
  if (category === OPPORTUNITY_CATEGORIES.DECLINE) return 'Deprioritize — existing vendor appears stable.';
  return 'Continue investigation before outreach.';
}

function buildExpectedOutcome(category, probabilityLevel) {
  if (category === OPPORTUNITY_CATEGORIES.IMMEDIATE) {
    return probabilityLevel === VALUE_LEVELS.HIGH ? 'Walkthrough' : 'Discovery call';
  }
  if (category === OPPORTUNITY_CATEGORIES.DEVELOPING) return 'Qualifying conversation';
  if (category === OPPORTUNITY_CATEGORIES.DECLINE) return 'No near-term vendor change expected';
  return 'Further intelligence before outreach';
}

function buildOpportunityReasoning(dimensions = {}, heuristics = []) {
  const reasons = [];
  for (const heuristic of heuristics.slice(0, 3)) {
    for (const implication of heuristic.implications || []) {
      if (implication && !reasons.includes(implication)) reasons.push(implication);
    }
  }
  for (const dim of [
    dimensions.businessValue,
    dimensions.timing,
    dimensions.strategicFit,
    dimensions.probability,
    dimensions.learningValue,
  ]) {
    for (const reason of dim?.reasoning || []) {
      if (reason && !reasons.includes(reason)) reasons.push(reason);
    }
  }
  return reasons.slice(0, 8);
}

function evaluateSingleOpportunity(entityRow = {}, input = {}) {
  const understanding = entityRow.understanding || null;
  const texts = understanding ? observationTexts(understanding) : [];
  const entityName = entityRow.name || understanding?.entity;
  const judgmentForEntity = findJudgmentForEntity(input.judgmentResult || {}, entityName);
  const activatedHeuristics = judgmentForEntity.activatedHeuristics || [];
  const contradictions = judgmentForEntity.contradictions || [];
  const candidate = entityRow.candidate || findCandidate(input.candidates || [], entityName, entityRow.entityId);
  const acquisitionOpportunity =
    entityRow.acquisitionOpportunity ||
    findAcquisitionOpportunity(input.acquisitionOpportunities || [], entityName, entityRow.entityId);

  const context = {
    texts,
    understanding,
    activatedHeuristics,
    contradictions,
    candidate,
    acquisitionOpportunity,
    warmIntroduction: entityRow.warmIntroduction || false,
  };

  const businessValue = evaluateBusinessValue(context);
  const timing = evaluateTiming(context);
  const strategicFit = evaluateStrategicFit(context, input.mission || {});
  const reachability = evaluateReachability(context);
  const probability = evaluateProbability(context);
  const learningValue = evaluateLearningValue(context, input.mission || {});

  context.reachability = reachability.dimension;
  const difficulty = evaluateDifficulty(context);

  const dimensions = {
    businessValue: businessValue.dimension,
    timing: timing.dimension,
    strategicFit: strategicFit.dimension,
    reachability: reachability.dimension,
    probability: probability.dimension,
    learningValue: learningValue.dimension,
    difficulty: difficulty.dimension,
  };

  const category = assignCategory(dimensions, activatedHeuristics, contradictions);
  const urgency = assignUrgency(category, timing.dimension.level);
  const opportunityReasoning = buildOpportunityReasoning(dimensions, activatedHeuristics);

  const confidenceValues = [probability.score, understanding?.confidence || 0, timing.score].filter(
    (v) => v > 0
  );
  const confidence =
    confidenceValues.length > 0
      ? Number((confidenceValues.reduce((a, b) => a + b, 0) / confidenceValues.length).toFixed(2))
      : 0;

  return buildOpportunity({
    entity: {
      id: entityRow.entityId || candidate?.id,
      name: entityName,
      kind: understanding?.kind || null,
    },
    mission: input.mission?.id || input.mission?.name || null,
    timing: timing.dimension,
    urgency,
    category,
    timelineStage: categoryToTimelineStage(category),
    expectedBusinessValue: businessValue.dimension,
    expectedDifficulty: difficulty.dimension,
    expectedProbability: probability.dimension,
    expectedLearningValue: learningValue.dimension,
    strategicFit: strategicFit.dimension,
    reachability: reachability.dimension,
    opportunityReasoning,
    supportingUnderstanding: understanding
      ? {
          entity: understanding.entity,
          kind: understanding.kind,
          assertions: understanding.assertions,
          confidence: understanding.confidence,
        }
      : null,
    recommendedAction: buildRecommendedAction(category, reachability.dimension.level),
    expectedOutcome: buildExpectedOutcome(category, probability.dimension.level),
    confidence,
    activatedHeuristics: activatedHeuristics.map((h) => ({
      name: h.name,
      heuristicId: h.heuristicId,
      category: h.category,
    })),
  });
}

function internalSortKey(opportunity, mission = {}) {
  const categoryRank = CATEGORY_RANK[opportunity.category] ?? 99;
  const levels = [
    opportunity.expectedBusinessValue?.level,
    opportunity.timing?.level,
    opportunity.expectedProbability?.level,
    opportunity.strategicFit?.level,
    opportunity.expectedLearningValue?.level,
    opportunity.reachability?.level,
  ];
  let levelSum = levels.reduce((sum, level) => sum + (LEVEL_ORDER[level] || 0), 0);
  if (mission.firstInVertical || mission.learningPriority === 'high') {
    levelSum += (LEVEL_ORDER[opportunity.expectedLearningValue?.level] || 0) * 0.5;
  }
  return [categoryRank, -levelSum, -opportunity.confidence];
}

function rankOpportunities(opportunities = [], mission = {}) {
  const sorted = [...opportunities].sort((a, b) => {
    const keyA = internalSortKey(a, mission);
    const keyB = internalSortKey(b, mission);
    for (let i = 0; i < keyA.length; i += 1) {
      if (keyA[i] !== keyB[i]) return keyA[i] - keyB[i];
    }
    return asText(a.entity.name).localeCompare(asText(b.entity.name));
  });

  return sorted.map((opp, index) =>
    buildOpportunity({
      ...opp,
      priority: index + 1,
    })
  );
}

function evaluateOpportunities(input = {}) {
  const entities = collectEntities(input);
  const mission = input.mission || {};
  const opportunities = entities.map((entity) => evaluateSingleOpportunity(entity, input));
  return rankOpportunities(opportunities, mission);
}

function compareOpportunities(higher, lower) {
  const advantages = [];
  const disadvantages = [];

  const dimensions = [
    ['expectedBusinessValue', 'Higher recurring value'],
    ['expectedProbability', 'Higher buying probability'],
    ['timing', 'Better timing'],
    ['expectedLearningValue', 'Higher learning value'],
    ['strategicFit', 'More strategic fit'],
    ['reachability', 'Better reachability'],
  ];

  for (const [field, label] of dimensions) {
    const aLevel = LEVEL_ORDER[higher[field]?.level] || 0;
    const bLevel = LEVEL_ORDER[lower[field]?.level] || 0;
    if (aLevel > bLevel) advantages.push(label);
    if (aLevel < bLevel) disadvantages.push(`${label.replace('Higher ', 'Lower ').replace('Better ', 'Weaker ')}`);
  }

  return {
    higher: {
      entity: higher.entity.name,
      priority: higher.priority,
      category: higher.category,
      advantages: advantages.length ? advantages : higher.opportunityReasoning.slice(0, 3),
      recommendedAction: higher.recommendedAction,
    },
    lower: {
      entity: lower.entity.name,
      priority: lower.priority,
      category: lower.category,
      notes:
        lower.category === OPPORTUNITY_CATEGORIES.DECLINE
          ? ['Existing vendor appears stable']
          : lower.opportunityReasoning.slice(0, 3),
      recommendedAction: lower.recommendedAction,
    },
    summary: `${higher.entity.name} ranks above ${lower.entity.name} based on ${advantages.slice(0, 3).join(', ') || 'multidimensional opportunity reasoning'}.`,
    notScoreBased: true,
    adr: 'ADR-084',
  };
}

function explainWhyFirst(opportunity, alternatives = []) {
  if (!opportunity) {
    return {
      spec: 'SPEC-164',
      adr: 'ADR-084',
      summary: 'No opportunity selected for explanation.',
    };
  }

  const nextBest = alternatives.find((a) => a.priority === 2) || alternatives[1] || null;
  const explanation = {
    spec: 'SPEC-164',
    adr: 'ADR-084',
    entity: opportunity.entity.name,
    priority: opportunity.priority,
    category: opportunity.category,
    businessValue: opportunity.expectedBusinessValue,
    timing: opportunity.timing,
    strategicFit: opportunity.strategicFit,
    probability: opportunity.expectedProbability,
    learningValue: opportunity.expectedLearningValue,
    reachability: opportunity.reachability,
    opportunityReasoning: opportunity.opportunityReasoning,
    recommendedAction: opportunity.recommendedAction,
    expectedOutcome: opportunity.expectedOutcome,
    expectedBusinessValue: opportunity.expectedBusinessValue?.reasoning?.[0] || 'See dimension reasoning',
    confidence: opportunity.confidence,
    notScoreBased: true,
  };

  if (nextBest) {
    explanation.comparison = compareOpportunities(opportunity, nextBest);
  }

  explanation.summary = [
    `${opportunity.entity.name} is priority ${opportunity.priority}.`,
    ...opportunity.opportunityReasoning.slice(0, 4),
    `Recommended action: ${opportunity.recommendedAction}`,
    `Expected outcome: ${opportunity.expectedOutcome}.`,
  ].join(' ');

  return explanation;
}

function detectOpportunityMovements(priorOpportunities = [], currentOpportunities = []) {
  const priorByEntity = new Map(
    priorOpportunities.map((o) => [entityKey(o.entity), o])
  );
  const movements = [];

  for (const current of currentOpportunities) {
    const key = entityKey(current.entity);
    const prior = priorByEntity.get(key);
    if (!prior) {
      movements.push({
        entity: current.entity.name,
        kind: 'new',
        fromCategory: null,
        toCategory: current.category,
        fromPriority: null,
        toPriority: current.priority,
        explanation: `${current.entity.name} entered the opportunity queue as ${current.category}.`,
        reasons: current.opportunityReasoning.slice(0, 4),
      });
      continue;
    }

    if (prior.category !== current.category || prior.priority !== current.priority) {
      const newReasons = current.opportunityReasoning.filter((r) => !(prior.opportunityReasoning || []).includes(r));
      movements.push({
        entity: current.entity.name,
        kind: 'moved',
        fromCategory: prior.category,
        toCategory: current.category,
        fromPriority: prior.priority,
        toPriority: current.priority,
        explanation: `${current.entity.name} moved from ${prior.category} to ${current.category}${
          prior.priority !== current.priority ? ` (priority ${prior.priority} → ${current.priority})` : ''
        }${newReasons.length ? ` because ${newReasons.join('; ')}` : ''}.`,
        reasons: newReasons.length ? newReasons : current.opportunityReasoning.slice(0, 4),
      });
    }
  }

  return movements;
}

function explainOvernightChanges(priorReport = {}, currentReport = {}) {
  const prior = priorReport.topOpportunities || priorReport.opportunities || [];
  const current = currentReport.topOpportunities || currentReport.opportunities || [];
  const movements = detectOpportunityMovements(prior, current);

  return {
    spec: 'SPEC-164',
    adr: 'ADR-084',
    movementCount: movements.length,
    movements,
    summary:
      movements.length === 0
        ? 'No significant opportunity movement since last evaluation.'
        : movements
            .map((m) => m.explanation)
            .slice(0, 5)
            .join(' '),
    explainsMovementNotJustEvidence: true,
  };
}

function recalculateForMissionObjectives(opportunities = [], missionObjectives = {}) {
  const mission = {
    ...(missionObjectives || {}),
    objectives: missionObjectives.objectives || missionObjectives.goals || [],
    beachhead: missionObjectives.beachhead || missionObjectives.targetSegment,
    firstInVertical: missionObjectives.firstInVertical,
    learningPriority: missionObjectives.learningPriority,
  };

  const recalculated = opportunities.map((opp) => {
    const entityRow = {
      name: opp.entity.name,
      entityId: opp.entity.id,
      understanding: opp.supportingUnderstanding
        ? {
            entity: opp.supportingUnderstanding.entity,
            entityId: opp.entity.id,
            kind: opp.supportingUnderstanding.kind,
            assertions: opp.supportingUnderstanding.assertions,
            confidence: opp.supportingUnderstanding.confidence,
          }
        : null,
    };

    return evaluateSingleOpportunity(entityRow, {
      mission,
      judgmentResult: {
        activatedHeuristics: opp.activatedHeuristics || [],
        perEntity: [
          {
            entity: opp.entity.name,
            activatedHeuristics: opp.activatedHeuristics || [],
          },
        ],
      },
    });
  });

  return rankOpportunities(recalculated, mission);
}

function buildOpportunityIntelligenceReport(input = {}) {
  const opportunities = evaluateOpportunities(input);
  const topOpportunities = opportunities.slice(0, 10);

  return {
    kind: 'opportunity_intelligence_report',
    spec: 'SPEC-164',
    adr: 'ADR-084',
    topOpportunities,
    opportunities,
    opportunityCount: opportunities.length,
    immediateCount: opportunities.filter((o) => o.category === OPPORTUNITY_CATEGORIES.IMMEDIATE).length,
    developingCount: opportunities.filter((o) => o.category === OPPORTUNITY_CATEGORIES.DEVELOPING).length,
    rankedByMultidimensionalReasoning: true,
    notScoreBased: true,
    invariant: 'Every recommendation must include explicit opportunity reasoning. No numeric lead score.',
    topOpportunity: topOpportunities[0] || null,
    summary: topOpportunities.length
      ? `${topOpportunities.length} top opportunit${topOpportunities.length === 1 ? 'y' : 'ies'} ranked by business value, timing, strategic fit, probability, and learning value — not lead score.`
      : 'No qualified opportunities evaluated yet.',
  };
}

function buildRecommendationFromOpportunity(opportunity, fallback = {}) {
  if (!opportunity) return fallback;

  return {
    kind: 'opportunity_intelligence',
    summary: [
      `${opportunity.entity.name} — Priority ${opportunity.priority}.`,
      ...opportunity.opportunityReasoning.slice(0, 3),
      `Recommended action: ${opportunity.recommendedAction}`,
      `Expected outcome: ${opportunity.expectedOutcome}.`,
    ].join(' '),
    entity: opportunity.entity.name,
    entityId: opportunity.entity.id,
    priority: opportunity.priority,
    category: opportunity.category,
    urgency: opportunity.urgency,
    recommendedAction: opportunity.recommendedAction,
    expectedOutcome: opportunity.expectedOutcome,
    expectedBusinessValue: opportunity.expectedBusinessValue,
    timing: opportunity.timing,
    strategicFit: opportunity.strategicFit,
    probability: opportunity.expectedProbability,
    learningValue: opportunity.expectedLearningValue,
    opportunityReasoning: opportunity.opportunityReasoning,
    confidence: opportunity.confidence,
    basedOnOpportunityIntelligence: true,
    basedOnHeuristics: true,
    basedOnUnderstanding: true,
    notDirectFromEvidence: true,
    notScoreBased: true,
    adr: 'ADR-084',
  };
}

module.exports = {
  evaluateOpportunities,
  evaluateSingleOpportunity,
  rankOpportunities,
  compareOpportunities,
  explainWhyFirst,
  explainOvernightChanges,
  detectOpportunityMovements,
  recalculateForMissionObjectives,
  buildOpportunityIntelligenceReport,
  buildRecommendationFromOpportunity,
  collectEntities,
  evaluateBusinessValue,
  evaluateTiming,
  evaluateStrategicFit,
  evaluateReachability,
  evaluateProbability,
  evaluateLearningValue,
};
