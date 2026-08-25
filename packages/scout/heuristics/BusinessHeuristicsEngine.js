'use strict';

/**
 * SPEC-162 — Business Heuristics Engine.
 * ADR-082 — Business judgment through reusable heuristics.
 *
 * Pipeline: Understanding → Match Heuristics → Score → Explain → Recommend
 *
 * Recommendations originate from activated heuristics, not directly from evidence.
 */

const { asText, buildActivatedHeuristic, buildHeuristicContradiction, OUTCOME_KINDS } = require('./types');
const { INITIAL_HEURISTICS, cloneHeuristicLibrary, getHeuristicById } = require('./HeuristicLibrary');

const STRENGTH_DELTA_WON = 0.05;
const STRENGTH_DELTA_LOST = 0.05;
const STRENGTH_MIN = 0.3;
const STRENGTH_MAX = 1.5;

function observationText(item) {
  return asText(item?.observation || item?.label || item?.text || item);
}

function matchesPattern(text, pattern) {
  if (typeof pattern === 'string') {
    return text.toLowerCase().includes(pattern.toLowerCase());
  }
  return pattern.test(text);
}

function collectEntityContext(understanding = {}) {
  const texts = [];
  const evidenceItems = [];

  for (const assertion of understanding.assertions || []) {
    texts.push(asText(assertion));
  }

  for (const evidence of [
    ...(understanding.supportingEvidence || []),
    ...(understanding.contradictoryEvidence || []),
  ]) {
    const text = observationText(evidence);
    if (text) {
      texts.push(text);
      evidenceItems.push({
        source: evidence.source || 'unknown',
        observation: text,
        id: evidence.id || null,
        kind: 'evidence',
      });
    }
  }

  return {
    entity: understanding.entity,
    entityId: understanding.entityId || null,
    kind: understanding.kind || null,
    confidence: understanding.confidence || 0,
    texts,
    evidenceItems,
    understanding,
  };
}

function collectMarketContext(businessUnderstandings = [], extraEvidence = []) {
  const entities = (businessUnderstandings || []).map(collectEntityContext);
  const marketTexts = [];
  const marketEvidence = [];

  for (const entity of entities) {
    marketTexts.push(...entity.texts);
    marketEvidence.push(...entity.evidenceItems);
  }

  for (const item of extraEvidence) {
    const text = observationText(item);
    if (text) {
      marketTexts.push(text);
      marketEvidence.push({
        source: item.source || 'collected',
        observation: text,
        id: item.id || null,
        kind: 'evidence',
      });
    }
  }

  return { entities, marketTexts, marketEvidence };
}

function countPatternMatches(texts = [], patterns = []) {
  const matchedPatterns = [];
  const triggeringEvidence = [];

  for (const pattern of patterns) {
    for (const text of texts) {
      if (matchesPattern(text, pattern)) {
        matchedPatterns.push(pattern);
        triggeringEvidence.push({
          source: 'pattern_match',
          observation: text,
          pattern: String(pattern),
        });
        break;
      }
    }
  }

  return {
    matchCount: matchedPatterns.length,
    triggeringEvidence: dedupeEvidence(triggeringEvidence),
  };
}

function dedupeEvidence(items = []) {
  const seen = new Set();
  return items.filter((item) => {
    const key = `${item.source}:${item.observation}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function scoreHeuristicActivation(heuristic, context = {}) {
  const { texts = [], evidenceItems = [], kind, confidence = 0 } = context;
  const conditions = heuristic.triggerConditions || {};
  const patternResult = countPatternMatches(texts, conditions.patterns || []);
  let matchCount = patternResult.matchCount;
  let triggeringEvidence = patternResult.triggeringEvidence.slice();

  if (conditions.understandingKinds?.length && kind && conditions.understandingKinds.includes(kind)) {
    matchCount += 1;
    triggeringEvidence.push({
      source: 'understanding_kind',
      observation: `Understanding kind "${kind}" matches heuristic trigger`,
    });
  }

  for (const assertionPattern of conditions.assertionPatterns || []) {
    for (const text of texts) {
      if (matchesPattern(text, assertionPattern)) {
        matchCount += 1;
        triggeringEvidence.push({ source: 'assertion', observation: text });
        break;
      }
    }
  }

  const minMatches = conditions.minMatches || 1;
  const minSignals = heuristic.evidenceRequirements?.minSignals || 1;
  const requiredMatches = Math.max(minMatches, minSignals);

  if (matchCount < requiredMatches) {
    return null;
  }

  const coverageRatio = Math.min(1, matchCount / Math.max(requiredMatches, matchCount));
  const baseScore = 0.55 + coverageRatio * 0.25;
  const understandingBoost = confidence * 0.12;
  const modifier = heuristic.confidenceModifier || 0;
  const strength = heuristic.strength != null ? heuristic.strength : 1;

  let score = (baseScore + understandingBoost + modifier) * strength;
  score = Number(Math.min(0.98, Math.max(0.35, score)).toFixed(2));

  const contradictoryEvidence = (heuristic.contradictoryEvidence || []).slice();
  for (const item of evidenceItems) {
    for (const pattern of conditions.patterns || []) {
      if (matchesPattern(item.observation, /stability|satisfied|long.?standing/i) && heuristic.id === 'vendor_instability') {
        contradictoryEvidence.push(item);
      }
    }
  }

  return buildActivatedHeuristic({
    heuristicId: heuristic.id,
    name: heuristic.name,
    category: heuristic.category,
    description: heuristic.description,
    score,
    confidence: score,
    implications: heuristic.implications,
    triggeringEvidence: dedupeEvidence([
      ...triggeringEvidence,
      ...evidenceItems.slice(0, Math.min(evidenceItems.length, matchCount)).map((e) => ({
        source: e.source,
        observation: e.observation,
      })),
    ]),
    contradictoryEvidence,
    entity: context.entity || null,
    entityId: context.entityId || null,
    strength,
  });
}

function matchHeuristicsForContext(heuristicLibrary = [], context = {}) {
  const activated = [];

  for (const heuristic of heuristicLibrary) {
    const activation = scoreHeuristicActivation(heuristic, context);
    if (activation) {
      activated.push(activation);
    }
  }

  return activated.sort((a, b) => b.score - a.score);
}

const HEURISTIC_TENSIONS = Object.freeze([
  ['growth_market', 'vendor_stability'],
  ['buying_readiness', 'vendor_stability'],
]);

function detectHeuristicContradictions(activated = [], heuristicLibrary = []) {
  const contradictions = [];
  const byId = new Map(activated.map((a) => [a.heuristicId, a]));

  for (const item of activated) {
    const definition = getHeuristicById(heuristicLibrary, item.heuristicId);
    for (const opposingId of definition?.contradicts || []) {
      const opposing = byId.get(opposingId);
      if (!opposing) continue;

      const pairKey = [item.heuristicId, opposingId].sort().join('::');
      if (contradictions.some((c) => c.pairKey === pairKey)) continue;

      contradictions.push({
        pairKey,
        ...buildHeuristicContradiction({
          heuristicA: item.heuristicId,
          heuristicB: opposingId,
          nameA: item.name,
          nameB: opposing.name,
          scoreA: item.score,
          scoreB: opposing.score,
          tension: `${item.name} (${item.score}) vs ${opposing.name} (${opposing.score}) — both judgments preserved; overall confidence reduced.`,
          confidencePenalty: 0.12,
        }),
      });
    }
  }

  for (const [idA, idB] of HEURISTIC_TENSIONS) {
    const a = byId.get(idA);
    const b = byId.get(idB);
    if (!a || !b) continue;

    const pairKey = [idA, idB].sort().join('::');
    if (contradictions.some((c) => c.pairKey === pairKey)) continue;

    contradictions.push({
      pairKey,
      ...buildHeuristicContradiction({
        heuristicA: idA,
        heuristicB: idB,
        nameA: a.name,
        nameB: b.name,
        scoreA: a.score,
        scoreB: b.score,
        tension: `${a.name} (${a.score}) vs ${b.name} (${b.score}) — growing commercial opportunity BUT long-standing vendor relationships may slow switching.`,
        confidencePenalty: 0.1,
      }),
    });
  }

  return contradictions;
}

function computeOverallJudgment(activated = [], contradictions = []) {
  if (!activated.length) {
    return {
      summary: 'Insufficient business signals to form judgment — continue investigation.',
      confidence: 0,
      priority: 'low',
      recommendationHint: 'Gather additional evidence before outreach prioritization.',
    };
  }

  const avgScore = activated.reduce((sum, h) => sum + h.score, 0) / activated.length;
  const penalty = contradictions.reduce((sum, c) => sum + (c.confidencePenalty || 0.1), 0);
  let confidence = Number(Math.max(0.15, Math.min(0.98, avgScore - penalty)).toFixed(2));

  const top = activated[0];
  const vendorInstability = activated.find((h) => h.heuristicId === 'vendor_instability');
  const buyingReadiness = activated.find((h) => h.heuristicId === 'buying_readiness');
  const growthMarket = activated.find((h) => h.heuristicId === 'growth_market');
  const vendorStability = activated.find((h) => h.heuristicId === 'vendor_stability');

  let summary;
  let priority = 'medium';
  let recommendationHint;

  if (contradictions.length) {
    summary = `Mixed business judgment — ${activated.length} heuristics activated with ${contradictions.length} tension${contradictions.length === 1 ? '' : 's'}. Review contradictions before prioritizing.`;
    priority = 'medium';
    recommendationHint = contradictions[0].tension;
  } else if (vendorInstability && buyingReadiness) {
    summary = 'Business appears likely to evaluate vendors within the next 90 days.';
    priority = 'high';
    recommendationHint = 'Prioritize outreach before competitors.';
  } else if (growthMarket && vendorStability) {
    summary = 'Growing commercial opportunity BUT long-standing vendor relationships may slow switching.';
    priority = 'medium';
    recommendationHint = contradictions[0]?.tension || 'Evaluate timing — growth signals present but vendor entrenchment likely.';
  } else if (growthMarket) {
    summary = 'Commercial opportunity increasing — early outreach recommended.';
    priority = 'high';
    recommendationHint = 'Prioritize outreach before competitors.';
  } else if (top) {
    summary = `${top.name} activated (${top.score}) — ${top.implications[0] || 'Review activated heuristics.'}`;
    priority = top.score >= 0.75 ? 'high' : 'medium';
    recommendationHint = top.implications[0] || null;
  } else {
    summary = 'Insufficient business signals to form judgment — continue investigation.';
    priority = 'low';
    recommendationHint = 'Gather additional evidence before outreach prioritization.';
  }

  return {
    summary,
    confidence,
    priority,
    recommendationHint,
    averageHeuristicScore: Number(avgScore.toFixed(2)),
    contradictionPenalty: Number(penalty.toFixed(2)),
  };
}

/**
 * Activate heuristics from synthesized business understanding.
 * @param {object} input
 * @returns {object}
 */
function activateHeuristics(input = {}) {
  const heuristicLibrary = input.heuristicLibrary || cloneHeuristicLibrary(INITIAL_HEURISTICS);
  const businessUnderstandings = input.businessUnderstandings || [];
  const extraEvidence = input.extraEvidence || [];
  const entityFilter = input.entity || null;

  const { entities, marketTexts, marketEvidence } = collectMarketContext(
    businessUnderstandings,
    extraEvidence
  );

  const perEntity = [];
  for (const entityContext of entities) {
    if (entityFilter && entityContext.entity !== entityFilter) continue;
    const activated = matchHeuristicsForContext(heuristicLibrary, entityContext);
    if (activated.length) {
      const contradictions = detectHeuristicContradictions(activated, heuristicLibrary);
      const overallJudgment = computeOverallJudgment(activated, contradictions);
      perEntity.push({
        entity: entityContext.entity,
        entityId: entityContext.entityId,
        activatedHeuristics: activated,
        contradictions,
        overallJudgment,
      });
    }
  }

  const marketActivated = matchHeuristicsForContext(heuristicLibrary, {
    texts: marketTexts,
    evidenceItems: marketEvidence,
    entity: 'Market',
    confidence: businessUnderstandings[0]?.confidence || 0,
  });

  const allActivated = dedupeActivated([
    ...perEntity.flatMap((row) => row.activatedHeuristics),
    ...marketActivated,
  ]);

  const contradictions = detectHeuristicContradictions(allActivated, heuristicLibrary);
  const overallJudgment = computeOverallJudgment(allActivated, contradictions);

  return {
    spec: 'SPEC-162',
    adr: 'ADR-082',
    judgmentNotFromEvidence: true,
    basedOnHeuristics: true,
    perEntity,
    activatedHeuristics: allActivated,
    contradictions,
    overallJudgment,
    heuristicCount: allActivated.length,
  };
}

function dedupeActivated(items = []) {
  const byKey = new Map();
  for (const item of items) {
    const key = `${item.entity || 'market'}::${item.heuristicId}`;
    const existing = byKey.get(key);
    if (!existing || item.score > existing.score) {
      byKey.set(key, item);
    }
  }
  return [...byKey.values()].sort((a, b) => b.score - a.score);
}

/**
 * Explain business judgment for operator traceability.
 * @param {object} judgmentResult
 * @param {object} [options]
 * @returns {object}
 */
function explainJudgment(judgmentResult = {}, options = {}) {
  const entity = options.entity || null;
  const entityBlock = entity
    ? (judgmentResult.perEntity || []).find((row) => row.entity === entity)
    : null;

  const activated = entityBlock?.activatedHeuristics || judgmentResult.activatedHeuristics || [];
  const contradictions = entityBlock?.contradictions || judgmentResult.contradictions || [];
  const overallJudgment =
    entityBlock?.overallJudgment || judgmentResult.overallJudgment || computeOverallJudgment(activated, contradictions);

  return {
    spec: 'SPEC-162',
    entity: entity || (activated[0]?.entity ?? null),
    businessJudgment: true,
    activatedHeuristics: activated.map((item, index) => ({
      rank: index + 1,
      name: item.name,
      heuristicId: item.heuristicId,
      score: item.score,
      confidence: item.confidence,
      implications: item.implications,
      evidence: (item.triggeringEvidence || []).map((e) => ({
        source: e.source,
        observation: e.observation,
      })),
      contradictoryEvidence: (item.contradictoryEvidence || []).map((e) => ({
        source: e.source,
        observation: e.observation,
      })),
    })),
    contradictions: contradictions.map((c) => ({
      heuristicA: c.nameA,
      heuristicB: c.nameB,
      scoreA: c.scoreA,
      scoreB: c.scoreB,
      tension: c.tension,
    })),
    overallJudgment: {
      summary: overallJudgment.summary,
      confidence: overallJudgment.confidence,
      priority: overallJudgment.priority,
      recommendationHint: overallJudgment.recommendationHint,
    },
    heuristicCount: activated.length,
    judgmentNotFromEvidence: true,
  };
}

/**
 * Build recommendation from activated heuristics — not directly from evidence.
 * @param {object} judgmentResult
 * @returns {object}
 */
function buildRecommendationFromHeuristics(judgmentResult = {}) {
  const activated = judgmentResult.activatedHeuristics || [];
  const overall = judgmentResult.overallJudgment || computeOverallJudgment(activated, judgmentResult.contradictions);

  if (!activated.length) {
    return {
      kind: 'insufficient_judgment',
      summary: overall.summary,
      confidence: overall.confidence,
      basedOnHeuristics: false,
      basedOnUnderstanding: true,
      notDirectFromEvidence: true,
      adr: 'ADR-082',
    };
  }

  const top = activated[0];
  const implication = overall.recommendationHint || top.implications[0] || top.description;

  return {
    kind: 'business_judgment',
    summary: `${overall.summary} ${implication ? `Recommendation: ${implication}` : ''}`.trim(),
    topHeuristic: top.name,
    topHeuristicId: top.heuristicId,
    topHeuristicScore: top.score,
    activatedCount: activated.length,
    contradictions: (judgmentResult.contradictions || []).length,
    confidence: overall.confidence,
    priority: overall.priority,
    basedOnHeuristics: true,
    basedOnUnderstanding: true,
    notDirectFromEvidence: true,
    adr: 'ADR-082',
  };
}

/**
 * Build mission-report Business Judgment section.
 * @param {object} judgmentResult
 * @returns {object}
 */
function buildBusinessJudgmentReport(judgmentResult = {}) {
  const activated = judgmentResult.activatedHeuristics || [];
  const overall = judgmentResult.overallJudgment || computeOverallJudgment(activated, judgmentResult.contradictions);

  return {
    spec: 'SPEC-162',
    adr: 'ADR-082',
    activatedHeuristics: activated.map((item) => ({
      name: item.name,
      heuristicId: item.heuristicId,
      category: item.category,
      score: item.score,
      confidence: item.confidence,
      implications: item.implications,
      entity: item.entity,
    })),
    contradictions: (judgmentResult.contradictions || []).map((c) => ({
      between: [c.nameA, c.nameB],
      tension: c.tension,
    })),
    overallJudgment: {
      summary: overall.summary,
      confidence: overall.confidence,
      priority: overall.priority,
    },
    perEntity: (judgmentResult.perEntity || []).map((row) => ({
      entity: row.entity,
      activatedHeuristics: row.activatedHeuristics.map((h) => ({
        name: h.name,
        score: h.score,
      })),
      overallJudgment: row.overallJudgment?.summary,
    })),
    judgmentNotFromEvidence: true,
    separatedFromUnderstanding: true,
  };
}

/**
 * Strengthen or weaken heuristics from customer outcomes without deleting them.
 * @param {object[]} heuristicLibrary
 * @param {object} input
 * @returns {object}
 */
function learnFromOutcome(heuristicLibrary = [], input = {}) {
  const library = cloneHeuristicLibrary(heuristicLibrary.length ? heuristicLibrary : INITIAL_HEURISTICS);
  const outcome = input.outcome;
  const contributingIds = input.contributingHeuristicIds || input.heuristicIds || [];

  if (!outcome || !contributingIds.length) {
    return { library, updated: [], outcome: null };
  }

  const delta = outcome === OUTCOME_KINDS.WON ? STRENGTH_DELTA_WON : -STRENGTH_DELTA_LOST;
  const updated = [];

  for (const heuristic of library) {
    if (!contributingIds.includes(heuristic.id)) continue;

    const previousStrength = heuristic.strength;
    let nextStrength = previousStrength + delta;
    nextStrength = Number(Math.min(STRENGTH_MAX, Math.max(STRENGTH_MIN, nextStrength)).toFixed(2));
    heuristic.strength = nextStrength;
    heuristic.learnedFrom = {
      outcome,
      at: new Date().toISOString(),
      previousStrength,
      nextStrength,
    };
    heuristic.updatedAt = new Date().toISOString();
    updated.push({
      id: heuristic.id,
      name: heuristic.name,
      previousStrength,
      nextStrength,
      outcome,
    });
  }

  return {
    library,
    updated,
    outcome,
    spec: 'SPEC-162',
  };
}

module.exports = {
  activateHeuristics,
  explainJudgment,
  buildRecommendationFromHeuristics,
  buildBusinessJudgmentReport,
  learnFromOutcome,
  matchHeuristicsForContext,
  detectHeuristicContradictions,
  computeOverallJudgment,
  scoreHeuristicActivation,
  collectEntityContext,
  collectMarketContext,
  STRENGTH_DELTA_WON,
  STRENGTH_DELTA_LOST,
  STRENGTH_MIN,
  STRENGTH_MAX,
};
