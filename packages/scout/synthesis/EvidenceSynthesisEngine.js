'use strict';

/**
 * SPEC-160 — Evidence Synthesis Engine.
 * ADR-080 — Evidence informs understanding; understanding informs recommendations.
 *
 * Pipeline: Evidence → Synthesis → Understanding → Confidence
 *
 * Scout never recommends from isolated observations.
 */

const { recordsMatch, mergeResolved } = require('../../max/scoutAcquisition/EntityResolution');
const { normalizeName } = require('../../capabilities/discovery/dedupe');
const {
  buildEvidence,
  buildUnderstanding,
  computeUnderstandingConfidence,
  UNDERSTANDING_KINDS,
  SYNTHESIS_REVISION_KINDS,
  nowIso,
  asText,
} = require('./types');

const ASSERTION_RULES = [
  {
    id: 'str_management',
    kind: UNDERSTANDING_KINDS.BUSINESS_MODEL,
    patterns: [
      /property\s+management/i,
      /vacation\s+rental/i,
      /short[-\s]?term\s+rental/i,
      /airbnb|vrbo|guest\s+services/i,
    ],
    minMatches: 2,
    assertion: 'Likely manages short-term rentals',
    growthAssertion: 'Growing STR management company',
  },
  {
    id: 'cleaning_need',
    kind: UNDERSTANDING_KINDS.SERVICE_NEED,
    patterns: [/hiring\s+clean/i, /cleaner/i, /cleaning\s+staff/i, /janitorial/i, /housekeeping/i],
    minMatches: 1,
    assertion: 'Likely recurring cleaning need',
  },
  {
    id: 'expansion',
    kind: UNDERSTANDING_KINDS.GROWTH,
    patterns: [/expand/i, /growth/i, /new\s+location/i, /hiring/i, /recently\s+expanded/i],
    minMatches: 1,
    assertion: 'Expanding rapidly',
  },
  {
    id: 'owner_decision_maker',
    kind: UNDERSTANDING_KINDS.DECISION_MAKER,
    patterns: [/owner/i, /founder/i, /president/i, /principal/i],
    minMatches: 1,
    assertion: 'Decision maker likely owner',
  },
];

const CONTRADICTION_RULES = [
  {
    id: 'commercial_vs_residential',
    priorPattern: /commercial\s+only|commercial\s+cleaning\s+only/i,
    newPattern: /residential\s+clean/i,
    priorAssertion: 'Commercial only',
    revisedAssertion: 'Mixed commercial and residential signals',
  },
  {
    id: 'residential_vs_str',
    priorPattern: /residential\s+only/i,
    newPattern: /vacation\s+rental|short[-\s]?term\s+rental/i,
    priorAssertion: 'Residential only',
    revisedAssertion: 'Residential and vacation rental signals',
  },
];

function observationText(evidence) {
  return asText(evidence.observation || evidence.label || evidence.text).toLowerCase();
}

function matchesPatterns(text, patterns = []) {
  return patterns.filter((p) => p.test(text)).length;
}

function normalizeCandidateEvidence(candidate) {
  const entityId = candidate.id || candidate.companyId;
  const entityName = asText(candidate.name || candidate.companyName);
  const items = [];

  for (const signal of candidate.signals || []) {
    items.push(
      buildEvidence({
        id: signal.id || `signal:${entityId}:${signal.type || items.length}`,
        source: signal.source || signal.type || 'signal',
        observation: signal.label || signal.text || signal.type,
        timestamp: signal.observedAt,
        entityId,
        entityName,
        provenance: { provider: signal.source, signalType: signal.type },
      })
    );
  }

  for (const row of candidate.evidence || []) {
    items.push(
      buildEvidence({
        id: row.id || `evidence:${entityId}:${items.length}`,
        source: row.source || row.kind || 'collected',
        observation: row.label || row.observation || row.kind,
        timestamp: row.observedAt,
        entityId,
        entityName,
        provenance: row.provenance || { provider: row.source },
      })
    );
  }

  if (candidate.category || candidate.industry) {
    items.push(
      buildEvidence({
        id: `category:${entityId}`,
        source: candidate.discoverySource || 'google_places',
        observation: `Business category: ${candidate.category || candidate.industry}`,
        entityId,
        entityName,
      })
    );
  }

  if (candidate.website) {
    items.push(
      buildEvidence({
        id: `website:${entityId}`,
        source: 'website',
        observation: `Website indicates ${candidate.websiteDescription || candidate.vertical || 'online presence'}`,
        entityId,
        entityName,
        provenance: { url: candidate.website },
      })
    );
  }

  return items;
}

function pickCanonicalName(candidates = []) {
  const names = candidates.map((c) => asText(c.name || c.companyName)).filter(Boolean);
  if (!names.length) return 'Unknown entity';
  return names.sort((a, b) => b.length - a.length)[0];
}

function sharedEntityToken(nameA, nameB) {
  const a = normalizeName(nameA);
  const b = normalizeName(nameB);
  if (!a || !b) return false;
  if (a === b) return true;

  const tokenA = a.split(/\s+/)[0];
  const tokenB = b.split(/\s+/)[0];
  if (tokenA.length >= 3 && tokenA === tokenB) return true;

  return a.includes(b) || b.includes(a);
}

function recordsMatchForSynthesis(a, b) {
  if (recordsMatch(a, b)) return true;
  const nameA = asText(a.name || a.companyName);
  const nameB = asText(b.name || b.companyName);
  return sharedEntityToken(nameA, nameB);
}

/**
 * Merge candidates that represent the same organization.
 * @param {object[]} candidates
 * @returns {{ groups: object[], merges: object[] }}
 */
function resolveEntityGroups(candidates = []) {
  const groups = [];
  const merges = [];

  for (const candidate of candidates) {
    let matchedIndex = -1;
    for (let i = 0; i < groups.length; i += 1) {
      if (recordsMatchForSynthesis(groups[i].canonical, candidate)) {
        matchedIndex = i;
        break;
      }
    }

    if (matchedIndex >= 0) {
      const group = groups[matchedIndex];
      group.members.push(candidate);
      group.canonical = mergeResolved(group.canonical, candidate);
      group.aliases = [
        ...new Set([
          ...group.aliases,
          asText(candidate.name || candidate.companyName),
          ...(candidate.aliases || []),
        ]),
      ].filter((name) => name && name !== pickCanonicalName([group.canonical]));
      merges.push({
        kind: SYNTHESIS_REVISION_KINDS.ENTITY_MERGED,
        at: nowIso(),
        entity: pickCanonicalName([group.canonical]),
        mergedNames: group.aliases,
        reason: 'Probable representations of one organization resolved to single entity',
      });
    } else {
      groups.push({
        canonical: { ...candidate },
        members: [candidate],
        aliases: (candidate.aliases || []).slice(),
        entityId: candidate.id || candidate.companyId || `entity-${normalizeName(candidate.name)}`,
      });
    }
  }

  return { groups, merges };
}

function detectAssertionsFromEvidence(evidence = []) {
  const assertions = [];
  const combined = evidence.map(observationText).join(' | ');

  for (const rule of ASSERTION_RULES) {
    const matchCount = matchesPatterns(combined, rule.patterns);
    if (matchCount >= rule.minMatches) {
      const sources = [...new Set(evidence.map((e) => e.source))];
      const useGrowth =
        rule.id === 'str_management' && sources.length >= 3 && /hiring|clean/i.test(combined);
      assertions.push({
        ruleId: rule.id,
        kind: rule.kind,
        text: useGrowth && rule.growthAssertion ? rule.growthAssertion : rule.assertion,
        supportingEvidenceIds: evidence.map((e) => e.id),
      });
    }
  }

  return assertions;
}

function detectContradictions(existingUnderstandings = [], newEvidence = []) {
  const contradictions = [];
  const newText = newEvidence.map(observationText).join(' | ');

  for (const understanding of existingUnderstandings) {
    for (const rule of CONTRADICTION_RULES) {
      const priorText = (understanding.assertions || []).join(' | ');
      if (rule.priorPattern.test(priorText) && rule.newPattern.test(newText)) {
        contradictions.push({
          ruleId: rule.id,
          understandingId: understanding.id,
          priorAssertion: rule.priorAssertion,
          revisedAssertion: rule.revisedAssertion,
          contradictoryEvidence: newEvidence.filter((e) => rule.newPattern.test(observationText(e))),
        });
      }
    }
  }

  return contradictions;
}

function buildReasoning(assertions, supportingEvidence, contradictoryEvidence = []) {
  const sources = [...new Set(supportingEvidence.map((e) => e.source))];
  const parts = [
    `${assertions.length} assertion(s) synthesized from ${supportingEvidence.length} observation(s) across ${sources.length} source(s).`,
  ];

  if (sources.length) {
    parts.push(`Sources: ${sources.join(', ')}.`);
  }

  for (const evidence of supportingEvidence.slice(0, 4)) {
    parts.push(`${evidence.source}: ${evidence.observation}`);
  }

  if (contradictoryEvidence.length) {
    parts.push(
      `${contradictoryEvidence.length} contradictory observation(s) retained — confidence reduced.`
    );
  }

  return parts.join(' ');
}

function synthesizeEntityUnderstanding(entityGroup) {
  const entityName = pickCanonicalName([entityGroup.canonical, ...entityGroup.members]);
  const entityId = entityGroup.entityId;
  const evidence = entityGroup.members.flatMap((member) => normalizeCandidateEvidence(member));
  const detected = detectAssertionsFromEvidence(evidence);

  if (!detected.length) {
    return null;
  }

  const primary = detected[0];
  const relatedEvidence = evidence.filter((e) =>
    primary.supportingEvidenceIds.includes(e.id)
  );
  const supporting =
    relatedEvidence.length >= 2 ? relatedEvidence : evidence.slice(0, Math.max(3, relatedEvidence.length));

  const understanding = buildUnderstanding({
    entity: entityName,
    entityId,
    aliases: entityGroup.aliases,
    kind: primary.kind,
    assertions: detected.map((a) => a.text),
    supportingEvidence: supporting,
    reasoning: buildReasoning(detected, supporting),
  });

  return understanding;
}

/**
 * Revise an existing understanding when contradictory evidence appears.
 * Contradictions are never discarded.
 */
function reviseUnderstandingWithContradiction(understanding, contradiction) {
  const contradictoryEvidence = [
    ...(understanding.contradictoryEvidence || []),
    ...contradiction.contradictoryEvidence,
  ];

  const revisedAssertions = (understanding.assertions || []).map((text) =>
    text === contradiction.priorAssertion ? contradiction.revisedAssertion : text
  );
  if (!revisedAssertions.includes(contradiction.revisedAssertion)) {
    revisedAssertions.push(contradiction.revisedAssertion);
  }

  const confidence = computeUnderstandingConfidence(
    understanding.supportingEvidence,
    contradictoryEvidence
  );

  const revision = {
    at: nowIso(),
    kind: SYNTHESIS_REVISION_KINDS.CONTRADICTION,
    reason: `Contradictory evidence: ${contradiction.priorAssertion} vs new observation`,
    before: { assertions: understanding.assertions, confidence: understanding.confidence },
    after: { assertions: revisedAssertions, confidence },
  };

  return {
    ...understanding,
    assertions: revisedAssertions,
    contradictoryEvidence,
    confidence,
    reasoning: buildReasoning(
      revisedAssertions.map((text) => ({ text })),
      understanding.supportingEvidence,
      contradictoryEvidence
    ),
    revisionHistory: [...(understanding.revisionHistory || []), revision],
    updatedAt: nowIso(),
  };
}

/**
 * Operator asks: "Why do you believe this?"
 * @param {object} understanding
 * @returns {object}
 */
function explainUnderstanding(understanding = {}) {
  return {
    entity: understanding.entity,
    assertions: understanding.assertions || [],
    confidence: understanding.confidence,
    reasoning: understanding.reasoning,
    supportingEvidence: (understanding.supportingEvidence || []).map((e) => ({
      id: e.id,
      source: e.source,
      observation: e.observation,
      timestamp: e.timestamp,
    })),
    contradictoryEvidence: (understanding.contradictoryEvidence || []).map((e) => ({
      id: e.id,
      source: e.source,
      observation: e.observation,
      timestamp: e.timestamp,
    })),
    revisionHistory: understanding.revisionHistory || [],
  };
}

/**
 * Full synthesis pass over discovered candidates.
 * @param {object} input
 * @returns {object}
 */
function synthesizeFromCandidates(input = {}) {
  const candidates = input.candidates || [];
  const priorUnderstandings = input.priorUnderstandings || [];
  const newEvidence = input.newEvidence || [];

  const { groups, merges } = resolveEntityGroups(candidates);
  const understandings = [];
  const revisions = [...merges];

  for (const group of groups) {
    const synthesized = synthesizeEntityUnderstanding(group);
    if (synthesized) {
      understandings.push(synthesized);
      revisions.push({
        at: nowIso(),
        kind: SYNTHESIS_REVISION_KINDS.CREATED,
        entity: synthesized.entity,
        assertions: synthesized.assertions,
        confidence: synthesized.confidence,
        reason: 'Multiple weak signals synthesized into business understanding',
      });
    }
  }

  let combined = understandings;
  const contradictions = detectContradictions(
    [...priorUnderstandings, ...combined],
    newEvidence.length ? newEvidence : candidates.flatMap(normalizeCandidateEvidence)
  );

  for (const contradiction of contradictions) {
    combined = combined.map((u) =>
      u.id === contradiction.understandingId ||
      (u.assertions || []).includes(contradiction.priorAssertion)
        ? reviseUnderstandingWithContradiction(u, contradiction)
        : u
    );
  }

  const buyingSignals = combined.filter((u) => u.kind === UNDERSTANDING_KINDS.SERVICE_NEED).length;
  const growthSignals = combined.filter((u) => u.kind === UNDERSTANDING_KINDS.GROWTH).length;
  const contradictionCount = combined.reduce(
    (sum, u) => sum + (u.contradictoryEvidence || []).length,
    0
  );
  const avgConfidence =
    combined.length > 0
      ? Number(
          (combined.reduce((s, u) => s + u.confidence, 0) / combined.length).toFixed(2)
        )
      : 0;

  return {
    understandings: combined,
    entityMerges: merges,
    revisions,
    contradictions,
    summary: {
      entityCount: groups.length,
      understandingCount: combined.length,
      buyingSignals,
      growthSignals,
      contradictions: contradictionCount,
      averageConfidence: avgConfidence,
    },
  };
}

/**
 * Incrementally add evidence and re-synthesize understandings.
 */
function applyEvidenceToUnderstandings(existingUnderstandings = [], newEvidence = []) {
  const prior = existingUnderstandings.slice();
  const contradictions = detectContradictions(prior, newEvidence);
  let next = prior.slice();

  for (const contradiction of contradictions) {
    next = next.map((u) =>
      (u.assertions || []).includes(contradiction.priorAssertion)
        ? reviseUnderstandingWithContradiction(u, contradiction)
        : u
    );
  }

  return { understandings: next, contradictions, revised: contradictions.length > 0 };
}

/**
 * Build mission-report-ready business understanding section.
 */
function buildBusinessUnderstandingReport(understandings = [], synthesisResult = {}) {
  const items = understandings.map((u) => ({
    entity: u.entity,
    aliases: u.aliases || [],
    assertions: u.assertions,
    confidence: u.confidence,
    reasoning: u.reasoning,
    supportingEvidence: (u.supportingEvidence || []).map((e) => ({
      source: e.source,
      observation: e.observation,
    })),
    contradictoryEvidence: (u.contradictoryEvidence || []).map((e) => ({
      source: e.source,
      observation: e.observation,
    })),
  }));

  return {
    businessUnderstanding: items,
    growth: synthesisResult.summary?.growthSignals ? 'High' : 'Unknown',
    buyingSignals: synthesisResult.summary?.buyingSignals || 0,
    contradictions: synthesisResult.summary?.contradictions || 0,
    confidence: synthesisResult.summary?.averageConfidence || 0,
    entityMerges: synthesisResult.entityMerges || [],
    synthesizedNotRaw: true,
  };
}

module.exports = {
  normalizeCandidateEvidence,
  resolveEntityGroups,
  detectAssertionsFromEvidence,
  detectContradictions,
  synthesizeEntityUnderstanding,
  reviseUnderstandingWithContradiction,
  explainUnderstanding,
  synthesizeFromCandidates,
  applyEvidenceToUnderstandings,
  buildBusinessUnderstandingReport,
  ASSERTION_RULES,
  CONTRADICTION_RULES,
};
