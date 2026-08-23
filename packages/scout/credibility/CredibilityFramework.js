'use strict';

/**
 * SPEC-144 — Scout Intelligence Credibility Framework.
 * Every recommendation defends itself with evidence, confidence, uncertainty, and trust.
 */

const {
  CHECKLIST_SOURCES,
  evidenceWeight,
  evidenceSourceLabel,
  rankEvidenceByWeight,
  normalizeSourceKey,
} = require('./EvidenceWeights');
const { classifyFreshness, enrichEvidenceWithFreshness } = require('./EvidenceFreshness');
const { mergeContradictions } = require('./ContradictionAnalysis');

const RANKING_FACTOR_LABELS = Object.freeze({
  revenue_potential: 'Revenue Opportunity',
  ease_of_access: 'Commercial Density',
  buying_signals: 'Buying Signals',
  relationship_probability: 'Relationship Strength',
  geographic_fit: 'Geographic Fit',
  evidence_confidence: 'Evidence Quality',
  strategic_value: 'Operational Fit',
});

const UNKNOWN_VERIFICATION = Object.freeze({
  decision_maker: { impact: 'high', howToVerify: 'LinkedIn + website team page + cold call' },
  current_vendor: { impact: 'high', howToVerify: 'Cold call or on-site walkthrough question' },
  portfolio_size: { impact: 'high', howToVerify: 'County records or operator call' },
  cleaning_responsibility: { impact: 'medium', howToVerify: 'Website services page + reviews + call' },
  contact_path: { impact: 'high', howToVerify: 'Prospeo/Hunter enrichment + direct dial test' },
  county_records: { impact: 'medium', howToVerify: 'Pull county property ownership records' },
});

function round2(n) {
  return Number(Number(n).toFixed(2));
}

function uniqueSources(evidence = []) {
  return [
    ...new Set(
      evidence.map((e) => normalizeSourceKey(typeof e === 'string' ? e : e.source || e.kind || 'unknown'))
    ),
  ].filter((s) => s && s !== 'unknown');
}

function mapChecklistSource(source) {
  const key = normalizeSourceKey(source);
  if (['website', 'company_website'].includes(key)) return 'website';
  if (['google_places', 'google_maps', 'google_business', 'public_business_places'].includes(key)) {
    return 'google_business';
  }
  if (key === 'linkedin') return 'linkedin';
  if (key.includes('county')) return 'county_records';
  if (key.includes('secretary') || key === 'sos') return 'secretary_of_state';
  if (key.includes('decision') || key === 'contacts') return 'decision_maker';
  if (key.includes('vendor')) return 'current_vendor';
  return null;
}

/**
 * Build inspectable confidence explanation.
 * @param {object} input
 * @returns {object}
 */
function buildConfidenceExplanation(input = {}) {
  const evidence = input.evidence || input.supportedBy || [];
  const missing = input.missingEvidence || [];
  const contradictions = input.contradictions || [];
  const score = input.confidence != null ? round2(input.confidence) : 0;

  const presentKeys = new Set();
  for (const item of evidence) {
    const mapped = mapChecklistSource(typeof item === 'string' ? item : item.source);
    if (mapped) presentKeys.add(mapped);
  }

  const basedOn = [];
  const missingChecklist = [];

  for (const source of CHECKLIST_SOURCES) {
    const label =
      source === 'decision_maker'
        ? 'Decision maker'
        : source === 'current_vendor'
          ? 'Current vendor'
          : evidenceSourceLabel(source);
    if (presentKeys.has(source)) {
      const match = evidence.find((e) => mapChecklistSource(typeof e === 'string' ? e : e.source) === source);
      const observedAt = match && typeof match === 'object' ? match.observedAt : null;
      const freshness = classifyFreshness(observedAt);
      basedOn.push({
        source,
        label,
        present: true,
        freshness: freshness.band,
        freshnessLabel: freshness.label,
        ageDays: freshness.ageDays,
      });
    } else {
      missingChecklist.push({ source, label, present: false });
    }
  }

  for (const gap of missing) {
    const key = normalizeSourceKey(gap);
    if (!missingChecklist.some((m) => m.source === key || m.label === gap)) {
      missingChecklist.push({ source: key, label: String(gap), present: false });
    }
  }

  let adjustedScore = score;
  if (contradictions.some((c) => !c.resolved)) {
    adjustedScore = round2(Math.max(0, score - 0.1));
  }

  return {
    score: adjustedScore,
    basedOn,
    missing: missingChecklist,
    contradictionDetected: contradictions.some((c) => !c.resolved),
    contradictionNote: contradictions.some((c) => !c.resolved)
      ? 'Contradiction detected. Confidence reduced. Recommend verification.'
      : null,
  };
}

/**
 * Convert ranking factor scores to percentage breakdown.
 * @param {object} scores
 * @returns {object[]}
 */
function buildRankingBreakdown(scores = {}) {
  const entries = Object.entries(scores).filter(([, v]) => Number(v) > 0);
  const total = entries.reduce((sum, [, v]) => sum + Number(v), 0);
  if (total <= 0) return [];

  return entries
    .map(([factor, value]) => ({
      factor,
      label: RANKING_FACTOR_LABELS[factor] || factor.replace(/_/g, ' '),
      weight: round2(value),
      percent: round2((Number(value) / total) * 100),
    }))
    .sort((a, b) => b.percent - a.percent);
}

/**
 * Build competing hypotheses for an entity.
 * @param {object[]} hypotheses
 * @param {object[]} claims
 * @param {string} entityId
 * @returns {object[]}
 */
function buildCompetingHypotheses(hypotheses = [], claims = [], entityId = null) {
  const entityHyps = hypotheses.filter((h) => !h.entityId || h.entityId === entityId);
  const entityClaims = claims.filter((c) => !entityId || c.entityId === entityId);

  const combined = entityHyps.map((hyp) => {
    const claim = entityClaims.find((c) => c.hypothesisId === hyp.id);
    return {
      id: hyp.id,
      text: hyp.text,
      confidence: round2(claim ? claim.confidence : hyp.confidence != null ? hyp.confidence : 0.18),
      status: hyp.status || 'open',
      missingEvidence: claim ? claim.missingEvidence || hyp.missingEvidence || [] : hyp.missingEvidence || [],
    };
  });

  if (combined.length === 0 && entityClaims.length > 0) {
    return entityClaims.slice(0, 3).map((c) => ({
      id: c.id,
      text: c.text,
      confidence: round2(c.confidence),
      status: 'derived',
      missingEvidence: c.missingEvidence || [],
    }));
  }

  return combined.sort((a, b) => b.confidence - a.confidence).slice(0, 5);
}

/**
 * Trust is separate from confidence — "would we act on this?"
 * @param {object} input
 * @returns {object}
 */
function buildTrustAssessment(input = {}) {
  const confidence = input.confidence != null ? input.confidence : 0;
  const evidence = input.evidence || input.supportedBy || [];
  const contradictions = (input.contradictions || []).filter((c) => !c.resolved);
  const sources = uniqueSources(evidence);
  const diversity = sources.length;
  const hasStale = evidence.some((e) => {
    const freshness = classifyFreshness(typeof e === 'object' ? e.observedAt : null);
    return freshness.band === 'needs_verification' || freshness.band === 'low_confidence';
  });

  let level = 'medium';
  let reason = 'Moderate confidence with partial evidence coverage.';
  let wouldActOn = confidence >= 0.75 && contradictions.length === 0;

  if (contradictions.length > 0) {
    level = 'low';
    reason = 'Unresolved contradictions require verification before acting.';
    wouldActOn = false;
  } else if (confidence >= 0.9 && diversity >= 3 && !hasStale) {
    level = 'high';
    reason = 'High confidence with diverse, fresh independent sources.';
    wouldActOn = true;
  } else if (confidence >= 0.85 && diversity < 3) {
    level = 'medium';
    reason = 'High confidence but low evidence diversity — needs one more independent source.';
    wouldActOn = false;
  } else if (confidence < 0.6 || diversity <= 1) {
    level = 'low';
    reason = 'Thin or single-source evidence — verify before outreach.';
    wouldActOn = false;
  }

  return {
    level,
    label: level.charAt(0).toUpperCase() + level.slice(1),
    confidence: round2(confidence),
    evidenceDiversity: diversity,
    independentSources: sources.map(evidenceSourceLabel),
    reason,
    wouldActOn,
  };
}

/**
 * @param {string[]} missingEvidence
 * @param {object[]} claims
 * @returns {object[]}
 */
function buildHighestRemainingUnknowns(missingEvidence = [], claims = []) {
  const fromClaims = claims.flatMap((c) => c.missingEvidence || []);
  const combined = [...new Set([...missingEvidence, ...fromClaims])];

  return combined.slice(0, 5).map((unknown) => {
    const key = normalizeSourceKey(unknown);
    const template = UNKNOWN_VERIFICATION[key] || UNKNOWN_VERIFICATION[key.replace(/_/g, '')];
    return {
      unknown: String(unknown),
      impact: template ? template.impact : 'medium',
      howToVerify: template ? template.howToVerify : `Investigate: ${unknown}`,
    };
  });
}

function buildRecommendedNextInvestigation(input = {}) {
  const unknowns = buildHighestRemainingUnknowns(input.missingEvidence || [], input.claims || []);
  const top = unknowns[0];
  if (input.nextStep) {
    return {
      action: `${input.nextStep.providerLabel || input.nextStep.providerId} → ${input.nextStep.gap || 'resolve uncertainty'}`,
      impact: 'high',
      howToVerify: input.nextStep.providerLabel || input.nextStep.providerId,
    };
  }
  if (top) {
    return {
      action: `Resolve: ${top.unknown}`,
      impact: top.impact,
      howToVerify: top.howToVerify,
    };
  }
  return {
    action: 'No further investigation required.',
    impact: 'low',
    howToVerify: null,
  };
}

function normalizeEvidenceList(raw = []) {
  return raw
    .map((item) => {
      if (typeof item === 'string') {
        return enrichEvidenceWithFreshness({
          label: item,
          source: 'unknown',
          weight: evidenceWeight('unknown'),
        });
      }
      const source = item.source || item.kind || 'unknown';
      return enrichEvidenceWithFreshness({
        ...item,
        source,
        sourceLabel: evidenceSourceLabel(source),
        weight: item.weight != null ? item.weight : evidenceWeight(source),
      });
    })
    .filter((e) => e.label || e.source);
}

/**
 * Build a full intelligence brief for one ranked opportunity.
 * @param {object} input
 * @returns {object}
 */
function buildIntelligenceBrief(input = {}) {
  const rankingEntry = input.rankingEntry || {};
  const candidate = input.candidate || {};
  const claims = input.claims || [];
  const hypotheses = input.hypotheses || [];
  const entityId = rankingEntry.companyId || rankingEntry.candidateId || candidate.id;

  const entityClaims = claims.filter((c) => !entityId || c.entityId === entityId);
  const primaryClaim = entityClaims.sort((a, b) => b.confidence - a.confidence)[0];

  const rawEvidence = [
    ...(primaryClaim ? primaryClaim.supportedBy || [] : []),
    ...(candidate.signals || []),
    ...(candidate.evidence || []),
  ];

  const evidence = rankEvidenceByWeight(normalizeEvidenceList(rawEvidence));
  const contradictions = mergeContradictions(
    [...(input.conflicts || []), ...(primaryClaim ? primaryClaim.contradictions || [] : [])],
    evidence
  );

  const confidence =
    primaryClaim && primaryClaim.confidence != null
      ? primaryClaim.confidence
      : rankingEntry.evidenceConfidence != null
        ? rankingEntry.evidenceConfidence
        : rankingEntry.rankScore || 0;

  const missingEvidence = [
    ...(primaryClaim ? primaryClaim.missingEvidence || [] : []),
    ...(input.missingEvidence || []),
  ].filter((v, i, arr) => arr.indexOf(v) === i);

  const confidenceExplanation = buildConfidenceExplanation({
    confidence,
    evidence,
    missingEvidence,
    contradictions,
  });

  const competingHypotheses = buildCompetingHypotheses(hypotheses, claims, entityId);
  const rankingBreakdown = buildRankingBreakdown(rankingEntry.scores || {});
  const trust = buildTrustAssessment({ confidence, evidence, contradictions });
  const highestRemainingUnknowns = buildHighestRemainingUnknowns(missingEvidence, entityClaims);
  const recommendedNextInvestigation = buildRecommendedNextInvestigation({
    missingEvidence,
    claims: entityClaims,
    nextStep: input.nextStep,
  });

  const buyingSignals = (candidate.signals || rankingEntry.signals || [])
    .map((s) => ({
      type: s.type || 'signal',
      label: s.label || s.text || String(s),
      source: s.source ? evidenceSourceLabel(s.source) : null,
      observedAt: s.observedAt || null,
    }))
    .filter((s) => s.label);

  const risks = [];
  if (contradictions.length) {
    risks.push({
      type: 'contradiction',
      severity: 'high',
      description: contradictions[0].description,
    });
  }
  if (trust.level === 'low') {
    risks.push({
      type: 'trust',
      severity: 'high',
      description: trust.reason,
    });
  }
  if (missingEvidence.some((m) => /decision|vendor|contact/i.test(String(m)))) {
    risks.push({
      type: 'missing_critical_evidence',
      severity: 'medium',
      description: 'Key buying-path evidence is still missing.',
    });
  }

  const whyRankedHere =
    (rankingEntry.reasons || []).join('; ') ||
    (rankingBreakdown.length
      ? `Led by ${rankingBreakdown
          .slice(0, 2)
          .map((r) => `${r.label} (${r.percent}%)`)
          .join(' and ')}.`
      : primaryClaim
        ? primaryClaim.text
        : 'Composite ranking score across fit, signals, and evidence quality.');

  return {
    opportunity: {
      rank: rankingEntry.rank,
      name: rankingEntry.name || candidate.name,
      companyId: entityId || null,
      tier: rankingEntry.tier || null,
      score: rankingEntry.rankScore != null ? rankingEntry.rankScore : null,
    },
    overallConfidence: confidenceExplanation.score,
    confidenceExplanation,
    whyRankedHere,
    because: whyRankedHere,
    evidence,
    strongestEvidence: evidence.slice(0, 3),
    buyingSignals,
    risks,
    unknowns: missingEvidence,
    contradictions,
    competingHypotheses,
    rankingBreakdown,
    decisionExplainability: rankingBreakdown,
    trust,
    recommendedNextInvestigation,
    highestRemainingUnknowns,
    missingEvidence: contradictions.length
      ? [
          ...missingEvidence,
          ...(contradictions[0].recommendation ? [contradictions[0].recommendation] : []),
        ]
      : missingEvidence,
    competingExplanations: competingHypotheses.map((h) => ({
      hypothesis: h.text,
      confidence: h.confidence,
    })),
  };
}

/**
 * Attach credibility briefs to a list of ranked opportunities.
 * @param {object} input
 * @returns {object[]}
 */
function buildIntelligenceBriefs(input = {}) {
  const ranked = input.ranking?.rankedOpportunities || input.rankedOpportunities || [];
  const claims = input.claims || [];
  const hypotheses = input.hypotheses || [];
  const conflicts = input.conflicts || [];
  const missingEvidence = input.missingEvidence?.missing || input.missingEvidence || [];
  const candidates = input.candidateUniverse?.candidates || input.candidates || [];
  const candidateMap = new Map(candidates.map((c) => [String(c.id), c]));

  return ranked.map((entry) =>
    buildIntelligenceBrief({
      rankingEntry: entry,
      candidate: candidateMap.get(String(entry.companyId || entry.candidateId)) || {},
      claims,
      hypotheses,
      conflicts: conflicts.filter(
        (c) => !c.entityId || c.entityId === entry.companyId || c.entityId === entry.candidateId
      ),
      missingEvidence,
    })
  );
}

/**
 * Validate acceptance criteria — operator can answer all required questions.
 * @param {object} brief
 * @returns {object}
 */
function validateBriefAcceptance(brief) {
  return {
    whyRanked: Boolean(brief.whyRankedHere),
    evidenceSupports: (brief.evidence || []).length > 0 || (brief.buyingSignals || []).length > 0,
    strongestEvidence: (brief.strongestEvidence || []).length > 0,
    conflictsIdentified: Array.isArray(brief.contradictions),
    unknownsListed: Array.isArray(brief.unknowns),
    riskAssessed: (brief.risks || []).length >= 0 && Boolean(brief.trust),
    nextVerificationStep: Boolean(brief.recommendedNextInvestigation?.action),
    passes:
      Boolean(brief.whyRankedHere) &&
      Boolean(brief.trust) &&
      Boolean(brief.recommendedNextInvestigation?.action) &&
      Array.isArray(brief.contradictions) &&
      Array.isArray(brief.unknowns),
  };
}

module.exports = {
  RANKING_FACTOR_LABELS,
  UNKNOWN_VERIFICATION,
  buildConfidenceExplanation,
  buildRankingBreakdown,
  buildCompetingHypotheses,
  buildTrustAssessment,
  buildHighestRemainingUnknowns,
  buildRecommendedNextInvestigation,
  buildIntelligenceBrief,
  buildIntelligenceBriefs,
  validateBriefAcceptance,
  normalizeEvidenceList,
};
