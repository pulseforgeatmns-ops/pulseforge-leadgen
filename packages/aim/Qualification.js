'use strict';

/**
 * SPEC-112 Phase 3 — Qualification model.
 * Scout reasons. Scores are explainable. Unknown stays unknown.
 */

const { RECOMMENDATIONS, SCORE_DIMENSIONS, toPercent, nowIso, asText } = require('./types');
const { evaluateIcpFit } = require('./MarketUnderstanding');
const { matchPainSignals } = require('./PainOntology');

const TIMELY_MS = 90 * 24 * 60 * 60 * 1000;

const READINESS_SIGNALS = [
  'hiring',
  'job posting',
  'job postings',
  'growth announcement',
  'growth announcements',
  'financing',
  'price increase',
  'price increases',
  'cost-cutting',
  'cost cutting',
];

function dimension(score01, reasons, unknowns = []) {
  return {
    score: toPercent(score01),
    score01: Number(Math.max(0, Math.min(1, score01)).toFixed(4)),
    reasons: Array.isArray(reasons) ? reasons.filter(Boolean) : [],
    unknowns: Array.isArray(unknowns) ? unknowns.filter(Boolean) : [],
  };
}

function uniqueSources(prospect = {}, extraSignals = []) {
  const rows = [
    ...(Array.isArray(prospect.signals) ? prospect.signals : []),
    ...((extraSignals || []).filter((s) => s && typeof s === 'object')),
  ];
  const sources = new Set();
  for (const row of rows) {
    if (row.source) sources.add(String(row.source));
    else if (row.type) sources.add(String(row.type));
  }
  if (prospect.website) sources.add('website');
  if (prospect.reviewReplyPattern) sources.add('reviews');
  return sources;
}

function timelySignalCount(prospect = {}, now = Date.now()) {
  const rows = Array.isArray(prospect.signals) ? prospect.signals : [];
  return rows.filter((row) => {
    const at = row.observedAt || row.observed_at;
    if (!at) return false;
    const ts = Date.parse(at);
    return Number.isFinite(ts) && now - ts <= TIMELY_MS;
  }).length;
}

function buyingReadinessScore(pain, prospect, now) {
  const blob = [
    prospect.notes,
    prospect.description,
    ...(Array.isArray(prospect.signals) ? prospect.signals.map((s) => s.label || s.text || s.type) : []),
  ]
    .filter(Boolean)
    .join(' ')
    .toLowerCase();
  const hits = READINESS_SIGNALS.filter((s) => blob.includes(s));
  const timely = timelySignalCount(prospect, now);
  if (!pain.matches.length && !hits.length) {
    return dimension(0.15, ['No timely pressure signals observed.'], [
      'Buying readiness is unknown — AIM does not invent intent.',
    ]);
  }
  const painBoost = pain.topPain ? pain.topPain.score * 0.45 : 0;
  const hitBoost = Math.min(hits.length, 3) * 0.12;
  const timeBoost = Math.min(timely, 2) * 0.1;
  const score = Math.min(0.92, 0.2 + painBoost + hitBoost + timeBoost);
  const reasons = [];
  if (pain.topPain) {
    reasons.push(`${pain.topPain.label} is evidenced — pressure exists in the operating loop.`);
  }
  if (hits.length) reasons.push(`Timely pressure signal: ${hits[0]}.`);
  if (timely) reasons.push(`${timely} signal(s) observed inside 90 days.`);
  return dimension(score, reasons, hits.length ? [] : ['No dated buying-pressure signal yet.']);
}

function evidenceQualityScore(pain, prospect, extraSignals) {
  const sources = uniqueSources(prospect, extraSignals);
  const matchCount = pain.matches.reduce((n, m) => n + m.hits.length, 0);
  if (!matchCount && sources.size === 0) {
    return dimension(0.12, [], ['Almost no observable evidence — quality cannot be high.']);
  }
  const score = Math.min(
    0.95,
    0.18 + Math.min(matchCount, 5) * 0.12 + Math.min(sources.size, 4) * 0.1
  );
  const reasons = [];
  if (matchCount) reasons.push(`${matchCount} ontology signal hit(s).`);
  if (sources.size) reasons.push(`${sources.size} independent source type(s).`);
  const unknowns = sources.size < 2 ? ['Evidence is concentrated in one source family.'] : [];
  return dimension(score, reasons, unknowns);
}

function confidenceScore(icp, evidence, pain) {
  const knownIcp = ['company', 'founder', 'size', 'geography'].filter((k) => {
    const field = icp && icp[k];
    return field && field.known;
  }).length;
  const icpCoverage = knownIcp / 4;
  const painCoverage = pain.matches.length ? Math.min(1, pain.matches.length / 3) : 0;
  const score = Math.min(
    0.93,
    evidence.score01 * 0.5 + icpCoverage * 0.25 + painCoverage * 0.25
  );
  const unknowns = [];
  if (icpCoverage < 1) unknowns.push('Some ICP dimensions remain unnamed or unevidenced.');
  if (!pain.matches.length) unknowns.push('No pain signals matched — confidence cannot be high.');
  return dimension(score, [
    `ICP coverage ${knownIcp}/4 named dimensions; ${pain.matches.length} pain match(es); evidence ${evidence.score}.`,
  ], unknowns);
}

function recommend({ icp, pain, evidence, readiness, excluded }) {
  if (excluded) {
    return {
      id: RECOMMENDATIONS.REJECT,
      label: 'Reject',
      reason: 'Prospect matches an AIM exclusion.',
    };
  }
  if (evidence.score < 30 && icp.score < 50) {
    return {
      id: RECOMMENDATIONS.UNKNOWN,
      label: 'Unknown',
      reason: 'Evidence is too thin to recommend pursuit. AIM does not guess.',
    };
  }
  if (icp.score >= 70 && pain.score >= 60 && evidence.score >= 50) {
    return {
      id: RECOMMENDATIONS.PURSUE,
      label: 'Pursue',
      reason: 'Strong ICP fit with evidenced pain and usable evidence quality.',
    };
  }
  if (icp.score >= 60 && pain.score >= 40) {
    return {
      id: RECOMMENDATIONS.NURTURE,
      label: 'Nurture',
      reason: 'Fit is real; pain or evidence is only partial.',
    };
  }
  if (icp.score >= 50) {
    return {
      id: RECOMMENDATIONS.WATCH,
      label: 'Watch',
      reason: 'Possible fit without enough pain evidence to act.',
    };
  }
  return {
    id: RECOMMENDATIONS.REJECT,
    label: 'Reject',
    reason: 'Does not resemble the businesses this client transforms.',
  };
}

/**
 * Qualify one prospect against an AIM.
 *
 * @param {object} aim
 * @param {object} prospect
 * @param {object} [opts]
 */
function qualifyProspect(aim, prospect = {}, opts = {}) {
  if (!aim || !aim.icp || !aim.painOntology) {
    throw new Error('AIM with icp and painOntology is required to qualify.');
  }
  const now = opts.now != null ? Number(opts.now) : Date.now();
  const extraSignals = opts.signals || [];
  const icpEval = evaluateIcpFit(aim.icp, prospect);
  const pain = matchPainSignals(aim.painOntology, prospect, { signals: extraSignals });
  const painScore01 = pain.topPain ? pain.topPain.score : 0;
  const icp = dimension(icpEval.score, icpEval.reasons, icpEval.unknowns);
  const painMatch = dimension(
    painScore01,
    pain.matches.map((m) => `${m.label}: ${m.percent}% (${m.hits.join(', ')})`),
    pain.matches.length ? [] : ['No ontology pain is evidenced yet.']
  );
  const evidence = evidenceQualityScore(pain, prospect, extraSignals);
  const readiness = buyingReadinessScore(pain, prospect, now);
  const confidence = confidenceScore(aim.icp, evidence, pain);
  const overall = recommend({
    icp,
    pain: painMatch,
    evidence,
    readiness,
    excluded: icpEval.excluded,
  });

  return {
    kind: 'aim_qualification',
    spec: 'SPEC-112',
    isOperatingFact: false,
    aimId: aim.id,
    clientKey: aim.clientKey,
    prospectId: asText(prospect.id || prospect.prospectId || prospect.companyId),
    prospectName: asText(prospect.name || prospect.companyName),
    dimensions: {
      icpFit: icp,
      painMatch,
      evidenceQuality: evidence,
      buyingReadiness: readiness,
      confidence,
    },
    pains: pain.matches,
    topPain: pain.topPain,
    overallRecommendation: overall,
    excluded: icpEval.excluded,
    createdAt: nowIso(opts.now),
  };
}

function isQualified(qualification) {
  if (!qualification) return false;
  const rec = qualification.overallRecommendation && qualification.overallRecommendation.id;
  return rec === RECOMMENDATIONS.PURSUE || rec === RECOMMENDATIONS.NURTURE;
}

module.exports = {
  SCORE_DIMENSIONS,
  qualifyProspect,
  isQualified,
  recommend,
};
