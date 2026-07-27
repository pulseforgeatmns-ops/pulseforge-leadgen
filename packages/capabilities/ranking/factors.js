'use strict';

/**
 * Explainable opportunity ranking factors (SPEC-026).
 * Every score references evidence only — absent evidence → 0 / risk, never invented.
 */

const {
  FACTOR_MAX,
  FACTOR_LABELS,
  buildFactorScore,
} = require('./types');
const {
  resolveActiveSignals,
  buyingSignalsForRanking,
} = require('../signals');

const DECISION_MAKER_TITLES =
  /\b(owner|founder|principal|partner|president|ceo|coo|office\s*manager|managing\s*partner|director)\b/i;

const BUYING_SIGNAL_KEYS = [
  'hiring',
  'hiringActivity',
  'hiring_activity',
  'expanding',
  'newLocation',
  'new_location',
  'facilityOps',
  'facility_ops',
  'buyingSignal',
  'buyingSignals',
];

/**
 * Score one prospect across all factors.
 * @param {object} prospect
 * @param {object} [ctx]
 * @param {object} [ctx.profile]
 * @param {object} [ctx.knowledge]
 * @param {object[]} [ctx.historicalOutcomes]
 * @returns {{ overallScore: number, factorScores: object[], topReasons: string[], risks: string[], confidence: number }}
 */
function scoreOpportunity(prospect, ctx = {}) {
  const profile = ctx.profile || null;
  const knowledge = resolveKnowledge(prospect, ctx.knowledge);
  const outcomes = Array.isArray(ctx.historicalOutcomes)
    ? ctx.historicalOutcomes
    : [];

  const factorScores = [
    scoreProfileMatch(prospect, profile),
    scoreBuyingSignals(prospect, knowledge),
    scoreCompanySize(prospect, knowledge),
    scoreDecisionMaker(prospect, knowledge),
    scorePersonalization(prospect, knowledge),
    scoreGeographicFit(prospect, profile),
    scoreHistoricalSuccess(prospect, profile, outcomes, knowledge),
    scoreEvidenceConfidence(prospect),
  ];

  const overallScore = factorScores.reduce((s, f) => s + f.score, 0);
  const matched = factorScores
    .filter((f) => f.matched && f.score > 0)
    .sort((a, b) => b.score - a.score);

  const topReasons = matched.slice(0, 4).map((f) => f.detail);

  const risks = collectRisks(prospect, factorScores, profile);
  const confidence = computeRankingConfidence(prospect, factorScores);

  return {
    overallScore: Math.max(0, Math.min(100, Math.round(overallScore))),
    factorScores,
    topReasons:
      topReasons.length > 0
        ? topReasons
        : ['Limited evidence — ranked cautiously for operator review'],
    risks,
    confidence,
  };
}

function scoreProfileMatch(prospect, profile) {
  const max = FACTOR_MAX.profile_match;
  const signals = Array.isArray(prospect.rankingSignals)
    ? prospect.rankingSignals
    : [];
  const positive = signals.filter((s) => s.matched && Number(s.weight) > 0);
  const industry = String(prospect.industry || '').toLowerCase();
  const targets = (profile && profile.industryTargets) || [];
  const industryHit = targets.some(
    (t) =>
      industry.includes(String(t).toLowerCase()) ||
      String(t).toLowerCase().includes(industry)
  );

  const refs = [];
  let score = 0;

  if (positive.length) {
    const ratio = Math.min(1, positive.length / 4);
    score += Math.round(max * 0.7 * ratio);
    refs.push(...positive.slice(0, 3).map((s) => s.signal || s.detail));
  }
  if (industryHit || (prospect.discoveryReason && /matched/i.test(prospect.discoveryReason))) {
    score += Math.round(max * 0.3);
    refs.push(industryHit ? `industry:${prospect.industry}` : 'discoveryReason');
  } else if (prospect.confidence >= 0.75 && !positive.length) {
    // Discovery already gated on profile — soft credit from discovery confidence only
    score += Math.round(max * 0.35);
    refs.push('discovery_confidence');
  }

  score = Math.min(max, score);
  return buildFactorScore({
    factor: 'profile_match',
    label: FACTOR_LABELS.profile_match,
    score,
    max,
    matched: score > 0,
    detail:
      score > 0
        ? profile
          ? `Matched Discovery Profile signals under ${profile.name}`
          : 'Matched prior discovery ranking signals'
        : 'No profile-match evidence present',
    evidenceRefs: refs,
  });
}

function scoreBuyingSignals(prospect, knowledge) {
  const max = FACTOR_MAX.buying_signals;
  const refs = [];
  let score = 0;

  // SPEC-031 / ADR-018: prefer structured Active Business Signals when present
  const active = resolveActiveSignals(prospect, knowledge);
  const structuredBuying = buyingSignalsForRanking(active);
  if (structuredBuying.length) {
    let weighted = 0;
    for (const s of structuredBuying) {
      const w = Number(s.influenceWeight) || 0;
      weighted += w;
      refs.push(s.type || s.title || s.id);
      if (Array.isArray(s.evidenceRefs)) {
        refs.push(...s.evidenceRefs.slice(0, 2));
      }
    }
    // Up to max from influence-weighted Active buying/growth signals
    score = Math.min(max, Math.round(max * Math.min(1, weighted / 1.5)));
    score = Math.min(max, Math.max(score, Math.min(max, structuredBuying.length * 5)));
    return buildFactorScore({
      factor: 'buying_signals',
      label: FACTOR_LABELS.buying_signals,
      score,
      max,
      matched: score > 0,
      detail:
        score > 0
          ? `Active Business Signals: ${structuredBuying
              .slice(0, 3)
              .map((s) => s.title || s.type)
              .join(', ')}`
          : 'No buying-signal evidence — scored 0',
      evidenceRefs: [...new Set(refs)].slice(0, 8),
    });
  }

  for (const key of BUYING_SIGNAL_KEYS) {
    if (key === 'buyingSignals') continue; // handled as arrays below
    if (truthy(prospect[key]) || truthy(knowledge[key])) {
      score += 5;
      refs.push(key);
    }
  }

  const signals = Array.isArray(prospect.buyingSignals)
    ? prospect.buyingSignals
    : Array.isArray(knowledge.buyingSignals)
      ? knowledge.buyingSignals
      : [];
  // Legacy string/object list without lifecycle — only count items with evidence or string labels
  const legacyUsable = signals.filter((s) => {
    if (typeof s === 'string' && s.trim()) return true;
    if (s && typeof s === 'object') {
      if (Array.isArray(s.evidence) && s.evidence.length) return true;
      if (Array.isArray(s.evidenceRefs) && s.evidenceRefs.length) return true;
      if (s.type || s.title || s.summary) return true;
    }
    return false;
  });
  if (legacyUsable.length) {
    score += Math.min(10, legacyUsable.length * 4);
    refs.push(
      ...legacyUsable.slice(0, 3).map((s) =>
        typeof s === 'string' ? s : s.type || s.title || s.summary || String(s)
      )
    );
  }

  // Soft website presence is NOT a buying signal — only enrichment-flagged activity
  if (prospect.hiringActivity || knowledge.hiringActivity) {
    score = Math.max(score, 8);
    refs.push('hiring_activity');
  }

  score = Math.min(max, score);
  return buildFactorScore({
    factor: 'buying_signals',
    label: FACTOR_LABELS.buying_signals,
    score,
    max,
    matched: score > 0,
    detail:
      score > 0
        ? `Buying signals evidenced: ${refs.slice(0, 3).join(', ')}`
        : 'No buying-signal evidence — scored 0',
    evidenceRefs: refs,
  });
}

function scoreCompanySize(prospect, knowledge) {
  const max = FACTOR_MAX.company_size;
  const refs = [];
  let score = 0;

  const employees =
    Number(prospect.employeeCount ?? prospect.employees ?? knowledge.employeeCount) ||
    null;
  const sizeLabel = String(
    prospect.companySize || prospect.size || knowledge.companySize || ''
  ).toLowerCase();

  if (employees != null && employees > 0) {
    refs.push(`employees:${employees}`);
    // Prefer small/mid commercial offices for cleaning ICP
    if (employees >= 5 && employees <= 50) score = max;
    else if (employees < 5) score = Math.round(max * 0.6);
    else if (employees <= 150) score = Math.round(max * 0.5);
    else score = Math.round(max * 0.2);
  } else if (sizeLabel) {
    refs.push(`size:${sizeLabel}`);
    if (/small|smb|boutique|local/.test(sizeLabel)) score = Math.round(max * 0.8);
    else if (/mid|medium/.test(sizeLabel)) score = Math.round(max * 0.7);
    else if (/enterprise|national|large/.test(sizeLabel)) score = Math.round(max * 0.25);
    else score = Math.round(max * 0.4);
  } else if (
    (prospect.rankingSignals || []).some(
      (s) => s.signal === 'multi_location' && s.matched
    )
  ) {
    refs.push('multi_location');
    score = Math.round(max * 0.55);
  }

  return buildFactorScore({
    factor: 'company_size',
    label: FACTOR_LABELS.company_size,
    score: Math.min(max, score),
    max,
    matched: score > 0,
    detail:
      score > 0
        ? `Company size evidenced (${refs.join(', ')})`
        : 'No company-size evidence — scored 0',
    evidenceRefs: refs,
  });
}

function scoreDecisionMaker(prospect, knowledge) {
  const max = FACTOR_MAX.decision_maker_confidence;
  const refs = [];
  let score = 0;

  const contacts = collectContacts(prospect, knowledge);
  const title =
    prospect.jobTitle ||
    prospect.contactTitle ||
    prospect.title ||
    (contacts[0] && (contacts[0].title || contacts[0].jobTitle)) ||
    '';
  const email =
    prospect.email ||
    (contacts[0] && contacts[0].email) ||
    knowledge.email ||
    null;
  const phone =
    prospect.phone ||
    (contacts[0] && contacts[0].phone) ||
    knowledge.phone ||
    null;

  if (title) {
    refs.push(`title:${title}`);
    score += DECISION_MAKER_TITLES.test(title)
      ? Math.round(max * 0.45)
      : Math.round(max * 0.2);
  }
  if (email) {
    refs.push('email');
    score += Math.round(max * 0.3);
  }
  if (phone) {
    refs.push('phone');
    score += Math.round(max * 0.25);
  }

  const enrichConf = Number(
    prospect.enrichmentConfidence ?? knowledge.enrichmentConfidence
  );
  if (Number.isFinite(enrichConf) && enrichConf >= 0.7) {
    refs.push('enrichment_confidence');
    score += Math.round(max * 0.1);
  }

  score = Math.min(max, score);
  return buildFactorScore({
    factor: 'decision_maker_confidence',
    label: FACTOR_LABELS.decision_maker_confidence,
    score,
    max,
    matched: score > 0,
    detail:
      score > 0
        ? `Reachable decision-maker evidence: ${refs.join(', ')}`
        : 'No decision-maker contact evidence — scored 0',
    evidenceRefs: refs,
  });
}

function scorePersonalization(prospect, knowledge) {
  const max = FACTOR_MAX.personalization_opportunities;
  const refs = [];
  let score = 0;

  if (prospect.website || knowledge.website) {
    score += 3;
    refs.push('website');
  }
  if (prospect.industry || knowledge.industry) {
    score += 2;
    refs.push('industry');
  }
  if (prospect.address || knowledge.address) {
    score += 2;
    refs.push('address');
  }
  const tech = prospect.techStack || knowledge.techStack;
  if (Array.isArray(tech) && tech.length) {
    score += 2;
    refs.push('tech_stack');
  }
  if (prospect.snippet || prospect.reviewSnippet || knowledge.snippet) {
    score += 2;
    refs.push('snippet');
  }
  if (prospect.personalizationHooks || knowledge.personalizationHooks) {
    score += 3;
    refs.push('personalization_hooks');
  }

  score = Math.min(max, score);
  return buildFactorScore({
    factor: 'personalization_opportunities',
    label: FACTOR_LABELS.personalization_opportunities,
    score,
    max,
    matched: score > 0,
    detail:
      score > 0
        ? `Personalization hooks available: ${refs.join(', ')}`
        : 'No personalization evidence — scored 0',
    evidenceRefs: refs,
  });
}

function scoreGeographicFit(prospect, profile) {
  const max = FACTOR_MAX.geographic_fit;
  const refs = [];
  const address = String(prospect.address || '').toLowerCase();
  if (!address) {
    return buildFactorScore({
      factor: 'geographic_fit',
      label: FACTOR_LABELS.geographic_fit,
      score: 0,
      max,
      matched: false,
      detail: 'No address evidence — geographic fit scored 0',
      evidenceRefs: [],
    });
  }

  const cities = (profile && profile.geography && profile.geography.cities) || [];
  const state = (profile && profile.geography && profile.geography.state) || null;
  const label = (profile && profile.geography && profile.geography.label) || '';

  let score = 0;
  for (const city of cities) {
    if (address.includes(String(city).toLowerCase())) {
      score = max;
      refs.push(`city:${city}`);
      break;
    }
  }
  if (!score && state && address.includes(String(state).toLowerCase())) {
    score = Math.round(max * 0.6);
    refs.push(`state:${state}`);
  }
  if (!score && label) {
    const tokens = String(label)
      .toLowerCase()
      .split(/[^a-z0-9]+/)
      .filter((t) => t.length > 2);
    if (tokens.some((t) => address.includes(t))) {
      score = Math.round(max * 0.7);
      refs.push(`geo_label:${label}`);
    }
  }
  // Soft credit when discovery already accepted under a geo profile
  if (!score && profile && prospect.confidence >= 0.7) {
    score = Math.round(max * 0.35);
    refs.push('discovery_geo_gate');
  }

  return buildFactorScore({
    factor: 'geographic_fit',
    label: FACTOR_LABELS.geographic_fit,
    score: Math.min(max, score),
    max,
    matched: score > 0,
    detail:
      score > 0
        ? `Geographic fit evidenced (${refs.join(', ')})`
        : 'Address present but outside profile geography',
    evidenceRefs: refs,
  });
}

function scoreHistoricalSuccess(prospect, profile, outcomes, knowledge) {
  const max = FACTOR_MAX.historical_success;
  const industry = String(prospect.industry || '').toLowerCase();
  const vertical =
    String(prospect.vertical || knowledge.vertical || industry).toLowerCase();

  const relevant = outcomes.filter((o) => {
    if (!o || o.successful !== true && o.outcome !== 'successful') return false;
    const oVert = String(o.vertical || o.industry || '').toLowerCase();
    if (oVert && vertical && (oVert.includes(vertical) || vertical.includes(oVert))) {
      return true;
    }
    const oInd = String(o.industry || '').toLowerCase();
    return !!(oInd && industry && (oInd.includes(industry) || industry.includes(oInd)));
  });

  if (relevant.length === 0) {
    // Knowledge analogs if provided explicitly
    const analogs = Array.isArray(knowledge.successfulAnalogs)
      ? knowledge.successfulAnalogs
      : [];
    if (analogs.length) {
      const score = Math.min(max, 4 + analogs.length * 2);
      return buildFactorScore({
        factor: 'historical_success',
        label: FACTOR_LABELS.historical_success,
        score,
        max,
        matched: true,
        detail: `${analogs.length} successful analog(s) in knowledge`,
        evidenceRefs: analogs.slice(0, 3).map((a) => a.id || a.companyName || String(a)),
      });
    }
    return buildFactorScore({
      factor: 'historical_success',
      label: FACTOR_LABELS.historical_success,
      score: 0,
      max,
      matched: false,
      detail: 'No historical success evidence for this vertical/geo — scored 0',
      evidenceRefs: [],
    });
  }

  const score = Math.min(max, 5 + relevant.length * 2);
  return buildFactorScore({
    factor: 'historical_success',
    label: FACTOR_LABELS.historical_success,
    score,
    max,
    matched: true,
    detail: `${relevant.length} successful historical outcome(s) in similar vertical`,
    evidenceRefs: relevant.slice(0, 3).map((o) => o.id || o.prospectId || 'outcome'),
  });
}

function scoreEvidenceConfidence(prospect) {
  const max = FACTOR_MAX.evidence_confidence;
  const refs = [];
  let score = 0;

  const conf = Number(prospect.confidence);
  if (Number.isFinite(conf)) {
    score += Math.round(max * Math.max(0, Math.min(1, conf)));
    refs.push(`discovery_confidence:${conf}`);
  }
  if (Array.isArray(prospect.evidence) && prospect.evidence.length) {
    score = Math.min(max, score + 1);
    refs.push('evidence_items');
  }
  if (prospect.enriched === true) {
    score = Math.min(max, score + 1);
    refs.push('enriched');
  }
  if (prospect.website && (prospect.email || prospect.phone)) {
    score = Math.min(max, score + 1);
    refs.push('contact_complete');
  }

  return buildFactorScore({
    factor: 'evidence_confidence',
    label: FACTOR_LABELS.evidence_confidence,
    score: Math.min(max, score),
    max,
    matched: score > 0,
    detail:
      score > 0
        ? `Evidence confidence from: ${refs.join(', ')}`
        : 'No evidence-confidence fields — scored 0',
    evidenceRefs: refs,
  });
}

function collectRisks(prospect, factorScores, profile) {
  const risks = [];
  const byFactor = Object.fromEntries(factorScores.map((f) => [f.factor, f]));

  if (!byFactor.buying_signals?.score) {
    risks.push('No evidenced buying signals');
  }
  if (!byFactor.decision_maker_confidence?.score) {
    risks.push('No reachable decision-maker contact');
  }
  if (!prospect.website) {
    risks.push('Missing website');
  }
  if (!prospect.email && !prospect.phone) {
    risks.push('Missing email and phone');
  }
  if (byFactor.geographic_fit && byFactor.geographic_fit.score === 0 && prospect.address) {
    risks.push('Address may be outside target geography');
  }
  if (byFactor.historical_success && byFactor.historical_success.score === 0) {
    risks.push('No historical success analogs');
  }
  if (
    (prospect.rankingSignals || []).some(
      (s) => s.signal === 'multi_location' && s.matched
    )
  ) {
    risks.push('Multi-location indicators — may be harder beachhead');
  }
  if (profile && Number(prospect.confidence) < Number(profile.minimumConfidence || 0.75)) {
    risks.push('Discovery confidence below profile minimum');
  }
  return risks;
}

function computeRankingConfidence(prospect, factorScores) {
  const filled = factorScores.filter((f) => f.score > 0).length;
  const coverage = filled / factorScores.length;
  const disc = Number.isFinite(Number(prospect.confidence))
    ? Number(prospect.confidence)
    : 0.5;
  return Number((0.45 * disc + 0.55 * coverage).toFixed(4));
}

function resolveKnowledge(prospect, knowledgeRoot) {
  if (!knowledgeRoot || typeof knowledgeRoot !== 'object') return {};
  const id = prospect.id || prospect.companyId || prospect.companyName;
  if (knowledgeRoot.byProspectId && id && knowledgeRoot.byProspectId[id]) {
    return knowledgeRoot.byProspectId[id];
  }
  if (knowledgeRoot.byCompanyName && prospect.companyName) {
    const hit = knowledgeRoot.byCompanyName[prospect.companyName];
    if (hit) return hit;
  }
  // Flat knowledge bag is allowed when single-prospect context
  if (knowledgeRoot.employeeCount != null || knowledgeRoot.buyingSignals) {
    return knowledgeRoot;
  }
  return {};
}

function collectContacts(prospect, knowledge) {
  const list = [];
  if (Array.isArray(prospect.contacts)) list.push(...prospect.contacts);
  if (Array.isArray(knowledge.contacts)) list.push(...knowledge.contacts);
  if (prospect.contact && typeof prospect.contact === 'object') {
    list.push(prospect.contact);
  }
  return list;
}

function truthy(v) {
  return v === true || v === 1 || v === '1' || v === 'true';
}

module.exports = {
  scoreOpportunity,
  scoreProfileMatch,
  scoreBuyingSignals,
  scoreCompanySize,
  scoreDecisionMaker,
  scorePersonalization,
  scoreGeographicFit,
  scoreHistoricalSuccess,
  scoreEvidenceConfidence,
  collectRisks,
};
