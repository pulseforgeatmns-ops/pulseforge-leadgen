'use strict';

/**
 * SPEC-100A — basic fit is not intent.
 * FIT: this resembles the kind of organization the tenant should serve.
 * TIMING / INTENT: something observable suggests a useful time to approach.
 * UNKNOWN: no evidence about vendor timing.
 */

const {
  asText,
  nowIso,
  isTimely,
  normalizeSignal,
  normalizeClaim,
  OPPORTUNITY_CLASSES,
  FIT_LEVELS,
  INTENT_STATES,
  REJECTION_REASONS,
} = require('./Types');
const { matchesGeography, matchesSegment } = require('./ExistingIntelligence');
const { qualifyCandidate } = require('./InvestigationProvenance');
const { evaluateIcpFit, qualifyProspect } = require('../../aim');

const FACILITY_PATTERNS = [
  /\b(propert(?:y|ies)|portfolio|multifamily|managed (?:units|doors|buildings)|facilities|office park|campus)\b/i,
  /\b(\d+)\s+(managed )?(propert(?:y|ies)|buildings?|doors|units|locations?)\b/i,
];

const RESIDENTIAL_ONLY_PATTERNS = [
  /\bresidential realtor\b/i,
  /\bhouse cleaning\b/i,
  /\bmaid service\b/i,
  /\bhome cleaning\b/i,
];

function haystack(candidate) {
  return [
    candidate.name,
    candidate.industry,
    candidate.vertical,
    candidate.segment,
    candidate.snippet,
    candidate.description,
    candidate.website,
    ...(Array.isArray(candidate.signals) ? candidate.signals.map((s) => s.label || s.text) : []),
  ]
    .filter(Boolean)
    .join(' ');
}

function facilityEvidence(candidate) {
  const text = haystack(candidate);
  for (const pattern of FACILITY_PATTERNS) {
    const match = text.match(pattern);
    if (match) return match[0];
  }
  if (/property/.test(String(candidate.industry || '').toLowerCase())) {
    return 'property management operator';
  }
  return null;
}

/**
 * Explainable basic-fit decision. Never writes buying intent from ICP/fit.
 * SPEC-112: when an AIM is present, Scout reasons over that market model
 * instead of the commercial-cleaning facility heuristic.
 */
function evaluateAimBasicFit(candidate, aim) {
  const icp = evaluateIcpFit(aim.icp, candidate);
  if (icp.excluded) {
    return {
      level: FIT_LEVELS.REJECTED,
      score: Number(icp.score.toFixed(2)),
      basicFit: false,
      reasons: icp.reasons,
      intent: INTENT_STATES.UNKNOWN,
      reasonCode: REJECTION_REASONS.EXCLUDED_SEGMENT,
    };
  }
  const score = icp.score;
  const level =
    score >= 0.72 ? FIT_LEVELS.STRONG : score >= 0.5 ? FIT_LEVELS.MODERATE : FIT_LEVELS.WEAK;
  if (!level || level === FIT_LEVELS.WEAK) {
    return {
      level: score < 0.35 ? FIT_LEVELS.REJECTED : FIT_LEVELS.WEAK,
      score: Number(score.toFixed(2)),
      basicFit: false,
      reasons: icp.reasons.length
        ? icp.reasons
        : [`${candidate.name} does not yet resemble the AIM ICP.`],
      intent: INTENT_STATES.UNKNOWN,
      reasonCode: REJECTION_REASONS.INSUFFICIENT_BUSINESS_FIT,
    };
  }
  return {
    level,
    score: Number(score.toFixed(2)),
    basicFit: true,
    reasons: icp.reasons,
    intent: INTENT_STATES.UNKNOWN,
    reasonCode: null,
  };
}

function evaluateBasicFit(candidate, searchDefinition = {}) {
  if (searchDefinition && searchDefinition.aim) {
    return evaluateAimBasicFit(candidate, searchDefinition.aim);
  }
  const reasons = [];
  const geography = searchDefinition.geography && searchDefinition.geography.label;
  const segments = searchDefinition.segments || [];
  const exclusions = searchDefinition.exclusions || [];
  const text = haystack(candidate);

  if (exclusions.length && matchesSegment(candidate, exclusions)) {
    return {
      level: FIT_LEVELS.REJECTED,
      score: 0.15,
      basicFit: false,
      reasons: [
        `Rejected: ${candidate.name} matches an excluded segment (${exclusions.join(', ')}).`,
      ],
      intent: INTENT_STATES.UNKNOWN,
      reasonCode: REJECTION_REASONS.EXCLUDED_SEGMENT,
    };
  }

  if (geography && !matchesGeography(candidate.location || candidate.address, geography)) {
    return {
      level: FIT_LEVELS.REJECTED,
      score: 0.2,
      basicFit: false,
      reasons: [
        `Rejected: ${candidate.name} is outside the approved service geography (${geography}).`,
      ],
      intent: INTENT_STATES.UNKNOWN,
      reasonCode: REJECTION_REASONS.OUTSIDE_GEOGRAPHY,
    };
  }

  if (RESIDENTIAL_ONLY_PATTERNS.some((p) => p.test(text))) {
    return {
      level: FIT_LEVELS.REJECTED,
      score: 0.18,
      basicFit: false,
      reasons: [
        `Rejected: residential operator with no evidence of managed commercial facilities.`,
      ],
      intent: INTENT_STATES.UNKNOWN,
      reasonCode: REJECTION_REASONS.INSUFFICIENT_BUSINESS_FIT,
    };
  }

  const segmentHit = !segments.length || matchesSegment(candidate, segments);
  if (!segmentHit) {
    return {
      level: FIT_LEVELS.REJECTED,
      score: 0.28,
      basicFit: false,
      reasons: [
        `Rejected: ${candidate.name} does not match the current acquisition segments (${segments.join(', ')}).`,
      ],
      intent: INTENT_STATES.UNKNOWN,
      reasonCode: REJECTION_REASONS.INSUFFICIENT_BUSINESS_FIT,
    };
  }

  const facility = facilityEvidence(candidate);
  const icp = candidate.icpScore != null ? Number(candidate.icpScore) : null;
  let score = 0.55;
  if (segmentHit) {
    reasons.push(
      `Industry/type matches the current target (${candidate.industry || segments.join(', ') || 'delegated segment'}).`
    );
    score += 0.12;
  }
  if (geography) {
    reasons.push(`Located inside ${geography}.`);
    score += 0.08;
  }
  if (facility) {
    reasons.push(`Facility/operating signal: ${facility}.`);
    score += 0.12;
  }
  if (icp != null && icp >= 70) {
    reasons.push(`Existing company score ${icp} supports commercial fit — not buying intent.`);
    score += 0.08;
  } else if (icp != null && icp >= 55) {
    reasons.push(`Existing company score ${icp} is moderate commercial fit.`);
    score += 0.03;
  }
  if (candidate.website) {
    reasons.push(`Public company website available (${candidate.website}).`);
    score += 0.04;
  }

  score = Math.max(0, Math.min(0.95, score));
  const level =
    score >= 0.72 ? FIT_LEVELS.STRONG : score >= 0.5 ? FIT_LEVELS.MODERATE : FIT_LEVELS.WEAK;

  if (level === FIT_LEVELS.WEAK) {
    return {
      level,
      score: Number(score.toFixed(2)),
      basicFit: false,
      reasons: [
        ...reasons,
        `${candidate.name} does not yet have enough facility/operating evidence to treat as a basic-fit buyer.`,
      ],
      intent: INTENT_STATES.UNKNOWN,
      reasonCode: REJECTION_REASONS.INSUFFICIENT_BUSINESS_FIT,
    };
  }

  return {
    level,
    score: Number(score.toFixed(2)),
    basicFit: true,
    reasons,
    intent: INTENT_STATES.UNKNOWN,
    reasonCode: null,
  };
}

function collectBasicEvidence(candidate, nowIsoValue = nowIso()) {
  const evidence = [];
  if (candidate.website) {
    evidence.push({
      id: `ev-${candidate.id}-website`,
      kind: 'company',
      sourceKind: 'observed_fact',
      label: `${candidate.name} website ${candidate.website}`,
      snapshot: {
        companyId: candidate.id,
        companyName: candidate.name,
        observedAt: candidate.updatedAt || candidate.evidenceObservedAt || nowIsoValue,
        source: 'company_website',
        evidenceType: 'company_record',
      },
    });
  }
  if (candidate.address || candidate.location) {
    evidence.push({
      id: `ev-${candidate.id}-location`,
      kind: 'company',
      sourceKind: 'observed_fact',
      label: `${candidate.name} location ${candidate.address || candidate.location}`,
      snapshot: {
        companyId: candidate.id,
        companyName: candidate.name,
        observedAt: candidate.updatedAt || nowIsoValue,
        source: candidate.source || 'existing_repository',
        evidenceType: 'location',
      },
    });
  }
  if (candidate.snippet || candidate.description) {
    evidence.push({
      id: `ev-${candidate.id}-description`,
      kind: 'company',
      sourceKind: 'observed_fact',
      label: candidate.snippet || candidate.description,
      snapshot: {
        companyId: candidate.id,
        companyName: candidate.name,
        observedAt: candidate.updatedAt || nowIsoValue,
        source: candidate.source || 'public_business_data',
        evidenceType: 'business_description',
      },
    });
  }
  for (const row of candidate.evidence || []) {
    if (row && row.id) evidence.push(row);
  }
  return evidence;
}

function classifyOpportunity({ fit, classified, qualification }) {
  if (!fit.basicFit) {
    return {
      classification: OPPORTUNITY_CLASSES.REJECTED,
      intent: INTENT_STATES.UNKNOWN,
    };
  }
  if (qualification && qualification.supported) {
    return {
      classification: OPPORTUNITY_CLASSES.SUPPORTED,
      intent: INTENT_STATES.TIMED,
    };
  }
  if (fit.level === FIT_LEVELS.STRONG && (!qualification || !qualification.signalBearing)) {
    return {
      classification: OPPORTUNITY_CLASSES.FIT,
      intent: INTENT_STATES.UNKNOWN,
    };
  }
  if (qualification && qualification.reason === REJECTION_REASONS.STALE_EVIDENCE) {
    return {
      classification: OPPORTUNITY_CLASSES.WATCH,
      intent: INTENT_STATES.UNKNOWN,
    };
  }
  if (fit.level === FIT_LEVELS.MODERATE) {
    return {
      classification: OPPORTUNITY_CLASSES.WATCH,
      intent: INTENT_STATES.UNKNOWN,
    };
  }
  if (classified && Number(classified.fit || 0) >= 0.7) {
    return {
      classification: OPPORTUNITY_CLASSES.FIT,
      intent: INTENT_STATES.UNKNOWN,
    };
  }
  return {
    classification: OPPORTUNITY_CLASSES.FIT,
    intent: INTENT_STATES.UNKNOWN,
  };
}

function attachFitToClassified(classified, candidate, searchDefinition, now = Date.now()) {
  const fit = evaluateBasicFit(candidate, searchDefinition);
  const evidence = collectBasicEvidence(candidate);
  const next = {
    ...classified,
    observations: Array.isArray(classified.observations) ? classified.observations : [],
    unknowns: Array.isArray(classified.unknowns) ? classified.unknowns : [],
    signals: Array.isArray(classified.signals) ? classified.signals : [],
    fit: fit.score,
    fitLevel: fit.level,
    fitReasons: fit.reasons,
    intent: fit.intent,
    evidenceRefs: [...(classified.evidenceRefs || []), ...evidence],
  };
  if (searchDefinition && searchDefinition.aim) {
    next.aimQualification = qualifyProspect(searchDefinition.aim, {
      ...candidate,
      signals: next.signals || candidate.signals,
      observations: next.observations || candidate.observations,
    });
  }
  if (fit.basicFit && !next.observations.some((o) => /basic fit|target profile/i.test(o.text || ''))) {
    next.observations = [
      ...next.observations,
      normalizeClaim(
        {
          kind: 'observation',
          text: `${candidate.name} meets the current acquisition profile. ${fit.reasons[0] || ''}`.trim(),
          entityId: candidate.id,
          observedAt: candidate.updatedAt || null,
        },
        'observation'
      ),
    ];
  }
  if (fit.intent === INTENT_STATES.UNKNOWN) {
    const hasIntentUnknown = (next.unknowns || []).some((u) =>
      /vendor timing|contract timing|dissatisfaction/i.test(u.text || '')
    );
    if (!hasIntentUnknown) {
      next.unknowns = [
        ...(next.unknowns || []),
        normalizeClaim(
          {
            kind: 'unknown',
            text: `Strong fit. Timing unknown for ${candidate.name}.`,
            entityId: candidate.id,
          },
          'unknown'
        ),
      ];
    }
  }
  const qualification = qualifyCandidate(next, { ...candidate, icpScore: candidate.icpScore }, now);
  const classifiedOpp = classifyOpportunity({ fit, classified: next, qualification });
  next.classification = classifiedOpp.classification;
  next.intent = classifiedOpp.intent;
  if (classifiedOpp.intent !== INTENT_STATES.TIMED) {
    next.timing = Math.min(Number(next.timing || 0), 0.35);
  }
  const timely = (next.signals || []).some((s) => {
    const type = normalizeSignal(s.type);
    return type && type !== 'decision_maker' && isTimely(s.observedAt, now);
  });
  if (!timely && fit.basicFit) {
    next.inferences = (next.inferences || []).filter(
      (inf) => !/currently (wants|replacing|buying)|ready to buy/i.test(inf.text || '')
    );
  }
  return {
    classified: next,
    fit,
    qualification,
    evidence,
    lastEvaluatedAt: nowIso(),
    evidenceObservedAt:
      (next.signals[0] && next.signals[0].observedAt) ||
      candidate.updatedAt ||
      nowIso(),
  };
}

function relevantPeopleRoles() {
  return [
    'operations manager',
    'facilities manager',
    'property manager',
    'office manager',
    'owner',
    'executive',
    'director of operations',
  ];
}

async function enrichPeopleSafe(candidate, enrichPeople) {
  if (typeof enrichPeople !== 'function') {
    return {
      people: candidate.people || [],
      failed: false,
      unknown: !(candidate.people && candidate.people.length),
    };
  }
  try {
    const people = await enrichPeople(candidate);
    return {
      people: Array.isArray(people) ? people : candidate.people || [],
      failed: false,
      unknown: !(people && people.length),
    };
  } catch {
    return {
      people: candidate.people || [],
      failed: true,
      unknown: true,
    };
  }
}

module.exports = {
  evaluateBasicFit,
  evaluateAimBasicFit,
  collectBasicEvidence,
  classifyOpportunity,
  attachFitToClassified,
  enrichPeopleSafe,
  relevantPeopleRoles,
  facilityEvidence,
};
