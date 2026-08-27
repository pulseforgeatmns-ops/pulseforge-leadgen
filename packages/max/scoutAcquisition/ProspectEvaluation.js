'use strict';

/**
 * SPEC-194 — Multi-dimensional prospect qualification.
 * Qualification (ICP fit), buying readiness, and investigation needs are separate judgments.
 */

const {
  asText,
  READINESS_STATES,
  EVIDENCE_KINDS,
  REJECTION_REASONS,
  QUALIFICATION_STATUSES,
  PROSPECT_BUCKETS,
} = require('./Types');

const NEGATIVE_SEGMENT_PATTERNS = [
  /\bno vacation rental/i,
  /\bno short[- ]term rental/i,
  /\bno str\b/i,
  /\bresidential management only\b/i,
  /\bresidential only\b/i,
  /\bdoes not manage vacation/i,
  /\bnot (?:a |an )?(?:vacation|short[- ]term) rental/i,
];

const STR_INCOMPLETE_PATTERNS = [
  /\bproperty management\b/i,
  /\bportfolio\b/i,
  /\bmanaged (?:units|doors|properties)\b/i,
];

function clamp01(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 0;
  return Math.max(0, Math.min(1, n));
}

function haystack(candidate, classified) {
  const parts = [
    candidate && candidate.name,
    candidate && candidate.industry,
    candidate && candidate.vertical,
    candidate && candidate.segment,
    candidate && candidate.snippet,
    candidate && candidate.description,
    classified && classified.name,
  ];
  if (classified && Array.isArray(classified.observations)) {
    for (const row of classified.observations) {
      parts.push(typeof row === 'object' ? row.text : row);
    }
  }
  if (classified && Array.isArray(classified.signals)) {
    for (const row of classified.signals) {
      parts.push(row.label || row.text);
    }
  }
  return parts.filter(Boolean).join(' ');
}

function detectNegativeSegmentEvidence(text) {
  if (!text) return null;
  for (const pattern of NEGATIVE_SEGMENT_PATTERNS) {
    const match = text.match(pattern);
    if (match) return match[0];
  }
  return null;
}

function detectStrEvidenceGap(text, segments) {
  const segList = (segments || []).map((s) => String(s).toLowerCase());
  const strTarget = segList.some((s) => /str|short_term|vacation_rental/.test(s));
  if (!strTarget) return false;
  return !/\b(vacation rental|short[- ]term rental|str portfolio|airbnb|vrbo)\b/i.test(text);
}

function deriveQualificationStatus(fit, qualification, candidate, classified, searchDefinition) {
  const text = haystack(candidate, classified);
  const negativeHit = detectNegativeSegmentEvidence(text);
  if (negativeHit) {
    return {
      status: QUALIFICATION_STATUSES.NOT_QUALIFIED,
      confidence: 0.88,
      reasons: [`Negative segment evidence: "${negativeHit}".`],
      contradictoryEvidence: [],
      reasonCode: REJECTION_REASONS.EXCLUDED_SEGMENT,
    };
  }

  if (fit && fit.basicFit && qualification && qualification.qualified) {
    const strGap = detectStrEvidenceGap(text, searchDefinition && searchDefinition.segments);
    if (strGap) {
      return {
        status: QUALIFICATION_STATUSES.UNCERTAIN,
        confidence: clamp01(Number(fit.score || classified && classified.fit || 0.65)),
        reasons: [
          ...(fit.reasons || []).slice(0, 3),
          'STR-specific operating evidence is incomplete — fit is plausible but not confirmed.',
        ],
        contradictoryEvidence: [],
        reasonCode: null,
      };
    }
    return {
      status: QUALIFICATION_STATUSES.QUALIFIED,
      confidence: clamp01(Number(fit.score || classified && classified.fit || 0.7)),
      reasons: fit.reasons || [`${(candidate && candidate.name) || 'Prospect'} meets the ICP/business-fit contract.`],
      contradictoryEvidence: [],
      reasonCode: null,
    };
  }

  if (fit && fit.reasonCode === REJECTION_REASONS.EXCLUDED_SEGMENT) {
    return {
      status: QUALIFICATION_STATUSES.NOT_QUALIFIED,
      confidence: 0.9,
      reasons: fit.reasons || ['Excluded segment.'],
      contradictoryEvidence: [],
      reasonCode: fit.reasonCode,
    };
  }

  const fitScore = Number((fit && fit.score) || (classified && classified.fit) || 0);
  if (fitScore >= 0.35 && fitScore < 0.5) {
    return {
      status: QUALIFICATION_STATUSES.UNCERTAIN,
      confidence: clamp01(fitScore),
      reasons: fit && fit.reasons
        ? fit.reasons
        : [`${(candidate && candidate.name) || 'Prospect'} is near the fit threshold; more evidence needed.`],
      contradictoryEvidence: [],
      reasonCode: fit && fit.reasonCode,
    };
  }

  return {
    status: QUALIFICATION_STATUSES.NOT_QUALIFIED,
    confidence: clamp01(1 - fitScore),
    reasons:
      (fit && fit.reasons) ||
      (qualification && qualification.rejectedBecause
        ? [qualification.rejectedBecause]
        : [`${(candidate && candidate.name) || 'Prospect'} did not meet the business-fit contract.`]),
    contradictoryEvidence: [],
    reasonCode: (fit && fit.reasonCode) || (qualification && qualification.reason) || REJECTION_REASONS.INSUFFICIENT_BUSINESS_FIT,
  };
}

function deriveReadinessAssessment(qualification, classified) {
  const readinessState =
    (qualification && qualification.readinessState) || READINESS_STATES.UNKNOWN;
  const signals = Array.isArray(classified && classified.signals) ? classified.signals : [];
  let confidence = 0.25;
  if (readinessState === READINESS_STATES.READY) confidence = 0.82;
  else if (readinessState === READINESS_STATES.NOT_READY) confidence = 0.72;
  else if (signals.length) confidence = 0.4;

  if (qualification && qualification.evidenceKind === EVIDENCE_KINDS.INSUFFICIENT_EVIDENCE) {
    confidence = Math.min(confidence, 0.35);
  }

  return {
    status: readinessState,
    confidence: Number(confidence.toFixed(2)),
    signals: signals.map((s) => ({
      type: s.type || null,
      label: asText(s.label || s.text) || null,
      observedAt: s.observedAt || null,
    })),
  };
}

function deriveInvestigationNeeds(qualificationStatus, readiness, fit, classified) {
  const missingEvidence = [];
  const unresolvedHypotheses = [];
  const unknowns = (classified && classified.unknowns) || [];

  for (const row of unknowns) {
    const text = typeof row === 'object' ? row.text : String(row || '');
    if (text) unresolvedHypotheses.push(text);
  }

  if (qualificationStatus.status === QUALIFICATION_STATUSES.UNCERTAIN) {
    missingEvidence.push('Segment / operating-model confirmation');
    unresolvedHypotheses.push('Does this business match the target segment and service need?');
  }

  if (
    qualificationStatus.status === QUALIFICATION_STATUSES.QUALIFIED &&
    readiness.status === READINESS_STATES.UNKNOWN
  ) {
    missingEvidence.push('Buying-readiness timing signals');
    unresolvedHypotheses.push(
      'What evidence would most reduce uncertainty about whether this business is worth contacting now?'
    );
  }

  if (!(classified && classified.evidenceRefs && classified.evidenceRefs.length)) {
    missingEvidence.push('Source-backed company evidence');
  }

  if (readiness.status === READINESS_STATES.UNKNOWN && !(classified && classified.signals && classified.signals.length)) {
    missingEvidence.push('Portfolio / growth / vendor-change investigation');
  }

  if (fit && fit.basicFit && readiness.status === READINESS_STATES.UNKNOWN) {
    missingEvidence.push('Website / portfolio / review / decision-maker enrichment');
  }

  return {
    missingEvidence: [...new Set(missingEvidence)],
    unresolvedHypotheses: [...new Set(unresolvedHypotheses)].slice(0, 6),
  };
}

function assignProspectBucket(qualificationStatus, readiness) {
  if (qualificationStatus.status === QUALIFICATION_STATUSES.NOT_QUALIFIED) {
    return PROSPECT_BUCKETS.EXCLUDED;
  }
  if (qualificationStatus.status === QUALIFICATION_STATUSES.UNCERTAIN) {
    return PROSPECT_BUCKETS.FIT_INVESTIGATION;
  }
  if (readiness.status === READINESS_STATES.READY) {
    return PROSPECT_BUCKETS.HIGH_PRIORITY;
  }
  if (readiness.status === READINESS_STATES.UNKNOWN) {
    return PROSPECT_BUCKETS.INVESTIGATION_REQUIRED;
  }
  if (readiness.status === READINESS_STATES.NOT_READY) {
    return PROSPECT_BUCKETS.NURTURE;
  }
  return PROSPECT_BUCKETS.INVESTIGATION_REQUIRED;
}

/**
 * Build canonical multi-dimensional prospect evaluation (SPEC-194).
 *
 * @param {object} input
 * @returns {object}
 */
function buildProspectEvaluation(input = {}) {
  const candidate = input.candidate || {};
  const classified = input.classified || {};
  const fit = input.fit || {};
  const qualification = input.qualification || {};
  const searchDefinition = input.searchDefinition || {};

  const identity = {
    companyId: asText(classified.companyId || candidate.id) || null,
    name: asText(classified.name || candidate.name) || null,
    industry: asText(candidate.industry || candidate.vertical || candidate.segment) || null,
    location: asText(candidate.location || candidate.address) || null,
  };

  const qualificationBlock = deriveQualificationStatus(
    fit,
    qualification,
    candidate,
    classified,
    searchDefinition
  );
  const readiness = deriveReadinessAssessment(qualification, classified);
  const investigation = deriveInvestigationNeeds(qualificationBlock, readiness, fit, classified);
  const bucket = assignProspectBucket(qualificationBlock, readiness);

  return {
    identity,
    qualification: qualificationBlock,
    readiness,
    investigation,
    bucket,
    // Legacy flat fields preserved for downstream compatibility (ADR-101).
    qualified: qualificationBlock.status === QUALIFICATION_STATUSES.QUALIFIED,
    supported: bucket === PROSPECT_BUCKETS.HIGH_PRIORITY,
    readinessState: readiness.status,
    evidenceKind: qualification.evidenceKind || null,
  };
}

function countByBucket(evaluations) {
  const counts = {
    highPriority: 0,
    investigationRequired: 0,
    nurture: 0,
    fitInvestigation: 0,
    excluded: 0,
  };
  for (const row of evaluations || []) {
    if (row.bucket === PROSPECT_BUCKETS.HIGH_PRIORITY) counts.highPriority += 1;
    else if (row.bucket === PROSPECT_BUCKETS.INVESTIGATION_REQUIRED) counts.investigationRequired += 1;
    else if (row.bucket === PROSPECT_BUCKETS.NURTURE) counts.nurture += 1;
    else if (row.bucket === PROSPECT_BUCKETS.FIT_INVESTIGATION) counts.fitInvestigation += 1;
    else counts.excluded += 1;
  }
  return counts;
}

function qualifiedEvaluationCount(evaluations) {
  return (evaluations || []).filter(
    (row) =>
      row.qualification &&
      (row.qualification.status === QUALIFICATION_STATUSES.QUALIFIED ||
        row.qualification.status === QUALIFICATION_STATUSES.UNCERTAIN)
  ).length;
}

function businessFitQualifiedCount(evaluations) {
  return (evaluations || []).filter(
    (row) => row.qualification && row.qualification.status === QUALIFICATION_STATUSES.QUALIFIED
  ).length;
}

module.exports = {
  NEGATIVE_SEGMENT_PATTERNS,
  buildProspectEvaluation,
  assignProspectBucket,
  deriveQualificationStatus,
  deriveReadinessAssessment,
  deriveInvestigationNeeds,
  detectNegativeSegmentEvidence,
  countByBucket,
  qualifiedEvaluationCount,
  businessFitQualifiedCount,
};
