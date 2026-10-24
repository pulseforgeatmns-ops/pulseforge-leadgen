'use strict';

/**
 * SPEC-146 — Unified conflict detection.
 * Merges SPEC-142 rule conflicts and SPEC-144 numeric contradictions
 * into first-class EvidenceConflict objects.
 */

const { detectContradictions, extractFactSnapshot } = require('../investigation/ContradictionDetection');
const { evidenceSourceLabel } = require('../credibility/EvidenceWeights');
const {
  CONFLICT_CATEGORIES,
  CONFLICT_SEVERITY,
  CONFLICT_SUBJECTS,
  buildEvidenceConflict,
} = require('./types');

function asText(value) {
  if (value == null) return '';
  return String(value).trim();
}

function extractNumericClaims(label) {
  const text = asText(label);
  const matches = [];
  const propertyMatch = text.match(/(\d+)\s*(?:managed\s*)?(?:propert(?:y|ies)|listings?)/i);
  if (propertyMatch) {
    matches.push({ field: 'property_count', value: Number(propertyMatch[1]), raw: text });
  }
  const listingMatch = text.match(/(\d+)\s*listings?/i);
  if (listingMatch && !propertyMatch) {
    matches.push({ field: 'listing_count', value: Number(listingMatch[1]), raw: text });
  }
  const employeeMatch = text.match(/(\d+)\s*employees?/i);
  if (employeeMatch) {
    matches.push({ field: 'employee_count', value: Number(employeeMatch[1]), raw: text });
  }
  return matches;
}

function normalizeSource(source) {
  return String(source || 'unknown')
    .trim()
    .toLowerCase()
    .replace(/[\s-]+/g, '_');
}

const SUBJECT_LABELS = Object.freeze({
  employee_count: 'Employee Count',
  property_count: 'Property Count',
  listing_count: 'Listing Count',
  ownership: 'Ownership',
  address: 'Address',
  phone: 'Phone',
  revenue_estimate: 'Revenue Estimate',
  service_area: 'Service Area',
  decision_maker: 'Decision Maker',
  operating_status: 'Operating Status',
  company_size: 'Company Size',
});

function subjectLabel(subject) {
  return SUBJECT_LABELS[subject] || subject.replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase());
}

function inferSeverity(subject, claims = []) {
  const criticalSubjects = new Set([
    CONFLICT_SUBJECTS.OWNERSHIP,
    CONFLICT_SUBJECTS.DECISION_MAKER,
    CONFLICT_SUBJECTS.OPERATING_STATUS,
    CONFLICT_SUBJECTS.PROPERTY_COUNT,
    CONFLICT_SUBJECTS.LISTING_COUNT,
  ]);
  const highSubjects = new Set([
    CONFLICT_SUBJECTS.EMPLOYEE_COUNT,
    CONFLICT_SUBJECTS.COMPANY_SIZE,
    CONFLICT_SUBJECTS.REVENUE,
    CONFLICT_SUBJECTS.SERVICE_AREA,
  ]);
  const lowSubjects = new Set([CONFLICT_SUBJECTS.ADDRESS, CONFLICT_SUBJECTS.PHONE]);

  if (criticalSubjects.has(subject)) return CONFLICT_SEVERITY.CRITICAL;
  if (highSubjects.has(subject)) return CONFLICT_SEVERITY.HIGH;
  if (lowSubjects.has(subject)) return CONFLICT_SEVERITY.LOW;

  if (claims.length >= 3) {
    const values = claims.map((c) => c.value).filter((v) => v != null);
    if (values.length >= 2) {
      const nums = values.filter((v) => typeof v === 'number');
      if (nums.length >= 2) {
        const ratio = Math.max(...nums) / Math.min(...nums);
        if (ratio >= 3) return CONFLICT_SEVERITY.HIGH;
      }
    }
  }

  return CONFLICT_SEVERITY.MEDIUM;
}

function extractClaimsFromEvidence(evidence = []) {
  const claims = [];

  for (const item of evidence) {
    const label = asText(item.label || item.text || '');
    const source = normalizeSource(item.source || item.kind || 'unknown');
    const observedAt = item.observedAt || item.observed_at || null;

    for (const numeric of extractNumericClaims(label)) {
      claims.push({
        subject: numeric.field,
        source,
        sourceLabel: evidenceSourceLabel(source),
        value: numeric.value,
        label,
        observedAt,
        raw: numeric.raw,
      });
    }

    const lower = label.toLowerCase();
    if (lower.includes('family') && lower.includes('own')) {
      claims.push({
        subject: CONFLICT_SUBJECTS.OWNERSHIP,
        source,
        sourceLabel: evidenceSourceLabel(source),
        value: label,
        label,
        observedAt,
      });
    }
    if (lower.includes('hiring') || lower.includes('job opening')) {
      claims.push({
        subject: CONFLICT_SUBJECTS.OPERATING_STATUS,
        source,
        sourceLabel: evidenceSourceLabel(source),
        value: label,
        label,
        observedAt,
      });
    }
    if (lower.includes('owner changed') || lower.includes('ownership transfer')) {
      claims.push({
        subject: CONFLICT_SUBJECTS.OWNERSHIP,
        source,
        sourceLabel: evidenceSourceLabel(source),
        value: label,
        label,
        observedAt,
      });
    }
  }

  return claims;
}

function claimsConflict(claims = []) {
  if (claims.length < 2) return false;
  const numericValues = claims.map((c) => c.value).filter((v) => typeof v === 'number');
  if (numericValues.length >= 2) {
    const unique = [...new Set(numericValues)];
    return unique.length > 1;
  }
  const textValues = claims.map((c) => asText(c.value).toLowerCase()).filter(Boolean);
  const uniqueText = [...new Set(textValues)];
  return uniqueText.length > 1;
}

function ruleConflictToEvidenceConflict(ruleConflict, candidate) {
  const subject =
    ruleConflict.fieldA === ruleConflict.fieldB
      ? ruleConflict.fieldA
      : `${ruleConflict.fieldA}_vs_${ruleConflict.fieldB}`;

  return buildEvidenceConflict({
    id: ruleConflict.id,
    subject,
    entityId: ruleConflict.entityId || candidate?.id,
    conflictingClaims: [
      {
        source: 'rule_a',
        sourceLabel: subjectLabel(ruleConflict.fieldA),
        value: ruleConflict.valueA,
        label: asText(ruleConflict.valueA),
      },
      {
        source: 'rule_b',
        sourceLabel: subjectLabel(ruleConflict.fieldB),
        value: ruleConflict.valueB,
        label: asText(ruleConflict.valueB),
      },
    ],
    providers: ['rule_detection'],
    category: CONFLICT_CATEGORIES.OBSERVATION,
    severity: inferSeverity(subject),
    confidence: 0,
    resolution: { resolved: ruleConflict.resolved === true },
    confidencePenalty: ruleConflict.confidencePenalty || 0.15,
    description: ruleConflict.description,
  });
}

function numericClaimsToConflict(subject, claims, entityId) {
  const providers = [...new Set(claims.map((c) => c.source))];
  const description = claims
    .map((c) => `${c.sourceLabel} reports ${c.value}.`)
    .join(' ');

  return buildEvidenceConflict({
    id: `conflict:${entityId}:${subject}:${providers.sort().join(':')}`,
    subject,
    entityId,
    conflictingClaims: claims.map((c) => ({
      source: c.source,
      sourceLabel: c.sourceLabel,
      value: c.value,
      label: c.label,
      observedAt: c.observedAt,
    })),
    providers,
    category: CONFLICT_CATEGORIES.SOURCE_AUTHORITY,
    severity: inferSeverity(subject, claims),
    confidence: 0,
    resolution: { resolved: false },
    confidencePenalty: 0.12,
    description,
  });
}

/**
 * Detect all evidence conflicts for a candidate.
 * @param {object} candidate
 * @param {object[]} evidence
 * @returns {object[]}
 */
function detectEvidenceConflicts(candidate, evidence = []) {
  const entityId = candidate?.id || 'unknown';
  const conflicts = [];
  const seen = new Set();

  const ruleConflicts = detectContradictions(candidate, evidence);
  for (const rc of ruleConflicts) {
    const ec = ruleConflictToEvidenceConflict(rc, candidate);
    seen.add(ec.id);
    conflicts.push(ec);
  }

  const claims = extractClaimsFromEvidence(evidence);
  const bySubject = {};
  for (const claim of claims) {
    if (!bySubject[claim.subject]) bySubject[claim.subject] = [];
    bySubject[claim.subject].push(claim);
  }

  for (const [subject, subjectClaims] of Object.entries(bySubject)) {
    if (!claimsConflict(subjectClaims)) continue;
    const ec = numericClaimsToConflict(subject, subjectClaims, entityId);
    if (seen.has(ec.id)) continue;
    seen.add(ec.id);
    conflicts.push(ec);
  }

  const facts = extractFactSnapshot(candidate, evidence);
  if (facts.ownership && facts.company_size) {
    const ownershipClaims = claims.filter((c) => c.subject === CONFLICT_SUBJECTS.OWNERSHIP);
    const sizeClaims = claims.filter((c) =>
      [CONFLICT_SUBJECTS.EMPLOYEE_COUNT, CONFLICT_SUBJECTS.COMPANY_SIZE].includes(c.subject)
    );
    if (
      ownershipClaims.length &&
      sizeClaims.length &&
      !conflicts.some((c) => c.subject.includes('ownership'))
    ) {
      const ec = buildEvidenceConflict({
        id: `conflict:${entityId}:ownership_size`,
        subject: 'ownership_vs_size',
        entityId,
        conflictingClaims: [
          ...ownershipClaims.slice(0, 1),
          ...sizeClaims.slice(0, 1),
        ].map((c) => ({
          source: c.source,
          sourceLabel: c.sourceLabel,
          value: c.value,
          label: c.label,
          observedAt: c.observedAt,
        })),
        providers: [...new Set([...ownershipClaims, ...sizeClaims].map((c) => c.source))],
        category: CONFLICT_CATEGORIES.OBSERVATION,
        severity: CONFLICT_SEVERITY.HIGH,
        resolution: { resolved: false },
        confidencePenalty: 0.15,
        description: 'Ownership claim may conflict with company size signals.',
      });
      if (!seen.has(ec.id)) {
        seen.add(ec.id);
        conflicts.push(ec);
      }
    }
  }

  return conflicts;
}

/**
 * Detect conflicts across multiple candidates.
 * @param {object[]} candidates
 * @returns {object[]}
 */
function detectAllEvidenceConflicts(candidates = []) {
  const all = [];
  for (const candidate of candidates) {
    const evidence = [
      ...(candidate.evidence || []),
      ...(candidate.signals || []).map((s) => ({
        ...s,
        source: s.source || 'signal',
        label: s.label || s.text,
      })),
    ];
    all.push(...detectEvidenceConflicts(candidate, evidence));
  }
  return all;
}

module.exports = {
  extractClaimsFromEvidence,
  extractNumericClaims,
  detectEvidenceConflicts,
  detectAllEvidenceConflicts,
  subjectLabel,
  inferSeverity,
};
