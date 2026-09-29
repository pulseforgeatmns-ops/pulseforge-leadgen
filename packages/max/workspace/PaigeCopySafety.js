'use strict';

/**
 * Customer-facing copy safety for Paige mission outbound variants.
 * Internal mission/scoring/operator rationale must never render verbatim in
 * subject, body, or CTA fields bound for prospect delivery.
 */

const BLOCKER = 'internal_reasoning_leakage';

const INTERNAL_COPY_PATTERNS = Object.freeze([
  { id: 'mission_focus_label', re: /\bmission focus\s*:/i },
  { id: 'why_now_fit_label', re: /\bwhy now\s*:\s*fit\b/i },
  { id: 'recurring_clients_syntax', re: /\brecurring_clients\b/i },
  { id: 'timing_score_tuple', re: /\btiming\s+0\.\d/i },
  { id: 'fit_score_tuple', re: /\bfit\s+0\.\d/i },
  { id: 'unknown_count', re: /\b\d+\s+unknowns?\b/i },
  { id: 'score_tuple_separator', re: /\bfit\s+0\.\d+\s*[·•|]\s*timing\s+0\.\d+/i },
  { id: 'achieve_metric_objective', re: /\bachieve\s+\d+\s+(?:recurring_clients|customers)\b/i },
  { id: 'priority_ranking_label', re: /\bpriority\s+\d+\b/i },
  { id: 'prioritize_first_wave', re: /\bprioritize\s+.+\s+(?:in the first|as the first)\b/i },
  { id: 'act_on_buying_signal', re: /\bact on buying signal\s*:/i },
  { id: 'max_recommendation_label', re: /\bmax recommendation\s*:/i },
  { id: 'objective_reason_label', re: /\bobjective reason\s*:/i },
  { id: 'internal_rationale_label', re: /\b(?:operator|internal)\s+rationale\s*:/i },
  { id: 'confidence_score', re: /\bconfidence\s*[:=]\s*0\.\d+/i },
  { id: 'stage_state_label', re: /\b(?:stage|state)\s*:\s*(?:prepare|understand|ready|plan)\b/i },
]);

function asText(value) {
  if (value == null) return '';
  return String(value).trim();
}

function collectCopyFields(variant = {}) {
  return [
    asText(variant.subject),
    asText(variant.body),
    asText(variant.cta),
  ].filter(Boolean);
}

function findInternalCopyViolations(text) {
  const hay = asText(text);
  if (!hay) return [];
  return INTERNAL_COPY_PATTERNS.filter(({ re }) => re.test(hay)).map(({ id, re }) => ({
    patternId: id,
    match: hay.match(re)?.[0] || null,
  }));
}

function validateCustomerFacingCopy(text) {
  const violations = findInternalCopyViolations(text);
  return {
    safe: violations.length === 0,
    blocker: violations.length ? BLOCKER : null,
    violations,
  };
}

function validatePaigeVariantCopy(variant = {}) {
  const fields = collectCopyFields(variant);
  const violations = [];
  for (const field of fields) {
    for (const hit of findInternalCopyViolations(field)) {
      violations.push(hit);
    }
  }
  return {
    safe: violations.length === 0,
    blocker: violations.length ? BLOCKER : null,
    violations,
  };
}

function validatePaigeVariantsPayload(payload = {}) {
  const variants = Array.isArray(payload.variants) ? payload.variants : [];
  const violations = [];
  for (const variant of variants) {
    const result = validatePaigeVariantCopy(variant);
    if (!result.safe) {
      violations.push({
        candidateId: variant.candidateId || variant.companyId || variant.variantId || null,
        violations: result.violations,
      });
    }
  }
  return {
    safe: violations.length === 0,
    blocker: violations.length ? BLOCKER : null,
    violations,
  };
}

function firstUnsafeVariant(payload = {}) {
  const variants = Array.isArray(payload.variants) ? payload.variants : [];
  for (const variant of variants) {
    const result = validatePaigeVariantCopy(variant);
    if (!result.safe) return { variant, ...result };
  }
  return null;
}

module.exports = {
  BLOCKER,
  INTERNAL_COPY_PATTERNS,
  findInternalCopyViolations,
  validateCustomerFacingCopy,
  validatePaigeVariantCopy,
  validatePaigeVariantsPayload,
  firstUnsafeVariant,
};
