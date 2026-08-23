'use strict';

/**
 * SPEC-143 — Investigation starting point from stored intelligence.
 * Every new mission begins with Known → Unknown → Need to verify → Need to discover.
 */

const { STARTING_POINT_BUCKETS, MEMORY_STATUS } = require('./types');
const { computeEffectiveConfidence, meetsConfidenceThreshold } = require('./MemoryConfidence');

function bucketClaim(claim, threshold, opts = {}) {
  const effective = computeEffectiveConfidence(claim, opts);
  const verified = claim.verified === true && effective >= threshold;
  const hasConflict = claim.status === MEMORY_STATUS.CONFLICT;

  if (hasConflict) {
    return {
      bucket: STARTING_POINT_BUCKETS.NEED_TO_VERIFY,
      claim: { ...claim, effectiveConfidence: effective },
      reason: 'conflict',
    };
  }
  if (verified && effective >= threshold) {
    return {
      bucket: STARTING_POINT_BUCKETS.KNOWN,
      claim: { ...claim, effectiveConfidence: effective },
      reason: 'verified',
    };
  }
  if (effective >= threshold * 0.7 && (claim.missingEvidence || []).length > 0) {
    return {
      bucket: STARTING_POINT_BUCKETS.NEED_TO_VERIFY,
      claim: { ...claim, effectiveConfidence: effective },
      reason: 'missing_evidence',
    };
  }
  if (effective > 0) {
    return {
      bucket: STARTING_POINT_BUCKETS.UNKNOWN,
      claim: { ...claim, effectiveConfidence: effective },
      reason: 'low_confidence',
    };
  }
  return {
    bucket: STARTING_POINT_BUCKETS.NEED_TO_DISCOVER,
    claim: { ...claim, effectiveConfidence: effective },
    reason: 'no_prior_knowledge',
  };
}

function buildSkippedSteps(investigationMemory, confidenceThreshold = 0.8) {
  if (!investigationMemory) return [];
  const steps = investigationMemory.attemptedSteps || [];
  const remaining = new Set(investigationMemory.remainingGaps || []);
  const overall = investigationMemory.overallConfidence || 0;

  return steps.filter((step) => {
    if (remaining.has(step.gap)) return false;
    return overall >= confidenceThreshold;
  });
}

/**
 * Build the investigation starting point from loaded memory.
 * @param {object} memory — output of loadIntelligenceMemory
 * @param {object} marketDefinition
 * @param {object[]} candidates — optional candidate list for entity matching
 * @param {object} opts
 * @returns {object}
 */
function buildInvestigationStartingPoint(memory = {}, marketDefinition = {}, candidates = [], opts = {}) {
  const threshold = opts.confidenceThreshold != null ? Number(opts.confidenceThreshold) : 0.8;
  const claims = memory.claims || [];
  const companies = memory.companies || [];
  const people = memory.people || [];
  const investigationMemory = memory.investigation || null;

  const known = [];
  const unknown = [];
  const needToVerify = [];
  const needToDiscover = [];

  for (const claim of claims) {
    const result = bucketClaim(claim, threshold, opts);
    const entry = {
      id: claim.id,
      entityId: claim.entityId,
      text: claim.text,
      effectiveConfidence: result.claim.effectiveConfidence,
      reason: result.reason,
    };
    switch (result.bucket) {
      case STARTING_POINT_BUCKETS.KNOWN:
        known.push(entry);
        break;
      case STARTING_POINT_BUCKETS.UNKNOWN:
        unknown.push(entry);
        break;
      case STARTING_POINT_BUCKETS.NEED_TO_VERIFY:
        needToVerify.push(entry);
        break;
      default:
        needToDiscover.push(entry);
    }
  }

  for (const company of companies) {
    const hasClaim = claims.some((c) => c.entityId === company.companyId);
    if (!hasClaim) {
      const effective = computeEffectiveConfidence(company, opts);
      if (meetsConfidenceThreshold(company, threshold * 0.7, opts)) {
        known.push({
          id: company.id,
          entityId: company.companyId,
          text: `Known company: ${company.name}`,
          effectiveConfidence: effective,
          reason: 'company_memory',
        });
      } else {
        needToDiscover.push({
          id: company.id,
          entityId: company.companyId,
          text: `Company ${company.name} needs deeper investigation`,
          effectiveConfidence: effective,
          reason: 'shallow_company_memory',
        });
      }
    }
  }

  const skippedSteps = buildSkippedSteps(investigationMemory, threshold);
  const preloadedClaims = known.map((k) => {
    const full = claims.find((c) => c.id === k.id);
    return full
      ? {
          id: full.claimId || full.id,
          text: full.text,
          entityId: full.entityId,
          confidence: k.effectiveConfidence,
          supportedBy: full.supportedBy || [],
          missingEvidence: full.missingEvidence || [],
          contradictions: full.contradictions || [],
          fromMemory: true,
        }
      : null;
  }).filter(Boolean);

  const inheritedEvidence = companies.flatMap((c) =>
    (c.evidence || []).map((e) => ({ ...e, entityId: c.companyId, fromMemory: true }))
  );

  return {
    known,
    unknown,
    needToVerify,
    needToDiscover,
    skippedSteps,
    preloadedClaims,
    inheritedEvidence,
    marketMemory: memory.market || null,
    investigationMemory,
    counts: {
      known: known.length,
      unknown: unknown.length,
      needToVerify: needToVerify.length,
      needToDiscover: needToDiscover.length,
      skippedSteps: skippedSteps.length,
      companies: companies.length,
      people: people.length,
      claims: claims.length,
    },
    geography: marketDefinition.geography || memory.market?.geography,
    segment: marketDefinition.segment || memory.market?.segment,
  };
}

module.exports = {
  buildInvestigationStartingPoint,
  buildSkippedSteps,
  bucketClaim,
};
