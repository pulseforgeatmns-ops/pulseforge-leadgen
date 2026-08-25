'use strict';

/**
 * SPEC-175 / ADR-092 — Candidate Minimum Contract.
 * A provider must return identity, location, source, and retrieval provenance
 * before evaluation begins. Identity is established by existence evidence —
 * website is enrichment, not the identity gate.
 */

const { asText, nowIso } = require('../../max/scoutAcquisition/Types');
const { establishBusinessIdentity } = require('../identity/BusinessIdentity');

const REQUIRED_FIELDS = Object.freeze(['identity', 'location', 'source', 'retrievalProvenance']);

function candidateIdentity(row = {}) {
  const established = establishBusinessIdentity(row);
  if (established.identityKey) return established.identityKey;
  return (
    asText(row.id || row.companyId || row.placeId || row.candidate_id || row.name) || null
  );
}

function candidateLocation(row = {}, workload = {}) {
  return (
    asText(
      row.location ||
        row.address ||
        row.geography ||
        row.discoveryCity ||
        workload.city
    ) || null
  );
}

function candidateSource(row = {}, workload = {}) {
  return (
    asText(row.discoverySource || row.source || workload.source) || null
  );
}

function buildRetrievalProvenance(row = {}, workload = {}) {
  const existing = row.retrievalProvenance || row.provenance || null;
  if (existing && typeof existing === 'object') {
    return {
      provider: asText(existing.provider || row.discoverySource || row.source || workload.source),
      workloadId: asText(existing.workloadId || workload.id),
      city: asText(existing.city || workload.city || row.discoveryCity),
      concept: asText(existing.concept || workload.concept || row.discoveryConcept),
      retrievedAt: existing.retrievedAt || row.discoveredAt || nowIso(),
    };
  }
  return {
    provider: asText(row.discoverySource || row.source || workload.source),
    workloadId: asText(workload.id),
    city: asText(workload.city || row.discoveryCity),
    concept: asText(workload.concept || row.discoveryConcept),
    retrievedAt: row.discoveredAt || nowIso(),
  };
}

/**
 * Validate a discovered candidate meets the minimum contract.
 * @param {object} row
 * @param {object} [workload]
 * @returns {{ valid: boolean, missing: string[], candidate: object|null }}
 */
function validateCandidateMinimum(row, workload = {}) {
  const identity = candidateIdentity(row);
  const location = candidateLocation(row, workload);
  const source = candidateSource(row, workload);
  const retrievalProvenance = buildRetrievalProvenance(row, workload);

  const missing = [];
  if (!identity) missing.push('identity');
  if (!location) missing.push('location');
  if (!source) missing.push('source');
  if (!retrievalProvenance.provider) missing.push('retrievalProvenance');

  if (missing.length) {
    return { valid: false, missing, candidate: null };
  }

  return {
    valid: true,
    missing: [],
    candidate: {
      ...row,
      identity,
      location,
      source,
      retrievalProvenance,
    },
  };
}

/**
 * Filter and normalize candidates to satisfy the minimum contract.
 * @param {object[]} candidates
 * @param {object} [workload]
 * @returns {{ accepted: object[], rejected: object[] }}
 */
function enforceCandidateMinimumContract(candidates = [], workload = {}) {
  const accepted = [];
  const rejected = [];
  for (const row of candidates || []) {
    const result = validateCandidateMinimum(row, workload);
    if (result.valid) {
      accepted.push(result.candidate);
    } else {
      rejected.push({
        row,
        missing: result.missing,
        reason: `Candidate minimum contract violation: missing ${result.missing.join(', ')}`,
      });
    }
  }
  return { accepted, rejected };
}

module.exports = {
  REQUIRED_FIELDS,
  candidateIdentity,
  candidateLocation,
  candidateSource,
  buildRetrievalProvenance,
  validateCandidateMinimum,
  enforceCandidateMinimumContract,
};
