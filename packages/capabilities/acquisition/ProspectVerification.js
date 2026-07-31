'use strict';

/**
 * Prospect Verification — CandidateSet → ProspectList (SPEC-060).
 * Shared by all acquisition methods. Reuses SPEC-024 verification gates.
 */

const { verifyCandidate } = require('../discovery/verification');
const { dedupeCandidates } = require('../discovery/dedupe');
const { buildProspect } = require('../discovery/types');
const {
  buildCandidateSet,
  buildAcquisitionEvidence,
} = require('./types');

/**
 * Verify a CandidateSet into a ProspectList payload + report.
 *
 * @param {object} input
 * @param {object} [input.candidateSet]
 * @param {object[]} [input.candidates]
 * @param {object} [input.profile] - Discovery profile (optional soft gates)
 * @param {object} [input.options]
 * @returns {object}
 */
function verifyCandidateSet(input = {}) {
  const set =
    input.candidateSet ||
    buildCandidateSet({
      candidates: input.candidates || [],
      provider: input.provider,
      acquisitionSource: input.acquisitionSource,
      missionId: input.missionId,
      operator: input.operator,
    });

  const profile = input.profile || {
    requiredSignals: [],
    industryTargets: [],
    geography: null,
  };
  const options = input.options || {};
  const operatorSupplied =
    options.operatorSupplied === true ||
    [
      'csv_import',
      'manual_prospect_list',
      'spreadsheet_paste',
      'existing_prospect_repository',
    ].includes(String(set.acquisitionSource || ''));
  // Operator / CSV / existing lists: company name is enough (SPEC-043 + SPEC-060)
  const skipStrictWebsite =
    options.skipStrictWebsite === true ||
    (options.skipStrictWebsite !== false && operatorSupplied);

  const effectiveProfile = skipStrictWebsite
    ? {
        ...profile,
        requiredSignals: (profile.requiredSignals || []).filter(
          (s) =>
            s !== 'active_website' &&
            s !== 'verified_address' &&
            s !== 'commercial_location'
        ),
      }
    : profile;

  const raw = Array.isArray(set.candidates) ? set.candidates : [];
  const deduped = dedupeCandidates(raw);
  const unique = Array.isArray(deduped.unique) ? deduped.unique : raw;
  const duplicatesRemoved = Number(deduped.duplicatesRemoved) || 0;

  const accepted = [];
  const rejected = [];
  const checks = [];

  for (const candidate of unique) {
    const result = verifyCandidate(candidate, effectiveProfile);
    checks.push({
      candidateId: candidate.id,
      companyName: candidate.companyName,
      ok: result.ok,
      confidence: result.confidence,
      failures: result.failures,
      checks: result.checks,
    });

    if (!result.ok && !options.acceptSoftFailures) {
      const hardClosed = (result.failures || []).some((f) =>
        /does not appear active/i.test(f)
      );
      if (skipStrictWebsite && !hardClosed) {
        accepted.push({
          ...candidate,
          verification: result,
          status: 'verified',
        });
        continue;
      }
      rejected.push({
        ...candidate,
        status: 'rejected',
        rejectionReasons: result.failures,
        verification: result,
      });
      continue;
    }

    accepted.push({
      ...candidate,
      verification: result,
      status: 'verified',
    });
  }

  const prospects = accepted.map((c, i) => {
    const built =
      typeof buildProspect === 'function'
        ? buildProspect({
            ...c,
            source: (c.provenance && c.provenance.acquisitionSource) || c.source,
            confidence: (c.verification && c.verification.confidence) || 0.7,
          })
        : null;
    const prospect = built || {
      id: c.id || `p_${i + 1}`,
      companyName: c.companyName,
      website: c.website,
      address: c.address,
      phone: c.phone,
      email: c.email,
      contactName: c.contactName,
      notes: c.notes,
      industry: c.industry,
      source: (c.provenance && c.provenance.acquisitionSource) || 'acquisition',
      status: 'verified',
      confidence: (c.verification && c.verification.confidence) || 0.7,
    };
    return {
      ...prospect,
      provenance: c.provenance || null,
      naics: c.naics || null,
      metadata: c.metadata || {},
    };
  });

  const verificationReport = {
    type: 'VerificationReport',
    inputCandidateCount: raw.length,
    acceptedCount: accepted.length,
    rejectedCount: rejected.length,
    duplicateCount: duplicatesRemoved,
    checks,
    provider: set.provider || null,
    acquisitionSource: set.acquisitionSource || null,
    operatorSupplied: Boolean(skipStrictWebsite),
    at: new Date().toISOString(),
  };

  const prospectList = {
    type: 'ProspectList',
    prospects,
    prospectCount: prospects.length,
    targetCount:
      input.targetCount != null ? Number(input.targetCount) : prospects.length,
    summary: {
      discovered: raw.length,
      verified: prospects.length,
      rejected: rejected.length,
      targetCount:
        input.targetCount != null
          ? Number(input.targetCount)
          : prospects.length,
      acquisitionSource: set.acquisitionSource || null,
      provider: set.provider || null,
      operatorSupplied: Boolean(skipStrictWebsite),
    },
    provenance: {
      acquisitionSource: set.acquisitionSource || null,
      provider: set.provider || null,
      missionId: set.missionId || null,
      verifiedAt: verificationReport.at,
    },
  };

  return {
    ok: prospects.length > 0,
    candidateSet: set,
    prospectList,
    rejectedProspects: rejected,
    verificationReport,
    evidence: [
      ...(Array.isArray(set.evidence) ? set.evidence : []),
      buildAcquisitionEvidence({
        provider: set.provider,
        acquisitionSource: set.acquisitionSource,
        candidateCount: prospects.length,
        missionId: set.missionId,
        summary: `Verified ${prospects.length}/${raw.length} candidates → ProspectList`,
        details: {
          rejected: rejected.length,
          duplicates: duplicatesRemoved,
        },
      }),
      {
        kind: 'verification_report',
        summary: verificationReport,
      },
    ],
    warnings: Array.isArray(set.warnings) ? set.warnings : [],
  };
}

module.exports = {
  verifyCandidateSet,
};
