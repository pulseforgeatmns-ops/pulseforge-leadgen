'use strict';

/**
 * Verification gates for Prospect Discovery (SPEC-024).
 * Website exists · Business appears active · Address verified · Category matches.
 */

const { matchIndustry, buildHaystack } = require('./ranking');

/**
 * @param {object} candidate
 * @param {object} profile
 * @returns {{ ok: boolean, confidence: number, failures: string[], checks: object }}
 */
function verifyCandidate(candidate, profile) {
  const failures = [];
  const checks = {
    websiteExists: false,
    businessActive: false,
    addressVerified: false,
    categoryMatches: false,
  };

  const website = candidate.website || candidate.url || null;
  checks.websiteExists = !!(website && String(website).trim());
  if (
    (profile.requiredSignals || []).includes('active_website') &&
    !checks.websiteExists
  ) {
    failures.push('Website does not exist');
  }

  const hay = buildHaystack(candidate);
  const closed = /\b(permanently\s+closed|out\s+of\s+business)\b/i.test(hay);
  checks.businessActive = !closed;
  if (!checks.businessActive) {
    failures.push('Business does not appear active');
  }

  const address = candidate.address && String(candidate.address).trim();
  checks.addressVerified = !!address;
  if (
    ((profile.requiredSignals || []).includes('verified_address') ||
      (profile.requiredSignals || []).includes('commercial_location')) &&
    !checks.addressVerified
  ) {
    failures.push('Address not verified');
  }

  // Geography soft check when profile has cities
  if (address && profile.geography && Array.isArray(profile.geography.cities)) {
    const cities = profile.geography.cities.map((c) => c.toLowerCase());
    const state = profile.geography.state
      ? String(profile.geography.state).toLowerCase()
      : null;
    const addrLower = address.toLowerCase();
    const inCity = cities.length === 0 || cities.some((c) => addrLower.includes(c));
    const inState = !state || addrLower.includes(state) || addrLower.includes(
      state === 'nh' ? 'new hampshire' : state
    );
    if (!inCity && !inState) {
      failures.push('Address outside profile geography');
      checks.addressVerified = false;
    }
  }

  const industryMatch = matchIndustry(hay, profile.industryTargets || []);
  checks.categoryMatches = !!industryMatch || (profile.industryTargets || []).length === 0;
  if ((profile.industryTargets || []).length && !industryMatch) {
    // Soft fail — still allow review if other signals strong; mark failure for required commercial_location
    if ((profile.requiredSignals || []).includes('commercial_location')) {
      failures.push('Category does not match objective');
    }
  }

  const passed = Object.values(checks).filter(Boolean).length;
  const total = Object.keys(checks).length;
  const confidence = passed / total;

  return {
    ok: failures.length === 0,
    confidence: Number(confidence.toFixed(4)),
    failures,
    checks,
    industryMatch,
  };
}

module.exports = {
  verifyCandidate,
};
