'use strict';

const { EVIDENCE_CLASS, buildFinding } = require('./types');

function gatherBusinessEvidence(input = {}) {
  const observedAt = new Date().toISOString();
  const findings = [];
  const business = {
    business_name: input.business_name || input.company || input.companyName || null,
    domain: input.domain || input.url || null,
    industry: input.industry || input.vertical || null,
    location: input.location || input.address || null,
    operating_status: input.operating_status || 'unknown',
    contact_name: input.contact || input.contact_name || null,
    email: input.email || null,
    phone: input.phone || null,
    google_rating: input.google_rating ?? null,
    google_review_count: input.google_review_count ?? null,
    multi_location: Boolean(input.multi_location),
    hiring_signal: Boolean(input.hiring_signal),
    recent_growth_signal: Boolean(input.recent_growth_signal),
    advertising_signal: Boolean(input.advertising_signal),
    website_platform: input.website_platform || null,
    service_value_proxy: input.service_value_proxy || null,
  };

  if (business.industry) {
    findings.push(buildFinding({
      id: 'biz_industry',
      evidence_class: EVIDENCE_CLASS.OBSERVED,
      category: 'business',
      summary: `Industry recorded as ${business.industry}`,
      source: 'scout_discovery',
      observed_at: observedAt,
      ref: 'business:industry',
    }));
  }

  if (business.location) {
    findings.push(buildFinding({
      id: 'biz_location',
      evidence_class: EVIDENCE_CLASS.OBSERVED,
      category: 'business',
      summary: `Location recorded as ${business.location}`,
      source: 'scout_discovery',
      observed_at: observedAt,
      ref: 'business:location',
    }));
  }

  if (business.google_rating != null) {
    findings.push(buildFinding({
      id: 'biz_rating',
      evidence_class: EVIDENCE_CLASS.OBSERVED,
      category: 'business',
      summary: `Google rating ${business.google_rating} (${business.google_review_count || 0} reviews)`,
      source: 'places',
      observed_at: observedAt,
      ref: 'business:google_rating',
    }));
  }

  for (const [flag, label] of [
    ['hiring_signal', 'Active hiring signal'],
    ['recent_growth_signal', 'Recent growth/expansion signal'],
    ['advertising_signal', 'Advertising/acquisition activity signal'],
  ]) {
    if (business[flag]) {
      findings.push(buildFinding({
        id: `biz_${flag}`,
        evidence_class: EVIDENCE_CLASS.OBSERVED,
        category: 'business',
        summary: label,
        source: 'scout_discovery',
        observed_at: observedAt,
        ref: `business:${flag}`,
      }));
    }
  }

  return { business, findings };
}

module.exports = {
  gatherBusinessEvidence,
};
