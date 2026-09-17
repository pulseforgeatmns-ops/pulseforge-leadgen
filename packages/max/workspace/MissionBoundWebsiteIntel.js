'use strict';

/**
 * Resolve website / domain intelligence from mission-bound upstream payloads.
 * Scout discovery → Max prioritization → mission-bound candidate builder.
 *
 * Never fabricates a domain from company name.
 */

const { normalizeDomain, asText } = require('./CanonicalOutboundIdentity');

const WEBSITE_FIELD_PATHS = Object.freeze([
  'website',
  'website_url',
  'url',
  'sourceUrl',
  'source_url',
  'domain',
  'companyDomain',
  'company_domain',
  'normalizedDomain',
  'normalized_domain',
  'placeWebsite',
  'place_website',
]);

const SOURCE_PRIORITY = Object.freeze([
  { key: 'max', label: 'max.prioritization' },
  { key: 'scout_opportunity', label: 'scout.discovery.opportunities' },
  { key: 'scout_prospect', label: 'scout.discovery.prospects' },
  { key: 'evidence', label: 'evidenceRefs.snapshot' },
]);

function readWebsiteField(row = {}, field) {
  const value = row[field];
  if (value == null) return null;
  const text = asText(value);
  return text || null;
}

function firstWebsiteField(row = {}) {
  if (!row || typeof row !== 'object') return null;
  for (const field of WEBSITE_FIELD_PATHS) {
    const value = readWebsiteField(row, field);
    if (!value) continue;
    const domainField = field === 'domain'
      || field === 'companyDomain'
      || field === 'company_domain'
      || field === 'normalizedDomain'
      || field === 'normalized_domain';
    return {
      raw: value,
      field,
      fieldPath: field,
      domain: normalizeDomain(domainField ? value : value),
      website: domainField ? (value.includes('://') ? value : `https://${value}`) : value,
    };
  }
  return null;
}

function websiteFromEvidenceRefs(refs = []) {
  if (!Array.isArray(refs)) return null;
  for (const ref of refs) {
    const snapshot = ref?.snapshot && typeof ref.snapshot === 'object' ? ref.snapshot : {};
    const hit = firstWebsiteField({
      website: snapshot.website || snapshot.url || snapshot.sourceUrl || snapshot.source_url,
      url: snapshot.url,
      sourceUrl: snapshot.sourceUrl || snapshot.source_url,
      domain: snapshot.domain,
    });
    if (hit) {
      return {
        ...hit,
        fieldPath: 'evidenceRefs.snapshot',
        evidenceRefId: ref.id || null,
        evidenceLabel: ref.label || null,
        evidenceSource: ref.source || snapshot.source || null,
      };
    }
  }
  return null;
}

function collectWebsiteFieldTraces({ target = {}, opp = {}, prospect = null } = {}) {
  const traces = [];
  const addTrace = (sourceKey, sourceLabel, row = {}) => {
    const hit = firstWebsiteField(row);
    if (!hit) return;
    traces.push({
      source: sourceKey,
      sourceLabel,
      field: hit.field,
      fieldPath: `${sourceLabel}.${hit.fieldPath}`,
      raw: hit.raw,
      domain: hit.domain,
      website: hit.website,
    });
    const evidenceHit = websiteFromEvidenceRefs(row.evidenceRefs);
    if (evidenceHit) {
      traces.push({
        source: 'evidence',
        sourceLabel: 'evidenceRefs.snapshot',
        field: evidenceHit.field,
        fieldPath: evidenceHit.fieldPath,
        raw: evidenceHit.raw,
        domain: evidenceHit.domain,
        website: evidenceHit.website,
        evidenceRefId: evidenceHit.evidenceRefId,
        evidenceLabel: evidenceHit.evidenceLabel,
        evidenceSource: evidenceHit.evidenceSource,
      });
    }
  };

  addTrace('max', 'max.prioritization.rankedTargets', target);
  addTrace('scout_opportunity', 'scout.discovery.opportunities', opp);
  if (prospect) addTrace('scout_prospect', 'scout.discovery.prospects', prospect);

  const evidenceOnly = websiteFromEvidenceRefs([
    ...(Array.isArray(target.evidenceRefs) ? target.evidenceRefs : []),
    ...(Array.isArray(opp.evidenceRefs) ? opp.evidenceRefs : []),
    ...(prospect && Array.isArray(prospect.evidenceRefs) ? prospect.evidenceRefs : []),
  ]);
  if (evidenceOnly && !traces.some((row) => row.source === 'evidence')) {
    traces.push({
      source: 'evidence',
      sourceLabel: 'evidenceRefs.snapshot',
      field: evidenceOnly.field,
      fieldPath: evidenceOnly.fieldPath,
      raw: evidenceOnly.raw,
      domain: evidenceOnly.domain,
      website: evidenceOnly.website,
      evidenceRefId: evidenceOnly.evidenceRefId,
      evidenceLabel: evidenceOnly.evidenceLabel,
      evidenceSource: evidenceOnly.evidenceSource,
    });
  }

  return traces;
}

/**
 * Pick the strongest upstream website/domain hit by source priority.
 * @returns {{ domain: string|null, website: string|null, source: string|null, fieldPath: string|null, evidenceRefs: object[]|null, traces: object[] }}
 */
function resolveMissionBoundWebsiteIntel({ target = {}, opp = {}, prospect = null } = {}) {
  const traces = collectWebsiteFieldTraces({ target, opp, prospect });
  for (const tier of SOURCE_PRIORITY) {
    const hit = traces.find((row) => row.source === tier.key);
    if (hit?.domain || hit?.website) {
      return {
        domain: hit.domain || normalizeDomain(hit.website),
        website: hit.website || null,
        source: tier.label,
        fieldPath: hit.fieldPath || null,
        evidenceRefs: hit.evidenceRefId
          ? [{ id: hit.evidenceRefId, label: hit.evidenceLabel, source: hit.evidenceSource }]
          : null,
        traces,
      };
    }
  }
  return {
    domain: null,
    website: null,
    source: null,
    fieldPath: null,
    evidenceRefs: null,
    traces,
  };
}

function applyWebsiteIntelToCandidate(row = {}, intel = {}) {
  const domain = intel?.domain || row.domain || null;
  const website = intel?.website || row.website || row.website_url || null;
  if (!domain && !website) {
    return {
      ...row,
      domain: null,
      website: null,
      website_url: null,
      websiteTraces: intel?.traces || row.websiteTraces || [],
    };
  }
  return {
    ...row,
    domain,
    website,
    website_url: website,
    websiteSource: intel.source || row.websiteSource || null,
    websiteFieldPath: intel.fieldPath || row.websiteFieldPath || null,
    websiteEvidenceRefs: intel.evidenceRefs || row.websiteEvidenceRefs || null,
    websiteTraces: intel.traces || row.websiteTraces || [],
  };
}

function candidateWebsiteFields(candidate = {}) {
  const website = candidate.website || candidate.website_url || null;
  const domain = normalizeDomain(candidate.domain || website);
  return {
    domain,
    website,
    websiteSource: candidate.websiteSource || null,
    websiteFieldPath: candidate.websiteFieldPath || null,
    websiteEvidenceRefs: candidate.websiteEvidenceRefs || null,
  };
}

module.exports = {
  WEBSITE_FIELD_PATHS,
  collectWebsiteFieldTraces,
  resolveMissionBoundWebsiteIntel,
  applyWebsiteIntelToCandidate,
  candidateWebsiteFields,
};
