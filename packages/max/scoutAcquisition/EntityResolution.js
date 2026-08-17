'use strict';

/**
 * SPEC-100A — resolve discovered businesses against existing PF companies
 * and deduplicate aliases (LLC / Mgmt / punctuation) to one entity.
 */

const { asText, clone, nowIso } = require('./Types');
const {
  normalizeName,
  normalizeWebsite,
} = require('../../capabilities/discovery/dedupe');

function normalizeAddress(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/\b(suite|ste|unit|floor|fl|#)\b/g, '')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim()
    .slice(0, 56);
}

function entityKeys(record) {
  const name = normalizeName(record.name || record.companyName || record.company);
  const domain = normalizeWebsite(record.website || record.url || record.domain);
  const address = normalizeAddress(record.address || record.location);
  const id = asText(record.id || record.companyId || record.placeId);
  return { name, domain, address, id, nameAddr: name && address ? `${name}|${address}` : name };
}

function recordsMatch(a, b) {
  const left = entityKeys(a);
  const right = entityKeys(b);
  if (left.id && right.id && left.id === right.id) return true;
  if (left.domain && right.domain && left.domain === right.domain) return true;
  if (left.name && right.name && left.name === right.name) {
    if (!left.address || !right.address) return true;
    if (left.address === right.address) return true;
    if (left.address.includes(right.address) || right.address.includes(left.address)) {
      return true;
    }
  }
  return false;
}

function mergeResolved(existing, incoming) {
  const out = { ...existing };
  for (const key of Object.keys(incoming || {})) {
    if (out[key] == null && incoming[key] != null) out[key] = incoming[key];
  }
  if (!out.aliases) out.aliases = [];
  const alias = incoming.name || incoming.companyName;
  if (alias && alias !== out.name && !out.aliases.includes(alias)) {
    out.aliases.push(alias);
  }
  if (Array.isArray(incoming.signals) && incoming.signals.length) {
    out.signals = [...(out.signals || []), ...incoming.signals];
  }
  if (Array.isArray(incoming.people) && incoming.people.length) {
    out.people = [...(out.people || []), ...incoming.people];
  }
  if (Array.isArray(incoming.evidence) && incoming.evidence.length) {
    out.evidence = [...(out.evidence || []), ...incoming.evidence];
  }
  out.resolvedFrom = Array.from(
    new Set([...(out.resolvedFrom || []), incoming.source || incoming.discoverySource].filter(Boolean))
  );
  return out;
}

/**
 * Resolve a discovered candidate against existing PF companies.
 * @returns {{ company: object, match: 'existing'|'new', existingId: string|null }}
 */
function resolveCandidate(candidate, existingCompanies = []) {
  for (const existing of existingCompanies) {
    if (recordsMatch(existing, candidate)) {
      return {
        company: mergeResolved(clone(existing), candidate),
        match: 'existing',
        existingId: existing.id,
      };
    }
  }
  const name = asText(candidate.name || candidate.companyName) || 'unknown';
  const id =
    asText(candidate.id || candidate.companyId) ||
    `disc-${normalizeName(name).replace(/\s+/g, '-') || 'company'}`;
  return {
    company: {
      id,
      tenantId: candidate.tenantId || null,
      name,
      industry: asText(candidate.industry || candidate.vertical || candidate.segment),
      location: asText(candidate.location || candidate.address || candidate.geography),
      website: asText(candidate.website || candidate.url),
      address: asText(candidate.address),
      icpScore: candidate.icpScore != null ? Number(candidate.icpScore) : candidate.icp_score,
      people: Array.isArray(candidate.people) ? candidate.people : [],
      signals: Array.isArray(candidate.signals) ? candidate.signals : [],
      evidence: Array.isArray(candidate.evidence) ? candidate.evidence : [],
      discoveredAt: candidate.discoveredAt || nowIso(),
      lastEvaluatedAt: candidate.lastEvaluatedAt || null,
      source: candidate.source || candidate.discoverySource || 'discovery',
      aliases: [],
      resolvedFrom: [candidate.source || candidate.discoverySource].filter(Boolean),
    },
    match: 'new',
    existingId: null,
  };
}

/**
 * Deduplicate a mixed list of existing + discovered companies.
 */
function resolveCandidateUniverse(existingCompanies, discoveredCandidates) {
  const resolved = [];
  const seen = [];
  let duplicatesRemoved = 0;
  let resolvedToExisting = 0;

  function ingest(record, fromDiscovery) {
    for (let i = 0; i < seen.length; i += 1) {
      if (recordsMatch(seen[i], record)) {
        resolved[i] = mergeResolved(resolved[i], record);
        seen[i] = resolved[i];
        duplicatesRemoved += 1;
        if (fromDiscovery && resolved[i].id && !String(record.id || '').startsWith('disc-')) {
          resolvedToExisting += 1;
        } else if (fromDiscovery) {
          resolvedToExisting += 1;
        }
        return;
      }
    }
    const step = fromDiscovery
      ? resolveCandidate(record, existingCompanies)
      : { company: clone(record), match: 'existing', existingId: record.id };
    resolved.push(step.company);
    seen.push(step.company);
    if (fromDiscovery && step.match === 'existing') resolvedToExisting += 1;
  }

  for (const company of existingCompanies || []) ingest(company, false);
  for (const candidate of discoveredCandidates || []) ingest(candidate, true);

  return {
    companies: resolved,
    duplicatesRemoved,
    resolvedToExisting,
    candidatesResolved: resolved.length,
  };
}

module.exports = {
  normalizeName,
  normalizeWebsite,
  normalizeAddress,
  entityKeys,
  recordsMatch,
  resolveCandidate,
  resolveCandidateUniverse,
  mergeResolved,
};
