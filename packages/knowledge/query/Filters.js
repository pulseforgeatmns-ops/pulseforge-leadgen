'use strict';

/**
 * Predicate helpers for Knowledge Query Engine filters (SPEC-001C).
 */

/**
 * @param {unknown} value
 * @returns {string}
 */
function asLower(value) {
  return value == null ? '' : String(value).toLowerCase();
}

/**
 * Case-insensitive substring match; empty needle always matches.
 * @param {unknown} haystack
 * @param {string|undefined|null} needle
 */
function includesCI(haystack, needle) {
  if (needle == null || needle === '') return true;
  return asLower(haystack).includes(asLower(needle));
}

/**
 * @param {object} node
 * @param {string} key
 */
function meta(node, key) {
  if (!node || !node.metadata || typeof node.metadata !== 'object') return null;
  return node.metadata[key];
}

/**
 * @param {object} node
 * @param {string} [createdAfter]
 */
function matchesCreatedAfter(node, createdAfter) {
  if (!createdAfter) return true;
  const t = Date.parse(node.createdAt);
  const after = Date.parse(createdAfter);
  if (!Number.isFinite(t) || !Number.isFinite(after)) return true;
  return t >= after;
}

/**
 * @param {object} node
 * @param {number|undefined|null} confidenceMin
 */
function matchesConfidenceMin(node, confidenceMin) {
  if (confidenceMin == null) return true;
  const min = Number(confidenceMin);
  if (!Number.isFinite(min)) return true;
  const conf = Number(node.confidence);
  if (!Number.isFinite(conf)) return false;
  return conf >= min;
}

/**
 * Technology may live on metadata.technology / tech_stack / techStack (array or string).
 * @param {object} node
 * @param {string|undefined|null} technology
 */
function matchesTechnology(node, technology) {
  if (technology == null || technology === '') return true;
  const needle = asLower(technology);
  const candidates = [
    node.technology,
    meta(node, 'technology'),
    meta(node, 'tech_stack'),
    meta(node, 'techStack'),
  ];
  for (const c of candidates) {
    if (c == null) continue;
    if (Array.isArray(c)) {
      if (c.some((item) => asLower(item).includes(needle))) return true;
    } else if (asLower(c).includes(needle)) {
      return true;
    }
  }
  return false;
}

/**
 * @param {object} node
 * @param {import('./QueryTypes').CompanyQuery} query
 */
function matchesCompany(node, query) {
  const industry = query.industry;
  const location = query.location;
  if (
    !includesCI(node.industry != null ? node.industry : meta(node, 'industry'), industry)
  ) {
    return false;
  }
  if (
    !includesCI(node.location != null ? node.location : meta(node, 'location'), location)
  ) {
    return false;
  }
  if (!matchesTechnology(node, query.technology)) return false;
  if (!matchesCreatedAfter(node, query.createdAfter)) return false;
  // Companies lack a native confidence field; metadata.confidence is optional.
  if (query.confidenceMin != null) {
    const conf =
      node.confidence != null ? node.confidence : meta(node, 'confidence');
    if (!matchesConfidenceMin({ confidence: conf }, query.confidenceMin)) return false;
  }
  return true;
}

/**
 * @param {object} node
 * @param {import('./QueryTypes').PersonQuery} query
 */
function matchesPerson(node, query) {
  if (query.email != null && query.email !== '') {
    if (asLower(node.email) !== asLower(query.email)) return false;
  }
  if (!includesCI(node.title, query.title)) return false;
  if (!includesCI(node.name, query.name)) return false;
  if (!matchesCreatedAfter(node, query.createdAfter)) return false;
  if (query.confidenceMin != null) {
    const conf =
      node.confidence != null ? node.confidence : meta(node, 'confidence');
    if (!matchesConfidenceMin({ confidence: conf }, query.confidenceMin)) return false;
  }
  return true;
}

/**
 * @param {object} node
 * @param {import('./QueryTypes').InteractionQuery} query
 */
function matchesInteraction(node, query) {
  if (query.channel != null && query.channel !== '') {
    if (asLower(node.channel) !== asLower(query.channel)) return false;
  }
  if (query.actionType != null && query.actionType !== '') {
    if (asLower(node.actionType) !== asLower(query.actionType)) return false;
  }
  if (!matchesCreatedAfter(node, query.createdAfter)) return false;
  if (query.occurredAfter) {
    const t = Date.parse(node.occurredAt || node.createdAt);
    const after = Date.parse(query.occurredAfter);
    if (Number.isFinite(t) && Number.isFinite(after) && t < after) return false;
  }
  return true;
}

/**
 * @param {object} node
 * @param {import('./QueryTypes').EvidenceQuery} query
 */
function matchesEvidence(node, query) {
  if (query.sourceType != null && query.sourceType !== '') {
    if (asLower(node.sourceType) !== asLower(query.sourceType)) return false;
  }
  if (query.sourceId != null && query.sourceId !== '') {
    if (String(node.sourceId) !== String(query.sourceId)) return false;
  }
  if (!matchesConfidenceMin(node, query.confidenceMin)) return false;
  if (!matchesCreatedAfter(node, query.createdAfter)) return false;
  return true;
}

/**
 * @param {object} node
 * @param {import('./QueryTypes').ClaimQuery} query
 */
function matchesClaim(node, query) {
  if (query.status != null && query.status !== '') {
    if (asLower(node.status) !== asLower(query.status)) return false;
  }
  if (!matchesConfidenceMin(node, query.confidenceMin)) return false;
  if (!matchesCreatedAfter(node, query.createdAfter)) return false;
  return true;
}

/**
 * Stable sort by id for deterministic filter results.
 * @param {object[]} nodes
 */
function sortById(nodes) {
  return [...nodes].sort((a, b) => String(a.id).localeCompare(String(b.id)));
}

module.exports = {
  asLower,
  includesCI,
  meta,
  matchesCreatedAfter,
  matchesConfidenceMin,
  matchesTechnology,
  matchesCompany,
  matchesPerson,
  matchesInteraction,
  matchesEvidence,
  matchesClaim,
  sortById,
};
