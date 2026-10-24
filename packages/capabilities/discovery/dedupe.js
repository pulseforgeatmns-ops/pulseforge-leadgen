'use strict';

/**
 * Deduplication for Prospect Discovery (SPEC-024).
 * Remove duplicates · Merge aliases · Prevent CRM duplicates · Tenant boundaries.
 */

/**
 * Normalize company name for alias merge.
 * @param {string} name
 */
function normalizeName(name) {
  return String(name || '')
    .toLowerCase()
    .replace(/\b(llc|inc|corp|ltd|pllc|pc|p\.c\.|co)\b/g, '')
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

/**
 * Normalize website/domain.
 * @param {string} website
 */
function normalizeWebsite(website) {
  if (!website) return '';
  return String(website)
    .toLowerCase()
    .replace(/^https?:\/\//, '')
    .replace(/^www\./, '')
    .replace(/\/.*$/, '')
    .trim();
}

/**
 * Deduplicate candidates in-memory.
 * @param {object[]} candidates
 * @param {object} [rules]
 * @returns {{ unique: object[], duplicatesRemoved: number }}
 */
function dedupeCandidates(candidates, rules = {}) {
  const byWebsite = rules.byWebsite !== false;
  const byNameAddress = rules.byNameAddress !== false;
  const seenWeb = new Map();
  const seenName = new Map();
  const unique = [];
  let duplicatesRemoved = 0;

  for (const c of candidates) {
    const web = normalizeWebsite(c.website || c.url);
    const nameKey = normalizeName(c.companyName || c.company);
    const addrKey = String(c.address || '')
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, ' ')
      .trim()
      .slice(0, 48);
    const nameAddr = `${nameKey}|${addrKey}`;

    if (byWebsite && web && seenWeb.has(web)) {
      mergeInto(seenWeb.get(web), c);
      duplicatesRemoved += 1;
      continue;
    }
    if (byNameAddress && nameKey && seenName.has(nameAddr)) {
      mergeInto(seenName.get(nameAddr), c);
      duplicatesRemoved += 1;
      continue;
    }

    unique.push(c);
    if (web) seenWeb.set(web, c);
    if (nameKey) seenName.set(nameAddr, c);
  }

  return { unique, duplicatesRemoved };
}

function mergeInto(target, alias) {
  if (!target._aliases) target._aliases = [];
  target._aliases.push(alias.companyName || alias.company || alias.id);
  if (!target.phone && alias.phone) target.phone = alias.phone;
  if (!target.website && (alias.website || alias.url)) {
    target.website = alias.website || alias.url;
  }
  if (!target.address && alias.address) target.address = alias.address;
}

/**
 * Mark CRM duplicates on candidates using an async lookup.
 * @param {object[]} candidates
 * @param {object} options
 * @param {string|number} options.tenantId
 * @param {string|number} options.clientId
 * @param {(query: object) => Promise<object[]>} [options.crmLookup]
 *   Returns existing rows: { companyName, website, isCustomer? }
 */
async function flagCrmDuplicates(candidates, options = {}) {
  if (!options.crmLookup || typeof options.crmLookup !== 'function') {
    return { flagged: 0 };
  }
  if (options.respectTenant === false) {
    return { flagged: 0 };
  }

  let existing = [];
  try {
    existing = await options.crmLookup({
      tenantId: options.tenantId,
      clientId: options.clientId,
      websites: candidates.map((c) => normalizeWebsite(c.website || c.url)).filter(Boolean),
      names: candidates.map((c) => normalizeName(c.companyName || c.company)).filter(Boolean),
    });
  } catch {
    return { flagged: 0, warning: 'CRM lookup failed — skipped duplicate check' };
  }

  if (!Array.isArray(existing) || !existing.length) {
    return { flagged: 0 };
  }

  const webSet = new Set(
    existing.map((e) => normalizeWebsite(e.website || e.url)).filter(Boolean)
  );
  const nameSet = new Set(
    existing.map((e) => normalizeName(e.companyName || e.company || e.name)).filter(Boolean)
  );
  const customerWebs = new Set(
    existing
      .filter((e) => e.isCustomer)
      .map((e) => normalizeWebsite(e.website || e.url))
      .filter(Boolean)
  );

  let flagged = 0;
  for (const c of candidates) {
    const web = normalizeWebsite(c.website || c.url);
    const name = normalizeName(c.companyName || c.company);
    if ((web && webSet.has(web)) || (name && nameSet.has(name))) {
      c._existingProspect = true;
      flagged += 1;
    }
    if (web && customerWebs.has(web)) {
      c._existingCustomer = true;
    }
  }
  return { flagged };
}

module.exports = {
  dedupeCandidates,
  flagCrmDuplicates,
  normalizeName,
  normalizeWebsite,
};
