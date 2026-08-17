'use strict';

/**
 * SPEC-100 — retrieve existing tenant-scoped acquisition intelligence
 * before performing new discovery.
 */

const { asText, clone, isPlainObject, normalizeSignal, REJECTION_REASONS } = require('./Types');
const { parseGeographyList } = require('./InvestigationProvenance');

function tokenize(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .trim()
    .split(/\s+/)
    .filter((t) => t.length > 1);
}

function matchesGeography(location, geography) {
  if (!geography) return true;
  const loc = String(location || '').toLowerCase();
  if (!loc) return false;
  const parts = parseGeographyList(geography);
  const haystacks = parts.length ? parts : [geography];
  return haystacks.some((part) => {
    const tokens = tokenize(part).filter(
      (t) => !['nh', 'tn', 'wv', 'area', 'greater', 'and'].includes(t)
    );
    if (!tokens.length) return loc.includes(String(part).toLowerCase());
    return tokens.some((t) => loc.includes(t));
  });
}

function matchesSegment(record, segments) {
  if (!segments || !segments.length) return true;
  const hay = [
    record.industry,
    record.vertical,
    record.segment,
    record.category,
    record.name,
  ]
    .map((v) => String(v || '').toLowerCase().replace(/[_-]+/g, ' '))
    .join(' ');
  return segments.some((seg) => {
    const needle = String(seg || '')
      .toLowerCase()
      .replace(/[_-]+/g, ' ')
      .trim();
    if (!needle) return false;
    const compact = needle.replace(/\s+/g, '');
    return (
      hay.includes(needle) ||
      hay.replace(/\s+/g, '').includes(compact) ||
      (needle.includes('property') && hay.includes('property'))
    );
  });
}

function normalizeCompany(raw, tenantId) {
  if (!raw || typeof raw !== 'object') return null;
  const id = asText(raw.id || raw.companyId);
  if (!id) return null;
  const owner = asText(raw.tenantId || raw.client_id || raw.clientId);
  if (owner && tenantId && String(owner) !== String(tenantId)) return null;
  return {
    id,
    tenantId: owner || tenantId,
    name: asText(raw.name || raw.companyName) || id,
    industry: asText(raw.industry || raw.vertical || raw.segment),
    location: asText(raw.location || raw.geography || raw.city),
    website: asText(raw.website),
    icpScore: raw.icp_score != null ? Number(raw.icp_score) : raw.icpScore,
    people: Array.isArray(raw.people) ? raw.people : [],
    signals: Array.isArray(raw.signals) ? raw.signals : [],
    evidence: Array.isArray(raw.evidence) ? raw.evidence : [],
    updatedAt: asText(raw.updatedAt || raw.updated_at || raw.observedAt),
  };
}

function normalizePerson(raw, tenantId) {
  if (!raw || typeof raw !== 'object') return null;
  const id = asText(raw.id || raw.personId || raw.prospectId);
  if (!id) return null;
  const owner = asText(raw.tenantId || raw.client_id || raw.clientId);
  if (owner && tenantId && String(owner) !== String(tenantId)) return null;
  const name =
    asText(raw.name) ||
    [raw.first_name, raw.last_name].filter(Boolean).join(' ').trim() ||
    id;
  return {
    id,
    tenantId: owner || tenantId,
    companyId: asText(raw.companyId || raw.company_id),
    name,
    jobTitle: asText(raw.job_title || raw.jobTitle || raw.title),
    decisionMaker: raw.decision_maker === true || raw.decisionMaker === true,
    observedAt: asText(raw.updatedAt || raw.updated_at || raw.observedAt),
  };
}

function attachPeople(companies, people) {
  const byCompany = new Map();
  for (const person of people) {
    if (!person.companyId) continue;
    if (!byCompany.has(person.companyId)) byCompany.set(person.companyId, []);
    byCompany.get(person.companyId).push(person);
  }
  return companies.map((c) => ({
    ...c,
    people: c.people.length ? c.people : byCompany.get(c.id) || [],
  }));
}

/**
 * Filter a tenant-scoped company/person repository to the delegated criteria.
 * Does not broaden geography or segment when results are sparse.
 *
 * @param {object} input
 * @returns {object}
 */
function retrieveExistingIntelligence(input = {}) {
  const tenantId = asText(input.authorizedTenantId || input.tenantId);
  const target = isPlainObject(input.targetContext) ? input.targetContext : {};
  const business = isPlainObject(input.businessContext) ? input.businessContext : {};
  const geography = asText(target.geography || business.serviceGeography);
  const segments = Array.isArray(target.segments)
    ? target.segments
    : Array.isArray(business.preferredSegments)
      ? business.preferredSegments
      : [];
  const exclusions = Array.isArray(business.exclusions) ? business.exclusions : [];

  const rawCompanies = Array.isArray(input.companies) ? input.companies : [];
  const rawPeople = Array.isArray(input.people) ? input.people : [];

  const tenantCompanies = rawCompanies
    .map((c) => normalizeCompany(c, tenantId))
    .filter(Boolean);

  const rejectedCandidates = [];
  const inScope = [];
  for (const company of tenantCompanies) {
    if (!matchesGeography(company.location, geography)) {
      rejectedCandidates.push({
        company,
        reason: REJECTION_REASONS.OUTSIDE_GEOGRAPHY,
      });
      continue;
    }
    if (exclusions.length && matchesSegment(company, exclusions)) {
      rejectedCandidates.push({
        company,
        reason: REJECTION_REASONS.EXCLUDED_SEGMENT,
      });
      continue;
    }
    if (!matchesSegment(company, segments)) {
      rejectedCandidates.push({
        company,
        reason: REJECTION_REASONS.INSUFFICIENT_BUSINESS_FIT,
      });
      continue;
    }
    inScope.push(company);
  }

  const people = rawPeople
    .map((p) => normalizePerson(p, tenantId))
    .filter(Boolean)
    .filter((p) => !tenantId || String(p.tenantId) === String(tenantId));

  const matched = attachPeople(inScope, people);
  const rejected = tenantCompanies.length - matched.length;

  return {
    tenantId,
    companies: matched.map(clone),
    discoveredCompanies: tenantCompanies.map(clone),
    rejectedCandidates: rejectedCandidates.map((row) => ({
      company: clone(row.company),
      reason: row.reason,
    })),
    retrievedBeforeInvestigate: true,
    criteria: {
      geography,
      segments: segments.slice(),
      exclusions: exclusions.slice(),
    },
    counts: {
      considered: tenantCompanies.length,
      matched: matched.length,
      rejected: Math.max(0, rejected),
    },
    sufficient: matched.length > 0 && input.requireFresh !== true,
  };
}

async function loadTenantRepository({ tenantId } = {}) {
  const id = asText(tenantId);
  if (!id || !process.env.DATABASE_URL) return { companies: [], people: [] };
  let db;
  try {
    db = require('../../../db');
  } catch {
    return { companies: [], people: [] };
  }
  if (!db || typeof db.query !== 'function') return { companies: [], people: [] };
  try {
    const companies = await db.query(
      `SELECT id, client_id, name, industry, location, website, icp_score, created_at
       FROM companies
       WHERE client_id = $1
       ORDER BY id ASC
       LIMIT 500`,
      [id]
    );
    const prospects = await db.query(
      `SELECT id, client_id, company_id, first_name, last_name, job_title, updated_at
       FROM prospects
       WHERE client_id = $1
         AND COALESCE(do_not_contact, false) = false
       ORDER BY id ASC
       LIMIT 500`,
      [id]
    );
    return {
      companies: (companies.rows || []).map((row) => ({
        id: String(row.id),
        tenantId: String(row.client_id),
        name: row.name,
        industry: row.industry,
        location: row.location,
        website: row.website,
        icpScore: row.icp_score,
        updatedAt: row.created_at,
        source: 'existing_pf',
      })),
      people: (prospects.rows || []).map((row) => ({
        id: String(row.id),
        tenantId: String(row.client_id),
        companyId: row.company_id != null ? String(row.company_id) : null,
        name: [row.first_name, row.last_name].filter(Boolean).join(' ').trim() || String(row.id),
        jobTitle: row.job_title,
        observedAt: row.updated_at,
      })),
    };
  } catch {
    return { companies: [], people: [] };
  }
}

async function loadRepository(input = {}) {
  if (typeof input.loadCompanies === 'function') {
    const loaded = await input.loadCompanies({
      tenantId: input.authorizedTenantId || input.tenantId,
      targetContext: input.targetContext,
      businessContext: input.businessContext,
    });
    const companies = Array.isArray(loaded)
      ? loaded
      : (loaded && loaded.companies) || [];
    const people = Array.isArray(loaded && loaded.people) ? loaded.people : input.people || [];
    return retrieveExistingIntelligence({ ...input, companies, people });
  }
  if (input.companies === undefined && input.defaultLoadFromDb !== false) {
    const loaded = await loadTenantRepository({
      tenantId: input.authorizedTenantId || input.tenantId,
    });
    if ((loaded.companies && loaded.companies.length) || (loaded.people && loaded.people.length)) {
      return retrieveExistingIntelligence({
        ...input,
        companies: loaded.companies,
        people: loaded.people,
      });
    }
  }
  return retrieveExistingIntelligence(input);
}

function signalLabel(signal) {
  const key = normalizeSignal(signal && (signal.type || signal.kind || signal));
  return key || null;
}

module.exports = {
  retrieveExistingIntelligence,
  loadRepository,
  loadTenantRepository,
  matchesGeography,
  matchesSegment,
  normalizeCompany,
  normalizePerson,
  signalLabel,
};
