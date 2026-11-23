'use strict';

const { TENANT_ID } = require('./leviScoutProductionContract');
const { inferCityFromProvenance } = require('./leviScoutProvenance');
const soda = require('./leviScoutSodaClient');

const USER_AGENT = soda.USER_AGENT;
const CHICAGO_OWNERS_URL = 'https://data.cityofchicago.org/resource/ezma-pppn.json';
const CHICAGO_LICENSES_URL = 'https://data.cityofchicago.org/resource/r5kz-chrr.json';
const DECISION_TITLES = Object.freeze([
  'OWNER', 'PRESIDENT', 'MANAGING MEMBER', 'MEMBER', 'MANAGER', 'CEO',
  'PRINCIPAL', 'PARTNER', 'MANAGING PARTNER', 'DIRECTOR',
]);

function fail(code) { throw Object.assign(new Error(code), { code }); }

function text(value) {
  return value == null ? '' : String(value).trim();
}

function sodaLiteral(value) {
  return String(value || '').replace(/'/g, "''");
}

function normalizeEntityName(value) {
  return text(value).toUpperCase().replace(/\s+/g, ' ');
}

function namesMatch(left, right) {
  const a = normalizeEntityName(left);
  const b = normalizeEntityName(right);
  return Boolean(a && b && a === b);
}

function normalizePhone(value) {
  const digits = text(value).replace(/\D/g, '');
  if (digits.length === 10) return `(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6)}`;
  if (digits.length === 11 && digits.startsWith('1')) {
    return `(${digits.slice(1, 4)}) ${digits.slice(4, 7)}-${digits.slice(7)}`;
  }
  return null;
}

function normalizeEmail(value) {
  const email = text(value).toLowerCase();
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email) ? email : null;
}

function sodaUrl(base, where, limit = 5) {
  const params = new URLSearchParams({
    $limit: String(limit),
    $where: where,
  });
  return `${base}?${params.toString()}`;
}

function provenance({ value, source, evidence, observedAt, field, limitations }) {
  if (value == null || value === '') return null;
  return Object.freeze({
    value,
    field,
    source,
    evidence,
    observedAt,
    limitations: limitations || 'Public-record value only. Ownership and contact details were not inferred.',
  });
}

function titleRank(title) {
  const upper = text(title).toUpperCase();
  const index = DECISION_TITLES.findIndex(token => upper.includes(token));
  return index === -1 ? DECISION_TITLES.length : index;
}

function chicagoOwnerFromPermit(row = {}) {
  for (let i = 1; i <= 15; i += 1) {
    const type = text(row[`contact_${i}_type`]).toUpperCase();
    const name = text(row[`contact_${i}_name`]);
    if (!type.startsWith('OWNER') || !name) continue;
    const city = text(row[`contact_${i}_city`]);
    const state = text(row[`contact_${i}_state`]);
    const zip = text(row[`contact_${i}_zipcode`] || row[`contact_${i}_zip`]);
    const phone = normalizePhone(row[`contact_${i}_phone`]);
    const email = normalizeEmail(row[`contact_${i}_email`]);
    return { name, type, city, state, zip, phone, email, index: i };
  }
  return null;
}

function licenseAddress(row = {}) {
  const street = text(row.address || row.street_address);
  const city = text(row.city);
  const state = text(row.state);
  const zip = text(row.zip_code || row.zip);
  const parts = [street, [city, state].filter(Boolean).join(', '), zip].filter(Boolean);
  return parts.length ? parts.join(' ') : null;
}

function pickDecisionMaker(rows = [], entityName) {
  const matched = rows.filter(row => namesMatch(row.doing_business_as_name, entityName)
    || namesMatch(row.legal_name, entityName));
  if (!matched.length) return null;
  const ranked = [...matched].sort((a, b) => titleRank(a.owner_title) - titleRank(b.owner_title));
  const row = ranked[0];
  const first = text(row.owner_first_name);
  const last = text(row.owner_last_name);
  const title = text(row.owner_title);
  if (!first && !last && !title) return null;
  return {
    row,
    title: title || null,
    name: [first, last].filter(Boolean).join(' ') || null,
  };
}

function pickLicense(rows = [], entityName) {
  return rows.find(row => namesMatch(row.doing_business_as_name, entityName)
    || namesMatch(row.legal_name, entityName)) || null;
}

function emptyLookup(overrides = {}) {
  return {
    city: null,
    citySource: null,
    requests: 0,
    timeouts: 0,
    retries: 0,
    ownersStatus: 'skipped',
    licensesStatus: 'skipped',
    ownersExactMatch: false,
    licensesExactMatch: false,
    partial: false,
    failures: [],
    ...overrides,
  };
}

async function queryDataset({ fetchImpl, url, clauses, budget, wait, telemetry }) {
  let lastStatus = 'empty';
  for (const where of clauses) {
    const result = await soda.boundedReadJson({
      fetchImpl,
      url: sodaUrl(url, where),
      budget,
      wait,
      telemetry,
    });
    lastStatus = result.status;
    if (result.rows.length) return { rows: result.rows, status: 'ok' };
    if (result.status === 'timeout' || result.status === 'http_error' || result.status === 'error' || result.status === 'skipped_budget') {
      return { rows: [], status: result.status };
    }
  }
  return { rows: [], status: lastStatus };
}

async function lookupChicago(fetchImpl, entityName, { wait, telemetry, budget } = {}) {
  const name = sodaLiteral(normalizeEntityName(entityName));
  const events = Array.isArray(telemetry) ? telemetry : [];
  const lookupBudget = budget || soda.createLookupBudget();
  if (!name) {
    return {
      owners: [],
      licenses: [],
      ownersStatus: 'empty',
      licensesStatus: 'empty',
      telemetry: events,
      requests: lookupBudget.used(),
    };
  }
  const clause = `(upper(doing_business_as_name)='${name}' OR upper(legal_name)='${name}')`;
  const dbaOnly = `upper(doing_business_as_name)='${name}'`;
  const owners = await queryDataset({
    fetchImpl,
    url: CHICAGO_OWNERS_URL,
    clauses: [dbaOnly, clause],
    budget: lookupBudget,
    wait,
    telemetry: events,
  });
  const licenses = await queryDataset({
    fetchImpl,
    url: CHICAGO_LICENSES_URL,
    clauses: [clause, dbaOnly],
    budget: lookupBudget,
    wait,
    telemetry: events,
  });
  return {
    owners: owners.rows,
    licenses: licenses.rows,
    ownersStatus: owners.status,
    licensesStatus: licenses.status,
    telemetry: events,
    requests: lookupBudget.used(),
  };
}

function summarizeLookup(events = []) {
  const timeouts = events.filter(event => event.status === 'timeout').length;
  const retries = events.filter(event => event.attempt > 1).length;
  const failures = events
    .filter(event => ['timeout', 'http_error', 'error', 'skipped_budget'].includes(event.status))
    .map(event => ({
      datasetId: event.datasetId || soda.datasetIdOf(event.url),
      status: event.status,
      attempt: event.attempt || null,
      code: event.code || null,
      httpStatus: event.httpStatus || null,
    }));
  return { timeouts, retries, failures };
}

function emptyResult(limitations, lookup = emptyLookup()) {
  return Object.freeze({
    operatingBusiness: null,
    decisionMakerRole: null,
    phone: null,
    email: null,
    address: null,
    verifiable: false,
    fabricated: false,
    paidProvidersUsed: false,
    limitations: Object.freeze([limitations].filter(Boolean)),
    lookup: Object.freeze(lookup),
  });
}

async function researchContact({
  tenantId = TENANT_ID,
  entityName,
  city,
  sourceRow = {},
  permalink,
  sourceUrls,
  location,
  provider,
  observedAt,
  fetch: fetchImpl = globalThis.fetch,
  cache,
  wait,
} = {}) {
  if (String(tenantId) !== String(TENANT_ID)) fail('levi_research_tenant_invalid');
  const name = text(entityName);
  if (!name) return emptyResult('No operating-entity name was present on the public permit.');
  const inferred = inferCityFromProvenance({
    city,
    sourceUrls: sourceUrls || (permalink ? [permalink] : []),
    permalink,
    location,
    provider,
  });
  const resolvedCity = inferred.city || text(city) || null;
  const stamp = text(observedAt) || new Date().toISOString().slice(0, 10);
  const source = permalink || null;
  const cacheKey = `${normalizeEntityName(name)}|${text(resolvedCity)}`;
  if (cache && cache.has(cacheKey)) return cache.get(cacheKey);

  const limitations = [];
  const telemetry = [];
  let lookup = emptyLookup({
    city: resolvedCity,
    citySource: inferred.source,
  });

  let operatingBusiness = provenance({
    value: name,
    field: 'operating_business',
    source,
    evidence: `Named as the entity owner or operating business on a public ${resolvedCity || 'permit'} record.`,
    observedAt: stamp,
    limitations: 'The permit names an entity. It does not prove current operations, ownership percentage, or financing intent.',
  });

  let decisionMakerRole = null;
  let phone = provenance({
    value: normalizePhone(sourceRow.owner_phone || sourceRow.owner_s_phone),
    field: 'phone',
    source,
    evidence: 'Phone number published on the NYC DOB NOW filing.',
    observedAt: stamp,
    limitations: 'Public filing phone. Not verified as a current direct line for a decision maker.',
  });
  let email = provenance({
    value: normalizeEmail(sourceRow.owner_email || sourceRow.owner_s_email),
    field: 'email',
    source,
    evidence: 'Email address published on the permit filing.',
    observedAt: stamp,
    limitations: 'Public filing email. Not verified as monitored or authorized for contact.',
  });
  let address = null;

  const chicagoPermitOwner = /chicago/i.test(text(resolvedCity)) ? chicagoOwnerFromPermit(sourceRow) : null;
  if (chicagoPermitOwner) {
    if (chicagoPermitOwner.phone && !phone) {
      phone = provenance({
        value: chicagoPermitOwner.phone,
        field: 'phone',
        source,
        evidence: `Owner contact phone on Chicago building permit contact_${chicagoPermitOwner.index}.`,
        observedAt: stamp,
        limitations: 'Permit contact phone. Not verified as a current business line.',
      });
    }
    if (chicagoPermitOwner.email && !email) {
      email = provenance({
        value: chicagoPermitOwner.email,
        field: 'email',
        source,
        evidence: `Owner contact email on Chicago building permit contact_${chicagoPermitOwner.index}.`,
        observedAt: stamp,
        limitations: 'Permit contact email. Not verified as monitored.',
      });
    }
    const mailing = [chicagoPermitOwner.city, chicagoPermitOwner.state, chicagoPermitOwner.zip]
      .filter(Boolean).join(', ');
    if (mailing) {
      limitations.push(`Permit owner mailing location (${mailing}) is not a registered business address and is not treated as a verifiable contact path.`);
    }
    if (!decisionMakerRole && chicagoPermitOwner.type) {
      decisionMakerRole = provenance({
        value: chicagoPermitOwner.type,
        field: 'decision_maker_role',
        source,
        evidence: `Chicago permit lists ${chicagoPermitOwner.name} with contact type ${chicagoPermitOwner.type}.`,
        observedAt: stamp,
        limitations: 'Permit contact type is a filing role, not a verified officer title or ownership percentage.',
      });
    }
  }

  if (/chicago/i.test(text(resolvedCity))) {
    const chicago = await lookupChicago(fetchImpl, name, { wait, telemetry });
    const summary = summarizeLookup(chicago.telemetry);
    lookup = {
      city: resolvedCity,
      citySource: inferred.source,
      requests: chicago.requests,
      timeouts: summary.timeouts,
      retries: summary.retries,
      ownersStatus: chicago.ownersStatus,
      licensesStatus: chicago.licensesStatus,
      ownersExactMatch: false,
      licensesExactMatch: false,
      partial: chicago.ownersStatus !== chicago.licensesStatus
        && [chicago.ownersStatus, chicago.licensesStatus].some(status => status === 'ok' || status === 'empty')
        && [chicago.ownersStatus, chicago.licensesStatus].some(status => ['timeout', 'http_error', 'error', 'skipped_budget'].includes(status)),
      failures: summary.failures,
    };
    const maker = pickDecisionMaker(chicago.owners, name);
    lookup.ownersExactMatch = Boolean(maker);
    if (maker) {
      decisionMakerRole = provenance({
        value: [maker.title, maker.name].filter(Boolean).join(' — ') || maker.title,
        field: 'decision_maker_role',
        source: CHICAGO_OWNERS_URL,
        evidence: `Chicago Business Owners filing lists ${[maker.name, maker.title].filter(Boolean).join(', ') || 'an owner'} for ${name}.`,
        observedAt: stamp,
        limitations: 'Public owner-title filing. Names are copied only as filed. No ownership percentage or contact path is assumed.',
      });
    }
    const license = pickLicense(chicago.licenses, name);
    lookup.licensesExactMatch = Boolean(license);
    if (license) {
      const registered = licenseAddress(license);
      if (registered) {
        address = provenance({
          value: registered,
          field: 'registered_business_address',
          source: CHICAGO_LICENSES_URL,
          evidence: `Chicago Business License ${text(license.license_id || license.license_number) || 'record'} lists this address for ${text(license.doing_business_as_name || license.legal_name || name)}.`,
          observedAt: stamp,
          limitations: 'Registered license address. Not a verified mailing preference or permission to contact.',
        });
      }
      const licensePhone = normalizePhone(license.phone || license.business_phone);
      if (licensePhone && !phone) {
        phone = provenance({
          value: licensePhone,
          field: 'phone',
          source: CHICAGO_LICENSES_URL,
          evidence: `Phone published on the Chicago Business License for ${name}.`,
          observedAt: stamp,
          limitations: 'License phone. Not verified as a current direct line.',
        });
      }
      const licenseEmail = normalizeEmail(license.email || license.business_email);
      if (licenseEmail && !email) {
        email = provenance({
          value: licenseEmail,
          field: 'email',
          source: CHICAGO_LICENSES_URL,
          evidence: `Email published on the Chicago Business License for ${name}.`,
          observedAt: stamp,
          limitations: 'License email. Not verified as monitored.',
        });
      }
      operatingBusiness = provenance({
        value: text(license.doing_business_as_name || license.legal_name || name),
        field: 'operating_business',
        source: CHICAGO_LICENSES_URL,
        evidence: `Matching Chicago Business License ${text(license.license_status) || 'record'} for ${name}.`,
        observedAt: stamp,
        limitations: 'License match is exact legal or DBA name only. Inactive or unrelated entities are not inferred.',
      });
    }
    if (summary.timeouts) {
      limitations.push(`Chicago SODA contact lookup timed out ${summary.timeouts} time(s); unmatched contact fields were left empty.`);
    }
    if (lookup.partial) {
      limitations.push('Chicago SODA contact lookup returned a partial result; only exact-matched fields from successful datasets were kept.');
    }
    if (!maker && !license) {
      limitations.push('No Chicago Business Owners or Business License row matched this exact legal or DBA name.');
    }
  }

  if (!phone && !email && !address) {
    limitations.push('No verifiable public business phone, email, or registered license address was found. Missing values were left empty.');
  }

  const result = Object.freeze({
    operatingBusiness,
    decisionMakerRole,
    phone,
    email,
    address,
    verifiable: Boolean(phone || email || address),
    fabricated: false,
    paidProvidersUsed: false,
    limitations: Object.freeze(limitations),
    lookup: Object.freeze(lookup),
  });
  if (cache) cache.set(cacheKey, result);
  return result;
}

async function measureChicagoExactMatchCoverage({
  names = [],
  fetch: fetchImpl = globalThis.fetch,
  wait,
} = {}) {
  const unique = [...new Set(names.map(name => normalizeEntityName(name)).filter(Boolean))];
  const results = [];
  for (const name of unique) {
    const telemetry = [];
    const chicago = await lookupChicago(fetchImpl, name, { wait, telemetry });
    const summary = summarizeLookup(chicago.telemetry);
    results.push({
      ownersExactMatch: Boolean(pickDecisionMaker(chicago.owners, name)),
      licensesExactMatch: Boolean(pickLicense(chicago.licenses, name)),
      ownersStatus: chicago.ownersStatus,
      licensesStatus: chicago.licensesStatus,
      requests: chicago.requests,
      timeouts: summary.timeouts,
      retries: summary.retries,
    });
  }
  const ownersMatches = results.filter(row => row.ownersExactMatch).length;
  const licensesMatches = results.filter(row => row.licensesExactMatch).length;
  const either = results.filter(row => row.ownersExactMatch || row.licensesExactMatch).length;
  const both = results.filter(row => row.ownersExactMatch && row.licensesExactMatch).length;
  return {
    queried: unique.length,
    ownersExactMatches: ownersMatches,
    licensesExactMatches: licensesMatches,
    eitherExactMatch: either,
    bothExactMatch: both,
    ownersMatchRate: unique.length ? ownersMatches / unique.length : 0,
    licensesMatchRate: unique.length ? licensesMatches / unique.length : 0,
    eitherMatchRate: unique.length ? either / unique.length : 0,
    timeouts: results.reduce((sum, row) => sum + row.timeouts, 0),
    requests: results.reduce((sum, row) => sum + row.requests, 0),
    retries: results.reduce((sum, row) => sum + row.retries, 0),
    fabricatedContacts: 0,
    results,
  };
}

module.exports = {
  USER_AGENT,
  CHICAGO_OWNERS_URL,
  CHICAGO_LICENSES_URL,
  isPaidEnrichmentUrl: soda.isPaidEnrichmentUrl,
  namesMatch,
  researchContact,
  lookupChicago,
  measureChicagoExactMatchCoverage,
};
