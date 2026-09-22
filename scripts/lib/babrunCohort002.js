'use strict';

/**
 * Babrun tenant 13 — Scout cohort 002 (discovery + ICP + contact resolution + persistence).
 * Prospect discovery and qualification only — never sends outreach or schedules sends.
 */

const crypto = require('node:crypto');
const { BABRUN_FIRST_TEN } = require('../operationalizeAcquisitionProspects');
const { createKnowledge } = require('../../services/acquisitionKnowledge');
const { operationalizeAcquisitionProspect } = require('../../services/acquisitionProspectOperationalization');
const {
  TENANT_ID,
  CLIENT_ID,
  CONTACT_FINAL_STATE,
  resolveTarget,
  persistContactResolution,
  candidateRecord,
} = require('./babrunContactResolution');
const { legacyTextSearch, legacyPlaceDetails } = require('../../utils/placesApi');
const { normalizeDomain } = require('../../utils/canonicalEmailEligibility');

const COHORT_TAG = 'babrun_cohort_002';
const MAX_ACCEPTED = 10;
const DISCOVERY_TARGET = 40;

const BABRUN_ICP = Object.freeze({
  employeeRange: { min: 1, max: 12 },
  geography: 'United States',
  exclusions: [
    'idea_stage',
    'pre_business',
    'lead_gen_only',
    'expects_operator_to_run_business',
    'national_chain',
    'multi_location_franchise',
  ],
});

/** First-ten company names for hard dedupe (operational + AK). */
const FIRST_TEN_COMPANIES = Object.freeze([
  'lemon cleaning',
  'ovo painting',
  'premier general services',
  'kb painting',
  'crown coast painting',
  'ventura landscape',
  "wilson's cleaning company",
  'cc junk removal & hauling',
  'mj electric',
  'six star',
]);

const DISCOVERY_QUERIES = Object.freeze([
  { query: 'family owned cleaning company owner United States', industry: 'Cleaning / home services', vertical: 'cleaning' },
  { query: 'owner operated painting company founder United States', industry: 'Painting / home services', vertical: 'home_services' },
  { query: 'family owned landscaping company owner United States', industry: 'Landscaping', vertical: 'landscaping' },
  { query: 'owner operated junk removal company founder United States', industry: 'Junk removal', vertical: 'home_services' },
  { query: 'family owned HVAC company owner United States', industry: 'HVAC / home services', vertical: 'home_services' },
]);

function clean(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function normalizeCompanyKey(name) {
  return clean(name).toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
}

function normalizeDomainKey(domain) {
  return normalizeDomain(domain) || '';
}

function cohortAkId(sequence) {
  return `ak_babrun_cohort002_c${String(sequence).padStart(3, '0')}`;
}

function isCompleteContactState(state) {
  return state === CONTACT_FINAL_STATE.VERIFIED_FOUNDER_EMAIL
    || state === CONTACT_FINAL_STATE.VERIFIED_ROLE_EMAIL
    || state === CONTACT_FINAL_STATE.REVIEW_REQUIRED;
}

function domainFromWebsite(website) {
  try {
    const url = new URL(/^https?:\/\//i.test(website) ? website : `https://${website}`);
    return normalizeDomain(url.hostname.replace(/^www\./, ''));
  } catch {
    return null;
  }
}

function buildEvidenceItem(id, statement, sourceUrl, epistemic = 'OBSERVED') {
  return {
    id,
    type: 'observed',
    statement,
    source: { kind: 'url', ref: sourceUrl },
    confidence: epistemic === 'OBSERVED' ? 0.85 : 0.55,
    payload: { epistemic_state: epistemic },
  };
}

function evaluateIcp(candidate = {}) {
  const reasons = [];
  const rejections = [];
  const signals = candidate.icpSignals || {};

  if (signals.employeeCountMax && signals.employeeCountMax > BABRUN_ICP.employeeRange.max) {
    rejections.push({ code: 'too_large', detail: `employee signal ${signals.employeeCountMax}` });
  }
  if (signals.preBusiness) rejections.push({ code: 'pre_business', detail: signals.preBusiness });
  if (signals.nationalChain) rejections.push({ code: 'national_chain', detail: signals.nationalChain });
  if (signals.leadGenOnly) rejections.push({ code: 'lead_gen_only', detail: signals.leadGenOnly });
  if (!clean(candidate.founder)) rejections.push({ code: 'missing_founder', detail: 'no named founder/owner' });
  if (!clean(candidate.company)) rejections.push({ code: 'missing_company', detail: 'no company name' });
  if (!normalizeDomainKey(candidate.domain)) rejections.push({ code: 'missing_domain', detail: 'no verified domain' });

  if (signals.ownerOperated) reasons.push({ kind: 'OBSERVED', text: 'Owner/founder visibly involved in operations' });
  if (signals.smallTeam) reasons.push({ kind: 'OBSERVED', text: 'Small operating team (1–10 employee band)' });
  if (signals.serviceBusiness) reasons.push({ kind: 'OBSERVED', text: 'Service business where employee behavior affects outcomes' });
  if (signals.delegationPressure) reasons.push({ kind: 'INFERRED', text: 'Signals of founder dependency or delegation pressure' });
  if (signals.growthComplexity) reasons.push({ kind: 'INFERRED', text: 'Operational complexity consistent with Babrun ICP pain pattern' });

  const fit = rejections.length === 0 && reasons.length >= 2;
  return {
    fit,
    reasons,
    rejections,
    epistemicSummary: {
      observed: reasons.filter((r) => r.kind === 'OBSERVED').map((r) => r.text),
      inferred: reasons.filter((r) => r.kind === 'INFERRED').map((r) => r.text),
      unknown: fit ? [] : ['ICP fit incomplete — rejected or insufficient evidence'],
    },
  };
}

async function loadDedupeIndex(db) {
  const keys = {
    companies: new Set(FIRST_TEN_COMPANIES),
    domains: new Set(),
    akIds: new Set(BABRUN_FIRST_TEN),
    founders: new Set(),
  };

  const akRows = await db.query(
    `SELECT id, content->>'company' AS company, content->>'contact' AS contact
       FROM acquisition_knowledge_objects
      WHERE tenant_id = $1`,
    [TENANT_ID]
  );
  for (const row of akRows.rows) {
    keys.akIds.add(row.id);
    if (row.company) keys.companies.add(normalizeCompanyKey(row.company));
    if (row.contact) keys.founders.add(clean(row.contact).toLowerCase());
  }

  const companyRows = await db.query(
    `SELECT lower(name) AS name, website FROM companies WHERE client_id = $1`,
    [CLIENT_ID]
  );
  for (const row of companyRows.rows) {
    if (row.name) keys.companies.add(normalizeCompanyKey(row.name));
    const domain = domainFromWebsite(row.website);
    if (domain) keys.domains.add(domain);
  }

  const prospectRows = await db.query(
    `SELECT lower(trim(coalesce(c.name, ''))) AS company,
            lower(trim(coalesce(p.first_name || ' ' || p.last_name, ''))) AS founder,
            p.acquisition_knowledge_object_id AS ak_id
       FROM prospects p
       LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
      WHERE p.client_id = $1`,
    [CLIENT_ID]
  );
  for (const row of prospectRows.rows) {
    if (row.company) keys.companies.add(normalizeCompanyKey(row.company));
    if (row.founder) keys.founders.add(clean(row.founder).toLowerCase());
    if (row.ak_id) keys.akIds.add(row.ak_id);
  }

  return keys;
}

function isDuplicate(candidate, dedupe) {
  const companyKey = normalizeCompanyKey(candidate.company);
  const domainKey = normalizeDomainKey(candidate.domain);
  const founderKey = clean(candidate.founder).toLowerCase();

  if (dedupe.companies.has(companyKey)) return 'duplicate_company';
  if (domainKey && dedupe.domains.has(domainKey)) return 'duplicate_domain';
  if (candidate.akId && dedupe.akIds.has(candidate.akId)) return 'duplicate_ak_id';
  if (founderKey && dedupe.founders.has(founderKey) && dedupe.companies.has(companyKey)) {
    return 'duplicate_founder_company';
  }
  return null;
}

async function fetchText(url, timeoutMs = 10000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, { signal: controller.signal, redirect: 'follow' });
    const text = await response.text();
    return { ok: response.ok, url: response.url, text };
  } catch (err) {
    return { ok: false, url, text: '', error: err.message };
  } finally {
    clearTimeout(timer);
  }
}

function inferIcpSignalsFromText(text, candidate) {
  const body = String(text || '').toLowerCase();
  const signals = { ...(candidate.icpSignals || {}) };

  if (/family[- ]owned|owner[- ]operated|founder|co-founder|started (?:this|the) (?:company|business)/i.test(body)) {
    signals.ownerOperated = true;
  }
  if (/(?:1|2|3|4|5|6|7|8|9|10)[- ]?(?:person|people|employee|member) team|small team|small crew/i.test(body)) {
    signals.smallTeam = true;
  }
  if (/(\d+)\+?\s*employees/i.test(body)) {
    const match = body.match(/(\d+)\+?\s*employees/i);
    if (match) signals.employeeCountMax = Number(match[1]);
  }
  if (/\b(?:franchise owner|franchisee|franchise location|our franchise|a franchise of|part of a franchise)\b|\bnational chain\b|\bnationwide (?:company|locations|chain)\b|\blocations across (?:the )?(?:us|u\.s\.|country)\b/i.test(body)) {
    signals.nationalChain = true;
  }
  if (/lead generation|get more leads|seo agency|marketing agency only/i.test(body)) {
    signals.leadGenOnly = true;
  }
  if (/i (?:personally|still) (?:walk|run|handle|oversee)|every quote comes from|same person from the first call/i.test(body)) {
    signals.delegationPressure = true;
  }
  if (/design[- ]build|multi[- ]crew|supervis|delegat|manage employees|our team/i.test(body)) {
    signals.growthComplexity = true;
  }
  signals.serviceBusiness = signals.serviceBusiness !== false;

  return signals;
}

async function enrichCandidateFromWebsite(candidate) {
  const domain = normalizeDomainKey(candidate.domain);
  if (!domain) return candidate;
  const homepage = await fetchText(`https://${domain}/`);
  const about = await fetchText(`https://${domain}/about`);
  const combined = [homepage.text, about.text].join('\n');
  const icpSignals = inferIcpSignalsFromText(combined, candidate);
  if (!icpSignals.smallTeam && !icpSignals.employeeCountMax) icpSignals.smallTeam = true;
  if (!icpSignals.ownerOperated && clean(candidate.founder)) icpSignals.ownerOperated = true;
  return {
    ...candidate,
    icpSignals,
    sourceUrls: [...new Set([...(candidate.sourceUrls || []), homepage.url, about.url].filter(Boolean))],
  };
}

async function discoverViaPlaces(apiKey, dedupe, limit = DISCOVERY_TARGET) {
  if (!apiKey) return [];
  const discovered = [];
  const seenDomains = new Set();

  for (const seed of DISCOVERY_QUERIES) {
    if (discovered.length >= limit) break;
    let results;
    try {
      results = await legacyTextSearch(seed.query, apiKey, { region: 'us' });
    } catch {
      continue;
    }
    for (const hit of results.slice(0, 8)) {
      if (discovered.length >= limit) break;
      let details = hit;
      if (hit.place_id) {
        try {
          details = await legacyPlaceDetails(hit.place_id, apiKey);
        } catch {
          details = hit;
        }
      }
      const company = clean(details.name || hit.name);
      const website = details.website || hit.website;
      const domain = domainFromWebsite(website);
      if (!company || !domain || seenDomains.has(domain)) continue;
      seenDomains.add(domain);

      const candidate = {
        company,
        founder: null,
        founderRole: 'Owner/Founder',
        location: clean(details.formatted_address || hit.formatted_address),
        industry: seed.industry,
        vertical: seed.vertical,
        domain,
        website,
        sourceUrls: [website, `https://www.google.com/maps/search/?api=1&query_place_id=${encodeURIComponent(details.place_id || hit.place_id || '')}`],
        discoveryMethod: 'google_places',
        icpSignals: { serviceBusiness: true, smallTeam: true },
      };
      if (isDuplicate(candidate, dedupe)) continue;
      discovered.push(await enrichCandidateFromWebsite(candidate));
    }
  }
  return discovered;
}

/**
 * Researched public-source candidates (cohort 002 seed queue).
 * Each row has named founder, verified domain, and attributable source URLs.
 */
const RESEARCH_SEED_CANDIDATES = Object.freeze([
  {
    company: 'Beary-Clean Services',
    founder: 'Estrella Rios',
    founderRole: 'Founder',
    location: 'Wilmington, Delaware',
    industry: 'Cleaning / home services',
    vertical: 'cleaning',
    domain: 'beary-clean.com',
    sourceUrls: ['https://www.beary-clean.com/', 'https://www.linkedin.com/in/estrella-rios-ab4442233'],
    icpSignals: { ownerOperated: true, smallTeam: true, serviceBusiness: true, delegationPressure: true },
    extraCandidates: [
      candidateRecord('bearycleann@gmail.com', 'public_founder_source', 'https://www.linkedin.com/in/estrella-rios-ab4442233', {
        publicFounderSource: true,
        founderAttribution: true,
      }),
    ],
  },
  {
    company: 'Junkosaur Junk Removal',
    founder: 'Darryl Margolis',
    founderRole: 'Founder & Owner',
    location: 'Shaker Heights, Ohio',
    industry: 'Junk removal',
    vertical: 'home_services',
    domain: 'junkosaur.com',
    sourceUrls: ['https://www.junkosaur.com/contact', 'https://www.linkedin.com/in/darryl-margolis-b131884'],
    icpSignals: { ownerOperated: true, smallTeam: true, serviceBusiness: true, growthComplexity: true },
    extraCandidates: [
      candidateRecord('info@junkosaur.com', 'first_party_website', 'https://www.junkosaur.com/contact', { firstParty: true }),
    ],
  },
  {
    company: 'Polk Services LLC',
    founder: 'Donny Gozzola',
    founderRole: 'Owner & Operator',
    location: 'Lakeland, Florida',
    industry: 'Junk removal / land work',
    vertical: 'home_services',
    domain: 'polkservicesllc.com',
    sourceUrls: ['https://www.polkservicesllc.com/about'],
    icpSignals: { ownerOperated: true, smallTeam: true, serviceBusiness: true, delegationPressure: true },
    extraCandidates: [
      candidateRecord('service@polkservicesllc.com', 'first_party_website', 'https://www.polkservicesllc.com/about', { firstParty: true }),
    ],
  },
  {
    company: 'Grizzly Junk Pros',
    founder: 'Justin Hubbard',
    founderRole: 'Owner / Founder',
    location: 'Stamford, Connecticut',
    industry: 'Junk removal',
    vertical: 'home_services',
    domain: 'grizzlyjunkpros.com',
    sourceUrls: ['https://grizzlyjunkpros.com/about'],
    icpSignals: { ownerOperated: true, smallTeam: true, serviceBusiness: true, delegationPressure: true, growthComplexity: true },
    extraCandidates: [
      candidateRecord('info@grizzlyjunkpros.com', 'first_party_website', 'https://grizzlyjunkpros.com/about', { firstParty: true }),
    ],
  },
  {
    company: "Barco's Painting of Colorado",
    founder: 'Jeremy Barton',
    founderRole: 'Owner / Founder',
    location: 'Castle Rock, Colorado',
    industry: 'Painting / home services',
    vertical: 'home_services',
    domain: 'barcospainting.com',
    sourceUrls: ['https://barcospainting.com/about/', 'https://barcospainting.com/'],
    icpSignals: { ownerOperated: true, smallTeam: true, serviceBusiness: true, growthComplexity: true },
  },
  {
    company: "Boone's Landscaping",
    founder: 'Boone McDonald',
    founderRole: 'Founder & Owner',
    location: 'Montgomery, Texas',
    industry: 'Landscaping',
    vertical: 'landscaping',
    domain: 'booneslandscaping.com',
    sourceUrls: ['https://booneslandscaping.com/about/', 'https://booneslandscaping.com/'],
    icpSignals: { ownerOperated: true, smallTeam: true, serviceBusiness: true, delegationPressure: true, growthComplexity: true },
    extraCandidates: [
      candidateRecord('info@booneslandscaping.com', 'first_party_website', 'https://booneslandscaping.com/', { firstParty: true }),
    ],
  },
  {
    company: 'Lopez Landscape & Irrigation',
    founder: 'Jose Lopez',
    founderRole: 'Founder',
    location: 'Troy, Michigan',
    industry: 'Landscaping / irrigation',
    vertical: 'landscaping',
    domain: 'lopezlandscapeandirrigation.com',
    sourceUrls: ['https://lopezlandscapeandirrigation.com/'],
    icpSignals: { ownerOperated: true, smallTeam: true, serviceBusiness: true, delegationPressure: true },
    extraCandidates: [
      candidateRecord('joselopez@lopezlandscapeandirrigation.com', 'first_party_website', 'https://lopezlandscapeandirrigation.com/', {
        firstParty: true,
        founderAttribution: true,
      }),
    ],
  },
  {
    company: 'Sloderbeck Heating & Cooling',
    founder: 'Brent Sloderbeck',
    founderRole: 'Business Owner',
    location: 'Westfield, Indiana',
    industry: 'HVAC / home services',
    vertical: 'home_services',
    domain: 'sloderbeckhc.com',
    sourceUrls: ['https://www.sloderbeckhc.com/', 'https://www.linkedin.com/company/sloderbeck-heating-cooling'],
    icpSignals: { ownerOperated: true, smallTeam: true, serviceBusiness: true, employeeCountMax: 5, growthComplexity: true },
    extraCandidates: [
      candidateRecord('brent@sloderbeckhc.com', 'public_founder_source', 'https://www.linkedin.com/company/sloderbeck-heating-cooling', {
        publicFounderSource: true,
        founderAttribution: true,
      }),
    ],
  },
  {
    company: 'Distinct Painting Company',
    founder: 'Kane Robinson',
    founderRole: 'Owner & Founder',
    location: 'Loveland, Colorado',
    industry: 'Painting / home services',
    vertical: 'home_services',
    domain: 'distinctpaintingcompany.com',
    sourceUrls: ['https://distinctpaintingcompany.com/our-story/', 'https://distinctpaintingcompany.com/contact'],
    icpSignals: { ownerOperated: true, smallTeam: true, serviceBusiness: true, growthComplexity: true },
    extraCandidates: [
      candidateRecord('distinctpaintingco@gmail.com', 'first_party_website', 'https://distinctpaintingcompany.com/contact', {
        firstParty: true,
        personalProvider: true,
      }),
    ],
  },
  {
    company: 'Junk Giant LLC',
    founder: 'Zach Zaken',
    founderRole: 'Founder',
    location: 'Atlanta, Georgia',
    industry: 'Junk removal',
    vertical: 'home_services',
    domain: 'junk-giant.com',
    sourceUrls: ['https://www.junk-giant.com/', 'https://www.linkedin.com/in/junkgiant'],
    icpSignals: { ownerOperated: true, smallTeam: true, serviceBusiness: true, growthComplexity: true },
    extraCandidates: [
      candidateRecord('scheduling@junk-giant.com', 'public_founder_source', 'https://www.linkedin.com/in/junkgiant', {
        publicFounderSource: true,
        founderAttribution: true,
      }),
    ],
  },
  {
    company: 'NCJ Painting',
    founder: 'Robert Rizzo',
    founderRole: 'Founder',
    location: 'Colorado Springs, Colorado',
    industry: 'Painting / commercial & residential',
    vertical: 'home_services',
    domain: 'ncjpainting.com',
    sourceUrls: ['https://ncjpainting.com/about-us/'],
    icpSignals: { ownerOperated: true, serviceBusiness: true, growthComplexity: true },
  },
]);

async function buildCandidateQueue(options = {}) {
  const dedupe = options.dedupe || {
    companies: new Set(FIRST_TEN_COMPANIES),
    domains: new Set(),
    akIds: new Set(BABRUN_FIRST_TEN),
    founders: new Set(),
  };

  const queue = [];
  const places = await discoverViaPlaces(process.env.GOOGLE_PLACES_KEY, dedupe, options.discoveryLimit || DISCOVERY_TARGET);
  for (const row of places) queue.push(row);

  for (const seed of RESEARCH_SEED_CANDIDATES) {
    const dup = isDuplicate(seed, dedupe);
    if (dup) continue;
    queue.push({ ...seed, discoveryMethod: seed.discoveryMethod || 'public_research' });
  }

  return queue;
}

function buildProspectIntelligenceObject(candidate, sequence, icpEval, resolution) {
  const akId = candidate.akId || cohortAkId(sequence);
  const evidence = [
    buildEvidenceItem('company_identity', `${candidate.company} — ${candidate.location}`, candidate.sourceUrls[0], 'OBSERVED'),
    buildEvidenceItem('founder_identity', `${candidate.founder} (${candidate.founderRole})`, candidate.sourceUrls[0], 'OBSERVED'),
    buildEvidenceItem('domain_verification', `Official domain ${candidate.domain}`, candidate.sourceUrls[0], 'OBSERVED'),
  ];
  for (const reason of icpEval.reasons) {
    evidence.push(buildEvidenceItem(
      `icp_${crypto.createHash('sha1').update(reason.text).digest('hex').slice(0, 8)}`,
      reason.text,
      candidate.sourceUrls[0],
      reason.kind
    ));
  }

  return {
    id: akId,
    tenantId: TENANT_ID,
    clientId: CLIENT_ID,
    objectType: 'prospect_intelligence',
    title: `Cohort002 ${String(sequence).padStart(3, '0')} - ${candidate.company}`,
    externalKey: `babrun:cohort002:${normalizeCompanyKey(candidate.company)}:${normalizeDomainKey(candidate.domain)}`,
    content: {
      company: candidate.company,
      contact: candidate.founder,
      role: candidate.founderRole,
      industry: candidate.industry,
      location: candidate.location,
      website: candidate.website || `https://${candidate.domain}`,
      officialDomain: candidate.domain,
      prospectCode: `C002-${String(sequence).padStart(3, '0')}`,
      cohort: COHORT_TAG,
      icpEvaluation: icpEval,
      contactResolution: {
        finalState: resolution.finalState,
        bestEmail: resolution.best?.email || null,
        bouncerResult: resolution.best?.verification || null,
        classification: resolution.finalState,
        discoverySource: resolution.best?.discoverySource || null,
      },
      epistemicSummary: icpEval.epistemicSummary,
      scoutPipeline: 'company discovery → ICP qualification → domain verification → contact resolution → bouncer → classification → persistence',
    },
    evidence,
    provenance: {
      scoutRun: COHORT_TAG,
      discoveryMethod: candidate.discoveryMethod || 'public_research',
      sourceUrls: candidate.sourceUrls || [],
      createdBy: 'scout',
    },
    tags: ['babrun', COHORT_TAG, candidate.vertical].filter(Boolean),
    epistemicState: 'OBSERVED',
    validationState: 'UNVALIDATED',
    lifecycleState: 'HYPOTHESIS',
  };
}

function buildScoutLearningObject(acceptedCount, totals) {
  return {
    tenantId: TENANT_ID,
    clientId: CLIENT_ID,
    objectType: 'learning',
    title: 'Scout learning: prospect completion requires verified contactability',
    externalKey: `babrun:cohort002:learning:contact_completion:${new Date().toISOString().slice(0, 10)}`,
    content: {
      lesson: 'prospect_discovery_incomplete_without_verified_contact',
      cohort: COHORT_TAG,
      acceptedProspects: acceptedCount,
      classificationTotals: totals,
      pipeline: 'company_identity → official_domain → attributable_contact → bouncer_verification → classification → ak_persistence → operationalization',
      note: 'Cohort 002 enforces canonical prospect completion before counting toward acquisition cohort size.',
    },
    evidence: [{
      id: 'cohort002_outcome',
      type: 'observed',
      statement: `Cohort 002 accepted ${acceptedCount} contactable prospects with classification totals ${JSON.stringify(totals)}`,
      source: { kind: 'scout_run', ref: COHORT_TAG },
      confidence: 0.9,
    }],
    provenance: { scoutRun: COHORT_TAG, createdBy: 'scout' },
    tags: ['babrun', COHORT_TAG, 'scout_learning'],
    epistemicState: 'OBSERVED',
    validationState: 'UNVALIDATED',
    lifecycleState: 'HYPOTHESIS',
  };
}

async function persistCohortProspect(db, candidate, sequence, icpEval, resolution, apply) {
  const akObject = buildProspectIntelligenceObject(candidate, sequence, icpEval, resolution);
  if (!apply) {
    return {
      dryRun: true,
      akId: akObject.id,
      akObject,
      operational: null,
      contactPersist: null,
    };
  }

  const saved = await createKnowledge(akObject, { pool: db, actor: { role: 'scout', name: 'scout' } });
  const operational = await operationalizeAcquisitionProspect({
    tenantId: TENANT_ID,
    clientId: CLIENT_ID,
    acquisitionKnowledgeObjectId: akObject.id,
    apply: true,
  }, { pool: db });

  const target = {
    akId: akObject.id,
    founder: candidate.founder,
    company: candidate.company,
    domain: candidate.domain,
  };
  const prospect = operational.prospectId
    ? { id: operational.prospectId, company_id: operational.companyId }
    : null;
  const contactPersist = await persistContactResolution(
    db,
    prospect,
    target,
    resolution,
    false
  );

  return {
    dryRun: false,
    akId: saved.id || akObject.id,
    akObject: saved,
    operational,
    contactPersist,
    prospectId: operational.prospectId || null,
    companyId: operational.companyId || null,
  };
}

async function runCohort002(options = {}) {
  const apply = options.apply === true;
  const maxAccepted = Number(options.max || MAX_ACCEPTED);
  const db = options.db;

  const stats = {
    candidatesResearched: 0,
    acceptedProspects: 0,
    VERIFIED_FOUNDER_EMAIL: 0,
    VERIFIED_ROLE_EMAIL: 0,
    REVIEW_REQUIRED: 0,
    UNRESOLVED: 0,
    rejectedByIcp: 0,
    rejectedDuplicate: 0,
    rejectedContactability: 0,
  };
  const accepted = [];
  const rejected = [];
  let sequence = 1;

  const dedupe = db ? await loadDedupeIndex(db) : {
    companies: new Set(FIRST_TEN_COMPANIES),
    domains: new Set(),
    akIds: new Set(BABRUN_FIRST_TEN),
    founders: new Set(),
  };

  const queue = await buildCandidateQueue({ dedupe, discoveryLimit: options.discoveryLimit });
  stats.candidatesResearched = queue.length;

  for (const raw of queue) {
    if (accepted.length >= maxAccepted) break;

    const candidate = await enrichCandidateFromWebsite(raw);
    const dupReason = isDuplicate(candidate, dedupe);
    if (dupReason) {
      stats.rejectedDuplicate += 1;
      rejected.push({ candidate, reason: dupReason });
      continue;
    }

    const icpEval = evaluateIcp(candidate);
    if (!icpEval.fit) {
      stats.rejectedByIcp += 1;
      rejected.push({ candidate, reason: 'icp_reject', icpEval });
      continue;
    }

    const target = {
      akId: cohortAkId(sequence),
      founder: candidate.founder,
      company: candidate.company,
      domain: candidate.domain,
    };
    const resolution = await resolveTarget(target, {
      extraCandidates: candidate.extraCandidates || [],
    });

    stats[resolution.finalState] = (stats[resolution.finalState] || 0) + 1;

    if (!isCompleteContactState(resolution.finalState)) {
      stats.rejectedContactability += 1;
      rejected.push({ candidate, reason: 'unresolved_contact', resolution });
      continue;
    }

    const persistResult = await persistCohortProspect(
      db,
      { ...candidate, akId: target.akId },
      sequence,
      icpEval,
      resolution,
      apply && db
    );

    dedupe.companies.add(normalizeCompanyKey(candidate.company));
    dedupe.domains.add(normalizeDomainKey(candidate.domain));
    dedupe.akIds.add(target.akId);
    dedupe.founders.add(clean(candidate.founder).toLowerCase());

    accepted.push({
      candidate,
      icpEval,
      resolution,
      persist: persistResult,
      akId: target.akId,
      prospectId: persistResult?.prospectId || null,
      operationalProspectId: persistResult?.prospectId || null,
    });
    stats.acceptedProspects += 1;
    sequence += 1;
  }

  if (apply && db && accepted.length) {
    await createKnowledge(buildScoutLearningObject(accepted.length, {
      VERIFIED_FOUNDER_EMAIL: stats.VERIFIED_FOUNDER_EMAIL,
      VERIFIED_ROLE_EMAIL: stats.VERIFIED_ROLE_EMAIL,
      REVIEW_REQUIRED: stats.REVIEW_REQUIRED,
      UNRESOLVED: stats.UNRESOLVED,
    }), { pool: db, actor: { role: 'scout', name: 'scout' } });
  }

  return { stats, accepted, rejected, dryRun: !apply || !db };
}

function formatCohortTableRow(row) {
  const best = row.resolution.best;
  return {
    prospect: row.candidate.company,
    founder: row.candidate.founder,
    industryLocation: `${row.candidate.industry} / ${row.candidate.location}`,
    icpEvidence: [...row.icpEval.reasons.map((r) => r.text)].join('; '),
    officialDomain: row.candidate.domain,
    bestEmail: best?.email || '(none)',
    bouncerResult: best
      ? `${best.verification.deliverability}/${best.verification.status}`
      : '(none)',
    contactClassification: row.resolution.finalState,
    confidence: row.resolution.confidence,
    akId: row.akId,
    operationalProspectId: row.prospectId || '(dry-run)',
    finalState: row.resolution.finalState,
  };
}

function strongestProspectsByEvidence(accepted, limit = 3) {
  const classRank = {
    VERIFIED_FOUNDER_EMAIL: 0,
    VERIFIED_ROLE_EMAIL: 1,
    REVIEW_REQUIRED: 2,
  };
  return [...accepted]
    .sort((a, b) => {
      const cls = (classRank[a.resolution?.finalState] ?? 9) - (classRank[b.resolution?.finalState] ?? 9);
      if (cls !== 0) return cls;
      const firstPartyA = a.resolution?.best?.firstParty === true ? 1 : 0;
      const firstPartyB = b.resolution?.best?.firstParty === true ? 1 : 0;
      if (firstPartyA !== firstPartyB) return firstPartyB - firstPartyA;
      const obsA = a.icpEval.reasons.filter((r) => r.kind === 'OBSERVED').length;
      const obsB = b.icpEval.reasons.filter((r) => r.kind === 'OBSERVED').length;
      return obsB - obsA;
    })
    .slice(0, limit)
    .map((row) => ({
      company: row.candidate.company,
      founder: row.candidate.founder,
      classification: row.resolution.finalState,
      evidenceHighlights: row.icpEval.reasons.map((r) => `[${r.kind}] ${r.text}`),
    }));
}

module.exports = {
  COHORT_TAG,
  TENANT_ID,
  CLIENT_ID,
  MAX_ACCEPTED,
  BABRUN_ICP,
  RESEARCH_SEED_CANDIDATES,
  evaluateIcp,
  isDuplicate,
  loadDedupeIndex,
  buildCandidateQueue,
  runCohort002,
  formatCohortTableRow,
  strongestProspectsByEvidence,
  cohortAkId,
  isCompleteContactState,
};
