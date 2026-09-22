'use strict';

/**
 * Babrun tenant 13 — first-ten contact resolution (CONTACT DISCOVERY + VERIFICATION ONLY).
 * Never sends mail. Never schedules outreach. Never modifies Emmett capacity.
 */

const { verifyEmail } = require('../../utils/emailVerifier');
const { isRolePattern } = require('../../utils/emailValidation');
const { classifyOutreachContactType, OUTREACH_CONTACT_TYPE } = require('../../utils/outreachContactType');
const {
  stampEmailProvenance,
  isPersonalEmailProviderDomain,
  normalizeDomain,
  resolveOfficialEnrichmentDomain,
} = require('../../utils/canonicalEmailEligibility');
const { persistableEmailSource } = require('../../utils/crmEmailProvenance');
const { invalidOutreachEmailReason } = require('../../utils/emailGuard');
const { safeIngestEnrichmentOutcome } = require('../../utils/maxSignalIngestion');
const { buildUrl } = require('../../utils/websiteEnrichmentCrawl');
const tiered = require('../../tieredEnrichmentAgent');

const TENANT_ID = '13';
const CLIENT_ID = 13;

const CONTACT_FINAL_STATE = Object.freeze({
  VERIFIED_FOUNDER_EMAIL: 'VERIFIED_FOUNDER_EMAIL',
  VERIFIED_ROLE_EMAIL: 'VERIFIED_ROLE_EMAIL',
  REVIEW_REQUIRED: 'REVIEW_REQUIRED',
  UNRESOLVED: 'UNRESOLVED',
});

/** Remaining seven — excludes Kaylee/KB (p012), Six Star (p024), MJ Electric (p025). */
const BABRUN_CONTACT_TARGETS = Object.freeze([
  {
    akId: 'ak_babrun_prospect_p001',
    founder: 'Max Walls',
    company: 'Lemon Cleaning',
    domain: 'lemonhomecleaning.com',
  },
  {
    akId: 'ak_babrun_prospect_p011',
    founder: 'Sebastian Thomas',
    company: 'OVO Painting',
    domain: 'ovopainting.com',
  },
  {
    akId: 'ak_babrun_prospect_p047',
    founder: 'Diego Louzada',
    company: 'Premier General Services',
    domain: 'premiergeneralservices.com',
  },
  {
    akId: 'ak_babrun_prospect_p022',
    founder: 'Hugo Rosales',
    company: 'Crown Coast Painting',
    domain: 'crowncoastpainting.com',
  },
  {
    akId: 'ak_babrun_prospect_p013',
    founder: 'Luis Ventura',
    company: 'Ventura Landscape',
    domain: 'venturalandscapehouston.com',
  },
  {
    akId: 'ak_babrun_prospect_p051',
    founder: 'Emmanual D. Wilson',
    company: "Wilson's Cleaning Company",
    domain: 'wilsoncleansllc.com',
  },
  {
    akId: 'ak_babrun_prospect_p003',
    founder: 'Sirewl Cooper',
    company: 'CC Junk Removal & Hauling',
    domain: 'cchaulsjunk.com',
  },
]);

const GENERIC_PREFIX_RE = /^(?:info|hello|contact|office|sales|support|admin|team)@/i;
const EMAIL_RE = /[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}/gi;
const NOISE_EMAIL_RE = /(?:example\.com|sentry|wixpress|\.png$|\.jpg$|\.webp$|@www\.|you@|jane@|name@email)/i;

function clean(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function founderFirstName(founder) {
  return clean(founder).split(/\s+/)[0]?.toLowerCase() || '';
}

function founderLastName(founder) {
  const parts = clean(founder).split(/\s+/).filter(Boolean);
  return parts.length > 1 ? parts[parts.length - 1].toLowerCase().replace(/[^a-z]/g, '') : '';
}

function localPart(email) {
  return clean(email).toLowerCase().split('@')[0] || '';
}

function emailDomain(email) {
  return clean(email).toLowerCase().split('@')[1] || '';
}

function isGenericRoleEmail(email) {
  return GENERIC_PREFIX_RE.test(clean(email).toLowerCase()) || isRolePattern(email);
}

function isFounderLocalPartMatch(email, founder) {
  const local = localPart(email).replace(/[^a-z]/g, '');
  const first = founderFirstName(founder).replace(/[^a-z]/g, '');
  const last = founderLastName(founder);
  if (!local || !first) return false;
  if (local === first) return true;
  if (last && (local === `${first}${last}` || local === `${first[0]}${last}`)) return true;
  return false;
}

function isNoiseEmail(email) {
  const normalized = clean(email).toLowerCase();
  if (!normalized || NOISE_EMAIL_RE.test(normalized)) return true;
  if (invalidOutreachEmailReason(normalized)) return true;
  return false;
}

function extractEmailsFromText(text, allowedDomains = []) {
  const found = new Set();
  const matches = String(text || '').match(EMAIL_RE) || [];
  for (const raw of matches) {
    const email = raw.toLowerCase();
    if (isNoiseEmail(email)) continue;
    if (!allowedDomains.length) {
      found.add(email);
      continue;
    }
    const domain = emailDomain(email);
    const allowed = allowedDomains.some((d) => domain === d || domain.endsWith(`.${d}`));
    if (allowed) found.add(email);
  }
  return [...found];
}

function relatedBrandDomains(primaryDomain) {
  const primary = normalizeDomain(primaryDomain);
  const related = new Set([primary].filter(Boolean));
  const aliases = {
    'lemonhomecleaning.com': ['thelemoncleaning.com'],
    'venturalandscapehouston.com': ['venturalawncare.com'],
  };
  for (const d of aliases[primary] || []) related.add(d);
  return [...related];
}

function buildPatternCandidates(founder, domains) {
  const first = founderFirstName(founder).replace(/[^a-z]/g, '');
  const last = founderLastName(founder);
  const patterns = [];
  for (const domain of domains) {
    if (!domain) continue;
    patterns.push(`${first}@${domain}`);
    if (last) {
      patterns.push(`${first}.${last}@${domain}`);
      patterns.push(`${first}${last}@${domain}`);
      patterns.push(`${first[0]}${last}@${domain}`);
    }
    patterns.push(`info@${domain}`, `contact@${domain}`, `hello@${domain}`);
  }
  return [...new Set(patterns.map((e) => e.toLowerCase()))];
}

function mapVerificationResult(result = {}) {
  const status = clean(result.status).toLowerCase();
  if (status === 'valid' || status === 'verified') {
    return { deliverability: 'valid', verified: true, status, reason: result.reason };
  }
  if (status === 'role') {
    return { deliverability: 'valid', verified: true, status, reason: 'role_pattern_mx_ok' };
  }
  if (status === 'invalid') {
    return { deliverability: 'invalid', verified: false, status, reason: result.reason };
  }
  if (status === 'catchall' || status === 'risky') {
    return { deliverability: status, verified: false, status, reason: result.reason };
  }
  if (result.reason === 'verifier_timeout') {
    return { deliverability: 'timeout', verified: false, status: 'unknown', reason: result.reason };
  }
  return { deliverability: 'unknown', verified: false, status: status || 'unknown', reason: result.reason };
}

function candidateRecord(email, discoveryMethod, discoverySource, extras = {}) {
  return {
    email: clean(email).toLowerCase(),
    discoveryMethod,
    discoverySource,
    patternGenerated: discoveryMethod === 'pattern_candidate',
    founderAttribution: extras.founderAttribution === true,
    firstParty: extras.firstParty === true,
    publicFounderSource: extras.publicFounderSource === true,
    roleGeneric: isGenericRoleEmail(email),
    personalProvider: isPersonalEmailProviderDomain(emailDomain(email)),
  };
}

function classifyCandidate(candidate, verification, founder) {
  if (!candidate?.email) return CONTACT_FINAL_STATE.UNRESOLVED;
  if (!verification?.verified) {
    if (verification?.deliverability === 'invalid') {
      return CONTACT_FINAL_STATE.UNRESOLVED;
    }
    if (
      verification?.deliverability === 'unknown'
      || verification?.deliverability === 'timeout'
      || verification?.deliverability === 'risky'
      || verification?.deliverability === 'catchall'
    ) {
      return CONTACT_FINAL_STATE.REVIEW_REQUIRED;
    }
    return CONTACT_FINAL_STATE.UNRESOLVED;
  }

  const founderOnFirstParty = candidate.firstParty
    && isFounderLocalPartMatch(candidate.email, founder);
  const hasFounderEvidence = candidate.publicFounderSource
    || founderOnFirstParty;

  if (hasFounderEvidence && !candidate.roleGeneric) {
    return CONTACT_FINAL_STATE.VERIFIED_FOUNDER_EMAIL;
  }
  if (candidate.roleGeneric || isGenericRoleEmail(candidate.email)) {
    return CONTACT_FINAL_STATE.VERIFIED_ROLE_EMAIL;
  }
  if (candidate.personalProvider && candidate.firstParty) {
    return CONTACT_FINAL_STATE.REVIEW_REQUIRED;
  }
  if (candidate.patternGenerated) {
    return CONTACT_FINAL_STATE.REVIEW_REQUIRED;
  }
  return CONTACT_FINAL_STATE.REVIEW_REQUIRED;
}

function confidenceFor(classification, verification, candidate) {
  if (classification === CONTACT_FINAL_STATE.UNRESOLVED) return 'none';
  if (verification.deliverability === 'unknown' || verification.deliverability === 'timeout') return 'low';
  if (classification === CONTACT_FINAL_STATE.VERIFIED_FOUNDER_EMAIL && candidate.firstParty) return 'high';
  if (classification === CONTACT_FINAL_STATE.VERIFIED_FOUNDER_EMAIL && candidate.publicFounderSource) return 'medium-high';
  if (classification === CONTACT_FINAL_STATE.VERIFIED_ROLE_EMAIL && candidate.firstParty) return 'medium-high';
  if (classification === CONTACT_FINAL_STATE.REVIEW_REQUIRED) return 'medium';
  return 'medium';
}

async function fetchText(url, timeoutMs = 10000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, { signal: controller.signal, redirect: 'follow' });
    const text = await response.text();
    return { ok: response.ok, status: response.status, url, text };
  } catch (err) {
    return { ok: false, status: 0, url, text: '', error: err.message };
  } finally {
    clearTimeout(timer);
  }
}

async function revalidateDomain(target) {
  const domain = normalizeDomain(target.domain);
  const homepageUrl = buildUrl(domain, '/');
  const homepage = await fetchText(homepageUrl);
  const contact = await fetchText(buildUrl(domain, '/contact'));
  const about = await fetchText(buildUrl(domain, '/about'));
  const combined = [homepage.text, contact.text, about.text].join('\n');
  const companyTokens = clean(target.company).toLowerCase().split(/\s+/).filter((t) => t.length > 3);
  const matchHits = companyTokens.filter((token) => combined.toLowerCase().includes(token));
  const confidence = matchHits.length >= Math.min(2, companyTokens.length) ? 'high'
    : matchHits.length >= 1 ? 'medium' : 'low';
  const valid = homepage.ok || contact.ok || about.ok;
  return {
    domain,
    officialDomainEvidence: valid ? 'first_party_website_reachable' : 'domain_unreachable',
    sourceUrl: homepage.ok ? homepage.url : (contact.ok ? contact.url : homepageUrl),
    companyDomainMatch: matchHits.length > 0,
    matchHits,
    confidence: valid ? confidence : 'invalid',
    verifiedAt: new Date().toISOString(),
    httpStatus: homepage.status || contact.status || 0,
  };
}

async function discoverWebsiteEmails(target, domainEvidence) {
  const domains = relatedBrandDomains(domainEvidence.domain);
  const row = {
    company_name: target.company,
    website: domainEvidence.sourceUrl,
    domain: domainEvidence.domain,
  };
  const crawl = await tiered._test.scrapeWebsite(row, { fetchDelayMs: 500 });
  const pages = crawl.pages || [];
  const pageBodies = [];
  for (const pageUrl of pages.slice(0, 8)) {
    const fetched = await fetchText(pageUrl);
    if (fetched.text) pageBodies.push({ url: pageUrl, text: fetched.text });
  }
  if (!pageBodies.length) {
    for (const path of ['/', '/contact', '/contact-us', '/about']) {
      const fetched = await fetchText(buildUrl(domainEvidence.domain, path));
      if (fetched.text) pageBodies.push({ url: fetched.url, text: fetched.text });
    }
  }

  const candidates = [];
  const pushEmail = (email, sourceUrl) => {
    if (!email) return;
    candidates.push(candidateRecord(email, 'first_party_website', sourceUrl, {
      firstParty: true,
      founderAttribution: isFounderLocalPartMatch(email, target.founder),
    }));
  };
  for (const page of pageBodies) {
    const onDomain = extractEmailsFromText(page.text, domains);
    const onPage = extractEmailsFromText(page.text, []);
    for (const email of [...new Set([...onDomain, ...onPage])]) {
      pushEmail(email, page.url);
    }
  }
  for (const email of crawl.emails || []) {
    const normalized = clean(email).toLowerCase();
    if (domains.includes(emailDomain(normalized))) {
      pushEmail(normalized, domainEvidence.sourceUrl);
    }
  }
  return { candidates, pages: pageBodies.map((p) => p.url) };
}

function appendPublicSourceCandidates(target, candidates) {
  const extras = [];
  if (target.akId === 'ak_babrun_prospect_p011') {
    extras.push(candidateRecord('sebastian@ovopainting.com', 'public_founder_source', 'https://www.nethalal.com/ovo-painting/serving-the-halal-industry/5064-roswell-rd-b202-office-a-atlanta-ga-30342', {
      publicFounderSource: true,
      founderAttribution: true,
    }));
  }
  return [...candidates, ...extras];
}

function appendPatternCandidates(target, candidates, domains) {
  const seen = new Set(candidates.map((c) => c.email));
  const patterns = buildPatternCandidates(target.founder, domains);
  const out = [...candidates];
  for (const email of patterns) {
    if (seen.has(email)) continue;
    seen.add(email);
    out.push(candidateRecord(email, 'pattern_candidate', `pattern:${localPart(email)}@${emailDomain(email)}`));
  }
  return out;
}

/** VALID > RISKY/CATCHALL > UNKNOWN/TIMEOUT > INVALID */
function verificationDeliverabilityRank(verification = {}) {
  const deliverability = clean(verification.deliverability).toLowerCase();
  if (deliverability === 'valid' || verification.verified === true) return 0;
  if (deliverability === 'risky' || deliverability === 'catchall') return 1;
  if (deliverability === 'unknown' || deliverability === 'timeout') return 2;
  if (deliverability === 'invalid') return 3;
  return 2;
}

function isInvalidVerification(verification = {}) {
  return verificationDeliverabilityRank(verification) >= 3;
}

/** Stronger attribution wins within equivalent verification quality. */
function attributionRank(candidate = {}) {
  if (candidate.founderAttribution && candidate.firstParty) return 0;
  if (candidate.publicFounderSource && candidate.founderAttribution) return 1;
  if (candidate.firstParty && (candidate.roleGeneric || isGenericRoleEmail(candidate.email))) return 2;
  if (candidate.firstParty && candidate.discoveryMethod !== 'pattern_candidate') return 2;
  if (candidate.discoveryMethod === 'crm_existing') return 3;
  if (!candidate.patternGenerated && !candidate.firstParty) return 3;
  return 4;
}

function levenshteinDistance(a, b) {
  const rows = a.length + 1;
  const cols = b.length + 1;
  const matrix = Array.from({ length: rows }, () => Array(cols).fill(0));
  for (let i = 0; i < rows; i += 1) matrix[i][0] = i;
  for (let j = 0; j < cols; j += 1) matrix[0][j] = j;
  for (let i = 1; i < rows; i += 1) {
    for (let j = 1; j < cols; j += 1) {
      const cost = a[i - 1] === b[j - 1] ? 0 : 1;
      matrix[i][j] = Math.min(
        matrix[i - 1][j] + 1,
        matrix[i][j - 1] + 1,
        matrix[i - 1][j - 1] + cost
      );
    }
  }
  return matrix[a.length][b.length];
}

/** Preserve typo-domain candidates distinctly; penalize in ranking only. */
function isLikelyTypoDomain(candidateDomain, officialDomain) {
  const candidate = normalizeDomain(candidateDomain);
  const official = normalizeDomain(officialDomain);
  if (!candidate || !official || candidate === official) return false;
  if (Math.abs(candidate.length - official.length) > 1) return false;
  return levenshteinDistance(candidate, official) === 1;
}

function typoDomainPenalty(candidate, officialDomain) {
  if (!officialDomain) return 0;
  return isLikelyTypoDomain(emailDomain(candidate.email), officialDomain) ? 1 : 0;
}

function pickBestCandidate(evaluated, options = {}) {
  const classRank = {
    [CONTACT_FINAL_STATE.VERIFIED_FOUNDER_EMAIL]: 0,
    [CONTACT_FINAL_STATE.VERIFIED_ROLE_EMAIL]: 1,
    [CONTACT_FINAL_STATE.REVIEW_REQUIRED]: 2,
    [CONTACT_FINAL_STATE.UNRESOLVED]: 3,
  };
  const officialDomain = options.officialDomain ? normalizeDomain(options.officialDomain) : null;
  const usable = evaluated.filter((row) => !isNoiseEmail(row.email));
  const nonInvalid = usable.filter((row) => !isInvalidVerification(row.verification));
  const pool = nonInvalid.length ? nonInvalid : usable;

  return [...pool].sort((a, b) => {
    const verificationDelta = verificationDeliverabilityRank(a.verification)
      - verificationDeliverabilityRank(b.verification);
    if (verificationDelta !== 0) return verificationDelta;

    const typoDelta = typoDomainPenalty(a, officialDomain) - typoDomainPenalty(b, officialDomain);
    if (typoDelta !== 0) return typoDelta;

    const attributionDelta = attributionRank(a) - attributionRank(b);
    if (attributionDelta !== 0) return attributionDelta;

    const cls = classRank[a.classification] - classRank[b.classification];
    if (cls !== 0) return cls;

    const pathDepth = (url) => {
      try {
        return new URL(url).pathname.split('/').filter(Boolean).length;
      } catch {
        return 99;
      }
    };
    const depthDelta = pathDepth(a.discoverySource) - pathDepth(b.discoverySource);
    if (depthDelta !== 0) return depthDelta;
    return a.email.localeCompare(b.email);
  })[0] || null;
}

async function evaluateCandidates(candidates, founder, verifyFn = verifyEmail) {
  const evaluated = [];
  for (const candidate of candidates) {
    const raw = await verifyFn(candidate.email);
    const verification = {
      ...mapVerificationResult(raw),
      method: raw.method || raw.vendor || null,
      raw: raw.raw || null,
      verifiedAt: new Date().toISOString(),
    };
    const classification = classifyCandidate(candidate, verification, founder);
    evaluated.push({
      ...candidate,
      verification,
      classification,
      outreachContactType: classification === CONTACT_FINAL_STATE.UNRESOLVED
        ? OUTREACH_CONTACT_TYPE.UNVERIFIED_EMAIL
        : classifyOutreachContactType(candidate.email, { verified: verification.verified }),
    });
  }
  return evaluated;
}

async function loadProspectByAkId(db, akId) {
  const result = await db.query(
    `SELECT p.*, c.name AS company_name, c.website, c.acquisition_metadata AS company_acquisition_metadata
       FROM prospects p
       LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
      WHERE p.client_id = $1
        AND p.acquisition_knowledge_object_id = $2
      LIMIT 1`,
    [CLIENT_ID, akId]
  );
  return result.rows[0] || null;
}

function crmExistingCandidates(prospect, domains) {
  if (!prospect?.email) return [];
  const email = clean(prospect.email).toLowerCase();
  const domain = emailDomain(email);
  const onBrand = domains.includes(domain);
  return [candidateRecord(email, 'crm_existing', 'prospects.email', {
    firstParty: onBrand,
    founderAttribution: isFounderLocalPartMatch(email, prospect.first_name || ''),
  })];
}

async function persistContactResolution(db, prospect, target, result, dryRun) {
  if (!prospect || dryRun) return { persisted: false, dryRun: true };

  const best = result.best;
  const contactEvidence = {
    contactResolution: {
      akObjectId: target.akId,
      resolvedAt: new Date().toISOString(),
      finalState: result.finalState,
      classification: best?.classification || CONTACT_FINAL_STATE.UNRESOLVED,
      bestEmail: best?.email || null,
      discoverySource: best?.discoverySource || null,
      discoveryMethod: best?.discoveryMethod || null,
      verification: best?.verification || null,
      domainEvidence: result.domainEvidence,
      candidates: result.evaluated.map((row) => ({
        email: row.email,
        discoveryMethod: row.discoveryMethod,
        discoverySource: row.discoverySource,
        classification: row.classification,
        verification: row.verification,
      })),
      alternateChannels: result.alternateChannels || [],
      scoutLearning: {
        lesson: 'prospect_discovery_incomplete_without_verified_contact',
        pipeline: 'company_identity → official_domain → attributable_contact → verification → classification',
        outcome: result.finalState,
      },
    },
  };

  const provenanceSource = persistableEmailSource(
    best?.discoveryMethod === 'first_party_website' ? 'website_email'
      : best?.discoveryMethod === 'public_founder_source' ? 'public_directory'
        : best?.discoveryMethod === 'crm_existing' ? 'existing_prospect_email'
          : null
  );

  const enrichmentProvenance = stampEmailProvenance(prospect.enrichment_provenance, provenanceSource || 'contact_resolution', {
    contact_classification: result.finalState,
    discovery_source: best?.discoverySource || null,
    discovery_method: best?.discoveryMethod || null,
    verifier: best?.verification?.method || null,
    status: best?.verification?.status || null,
    resolved_at: new Date().toISOString(),
  });

  const sets = [
    'acquisition_metadata = COALESCE(acquisition_metadata, \'{}\'::jsonb) || $1::jsonb',
    'enrichment_provenance = COALESCE(enrichment_provenance, \'{}\'::jsonb) || $2::jsonb',
    'updated_at = NOW()',
  ];
  const values = [JSON.stringify(contactEvidence), JSON.stringify(enrichmentProvenance)];

  if (
    best?.email
    && best.verification?.verified
    && (result.finalState === CONTACT_FINAL_STATE.VERIFIED_FOUNDER_EMAIL
      || result.finalState === CONTACT_FINAL_STATE.VERIFIED_ROLE_EMAIL)
  ) {
    sets.push(`email = $${values.length + 1}`);
    values.push(best.email);
    sets.push(`email_verified = $${values.length + 1}`);
    values.push(true);
    sets.push(`email_verification_method = $${values.length + 1}`);
    values.push(best.verification.method || 'bouncer');
    sets.push(`email_status = $${values.length + 1}`);
    values.push(best.verification.status === 'role' ? 'role' : 'valid');
    sets.push(`verified_at = COALESCE(verified_at, NOW())`);
    sets.push(`verifier_checked_at = NOW()`);
    sets.push(`verifier_response = $${values.length + 1}::jsonb`);
    values.push(JSON.stringify(best.verification.raw || null));
  }

  values.push(prospect.id, CLIENT_ID);
  await db.query(
    `UPDATE prospects
        SET ${sets.join(', ')}
      WHERE id = $${values.length - 1}
        AND client_id = $${values.length}`,
    values
  );

  if (result.domainEvidence?.domain) {
    await db.query(
      `UPDATE companies
          SET website = COALESCE(NULLIF(TRIM(website), ''), $1),
              acquisition_metadata = COALESCE(acquisition_metadata, '{}'::jsonb) || $2::jsonb,
              updated_at = NOW()
        WHERE id = $3
          AND client_id = $4`,
      [
        `https://${result.domainEvidence.domain}`,
        JSON.stringify({ domainEvidence: result.domainEvidence }),
        prospect.company_id,
        CLIENT_ID,
      ]
    );
  }

  await db.query(
    `INSERT INTO agent_log (agent_name, action, prospect_id, payload, status, ran_at, client_id)
     VALUES ($1, $2, $3, $4::jsonb, $5, NOW(), $6)`,
    [
      'contact_resolution',
      'babrun_first_ten_contact_resolution',
      prospect.id,
      JSON.stringify({
        akObjectId: target.akId,
        finalState: result.finalState,
        bestEmail: best?.email || null,
        scoutLearning: contactEvidence.contactResolution.scoutLearning,
      }),
      'success',
      CLIENT_ID,
    ]
  );

  await safeIngestEnrichmentOutcome({
    prospectId: prospect.id,
    clientId: CLIENT_ID,
    sourceRecordId: `babrun_contact_resolution:${target.akId}:${new Date().toISOString().slice(0, 10)}`,
    eventTimestamp: new Date(),
    status: result.finalState === CONTACT_FINAL_STATE.UNRESOLVED ? 'failed' : 'success',
    payload: {
      provider: 'babrun_contact_resolution',
      final_state: result.finalState,
      classification: best?.classification || null,
      verified_email: best?.verification?.verified === true,
    },
  });

  return { persisted: true };
}

function extractAlternateChannels(pageText, domainEvidence) {
  const text = String(pageText || '');
  const phones = [...new Set(text.match(/(?:\+1\s*)?(?:\(\d{3}\)|\d{3})[\s.-]?\d{3}[\s.-]?\d{4}/g) || [])];
  return {
    phones: phones.slice(0, 3),
    websiteContactForm: /form|request a quote|get in touch|contact us/i.test(text),
    domain: domainEvidence?.domain || null,
  };
}

async function resolveTarget(target, options = {}) {
  const domainEvidence = await revalidateDomain(target);
  const domains = relatedBrandDomains(domainEvidence.domain);
  const websiteDiscovery = await discoverWebsiteEmails(target, domainEvidence);
  let candidates = websiteDiscovery.candidates;
  candidates = appendPublicSourceCandidates(target, candidates);
  if (options.prospect) {
    candidates = [...crmExistingCandidates(options.prospect, domains), ...candidates];
  }
  candidates = appendPatternCandidates(target, candidates, domains);

  const deduped = [];
  const seen = new Set();
  for (const c of candidates) {
    if (seen.has(c.email)) continue;
    seen.add(c.email);
    deduped.push(c);
  }

  const evaluated = await evaluateCandidates(deduped, target.founder, options.verifyEmail || verifyEmail);
  const best = pickBestCandidate(evaluated, { officialDomain: domainEvidence.domain });
  const finalState = best?.classification || CONTACT_FINAL_STATE.UNRESOLVED;

  const pageSample = await fetchText(buildUrl(domainEvidence.domain, '/'));
  const alternateChannels = extractAlternateChannels(pageSample.text, domainEvidence);

  return {
    target,
    domainEvidence,
    candidatesFound: deduped.map((c) => c.email),
    evaluated,
    best,
    finalState,
    confidence: best ? confidenceFor(finalState, best.verification, best) : 'none',
    alternateChannels,
    pages: websiteDiscovery.pages,
  };
}

function formatReportRow(result) {
  const best = result.best;
  return {
    prospect: `${result.target.founder} — ${result.target.company}`,
    domain: result.domainEvidence.domain,
    candidatesFound: result.candidatesFound.join(', ') || '(none)',
    bestEmail: best?.email || '(none)',
    discoverySource: best?.discoverySource || '(none)',
    verificationResult: best
      ? `${best.verification.deliverability}/${best.verification.status} (${best.verification.method || 'n/a'})`
      : '(none)',
    contactClassification: result.finalState,
    confidence: result.confidence,
    finalState: result.finalState,
  };
}

function summarizeResults(results) {
  const totals = {
    VERIFIED_FOUNDER_EMAIL: 0,
    VERIFIED_ROLE_EMAIL: 0,
    REVIEW_REQUIRED: 0,
    UNRESOLVED: 0,
  };
  for (const row of results) {
    totals[row.finalState] = (totals[row.finalState] || 0) + 1;
  }
  return totals;
}

module.exports = {
  TENANT_ID,
  CLIENT_ID,
  BABRUN_CONTACT_TARGETS,
  CONTACT_FINAL_STATE,
  resolveTarget,
  persistContactResolution,
  loadProspectByAkId,
  formatReportRow,
  summarizeResults,
  // test hooks
  classifyCandidate,
  mapVerificationResult,
  isFounderLocalPartMatch,
  pickBestCandidate,
  verificationDeliverabilityRank,
  attributionRank,
  isLikelyTypoDomain,
  isInvalidVerification,
};
