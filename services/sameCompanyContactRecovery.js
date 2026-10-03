'use strict';

/**
 * Same-company alternate contact recovery for Max replenishment.
 * Reuses canonical company rows; never creates duplicate companies.
 */

const axios = require('axios');
const {
  normalizeDomain,
  resolveEmailVerification,
  searchProspeoContactsForDomain,
  filterScrapedWebsiteEmails,
} = require('../leadgen');
const { invalidOutreachEmailReason } = require('../utils/emailGuard');
const { normalizeVertical } = require('../utils/normalize');
const { stampEmailProvenance } = require('../utils/canonicalEmailEligibility');
const { governedContactReason } = require('../utils/governedContactEligibility');
const {
  classifyInventoryOwnership,
  classifyOwnershipRow,
  evaluateColdOutboundEligibility,
  OWNERSHIP_KINDS,
} = require('./outboundInventory');

const RECOVERY_TERMINAL_REASONS = Object.freeze([
  'alternate_contact_resolved',
  'no_alternate_contact_found',
  'alternate_email_missing',
  'alternate_email_unverified',
  'alternate_email_invalid',
  'alternate_email_provenance_missing',
  'alternate_email_binding_invalid',
  'alternate_contact_owned',
  'alternate_contact_suppressed',
  'alternate_contact_dnc',
  'alternate_contact_already_attempted',
  'provider_unavailable',
  'provider_error',
]);

const RECOVERY_LOSS_REASONS = RECOVERY_TERMINAL_REASONS.filter(reason => reason !== 'alternate_contact_resolved');

const PREFERRED_TITLE_INCLUDES = Object.freeze([
  'owner',
  'property manager',
  'operations',
  'facilities',
  'general manager',
  'office manager',
  'managing partner',
  'partner',
  'principal',
  'founder',
  'president',
]);

const ROLE_RANKERS = Object.freeze([
  { rank: 0, re: /\b(owner|founder|principal|president|managing\s+partner)\b/i },
  { rank: 1, re: /\bproperty\s*manager\b/i },
  { rank: 2, re: /\boperations\b/i },
  { rank: 3, re: /\bfacilities\b/i },
  { rank: 4, re: /\bgeneral\s*manager\b|\bgm\b/i },
  { rank: 5, re: /\boffice\s*manager\b/i },
  { rank: 6, re: /^(?:info|contact|office|hello|admin|team)@/i },
]);

function emptyAlternateContactTelemetry() {
  return {
    sameCompanyCandidatesAttempted: 0,
    alternateContactsResolved: 0,
    alternateContactsVerified: 0,
    alternateContactsRejected: 0,
    alternateContactsAddedToCleanInventory: 0,
    alternateContactLossReasons: Object.fromEntries(RECOVERY_LOSS_REASONS.map(k => [k, 0])),
    terminalReasons: [],
  };
}

function mergeAlternateTelemetry(target, delta = {}) {
  if (!target || !delta || target === delta) return target;
  for (const key of [
    'sameCompanyCandidatesAttempted',
    'alternateContactsResolved',
    'alternateContactsVerified',
    'alternateContactsRejected',
    'alternateContactsAddedToCleanInventory',
  ]) {
    target[key] = Number(target[key] || 0) + Number(delta[key] || 0);
  }
  if (!target.alternateContactLossReasons) target.alternateContactLossReasons = {};
  const losses = delta.alternateContactLossReasons || {};
  for (const reason of RECOVERY_LOSS_REASONS) {
    target.alternateContactLossReasons[reason] = Number(target.alternateContactLossReasons[reason] || 0)
      + Number(losses[reason] || 0);
  }
  if (Array.isArray(delta.terminalReasons) && delta.terminalReasons.length) {
    target.terminalReasons = [...(target.terminalReasons || []), ...delta.terminalReasons];
  }
  return target;
}

function clampCohortCounters(admission = {}) {
  const evaluated = Math.max(0, Number(admission.evaluated || 0));
  const cap = (value) => {
    const n = Math.max(0, Number(value || 0));
    if (!evaluated) return n;
    return Math.min(n, evaluated);
  };
  admission.sameCompanyCandidatesAttempted = cap(admission.sameCompanyCandidatesAttempted);
  if (admission.rejected && admission.rejected.same_company_different_contact != null) {
    admission.rejected.same_company_different_contact = cap(admission.rejected.same_company_different_contact);
  }
  return admission;
}

function recordAlternateLoss(telemetry, reason) {
  if (!telemetry || !reason) return;
  telemetry.alternateContactsRejected = Number(telemetry.alternateContactsRejected || 0) + 1;
  if (telemetry.alternateContactLossReasons && telemetry.alternateContactLossReasons[reason] != null) {
    telemetry.alternateContactLossReasons[reason] += 1;
  } else if (telemetry.alternateContactLossReasons) {
    telemetry.alternateContactLossReasons[reason] = 1;
  }
  if (Array.isArray(telemetry.terminalReasons)) telemetry.terminalReasons.push(reason);
}

function recordAlternateSuccess(telemetry) {
  if (!telemetry) return;
  if (Array.isArray(telemetry.terminalReasons)) telemetry.terminalReasons.push('alternate_contact_resolved');
}

function preferredRoleRank(contact = {}) {
  const title = String(contact.title || contact.job_title || '').trim();
  const email = String(contact.email || '').trim().toLowerCase();
  const haystack = `${title} ${email}`;
  for (const ranker of ROLE_RANKERS) {
    if (ranker.re.test(haystack) || ranker.re.test(email)) return ranker.rank;
  }
  return 20;
}

function sortPreferredContacts(contacts = []) {
  return [...contacts].sort((a, b) => preferredRoleRank(a) - preferredRoleRank(b));
}

function canonicalProspectUnavailable(row, now = Date.now()) {
  if (!row) return true;
  if (governedContactReason(row)) return true;
  if (row.do_not_contact === true) return true;
  if (row.email_verified !== true) return true;
  const ownership = classifyOwnershipRow(row, now);
  return ownership.kind !== OWNERSHIP_KINDS.CLEAR;
}

function splitContactName(contact) {
  const parts = String(contact || '').trim().split(/\s+/).filter(Boolean);
  if (!parts.length) return { firstName: null, lastName: null };
  return {
    firstName: parts[0],
    lastName: parts.slice(1).join(' ') || null,
  };
}

async function loadCompanyProspectRows(pool, { domain, companyName, companyId }) {
  const { rows } = await pool.query(`
    SELECT p.id, p.company_id, p.email, p.email_verified, p.email_status, p.do_not_contact, p.enrichment_provenance,
      p.assigned_ao_id, p.closer_id, p.last_contacted_at, p.last_reply_at, p.vertical,
      p.first_name, p.last_name, p.job_title, p.service_area_match, c.name, c.domain, c.website,
      EXISTS(SELECT 1 FROM ao_prospect_tasks t WHERE t.client_id=10 AND t.prospect_id=p.id) AS has_ao_task,
      EXISTS(SELECT 1 FROM touchpoints t WHERE t.client_id=10 AND t.prospect_id=p.id
        AND t.action_type IN ('email_sent','sent','outbound_email','call','call_attempt','inbound_reply','reply','email_reply','reply_received')) AS prior_touch
    FROM prospects p
    JOIN companies c ON c.id=p.company_id AND c.client_id=p.client_id
    WHERE p.client_id=10
      AND (
        ($1::text IS NOT NULL AND c.id::text = $1)
        OR ($2::text IS NOT NULL AND lower(c.domain)=lower($2))
        OR ($3::text <> '' AND lower(trim(c.name))=lower(trim($3)))
      )
    ORDER BY p.updated_at DESC NULLS LAST
  `, [companyId || null, domain || null, companyName || '']);
  return rows;
}

async function collectAttemptedEmails(pool, companyId) {
  const attempted = await pool.query(`
    SELECT lower(email) AS email FROM acquisition_outbound_items
    WHERE tenant_id='10' AND company_id=$1 AND attempted_at IS NOT NULL AND email IS NOT NULL
  `, [String(companyId)]).catch(() => ({ rows: [] }));
  return new Set(attempted.rows.map(row => String(row.email || '').toLowerCase()).filter(Boolean));
}

async function loadPfIntelligenceContacts(pool, { companyId, domain, companyName, knownEmails }) {
  const rows = await loadCompanyProspectRows(pool, { domain, companyName, companyId }).catch(() => []);
  const contacts = [];
  for (const row of rows) {
    const email = String(row.email || '').trim().toLowerCase();
    if (!email || knownEmails.has(email)) continue;
    // A legacy CRM address alone is not acquisition evidence. Fresh sources
    // below may rediscover it and repair the canonical record.
    if (!row.enrichment_provenance?.email?.source) continue;
    contacts.push({
      email,
      contact: `${row.first_name || ''} ${row.last_name || ''}`.trim(),
      title: row.job_title || null,
      source: ['pf_intelligence'],
      enrichmentProvenance: row.enrichment_provenance,
      prospectId: row.id,
    });
  }
  return { status: 'ok', contacts };
}

async function listWebsiteContactsForDomain(domain, website) {
  if (!domain && !website) return { status: 'unavailable', contacts: [] };
  try {
    const { crawlWebsite, normalizeDomain: crawlNormalizeDomain } = require('../utils/websiteEnrichmentCrawl');
    const normalizedDomain = crawlNormalizeDomain(domain || website);
    if (!normalizedDomain) return { status: 'unavailable', contacts: [] };
    const { pages } = await crawlWebsite(normalizedDomain, async (url) => {
      const res = await axios.get(url, {
        timeout: 5000,
        headers: { 'User-Agent': 'Mozilla/5.0 (compatible; Googlebot/2.1)' },
        validateStatus: () => true,
      });
      return {
        ok: res.status >= 200 && res.status < 400,
        status: res.status,
        text: res.data,
        url: res.request?.res?.responseURL || url,
      };
    }, { maxSuccessfulPages: 8 });
    const contacts = [];
    const seen = new Set();
    for (const page of pages || []) {
      const emails = filterScrapedWebsiteEmails(page.text, normalizedDomain);
      const text = String(page.text || '').replace(/<[^>]+>/g, ' ');
      for (const email of emails) {
        const normalized = String(email || '').trim().toLowerCase();
        if (!normalized || seen.has(normalized)) continue;
        seen.add(normalized);
        const idx = text.toLowerCase().indexOf(normalized);
        const window = idx >= 0 ? text.slice(Math.max(0, idx - 80), idx + normalized.length + 80) : '';
        contacts.push({
          email: normalized,
          contact: '',
          title: window.replace(/\s+/g, ' ').trim().slice(0, 80) || null,
          source: ['website_email'],
          sourceUrl: page.url,
        });
      }
    }
    return { status: 'ok', contacts };
  } catch (_err) {
    return { status: 'error', contacts: [] };
  }
}

async function listHunterContactsForDomain(domain) {
  const hunterKey = process.env.HUNTER_API_KEY;
  if (!hunterKey) return { status: 'unavailable', contacts: [] };
  try {
    const res = await axios.get('https://api.hunter.io/v2/domain-search', {
      params: { domain, api_key: hunterKey, limit: 10 },
      timeout: 8000,
    });
    const contacts = [];
    for (const row of res.data?.data?.emails || []) {
      const email = String(row.value || '').trim().toLowerCase();
      if (!email) continue;
      contacts.push({
        email,
        contact: `${row.first_name || ''} ${row.last_name || ''}`.trim(),
        title: row.position || null,
        source: ['hunter'],
      });
    }
    return { status: 'ok', contacts };
  } catch (_err) {
    return { status: 'error', contacts: [] };
  }
}

async function resolveAlternateContacts({
  domain,
  website = null,
  companyId = null,
  companyName = null,
  discoveredEmail = null,
  knownEmails = [],
  pool = null,
  enrich = null,
  sources = {},
}) {
  const known = new Set((knownEmails || []).map(e => String(e || '').trim().toLowerCase()).filter(Boolean));
  const discovered = String(discoveredEmail || '').trim().toLowerCase();
  const candidates = [];
  const sourceStatus = {
    pf_intelligence: 'skipped',
    website: 'skipped',
    prospeo: 'skipped',
    hunter: 'skipped',
  };

  const pushUnique = (row) => {
    const email = String(row?.email || '').trim().toLowerCase();
    const contact = {
      ...row,
      email: email || null,
      contact: row.contact || '',
      title: row.title || row.job_title || null,
      source: row.source || [],
    };
    if (email && known.has(email)) return;
    const duplicate = email ? candidates.findIndex(existing => existing.email === email) : -1;
    if (duplicate >= 0) {
      if (['scout_discovery', 'pf_intelligence'].includes(candidates[duplicate].source?.[0])
        && contact.source.some(source => ['prospeo', 'hunter', 'website', 'website_email'].includes(source))) {
        candidates[duplicate] = contact;
      }
      return;
    }
    candidates.push(contact);
  };

  if (discovered && !known.has(discovered)) {
    pushUnique({ email: discovered, contact: '', source: ['scout_discovery'] });
  }

  const pf = sources.pfIntelligence
    || (pool ? ((input) => loadPfIntelligenceContacts(pool, input)) : null);
  if (pf) {
    const result = await pf({ companyId, domain, companyName, knownEmails: known });
    sourceStatus.pf_intelligence = result.status || 'ok';
    for (const row of result.contacts || []) pushUnique(row);
  }

  const inTest = Boolean(process.env.NODE_TEST_CONTEXT) || process.env.NODE_ENV === 'test';
  const stubLive = (Boolean(enrich) || inTest) && !sources.website && !sources.hunter;
  const websiteFn = sources.website
    || (stubLive
      ? async () => ({ status: 'ok', contacts: [] })
      : ((input) => listWebsiteContactsForDomain(input.domain, input.website)));
  const websiteResult = await websiteFn({ domain, website });
  sourceStatus.website = websiteResult.status || 'ok';
  for (const row of websiteResult.contacts || []) pushUnique(row);

  const prospeoFn = sources.prospeo || (async (input) => {
    const result = await (enrich
      ? enrich(input.domain, { excludeEmails: [], titleIncludes: PREFERRED_TITLE_INCLUDES })
      : searchProspeoContactsForDomain(input.domain, {
        excludeEmails: [],
        titleIncludes: PREFERRED_TITLE_INCLUDES,
      }));
    if (Array.isArray(result)) return { status: 'ok', contacts: result };
    if (result?.ok === false) {
      return { status: result.reason === 'provider_unavailable' ? 'unavailable' : 'error', contacts: [] };
    }
    return { status: 'ok', contacts: result?.contacts || [] };
  });
  const prospeoResult = await prospeoFn({ domain });
  sourceStatus.prospeo = prospeoResult.status || 'ok';
  for (const row of prospeoResult.contacts || []) {
    pushUnique({ ...row, source: row.source || ['prospeo'] });
  }

  const hunterFn = sources.hunter
    || (stubLive
      ? async () => ({ status: 'ok', contacts: [] })
      : ((input) => listHunterContactsForDomain(input.domain)));
  const hunterResult = await hunterFn({ domain });
  sourceStatus.hunter = hunterResult.status || 'ok';
  for (const row of hunterResult.contacts || []) pushUnique(row);

  const statuses = Object.values(sourceStatus);
  const okCount = statuses.filter(status => status === 'ok').length;
  const unavailableCount = statuses.filter(status => status === 'unavailable').length;
  const errorCount = statuses.filter(status => status === 'error').length;

  let emptyReason = 'no_alternate_contact_found';
  if (!candidates.length) {
    if (okCount === 0 && errorCount > 0) emptyReason = 'provider_error';
    else if (okCount === 0 && unavailableCount > 0) emptyReason = 'provider_unavailable';
  }

  return {
    contacts: sortPreferredContacts(candidates),
    sourceStatus,
    emptyReason,
  };
}

async function admitAlternateProspect(pool, {
  companyId,
  email,
  contact,
  verification,
  vertical,
  serviceAreaMatch,
  discoveryMethod,
  websiteUrl,
  existingProspectId = null,
  enrichmentProvenance,
}) {
  if (existingProspectId) {
    const updated = await pool.query(`
      UPDATE prospects
      SET email_verified = $2,
          email_verification_method = $3,
          verified_at = $4,
          do_not_contact = CASE WHEN $5 THEN true ELSE do_not_contact END,
          email_status = $6,
          verifier_response = $7::jsonb,
          verifier_checked_at = $8,
          notes = COALESCE(notes, '') || $9,
          enrichment_provenance = $10::jsonb
      WHERE id = $1 AND client_id = 10 AND company_id = $11
        AND lower(email) = lower($12) AND COALESCE(do_not_contact,false)=false
    `, [
      existingProspectId,
      verification.emailVerified,
      verification.emailVerificationMethod,
      verification.verifiedAt,
      verification.doNotContact,
      verification.emailStatus,
      JSON.stringify(verification.verifierResponse || null),
      verification.verifierCheckedAt,
      ' | recovered via same-company alternate contact verification.',
      JSON.stringify(enrichmentProvenance),
      companyId,
      email,
    ]);
    return updated.rowCount === 1 ? existingProspectId : null;
  }

  const names = splitContactName(contact);
  const insert = await pool.query(`
    INSERT INTO prospects (
      company_id, first_name, last_name, email, phone, status, source, icp_score, notes, vertical,
      client_id, service_area_match, discovery_method, website_url,
      email_verified, email_verification_method, verified_at, do_not_contact,
      email_status, verifier_response, verifier_checked_at, enrichment_provenance
    ) VALUES ($1, $15, $16, $2, NULL, 'cold', 'scout', 70, $3, $4, 10, $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb, $14, $17::jsonb)
    ON CONFLICT (email) DO NOTHING
    RETURNING id
  `, [
    companyId,
    email,
    verification.note || 'Recovered via same-company alternate contact enrichment.',
    normalizeVertical(vertical) || 'unknown',
    serviceAreaMatch,
    discoveryMethod || 'same_company_alternate_recovery',
    websiteUrl || null,
    verification.emailVerified,
    verification.emailVerificationMethod,
    verification.verifiedAt,
    verification.doNotContact,
    verification.emailStatus,
    JSON.stringify(verification.verifierResponse || null),
    verification.verifierCheckedAt,
    names.firstName,
    names.lastName,
    JSON.stringify(enrichmentProvenance),
  ]);
  return insert.rows[0]?.id || null;
}

function classifyProviderEmptyReason(resolved) {
  return resolved.emptyReason || 'no_alternate_contact_found';
}

async function attemptSameCompanyAlternateRecovery(store, pool, {
  company,
  ownership = null,
  scoutContext = {},
  telemetry = null,
  now = Date.now(),
  verify = resolveEmailVerification,
  enrich = null,
  sources = {},
} = {}) {
  const stats = telemetry || emptyAlternateContactTelemetry();
  stats.sameCompanyCandidatesAttempted += 1;

  const name = String(company.name || '').trim();
  const website = String(company.website || '').trim() || null;
  const domain = normalizeDomain(company.domain || website);
  if (!name || !domain) {
    recordAlternateLoss(stats, 'no_alternate_contact_found');
    return { ok: false, reason: 'no_alternate_contact_found', telemetry: stats };
  }

  const classified = ownership && ownership.kind === OWNERSHIP_KINDS.SAME_COMPANY_DIFFERENT_CONTACT
    ? ownership
    : await classifyInventoryOwnership(store, { company: name, domain, website }, { pool, now });
  if (classified.kind !== OWNERSHIP_KINDS.SAME_COMPANY_DIFFERENT_CONTACT) {
    if (classified.kind === OWNERSHIP_KINDS.VALID_COLLISION) {
      recordAlternateLoss(stats, 'alternate_contact_owned');
      return { ok: false, reason: 'alternate_contact_owned', telemetry: stats };
    }
    return { ok: false, reason: 'not_same_company_candidate', telemetry: stats };
  }

  const companyId = classified.companyId;
  const rows = await loadCompanyProspectRows(pool, { domain, companyName: name, companyId });
  const resolvedCompanyId = companyId || rows[0]?.company_id;
  if (!resolvedCompanyId) {
    recordAlternateLoss(stats, 'no_alternate_contact_found');
    return { ok: false, reason: 'no_alternate_contact_found', telemetry: stats };
  }

  const hasUsableCanonical = rows.some(row => !canonicalProspectUnavailable(row, now));
  if (hasUsableCanonical) {
    return { ok: false, reason: 'canonical_still_usable', telemetry: stats };
  }

  // Missing acquisition evidence is repairable only by observing the address
  // again at a real source. Do not exclude it from website/provider discovery.
  const knownEmails = new Set(rows.filter(row => row.do_not_contact === true
    || classifyOwnershipRow(row, now).kind !== OWNERSHIP_KINDS.CLEAR
    || !governedContactReason(row))
    .map(row => String(row.email || '').trim().toLowerCase()).filter(Boolean));
  const attemptedEmails = await collectAttemptedEmails(pool, resolvedCompanyId);
  const resolved = await resolveAlternateContacts({
    domain,
    website,
    companyId: resolvedCompanyId,
    companyName: name,
    discoveredEmail: company.email || company.contactEmail,
    knownEmails: [...knownEmails],
    pool,
    enrich,
    sources,
  });

  if (!resolved.contacts.length) {
    const reason = classifyProviderEmptyReason(resolved);
    recordAlternateLoss(stats, reason);
    return { ok: false, reason, telemetry: stats };
  }

  let terminalReason = 'no_alternate_contact_found';
  for (const alternate of resolved.contacts) {
    if (!alternate.email) {
      terminalReason = 'alternate_email_missing';
      continue;
    }
    stats.alternateContactsResolved += 1;
    if (invalidOutreachEmailReason(alternate.email)) {
      terminalReason = 'alternate_email_invalid';
      continue;
    }
    if (attemptedEmails.has(alternate.email)) {
      terminalReason = 'alternate_contact_already_attempted';
      continue;
    }
    const existingRow = alternate.prospectId
      ? rows.find(row => String(row.id) === String(alternate.prospectId))
      : rows.find(row => String(row.email || '').toLowerCase() === alternate.email);
    if (existingRow && !alternate.prospectId) alternate.prospectId = existingRow.id;
    if (existingRow?.do_not_contact === true) {
      terminalReason = 'alternate_contact_dnc';
      continue;
    }
    if (alternate.prospectId && (!existingRow || String(existingRow.company_id) !== String(resolvedCompanyId))) {
      terminalReason = 'alternate_email_binding_invalid';
      continue;
    }

    const verification = await verify(alternate.email, {
      email: alternate.email,
      contact: alternate.contact,
      source: alternate.source || ['same_company_recovery'],
      url: website,
    });
    if (verification.emailVerified === true && verification.doNotContact === true) {
      terminalReason = 'alternate_contact_dnc';
      continue;
    }
    if (verification.reject || verification.emailVerified !== true) {
      const status = String(verification.emailStatus || '').toLowerCase();
      terminalReason = verification.reject || status === 'invalid'
        ? 'alternate_email_invalid'
        : 'alternate_email_unverified';
      continue;
    }
    stats.alternateContactsVerified += 1;

    // Persist actual acquisition evidence, never a verification/read-path label.
    const source = (alternate.source || []).map(x => x === 'website' ? 'website_email' : x).join('+');
    const enrichmentProvenance = source === 'pf_intelligence'
      ? (existingRow?.enrichment_provenance || alternate.enrichmentProvenance || {})
      : stampEmailProvenance(existingRow?.enrichment_provenance, source, {
        source_url: alternate.sourceUrl || null, verifier: verification.emailVerificationMethod,
        status: verification.emailStatus, resolved_at: new Date().toISOString(),
      });
    const contactReason = governedContactReason({ email: alternate.email, domain,
      email_verified: true, email_status: verification.emailStatus, do_not_contact: verification.doNotContact,
      enrichment_provenance: enrichmentProvenance });
    if (contactReason) {
      terminalReason = contactReason === 'missing_email_provenance'
        ? 'alternate_email_provenance_missing' : 'alternate_email_binding_invalid';
      continue;
    }

    const candidate = {
      candidateId: `same_company_${resolvedCompanyId}_${alternate.email}`,
      prospectId: alternate.prospectId ? String(alternate.prospectId) : null,
      companyId: String(resolvedCompanyId),
      company: name,
      domain,
      website,
      email: alternate.email,
    };
    const ownershipBlock = await store.candidateOwnership(candidate);
    if (ownershipBlock) {
      terminalReason = 'alternate_contact_owned';
      continue;
    }
    const suppression = await store.suppression(candidate, '__max_inventory_buffer__');
    if (suppression) {
      terminalReason = suppression === 'already_attempted'
        ? 'alternate_contact_already_attempted'
        : 'alternate_contact_suppressed';
      continue;
    }

    const eligibility = evaluateColdOutboundEligibility({
      businessFit: 'qualified',
      geography: 'in_scope',
      contactVerified: true,
      dnc: verification.doNotContact === true,
      suppression: null,
      ownership: 'clear',
      buyerReadiness: 'unknown',
      emailReason: null,
    });
    if (!eligibility.eligible) {
      terminalReason = eligibility.reason === 'dnc' ? 'alternate_contact_dnc' : 'alternate_contact_suppressed';
      continue;
    }

    const prospectId = await admitAlternateProspect(pool, {
      companyId: resolvedCompanyId,
      email: alternate.email,
      contact: alternate.contact,
      verification,
      vertical: company.vertical || rows[0]?.vertical || scoutContext.scope?.segment,
      serviceAreaMatch: rows[0]?.service_area_match ?? true,
      discoveryMethod: 'same_company_alternate_recovery',
      websiteUrl: website,
      existingProspectId: alternate.prospectId || null,
      enrichmentProvenance,
    });
    if (!prospectId) {
      terminalReason = 'alternate_contact_owned';
      continue;
    }

    stats.alternateContactsAddedToCleanInventory += 1;
    recordAlternateSuccess(stats);
    return {
      ok: true,
      reason: 'alternate_contact_resolved',
      prospectId: String(prospectId),
      email: alternate.email,
      telemetry: stats,
    };
  }

  recordAlternateLoss(stats, terminalReason);
  return { ok: false, reason: terminalReason, telemetry: stats };
}

module.exports = {
  RECOVERY_LOSS_REASONS,
  RECOVERY_TERMINAL_REASONS,
  PREFERRED_TITLE_INCLUDES,
  emptyAlternateContactTelemetry,
  mergeAlternateTelemetry,
  clampCohortCounters,
  attemptSameCompanyAlternateRecovery,
  canonicalProspectUnavailable,
  preferredRoleRank,
  _test: {
    resolveAlternateContacts,
    loadCompanyProspectRows,
    sortPreferredContacts,
  },
};
