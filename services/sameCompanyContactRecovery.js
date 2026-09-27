'use strict';

/**
 * Same-company alternate contact recovery for Max replenishment.
 * Reuses canonical company rows; never creates duplicate companies.
 */

const { normalizeDomain, resolveEmailVerification, listProspeoContactsForDomain } = require('../leadgen');
const { canonicalOutboundEmailIneligibilityReason } = require('../utils/canonicalEmailEligibility');
const { invalidOutreachEmailReason } = require('../utils/emailGuard');
const { normalizeVertical } = require('../utils/normalize');
const {
  classifyInventoryOwnership,
  classifyOwnershipRow,
  evaluateColdOutboundEligibility,
  OWNERSHIP_KINDS,
} = require('./outboundInventory');

const RECOVERY_LOSS_REASONS = Object.freeze([
  'no_alternate_contact_found',
  'alternate_email_unverified',
  'alternate_email_invalid',
  'alternate_contact_suppressed',
  'alternate_contact_owned',
  'alternate_contact_already_attempted',
]);

function emptyAlternateContactTelemetry() {
  return {
    sameCompanyCandidatesAttempted: 0,
    alternateContactsResolved: 0,
    alternateContactsVerified: 0,
    alternateContactsRejected: 0,
    alternateContactsAddedToCleanInventory: 0,
    alternateContactLossReasons: Object.fromEntries(RECOVERY_LOSS_REASONS.map(k => [k, 0])),
  };
}

function mergeAlternateTelemetry(target, delta = {}) {
  if (!target || !delta) return target;
  for (const key of [
    'sameCompanyCandidatesAttempted',
    'alternateContactsResolved',
    'alternateContactsVerified',
    'alternateContactsRejected',
    'alternateContactsAddedToCleanInventory',
  ]) {
    target[key] = Number(target[key] || 0) + Number(delta[key] || 0);
  }
  const losses = delta.alternateContactLossReasons || {};
  for (const reason of RECOVERY_LOSS_REASONS) {
    target.alternateContactLossReasons[reason] = Number(target.alternateContactLossReasons[reason] || 0)
      + Number(losses[reason] || 0);
  }
  return target;
}

function recordAlternateLoss(telemetry, reason) {
  if (!telemetry || !reason) return;
  telemetry.alternateContactsRejected = Number(telemetry.alternateContactsRejected || 0) + 1;
  if (telemetry.alternateContactLossReasons[reason] != null) {
    telemetry.alternateContactLossReasons[reason] += 1;
  }
}

function canonicalProspectUnavailable(row, now = Date.now()) {
  if (!row) return true;
  if (canonicalOutboundEmailIneligibilityReason(row)) return true;
  if (row.do_not_contact === true) return true;
  if (row.email_verified !== true) return true;
  const ownership = classifyOwnershipRow(row, now);
  return ownership.kind !== OWNERSHIP_KINDS.CLEAR;
}

async function loadCompanyProspectRows(pool, { domain, companyName, companyId }) {
  const { rows } = await pool.query(`
    SELECT p.id, p.company_id, p.email, p.email_verified, p.email_status, p.do_not_contact,
      p.assigned_ao_id, p.closer_id, p.last_contacted_at, p.last_reply_at, p.vertical,
      p.service_area_match, c.name, c.domain, c.website,
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

async function collectBlockedEmails(pool, store, companyId, prospectRows = []) {
  const blocked = new Set(
    prospectRows.map(row => String(row.email || '').trim().toLowerCase()).filter(Boolean)
  );
  const attempted = await pool.query(`
    SELECT lower(email) AS email FROM acquisition_outbound_items
    WHERE tenant_id='10' AND company_id=$1 AND attempted_at IS NOT NULL AND email IS NOT NULL
  `, [String(companyId)]).catch(() => ({ rows: [] }));
  for (const row of attempted.rows) {
    if (row.email) blocked.add(String(row.email).toLowerCase());
  }
  return blocked;
}

async function resolveAlternateContacts({
  domain,
  discoveredEmail = null,
  excludeEmails = [],
  enrich = listProspeoContactsForDomain,
}) {
  const candidates = [];
  const discovered = String(discoveredEmail || '').trim().toLowerCase();
  if (discovered && !excludeEmails.includes(discovered)) {
    candidates.push({ email: discovered, contact: '', source: ['scout_discovery'] });
  }
  const prospeo = await enrich(domain, { excludeEmails: [...excludeEmails, discovered].filter(Boolean) });
  for (const row of prospeo || []) {
    const email = String(row.email || '').trim().toLowerCase();
    if (!email || excludeEmails.includes(email)) continue;
    candidates.push({
      email,
      contact: row.contact || '',
      title: row.title || null,
      source: ['prospeo'],
    });
  }
  const seen = new Set();
  return candidates.filter(row => {
    if (seen.has(row.email)) return false;
    seen.add(row.email);
    return true;
  });
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
}) {
  const insert = await pool.query(`
    INSERT INTO prospects (
      company_id, first_name, last_name, email, phone, status, source, icp_score, notes, vertical,
      client_id, service_area_match, discovery_method, website_url,
      email_verified, email_verification_method, verified_at, do_not_contact,
      email_status, verifier_response, verifier_checked_at
    ) VALUES ($1, NULL, NULL, $2, NULL, 'cold', 'scout', 70, $3, $4, 10, $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb, $14)
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
  ]);
  return insert.rows[0]?.id || null;
}

async function attemptSameCompanyAlternateRecovery(store, pool, {
  company,
  scoutContext = {},
  telemetry = null,
  now = Date.now(),
  verify = resolveEmailVerification,
  enrich = listProspeoContactsForDomain,
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

  const ownership = await classifyInventoryOwnership(store, { company: name, domain, website, email: company.email }, { pool, now });
  if (ownership.kind !== OWNERSHIP_KINDS.SAME_COMPANY_DIFFERENT_CONTACT) {
    return { ok: false, reason: 'not_same_company_candidate', telemetry: stats };
  }

  const companyId = ownership.companyId;
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

  const excludeEmails = await collectBlockedEmails(pool, store, resolvedCompanyId, rows);
  const alternates = await resolveAlternateContacts({
    domain,
    discoveredEmail: company.email || company.contactEmail,
    excludeEmails: [...excludeEmails],
    enrich,
  });
  if (!alternates.length) {
    recordAlternateLoss(stats, 'no_alternate_contact_found');
    return { ok: false, reason: 'no_alternate_contact_found', telemetry: stats };
  }

  for (const alternate of alternates) {
    stats.alternateContactsResolved += 1;
    if (!alternate.email || invalidOutreachEmailReason(alternate.email)) {
      recordAlternateLoss(stats, 'alternate_email_invalid');
      continue;
    }

    const verification = await verify(alternate.email, {
      email: alternate.email,
      contact: alternate.contact,
      source: alternate.source || ['same_company_recovery'],
      url: website,
    });
    if (verification.reject || verification.emailVerified !== true) {
      recordAlternateLoss(stats, verification.reject ? 'alternate_email_invalid' : 'alternate_email_unverified');
      continue;
    }
    stats.alternateContactsVerified += 1;

    const candidate = {
      candidateId: `same_company_${resolvedCompanyId}_${alternate.email}`,
      prospectId: null,
      companyId: String(resolvedCompanyId),
      company: name,
      domain,
      website,
      email: alternate.email,
    };
    const ownershipBlock = await store.candidateOwnership(candidate);
    if (ownershipBlock) {
      recordAlternateLoss(stats, 'alternate_contact_owned');
      continue;
    }
    const suppression = await store.suppression(candidate, '__max_inventory_buffer__');
    if (suppression) {
      recordAlternateLoss(stats, suppression === 'already_attempted'
        ? 'alternate_contact_already_attempted'
        : 'alternate_contact_suppressed');
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
      recordAlternateLoss(stats, 'alternate_contact_suppressed');
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
    });
    if (!prospectId) {
      recordAlternateLoss(stats, 'alternate_contact_owned');
      continue;
    }

    stats.alternateContactsAddedToCleanInventory += 1;
    return {
      ok: true,
      reason: 'alternate_contact_admitted',
      prospectId: String(prospectId),
      email: alternate.email,
      telemetry: stats,
    };
  }

  if (!stats.alternateContactsRejected) {
    recordAlternateLoss(stats, 'no_alternate_contact_found');
  }
  return { ok: false, reason: 'alternate_exhausted', telemetry: stats };
}

module.exports = {
  RECOVERY_LOSS_REASONS,
  emptyAlternateContactTelemetry,
  mergeAlternateTelemetry,
  attemptSameCompanyAlternateRecovery,
  canonicalProspectUnavailable,
  _test: {
    resolveAlternateContacts,
    loadCompanyProspectRows,
  },
};
