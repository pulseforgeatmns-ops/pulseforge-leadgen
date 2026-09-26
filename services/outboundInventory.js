'use strict';

/**
 * Governed outbound inventory: funnel stock, replenishment yield,
 * ownership collision classification, and cold-outbound eligibility.
 *
 * Buyer-readiness UNKNOWN is not an admission requirement.
 */

const { canonicalOutboundEmailIneligibilityReason } = require('../utils/canonicalEmailEligibility');
const { normalizeDomain } = require('../utils/canonicalEmailEligibility');

const FUNNEL_STAGES = Object.freeze([
  'discovered',
  'fit',
  'admittedToEnrichment',
  'enrichmentPending',
  'promotedVerified',
  'unresolved',
  'permanentlyRejected',
]);

const OWNERSHIP_KINDS = Object.freeze({
  CLEAR: 'clear',
  VALID_COLLISION: 'valid_ownership_collision',
  STALE: 'stale_ownership',
  SAME_COMPANY_DIFFERENT_CONTACT: 'same_company_different_contact',
  ALREADY_USABLE_CANONICAL: 'already_usable_canonical',
  PRIOR_CONTACT: 'prior_contact_or_human_owned',
  AO_OWNED: 'ao_owned_alias',
});

const STALE_OWNERSHIP_MS = 180 * 24 * 60 * 60 * 1000;

function emptyFunnel() {
  return Object.fromEntries(FUNNEL_STAGES.map(stage => [stage, 0]));
}

function rate(numerator, denominator) {
  const n = Number(numerator || 0);
  const d = Number(denominator || 0);
  if (!d) return 0;
  return Number((n / d).toFixed(4));
}

function buildReplenishmentYield({
  admission = {},
  enrichment = {},
  recovered = 0,
} = {}) {
  const candidatesDiscovered = Number(admission.discovered || 0);
  const evaluated = Number(admission.evaluated || 0);
  const fit = Number(admission.fit || 0);
  const admittedToEnrichment = Number(admission.admittedToEnrichment || 0);
  const emailResolved = Number(enrichment.emailResolved || 0);
  const emailVerified = Number(enrichment.emailVerified || 0);
  const promoted = Number(enrichment.promoted || 0);
  const recoveredCount = Number(recovered || enrichment.recovered || 0);
  const cleanInventoryAdded = promoted + recoveredCount;

  return {
    candidatesDiscovered,
    evaluated,
    fit,
    admittedToEnrichment,
    emailResolved,
    emailVerified,
    promoted,
    recovered: recoveredCount,
    cleanInventoryAdded,
    rates: {
      fitRate: rate(fit, evaluated),
      enrichmentAdmissionRate: rate(admittedToEnrichment, fit),
      contactResolutionRate: rate(emailResolved, admittedToEnrichment),
      verificationRate: rate(emailVerified, emailResolved),
      cleanInventoryYield: rate(cleanInventoryAdded, evaluated),
    },
  };
}

function mergeFunnel(stock = {}, cycle = {}) {
  const next = emptyFunnel();
  for (const stage of FUNNEL_STAGES) {
    next[stage] = Math.max(0, Number(stock[stage] || 0));
  }
  next.discovered = Math.max(next.discovered, Number(cycle.discovered || 0));
  next.fit = Math.max(next.fit, Number(cycle.fit || 0));
  if (cycle.admittedToEnrichment != null) {
    next.admittedToEnrichment = Number(cycle.admittedToEnrichment || 0);
  }
  if (cycle.promotedVerified != null) {
    next.promotedVerified = Number(cycle.promotedVerified || 0);
  }
  if (cycle.permanentlyRejected != null) {
    next.permanentlyRejected = Number(cycle.permanentlyRejected || 0);
  }
  return next;
}

function rejectedCount(admission = {}) {
  const rejected = admission.rejected || {};
  return Object.values(rejected).reduce((sum, n) => sum + Number(n || 0), 0);
}

/**
 * Buyer-readiness is prioritization intelligence, never an outbound gate.
 * UNKNOWN is eligible for cold outbound when the other fail-closed
 * safeguards are clear.
 */
function evaluateColdOutboundEligibility({
  businessFit = 'qualified',
  geography = 'in_scope',
  contactVerified = false,
  dnc = false,
  suppression = null,
  ownership = 'clear',
  buyerReadiness = 'unknown',
  emailReason = null,
} = {}) {
  if (businessFit !== 'qualified') return { eligible: false, reason: 'business_fit' };
  if (geography !== 'in_scope') return { eligible: false, reason: 'geography' };
  if (emailReason) return { eligible: false, reason: emailReason };
  if (contactVerified !== true) return { eligible: false, reason: 'contact_not_verified' };
  if (dnc === true) return { eligible: false, reason: 'dnc' };
  if (suppression) return { eligible: false, reason: suppression };
  if (ownership && ownership !== OWNERSHIP_KINDS.CLEAR && ownership !== 'clear') {
    return { eligible: false, reason: ownership };
  }
  return {
    eligible: true,
    reason: null,
    buyerReadiness: buyerReadiness || 'unknown',
    prioritizationOnly: buyerReadiness === 'unknown' || buyerReadiness == null,
  };
}

function classifyOwnershipRow(row, now = Date.now()) {
  if (!row) return { kind: OWNERSHIP_KINDS.CLEAR, reason: null };
  const hasHumanOwner = Boolean(row.assigned_ao_id || row.closer_id || row.has_ao_task);
  const priorTouch = Boolean(row.prior_touch || row.last_reply_at || row.last_contacted_at);
  const lastContact = row.last_contacted_at ? +new Date(row.last_contacted_at) : 0;
  const stale = priorTouch && !hasHumanOwner && lastContact > 0 && (now - lastContact) > STALE_OWNERSHIP_MS;

  if (hasHumanOwner) {
    return { kind: OWNERSHIP_KINDS.VALID_COLLISION, reason: row.assigned_ao_id ? 'ao_owned' : 'human_owned' };
  }
  if (stale) {
    return { kind: OWNERSHIP_KINDS.STALE, reason: 'stale_prior_contact' };
  }
  if (priorTouch) {
    return { kind: OWNERSHIP_KINDS.VALID_COLLISION, reason: 'prior_contact' };
  }
  return { kind: OWNERSHIP_KINDS.CLEAR, reason: null };
}

async function classifyInventoryOwnership(store, candidate = {}, opts = {}) {
  const pool = opts.pool || store?.pool;
  if (!pool) {
    const blocked = store?.candidateOwnership ? await store.candidateOwnership(candidate) : null;
    return blocked
      ? { kind: OWNERSHIP_KINDS.VALID_COLLISION, reason: blocked, recoverable: false }
      : { kind: OWNERSHIP_KINDS.CLEAR, reason: null, recoverable: false };
  }

  const domain = normalizeDomain(candidate.domain || candidate.website || candidate.email);
  const email = String(candidate.email || '').trim().toLowerCase();
  const company = String(candidate.company || candidate.name || '').trim();
  const { rows } = await pool.query(`
    SELECT p.id, p.company_id, p.email, p.email_verified, p.email_status, p.do_not_contact,
      p.assigned_ao_id, p.closer_id, p.last_contacted_at, p.last_reply_at,
      p.service_area_match, p.vertical, c.name, c.domain, c.website,
      EXISTS(SELECT 1 FROM ao_prospect_tasks t WHERE t.client_id=10 AND t.prospect_id=p.id) AS has_ao_task,
      EXISTS(SELECT 1 FROM touchpoints t WHERE t.client_id=10 AND t.prospect_id=p.id
        AND t.action_type IN ('email_sent','sent','outbound_email','call','call_attempt','inbound_reply','reply','email_reply','reply_received')) AS prior_touch
    FROM prospects p
    JOIN companies c ON c.id=p.company_id AND c.client_id=p.client_id
    WHERE p.client_id=10
      AND (
        ($1::text IS NOT NULL AND lower(p.email)=lower($1))
        OR ($2::text IS NOT NULL AND lower(c.domain)=lower($2))
        OR ($3::text <> '' AND lower(trim(c.name))=lower(trim($3)))
      )
    ORDER BY p.email_verified DESC NULLS LAST, p.updated_at DESC NULLS LAST
  `, [email || null, domain || null, company]);

  if (!rows.length) {
    const blocked = store?.candidateOwnership ? await store.candidateOwnership(candidate) : null;
    return blocked
      ? { kind: OWNERSHIP_KINDS.VALID_COLLISION, reason: blocked, recoverable: false }
      : { kind: OWNERSHIP_KINDS.CLEAR, reason: null, recoverable: false };
  }

  const exactEmail = email
    ? rows.find(row => String(row.email || '').toLowerCase() === email)
    : null;
  const usable = rows.find(row => {
    if (canonicalOutboundEmailIneligibilityReason(row)) return false;
    if (row.do_not_contact === true) return false;
    const classified = classifyOwnershipRow(row, opts.now);
    return classified.kind === OWNERSHIP_KINDS.CLEAR;
  });
  if (usable || (exactEmail && !canonicalOutboundEmailIneligibilityReason(exactEmail) && classifyOwnershipRow(exactEmail, opts.now).kind === OWNERSHIP_KINDS.CLEAR)) {
    const prospect = usable || exactEmail;
    return {
      kind: OWNERSHIP_KINDS.ALREADY_USABLE_CANONICAL,
      reason: 'existing_canonical_prospect',
      recoverable: true,
      prospectId: String(prospect.id),
      companyId: String(prospect.company_id || ''),
    };
  }

  if (exactEmail) {
    const classified = classifyOwnershipRow(exactEmail, opts.now);
    return {
      ...classified,
      recoverable: false,
      prospectId: String(exactEmail.id),
    };
  }

  const owned = rows.map(row => classifyOwnershipRow(row, opts.now))
    .find(row => row.kind === OWNERSHIP_KINDS.VALID_COLLISION || row.kind === OWNERSHIP_KINDS.STALE);
  if (owned) {
    return { ...owned, recoverable: false, sameCompany: true };
  }

  return {
    kind: OWNERSHIP_KINDS.SAME_COMPANY_DIFFERENT_CONTACT,
    reason: 'existing_company_without_blocking_ownership',
    recoverable: false,
    sameCompany: true,
  };
}

async function loadScoutFunnelStock(pool) {
  const funnel = emptyFunnel();
  if (!pool?.query) return funnel;

  const unenriched = await pool.query(`
    SELECT
      count(*)::int AS discovered,
      count(*) FILTER (WHERE COALESCE(enrichment_attempts,0)=0)::int AS admitted,
      count(*) FILTER (
        WHERE COALESCE(enrichment_attempts,0)>0
          AND COALESCE(enrichment_attempts,0)<3
      )::int AS pending,
      count(*) FILTER (WHERE COALESCE(enrichment_attempts,0)>=3)::int AS unresolved
    FROM scout_unenriched
    WHERE client_id=10
      AND source='max_buffer_replenishment'
  `).catch(() => ({ rows: [{}] }));

  const promoted = await pool.query(`
    SELECT count(*)::int AS n
    FROM prospects
    WHERE client_id=10
      AND email IS NOT NULL
      AND email_verified=true
      AND COALESCE(do_not_contact,false)=false
  `).catch(() => ({ rows: [{}] }));

  const row = unenriched.rows[0] || {};
  funnel.discovered = Number(row.discovered || 0);
  funnel.admittedToEnrichment = Number(row.admitted || 0);
  funnel.enrichmentPending = Number(row.pending || 0);
  funnel.unresolved = Number(row.unresolved || 0);
  funnel.promotedVerified = Number(promoted.rows[0]?.n || 0);
  return funnel;
}

async function loadInventoryTimestamps(pool) {
  if (!pool?.query) {
    return { lastSuccessfulReplenishmentAt: null, lastSuccessfulPromotionAt: null };
  }
  const events = await pool.query(`
    SELECT
      max(created_at) FILTER (
        WHERE event_type IN ('max_outbound_control','inventory_replenished')
          AND COALESCE((payload->>'scoutQueued')::int,0)
            + COALESCE((payload->>'scoutPromoted')::int,0)
            + COALESCE((payload->>'eligible')::int,0) > 0
      ) AS last_replenishment,
      max(created_at) FILTER (
        WHERE event_type IN ('max_outbound_control','inventory_replenished')
          AND COALESCE((payload->>'scoutPromoted')::int,0) > 0
      ) AS last_promotion
    FROM acquisition_outbound_events
    WHERE tenant_id='10'
  `).catch(() => ({ rows: [{}] }));
  const row = events.rows[0] || {};
  return {
    lastSuccessfulReplenishmentAt: row.last_replenishment ? new Date(row.last_replenishment).toISOString() : null,
    lastSuccessfulPromotionAt: row.last_promotion ? new Date(row.last_promotion).toISOString() : null,
  };
}

module.exports = {
  FUNNEL_STAGES,
  OWNERSHIP_KINDS,
  STALE_OWNERSHIP_MS,
  emptyFunnel,
  buildReplenishmentYield,
  mergeFunnel,
  rejectedCount,
  evaluateColdOutboundEligibility,
  classifyOwnershipRow,
  classifyInventoryOwnership,
  loadScoutFunnelStock,
  loadInventoryTimestamps,
};
