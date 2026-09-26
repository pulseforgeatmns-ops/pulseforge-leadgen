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

const NET_CLEAN_LOSS_BUCKETS = Object.freeze([
  'unclassifiable_vertical',
  'insufficient_business_fit',
  'owned_elsewhere',
  'same_company_different_contact',
  'enrichment_unresolved',
  'email_not_verified',
  'invalid_outreach_email',
]);

const LOSS_BUCKET_GUIDANCE = Object.freeze({
  unclassifiable_vertical: {
    exclusionCorrect: true,
    recoverableWithoutWeakeningSafety: false,
    producerFix: 'Improve vertical classification signals in discovery provenance and mission segment mapping.',
  },
  insufficient_business_fit: {
    exclusionCorrect: true,
    recoverableWithoutWeakeningSafety: false,
    producerFix: 'Tighten Places/query seeds to mission ICP; reject thin or contradictory business descriptions upstream.',
  },
  owned_elsewhere: {
    exclusionCorrect: true,
    recoverableWithoutWeakeningSafety: false,
    producerFix: 'Route to canonical prospect recovery instead of net-new admission when ownership is clear.',
  },
  same_company_different_contact: {
    exclusionCorrect: true,
    recoverableWithoutWeakeningSafety: true,
    producerFix: 'Promote alternate verified contact only through canonical same-company enrichment, not duplicate prospects.',
  },
  enrichment_unresolved: {
    exclusionCorrect: true,
    recoverableWithoutWeakeningSafety: true,
    producerFix: 'Retry enrichment with website/email discovery; widen Prospeo title filters only within ICP.',
  },
  email_not_verified: {
    exclusionCorrect: true,
    recoverableWithoutWeakeningSafety: true,
    producerFix: 'Complete Bouncer verification before promotion; do not bypass verification gates.',
  },
  invalid_outreach_email: {
    exclusionCorrect: true,
    recoverableWithoutWeakeningSafety: false,
    producerFix: 'Reject role/generic inboxes at enrichment; prefer owner or office-manager resolution.',
  },
});

function computeCleanInventoryGrowth(cleanBefore = [], cleanAfter = []) {
  const beforeIds = new Set((cleanBefore || []).map(row => String(row.prospectId || row.candidateId || row.id || '')).filter(Boolean));
  const afterIds = new Set((cleanAfter || []).map(row => String(row.prospectId || row.candidateId || row.id || '')).filter(Boolean));
  const newCleanInventoryAdded = [...afterIds].filter(id => !beforeIds.has(id)).length;
  const netCleanInventoryDelta = afterIds.size - beforeIds.size;
  return {
    preCycleCleanInventory: beforeIds.size,
    postCycleCleanInventory: afterIds.size,
    newCleanInventoryAdded,
    netCleanInventoryDelta,
  };
}

function buildReplenishmentLossBuckets({
  admission = {},
  enrichment = {},
  cleanExclusions = {},
} = {}) {
  const evaluated = Number(admission.evaluated || 0);
  const rejected = admission.rejected || {};
  const rawCounts = {
    unclassifiable_vertical: Number(rejected.unclassifiable_vertical || 0),
    insufficient_business_fit: Number(rejected.insufficient_business_fit || 0)
      + Number(rejected.segment_mismatch || 0)
      + Number(rejected.contradictory_business_type || 0),
    owned_elsewhere: Number(rejected.owned_elsewhere || 0)
      + Number(rejected.valid_ownership_collision || 0)
      + Number(rejected.stale_ownership || 0),
    same_company_different_contact: Number(rejected.same_company_different_contact || 0),
    enrichment_unresolved: Number(enrichment.unresolved || 0),
    email_not_verified: Number(cleanExclusions.email_not_verified || 0),
    invalid_outreach_email: Number(cleanExclusions.invalid_outreach_email || 0),
  };

  const buckets = NET_CLEAN_LOSS_BUCKETS.map(key => {
    const count = rawCounts[key] || 0;
    const guidance = LOSS_BUCKET_GUIDANCE[key] || {};
    return {
      bucket: key,
      count,
      pctOfEvaluated: rate(count, evaluated),
      exclusionCorrect: guidance.exclusionCorrect !== false,
      recoverableWithoutWeakeningSafety: guidance.recoverableWithoutWeakeningSafety === true,
      producerFix: guidance.producerFix || null,
    };
  });

  const auditedTotal = buckets.reduce((sum, row) => sum + row.count, 0);
  return { evaluated, buckets, auditedTotal };
}

function buildReplenishmentYield({
  admission = {},
  enrichment = {},
  recovered = 0,
  inventoryGrowth = null,
} = {}) {
  const candidatesDiscovered = Number(admission.discovered || 0);
  const evaluated = Number(admission.evaluated || 0);
  const fit = Number(admission.fit || 0);
  const admittedToEnrichment = Number(admission.admittedToEnrichment || 0);
  const emailResolved = Number(enrichment.emailResolved || 0);
  const emailVerified = Number(enrichment.emailVerified || 0);
  const newPromotions = Number(enrichment.promoted || 0);
  const recoveredExisting = Number(recovered || enrichment.recovered || 0);
  const newVerifiedPromotions = Math.min(newPromotions, Number(enrichment.emailVerified || 0));
  const growth = inventoryGrowth || {};
  const preCycleCleanInventory = Number(
    growth.preCycleCleanInventory ?? growth.preCycle ?? 0
  );
  const postCycleCleanInventory = Number(
    growth.postCycleCleanInventory ?? growth.postCycle ?? preCycleCleanInventory
  );
  const netCleanInventoryDelta = growth.netCleanInventoryDelta != null
    ? Number(growth.netCleanInventoryDelta)
    : postCycleCleanInventory - preCycleCleanInventory;
  const newCleanInventoryAdded = growth.newCleanInventoryAdded != null
    ? Number(growth.newCleanInventoryAdded)
    : Math.max(0, netCleanInventoryDelta);

  return {
    candidatesDiscovered,
    evaluated,
    fit,
    admittedToEnrichment,
    emailResolved,
    emailVerified,
    newPromotions,
    newVerifiedPromotions,
    recoveredExisting,
    newCleanInventoryAdded,
    netCleanInventoryDelta,
    preCycleCleanInventory,
    postCycleCleanInventory,
    promoted: newPromotions,
    recovered: recoveredExisting,
    cleanInventoryAdded: newCleanInventoryAdded,
    rates: {
      fitRate: rate(fit, evaluated),
      enrichmentAdmissionRate: rate(admittedToEnrichment, fit),
      contactResolutionRate: rate(emailResolved, admittedToEnrichment),
      verificationRate: rate(emailVerified, emailResolved),
      cleanInventoryYield: rate(newCleanInventoryAdded, evaluated),
      netCleanInventoryYield: rate(Math.max(0, netCleanInventoryDelta), evaluated),
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
  const empty = {
    lastReplenishmentAttemptAt: null,
    lastInventoryGrowthAt: null,
    lastNewPromotionAt: null,
    lastRecoveryAt: null,
    lastSuccessfulReplenishmentAt: null,
    lastSuccessfulPromotionAt: null,
  };
  if (!pool?.query) return empty;
  const events = await pool.query(`
    SELECT
      max(created_at) FILTER (
        WHERE event_type IN ('max_outbound_control','inventory_replenished')
          AND COALESCE((payload->>'scoutInvoked')::boolean,false) = true
      ) AS last_attempt,
      max(created_at) FILTER (
        WHERE event_type IN ('max_outbound_control','inventory_replenished')
          AND COALESCE((payload->>'netCleanInventoryDelta')::int,0) > 0
      ) AS last_inventory_growth,
      max(created_at) FILTER (
        WHERE event_type IN ('max_outbound_control','inventory_replenished')
          AND COALESCE((payload->>'newPromotions')::int,0) > 0
      ) AS last_new_promotion,
      max(created_at) FILTER (
        WHERE event_type IN ('max_outbound_control','inventory_replenished')
          AND COALESCE((payload->>'recoveredExisting')::int,0) > 0
      ) AS last_recovery
    FROM acquisition_outbound_events
    WHERE tenant_id='10'
  `).catch(() => ({ rows: [{}] }));
  const row = events.rows[0] || {};
  const lastInventoryGrowthAt = row.last_inventory_growth
    ? new Date(row.last_inventory_growth).toISOString()
    : null;
  const lastNewPromotionAt = row.last_new_promotion
    ? new Date(row.last_new_promotion).toISOString()
    : null;
  const lastRecoveryAt = row.last_recovery
    ? new Date(row.last_recovery).toISOString()
    : null;
  const lastReplenishmentAttemptAt = row.last_attempt
    ? new Date(row.last_attempt).toISOString()
    : null;
  return {
    lastReplenishmentAttemptAt,
    lastInventoryGrowthAt,
    lastNewPromotionAt,
    lastRecoveryAt,
    lastSuccessfulReplenishmentAt: lastInventoryGrowthAt,
    lastSuccessfulPromotionAt: lastNewPromotionAt,
  };
}

module.exports = {
  FUNNEL_STAGES,
  OWNERSHIP_KINDS,
  NET_CLEAN_LOSS_BUCKETS,
  STALE_OWNERSHIP_MS,
  emptyFunnel,
  buildReplenishmentYield,
  buildReplenishmentLossBuckets,
  computeCleanInventoryGrowth,
  mergeFunnel,
  rejectedCount,
  evaluateColdOutboundEligibility,
  classifyOwnershipRow,
  classifyInventoryOwnership,
  loadScoutFunnelStock,
  loadInventoryTimestamps,
};
