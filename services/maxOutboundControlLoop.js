'use strict';
const { anchorOutreachReviewSql } = require('../utils/anchorOutreachReview');
const { withResearchProviderScope, assertResearchProviderAllowed, rethrowResearchProviderControl, isResearchProviderControlError } = require('../utils/anchorResearchProviderPause');
function rethrowUnlessResearchControl(error) { if (!isResearchProviderControlError(error)) throw error; }

const { loadRecoveryState } = require('./anchorEmailRecovery');
const { assertLegacyPreparation } = require('./anchorEmailRecoveryIntegration');

const { isSubstralPreparationGrant, substralPreparationGrantReason } = require('./substralPreparationGrant');
const { admissionReceiptDiagnostic, providerReceiptDiagnostic, scoutRampDiagnostic, controlCycleDiagnostic, evidenceProgressionDiagnostic, emitDiagnostic } = require('./outboundControlDiagnostics');
const { governedContactReason, founderFirst, contactEvidence } = require('../utils/governedContactEligibility');

const { canonicalOutboundEmailIneligibilityReason, normalizeDomain } = require('../utils/canonicalEmailEligibility');
const { normalizeVertical } = require('../utils/normalize');
const {
  ENRICHABLE_SCOUT_VERTICALS,
  MISSION_SCOPED_FACILITIES_VERTICALS,
  CANONICAL_ENRICHABLE_SCOUT_VERTICALS,
  evaluateReplenishmentAdmission,
  restaurantBusinessScopeReason,
  formatProvenanceNotes,
  createReplenishmentAdmissionCounters,
  recordReplenishmentRejection,
  missionCompatibleVerticals,
} = require('../utils/replenishmentVertical');
const { GovernedOutboundStore } = require('./governedOutboundStore');
const { adapters: createGovernedAdapters } = require('./governedOutboundAdapters');
const {
  assessOperatingCapacity,
  computeScheduleLimitedCapacity,
} = require('../packages/emmett-outbound/OperatingCapacity');
const { resolveOperatorDelegatedMaximumDailyCapacity } = require('../packages/emmett-outbound/OperatorDelegatedCapacity');
const {
  buildReplenishmentYield,
  buildReplenishmentLossBuckets,
  computeCleanInventoryGrowth,
  emptyFunnel,
  evaluateColdOutboundEligibility,
  classifyInventoryOwnership,
  OWNERSHIP_KINDS,
  loadScoutFunnelStock,
  loadInventoryTimestamps,
  rejectedCount,
} = require('./outboundInventory');
const {
  isProspectServiceAreaConfirmed,
  resolveMissionAllowedCities,
} = require('../utils/missionGeography');
const {
  emptyAlternateContactTelemetry,
  mergeAlternateTelemetry,
  clampCohortCounters,
  attemptSameCompanyAlternateRecovery,
} = require('./sameCompanyContactRecovery');
const {
  retryUnverifiedEmails,
  emptyVerificationRetryTelemetry,
  mergeVerificationRetryTelemetry,
  verificationRetryFailureReasons,
} = require('./emailVerificationRecovery');
const {
  evaluatePreparationRefill,
  remainingDispatchCapacity,
  remainingScheduleSlots,
  observabilityFromRefill,
  finalizePreparationObservability,
} = require('./governedOutboundRefill');
const { clock, hash, fail } = require('../packages/acquisition-mission/DailyOutboundPolicy');
const {
  createGovernedOutboundContext,
  resolveGovernedAuthorizationTenantId,
  resolveReplenishmentTenantContext,
} = require('./governedOutboundContext');
const { governedOutboundEnabledForTenant, governedOutboundPreparationEnabledForTenant } = require('./governedOutboundTenant');
const { isSmallBusinessOwnerSegment } = require('../utils/canonicalBusinessTaxonomy');

function preparationGrantActive(store, program) {
  if (program?.mode !== 'active' && !isSubstralPreparationGrant(program)) return false;
  const tid = String(store?.tenantId ?? program?.tenant_id ?? '').trim();
  if (!tid || !['1', '10', '13', '17'].includes(tid)) return true;
  return governedOutboundPreparationEnabledForTenant(tid);
}

const DEFAULT_TARGET_DAYS = 3;
const DEFAULT_ENRICHMENT_BATCH = 5;
const MAX_ENRICHMENT_BATCHES_PER_CYCLE = 3;
const SCOUT_DISCOVERY_BACKOFF_THRESHOLD = 3;
const SCOUT_DISCOVERY_BACKOFF_MINUTES = 60;
const SCOUT_DISCOVERY_MAX_GENERATION = 3;
const REPLENISHMENT_TAXONOMY_VERSION = 3;

function boundedInt(value, fallback, min = 1, max = 100) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, Math.trunc(n)));
}

function resolveOperatingCapacity({
  operatingCapacity = null,
  dailyCap,
  emmettCapacity,
  assessed = null,
  sentToday = 0,
  totalAttempted = 0,
  policy = null,
  now = new Date(),
} = {}) {
  if (operatingCapacity?.planningDailyCapacity != null) {
    return operatingCapacity;
  }
  if (operatingCapacity && (
    operatingCapacity.dispatchCapacityNow != null
    || operatingCapacity.dispatchableDailyCapacity != null
    || operatingCapacity.effectiveDailyCapacity != null
  )) {
    return operatingCapacity;
  }
  const grantPolicy = policy || { dailyCap };
  return assessOperatingCapacity({
    assessed: assessed || {
      capacity: { recommended: emmettCapacity },
      governor: { outcome: 'proceed', halt: false },
      health: { score: 0 },
    },
    policy: grantPolicy,
    sentToday,
    totalAttempted,
    emmettCapacity,
    now,
    schedule: grantPolicy.startHour != null || grantPolicy.spacingMinutes != null
      ? {
        allowedSendWindow: {
          startHour: grantPolicy.startHour ?? 9,
          endHour: grantPolicy.endHour ?? 17,
          timezone: grantPolicy.timeZone || 'America/New_York',
        },
        minSpacingMinutes: grantPolicy.spacingMinutes ?? grantPolicy.minSpacingMinutes ?? 60,
      }
      : undefined,
  });
}

function buildControlPlan({
  dailyCap,
  emmettCapacity,
  operatingCapacity = null,
  assessed = null,
  sentToday = 0,
  totalAttempted = 0,
  cleanInventory = 0,
  targetDays = DEFAULT_TARGET_DAYS,
  policy = null,
  now = new Date(),
}) {
  const operating = resolveOperatingCapacity({
    operatingCapacity,
    dailyCap,
    emmettCapacity,
    assessed,
    sentToday,
    totalAttempted,
    policy,
    now,
  });
  const authorizationLimitedCapacity = Math.max(0, Number(
    operating.authorizationLimitedCapacity ?? operating.effectiveDailyCapacity ?? 0
  ));
  const scheduleLimitedCapacity = operating.scheduleLimitedCapacity != null
    ? Math.max(0, Number(operating.scheduleLimitedCapacity))
    : (operating.allowedSendWindow && operating.minSpacingMinutes != null
      ? computeScheduleLimitedCapacity({
        allowedSendWindow: operating.allowedSendWindow,
        minSpacingMinutes: operating.minSpacingMinutes,
        dispatchDayAllowed: operating.dispatchDayAllowed !== false,
      })
      : 0);
  const nextEligibleScheduleCapacity = Math.max(0, Number(
    operating.nextEligibleScheduleCapacity ?? scheduleLimitedCapacity
  ));
  const dispatchCapacityNow = Math.max(0, Number(
    operating.dispatchCapacityNow ?? operating.dispatchableDailyCapacity ?? 0
  ));
  let planningDailyCapacity = Math.max(0, Number(operating.planningDailyCapacity ?? NaN));
  if (!Number.isFinite(planningDailyCapacity)) {
    const legacyDispatchable = Math.max(0, Number(operating.dispatchableDailyCapacity ?? 0));
    if (legacyDispatchable > 0) {
      planningDailyCapacity = Math.min(authorizationLimitedCapacity, legacyDispatchable);
    } else if (authorizationLimitedCapacity > 0 && nextEligibleScheduleCapacity > 0) {
      planningDailyCapacity = Math.min(authorizationLimitedCapacity, nextEligibleScheduleCapacity);
    } else {
      planningDailyCapacity = Math.max(0, authorizationLimitedCapacity);
    }
  }
  const dispatchableDailyCapacity = dispatchCapacityNow;
  const buffer = planningDailyCapacity * boundedInt(targetDays, DEFAULT_TARGET_DAYS, 1, 7);
  const target = policy?.totalCap != null ? Math.min(buffer, Math.max(0, policy.totalCap - Number(totalAttempted || 0))) : buffer;
  const clean = Math.max(0, Number(cleanInventory || 0));
  const deficit = Math.max(0, target - clean);
  const todayRemaining = Math.max(0, dispatchCapacityNow - Math.max(0, Number(sentToday || 0)));

  let state = 'healthy';
  if (planningDailyCapacity <= 0) state = 'delivery_halted';
  else if (clean < planningDailyCapacity) state = 'critical';
  else if (clean < target) state = 'replenish';

  return {
    state,
    safeDailyCapacity: planningDailyCapacity,
    planningDailyCapacity,
    dispatchCapacityNow,
    dispatchableDailyCapacity,
    authorizationLimitedCapacity,
    scheduleLimitedCapacity,
    nextEligibleScheduleCapacity,
    effectiveDailyCapacity: authorizationLimitedCapacity,
    recommendedSafeDailyCapacity: Number(operating.recommendedSafeDailyCapacity || 0),
    limitingFactor: operating.limitingFactor || null,
    capacityLimitingAuthority: operating.capacityLimitingAuthority || null,
    capacityReason: operating.capacityReason || null,
    operatorDelegatedMaximumDailyCapacity: operating.operatorDelegatedMaximumDailyCapacity
      ?? (policy ? resolveOperatorDelegatedMaximumDailyCapacity(policy) : null),
    governor: operating.governor || null,
    healthScore: operating.healthScore ?? null,
    todayRemaining,
    targetDays: boundedInt(targetDays, DEFAULT_TARGET_DAYS, 1, 7),
    targetInventory: target,
    cleanInventory: clean,
    deficit,
    shouldReplenish: deficit > 0 && planningDailyCapacity > 0,
    dispatchUnavailableNow: dispatchCapacityNow <= 0,
  };
}

function firstPresent(...values) {
  for (const value of values) {
    if (value == null) continue;
    const text = String(value).trim();
    if (text) return text;
  }
  return null;
}

function resolveScoutRampAllowedCities(scope = {}) {
  return resolveMissionAllowedCities({
    missionCities: scope.cities,
    cities: scope.cities,
    region: scope.region,
    geography: {
      cities: scope.cities,
      region: scope.region,
      scope: scope.scope,
    },
  });
}

function sourceScope(source) {
  const payload = source?.mission || source?.payload || source || {};
  const structured = payload.structuredMission || {};
  const market = structured.market || {};
  const geography = structured.geography || {};
  const offer = structured.offer || {};
  return {
    segment: normalizeVertical(market.segment || payload.targetSegment || source?.target_segment || ''),
    industry: normalizeVertical(market.industry || ''),
    eligibleSubsegments: Array.isArray(market.eligibleSubsegments) ? market.eligibleSubsegments.map(normalizeVertical).filter(Boolean) : [],
    region: geography.region || null,
    scope: geography.scope || null,
    cities: Array.isArray(geography.cities) ? geography.cities.map(x => String(x).toLowerCase()) : [],
    commercialCapability: firstPresent(
      market.capability,
      market.commercialCapability,
      offer.capability,
      payload.commercialCapability,
    ),
    businessType: firstPresent(market.businessType, payload.businessType),
    offerId: firstPresent(offer.id, offer.offerId, payload.offerId),
  };
}

function segmentAliases(scope) {
  if (scope.eligibleSubsegments?.length) {
    const aliases = new Set();
    for (const segment of scope.eligibleSubsegments) {
      for (const alias of segmentAliases({ segment })) aliases.add(alias);
    }
    return aliases;
  }
  const aliases = new Set([scope.segment, scope.industry].filter(Boolean));
  if (scope.segment === 'short_term_rental' || scope.segment === 'short_term_rental_operators') {
    ['short_term_rental', 'str_manager', 'property_manager', 'property_management', 'hospitality']
      .forEach(x => aliases.add(x));
  }
  if (['property_manager', 'property_management'].includes(scope.segment)) {
    ['property_manager', 'property_management', 'str_manager'].forEach(x => aliases.add(x));
  }
  if (scope.segment === 'restaurant_foh') aliases.add('restaurant');
  if (scope.segment === 'managed_facilities') {
    missionCompatibleVerticals({ missionSegment: scope.segment }).forEach(x => aliases.add(x));
  }
  if (isSmallBusinessOwnerSegment(scope.segment)) {
    missionCompatibleVerticals({ missionSegment: scope.segment }).forEach(x => aliases.add(x));
  }
  return aliases;
}

function confirmedServiceAreaMatch(value) {
  if (value === true) return true;
  if (value === false || value == null) return false;
  return String(value).trim().length > 0;
}

function prospectMissionVertical(row, scope) {
  const vertical = normalizeVertical(row.vertical || row.industry || '');
  if (vertical && vertical !== 'unknown') return vertical;
  const { verticalFromKnowledgeContent } = require('./existingInventoryRecovery');
  const fromKnowledge = verticalFromKnowledgeContent(row.knowledge_content || {});
  if (fromKnowledge) return fromKnowledge;
  if (['small_business_owner', 'small_business_owners', 'founder_led_smb', 'founder_led_small_business']
    .includes(scope.segment)) {
    return scope.segment;
  }
  return vertical;
}

function missionCandidateReason(row, scope, tenantId = null) {
  if (String(tenantId || row.client_id || '') === '1') {
    const offer = require('../utils/pulseforgeCashflowOffer');
    if (scope.offerId !== offer.PULSEFORGE_CASHFLOW_OFFER_ID) return offer.SOURCE_REVIEW_BLOCKER;
    return offer.pulseforgeTargetReason(row);
  }
  if (!isProspectServiceAreaConfirmed(row, scope)) return 'service_area_not_confirmed';
  const vertical = prospectMissionVertical(row, scope);
  const aliases = segmentAliases(scope);
  if (aliases.size && (!vertical || !aliases.has(vertical))) return 'mission_segment_mismatch';
  if (vertical === 'restaurant') {
    return restaurantBusinessScopeReason({ ...row, name: row.company_name || row.name }, {
      missionSegments: scope.eligibleSubsegments?.length ? scope.eligibleSubsegments : [scope.segment],
    });
  }
  return null;
}

async function loadSource(pool, program, tenantId) {
  const tid = resolveGovernedAuthorizationTenantId({ tenantId, program });
  return (await pool.query(
    'SELECT id,objective,target_segment,payload FROM acquisition_missions WHERE tenant_id=$1 AND id=$2',
    [tid, program.source_mission_id]
  )).rows[0] || null;
}

async function loadCleanInventory(pool, store, source, clientId, policy = {}) {
  const cid = Number(clientId ?? store.clientId);
  if (!Number.isFinite(cid)) {
    throw Object.assign(new Error('governed_outbound_tenant_required'), { code: 'governed_outbound_tenant_required' });
  }
  const scope = sourceScope(source);
  const websiteAssessmentSelect = cid === 1
    ? `, woa.id AS website_opportunity_assessment_id,
      woa.payload AS website_opportunity_payload,
      woa.opportunity_score AS website_opportunity_score,
      woa.recommended_action AS website_recommended_action`
    : '';
  const websiteAssessmentJoin = cid === 1
    ? `LEFT JOIN LATERAL (
        SELECT a.* FROM website_opportunity_assessments a
        WHERE a.client_id=p.client_id
          AND (a.prospect_id=p.id OR a.domain=c.domain)
        ORDER BY (a.prospect_id=p.id) DESC, a.updated_at DESC
        LIMIT 1
      ) woa ON true`
    : '';
  const { rows } = await pool.query(`
    SELECT p.*, c.name AS company_name, c.domain AS company_domain, c.website AS company_website,
      c.industry AS industry, c.location AS company_location${websiteAssessmentSelect}
    FROM prospects p
    JOIN companies c ON c.id=p.company_id AND c.client_id=p.client_id
    ${websiteAssessmentJoin}
    WHERE p.client_id=$1
      AND p.email IS NOT NULL
      AND COALESCE(p.do_not_contact,false)=false
        AND COALESCE((to_jsonb(p)->>'ao_outreach_review_required')::boolean,false) = false AND ${anchorOutreachReviewSql('p')}
    ORDER BY p.updated_at DESC NULLS LAST, p.id
  `, [cid]);

  const knowledge = await require('./acquisitionMissionInventory').loadKnowledgeInventory(pool, { ...(source?.mission || source?.payload || source), tenantId: String(cid) }, policy);
  const qualifiedKnowledge = new Set(knowledge.filter(r => !r.qualificationReason).map(r => String(r.id)));
  const knowledgeByProspectId = new Map(knowledge.map(r => [String(r.id), r]));
  const clean = [];
  const seenCompanies = new Set();
  const seenCompanyNames = new Set();
  const seenEmails = new Set();
  const excluded = [];
  const exclusionCounts = {};
  const bump = reason => {
    if (!reason) return;
    exclusionCounts[reason] = (exclusionCounts[reason] || 0) + 1;
  };
  for (const row of rows.sort(founderFirst)) {
    const knowledgeRow = knowledgeByProspectId.get(String(row.id));
    const scopedRow = knowledgeRow
      ? { ...row, knowledge_content: knowledgeRow.knowledge_content }
      : row;
    // Reviewed knowledge must not bypass explicit restaurant source/geography boundaries.
    const restaurantCandidate = prospectMissionVertical(scopedRow, scope) === 'restaurant'
      && (cid === 10 || (scope.eligibleSubsegments?.length ? scope.eligibleSubsegments : [scope.segment])
        .some(segment => ['restaurant', 'restaurant_foh'].includes(segment)));
    const missionReason = qualifiedKnowledge.has(String(row.id)) && !restaurantCandidate
      ? null : missionCandidateReason(scopedRow, scope, cid);
    const emailReason = missionReason ? null : governedContactReason(row, policy);
    const candidate = {
      candidateId: String(row.id),
      prospectId: String(row.id),
      companyId: String(row.company_id || ''),
      company: row.company_name,
      location: row.company_location || row.location,
      industry: row.industry,
      vertical: row.vertical,
      domain: row.company_domain,
      website: row.company_website,
      email: String(row.email || '').toLowerCase(),
      contactClassification: contactEvidence(row).classification,
      assessmentId: row.website_opportunity_assessment_id || null,
      assessmentPayload: row.website_opportunity_payload || null,
      opportunityScore: row.website_opportunity_score ?? null,
      recommendedAction: row.website_recommended_action || null,
    };
    const ownership = missionReason || emailReason
      ? null
      : await store.candidateOwnership(candidate);
    const suppression = missionReason || emailReason || ownership
      ? null
      : await store.suppression(candidate, '__max_inventory_buffer__');
    const eligibility = evaluateColdOutboundEligibility({
      businessFit: missionReason ? 'not_qualified' : 'qualified',
      geography: missionReason === 'service_area_not_confirmed' ? 'out_of_scope' : 'in_scope',
      contactVerified: !emailReason,
      dnc: row.do_not_contact === true,
      suppression,
      ownership: ownership || 'clear',
      buyerReadiness: row.buyer_readiness || row.buyerReadiness || 'unknown',
      emailReason,
    });
    const companyNameKey = require('../utils/companyIdentityName').companyIdentityNameKey(candidate.company);
    const duplicate = eligibility.eligible && (seenCompanies.has(candidate.companyId) || seenEmails.has(candidate.email)
      || (companyNameKey && seenCompanyNames.has(companyNameKey)));
    const blocked = cid === 1 && missionReason
      ? missionReason
      : (duplicate ? 'duplicate_company_or_email' : (eligibility.eligible ? null : eligibility.reason));
    if (blocked) {
      excluded.push({ prospectId: candidate.prospectId, reason: blocked });
      bump(blocked);
    } else {
      seenCompanies.add(candidate.companyId); seenEmails.add(candidate.email);
      if (companyNameKey) seenCompanyNames.add(companyNameKey);
      clean.push({ ...candidate, buyerReadiness: eligibility.buyerReadiness });
    }
  }
  return { clean, excluded, scope, exclusionCounts };
}

function scoutInput(program, source, plan, tenantContext = null, discoveryGeneration = 0) {
  const scope = sourceScope(source);
  const payload = source?.payload || {};
  const tenant = resolveReplenishmentTenantContext({
    governedContext: tenantContext?.governedContext || tenantContext,
    tenantId: tenantContext?.tenantId,
    program,
  });
  const tenantId = tenant.tenantId;
  const region = firstPresent(scope.region, (scope.cities || []).join(', '));
  if (!region) {
    throw Object.assign(new Error('replenishment_geography_required'), {
      code: 'replenishment_geography_required',
    });
  }
  const segment = firstPresent(scope.segment, scope.industry);
  const segments = scope.eligibleSubsegments?.length ? scope.eligibleSubsegments : (segment ? [segment] : []);
  const commercialCapability = firstPresent(scope.commercialCapability);
  const businessType = firstPresent(scope.businessType, scope.industry, segment);
  const pulseforgeCashflow = scope.offerId
    === require('../utils/pulseforgeCashflowOffer').PULSEFORGE_CASHFLOW_OFFER_ID;
  return {
    authorizedTenantId: tenantId,
    tenantId,
    workflow: 'outbound_inventory_replenishment',
    inventoryDeficit: plan.deficit,
    question: `Max needs Scout to replenish verified outbound inventory for ${segment || 'the approved segment'} in ${region}.`,
    objective: `Find enough net-new, in-scope prospects to close an outbound inventory deficit of ${plan.deficit} while preserving ownership, prior-contact, DNC and suppression boundaries.`,
    reason: `Planning daily capacity is ${plan.planningDailyCapacity ?? plan.safeDailyCapacity} (dispatch now ${plan.dispatchCapacityNow ?? 0}; Emmett recommends ${plan.recommendedSafeDailyCapacity ?? plan.safeDailyCapacity}); Max requires a ${plan.targetDays}-day buffer of ${plan.targetInventory}, but only ${plan.cleanInventory} clean prospects are currently available.`,
    authority: 'observe',
    force: true,
    businessContext: {
      serviceGeography: region,
      commercialCapability,
      preferredSegments: segments,
      acquisitionDirection: source?.objective || payload.objective || null,
      exclusions: payload.constraints || [],
    },
    targetContext: {
      geography: region,
      geographyScope: scope.scope || null,
      cities: ((source?.mission || source?.payload || source || {}).structuredMission?.geography?.cities || scope.cities).slice(),
      missionBound: true,
      segments,
      businessType,
      desiredSignals: pulseforgeCashflow
        ? ['southern_new_hampshire_business', 'plumbing_heating_hvac', 'public_inquiry_or_booking_path']
        : ['decision_maker', 'service_gap', 'portfolio_growth', 'turnover_support'],
      discoveryGeneration: Math.max(0, Number(discoveryGeneration || 0)),
    },
    operatorDirection: 'Maintain verified inventory ahead of governed outbound demand. Do not contact prospects.',
  };
}

function scoutSearchFingerprint(program, source) {
  const scope = sourceScope(source);
  return hash({
    version: REPLENISHMENT_TAXONOMY_VERSION,
    tenantId: String(program?.tenant_id || program?.tenantId || ''),
    sourceMissionId: program?.source_mission_id || program?.sourceMissionId || null,
    segment: scope.segment,
    industry: scope.industry,
    eligibleSubsegments: scope.eligibleSubsegments,
    region: scope.region,
    geographyScope: scope.scope,
    cities: scope.cities,
  });
}

async function loadScoutDiscoveryBackoff(pool, tenantId, fingerprint, now = new Date(), opts = {}) {
  const threshold = Number(opts.threshold || SCOUT_DISCOVERY_BACKOFF_THRESHOLD);
  const cooldownMinutes = Number(opts.cooldownMinutes || SCOUT_DISCOVERY_BACKOFF_MINUTES);
  const { rows } = await pool.query(`
    SELECT created_at,payload
      FROM acquisition_outbound_events
     WHERE tenant_id=$1
       AND event_type='max_outbound_control'
       AND payload->>'scoutSearchFingerprint'=$2
       AND payload->>'scoutDiscoveryAttempted'='true'
     ORDER BY created_at DESC
     LIMIT $3`, [String(tenantId), fingerprint, threshold]);
  if (rows.length < threshold) return null;
  const repeatedZeroYield = rows.every(row =>
    Number(row.payload?.newCleanInventoryAdded || 0) === 0
    && Number(row.payload?.newPromotions || 0) === 0
    && Number(row.payload?.recoveredExisting || 0) === 0
  );
  if (!repeatedZeroYield) return null;
  const latestGeneration = rows.reduce((max, row) => Math.max(
    max,
    Number(row.payload?.scoutSearchGeneration || 0)
  ), 0);
  const lastAttemptAt = new Date(rows[0].created_at);
  const retryAt = new Date(lastAttemptAt.getTime() + cooldownMinutes * 60_000);
  if (+retryAt <= +now) return null;
  return {
    reason: latestGeneration < SCOUT_DISCOVERY_MAX_GENERATION
      ? 'repeated_zero_yield_rotate_search'
      : 'repeated_zero_yield_generation_limit_reached',
    fingerprint,
    consecutiveZeroYieldAttempts: rows.length,
    searchGeneration: Math.min(latestGeneration + 1, SCOUT_DISCOVERY_MAX_GENERATION),
    diversify: latestGeneration < SCOUT_DISCOVERY_MAX_GENERATION,
    lastAttemptAt: lastAttemptAt.toISOString(),
    retryAt: retryAt.toISOString(),
  };
}

async function persistDiscoveredCompanies(pool, store, {
  companies = [],
  searchDefinition = null,
  scoutContext = {},
  tenantId = null,
} = {}) {
  const tenant = resolveReplenishmentTenantContext({
    governedContext: scoutContext.governedContext,
    tenantId: tenantId || scoutContext.authorizedTenantId,
    program: scoutContext.program,
  });
  let inserted = 0;
  const counters = createReplenishmentAdmissionCounters();
    counters.discovered = companies.length;
  counters.recovered = 0;
  counters.alreadyQueued = 0;
  const queueAccounting = require('./scoutQueueAccounting').createQueueAccounting();
  mergeAlternateTelemetry(counters, emptyAlternateContactTelemetry());

  const scope = scoutContext.scope || {};
  const admissionContext = {
    tenantId: tenant.tenantId,
    missionSegment: scope.segment || (searchDefinition?.segments || [])[0] || null,
    missionSegments: scope.eligibleSubsegments,
    missionCities: scope.cities,
    region: scope.region,
    allowedCities: scoutContext.allowedCities || scoutContext.serviceAreas || null,
    service_area: scoutContext.serviceAreas || null,
    clientConfig: scoutContext.clientConfig || null,
    discoveryQuery: scoutContext.discoveryQuery || null,
    discoveryConcept: scoutContext.discoveryConcept || null,
    discoveryCity: scoutContext.discoveryCity || null,
    discoverySource: scoutContext.discoverySource || null,
  };

  let websiteInvestigations = 0;
  let checkpointDeferred = false;
  const ticketPass = scoutContext.ticketPass || null;
  const finishTicket = async (company, outcome, write) => ticketPass
    ? ticketPass.finish(company._scoutTicketRefs, outcome, write)
    : { applied: true, result: write ? await write(pool) : null };
  const evidenceCursor = scoutContext.evidenceCursor || null;
  if (evidenceCursor) {
    require('./scoutBusinessEvidenceCursor').assertCursorTenantBindings({ tenantId: tenant.tenantId, store, scoutContext });
    evidenceCursor.assertTenant(tenant.tenantId);
    const eligible = [];
    for (const company of companies) {
      if (company.tenantId != null && String(company.tenantId) !== tenant.tenantId) throw Error('Scout evidence cursor candidate tenant mismatch');
      const name = String(company.name || '').trim();
      const website = String(company.website || '').trim() || null;
      const domain = normalizeDomain(company.domain || website);
      if (!name || !domain || evaluateReplenishmentAdmission(company, admissionContext).reason !== 'unclassifiable_vertical') continue;
      // Selection is read-only. The processing loop rechecks ownership before
      // each actual request. Newly visited same-company work must not activate
      // alternate-contact providers, even for correctly tenant-bound Anchor.
      const ownership = await classifyInventoryOwnership(store, { company: name, domain, website }, {
        pool, clientId: tenant.clientId, policy: scoutContext.program?.policy,
      });
      if (ownership.kind === OWNERSHIP_KINDS.CLEAR) eligible.push(company);
    }
    evidenceCursor.select(eligible);
  }
  const decisions = [];
  for (let company of companies) {
    // A claim is a durable reservation, not an in-memory seen set. Never spend
    // or write for a stale/expired worker; leave the ticket independently due.
    if (ticketPass && !(await ticketPass.renew(company._scoutTicketRefs))) {
      checkpointDeferred = true;
      continue;
    }
    const reject = reason => {
      recordReplenishmentRejection(counters, reason);
      decisions.push({ name: company.name, domain: company.domain || company.website, location: company.location, reason });
    };
    counters.evaluated += 1;
    const name = String(company.name || '').trim();
    const website = String(company.website || '').trim() || null;
    const domain = normalizeDomain(company.domain || website);
    if (!name || !domain) {
      reject('insufficient_business_fit');
      await finishTicket(company, { state: 'blocked', reason: 'identity_incomplete' });
      continue;
    }

    const ownership = await classifyInventoryOwnership(store, { company: name, domain, website }, {
      pool,
      clientId: tenant.clientId,
      policy: scoutContext.program?.policy,
    });
    if (ownership.kind === OWNERSHIP_KINDS.ALREADY_USABLE_CANONICAL) {
      counters.alreadyUsable = (counters.alreadyUsable || 0) + 1;
      await finishTicket(company, { state: 'deferred', reason: 'already_usable_observed', retryAfterMs: 24 * 60 * 60_000 });
      continue;
    }
    if (ownership.kind === OWNERSHIP_KINDS.CONTACT_POLICY_HOLD) {
      checkpointDeferred = true;
      counters.contactPolicyDeferred = (counters.contactPolicyDeferred || 0) + 1;
      reject('same_company_different_contact');
      await finishTicket(company, { state: 'deferred', reason: 'contact_policy_hold', retryAfterMs: 24 * 60 * 60_000 });
      continue;
    }
    if (ownership.kind === OWNERSHIP_KINDS.VALID_COLLISION
      || ownership.kind === OWNERSHIP_KINDS.PRIOR_CONTACT
      || ownership.kind === OWNERSHIP_KINDS.AO_OWNED) {
      reject('owned_elsewhere');
      await finishTicket(company, { state: 'deferred', reason: 'ownership_policy_observed', retryAfterMs: 24 * 60 * 60_000,
        evidence: { ownershipKind: ownership.kind, reason: ownership.reason || 'governed_ownership', tenantId: tenant.tenantId } });
      continue;
    }
    if (ownership.kind === OWNERSHIP_KINDS.STALE) {
      reject('stale_ownership');
      await finishTicket(company, { state: 'deferred', reason: 'ownership_unresolved', retryAfterMs: 24 * 60 * 60_000 });
      continue;
    }
    let businessEvidence = null;
    let investigatedThisCandidate = false;
    let evidenceOutcome = null;
    const initialAdmission = evaluateReplenishmentAdmission(company, admissionContext);
    if (initialAdmission.reason === 'unclassifiable_vertical' && scoutContext.investigateBusinessEvidence && websiteInvestigations < 20
      && (!evidenceCursor || (ownership.kind === OWNERSHIP_KINDS.CLEAR && evidenceCursor.take(company)))) {
      websiteInvestigations += 1;
      investigatedThisCandidate = true;
      const response = await require('./scoutWebsiteBusinessEvidence').acquireBusinessEvidence(company, admissionContext,
        ticketPass ? { reportOutcome: true } : undefined);
      evidenceOutcome = ticketPass ? response : null;
      const observed = ticketPass ? response?.result : response;
      if (observed) { company = observed.candidate; businessEvidence = observed.evidence; }
    }
    const admission = evaluateReplenishmentAdmission(company, admissionContext);
    if (!admission.admitted) {
      // Missing website evidence may reflect the per-run investigation cap or
      // a temporary fetch failure. Retain replay until this is resolved.
      if (admission.reason === 'unclassifiable_vertical') checkpointDeferred = true;
      reject(admission.reason);
      await finishTicket(company, admission.reason === 'unclassifiable_vertical'
        ? !investigatedThisCandidate ? { state: 'deferred', reason: 'evidence_budget_deferred', retryAfterMs: 0 }
          : evidenceOutcome?.outcome === 'fetch_failed' && evidenceOutcome.retryable === true
            ? { state: 'deferred', reason: 'transient_business_evidence_failure', retryAfterMs: 24 * 60 * 60_000 }
            : { state: 'blocked', reason: evidenceOutcome?.outcome === 'no_evidence'
              ? 'unresolved_business_evidence' : 'business_evidence_dependency',
            evidence: { outcome: evidenceOutcome?.outcome || 'unknown', reason: evidenceOutcome?.reason || 'unreported_crawl_outcome' } }
        : ['contradictory_business_type', 'segment_mismatch'].includes(admission.reason)
          ? { state: 'handled', reason: 'policy_excluded', evidence: { admissionReason: admission.reason,
            provenance: admission.provenance || null, tenantId: tenant.tenantId } }
          : { state: 'blocked', reason: admission.reason === 'outside_geography' ? 'geography_unresolved' : 'identity_incomplete' });
      continue;
    }
    if (tenant.tenantId === '1') {
      const offer = require('../utils/pulseforgeCashflowOffer');
      const reason = scope.offerId !== offer.PULSEFORGE_CASHFLOW_OFFER_ID
        ? offer.SOURCE_REVIEW_BLOCKER
        : offer.pulseforgeTargetReason({ ...company, website, domain, industry: admission.vertical });
      if (reason) { reject(reason); await finishTicket(company, { state: 'deferred', reason: 'ownership_unresolved', retryAfterMs: 24 * 60 * 60_000 }); continue; }
    }

    if (ownership.kind === OWNERSHIP_KINDS.SAME_COMPANY_DIFFERENT_CONTACT) {
      if (ticketPass) {
        // The legacy helper spends and writes outside this ticket transaction.
        // Do not activate it until business-scoped reservation + fenced writes
        // are implemented; retain independent work without replaying queries.
        checkpointDeferred = true; reject('same_company_different_contact');
        await finishTicket(company, { state: 'blocked', reason: tenant.tenantId === '13'
          ? 'recovery_tenant_unsupported' : 'recovery_reservation_required' });
        continue;
      }
      if (typeof store.one === 'function') {
        const recent = await store.one(`SELECT id FROM acquisition_outbound_events
          WHERE tenant_id=$1 AND event_type='scout_contact_recovery_attempt'
          AND payload->>'companyId'=$2 AND created_at>now()-interval '1 hour' LIMIT 1`,
        [tenant.tenantId, ownership.companyId]);
        if (recent) { checkpointDeferred = true; counters.recoveryBackoff = (counters.recoveryBackoff || 0) + 1;
          await finishTicket(company, { state: 'deferred', reason: 'recovery_backoff', retryAfterMs: 60 * 60_000 }); continue; }
      }
      const recovery = await attemptSameCompanyAlternateRecovery(store, pool, {
        tenantId: tenant.tenantId,
        company: { ...company, vertical: admission.vertical },
        ownership,
        scoutContext: { ...scoutContext, admittedVertical: admission.vertical, businessEvidence },
        sources: scoutContext.recoverySources,
      });
      mergeAlternateTelemetry(counters, recovery.telemetry || {});
      if (recovery.telemetry?.sameCompanyCandidatesAttempted > 0 && typeof store.event === 'function') await store.event('scout_contact_recovery_attempt', require('node:crypto').randomUUID(),
        { companyId: ownership.companyId, domain, reason: recovery.reason, telemetry: recovery.telemetry });
      if (recovery.ok) {
        counters.recovered += 1;
        await finishTicket(company, { state: 'handled', reason: 'recovered' });
        continue;
      }
      checkpointDeferred = true;
      reject('same_company_different_contact');
      await finishTicket(company, ['recovery_tenant_unsupported', 'provider_unavailable'].includes(recovery.reason)
        ? { state: 'blocked', reason: recovery.reason }
        : { state: 'deferred', reason: 'recovery_failed', retryAfterMs: 60 * 60_000 });
      continue;
    }

    counters.fit += 1;
    const notes = formatProvenanceNotes(
      'Discovered by Max-directed Scout inventory replenishment; no contact performed.',
      admission.provenance
    ) + (businessEvidence ? ` | business_evidence: ${JSON.stringify(businessEvidence)}` : '');
    const writeQueue = async db => {
      if (ticketPass) {
        // Different provider identities may resolve to the same canonical queue
        // predicate. Serialize both OR keys before INSERT ... WHERE NOT EXISTS.
        for (const key of [`${tenant.tenantId}:domain:${domain.toLowerCase()}`, `${tenant.tenantId}:name:${name.toLowerCase()}`].sort()) {
          await db.query('SELECT pg_advisory_xact_lock(261023,hashtext($1))', [key]);
        }
      }
      const insertedRow = await db.query(`
      INSERT INTO scout_unenriched (
        client_id, company, website_url, domain, vertical, location, source,
        enrichment_attempts, last_attempt_at, notes
      )
      SELECT $8,$1,$2,$3,$4,$5,'max_buffer_replenishment',0,NULL,$6
      WHERE NOT EXISTS (
        SELECT 1 FROM scout_unenriched u
        WHERE u.client_id=$8 AND (
          lower(u.domain)=lower($3) OR lower(trim(u.company))=lower(trim($1))
        )
        AND NOT (
          u.source = 'max_buffer_replenishment'
          AND COALESCE(u.enrichment_attempts, 0) = 0
          AND NOT (lower(trim(u.vertical)) = ANY($7::text[]))
        )
      )
      RETURNING id
    `, [
      name,
      website || `https://${domain}`,
      domain,
      admission.vertical,
      company.location || admission.provenance?.discoveryCity || null,
      notes,
      CANONICAL_ENRICHABLE_SCOUT_VERTICALS.map(v => v.toLowerCase()),
      tenant.clientId,
    ]);
      if (ticketPass && !insertedRow.rowCount) {
        const matched = await db.query(`SELECT u.id, u.client_id,
          ${require('./scoutQueueAccounting').ELIGIBILITY_SQL} AS eligibility_bucket,
          ${require('./scoutQueueAccounting').RETRY_AFTER_SQL} AS retry_after_ms,
          u.enrichment_attempts,u.last_attempt_at
          FROM scout_unenriched u
          WHERE u.client_id=$1 AND (lower(u.domain)=lower($2) OR lower(trim(u.company))=lower(trim($3)))
          AND NOT (u.source = 'max_buffer_replenishment'
            AND COALESCE(u.enrichment_attempts, 0) = 0
            AND NOT (lower(trim(u.vertical)) = ANY($4::text[])))`,
        [tenant.clientId, domain, name, CANONICAL_ENRICHABLE_SCOUT_VERTICALS.map(v => v.toLowerCase()),
          enrichmentVerticals(tenant, scoutContext)]);
        if (!matched.rows.length) throw Error('Scout ticket queue handling was not confirmed');
        queueAccounting.observe(matched.rows, { tenantId: tenant.tenantId });
        if (!matched.rows.some(row => row.eligibility_bucket === 'eligible_now')) {
          const retryable = matched.rows.filter(row => ['retry_cooldown', 'attempt_limit_hold'].includes(row.eligibility_bucket)
            && !(row.eligibility_bucket === 'attempt_limit_hold' && row.last_attempt_at == null));
          const earliest = retryable.sort((a,b) => Number(a.retry_after_ms)-Number(b.retry_after_ms))[0];
          throw Object.assign(Error('Existing Scout queue work is held'), { scoutQueueHeld: true,
            scoutQueueOutcome: earliest ? { state: 'deferred',
              reason: earliest.eligibility_bucket === 'attempt_limit_hold' ? 'queue_attempt_cooldown' : 'queue_retry_cooldown',
              retryAfterMs: Math.max(60*60_000, Math.ceil(Number(earliest.retry_after_ms))) }
              : { state: 'blocked', reason: matched.rows.every(row => row.eligibility_bucket === 'unsupported_vertical')
                ? 'queue_unsupported_vertical' : 'queue_attempt_limit' } });
        }
      }
      return insertedRow;
    };
    let handled;
    try { handled = await finishTicket(company, { state: 'handled', reason: 'queued' }, writeQueue); }
    catch (error) {
      if (!ticketPass || !error.scoutQueueHeld) throw error;
      counters.alreadyQueued += 1;
      checkpointDeferred = true;
      await finishTicket(company, error.scoutQueueOutcome);
      continue;
    }
    if (!handled.applied) { checkpointDeferred = true; continue; }
    const result = handled.result || { rowCount: 0, rows: [] };
    inserted += result.rowCount;
    if (result.rowCount) counters.admittedToEnrichment += 1;
    else {
      counters.alreadyQueued += 1;
      // Read-only observation of the same tenant-scoped duplicate predicate.
      // Failure cannot change admission or reset retry/attempt controls.
      try {
        if (ticketPass) continue; // Already measured within the handling transaction.
        const matched = await pool.query(`SELECT u.id, u.client_id,
          ${require('./scoutQueueAccounting').ELIGIBILITY_SQL} AS eligibility_bucket
          FROM scout_unenriched u
          WHERE u.client_id=$1 AND (lower(u.domain)=lower($2) OR lower(trim(u.company))=lower(trim($3)))
          AND NOT (u.source = 'max_buffer_replenishment'
            AND COALESCE(u.enrichment_attempts, 0) = 0
            AND NOT (lower(trim(u.vertical)) = ANY($4::text[])))`,
        [tenant.clientId, domain, name, CANONICAL_ENRICHABLE_SCOUT_VERTICALS.map(v => v.toLowerCase()),
          enrichmentVerticals(tenant, scoutContext)]);
        queueAccounting.observe(matched.rows, { tenantId: tenant.tenantId });
      } catch (_) { queueAccounting.observe(null); }
    }
  }

  counters.queueMatches = queueAccounting.snapshot();
  counters.decisions = decisions;
  counters.websiteInvestigations = websiteInvestigations;
  clampCohortCounters(counters);
  return {
    inserted,
    admission: counters,
    checkpointComplete: !checkpointDeferred,
    tenantId: tenant.tenantId,
    clientId: tenant.clientId,
  };
}

function mapReuseCompanyRows(rows, tenantId) {
  return rows.map(row => ({
    id: String(row.id),
    tenantId: String(row.tenant_id || row.client_id || tenantId || ''),
    name: row.name,
    website: row.website || (row.domain ? `https://${row.domain}` : null),
    industry: row.vertical || 'short_term_rental',
    location: row.location || null,
    icpScore: row.icp_score,
    updatedAt: row.updated_at,
  }));
}

async function loadReuseCompanies(pool, tenantId) {
  const tenant = resolveReplenishmentTenantContext({ tenantId });
  const { rows } = await pool.query(`
    SELECT c.id,c.name,c.domain,c.website,c.location,p.vertical,p.icp_score,p.updated_at
    FROM companies c
    LEFT JOIN prospects p ON p.company_id=c.id AND p.client_id=c.client_id
    WHERE c.client_id=$1
  `, [tenant.clientId]);
  return mapReuseCompanyRows(rows, tenant.tenantId);
}

function enrichmentVerticals(tenant, tenantContext) {
  let verticals = ENRICHABLE_SCOUT_VERTICALS;
  if (tenant.tenantId === '1') {
    const offer = require('../utils/pulseforgeCashflowOffer');
    const source = tenantContext?.source;
    const reason = offer.pulseforgeMissionReason(source?.mission || source?.payload || source);
    if (reason) throw Object.assign(new Error(reason), { code: reason });
    const scope = sourceScope(source);
    verticals = missionCompatibleVerticals({ missionSegments: scope.eligibleSubsegments });
  } else if (tenant.tenantId === '10') {
    const scope = tenantContext?.scope || sourceScope(tenantContext?.source);
    const segments = scope.eligibleSubsegments?.length ? scope.eligibleSubsegments : [scope.segment];
    // Only an explicit Anchor facilities mission opts into the new categories.
    // Keep its entire reviewed scope (including restaurant FOH), not the legacy
    // catch-all. Unsupported mixed scopes resolve to [] and select no queue rows.
    if (segments.some(segment => segment === 'managed_facilities'
      || MISSION_SCOPED_FACILITIES_VERTICALS.includes(segment))) {
      verticals = missionCompatibleVerticals({ missionSegments: segments });
    }
  }
  return verticals;
}

async function runEnrichmentBatches(enrichment, pool, requested, tenantContext = null) {
  const tenant = resolveReplenishmentTenantContext({
    governedContext: tenantContext?.governedContext,
    tenantId: tenantContext?.tenantId,
    program: tenantContext?.program,
  });
  const verticals = enrichmentVerticals(tenant, tenantContext);
  const summaries = [];
  let promoted = 0;
  let recovered = 0;
  let emailResolved = 0;
  let emailVerified = 0;
  let considered = 0;
  const batches = Math.min(
    MAX_ENRICHMENT_BATCHES_PER_CYCLE,
    Math.ceil(Math.max(0, requested) / DEFAULT_ENRICHMENT_BATCH)
  );
  for (let i = 0; i < batches; i += 1) {
    const remaining = Math.max(1, requested - promoted - recovered);
    const summary = await enrichment.run({
      client_id: tenant.clientId,
      tenantId: tenant.tenantId,
      limit: Math.min(DEFAULT_ENRICHMENT_BATCH, remaining),
      retryHours: 1,
      verticals,
      db: pool,
    });
    summaries.push(summary);
    promoted += Number(summary?.promoted || 0);
    recovered += Number(summary?.recovered || 0);
    emailResolved += Number(summary?.emailResolved || 0);
    emailVerified += Number(summary?.emailVerified || 0);
    considered += Number(summary?.considered || 0);
    if (!summary?.considered) break;
  }
  return { promoted, recovered, emailResolved, emailVerified, considered, summaries };
}

// The recovery overlay belongs only to Anchor. Other tenants never acquire its
// locks or read its catalog/control/ledger, including when the schema is absent.
async function researchAuthority(pool, program, tenantId, now = new Date()) {
  if (program?.tenant_id != null && String(program.tenant_id) !== String(tenantId)) fail('governed_outbound_tenant_mismatch');
  if (String(tenantId) !== '10') return null;
  const state = await loadRecoveryState(pool, { now });
  if (state.mode === 'legacy') return null;
  if (state.mode !== 'scoped_resume') fail('anchor_recovery_separate_preparation_approval_required');
  return require('./anchorScopedResume').assertScopedResumeCapability(pool,
    { program, capability: 'research', operation: 'research_existing_source' }, { now });
}

async function consumeResearch(pool, authority, now) {
  if (!authority) return;
  await require('./anchorScopedResumeControl').consumeScopedOperation(pool, {
    manifest: authority.manifest, capability: 'research', operation: 'research_existing_source',
    operationId: require('node:crypto').randomUUID(), units: 1,
  }, { now });
}

async function defaultScoutRamp(input = {}) {
  const governed = input.governedContext || createGovernedOutboundContext({ program: input.program });
  const tenant = resolveReplenishmentTenantContext({ governedContext: governed, program: input.program });
  return withResearchProviderScope(tenant.tenantId,
    () => defaultScoutRampResearchScoped({ ...input, governedContext: governed }));
}

async function defaultScoutRampResearchScoped({
  cycleId = null,
  pool,
  store,
  program,
  source,
  plan,
  logger = console,
  skipVerificationRetry = false,
  enrichment = null,
  runDiscovery = null,
  governedContext = null,
  searchFingerprint = null,
  loadDiscoveryBackoff = loadScoutDiscoveryBackoff,
  recoverExistingInventory = null,
  createQueryCheckpointSession = null,
  createBusinessEvidenceCursorStore = null,
  createCandidateTicketStore = null,
  createPageCheckpointStore = null,
  createTenantDiscoveryMemory = null,
  now = new Date(),
} = {}) {
  const governed = governedContext || createGovernedOutboundContext({ program });
  const tenant = resolveReplenishmentTenantContext({ governedContext: governed, program });
  const authority = await researchAuthority(pool, program, tenant.tenantId, now);
  // Commit consumption before recovery, enrichment, discovery or injected work.
  await consumeResearch(pool, authority, now);
  const tenantBinding = { ...tenant, program, governedContext: governed, source };
  const enricher = enrichment || require('../scoutUnenrichedEnrichmentAgent');
  const recoverExisting = recoverExistingInventory
    || require('./existingInventoryRecovery').recoverExistingGovernedInventory;
  let existingRecovery = null;
  try {
    existingRecovery = await recoverExisting({
      pool,
      program,
      source,
      tenantId: tenant.tenantId,
    });
  } catch (err) {
    rethrowResearchProviderControl(err);
    logger.warn?.('[max-outbound-control] existing inventory recovery failed', err.message || err);
  }
  const first = await runEnrichmentBatches(enricher, pool, plan.deficit, tenantBinding);
  let promoted = first.promoted + first.recovered;
  let discovery = null;
  let discoveryAttempted = false;
  let discoveryBackoff = null;
  let searchGeneration = 0;
  let persisted = { inserted: 0, admission: createReplenishmentAdmissionCounters() };
  let admissionMeasured = false;
  let queryProgression = null;
  let evidenceProgression = evidenceProgressionDiagnostic(null);
  let candidateWork = null;

  const recoveredExistingCount = Number(existingRecovery?.payload?.qualifiedCount || 0);
  if (promoted < plan.deficit && recoveredExistingCount <= 0) {
    const fingerprint = searchFingerprint || scoutSearchFingerprint(program, source);
    const scope = sourceScope(source);
    const allowedCities = resolveScoutRampAllowedCities(scope);
    const persistPass = async (input, ticketPass = null) => {
      const persist = evidenceCursor => persistDiscoveredCompanies(pool, store, {
        ...input, tenantId: tenant.tenantId,
        scoutContext: { scope, source, allowedCities, serviceAreas: allowedCities,
          clientId: tenant.clientId, authorizedTenantId: tenant.tenantId, governedContext: governed,
          program, investigateBusinessEvidence: true, evidenceCursor, ticketPass },
      });
      if (input.companies?.length) {
        const createCursor = createBusinessEvidenceCursorStore
          || require('./scoutBusinessEvidenceCursor').createBusinessEvidenceCursorStore;
        try {
          const cursorRun = await createCursor({ pool, tenantId: tenant.tenantId, fingerprint }).withPass(persist);
          evidenceProgression = evidenceProgressionDiagnostic(cursorRun.progression);
          persisted = cursorRun.result;
        } catch (error) {
          evidenceProgression = evidenceProgressionDiagnostic(error.evidenceProgression || { status: 'failed' });
          throw error;
        }
      } else persisted = await persist(null);
      admissionMeasured = true;
      return persisted;
    };
    const ticketDispatch = require('./scoutCandidateTicketDispatch');
    const durableEnabled = ticketDispatch.ENABLED_TENANTS.has(tenant.tenantId)
      && (createCandidateTicketStore || process.env.SCOUT_DURABLE_CANDIDATES_ENABLED === 'true'
        || require('./scoutTenantDiscoveryMemory').isTenantDiscoveryMemoryEnabled(tenant.tenantId));
    let tickets = null;
    let dispatcher = null;
    let pages = null;
    let tenantMemory = null;
    let coverageEpochBlocked = false;
    if (durableEnabled) {
      tickets = (createCandidateTicketStore || require('./scoutCandidateTickets').createCandidateTicketStore)({
        pool, tenantId: tenant.tenantId, fingerprint,
      });
      await tickets.assertReady(); // Fail closed before any new discovery/provider work.
      let coverageEpoch = null;
      try {
        coverageEpoch = require('./scoutCoverageEpoch').loadCoverageEpochConfig({ tenantId: tenant.tenantId, fingerprint });
      } catch (error) {
        if (error.code !== 'SCOUT_COVERAGE_EPOCH_CONFIG_INVALID') throw error;
        // Invalid permission blocks acquisition, never the already-owned ticket
        // backlog. Do not expose the manifest or an arbitrary parser message.
        coverageEpochBlocked = true;
      }
      pages = (createPageCheckpointStore || require('./scoutQueryContinuation').createPageCheckpointStore)({
        db: pool, tenantId: tenant.tenantId, fingerprint, coverageEpoch,
      });
      if (typeof pages.assertReady === 'function') await pages.assertReady();
      if (require('./scoutTenantDiscoveryMemory').ENABLED_TENANTS.has(tenant.tenantId)) {
        tenantMemory=(createTenantDiscoveryMemory || require('./scoutTenantDiscoveryMemory').createTenantDiscoveryMemory)({
          pool,tenantId:tenant.tenantId,fingerprint,
        });
        await tenantMemory.assertReady();
      }
      const dependencyKey = ticketDispatch.candidateTicketDependencyKey({ tenantId: tenant.tenantId, program, source });
      dispatcher = ticketDispatch.createCandidateTicketDispatcher({ ticketStore: tickets,
        tenantId: tenant.tenantId, dependencyKey, persist: persistPass });
      candidateWork = await dispatcher.snapshot();
    }
    discoveryBackoff = await loadDiscoveryBackoff(pool, tenant.tenantId, fingerprint, now);
    if (discoveryBackoff?.diversify) {
      searchGeneration = Number(discoveryBackoff.searchGeneration || 1);
      if (typeof store.event === 'function') {
        await store.event('scout_replenishment_plan_reconsidered', [fingerprint, searchGeneration], {
          programId: program.id,
          ...discoveryBackoff,
          reason: 'repeated_zero_yield_reconsider_plan',
        });
      }
    } else if (discoveryBackoff) {
      discovery = { kind: 'backoff', ...discoveryBackoff };
      if (typeof store.event === 'function') {
        await store.event('scout_replenishment_backoff', [fingerprint, discoveryBackoff.retryAt], {
          programId: program.id,
          ...discoveryBackoff,
        });
      }
    }
    if (coverageEpochBlocked) {
      discovery = { kind: 'blocked', reason: 'coverage_epoch_config_invalid' };
      queryProgression = ticketDispatch.queryProgressionDiagnostic({ status: 'not_observed',
        coverageEpochStatus: 'configuration_invalid' });
      emitDiagnostic(logger, '[max-outbound-control] Scout query progression', {
        tenantId: tenant.tenantId, cycleId, ...queryProgression,
      });
      if (typeof store.event === 'function') await store.event('tick_blocked',
        [tenant.tenantId, fingerprint, 'coverage_epoch_config_invalid'], { programId: program.id,
          reason: 'coverage_epoch_config_invalid', action: 'review_exact_coverage_epoch_permission' });
    }
    if (!coverageEpochBlocked && (!discoveryBackoff || discoveryBackoff.diversify)) {
      const discover = runDiscovery
        || ((input, opts) => require('./scoutAcquisitionIntelligence').runAcquisitionIntelligenceLoop(input, opts));
      const checkpoints = require('./scoutQueryCheckpoints');
      const checkpointSession = createQueryCheckpointSession
        ? createQueryCheckpointSession({ pool, tenantId: tenant.tenantId, fingerprint, ticketStore: tickets, pageStore: pages, tenantMemory })
        : checkpoints.createCheckpointSession({ store: checkpoints.createCheckpointStore({
          db: pool, tenantId: tenant.tenantId, fingerprint,
        }), ticketStore: tickets, pageStore: pages, tenantMemory });
      const placesProvider = require('../packages/capabilities/discovery/providers/PlacesProvider')
        .createPlacesProvider({ queryCheckpointSession: checkpointSession, tenantDiscoveryMemory:tenantMemory });
      discoveryAttempted = true;
      try { discovery = await discover(
        scoutInput({ ...program, tenant_id: tenant.tenantId }, source, plan, tenant, searchGeneration),
        {
          loadCompanies: async () => loadReuseCompanies(pool, tenant.tenantId),
          persistCompanies: input => dispatcher ? dispatcher.drain(input) : persistPass(input),
          enablePlaces: true,
          placesProvider,
          acknowledgeDiscovery: () => checkpointSession.acknowledge(),
        }
      ); } catch (error) {
        rethrowResearchProviderControl(error);
        if (!dispatcher) throw error;
        discovery = { kind: 'blocked', reason: 'discovery_execution_failed' };
      }

      const checkpointSnapshot = checkpointSession.snapshot();
      queryProgression = ticketDispatch.queryProgressionDiagnostic(checkpointSnapshot);
      const queryDependency = dispatcher ? ticketDispatch.observedQueryDependency(queryProgression) : null;
      if (queryDependency) {
        // Per-query page checkpoints are durable provider backoff: no automatic
        // refetch/refresh of exhausted or uncertain units. Alert only on a new
        // fingerprint/status combination, never claim whole-market exhaustion.
        discovery = { kind: 'blocked', reason: queryDependency.reason };
        if (typeof store.event === 'function') await store.event('tick_blocked',
          [tenant.tenantId, fingerprint, JSON.stringify(queryDependency.statuses)], { programId: program.id, ...queryDependency });
      }
      emitDiagnostic(logger, '[max-outbound-control] Scout query progression', {
        tenantId: tenant.tenantId, cycleId, ...queryProgression, evidenceProgression,
      });
      if (typeof store.event === 'function') {
        await store.event('scout_query_progression', [cycleId || fingerprint, searchGeneration], {
          programId: program.id,
          ...queryProgression,
          evidenceProgression,
        });
      }

    }
    // This is independent of query generation, observed-query completion and
    // zero-yield discovery backoff. A durable deferral never relies on replaying
    // its old Places query to become runnable again.
    if (dispatcher) {
      if (!dispatcher.attempted) {
        try { await dispatcher.drain({ companies: [] }); }
        catch (error) {
          rethrowResearchProviderControl(error);
          discovery = { kind: 'blocked', reason: 'candidate_processing_failed' };
        }
      }
      candidateWork = await dispatcher.snapshot();
      emitDiagnostic(logger, '[max-outbound-control] Scout candidate work',
        { tenantId: tenant.tenantId, cycleId, ...candidateWork });
      if (typeof store.event === 'function') await store.event('scout_candidate_work_progression', [tenant.tenantId, cycleId || fingerprint],
        { programId: program.id, ...candidateWork });
    }
    if (tenantMemory && typeof store.event==='function') await store.event('scout_tenant_search_yield',
      [tenant.tenantId,cycleId || fingerprint],{programId:program.id,...tenantMemory.snapshot(),
        admissionsConfirmed:admissionMeasured,newQueueAdmissionsThisCycle:admissionMeasured ? Number(persisted.inserted || 0) : null});
    if (persisted.inserted > 0 && promoted < plan.deficit) {
      const second = await runEnrichmentBatches(enricher, pool, plan.deficit - promoted, tenantBinding);
      promoted += second.promoted + second.recovered;
      first.promoted += second.promoted;
      first.recovered += second.recovered;
      first.emailResolved += second.emailResolved;
      first.emailVerified += second.emailVerified;
      first.considered += second.considered;
      first.summaries.push(...second.summaries);
    }
  }

  const yieldReport = buildReplenishmentYield({
    admission: persisted.admission || {},
    enrichment: first,
    recovered: first.recovered + Number(persisted.admission?.recovered || 0) + recoveredExistingCount,
  });

  emitDiagnostic(logger, '[max-outbound-control] Scout ramp', scoutRampDiagnostic({
    tenantId: tenant.tenantId, cycleId, requested: plan.deficit, promoted,
    recoveredExisting: recoveredExistingCount, discoveredQueued: persisted.inserted,
    admission: persisted.admission, enrichment: first, discovery,
  }));
  const recoveredExisting = first.recovered + Number(persisted.admission?.recovered || 0) + recoveredExistingCount;
  const verificationRetry = skipVerificationRetry
    ? emptyVerificationRetryTelemetry()
    : await retryUnverifiedEmails(pool, { clientId: tenant.clientId });
  if (persisted.admission) mergeVerificationRetryTelemetry(persisted.admission, verificationRetry);
  return {
    promoted,
    recovered: recoveredExisting,
    recoveredExisting,
    enrichmentPromoted: first.promoted,
    enrichmentRecovered: first.recovered,
    enrichmentConsidered: first.considered,
    enrichmentUnresolved: first.summaries.reduce((sum, row) => sum + Number(row?.unresolved || 0), 0),
    emailResolved: first.emailResolved,
    emailVerified: first.emailVerified,
    enrichmentBatches: first.summaries,
    discoveredQueued: persisted.inserted,
    admission: persisted.admission || null,
    admissionMeasured,
    yield: yieldReport,
    discovery,
    discoveryAttempted,
    discoveryBackoff,
    queryProgression,
    evidenceProgression,
    candidateWork,
    searchFingerprint: searchFingerprint || scoutSearchFingerprint(program, source),
    searchGeneration,
    verificationRetry,
  };
}

async function capturePreparationObservability({
  store,
  program,
  operating = {},
  sentToday = 0,
  cleanInventory = 0,
  inventoryExclusions = [],
  now = new Date(),
} = {}) {
  let pendingPrepared = 0;
  let pendingPreparedMeasured = false;
  let lastSendAt = null;
  try {
    const day = clock(now).day;
    const envelope = typeof store.envelope === 'function' ? await store.envelope(day) : null;
    if (typeof store.envelope === 'function' && !envelope) pendingPreparedMeasured = true;
    if (envelope && typeof store.items === 'function') {
      const items = await store.items(envelope.id);
      pendingPrepared = items.filter(row => row.status === 'pending').length;
      pendingPreparedMeasured = true;
      for (const row of items) {
        if (!row.attempted_at) continue;
        const at = +new Date(row.attempted_at);
        if (!lastSendAt || at > +lastSendAt) lastSendAt = new Date(at);
      }
    }
  } catch (_err) {
    pendingPrepared = 0;
    pendingPreparedMeasured = false;
  }
  const remainingCap = remainingDispatchCapacity({
    dispatchCapacityNow: operating.dispatchCapacityNow ?? 0,
    sentToday,
  });
  const remainingSlots = remainingScheduleSlots({
    now,
    lastSendAt,
    allowedSendWindow: operating.allowedSendWindow || {
      startHour: program?.policy?.startHour ?? 9,
      endHour: program?.policy?.endHour ?? 17,
      timezone: program?.policy?.timeZone || 'America/New_York',
    },
    minSpacingMinutes: operating.minSpacingMinutes ?? program?.policy?.spacingMinutes ?? 60,
    dispatchDayAllowed: operating.dispatchDayAllowed !== false,
  });
  const dailyRemaining = Math.max(
    0,
    Number(operating.authorizationLimitedCapacity
      ?? resolveOperatorDelegatedMaximumDailyCapacity(program?.policy)
      ?? program?.policy?.dailyCap ?? 0) - Number(sentToday || 0)
  );
  const plan = evaluatePreparationRefill({
    pendingPreparedCount: pendingPrepared,
    remainingDispatchCapacity: remainingCap,
    remainingScheduleSlots: remainingSlots,
    cleanInventory,
    governor: operating.governor,
    grantActive: preparationGrantActive(store, program),
    dailyAuthorizationRemaining: dailyRemaining,
    totalAuthorizationRemaining: operating.remainingTotalAuthorization,
    planningDailyCapacity: operating.planningDailyCapacity,
  });
  return { pendingPreparedMeasured, ...observabilityFromRefill(plan, {
    sentToday,
    pendingPrepared,
    remainingDispatchCapacity: remainingCap,
    remainingScheduleSlots: remainingSlots,
    cleanInventory,
    preparationDecisions: inventoryExclusions.map(row => ({
      candidateId: row.prospectId, prospectId: row.prospectId,
      source: 'inventory', outcome: 'rejected', reason: row.reason,
    })),
  }) };
}

async function applyPreparationRefill({
  pool,
  tenantId,
  preparation = {},
  execute = true,
  runPreparationRefill = null,
  controlNow = new Date(),
  governedContext = null,
  program = null,
} = {}) {
  if (!preparation.prepareRequested) {
    return finalizePreparationObservability(preparation);
  }
  if (execute === false) {
    return finalizePreparationObservability({
      ...preparation,
      prepareSkippedReason: 'control_execute_disabled',
    });
  }
  const authority = await assertLegacyPreparation(pool, { tenantId: tenantId ?? governedContext?.tenantId,
    program, operation: 'refill' }, { now: controlNow });
  if (authority?.manifest?.kind === 'scoped_resume' && runPreparationRefill) {
    await require('./anchorScopedResumeControl').consumeScopedOperation(pool, {
      manifest: authority.manifest, capability: 'preparation', operation: 'refill',
      operationId: require('node:crypto').randomUUID(), units: 1,
    }, { now: controlNow });
  }
  const refill = runPreparationRefill
    ? await runPreparationRefill()
    : await require('./governedOutbound').productionService(pool, {
      tenantId,
      governedContext,
      now: controlNow instanceof Date ? controlNow : undefined,
    }).runPreparationRefill();
  if (refill?.halted === 'overlap' || refill?.prepareSkippedReason === 'send_lock_overlap') {
    return finalizePreparationObservability({
      ...preparation,
      preparedAdded: 0,
      prepareSkippedReason: 'send_lock_overlap',
    });
  }
  return finalizePreparationObservability({
    ...preparation,
    pendingPrepared: refill.pendingPrepared ?? preparation.pendingPrepared,
    preparedAdded: refill.preparedAdded ?? 0,
    prepareSkippedReason: refill.prepareSkippedReason ?? null,
    preparationDecisions: refill.preparationDecisions ?? preparation.preparationDecisions ?? [],
  });
}

// Anchor replenishment holds were stdout-only, so a governed halt looked the
// same as a healthy idle cycle. Keep one pending operator item per reason; it
// grants nothing and is skipped while an identical item awaits review.
async function recordReplenishmentHold(pool, { tenantId, clientId, programId, reason, detail = null, logger = console }) {
  if (String(tenantId) !== '10' || !reason || !pool?.query) return;
  try {
    await pool.query(`
      INSERT INTO agent_actions (created_by, action_type, title, description, payload, status, client_id)
      SELECT 'max', 'scout_replenishment_blocked', $1, $2, $3::jsonb, 'pending', $4
      WHERE NOT EXISTS (
        SELECT 1 FROM agent_actions
         WHERE client_id = $4 AND action_type = 'scout_replenishment_blocked'
           AND status = 'pending' AND payload->>'reason' = $5
      )
    `, [
      'Anchor Scout replenishment is blocked',
      `Scout discovery, enrichment and inventory replenishment are not running: ${reason}${detail ? ` (${detail})` : ''}. `
        + 'This is a governance hold, not an empty result. Restoring it needs the matching operator approval.',
      JSON.stringify({ tenantId: String(tenantId), programId: programId || null, reason, detail }),
      clientId,
      reason,
    ]);
  } catch (error) {
    logger.warn?.('[max-outbound-control] replenishment hold record failed', error.message || error);
  }
}

async function runMaxOutboundControlLoop(options = {}) {
  const cycleId = options.cycleId || require('node:crypto').randomUUID();
  const cycleStartedAt = new Date().toISOString();
  const pool = options.pool || require('../db');
  const logger = options.logger || console;
  const programEarly = options.program || null;
  const seedTenantId = options.tenantId || options.store?.tenantId || programEarly?.tenant_id;
  const governedContext = options.governedContext || createGovernedOutboundContext({
    tenantId: seedTenantId,
    program: programEarly,
  });
  const store = options.store || new GovernedOutboundStore(pool, governedContext.tenantId);
  const program = programEarly || await store.program();
  if (!program || ['paused', 'revoked'].includes(program.mode)) {
    return { halted: 'no_enabled_program' };
  }

  const governed = createGovernedOutboundContext({
    governedContext,
    program,
    tenantId: governedContext.tenantId,
  });

  if (program.tenant_id != null && String(program.tenant_id) !== governed.tenantId) fail('governed_outbound_tenant_mismatch');
  // Non-Anchor callers retain the legacy branch without a recovery dependency.
  // A one-item recovery manifest still grants no preparation or research.
  const controlNow = options.now instanceof Date ? options.now : new Date(options.now || Date.now());
  const recoveryState = governed.tenantId === '10'
    ? await loadRecoveryState(pool, { now: controlNow }) : { mode: 'legacy' };
  if (!['legacy', 'scoped_resume'].includes(recoveryState.mode) && options.execute !== false) {
    await recordReplenishmentHold(pool, { tenantId: governed.tenantId, clientId: store.clientId, programId: program.id,
      reason: 'anchor_recovery_separate_preparation_approval_required',
      detail: [recoveryState.mode, recoveryState.reason].filter(Boolean).join(':'), logger });
    return { halted: 'anchor_recovery_separate_preparation_approval_required', programId: program.id, recoveryMode: recoveryState.mode };
  }

  let source = options.source || await loadSource(pool, program, governed.tenantId);
  if (!source) return { halted: 'source_mission_missing', programId: program.id };
  if (governed.tenantId === '1') {
    const reason = require('../utils/pulseforgeCashflowOffer').pulseforgeMissionReason(source.mission || source.payload || source);
    if (reason) return { halted: reason, programId: program.id };
  }

  const governedAdapters = options.governedAdapters || createGovernedAdapters(pool, { governedContext: governed });
  if (governed.tenantId === '17' && (program.mode === 'shadow' || program.policy.preparationOnly)) {
    const reviewedSource = await governedAdapters.loadMission(program.source_mission_id);
    const reason = substralPreparationGrantReason(program, reviewedSource?.mission, controlNow);
    if (reason) return { halted: reason, programId: program.id };
    source = reviewedSource; // Discovery uses the same hash-checked source, never a stale caller snapshot.
    const budget = await store.preparationCounts(program, clock(controlNow).day);
    if (budget.total >= program.policy.totalCap || budget.today >= program.policy.dailyCap) {
      return { halted: 'preparation_budget_exhausted', programId: program.id };
    }
  }
  const infrastructure = options.infrastructure
    || await governedAdapters.infrastructure(program, controlNow, null, { mode: 'planning' });
  const inventoryBefore = options.inventory
    || await loadCleanInventory(pool, store, source, store.clientId, program.policy);
  const sentToday = Number(infrastructure?.snapshot?.sentToday || 0);
  const operating = resolveOperatingCapacity({
    operatingCapacity: infrastructure.operating,
    dailyCap: program.policy.dailyCap,
    policy: program.policy,
    emmettCapacity: infrastructure.cap,
    assessed: infrastructure.assessed,
    sentToday,
    totalAttempted: infrastructure.totalAttempted,
    now: controlNow,
  });
  const timestamps = options.timestamps || await loadInventoryTimestamps(pool, store.tenantId);
  const funnelStock = options.funnel || await loadScoutFunnelStock(pool, store.clientId);
  const plan = buildControlPlan({
    dailyCap: program.policy.dailyCap,
    emmettCapacity: infrastructure.cap,
    operatingCapacity: operating,
    assessed: infrastructure.assessed,
    sentToday,
    totalAttempted: infrastructure.totalAttempted,
    cleanInventory: inventoryBefore.clean.length,
    targetDays: options.targetDays || process.env.ANCHOR_MAX_OUTBOUND_BUFFER_DAYS || DEFAULT_TARGET_DAYS,
    policy: program.policy,
    now: controlNow,
  });

  let scout = null;
  // Prepared-item caps are not authority to spend on discovery/enrichment.
  let discoveryBlockedReason = funnelStock.available === false
    ? 'scout_funnel_unavailable'
    : (program.policy.preparationOnly ? 'preparation_research_not_authorized' : null);
  let scopedResearch = null;
  if (recoveryState.mode === 'scoped_resume' && options.execute !== false && plan.shouldReplenish && !discoveryBlockedReason) {
    try { scopedResearch = await researchAuthority(pool, program, governed.tenantId, controlNow); }
    catch (error) { discoveryBlockedReason = error.code || 'anchor_scoped_resume_research_unavailable'; }
  }
  // A research pause is a governed block, not a failed cycle: check it before
  // consuming scoped research so the cycle still measures and records inventory.
  if (plan.shouldReplenish && options.execute !== false && !discoveryBlockedReason) {
    try { assertResearchProviderAllowed('research', governed.tenantId); }
    catch (error) {
      rethrowUnlessResearchControl(error);
      discoveryBlockedReason = error.code;
    }
  }
  if (plan.shouldReplenish && options.execute !== false && discoveryBlockedReason) {
    await recordReplenishmentHold(pool, { tenantId: governed.tenantId, clientId: store.clientId, programId: program.id,
      reason: discoveryBlockedReason, logger });
  }
  const searchFingerprint = scoutSearchFingerprint(program, source);
  if (plan.shouldReplenish && options.execute !== false && !discoveryBlockedReason) {
    const ramp = options.scoutRamp || defaultScoutRamp;
    // Native Scout reserves at its own boundary; injected callbacks cannot skip it.
    if (options.scoutRamp) await consumeResearch(pool, scopedResearch, controlNow);
    scout = await ramp({
      pool, store, program, source, plan, logger, governedContext: governed,
      searchFingerprint, now: controlNow, cycleId,
    });
  }

  const inventoryAfter = options.inventoryAfter
    || (plan.shouldReplenish && options.execute !== false
      ? await loadCleanInventory(pool, store, source, store.clientId, program.policy)
      : inventoryBefore);

  const inventoryGrowth = computeCleanInventoryGrowth(inventoryBefore.clean, inventoryAfter.clean);
  let aoRefill = null;
  if (governed.tenantId === '10' && recoveryState.mode === 'legacy' && options.execute !== false) {
    try {
      const refillAoBooks = options.refillAoBooks
        || require('../utils/aoAccountFill').fillActiveAoAccounts;
      aoRefill = await refillAoBooks({
        clientId: store.clientId,
        db: pool,
        dryRun: false,
        minPerAo: 12,
        targetPerAo: 20,
        preserveGovernedEmailInventory: true,
      });
    } catch (err) {
      rethrowResearchProviderControl(err);
      logger.warn?.('[max-outbound-control] AO refill failed', err.message || err);
      aoRefill = { error: err.message || String(err), assigned: [] };
    }
  }
  if (scout) {
    scout.yield = buildReplenishmentYield({
      admission: scout.admission || {},
      enrichment: {
        considered: scout.enrichmentConsidered,
        promoted: Number(scout.enrichmentPromoted ?? scout.promoted ?? 0),
        recovered: Number(scout.enrichmentRecovered ?? 0),
        emailResolved: Number(scout.emailResolved || 0),
        emailVerified: Number(scout.emailVerified || 0),
        unresolved: Number(scout.enrichmentUnresolved || 0),
      },
      recovered: Number(scout.recoveredExisting ?? scout.recovered ?? 0),
      inventoryGrowth,
    });
    scout.lossBuckets = buildReplenishmentLossBuckets({
      admission: scout.admission || {},
      enrichment: {
        unresolved: Number(scout.enrichmentUnresolved || 0),
      },
      cleanExclusions: inventoryAfter.exclusionCounts || {},
    });
  }

  const finalPlan = buildControlPlan({
    dailyCap: program.policy.dailyCap,
    emmettCapacity: infrastructure.cap,
    operatingCapacity: operating,
    assessed: infrastructure.assessed,
    sentToday,
    totalAttempted: infrastructure.totalAttempted,
    cleanInventory: inventoryAfter.clean.length,
    targetDays: plan.targetDays,
    policy: program.policy,
    now: controlNow,
  });

  const nowIso = new Date().toISOString();
  const netCleanInventoryDelta = inventoryGrowth.netCleanInventoryDelta;
  const newPromotions = Number(scout?.yield?.newPromotions || 0);
  const recoveredExisting = Number(scout?.yield?.recoveredExisting || 0);
  const lastReplenishmentAttemptAt = scout ? nowIso : timestamps.lastReplenishmentAttemptAt;
  const lastInventoryGrowthAt = scout && netCleanInventoryDelta > 0
    ? nowIso
    : timestamps.lastInventoryGrowthAt;
  const lastNewPromotionAt = scout && newPromotions > 0
    ? nowIso
    : timestamps.lastNewPromotionAt;
  const lastRecoveryAt = scout && recoveredExisting > 0
    ? nowIso
    : timestamps.lastRecoveryAt;
  const lastSuccessfulReplenishmentAt = lastInventoryGrowthAt;
  const lastSuccessfulPromotionAt = lastNewPromotionAt;

  const cycleFunnel = emptyFunnel();
  if (scout?.admission) {
    cycleFunnel.discovered = Number(scout.admission.discovered || 0);
    cycleFunnel.fit = Number(scout.admission.fit || 0);
    cycleFunnel.admittedToEnrichment = Number(scout.admission.admittedToEnrichment || 0);
    cycleFunnel.permanentlyRejected = rejectedCount(scout.admission);
  }
  cycleFunnel.promotedVerified = Number(scout?.promoted || 0);
  const funnel = funnelStock.available === false ? { ...funnelStock } : {
    ...funnelStock,
    discovered: Math.max(Number(funnelStock.discovered || 0), cycleFunnel.discovered),
    fit: Number(funnelStock.fit || 0) + cycleFunnel.fit,
    admittedToEnrichment: Number(funnelStock.admittedToEnrichment || 0) + cycleFunnel.admittedToEnrichment,
    promotedVerified: Number(funnelStock.promotedVerified || 0),
    permanentlyRejected: Number(funnelStock.permanentlyRejected || 0) + cycleFunnel.permanentlyRejected,
  };

  const preparationBefore = await capturePreparationObservability({
    store, program, operating, sentToday, cleanInventory: inventoryAfter.clean.length,
    inventoryExclusions: inventoryAfter.excluded || [], now: controlNow,
  });
  const preparation = await applyPreparationRefill({
    pool,
    tenantId: governed.tenantId,
    program,
    execute: options.execute !== false,
    runPreparationRefill: options.runPreparationRefill,
    controlNow,
    governedContext: governed,
    preparation: preparationBefore,
  }).catch(error => {
    if (recoveryState.mode !== 'scoped_resume') throw error;
    return finalizePreparationObservability({ ...preparationBefore, preparedAdded: 0,
      prepareSkippedReason: error.code || 'anchor_scoped_resume_preparation_unavailable' });
  });
  const sameCompanyDifferentContact = Number(
    scout?.admission?.rejected?.same_company_different_contact || 0
  );
  const verificationRetry = scout?.verificationRetry || scout?.admission || {};

  const ramp = typeof store.rampMetrics === 'function'
    ? await store.rampMetrics(program, clock(controlNow).day) : null;
  const controlEvent = {
    programId: program.id,
    cycleId,
    ramp,
    cycleStartedAt,
    cycleCompletedAt: new Date().toISOString(),
    sendingEnabled: governedOutboundEnabledForTenant(governed.tenantId),
    preparationEnabled: governedOutboundPreparationEnabledForTenant(governed.tenantId),
    policyHash: program.policy_hash,
    sourceMissionId: program.source_mission_id,
    emmettCapacity: operating.recommendedSafeDailyCapacity,
    operatorDelegatedMaximum: finalPlan.operatorDelegatedMaximumDailyCapacity
      ?? resolveOperatorDelegatedMaximumDailyCapacity(program.policy),
    recommendedSafeDailyCapacity: finalPlan.recommendedSafeDailyCapacity,
    authorizationLimitedCapacity: finalPlan.authorizationLimitedCapacity,
    capacityLimitingAuthority: finalPlan.capacityLimitingAuthority ?? operating.capacityLimitingAuthority,
    scheduledToday: infrastructure.operating?.scheduledToday ?? infrastructure.envelope?.currentScheduledCount ?? 0,
    executingToday: infrastructure.operating?.executingToday ?? infrastructure.envelope?.currentExecutingCount ?? 0,
    scheduleLimitedCapacity: finalPlan.scheduleLimitedCapacity,
    nextEligibleScheduleCapacity: finalPlan.nextEligibleScheduleCapacity,
    dispatchCapacityNow: finalPlan.dispatchCapacityNow,
    planningDailyCapacity: finalPlan.planningDailyCapacity,
    dispatchableDailyCapacity: finalPlan.dispatchableDailyCapacity,
    effectiveDailyCapacity: finalPlan.effectiveDailyCapacity,
    dispatchUnavailableNow: finalPlan.dispatchUnavailableNow,
    limitingFactor: finalPlan.limitingFactor,
    capacityReason: finalPlan.capacityReason,
    sentToday,
    pendingPrepared: preparation.pendingPrepared,
    pendingPreparedMeasured: preparation.pendingPreparedMeasured === true,
    sentTodayMeasured: Number.isSafeInteger(infrastructure?.snapshot?.sentToday)
      && infrastructure.snapshot.sentToday >= 0,
    remainingDispatchCapacity: preparation.remainingDispatchCapacity,
    remainingScheduleSlots: preparation.remainingScheduleSlots,
    cleanInventory: preparation.cleanInventory,
    prepareRequested: preparation.prepareRequested,
    preparedAdded: preparation.preparedAdded,
    prepareSkippedReason: preparation.prepareSkippedReason,
    preparationDecisions: preparation.preparationDecisions || [],
    bufferTarget: finalPlan.targetInventory,
    cleanInventoryBefore: inventoryBefore.clean.length,
    cleanInventoryAfter: inventoryAfter.clean.length,
    netCleanInventoryDelta,
    newPromotions,
    recoveredExisting,
    newCleanInventoryAdded: inventoryGrowth.newCleanInventoryAdded,
    scoutPromotionCleanCohort: require('./scoutPromotionCohort').promotionCohort(scout?.enrichmentBatches, inventoryAfter),
    cleanInventoryExclusions: inventoryAfter.exclusionCounts || {},
    deficit: finalPlan.deficit,
    state: finalPlan.state,
    scoutInvoked: Boolean(scout),
    scoutPromoted: newPromotions,
    scoutQueued: Number(scout?.discoveredQueued || 0),
    scoutRecovered: recoveredExisting,
    discoveryBlockedReason,
    scoutSearchFingerprint: searchFingerprint,
    scoutSearchGeneration: Number(scout?.searchGeneration || 0),
    scoutDiscoveryAttempted: Boolean(scout?.discoveryAttempted),
    scoutProviderReceipt: providerReceiptDiagnostic(store.tenantId, cycleId, scout?.discovery),
    scoutAdmissionReceipt: admissionReceiptDiagnostic(store.tenantId, cycleId, scout?.admission, scout?.admissionMeasured),
    scoutEvidenceProgression: evidenceProgressionDiagnostic(scout?.evidenceProgression),
    scoutCandidateWork: require('./scoutCandidateTicketDispatch').candidateTicketDiagnostic(scout?.candidateWork),
    scoutQueryProgression: require('./scoutCandidateTicketDispatch').queryProgressionDiagnostic(scout?.queryProgression),
    scoutBackoff: scout?.discoveryBackoff || null,
    aoRefill: aoRefill ? {
      assigned: Number(aoRefill.assigned?.length || 0),
      skipped: aoRefill.skipped || [],
      error: aoRefill.error || null,
    } : null,
    sameCompanyCandidatesAttempted: Number(scout?.admission?.sameCompanyCandidatesAttempted || 0),
    sameCompanyDifferentContact,
    alternateContactsResolved: Number(scout?.admission?.alternateContactsResolved || 0),
    alternateContactsVerified: Number(scout?.admission?.alternateContactsVerified || 0),
    alternateContactsRejected: Number(scout?.admission?.alternateContactsRejected || 0),
    alternateContactsAddedToCleanInventory: Number(scout?.admission?.alternateContactsAddedToCleanInventory || 0),
    alternateContactLossReasons: scout?.admission?.alternateContactLossReasons || null,
    emailNotVerified: Number(
      verificationRetry.emailNotVerified
      || inventoryAfter.exclusionCounts?.email_not_verified
      || 0
    ),
    verificationRetryAttempted: Number(verificationRetry.verificationRetryAttempted || 0),
    verificationRetryValid: Number(verificationRetry.verificationRetryValid || 0),
    verificationRetryRisky: Number(verificationRetry.verificationRetryRisky || 0),
    verificationRetryInvalid: Number(verificationRetry.verificationRetryInvalid || 0),
    verificationRetryFailed: Number(verificationRetry.verificationRetryFailed || 0),
    verificationRetryFailureReasons: verificationRetryFailureReasons(verificationRetry.verificationRetryFailureReasons),
    verificationRetryLoadFailed: Number(verificationRetry.verificationRetryLoadFailed || 0),
    yield: scout?.yield || null,
    lossBuckets: scout?.lossBuckets || null,
    funnel,
    lastReplenishmentAttemptAt,
    lastInventoryGrowthAt,
    lastNewPromotionAt,
    lastRecoveryAt,
    lastSuccessfulReplenishmentAt,
    lastSuccessfulPromotionAt,
  };
  if (recoveryState.mode === 'legacy') await store.event('max_outbound_control', [program.id, cycleId], controlEvent);
  if (scout) {
    emitDiagnostic(logger, '[max-outbound-control] Scout provider receipt', controlEvent.scoutProviderReceipt);
    emitDiagnostic(logger, '[max-outbound-control] Scout admission receipt', controlEvent.scoutAdmissionReceipt);
  }
  emitDiagnostic(logger, '[max-outbound-control] measured summary',
    { ...controlCycleDiagnostic(store.tenantId, controlEvent),
      eventWriteCompleted: recoveryState.mode === 'legacy' }
  );

  return {
    cycleId,
    ramp,
    cycleStartedAt,
    cycleCompletedAt: new Date().toISOString(),
    programId: program.id,
    mode: program.mode,
    emmett: {
      recommendedSafeDailyCapacity: finalPlan.recommendedSafeDailyCapacity,
      authorizationLimitedCapacity: finalPlan.authorizationLimitedCapacity,
      scheduleLimitedCapacity: finalPlan.scheduleLimitedCapacity,
      nextEligibleScheduleCapacity: finalPlan.nextEligibleScheduleCapacity,
      dispatchCapacityNow: finalPlan.dispatchCapacityNow,
      planningDailyCapacity: finalPlan.planningDailyCapacity,
      dispatchableDailyCapacity: finalPlan.dispatchableDailyCapacity,
      effectiveDailyCapacity: finalPlan.effectiveDailyCapacity,
      limitingFactor: finalPlan.limitingFactor,
      capacityReason: finalPlan.capacityReason,
      safeCapacity: finalPlan.planningDailyCapacity,
      dispatchUnavailableNow: finalPlan.dispatchUnavailableNow,
      governor: finalPlan.governor || infrastructure.assessed?.governor?.outcome || null,
      healthScore: finalPlan.healthScore ?? infrastructure.assessed?.health?.score ?? null,
    },
    plan: {
      ...finalPlan,
      lastReplenishmentAttemptAt,
      lastInventoryGrowthAt,
      lastNewPromotionAt,
      lastRecoveryAt,
      lastSuccessfulReplenishmentAt,
      lastSuccessfulPromotionAt,
      exclusionCounts: inventoryAfter.exclusionCounts || {},
    },
    scout,
    discoveryBlockedReason,
    aoRefill,
    funnel,
    yield: scout?.yield || null,
    excludedCount: inventoryAfter.excluded.length,
    cleanInventoryExclusions: inventoryAfter.exclusionCounts || {},
    inventoryGrowth,
    lastReplenishmentAttemptAt,
    lastInventoryGrowthAt,
    lastNewPromotionAt,
    lastRecoveryAt,
    lastSuccessfulReplenishmentAt,
    lastSuccessfulPromotionAt,
    lossBuckets: scout?.lossBuckets || null,
    sourceScope: inventoryAfter.scope,
    ...preparation,
    sameCompanyDifferentContact,
    emailNotVerified: Number(
      verificationRetry.emailNotVerified
      || inventoryAfter.exclusionCounts?.email_not_verified
      || 0
    ),
    verificationRetryValid: Number(verificationRetry.verificationRetryValid || 0),
    verificationRetry: scout?.verificationRetry || null,
  };
}

module.exports = {
  DEFAULT_TARGET_DAYS,
  MAX_ENRICHMENT_BATCHES_PER_CYCLE,
  ENRICHABLE_SCOUT_VERTICALS,
  buildControlPlan,
  resolveOperatingCapacity,
  sourceScope,
  confirmedServiceAreaMatch,
  missionCandidateReason,
  loadCleanInventory,
  runMaxOutboundControlLoop,
  capturePreparationObservability,
  applyPreparationRefill,
  defaultScoutRamp,
  resolveReplenishmentTenantContext,
  resolveScoutRampAllowedCities,
  scoutSearchFingerprint,
  loadScoutDiscoveryBackoff,
  _test: {
    recordReplenishmentHold,
    scoutInput,
    mapReuseCompanyRows,
    loadReuseCompanies,
    persistDiscoveredCompanies,
    runEnrichmentBatches,
    defaultScoutRamp,
    resolveReplenishmentTenantContext,
    resolveScoutRampAllowedCities,
    scoutSearchFingerprint,
    loadScoutDiscoveryBackoff,
  },
};
