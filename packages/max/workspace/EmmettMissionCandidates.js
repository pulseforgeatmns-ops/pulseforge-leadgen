'use strict';

/**
 * SPEC-068 — Mission-bound candidate set for Emmett queue cognition.
 * Lineage: Scout qualified opportunities → Max prioritization → Emmett queue.
 * SPEC-212 — Candidates matched to bound message variants by candidateId.
 */

const { SPECIALISTS, CONTRIBUTION_KINDS, asText, MESSAGE_BINDING_SCOPES } = require('../../acquisition-mission/types');
const { unwrapSpecialistPayload } = require('../../acquisition-mission/ContributionSupersession');
const { resolveMissionBoundRecipientEmail } = require('./MissionBoundCrmResolver');
const {
  isGooglePlaceId,
  prospectIdentity,
  identityKeysFrom,
  canonicalOutboundIdentity,
} = require('./CanonicalOutboundIdentity');

function latestContribution(contributions = [], specialist, kind) {
  return [...contributions]
    .reverse()
    .find((row) => row.specialist === specialist && (!kind || row.kind === kind));
}

function findLatestScoutDiscovery(contributions = []) {
  return latestContribution(contributions, SPECIALISTS.SCOUT, CONTRIBUTION_KINDS.DISCOVERY);
}

function findMaxPrioritization(contributions = []) {
  return latestContribution(contributions, SPECIALISTS.MAX, CONTRIBUTION_KINDS.PRIORITIZATION);
}

function findPaigeVariants(contributions = []) {
  return latestContribution(contributions, SPECIALISTS.PAIGE, CONTRIBUTION_KINDS.VARIANTS);
}

function signalObservedAt(signals = []) {
  for (const sig of signals) {
    if (sig && sig.observedAt) return sig.observedAt;
    if (sig && sig.timestamp) return sig.timestamp;
  }
  return null;
}

function buildPaigeReadinessMetadata(paigePayload = {}) {
  const variants = Array.isArray(paigePayload.variants) ? paigePayload.variants : [];
  const primary = variants[0] || null;
  return {
    ready: variants.length > 0,
    variantCount: variants.length,
    primaryLabel: primary?.label || null,
    hasSubjects: Boolean(paigePayload.subjects?.length || primary?.subject),
    hasCta: Boolean(paigePayload.cta || primary?.cta),
    experiments: Array.isArray(paigePayload.experiments) ? paigePayload.experiments.length : 0,
  };
}

function isCompanyLevelTarget(target, segmentLabel) {
  if (!target || typeof target !== 'object') return false;
  if (target.companyId || target.id) return true;
  const name = asText(target.name || target.label);
  if (!name) return false;
  if (segmentLabel && name.toLowerCase() === String(segmentLabel).toLowerCase()) return false;
  if (target.segment && !target.companyId && target.fit == null && target.timing == null) return false;
  return true;
}

/**
 * SPEC-212 — Find the variant bound to this specific candidate identity.
 * Variants are bound to prospect intelligence and must not be cross-assigned.
 * Place IDs / company IDs / candidate IDs are aliases of the same Max identity.
 */
function variantIdentityKeys(variant = {}) {
  return identityKeysFrom({
    candidateId: variant.candidateId,
    id: variant.id,
    companyId: variant.companyId,
    placeId: variant.placeId,
  });
}

function findBoundVariant(variants = [], candidateId) {
  if (!Array.isArray(variants) || !variants.length) return null;
  const keys = Array.isArray(candidateId)
    ? candidateId.map((value) => String(value || '').trim()).filter(Boolean)
    : [String(candidateId || '').trim()].filter(Boolean);
  if (keys.length) {
    const keySet = new Set(keys);
    const exactMatch = variants.find((variant) =>
      variantIdentityKeys(variant).some((key) => keySet.has(key))
    );
    if (exactMatch) return exactMatch;
  }
  if (variants.length === 1 && variants[0].bindingScope === MESSAGE_BINDING_SCOPES.MISSION) {
    return variants[0];
  }
  return null;
}

/**
 * Build queue candidates strictly from mission contributions — never client-wide CRM.
 * SPEC-212: Each candidate receives the message variant bound to its candidateId.
 */
function contributionBody(row) {
  return unwrapSpecialistPayload(row) || {};
}

function buildMissionBoundCandidates(mission, contributions = [], opts = {}) {
  const crmByProspectId = opts.crmByProspectId || null;
  const scoutRow = findLatestScoutDiscovery(contributions);
  const maxRow = findMaxPrioritization(contributions);
  const paigeRow = findPaigeVariants(contributions);
  const scoutPayload = contributionBody(scoutRow);
  const maxPayload = contributionBody(maxRow);
  const paigePayload = contributionBody(paigeRow);
  const paigeReady = buildPaigeReadinessMetadata(paigePayload);
  const plan = mission.structuredMission || mission.missionPlanDraft || {};
  const segmentLabel = plan.market?.label || plan.market?.segment || mission.targetSegment;

  const opportunities = scoutPayload.opportunities || [];
  const prospects = scoutPayload.prospects || [];
  let rankedTargets = (maxPayload.rankedTargets || []).filter((row) => isCompanyLevelTarget(row, segmentLabel));
  if (!rankedTargets.length) {
    rankedTargets = (maxPayload.priorities || []).filter((row) => isCompanyLevelTarget(row, segmentLabel));
  }

  const oppByKey = new Map();
  for (const opp of opportunities) {
    if (opp.companyId) oppByKey.set(String(opp.companyId), opp);
    if (opp.id) oppByKey.set(String(opp.id), opp);
    if (opp.placeId) oppByKey.set(String(opp.placeId), opp);
    if (opp.name) oppByKey.set(String(opp.name).toLowerCase(), opp);
  }

  const vertical = segmentLabel || 'unknown';

  const candidates = [];

  const addCandidate = (target, index) => {
    const name = target.name || target.segment || target.label;
    const opp = oppByKey.get(String(target.companyId || ''))
      || oppByKey.get(String(target.id || ''))
      || oppByKey.get(String(target.placeId || ''))
      || (name ? oppByKey.get(String(name).toLowerCase()) : null)
      || {};
    const prospect = prospects.find((row) =>
      (target.companyId && row.companyId === target.companyId)
      || (target.id && (row.companyId === target.id || row.id === target.id))
      || (target.placeId && (row.placeId === target.placeId || row.id === target.placeId))
      || (name && row.company === name)
      || (name && row.name && String(row.name).includes(String(name).split(' ')[0])));

    const resolvedProspectId = prospectIdentity(prospect);

    const rank = Number(target.rank || index + 1);
    const fit = target.fit != null ? Number(target.fit) : (opp.fit != null ? Number(opp.fit) : 0.7);
    const timing = target.timing != null ? Number(target.timing) : (opp.timing != null ? Number(opp.timing) : 0.5);
    const signals = target.signals || opp.signals || [];
    const maxPriority = Math.max(0.1, 1 - (rank - 1) * 0.12);

    const identity = canonicalOutboundIdentity(target, {
      placeId: opp.placeId || opp.place_id || prospect?.placeId,
      prospect: prospect || {},
      website: opp.website || opp.url || prospect?.website || prospect?.url,
      fallbackId: `mission-target-${rank}`,
    });
    const candidateId = identity.candidateId;
    const crmProspectId = identity.crmProspectId || resolvedProspectId || null;
    const queueProspectId = candidateId;

    const row = {
      id: candidateId,
      candidateId,
      companyId: identity.companyId || candidateId,
      placeId: identity.placeId || (isGooglePlaceId(candidateId) ? candidateId : null),
      crmProspectId,
      prospectId: queueProspectId,
      domain: identity.domain || null,
      email: resolveMissionBoundRecipientEmail({
        discoveryEmail: prospect?.email,
        missionBoundKey: candidateId,
        prospectId: crmProspectId,
        companyId: identity.companyId,
        domain: identity.domain,
        crmByProspectId,
      }),
      company: name || opp.name || prospect?.company || `Target ${rank}`,
      vertical: String(vertical).toLowerCase(),
      maxPriority,
      maxReason: target.rationale || maxPayload.objectiveReason || null,
      buyingSignalAt: signalObservedAt(signals),
      icpScore: Math.round(fit * 100),
      expectedResponse: Math.min(0.25, timing * 0.12 + fit * 0.05),
      missionBound: true,
      scoutRank: rank,
      source: 'mission_intelligence',
    };

    if (paigeReady.ready && paigePayload.variants?.length) {
      const boundVariant = findBoundVariant(
        paigePayload.variants,
        identity.keys
      );
      if (boundVariant) {
        row.paige = {
          author: 'paige',
          source: 'paige',
          ready: true,
          variantLabel: boundVariant.label || 'Primary',
          subject: boundVariant.subject || null,
          body: boundVariant.body || null,
          candidateId: boundVariant.candidateId || String(candidateId),
          variantId: boundVariant.variantId || null,
          bindingScope: boundVariant.bindingScope || 'prospect',
          attributableIntelligence: boundVariant.attributableIntelligence || null,
        };
        row.contentSource = 'paige';
        row.cta = boundVariant.cta || paigePayload.cta || 'Reply to schedule a walkthrough';
      }
    }

    candidates.push(row);
  };

  if (rankedTargets.length) {
    rankedTargets.forEach((target, index) => addCandidate(target, index));
  } else if (scoutPayload.rankedProspects?.length) {
    scoutPayload.rankedProspects.forEach((row, index) => addCandidate({
      rank: row.rank || index + 1,
      id: row.id || row.companyId || row.placeId,
      companyId: row.companyId || row.id,
      placeId: row.placeId || row.place_id,
      name: row.name,
      fit: row.fit,
      timing: row.timing,
      signals: row.signals,
      rationale: row.rationale,
      website: row.website || row.url,
    }, index));
  } else if (opportunities.length) {
    opportunities.forEach((opp, index) => addCandidate({
      rank: index + 1,
      id: opp.id || opp.companyId || opp.placeId,
      companyId: opp.companyId || opp.id,
      placeId: opp.placeId || opp.place_id,
      name: opp.name,
      fit: opp.fit,
      timing: opp.timing,
      signals: opp.signals,
      website: opp.website || opp.url,
    }, index));
  } else if (scoutPayload.companies?.length) {
    scoutPayload.companies.forEach((company, index) => addCandidate({
      rank: index + 1,
      id: company.id || company.placeId,
      companyId: company.id,
      placeId: company.placeId || company.place_id,
      name: company.name,
      website: company.website || company.url,
    }, index));
  }

  return candidates;
}

/**
 * Scout contact / CRM prospect row IDs when discovery attached a separate people record.
 * May be null when only company-level targets exist; do not use as the mission universe key.
 */
function listMissionBoundProspectIds(mission, contributions = [], opts = {}) {
  return [...new Set(
    buildMissionBoundCandidates(mission, contributions, opts)
      .map((row) => row.crmProspectId)
      .filter(Boolean)
      .map(String)
  )];
}

function listMissionBoundCrmLookupKeys(mission, contributions = [], opts = {}) {
  return [...new Set(
    buildMissionBoundCandidates(mission, contributions, opts)
      .flatMap((row) => identityKeysFrom(row))
      .filter(Boolean)
      .map(String)
  )];
}

/**
 * Canonical mission-bound company/candidate IDs (Max rankedTargets.id / companyId).
 * Queue items expose this value as prospectId for SPEC-212 compatibility — it is not prospects.id.
 */
function listMissionBoundCompanyIds(mission, contributions = [], opts = {}) {
  return [...new Set(
    buildMissionBoundCandidates(mission, contributions, opts)
      .map((row) => row.id)
      .filter(Boolean)
      .map(String)
  )];
}

module.exports = {
  latestContribution,
  findLatestScoutDiscovery,
  findMaxPrioritization,
  findPaigeVariants,
  buildPaigeReadinessMetadata,
  prospectIdentity,
  findBoundVariant,
  buildMissionBoundCandidates,
  listMissionBoundProspectIds,
  listMissionBoundCompanyIds,
  listMissionBoundCrmLookupKeys,
};
