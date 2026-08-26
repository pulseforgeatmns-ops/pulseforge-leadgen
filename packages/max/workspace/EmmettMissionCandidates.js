'use strict';

/**
 * SPEC-068 — Mission-bound candidate set for Emmett queue cognition.
 * Lineage: Scout qualified opportunities → Max prioritization → Emmett queue.
 */

const { SPECIALISTS, CONTRIBUTION_KINDS, asText } = require('../../acquisition-mission/types');

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
 * Build queue candidates strictly from mission contributions — never client-wide CRM.
 */
function buildMissionBoundCandidates(mission, contributions = []) {
  const scoutRow = findLatestScoutDiscovery(contributions);
  const maxRow = findMaxPrioritization(contributions);
  const paigeRow = findPaigeVariants(contributions);
  const scoutPayload = scoutRow?.payload || {};
  const maxPayload = maxRow?.payload || {};
  const paigePayload = paigeRow?.payload || {};
  const paigeReady = buildPaigeReadinessMetadata(paigePayload);
  const primaryVariant = (paigePayload.variants || [])[0] || null;
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
    if (opp.name) oppByKey.set(String(opp.name).toLowerCase(), opp);
  }

  const vertical = segmentLabel || 'unknown';

  const usedProspectIds = new Set();
  const candidates = [];

  const addCandidate = (target, index) => {
    const name = target.name || target.segment || target.label;
    const opp = oppByKey.get(String(target.companyId || ''))
      || oppByKey.get(String(target.id || ''))
      || (name ? oppByKey.get(String(name).toLowerCase()) : null)
      || {};
    const prospect = prospects.find((row) =>
      (target.companyId && row.companyId === target.companyId)
      || (name && row.company === name)
      || (name && row.name && String(row.name).includes(String(name).split(' ')[0])))
      || prospects.find((row) => !usedProspectIds.has(row.id));

    if (prospect?.id) usedProspectIds.add(prospect.id);

    const rank = Number(target.rank || index + 1);
    const fit = target.fit != null ? Number(target.fit) : (opp.fit != null ? Number(opp.fit) : 0.7);
    const timing = target.timing != null ? Number(target.timing) : (opp.timing != null ? Number(opp.timing) : 0.5);
    const signals = target.signals || opp.signals || [];
    const maxPriority = Math.max(0.1, 1 - (rank - 1) * 0.12);

    const row = {
      id: target.companyId || prospect?.id || `mission-target-${rank}`,
      prospectId: prospect?.id || null,
      email: prospect?.email || null,
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

    if (paigeReady.ready && primaryVariant) {
      row.paige = {
        author: 'paige',
        source: 'paige',
        ready: true,
        variantLabel: primaryVariant.label || 'Primary',
        subject: primaryVariant.subject || paigePayload.subjects?.[0] || null,
        body: primaryVariant.body || paigePayload.messaging || null,
      };
      row.contentSource = 'paige';
    }

    candidates.push(row);
  };

  if (rankedTargets.length) {
    rankedTargets.forEach((target, index) => addCandidate(target, index));
  } else if (scoutPayload.rankedProspects?.length) {
    scoutPayload.rankedProspects.forEach((row, index) => addCandidate({
      rank: row.rank || index + 1,
      companyId: row.id,
      name: row.name,
      fit: row.fit,
      timing: row.timing,
      signals: row.signals,
      rationale: row.rationale,
    }, index));
  } else if (opportunities.length) {
    opportunities.forEach((opp, index) => addCandidate({
      rank: index + 1,
      companyId: opp.companyId || opp.id,
      name: opp.name,
      fit: opp.fit,
      timing: opp.timing,
      signals: opp.signals,
    }, index));
  } else if (scoutPayload.companies?.length) {
    scoutPayload.companies.forEach((company, index) => addCandidate({
      rank: index + 1,
      companyId: company.id,
      name: company.name,
    }, index));
  }

  return candidates;
}

module.exports = {
  latestContribution,
  findLatestScoutDiscovery,
  findMaxPrioritization,
  findPaigeVariants,
  buildPaigeReadinessMetadata,
  buildMissionBoundCandidates,
};
