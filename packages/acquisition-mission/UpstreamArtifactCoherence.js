'use strict';

/**
 * Guard against READY / APPROVE_EXECUTION when canonical upstream artifacts disagree.
 */

const { SPECIALISTS, CONTRIBUTION_KINDS } = require('./types');
const { selectCanonicalContribution } = require('./CanonicalContributionSelection');
const { unwrapSpecialistPayload } = require('./ContributionSupersession');
const { evaluatePrioritizationReadiness } = require('./DecisionReadiness');

function countScoutCandidates(contributionRow) {
  if (!contributionRow) return 0;
  const body = contributionRow.payload?.payload && typeof contributionRow.payload.payload === 'object'
    ? contributionRow.payload.payload
    : (contributionRow.payload || {});
  if (Number.isFinite(Number(body.qualifiedCount))) return Number(body.qualifiedCount);
  if (Number.isFinite(Number(body.candidateUniverseCount))) return Number(body.candidateUniverseCount);
  if (Array.isArray(body.candidateUniverse) && body.candidateUniverse.length) {
    return body.candidateUniverse.length;
  }
  if (Number.isFinite(Number(body.rankedProspectCount))) return Number(body.rankedProspectCount);
  if (Array.isArray(body.rankedProspects) && body.rankedProspects.length) {
    return body.rankedProspects.length;
  }
  if (Array.isArray(body.opportunities) && body.opportunities.length) {
    return body.opportunities.length;
  }
  const artifact = body.discoveryArtifact;
  if (artifact && Array.isArray(artifact.rankedProspects) && artifact.rankedProspects.length) {
    return artifact.rankedProspects.length;
  }
  return 0;
}

function evaluateUpstreamArtifactCoherence(mission, contributions = []) {
  const scout = selectCanonicalContribution(contributions, {
    missionId: mission?.id,
    specialist: SPECIALISTS.SCOUT,
    kind: CONTRIBUTION_KINDS.DISCOVERY,
    mission,
  });
  const max = selectCanonicalContribution(contributions, {
    missionId: mission?.id,
    specialist: SPECIALISTS.MAX,
    kind: CONTRIBUTION_KINDS.PRIORITIZATION,
    mission,
  });

  const scoutPayload = scout ? unwrapSpecialistPayload(scout) : null;
  const discoveryReadiness = scoutPayload ? evaluatePrioritizationReadiness(scoutPayload) : null;
  const candidates = scout ? countScoutCandidates(scout) : 0;
  const rankedCount = max ? (
    Array.isArray(max.payload?.rankedTargets) ? max.payload.rankedTargets.length
      : Array.isArray(max.payload?.priorities) ? max.payload.priorities.length
        : 0
  ) : 0;

  const blockers = [];
  if (!scout) blockers.push('missing_scout_discovery');
  if (candidates <= 0) blockers.push('scout_candidate_count_zero');
  if (discoveryReadiness && discoveryReadiness.sufficient !== true) {
    blockers.push(discoveryReadiness.primaryBlocker?.code || 'discovery_not_ready');
  }
  if (max && rankedCount > 0 && candidates <= 0) {
    blockers.push('max_without_scout_candidates');
  }
  if (!max && mission?.stage === 'ready') {
    blockers.push('missing_max_prioritization');
  }

  return {
    coherent: blockers.length === 0,
    scoutContributionId: scout?.id || null,
    maxContributionId: max?.id || null,
    scoutCandidateCount: candidates,
    rankedCount,
    discoveryReadiness: discoveryReadiness
      ? {
        sufficient: discoveryReadiness.sufficient === true,
        primaryBlocker: discoveryReadiness.primaryBlocker || null,
      }
      : null,
    prioritizationReady: discoveryReadiness?.sufficient === true,
    blockers,
  };
}

function isUpstreamArtifactChainCoherent(mission, contributions = []) {
  return evaluateUpstreamArtifactCoherence(mission, contributions).coherent;
}

module.exports = {
  evaluateUpstreamArtifactCoherence,
  isUpstreamArtifactChainCoherent,
};
