'use strict';

/**
 * SPEC-193 / ADR-077 — Post-Discovery decision readiness.
 * pendingOperatorDecision after Scout commits must reflect executable evidence state.
 */

const { STAGES, OPERATOR_DECISION_KINDS } = require('./types');
const { presentationFromDiscoveryPayload } = require('./DiscoveryPresentation');
const { hasSufficientEvidenceForPrioritization } = require('./DiscoveryPayload');
const { READINESS_STATES } = require('../max/scoutAcquisition/Types');

const INVESTIGATION_CHOICES = Object.freeze([
  { label: 'Continue investigation' },
  { label: 'Modify mission' },
  { label: 'Cancel' },
]);

function hasTypedBuyingSignals(presentation) {
  const signals = (presentation && presentation.buyingSignalsRaw) || [];
  return signals.some((signal) => {
    if (typeof signal === 'object') return Boolean(signal.label && signal.type);
    return String(signal).split(/\s+/).length >= 2;
  });
}

function hasRealProvenance(presentation) {
  const evidenceItems = (presentation && presentation.evidenceRaw) || [];
  return evidenceItems.some((item) => {
    if (typeof item === 'object') {
      return item.source && !/test fixture/i.test(String(item.source));
    }
    return item && String(item).toLowerCase() !== 'fixture';
  });
}

function candidateProductionReason(presentation, discoveryPayload = {}) {
  const fitCandidates =
    discoveryPayload.discoveryArtifact?.fitCandidates ||
    discoveryPayload.fitCandidates ||
    [];
  if (fitCandidates.length > 0) {
    return `${fitCandidates.length} qualified prospect${fitCandidates.length === 1 ? '' : 's'} found; buying readiness is unknown for all.`;
  }
  const rawCount =
    discoveryPayload.candidateUniverseCount != null
      ? Number(discoveryPayload.candidateUniverseCount)
      : presentation && presentation.candidateUniverseCount != null
        ? Number(presentation.candidateUniverseCount)
        : null;
  if (rawCount != null && rawCount > 0) {
    return `Google Places returned ${rawCount} business${rawCount === 1 ? '' : 'es'}, but none became qualified prospects.`;
  }
  const providerExecution = presentation && presentation.providerExecution;
  if (Array.isArray(providerExecution) && providerExecution.length) {
    const succeeded = providerExecution.filter((row) => row && row.succeeded !== false);
    if (succeeded.length) {
      return 'Discovery providers returned businesses, but none became qualified prospects.';
    }
  }
  return 'No ranked prospects were produced from the current investigation.';
}

function coverageIncompleteReason(presentation) {
  const coverage = presentation && presentation.coverage;
  if (coverage && Array.isArray(coverage.warnings) && coverage.warnings.length) {
    return coverage.warnings.join(' ');
  }
  if (coverage && coverage.cities) {
    const searched = coverage.cities.searched != null ? coverage.cities.searched : 0;
    const planned = coverage.cities.planned != null ? coverage.cities.planned : 0;
    if (planned > searched) {
      return `Only ${searched} / ${planned} cities searched.`;
    }
  }
  return 'Discovery coverage is incomplete.';
}

/**
 * Evaluate whether a committed Discovery artifact can advertise prioritization approval.
 * @param {object} discoveryPayload
 * @returns {{ sufficient: boolean, presentation: object, blockers: object[], primaryBlocker: object|null }}
 */
function evaluatePrioritizationReadiness(discoveryPayload = {}) {
  const presentation = presentationFromDiscoveryPayload(discoveryPayload);
  const sufficient = hasSufficientEvidenceForPrioritization(presentation);
  const blockers = [];

  if (presentation.blocked) {
    blockers.push({
      code: discoveryPayload.blockerCode || 'discovery_blocked',
      label: 'Discovery Blocked',
      reason:
        presentation.summary
        || discoveryPayload.summary
        || 'Discovery did not complete successfully.',
      recommendedAction: 'Continue investigation',
      waitingOn: 'More discovery evidence',
    });
  }

  if (presentation.discoveryStatus === 'incomplete') {
    blockers.push({
      code: 'coverage_incomplete',
      label: 'Discovery Coverage Incomplete',
      reason: coverageIncompleteReason(presentation),
      recommendedAction: 'Continue investigation',
      waitingOn: 'More discovery evidence',
    });
  }

  if (!presentation.rankedProspects || !presentation.rankedProspects.length) {
    blockers.push({
      code: 'no_ranked_prospects',
      label: 'No Prioritizable Prospects',
      reason: candidateProductionReason(presentation, discoveryPayload),
      recommendedAction: 'Continue investigation',
      waitingOn: 'More discovery evidence',
    });
  } else if (!hasTypedBuyingSignals(presentation)) {
    const hasUnknownReadiness = (presentation.rankedProspects || []).some(
      (row) =>
        row.readinessState === READINESS_STATES.UNKNOWN || row.readinessState == null
    );
    if (!hasUnknownReadiness) {
      blockers.push({
        code: 'missing_buying_signals',
        label: 'Insufficient Buying Signals',
        reason: 'Ranked prospects exist but typed buying signals are missing.',
        recommendedAction: 'Continue investigation',
        waitingOn: 'More discovery evidence',
      });
    }
  } else if (!hasRealProvenance(presentation)) {
    blockers.push({
      code: 'missing_provenance',
      label: 'Insufficient Evidence Provenance',
      reason: 'Ranked prospects and signals exist but attributable provenance is missing.',
      recommendedAction: 'Continue investigation',
      waitingOn: 'More discovery evidence',
    });
  }

  return {
    sufficient,
    presentation,
    blockers,
    primaryBlocker: blockers[0] || null,
  };
}

/**
 * Canonical post-Discovery pending operator decision.
 * @param {object} discoveryPayload
 * @returns {object}
 */
function buildPostDiscoveryPendingDecision(discoveryPayload = {}) {
  const readiness = evaluatePrioritizationReadiness(discoveryPayload);
  if (readiness.sufficient) {
    return {
      stage: STAGES.DISCOVER,
      kind: OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL,
      prompt: 'Approve prioritization?',
    };
  }

  const blocker = readiness.primaryBlocker || {};
  return {
    stage: STAGES.DISCOVER,
    kind: OPERATOR_DECISION_KINDS.DISCOVERY_INVESTIGATION,
    prompt: 'Continue investigation?',
    blocker: blocker.label || 'More discovery evidence required',
    reason: blocker.reason || 'Discovery evidence is insufficient for prioritization.',
    recommendedAction: blocker.recommendedAction || 'Continue investigation',
    waitingOn: blocker.waitingOn || 'More discovery evidence',
    choices: INVESTIGATION_CHOICES.map((row) => ({ ...row })),
    blockers: readiness.blockers,
  };
}

function discoveryNeedsInvestigation(discoveryPayload = {}) {
  return !evaluatePrioritizationReadiness(discoveryPayload).sufficient;
}

module.exports = {
  evaluatePrioritizationReadiness,
  buildPostDiscoveryPendingDecision,
  discoveryNeedsInvestigation,
  INVESTIGATION_CHOICES,
};
