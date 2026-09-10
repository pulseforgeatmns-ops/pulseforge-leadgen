'use strict';

/**
 * SPEC-248 — Canonical acquisition approach decision.
 * Max owns the mission-level approach; channel specialists remain advisory/execution-bound.
 */

const {
  ACQUISITION_APPROACHES,
  BLOCKER_KINDS,
  CONTRIBUTION_KINDS,
  SPECIALISTS,
  asText,
  clone,
  nowIso,
} = require('./types');

const SUPPORTED_PREPARATION_APPROACHES = new Set([
  ACQUISITION_APPROACHES.OUTBOUND,
  ACQUISITION_APPROACHES.BOTH,
]);

function normalizeApproach(value) {
  const key = asText(value).toLowerCase();
  if (key === 'email' || key === 'outreach' || key === 'outbound_email') {
    return ACQUISITION_APPROACHES.OUTBOUND;
  }
  if (key === 'paid_acquisition' || key === 'paid_media' || key === 'ads') {
    return ACQUISITION_APPROACHES.PAID;
  }
  if (Object.values(ACQUISITION_APPROACHES).includes(key)) return key;
  return '';
}

function findLatestAcquisitionApproach(contributions = []) {
  return [...(contributions || [])]
    .reverse()
    .find(
      (row) =>
        row.specialist === SPECIALISTS.MAX &&
        row.kind === CONTRIBUTION_KINDS.ACQUISITION_APPROACH
    ) || null;
}

function approachFromContribution(row) {
  const payload = (row && row.payload) || {};
  const decision = payload.acquisitionApproach || payload.approachDecision || payload;
  const selected = normalizeApproach(
    decision.selectedApproach || decision.approach || payload.selectedApproach || payload.approach
  );
  return selected ? { payload, decision, selected } : null;
}

function latestApproachDecision(contributions = []) {
  const row = findLatestAcquisitionApproach(contributions);
  const parsed = approachFromContribution(row);
  return parsed ? { ...parsed, contribution: row } : null;
}

function approachPermitsOutbound(contributions = []) {
  const parsed = latestApproachDecision(contributions);
  return Boolean(parsed && SUPPORTED_PREPARATION_APPROACHES.has(parsed.selected));
}

function approachRequiresUnsupportedCapability(contributions = []) {
  const parsed = latestApproachDecision(contributions);
  if (!parsed) return false;
  return parsed.selected === ACQUISITION_APPROACHES.PAID;
}

function buildApproachBlocker(decision) {
  if (!decision || !decision.selected) {
    return {
      kind: BLOCKER_KINDS.WAITING_FOR_ACQUISITION_APPROACH,
      specialist: SPECIALISTS.MAX,
      reason: 'Acquisition approach decision is required before channel-specific preparation.',
    };
  }
  if (decision.selected === ACQUISITION_APPROACHES.PAID) {
    return {
      kind: BLOCKER_KINDS.UNSUPPORTED_ACQUISITION_APPROACH,
      specialist: SPECIALISTS.MAX,
      reason: 'Paid acquisition preparation is not implemented for this mission yet.',
    };
  }
  if (decision.selected === ACQUISITION_APPROACHES.DEFER) {
    return {
      kind: BLOCKER_KINDS.ACQUISITION_APPROACH_DEFERRED,
      specialist: SPECIALISTS.MAX,
      reason: decision.decision.rationale || 'Max deferred channel-specific preparation.',
    };
  }
  if (decision.selected === ACQUISITION_APPROACHES.BLOCKED) {
    const blocker = Array.isArray(decision.decision.blockers) ? decision.decision.blockers[0] : null;
    return {
      kind: BLOCKER_KINDS.ACQUISITION_APPROACH_BLOCKED,
      specialist: SPECIALISTS.MAX,
      reason:
        (blocker && (blocker.reason || blocker.label)) ||
        decision.decision.rationale ||
        'Acquisition approach is blocked.',
    };
  }
  return null;
}

function summarizeApproachNextStep(selected) {
  if (selected === ACQUISITION_APPROACHES.OUTBOUND) {
    return 'Proceed to outbound preparation.';
  }
  if (selected === ACQUISITION_APPROACHES.BOTH) {
    return 'Proceed to outbound preparation; paid preparation remains unsupported until a later capability is added.';
  }
  if (selected === ACQUISITION_APPROACHES.PAID) {
    return 'Stop before outbound preparation; paid preparation is not implemented yet.';
  }
  if (selected === ACQUISITION_APPROACHES.DEFER) {
    return 'Stop before channel-specific preparation until more evidence or operator direction exists.';
  }
  return 'Stop before channel-specific preparation until blockers are resolved.';
}

function createAcquisitionApproachPayload(input = {}) {
  const selected = normalizeApproach(input.selectedApproach || input.approach)
    || ACQUISITION_APPROACHES.BLOCKED;
  const rationale = asText(input.rationale) || (
    selected === ACQUISITION_APPROACHES.OUTBOUND
      ? 'Ranked prospects and current mission evidence support a bounded outbound motion.'
      : selected === ACQUISITION_APPROACHES.BOTH
        ? 'Mission evidence supports outbound now while preserving paid acquisition as a future parallel path.'
        : selected === ACQUISITION_APPROACHES.PAID
          ? 'Mission evidence points to paid acquisition, but paid preparation is not implemented yet.'
          : selected === ACQUISITION_APPROACHES.DEFER
            ? 'Current evidence does not justify channel-specific preparation yet.'
            : 'Current evidence is insufficient or blocked.'
  );
  const confidence = input.confidence != null ? Number(input.confidence) : (
    selected === ACQUISITION_APPROACHES.BLOCKED || selected === ACQUISITION_APPROACHES.DEFER ? 0.35 : 0.7
  );
  const evidence = Array.isArray(input.evidence) ? clone(input.evidence) : [];
  const blockers = Array.isArray(input.blockers) ? clone(input.blockers) : [];
  const unknowns = Array.isArray(input.unknowns) ? clone(input.unknowns) : [];
  const constraints = Array.isArray(input.constraints) ? clone(input.constraints) : [];
  const requiresSpecialistEvidence = input.requiresSpecialistEvidence === true
    || selected === ACQUISITION_APPROACHES.PAID
    || selected === ACQUISITION_APPROACHES.BOTH;

  return {
    acquisitionApproach: {
      spec: 'SPEC-248',
      version: 1,
      selectedApproach: selected,
      approach: selected,
      rationale,
      confidence,
      evidence,
      constraints,
      blockers,
      unknowns,
      requiresSpecialistEvidence,
      unsupportedCapabilities: (
        selected === ACQUISITION_APPROACHES.PAID ||
        selected === ACQUISITION_APPROACHES.BOTH
      )
        ? ['paid_preparation']
        : [],
      decidedAt: input.decidedAt || nowIso(),
      nextStep: asText(input.nextStep) || summarizeApproachNextStep(selected),
    },
    selectedApproach: selected,
    approach: selected,
    rationale,
    confidence,
    evidence,
    constraints,
    blockers,
    unknowns,
    requiresSpecialistEvidence,
    recommendations: [summarizeApproachNextStep(selected)],
  };
}

module.exports = {
  SUPPORTED_PREPARATION_APPROACHES,
  normalizeApproach,
  findLatestAcquisitionApproach,
  latestApproachDecision,
  approachPermitsOutbound,
  approachRequiresUnsupportedCapability,
  buildApproachBlocker,
  summarizeApproachNextStep,
  createAcquisitionApproachPayload,
};
