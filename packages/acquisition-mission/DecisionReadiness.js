'use strict';

/**
 * ADR-077 — Operator decisions are executable contracts.
 * Readiness is evaluated before rendering; presentation never duplicates these rules.
 */

const { STAGES, OPERATOR_DECISION_KINDS, SPECIALISTS, CONTRIBUTION_KINDS } = require('./types');
const { isStructuredMissionApproved, isReadyForLock } = require('./StructuredMission');
const {
  hasSufficientEvidenceForPrioritization,
} = require('./DiscoveryPayload');
const {
  presentationFromDiscoveryPayload,
  findLatestDiscoveryContribution,
} = require('./DiscoveryPresentation');

const PLAN_KINDS = new Set([
  OPERATOR_DECISION_KINDS.PLAN_APPROVAL,
  OPERATOR_DECISION_KINDS.PLAN_EDIT,
]);

function pendingKind(mission) {
  const pending = mission && mission.pendingOperatorDecision;
  return pending && pending.kind ? pending.kind : null;
}

function hasDiscoveryArtifact(snapshotOrMission, extras = {}) {
  const contributions = contributionsFrom(snapshotOrMission, extras);
  return contributions.some(
    (row) => row.specialist === SPECIALISTS.SCOUT && row.kind === CONTRIBUTION_KINDS.DISCOVERY
  );
}

function hasPendingPlanClarification(snapshot) {
  const mission = missionFrom(snapshot) || {};
  if (isStructuredMissionApproved(mission)) return false;
  if (mission.planCancelled) return false;
  return pendingKind(mission) === OPERATOR_DECISION_KINDS.PLAN_CLARIFICATION;
}

function hasPendingPlanApproval(snapshot) {
  const mission = missionFrom(snapshot) || {};
  if (isStructuredMissionApproved(mission)) return false;
  if (hasPendingPlanClarification(snapshot)) return false;
  if (mission.planCancelled) return false;
  return PLAN_KINDS.has(pendingKind(mission));
}

function hasPendingDiscoveryApproval(snapshot) {
  const mission = missionFrom(snapshot) || {};
  if (hasPendingPlanClarification(snapshot)) return false;
  if (hasPendingPlanApproval(snapshot)) return false;
  if (!isStructuredMissionApproved(mission)) return false;
  if (mission.stage && mission.stage !== STAGES.DISCOVER) return false;
  if (hasDiscoveryArtifact(snapshot)) return false;
  return pendingKind(mission) === OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL;
}

/**
 * @typedef {object} DecisionReadiness
 * @property {boolean} executable
 * @property {string} [blockingReason]
 * @property {string[]} [missingEvidence]
 * @property {string} [recommendedAction]
 * @property {number} [coveragePercent]
 */

function missionFrom(snapshotOrMission) {
  if (!snapshotOrMission) return null;
  return snapshotOrMission.mission || snapshotOrMission;
}

function contributionsFrom(snapshotOrMission, extras = {}) {
  if (extras.contributions) return extras.contributions;
  if (snapshotOrMission && snapshotOrMission.contributions) {
    return snapshotOrMission.contributions;
  }
  return [];
}

function discoveryPresentationFromSnapshot(snapshot, extras = {}) {
  if (snapshot && snapshot.discoveryArtifact) return snapshot.discoveryArtifact;
  const contributions = contributionsFrom(snapshot, extras);
  const row = findLatestDiscoveryContribution(contributions);
  if (!row) return null;
  return presentationFromDiscoveryPayload(row.payload || {});
}

function coverageRatioPercent(coverage) {
  const cov = coverage && typeof coverage === 'object' ? coverage : {};
  const ratios = [];
  for (const key of ['cities', 'concepts', 'sources', 'searches']) {
    const row = cov[key];
    if (!row || row.planned == null || Number(row.planned) <= 0) continue;
    const searched = Number(row.searched != null ? row.searched : row.executed || 0);
    const planned = Number(row.planned);
    ratios.push(Math.min(1, searched / planned));
  }
  if (!ratios.length) return null;
  return Math.round((ratios.reduce((sum, n) => sum + n, 0) / ratios.length) * 100);
}

function missingEvidenceFromPresentation(presentation) {
  if (!presentation) return ['Discovery artifact'];
  const missing = [];
  const coverage = presentation.coverage && typeof presentation.coverage === 'object'
    ? presentation.coverage
    : {};
  const plan = presentation.discoveryPlan || {};

  if (presentation.discoveryStatus === 'incomplete') {
    for (const key of ['cities', 'concepts', 'sources']) {
      const row = coverage[key];
      if (row && row.planned != null && row.searched != null && row.searched < row.planned) {
        const gap = Number(row.planned) - Number(row.searched);
        missing.push(`${gap} unsearched ${key}`);
      }
    }
    const pendingConcepts = plan.pendingConcepts || plan.missingConcepts || [];
    pendingConcepts.forEach((item) => {
      const label = typeof item === 'string' ? item : (item && item.label) || null;
      if (label) missing.push(label);
    });
    const pendingSources = plan.pendingSources || plan.missingSources || [];
    pendingSources.forEach((item) => {
      const label = typeof item === 'string' ? item : (item && item.label) || null;
      if (label) missing.push(label);
    });
    const warnings = coverage.warnings || [];
    warnings.slice(0, 5).forEach((warning) => {
      if (typeof warning === 'string' && !missing.includes(warning)) missing.push(warning);
    });
  }

  if (!presentation.rankedProspects || !presentation.rankedProspects.length) {
    missing.push('Ranked prospects');
  }
  if (!presentation.summary) {
    missing.push('Discovery summary');
  }

  const signals = presentation.buyingSignals || [];
  const hasSpecificSignals = signals.some((s) => {
    if (typeof s === 'object') return Boolean(s.label && s.type);
    return String(s).split(/\s+/).length >= 2;
  });
  if (!hasSpecificSignals) {
    missing.push('Attributable buying signals');
  }

  const evidenceItems = presentation.evidence || [];
  const hasProvenance = evidenceItems.some((e) => {
    if (typeof e === 'object') {
      return e.source && !/test fixture/i.test(String(e.source));
    }
    return e && String(e).toLowerCase() !== 'fixture';
  });
  if (!hasProvenance) {
    missing.push('Non-fixture evidence provenance');
  }

  return [...new Set(missing.filter(Boolean))];
}

/**
 * Evaluate readiness for prioritization approval (Approve findings).
 * @param {object} presentation
 * @returns {DecisionReadiness}
 */
function evaluatePrioritizationReadiness(presentation) {
  if (!presentation) {
    return {
      executable: false,
      blockingReason: 'Discovery artifact is missing.',
      missingEvidence: ['Discovery artifact'],
      recommendedAction: 'Run Scout investigation before approving findings.',
    };
  }

  if (presentation.blocked) {
    return {
      executable: false,
      blockingReason: presentation.summary || 'Discovery is blocked.',
      missingEvidence: missingEvidenceFromPresentation(presentation),
      recommendedAction: 'Resolve the discovery blocker, then retry investigation.',
      coveragePercent: coverageRatioPercent(presentation.coverage),
    };
  }

  const missingEvidence = missingEvidenceFromPresentation(presentation);
  const coveragePercent = coverageRatioPercent(presentation.coverage);
  const executable = hasSufficientEvidenceForPrioritization(presentation);

  if (executable) {
    return {
      executable: true,
      coveragePercent,
    };
  }

  const blockingReason = presentation.discoveryStatus === 'incomplete'
    ? 'Discovery coverage is incomplete.'
    : 'Discovery evidence is insufficient for prioritization.';

  return {
    executable: false,
    blockingReason,
    missingEvidence,
    recommendedAction: presentation.discoveryStatus === 'incomplete'
      ? 'Continue investigation.'
      : 'Request additional discovery evidence.',
    coveragePercent,
  };
}

/**
 * @param {object} snapshot
 * @returns {DecisionReadiness}
 */
function canApprovePlan(snapshot) {
  const mission = missionFrom(snapshot) || {};
  if (isStructuredMissionApproved(mission)) {
    return {
      executable: false,
      blockingReason: 'Mission plan is already approved.',
      recommendedAction: 'Approve discovery to begin Scout investigation.',
    };
  }
  if (hasPendingPlanClarification(snapshot)) {
    return {
      executable: false,
      blockingReason: 'Mission plan has unresolved ambiguities.',
      recommendedAction: 'Answer clarification questions before approving the plan.',
    };
  }
  if (mission.planCancelled) {
    return {
      executable: false,
      blockingReason: 'Mission plan was cancelled.',
      recommendedAction: 'Create a new mission or resume planning.',
    };
  }
  if (!mission.missionPlanDraft) {
    return {
      executable: false,
      blockingReason: 'No mission plan draft exists.',
      recommendedAction: 'Complete mission planning before approval.',
    };
  }
  if (!isReadyForLock(mission.missionPlanDraft)) {
    return {
      executable: false,
      blockingReason: 'Mission plan is not ready to lock.',
      recommendedAction: 'Resolve plan ambiguities before approval.',
    };
  }
  if (pendingKind(mission) !== OPERATOR_DECISION_KINDS.PLAN_APPROVAL
    && pendingKind(mission) !== OPERATOR_DECISION_KINDS.PLAN_EDIT) {
    return {
      executable: false,
      blockingReason: 'Plan approval is not the active pending decision.',
      recommendedAction: 'Follow the current operator workflow.',
    };
  }
  return { executable: true };
}

/**
 * @param {object} snapshot
 * @returns {DecisionReadiness}
 */
function canApproveDiscovery(snapshot) {
  const mission = missionFrom(snapshot) || {};
  if (hasPendingPlanClarification(snapshot) || hasPendingPlanApproval(snapshot)) {
    return {
      executable: false,
      blockingReason: 'Mission plan approval is required first.',
      recommendedAction: 'Approve the mission plan before discovery.',
    };
  }
  if (!isStructuredMissionApproved(mission)) {
    return {
      executable: false,
      blockingReason: 'Mission plan is not locked.',
      recommendedAction: 'Approve the mission plan before discovery.',
    };
  }
  if (mission.stage && mission.stage !== STAGES.DISCOVER) {
    return {
      executable: false,
      blockingReason: `Discovery cannot execute while the mission is at ${mission.stage}.`,
      recommendedAction: 'Return to the discover stage before approving discovery.',
    };
  }
  if (hasDiscoveryArtifact(snapshot)) {
    return {
      executable: false,
      blockingReason: 'Discovery has already executed.',
      recommendedAction: 'Review discovery findings or continue investigation.',
    };
  }
  if (pendingKind(mission) !== OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL) {
    return {
      executable: false,
      blockingReason: 'Discovery approval is not the active pending decision.',
      recommendedAction: 'Follow the current operator workflow.',
    };
  }
  return { executable: true };
}

/**
 * @param {object} snapshot
 * @param {object} [extras]
 * @returns {DecisionReadiness}
 */
function canApprovePrioritization(snapshot, extras = {}) {
  const mission = missionFrom(snapshot) || {};
  if (hasPendingPlanClarification(snapshot) || hasPendingPlanApproval(snapshot)) {
    return {
      executable: false,
      blockingReason: 'Mission plan approval is required first.',
      recommendedAction: 'Approve the mission plan before findings review.',
    };
  }
  if (hasPendingDiscoveryApproval(snapshot)) {
    return {
      executable: false,
      blockingReason: 'Discovery approval is required before findings review.',
      recommendedAction: 'Approve discovery to run Scout investigation.',
    };
  }
  if (!isStructuredMissionApproved(mission)) {
    return {
      executable: false,
      blockingReason: 'Mission plan is not locked.',
      recommendedAction: 'Approve the mission plan before findings review.',
    };
  }
  if (mission.stage && mission.stage !== STAGES.DISCOVER) {
    return {
      executable: false,
      blockingReason: `Prioritization cannot execute while the mission is at ${mission.stage}.`,
      recommendedAction: 'Return to discovery review before approving findings.',
    };
  }
  if (!hasDiscoveryArtifact(snapshot, extras)) {
    return {
      executable: false,
      blockingReason: 'Discovery artifact is required before prioritization.',
      missingEvidence: ['Discovery artifact'],
      recommendedAction: 'Run Scout investigation before approving findings.',
    };
  }
  if (pendingKind(mission) !== OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL) {
    return {
      executable: false,
      blockingReason: 'Prioritization approval is not the active pending decision.',
      recommendedAction: 'Follow the current operator workflow.',
    };
  }
  const presentation = discoveryPresentationFromSnapshot(snapshot, extras);
  return evaluatePrioritizationReadiness(presentation);
}

/**
 * Build readiness for the active pending operator decision.
 * @param {object} snapshot
 * @param {object} [extras]
 * @returns {DecisionReadiness | null}
 */
function buildDecisionReadiness(snapshot, extras = {}) {
  const mission = missionFrom(snapshot) || {};
  const kind = pendingKind(mission);
  if (!kind) return null;

  if (kind === OPERATOR_DECISION_KINDS.PLAN_APPROVAL
    || kind === OPERATOR_DECISION_KINDS.PLAN_EDIT) {
    return canApprovePlan(snapshot);
  }
  if (kind === OPERATOR_DECISION_KINDS.PLAN_CLARIFICATION) {
    return {
      executable: true,
      recommendedAction: 'Answer the clarification question.',
    };
  }
  if (kind === OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL) {
    return canApproveDiscovery(snapshot);
  }
  if (kind === OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL) {
    return canApprovePrioritization(snapshot, extras);
  }
  if (kind === OPERATOR_DECISION_KINDS.DISCOVERY_INVESTIGATION) {
    const presentation = discoveryPresentationFromSnapshot(snapshot, extras);
    const readiness = evaluatePrioritizationReadiness(presentation);
    return {
      executable: false,
      blockingReason: readiness.blockingReason || 'Discovery coverage is incomplete.',
      missingEvidence: readiness.missingEvidence,
      recommendedAction: readiness.recommendedAction || 'Continue investigation.',
      coveragePercent: readiness.coveragePercent,
    };
  }
  return null;
}

/**
 * Derive pendingOperatorDecision after Scout discovery commits.
 * @param {object} presentation
 * @returns {object}
 */
function buildPostDiscoveryPendingDecision(presentation) {
  const readiness = evaluatePrioritizationReadiness(presentation);
  if (readiness.executable) {
    return {
      stage: STAGES.DISCOVER,
      kind: OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL,
      prompt: 'Approve findings?',
      actions: [
        'Approve findings',
        'Request additional investigation',
        'Adjust mission',
        'Cancel',
      ],
      readiness,
    };
  }

  return {
    stage: STAGES.DISCOVER,
    kind: OPERATOR_DECISION_KINDS.DISCOVERY_INVESTIGATION,
    prompt: 'Discovery Coverage Incomplete',
    headline: 'Discovery Coverage Incomplete',
    actions: [
      'Continue Investigation',
      'Modify Mission',
      'Accept Incomplete Investigation',
    ],
    coveragePercent: readiness.coveragePercent,
    missingEvidence: readiness.missingEvidence,
    readiness,
  };
}

function rollbackStageLabel(action) {
  if (action === 'plan_approved') return 'Mission plan';
  if (action === 'discovery_approved') return 'Discovery';
  if (action === 'prioritization_approved') return 'Prioritization';
  if (action === 'plan_clarified') return 'Mission plan clarification';
  return 'Mission stage';
}

module.exports = {
  coverageRatioPercent,
  missingEvidenceFromPresentation,
  evaluatePrioritizationReadiness,
  canApprovePlan,
  canApproveDiscovery,
  canApprovePrioritization,
  buildDecisionReadiness,
  buildPostDiscoveryPendingDecision,
  discoveryPresentationFromSnapshot,
  rollbackStageLabel,
};
