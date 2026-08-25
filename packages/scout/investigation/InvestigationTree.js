'use strict';

/**
 * SPEC-158 — Investigation Tree.
 * Mission → Market Definition → Hypotheses → Investigation Branches → Evidence
 */

const { HYPOTHESIS_STATUS } = require('./SearchHypothesisEngine');

const BRANCH_STATUS = Object.freeze({
  PENDING: 'pending',
  EXECUTING: 'executing',
  COMPLETED: 'completed',
  FAILED: 'failed',
  SPAWNED: 'spawned',
});

function nowIso() {
  return new Date().toISOString();
}

function buildBranch(partial = {}) {
  return {
    id: partial.id || `branch-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
    hypothesisId: partial.hypothesisId || null,
    hypothesis: partial.hypothesis || null,
    parentBranchId: partial.parentBranchId || null,
    searchTerms: Array.isArray(partial.searchTerms) ? partial.searchTerms.slice() : [],
    status: partial.status || BRANCH_STATUS.PENDING,
    evidence: partial.evidence || null,
    resultCount: partial.resultCount != null ? partial.resultCount : 0,
    confidence: partial.confidence != null ? partial.confidence : null,
    spawnedBranches: Array.isArray(partial.spawnedBranches) ? partial.spawnedBranches.slice() : [],
    startedAt: partial.startedAt || null,
    completedAt: partial.completedAt || null,
  };
}

/**
 * Create an investigation tree rooted in market definition.
 * @param {object} marketDefinition
 * @param {object[]} hypotheses
 * @returns {object}
 */
function createInvestigationTree(marketDefinition = {}, hypotheses = []) {
  return {
    version: 'SPEC-158',
    marketDefinition: {
      market: marketDefinition.market,
      geography: marketDefinition.geography,
      operatorSegment: marketDefinition.operatorSegment,
      terminology: marketDefinition.terminology,
    },
    root: {
      id: 'root',
      type: 'market_definition',
      label: marketDefinition.market || 'Target market',
      geography: marketDefinition.geography,
      createdAt: nowIso(),
    },
    hypotheses: hypotheses.slice(),
    branches: [],
    finalUnderstanding: null,
  };
}

/**
 * Add an investigation branch for a hypothesis.
 * @param {object} tree
 * @param {object} hypothesis
 * @param {object} [opts]
 * @returns {object}
 */
function addHypothesisBranch(tree, hypothesis, opts = {}) {
  const branch = buildBranch({
    hypothesisId: hypothesis.id,
    hypothesis: {
      id: hypothesis.id,
      text: hypothesis.text,
      rationale: hypothesis.rationale,
    },
    parentBranchId: opts.parentBranchId || hypothesis.parentId || null,
    searchTerms: hypothesis.searchTerms || [],
    status: BRANCH_STATUS.PENDING,
  });
  tree.branches.push(branch);
  return branch;
}

/**
 * Record evidence on a branch after search execution.
 * @param {object} tree
 * @param {string} branchId
 * @param {object} evidence
 * @returns {object|null}
 */
function recordBranchEvidence(tree, branchId, evidence = {}) {
  const branch = tree.branches.find((b) => b.id === branchId);
  if (!branch) return null;

  branch.status = BRANCH_STATUS.COMPLETED;
  branch.evidence = evidence;
  branch.resultCount = evidence.resultCount != null ? evidence.resultCount : 0;
  branch.confidence = evidence.confidence != null ? evidence.confidence : branch.confidence;
  branch.completedAt = nowIso();
  if (!branch.startedAt) branch.startedAt = branch.completedAt;

  const hyp = tree.hypotheses.find((h) => h.id === branch.hypothesisId);
  if (hyp && evidence.hypothesisStatus) {
    hyp.status = evidence.hypothesisStatus;
    hyp.confidence = evidence.confidence;
    hyp.evidence = {
      resultCount: branch.resultCount,
      uniqueCandidates: evidence.uniqueCandidates,
      searchTerms: branch.searchTerms,
    };
  }

  return branch;
}

/**
 * Spawn a child branch from a failed or inconclusive parent.
 * @param {object} tree
 * @param {string} failedBranchId
 * @param {object} newHypothesis
 * @returns {object}
 */
function spawnBranchFromFailure(tree, failedBranchId, newHypothesis) {
  const parent = tree.branches.find((b) => b.id === failedBranchId);
  if (parent) {
    parent.status = parent.resultCount > 0 ? BRANCH_STATUS.COMPLETED : BRANCH_STATUS.FAILED;
  }

  const branch = addHypothesisBranch(tree, newHypothesis, { parentBranchId: failedBranchId });
  branch.status = BRANCH_STATUS.SPAWNED;
  if (parent) parent.spawnedBranches.push(branch.id);

  const existing = tree.hypotheses.find((h) => h.id === newHypothesis.id);
  if (!existing) tree.hypotheses.push(newHypothesis);

  return branch;
}

/**
 * Set final understanding after investigation completes.
 * @param {object} tree
 * @param {object} understanding
 * @returns {object}
 */
function setFinalUnderstanding(tree, understanding = {}) {
  tree.finalUnderstanding = {
    market: understanding.market || tree.marketDefinition.market,
    geography: understanding.geography || tree.marketDefinition.geography,
    dominantTerminology: understanding.dominantTerminology || null,
    confirmedHypotheses: (tree.hypotheses || [])
      .filter((h) => h.status === HYPOTHESIS_STATUS.CONFIRMED)
      .map((h) => ({
        hypothesis: h.text,
        evidence: h.evidence,
        confidence: h.confidence,
      })),
    rejectedHypotheses: (tree.hypotheses || [])
      .filter((h) => h.status === HYPOTHESIS_STATUS.REJECTED)
      .map((h) => ({ hypothesis: h.text, evidence: h.evidence })),
    totalCandidates: understanding.totalCandidates != null ? understanding.totalCandidates : 0,
    revisedMarketDefinition: understanding.revisedMarketDefinition || null,
    summary: understanding.summary || '',
    completedAt: nowIso(),
  };
  return tree;
}

/**
 * Serialize tree for mission reports.
 * @param {object} tree
 * @returns {object}
 */
function serializeInvestigationTree(tree = {}) {
  return {
    marketDefinition: tree.marketDefinition,
    hypotheses: (tree.hypotheses || []).map((h) => ({
      hypothesis: h.text,
      searchTerms: h.searchTerms,
      status: h.status,
      evidence: h.evidence,
      confidence: h.confidence,
      rationale: h.rationale,
    })),
    branches: (tree.branches || []).map((b) => ({
      id: b.id,
      hypothesis: b.hypothesis && b.hypothesis.text,
      searchTerms: b.searchTerms,
      status: b.status,
      resultCount: b.resultCount,
      confidence: b.confidence,
      parentBranchId: b.parentBranchId,
      spawnedBranches: b.spawnedBranches,
    })),
    finalUnderstanding: tree.finalUnderstanding,
  };
}

module.exports = {
  BRANCH_STATUS,
  createInvestigationTree,
  addHypothesisBranch,
  recordBranchEvidence,
  spawnBranchFromFailure,
  setFinalUnderstanding,
  serializeInvestigationTree,
};
