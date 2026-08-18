'use strict';

/**
 * SPEC-113 Stage 4 — Human review.
 * Exactly like CIE. Nothing publishes automatically.
 * Accept · Edit · Merge · Remove. Every concept stays explainable.
 */

const {
  WORKSPACE_STATUS,
  CONCEPT_STATUS,
  REVIEW_ACTIONS,
  asText,
  asList,
  nowIso,
  newId,
} = require('./types');

function assertReviewable(workspace) {
  const status = workspace && workspace.status;
  if (!workspace) {
    const err = new Error('Workspace not found.');
    err.code = 'aic_not_found';
    throw err;
  }
  if (status === WORKSPACE_STATUS.PUBLISHED) {
    const err = new Error('Published AIM is immutable. Compile a new version to change concepts.');
    err.code = 'aic_published_immutable';
    throw err;
  }
  if (![WORKSPACE_STATUS.IN_REVIEW, WORKSPACE_STATUS.ONTOLOGY_READY, WORKSPACE_STATUS.EXTRACTED, WORKSPACE_STATUS.APPROVED].includes(status)) {
    const err = new Error('Compile the workspace before reviewing concepts.');
    err.code = 'aic_not_ready_for_review';
    throw err;
  }
}

function findConcept(workspace, conceptId) {
  const concept = (workspace.concepts || []).find((c) => c.id === conceptId);
  if (!concept) {
    const err = new Error(`Concept not found: ${conceptId}`);
    err.code = 'aic_concept_not_found';
    throw err;
  }
  return concept;
}

function recordReview(workspace, concept, action, payload, operator) {
  workspace.reviews = workspace.reviews || [];
  workspace.reviews.push({
    id: newId('rev'),
    conceptId: concept.id,
    action,
    operator: asText(operator) || 'operator',
    payload: payload || {},
    createdAt: nowIso(),
  });
}

function acceptConcept(workspace, conceptId, opts = {}) {
  assertReviewable(workspace);
  const concept = findConcept(workspace, conceptId);
  concept.status = CONCEPT_STATUS.ACCEPTED;
  concept.operatorApproval = {
    action: REVIEW_ACTIONS.ACCEPT,
    operator: asText(opts.operator) || 'operator',
    at: nowIso(),
  };
  recordReview(workspace, concept, REVIEW_ACTIONS.ACCEPT, {}, opts.operator);
  workspace.status = WORKSPACE_STATUS.IN_REVIEW;
  workspace.updatedAt = nowIso();
  return concept;
}

function editConcept(workspace, conceptId, patch = {}, opts = {}) {
  assertReviewable(workspace);
  const concept = findConcept(workspace, conceptId);
  concept.originalStatement = concept.originalStatement || concept.statement;
  if (patch.statement != null) concept.statement = asText(patch.statement);
  if (patch.label != null) concept.label = asText(patch.label);
  concept.status = CONCEPT_STATUS.EDITED;
  concept.operatorApproval = {
    action: REVIEW_ACTIONS.EDIT,
    operator: asText(opts.operator) || 'operator',
    at: nowIso(),
  };
  recordReview(workspace, concept, REVIEW_ACTIONS.EDIT, patch, opts.operator);
  workspace.status = WORKSPACE_STATUS.IN_REVIEW;
  workspace.updatedAt = nowIso();
  return concept;
}

function removeConcept(workspace, conceptId, opts = {}) {
  assertReviewable(workspace);
  const concept = findConcept(workspace, conceptId);
  concept.status = CONCEPT_STATUS.REMOVED;
  concept.operatorApproval = {
    action: REVIEW_ACTIONS.REMOVE,
    operator: asText(opts.operator) || 'operator',
    at: nowIso(),
  };
  recordReview(workspace, concept, REVIEW_ACTIONS.REMOVE, {}, opts.operator);
  workspace.status = WORKSPACE_STATUS.IN_REVIEW;
  workspace.updatedAt = nowIso();
  return concept;
}

function mergeConcepts(workspace, input = {}, opts = {}) {
  assertReviewable(workspace);
  const survivor = findConcept(workspace, input.mergeInto || input.survivorId);
  const absorbedIds = asList(input.absorbedIds || input.ids).filter((id) => id !== survivor.id);
  if (!absorbedIds.length) {
    const err = new Error('Merge requires absorbedIds.');
    err.code = 'aic_merge_ids_required';
    throw err;
  }
  const absorbed = absorbedIds.map((id) => findConcept(workspace, id));
  const statements = [survivor.statement, ...absorbed.map((c) => c.statement)].filter(Boolean);
  survivor.statement = asText(input.statement) || statements.join(' ');
  survivor.status = CONCEPT_STATUS.EDITED;
  survivor.mergedFrom = absorbedIds;
  survivor.operatorApproval = {
    action: REVIEW_ACTIONS.MERGE,
    operator: asText(opts.operator) || 'operator',
    at: nowIso(),
  };
  for (const row of absorbed) {
    row.status = CONCEPT_STATUS.MERGED;
    row.mergedInto = survivor.id;
  }
  workspace.edges = (workspace.edges || []).map((edge) => {
    const next = { ...edge };
    if (absorbedIds.includes(next.from)) next.from = survivor.id;
    if (absorbedIds.includes(next.to)) next.to = survivor.id;
    return next;
  });
  recordReview(
    workspace,
    survivor,
    REVIEW_ACTIONS.MERGE,
    { absorbedIds, statement: survivor.statement },
    opts.operator
  );
  workspace.status = WORKSPACE_STATUS.IN_REVIEW;
  workspace.updatedAt = nowIso();
  return survivor;
}

function reviewConcept(workspace, conceptId, input = {}, opts = {}) {
  const action = asText(input.action).toLowerCase();
  if (action === REVIEW_ACTIONS.ACCEPT) return acceptConcept(workspace, conceptId, opts);
  if (action === REVIEW_ACTIONS.EDIT) return editConcept(workspace, conceptId, input, opts);
  if (action === REVIEW_ACTIONS.REMOVE) return removeConcept(workspace, conceptId, opts);
  if (action === REVIEW_ACTIONS.MERGE) return mergeConcepts(workspace, { ...input, mergeInto: input.mergeInto || conceptId }, opts);
  const err = new Error(`Unknown review action: ${action || '(empty)'}`);
  err.code = 'aic_unknown_review_action';
  throw err;
}

function publishableConcepts(workspace) {
  return (workspace.concepts || []).filter((c) =>
    c.status === CONCEPT_STATUS.ACCEPTED || c.status === CONCEPT_STATUS.EDITED
  );
}

function remainingProposed(workspace) {
  return (workspace.concepts || []).filter((c) => c.status === CONCEPT_STATUS.PROPOSED);
}

function approveWorkspace(workspace, opts = {}) {
  assertReviewable(workspace);
  const acceptRemaining = opts.acceptRemaining !== false;
  if (acceptRemaining) {
    for (const concept of remainingProposed(workspace)) {
      acceptConcept(workspace, concept.id, opts);
    }
  }
  const leftover = remainingProposed(workspace);
  if (leftover.length) {
    const err = new Error('Approve requires every proposed concept to be accepted, edited, merged, or removed.');
    err.code = 'aic_unreviewed_concepts';
    err.conceptIds = leftover.map((c) => c.id);
    throw err;
  }
  if (!publishableConcepts(workspace).length) {
    const err = new Error('Approve requires at least one accepted or edited concept.');
    err.code = 'aic_nothing_to_publish';
    throw err;
  }
  workspace.status = WORKSPACE_STATUS.APPROVED;
  workspace.approvedAt = nowIso();
  workspace.approvedBy = asText(opts.operator) || 'operator';
  workspace.updatedAt = nowIso();
  return workspace;
}

module.exports = {
  assertReviewable,
  acceptConcept,
  editConcept,
  removeConcept,
  mergeConcepts,
  reviewConcept,
  approveWorkspace,
  publishableConcepts,
  remainingProposed,
};
