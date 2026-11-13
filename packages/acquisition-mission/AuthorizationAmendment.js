'use strict';

/**
 * P0 Max reliability — governed amendments to durable authorization constraints.
 *
 * Constraints only ever tighten on their own (AuthorizationConstraints). The ONLY
 * way to lift or change one is an amendment that:
 *
 *   - is approved by an authorized human operator (role admin | manager; never
 *     Max, never the autonomous operator, never a client user);
 *   - names the exact change (classes to lift / add) and a reason;
 *   - is bound to the hash of the constraints the operator reviewed, so an
 *     approval of a stale view is refused (`authorization_amendment_stale`);
 *   - commits as an OPERATOR TME stage: the operator `constraints`
 *     contribution (the audit record) and the new mission constraints are
 *     written in one durable commit, or nothing changes;
 *   - is idempotent by `amendmentId`: a retry or replay after restart returns
 *     the already-committed amendment and never applies it twice.
 *
 * Until that commit lands, the existing constraints stay enforced by the router
 * and TME. Tenant isolation is enforced by the engine's tenant-scoped reads.
 *
 * Callers must ALSO hold the transport-level step-up (fresh password
 * confirmation) for any loosening change — see routes/acquisitionMissions.js.
 */

const crypto = require('crypto');
const { amoError, SPECIALISTS, CONTRIBUTION_KINDS, nowIso } = require('./types');
const { ACTION_CLASSES, EXTERNAL_ACTION_CLASSES, SPEC } = require('./AuthorizationConstraints');

const AMENDMENT_ACTION = 'authorization_amendment';
const AMENDMENT_STAGE = 'authorization_amendment';
const APPROVER_ROLES = new Set(['admin', 'manager']);
const NON_HUMAN_OPERATORS = new Set(['max', 'autonomous', 'system', 'operator']);
const VALID_CLASSES = new Set(Object.values(ACTION_CLASSES));

function err(code, message, extras = {}) {
  const e = amoError(code, message);
  e.spec = SPEC;
  Object.assign(e, extras);
  return e;
}

function sortedUnique(values) {
  return [...new Set((values || []).map(String).filter(Boolean))].sort();
}

/**
 * Stable hash of the enforceable part of the constraints the operator reviewed.
 * @param {object|null} constraints
 */
function authorizationConstraintsHash(constraints) {
  const c = constraints || {};
  const canonical = JSON.stringify({
    reviewOnly: c.reviewOnly === true,
    prohibited: sortedUnique(c.prohibited),
    revision: Number(c.revision) || 0,
  });
  return crypto.createHash('sha256').update(canonical).digest('hex');
}

function normalizeChange(change = {}) {
  const lift = sortedUnique(change.lift);
  const add = sortedUnique(change.add);
  for (const cls of [...lift, ...add]) {
    if (!VALID_CLASSES.has(cls)) {
      throw err('authorization_amendment_invalid', `Unknown action class: ${cls}`);
    }
  }
  if (lift.some((cls) => add.includes(cls))) {
    throw err('authorization_amendment_invalid', 'A class cannot be lifted and added in the same amendment.');
  }
  const reason = String(change.reason || '').trim();
  if (!reason) throw err('authorization_amendment_invalid', 'An amendment requires a reason.');
  if (reason.length > 1000) throw err('authorization_amendment_invalid', 'Amendment reason is too long.');
  return { lift, add, reason };
}

/**
 * Compute the constraints an amendment would produce. Pure; changes nothing.
 * @param {object|null} current
 * @param {{ lift?: string[], add?: string[], reason: string }} change
 */
function computeAmendedConstraints(current, change) {
  const { lift, add, reason } = normalizeChange(change);
  const before = current || null;
  const prohibitedBefore = sortedUnique(before && before.prohibited);
  const prohibitedAfter = sortedUnique([...prohibitedBefore.filter((cls) => !lift.includes(cls)), ...add]);
  const lifted = lift.filter((cls) => prohibitedBefore.includes(cls));
  const added = add.filter((cls) => !prohibitedBefore.includes(cls));
  // Review-only means every external class is prohibited; lifting any one ends it.
  const reviewOnlyAfter = Boolean(before && before.reviewOnly) &&
    EXTERNAL_ACTION_CLASSES.every((cls) => prohibitedAfter.includes(cls));
  return {
    lift,
    add,
    reason,
    lifted,
    added,
    loosens: lifted.length > 0 || (Boolean(before && before.reviewOnly) && !reviewOnlyAfter),
    changed: lifted.length > 0 || added.length > 0,
    after: {
      spec: SPEC,
      version: 1,
      reviewOnly: reviewOnlyAfter,
      prohibited: prohibitedAfter,
      sources: (before && before.sources) || [],
      origin: (before && before.origin) || 'operator_amendment',
      establishedAt: (before && before.establishedAt) || nowIso(),
      revision: (Number(before && before.revision) || 0) + 1,
      amendments: (before && Array.isArray(before.amendments)) ? [...before.amendments] : [],
    },
  };
}

/**
 * @param {{ engine: object, missionId: string, tenantId: string }} input
 */
function previewAuthorization({ engine, missionId, tenantId }) {
  const mission = engine.get(missionId, tenantId);
  if (!mission) throw amoError('amo_mission_not_found', `Unknown mission: ${missionId}`);
  const constraints = mission.authorizationConstraints || null;
  return {
    missionId: mission.id,
    tenantId: String(tenantId),
    constraints,
    constraintsHash: authorizationConstraintsHash(constraints),
    actionClasses: [...VALID_CLASSES].sort(),
  };
}

function assertApprover(approver) {
  const id = String((approver && approver.id) || '').trim();
  const role = String((approver && approver.role) || '').toLowerCase();
  if (!id || NON_HUMAN_OPERATORS.has(id.toLowerCase()) || id.toLowerCase().startsWith('autonomous')) {
    throw err('authorization_amendment_forbidden', 'Only a signed-in human operator can amend mission authorization.');
  }
  if (!APPROVER_ROLES.has(role)) {
    throw err('authorization_amendment_forbidden', 'Amending mission authorization requires an admin or manager.');
  }
  return { id, role, name: String((approver && approver.name) || id) };
}

function findAmendment(engine, missionId, amendmentId) {
  return engine.store.listContributions(missionId).find((row) =>
    row.specialist === SPECIALISTS.OPERATOR &&
    row.kind === CONTRIBUTION_KINDS.CONSTRAINTS &&
    row.payload && row.payload.action === AMENDMENT_ACTION &&
    row.payload.amendmentId === amendmentId
  ) || null;
}

/**
 * Apply an operator-approved amendment as one TME stage.
 *
 * @param {object} input
 * @param {object} input.engine
 * @param {string} input.missionId
 * @param {string} input.tenantId
 * @param {{ lift?: string[], add?: string[], reason: string }} input.change
 * @param {string} input.expectedConstraintsHash - hash the operator reviewed
 * @param {string} input.amendmentId - client-supplied idempotency key
 * @param {{ id: string, role: string, name?: string }} input.approver
 * @param {Function|null} [input.persistDurable] - durable commit (persistStageCommit)
 * @param {object} [input.pool]
 */
async function applyAuthorizationAmendment(input = {}) {
  const { engine, missionId, tenantId } = input;
  if (!engine || !missionId || tenantId == null || tenantId === '') {
    throw err('authorization_amendment_invalid', 'engine, missionId and tenantId are required.');
  }
  const approver = assertApprover(input.approver);
  const amendmentId = String(input.amendmentId || '').trim();
  if (!/^[A-Za-z0-9_.:-]{8,128}$/.test(amendmentId)) {
    throw err('authorization_amendment_invalid', 'A unique amendmentId (8-128 safe characters) is required.');
  }
  const expectedHash = String(input.expectedConstraintsHash || '');
  if (!/^[a-f0-9]{64}$/.test(expectedHash)) {
    throw err('authorization_amendment_invalid', 'expectedConstraintsHash from the reviewed preview is required.');
  }
  // Tenant-scoped read: another tenant's mission is never visible.
  const mission = engine.get(missionId, tenantId);
  if (!mission) throw amoError('amo_mission_not_found', `Unknown mission: ${missionId}`);

  const prior = findAmendment(engine, missionId, amendmentId);
  if (prior) {
    return {
      alreadyApplied: true,
      amendment: prior,
      constraints: engine.get(missionId, tenantId).authorizationConstraints || null,
    };
  }
  const plan = computeAmendedConstraints(mission.authorizationConstraints || null, input.change);
  if (!plan.changed) {
    throw err('authorization_amendment_noop', 'The amendment does not change the mission authorization.');
  }
  // Loosening is reserved to admins (who must also pass the password step-up
  // at the transport); managers may only tighten.
  if (plan.loosens && approver.role !== 'admin') {
    throw err('authorization_amendment_forbidden', 'Lifting a mission restriction requires an admin.');
  }

  const { executeMissionStage } = require('./TransactionalExecution');
  const { reopenResearchPlanning } = require('./ResearchPlanning');
  const { bumpMissionVersion } = require('./TransactionalExecution');
  const staged = await executeMissionStage({
    engine,
    missionId,
    tenantId,
    pool: input.pool,
    specialist: SPECIALISTS.OPERATOR,
    stage: AMENDMENT_STAGE,
    operatorId: approver.id,
    validatePreconditions: ({ mission: current }) => {
      // Re-read inside the transaction: the approval binds to what was reviewed.
      const actual = authorizationConstraintsHash(current.authorizationConstraints || null);
      if (actual !== expectedHash) {
        throw err('authorization_amendment_stale',
          'Mission authorization changed since it was reviewed. Review the current restrictions and approve again.',
          { expectedConstraintsHash: expectedHash, actualConstraintsHash: actual });
      }
      return { constraintsHash: actual };
    },
    execute: async () => ({ missionId }),
    validateOutput: () => true,
    commit: ({ transactionId }) => {
      const current = engine.get(missionId, tenantId);
      const recomputed = computeAmendedConstraints(current.authorizationConstraints || null, input.change);
      // P0 — permitting research on a review-only plan reopens planning in this
      // same commit: the deferred market/region questions become a pending
      // clarification and the plan must be approved again before discovery.
      // Computed before the audit record so the record states what changed.
      const reopened = reopenResearchPlanning(current, recomputed.after);
      const supersededPlan = reopened ? (current.structuredMission || current.missionPlanDraft || null) : null;
      const record = {
        amendmentId,
        lifted: recomputed.lifted,
        added: recomputed.added,
        reason: recomputed.reason,
        loosens: recomputed.loosens,
        approvedBy: { id: approver.id, role: approver.role, name: approver.name },
        approvedAt: nowIso(),
        transactionId,
        beforeHash: expectedHash,
        revision: recomputed.after.revision,
        ...(reopened ? {
          planningReopened: true,
          // The prior (review-only) plan and any approval of it no longer stand.
          supersededPlan: {
            contractHash: (supersededPlan && supersededPlan.contractHash) || null,
            approved: Boolean(current.structuredMission && current.structuredMission.immutable),
          },
          missionVersionBefore: Number(current.version) || 0,
          pendingDecisionAfter: (reopened.pendingOperatorDecision && reopened.pendingOperatorDecision.kind) || null,
        } : {}),
      };
      const contribution = engine.contribute(missionId, {
        specialist: SPECIALISTS.OPERATOR,
        kind: CONTRIBUTION_KINDS.CONSTRAINTS,
        payload: {
          action: AMENDMENT_ACTION,
          ...record,
          before: current.authorizationConstraints || null,
          after: { ...recomputed.after, amendments: undefined },
        },
      }, { tenantId }).contribution;
      const amended = {
        ...recomputed.after,
        amendments: [...recomputed.after.amendments, { ...record, contributionId: contribution.id }],
      };
      const next = {
        ...engine.get(missionId, tenantId),
        ...(reopened || {}),
        authorizationConstraints: amended,
      };
      // Reopening replaces the plan and the pending decision: a mission mutation,
      // so it carries a new version and this transaction's id (same convention
      // as plan approval). A constraints-only amendment is unchanged.
      if (reopened) bumpMissionVersion(next, transactionId);
      engine.store.putMission(next);
      return { contribution, constraints: amended, snapshot: engine.inspect(missionId, { tenantId }) };
    },
    persistDurable: input.persistDurable || null,
  });

  return {
    alreadyApplied: false,
    amendment: staged.commitResult.contribution,
    constraints: staged.commitResult.constraints,
    constraintsHash: authorizationConstraintsHash(staged.commitResult.constraints),
    transactionId: staged.transactionId,
    loosens: plan.loosens,
  };
}

module.exports = {
  AMENDMENT_ACTION,
  AMENDMENT_STAGE,
  authorizationConstraintsHash,
  computeAmendedConstraints,
  previewAuthorization,
  applyAuthorizationAmendment,
};
