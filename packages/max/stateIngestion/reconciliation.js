'use strict';

const { CLAIM_TYPES, SAFETY_CLASS, RESOLUTION } = require('./types');
const { interpretSpreadsheetStatus } = require('./spreadsheetStatus');
const { CHANGE_TYPES } = require('./spreadsheetChangeTypes');

function reconcileOwnership(existingAoId, incomingAo, resolutionStatus) {
  if (resolutionStatus === RESOLUTION.CONFLICT) {
    return {
      mutation: null,
      conflict: {
        conflict_type: 'ownership',
        existing: { assigned_ao_id: existingAoId },
        incoming: { assigned_ao_id: incomingAo?.id || null },
      },
    };
  }
  if (existingAoId != null && incomingAo?.id != null && String(existingAoId) !== String(incomingAo.id)) {
    return {
      mutation: null,
      conflict: {
        conflict_type: 'ownership',
        existing: { assigned_ao_id: existingAoId },
        incoming: { assigned_ao_id: incomingAo.id },
      },
    };
  }
  return { mutation: null, conflict: null };
}

function buildMutationsFromSpreadsheetFieldPlan({
  spreadsheetFieldPlan,
  prospect,
  conflicts: planConflicts,
  unresolved: planUnresolved,
}) {
  const mutations = [];
  const conflicts = [...(planConflicts || [])];
  const unresolved = [...(planUnresolved || [])];
  if (!spreadsheetFieldPlan || !prospect) {
    return { mutations, conflicts, unresolved };
  }

  for (const change of spreadsheetFieldPlan.proposedChanges || []) {
    if (change.safe === false) continue;
    switch (change.type) {
      case CHANGE_TYPES.ADD_NOTE:
        mutations.push({
          entity_type: 'prospect',
          entity_id: prospect.id,
          field_name: 'activity_append',
          intended_value: { kind: 'note', notes: change.value, provenance: change.provenance },
          safety_class: SAFETY_CLASS.A,
        });
        break;
      case CHANGE_TYPES.ADD_EMAIL:
        mutations.push({
          entity_type: 'prospect',
          entity_id: prospect.id,
          field_name: 'email_add',
          intended_value: change.value,
          safety_class: SAFETY_CLASS.A,
        });
        break;
      case CHANGE_TYPES.ADD_PHONE:
        mutations.push({
          entity_type: 'prospect',
          entity_id: prospect.id,
          field_name: 'phone_add',
          intended_value: change.value,
          safety_class: SAFETY_CLASS.A,
        });
        break;
      case CHANGE_TYPES.ADD_CONTACT:
        mutations.push({
          entity_type: 'contact',
          entity_id: prospect.id,
          field_name: 'create',
          intended_value: { ...change.value, prospect_id: prospect.id },
          safety_class: SAFETY_CLASS.A,
        });
        break;
      case CHANGE_TYPES.UPDATE_DISPOSITION:
        mutations.push({
          entity_type: 'prospect',
          entity_id: prospect.id,
          field_name: 'disposition_status',
          intended_value: change.value,
          safety_class: SAFETY_CLASS.B,
        });
        break;
      case CHANGE_TYPES.SET_SALES_PRIORITY:
        mutations.push({
          entity_type: 'prospect',
          entity_id: prospect.id,
          field_name: 'sales_priority',
          intended_value: change.value,
          safety_class: SAFETY_CLASS.A,
        });
        break;
      case CHANGE_TYPES.UPDATE_DECISION_MAKER_SIGNAL:
        mutations.push({
          entity_type: 'prospect',
          entity_id: prospect.id,
          field_name: 'decision_maker_correction',
          intended_value: change.value,
          safety_class: SAFETY_CLASS.B,
        });
        break;
      case CHANGE_TYPES.ADD_PAIN_POINT:
        mutations.push({
          entity_type: 'prospect',
          entity_id: prospect.id,
          field_name: 'pain_point_append',
          intended_value: change.value,
          safety_class: SAFETY_CLASS.A,
        });
        break;
      case CHANGE_TYPES.UPDATE_NEXT_ACTION:
        mutations.push({
          entity_type: 'prospect',
          entity_id: prospect.id,
          field_name: 'ao_next_action',
          intended_value: 'follow_up',
          safety_class: SAFETY_CLASS.A,
        });
        mutations.push({
          entity_type: 'prospect',
          entity_id: prospect.id,
          field_name: 'next_action_due_hint',
          intended_value: change.value,
          safety_class: SAFETY_CLASS.A,
        });
        break;
      case CHANGE_TYPES.ADD_FOLLOW_UP:
        mutations.push({
          entity_type: 'prospect',
          entity_id: prospect.id,
          field_name: 'follow_up_state',
          intended_value: 'awaiting_contact',
          safety_class: SAFETY_CLASS.A,
        });
        break;
      default:
        break;
    }
  }

  for (const conflict of spreadsheetFieldPlan.conflicts || []) {
    conflicts.push({ spreadsheet_conflict: conflict });
  }
  for (const amb of spreadsheetFieldPlan.ambiguities || []) {
    unresolved.push({ spreadsheet_ambiguity: amb });
  }

  return { mutations, conflicts, unresolved };
}

function buildMutationsFromClaims({
  claims,
  resolutions,
  bindings,
  sourceType,
  existingProspect = null,
  operatorCorrection = false,
  spreadsheetFieldPlan = null,
}) {
  const mutations = [];
  const conflicts = [];
  const unresolved = [];

  const accountResolution = bindings.account;
  const aoResolution = bindings.ao;
  const prospect = accountResolution?.entity?.kind === 'prospect'
    ? accountResolution.entity
    : existingProspect;

  for (const claim of claims) {
    const resolution = resolutions[claim.claim_type + JSON.stringify(claim.payload)] || resolutions[claim._key];
    if (resolution?.status === RESOLUTION.AMBIGUOUS || resolution?.status === RESOLUTION.UNRESOLVED) {
      if (['AO', 'ACCOUNT', 'CONTACT', 'OWNERSHIP'].includes(claim.claim_type)) {
        unresolved.push({ claim, resolution });
        continue;
      }
    }
    if (resolution?.status === RESOLUTION.CONFLICT) {
      conflicts.push({ claim, resolution });
      continue;
    }
  }

  if (bindings.ownershipConflict) {
    conflicts.push(bindings.ownershipConflict);
  }

  const appendEvidence = {
    entity_type: 'ingestion',
    field_name: 'evidence_append',
    intended_value: { sourceType, claims: claims.map(c => c.claim_type) },
    safety_class: SAFETY_CLASS.A,
  };
  mutations.push(appendEvidence);

  if (prospect) {
    for (const claim of claims) {
      if (claim.claim_type === CLAIM_TYPES.SIGNAL && claim.payload?.signal === 'spreadsheet_status') {
        const mapped = interpretSpreadsheetStatus(claim.payload.label);
        if (mapped.kind === 'disposition' && mapped.confidence === 'high') {
          mutations.push({
            entity_type: 'prospect',
            entity_id: prospect.id,
            field_name: 'disposition_status',
            intended_value: mapped.disposition_status,
            safety_class: SAFETY_CLASS.B,
            claim,
          });
        } else if (mapped.kind === 'sales_priority') {
          mutations.push({
            entity_type: 'prospect',
            entity_id: prospect.id,
            field_name: 'sales_priority',
            intended_value: mapped.sales_priority,
            safety_class: SAFETY_CLASS.A,
            claim,
          });
        }
      }
      if (claim.claim_type === CLAIM_TYPES.EVENT) {
        mutations.push({
          entity_type: 'prospect',
          entity_id: prospect.id,
          field_name: 'ao_last_touch_at',
          intended_value: new Date().toISOString(),
          safety_class: SAFETY_CLASS.A,
          claim,
        });
        mutations.push({
          entity_type: 'prospect',
          entity_id: prospect.id,
          field_name: 'activity_append',
          intended_value: claim.payload,
          safety_class: SAFETY_CLASS.A,
          claim,
        });
      }
      if (claim.claim_type === CLAIM_TYPES.NEXT_EXPECTED_EVENT) {
        mutations.push({
          entity_type: 'expectation',
          entity_id: prospect.id,
          field_name: 'next_expected_event',
          intended_value: claim.payload,
          safety_class: SAFETY_CLASS.A,
          claim,
        });
        mutations.push({
          entity_type: 'prospect',
          entity_id: prospect.id,
          field_name: 'follow_up_state',
          intended_value: 'awaiting_contact',
          safety_class: SAFETY_CLASS.A,
          claim,
        });
      }
      if (claim.claim_type === CLAIM_TYPES.EXPECTED_WINDOW) {
        mutations.push({
          entity_type: 'expectation',
          entity_id: prospect.id,
          field_name: 'expected_window',
          intended_value: claim.payload,
          safety_class: SAFETY_CLASS.A,
          claim,
        });
      }
      if (claim.claim_type === CLAIM_TYPES.PIPELINE_IMPLICATION) {
        mutations.push({
          entity_type: 'prospect',
          entity_id: prospect.id,
          field_name: 'relationship_active',
          intended_value: true,
          safety_class: SAFETY_CLASS.A,
          claim,
        });
        mutations.push({
          entity_type: 'prospect',
          entity_id: prospect.id,
          field_name: 'suppress_cold_outreach',
          intended_value: true,
          safety_class: SAFETY_CLASS.B,
          claim,
        });
      }
      if (claim.claim_type === CLAIM_TYPES.NEXT_ACTION) {
        mutations.push({
          entity_type: 'prospect',
          entity_id: prospect.id,
          field_name: 'ao_next_action',
          intended_value: claim.payload?.action === 'call' ? 'call' : 'follow_up',
          safety_class: SAFETY_CLASS.A,
          claim,
        });
      }
      if (claim.claim_type === CLAIM_TYPES.DUE_WINDOW) {
        mutations.push({
          entity_type: 'prospect',
          entity_id: prospect.id,
          field_name: 'next_action_due_hint',
          intended_value: claim.payload?.due,
          safety_class: SAFETY_CLASS.A,
          claim,
        });
      }
      if (claim.claim_type === CLAIM_TYPES.OPERATOR_CORRECTION || operatorCorrection) {
        mutations.push({
          entity_type: 'prospect',
          entity_id: prospect.id,
          field_name: 'operator_correction',
          intended_value: claim.payload,
          safety_class: SAFETY_CLASS.B,
          claim,
        });
      }
    }

    const ownershipClaim = claims.find(c => c.claim_type === CLAIM_TYPES.OWNERSHIP);
    if (ownershipClaim && aoResolution?.entity) {
      const own = reconcileOwnership(prospect.assigned_ao_id, aoResolution.entity);
      if (own.conflict) {
        conflicts.push({ claim: ownershipClaim, conflict: own.conflict });
      } else if (prospect.assigned_ao_id == null) {
        mutations.push({
          entity_type: 'prospect',
          entity_id: prospect.id,
          field_name: 'assigned_ao_id',
          intended_value: aoResolution.entity.id,
          safety_class: SAFETY_CLASS.B,
          claim: ownershipClaim,
        });
      }
    }
  } else {
    const accountClaim = claims.find(c => c.claim_type === CLAIM_TYPES.ACCOUNT);
    const aoClaim = claims.find(c => c.claim_type === CLAIM_TYPES.AO);
    const contactClaims = claims.filter(c => c.claim_type === CLAIM_TYPES.CONTACT);
    const hasNewProspectSignal = claims.some(c =>
      c.claim_type === CLAIM_TYPES.PIPELINE_IMPLICATION
      && c.payload?.state === 'ao_reported_new_prospect'
    ) || sourceType === 'AO_REPORTED';
    const accountResolution = bindings.account;
    const accountBlocked = accountResolution?.status === RESOLUTION.AMBIGUOUS;

    if (accountBlocked && !unresolved.some(u => u.claim?.claim_type === CLAIM_TYPES.ACCOUNT)) {
      unresolved.push({ claim: accountClaim, resolution: accountResolution });
    } else if (
      accountClaim
      && !accountResolution?.entity
      && (hasNewProspectSignal || contactClaims.length)
    ) {
      mutations.push({
        entity_type: 'company',
        field_name: 'create',
        intended_value: { name: accountClaim.payload.name },
        safety_class: SAFETY_CLASS.A,
        claim: accountClaim,
      });
      mutations.push({
        entity_type: 'prospect',
        field_name: 'create',
        intended_value: {
          company_name: accountClaim.payload.name,
          assigned_ao_id: aoResolution?.entity?.id || null,
          source: 'AO_REPORTED',
          contact: contactClaims.reduce((acc, c) => ({ ...acc, ...c.payload }), {}),
          partial_unknowns: claims.filter(c => c.claim_type === CLAIM_TYPES.UNKNOWN_FIELD),
        },
        safety_class: SAFETY_CLASS.A,
        claim: accountClaim,
      });
    } else if (accountClaim && accountResolution?.status === RESOLUTION.UNRESOLVED && !hasNewProspectSignal) {
      unresolved.push({ claim: accountClaim, resolution: accountResolution });
    }
  }

  for (const claim of claims) {
    if (claim.claim_type === CLAIM_TYPES.UNKNOWN_FIELD) {
      mutations.push({
        entity_type: 'work_item',
        field_name: 'identify_missing_field',
        intended_value: claim.payload,
        safety_class: SAFETY_CLASS.A,
        claim,
      });
    }
  }

  if (spreadsheetFieldPlan && existingProspect) {
    const extra = buildMutationsFromSpreadsheetFieldPlan({
      spreadsheetFieldPlan,
      prospect: existingProspect,
      conflicts,
      unresolved,
    });
    mutations.push(...extra.mutations);
  }

  return { mutations, conflicts, unresolved };
}

module.exports = {
  reconcileOwnership,
  buildMutationsFromClaims,
  buildMutationsFromSpreadsheetFieldPlan,
};
