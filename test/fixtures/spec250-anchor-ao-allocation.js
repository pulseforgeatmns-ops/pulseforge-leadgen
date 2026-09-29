'use strict';

/**
 * SPEC-250 Anchor AO initial prospect allocation fixture.
 *
 * Sufficient subset: AO × segment allocation judgment, non-round-robin method,
 * learning hypothesis, Jake-owned exclusion, and operator provenance.
 * Not every prospect. Assignment is a bounded fit hypothesis, not qualification evidence.
 */

const ANCHOR_AO_ALLOCATION_FIXTURE = {
  kind: 'OPERATOR_JUDGMENT',
  tenant_id: 'tenant:anchor',
  client_id: 10,
  judgment_key: 'ao-initial-prospect-allocation',
  judgment_kind: 'ALLOCATION_DECISION',
  label: 'AO INITIAL PROSPECT ALLOCATION — OPERATOR DECISION',
  provenance: {
    origin: 'OPERATOR',
    origin_kind: 'operator_authored',
    actor_kind: 'authenticated_operator',
    actor_id: 'operator:authenticated',
  },
  operator: {
    id: 'operator:authenticated',
    role: 'admin',
  },
  associated_mission_id: 'acquisition-mission:anchor-ao-initial-allocation',
  associated_objective_identity_key: 'objective:anchor-ao-initial-allocation',
  propositions: [
    {
      judgment_slot: 'allocation:zack_bunker',
      statement: 'Zack Bunker should receive larger property-management, regional, multi-location, and strategic commercial accounts.',
      epistemic_state: 'KNOWN',
      temporal_status: 'CURRENT',
      modality: 'INTENDED',
      rationale_points: [
        '15+ years outside sales',
        'territory/key-account experience',
        'relationship penetration',
        'New England network',
        'property-management leverage',
      ],
    },
    {
      judgment_slot: 'allocation:tony_jackson',
      statement: 'Tony Jackson should receive facilities-heavy, manufacturing, institutional, and operationally complex accounts.',
      epistemic_state: 'KNOWN',
      temporal_status: 'CURRENT',
      modality: 'INTENDED',
      rationale_points: [
        'commercial-cleaning operational experience',
        'ABM/UNH housekeeping management',
        'Amtrak cleaning audit/standards work',
        'facilities/staffing/quality-control credibility',
      ],
    },
    {
      judgment_slot: 'allocation:rory_matthews',
      statement: 'Rory Matthews should receive smaller/local property managers, professional offices, law/CPA/dental practices, and more structured recurring-commercial accounts.',
      epistemic_state: 'KNOWN',
      temporal_status: 'CURRENT',
      modality: 'INTENDED',
      rationale_points: [
        'strong communicator',
        'coachable/research-oriented',
        'less traditional outside-sales experience',
        'should begin with shorter decision paths and structured local accounts',
      ],
    },
    {
      judgment_slot: 'allocation_method',
      statement: 'Allocation is intentionally not round-robin.',
      epistemic_state: 'KNOWN',
      temporal_status: 'CURRENT',
      modality: 'INTENDED',
    },
    {
      judgment_slot: 'exclusion:jake_owned_warm_relationships',
      statement: 'Existing Jake-owned warm relationships remain excluded unless explicitly transferred.',
      epistemic_state: 'KNOWN',
      temporal_status: 'CURRENT',
      modality: 'INTENDED',
    },
    {
      judgment_slot: 'assignment_epistemic_boundary',
      statement: 'Prospect assignment represents a bounded hypothesis about fit, not evidence that the prospect is qualified.',
      epistemic_state: 'KNOWN',
      temporal_status: 'CURRENT',
      modality: 'INTENDED',
    },
    {
      judgment_slot: 'learning_hypothesis:ao_segment_fit',
      statement: 'AO × segment fit should be measured.',
      epistemic_state: 'HYPOTHESIS',
      temporal_status: 'CURRENT',
      modality: 'INTENDED',
    },
  ],
};

module.exports = { ANCHOR_AO_ALLOCATION_FIXTURE };
