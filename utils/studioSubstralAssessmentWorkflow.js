'use strict';

/** SPEC-SUBSTRAL-PF-001 assessment workflow stages and outcomes. */

const ASSESSMENT_STAGE = Object.freeze({
  REQUESTED: 'REQUESTED',
  EVIDENCE_COLLECTION: 'EVIDENCE_COLLECTION',
  HUMAN_REVIEW: 'HUMAN_REVIEW',
  DIAGNOSIS_READY: 'DIAGNOSIS_READY',
  DELIVERED: 'DELIVERED',
  FOLLOW_UP: 'FOLLOW_UP',
  CLOSED: 'CLOSED',
});

const ASSESSMENT_OUTCOME = Object.freeze({
  NO_WORK_REQUIRED: 'NO_WORK_REQUIRED',
  TARGETED_FIX: 'TARGETED_FIX',
  REDESIGN_BUILD: 'REDESIGN_BUILD',
  INSUFFICIENT_EVIDENCE: 'INSUFFICIENT_EVIDENCE',
});

const LAYER_KEYS = Object.freeze([
  'performance',
  'accessibility',
  'conversion',
  'search',
  'trust',
  'design',
]);

function defaultNextAction(stage) {
  switch (stage) {
    case ASSESSMENT_STAGE.REQUESTED:
      return 'Collect live-site evidence across six layers before outreach or diagnosis.';
    case ASSESSMENT_STAGE.EVIDENCE_COLLECTION:
      return 'Finish evidence collection; preserve MEASURED/OBSERVED/INFERRED/UNKNOWN labels.';
    case ASSESSMENT_STAGE.HUMAN_REVIEW:
      return 'Operator reviews evidence and stated decision context; no client-facing diagnosis yet.';
    case ASSESSMENT_STAGE.DIAGNOSIS_READY:
      return 'Deliver paid assessment conclusions after human review.';
    case ASSESSMENT_STAGE.DELIVERED:
      return 'Confirm payment and schedule follow-up if the prospect wants implementation help.';
    case ASSESSMENT_STAGE.FOLLOW_UP:
      return 'Close or expand only from an explicit downstream decision — not assumed redesign.';
    case ASSESSMENT_STAGE.CLOSED:
      return 'No further action unless a new assessment request arrives.';
    default:
      return 'Review assessment stage and evidence summary.';
  }
}

module.exports = {
  ASSESSMENT_STAGE,
  ASSESSMENT_OUTCOME,
  LAYER_KEYS,
  defaultNextAction,
};
