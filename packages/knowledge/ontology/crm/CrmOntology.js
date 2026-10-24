'use strict';

const { createDomainOntology } = require('../assertDomainOntology');

const CRM_ENTITY_TYPES = Object.freeze({
  COMPANY: 'company',
  PERSON: 'person',
  INTERACTION: 'interaction',
});

const CRM_RELATIONSHIP_TYPES = Object.freeze({
  HAS_CONTACT: 'HAS_CONTACT',
  PARTICIPATED_IN: 'PARTICIPATED_IN',
  USES: 'USES',
  LOCATED_IN: 'LOCATED_IN',
  KNOWS: 'KNOWS',
  WORKS_FOR: 'WORKS_FOR',
});

const CRM_OBSERVATION_TYPES = Object.freeze({
  EMAIL_SENT: 'email_sent',
  CALL_LOGGED: 'call_logged',
  MEETING_BOOKED: 'meeting_booked',
  CRM_FIELD_UPDATE: 'crm_field_update',
});

const CRM_CLAIM_VOCABULARY = Object.freeze([
  { id: 'prospect_interested', label: 'Prospect Interested' },
  { id: 'decision_maker_engaged', label: 'Decision Maker Engaged' },
  { id: 'icp_fit_strong', label: 'ICP Fit Strong' },
  { id: 'engagement_declining', label: 'Engagement Declining' },
  { id: 'relationship_at_risk', label: 'Relationship At Risk' },
]);

const CRM_OUTCOME_VOCABULARY = Object.freeze([
  { id: 'meeting_booked', label: 'Meeting Booked' },
  { id: 'deal_won', label: 'Deal Won' },
  { id: 'deal_lost', label: 'Deal Lost' },
  { id: 'no_response', label: 'No Response' },
  { id: 'unsubscribed', label: 'Unsubscribed' },
]);

function createCrmOntology() {
  return createDomainOntology({
    id: 'crm',
    label: 'CRM',
    entityTypes: Object.values(CRM_ENTITY_TYPES),
    subjectTypes: [CRM_ENTITY_TYPES.COMPANY],
    relationshipTypes: Object.values(CRM_RELATIONSHIP_TYPES),
    observationTypes: Object.values(CRM_OBSERVATION_TYPES),
    claimVocabulary: [...CRM_CLAIM_VOCABULARY],
    outcomeVocabulary: [...CRM_OUTCOME_VOCABULARY],
  });
}

module.exports = {
  CRM_ENTITY_TYPES,
  CRM_RELATIONSHIP_TYPES,
  CRM_OBSERVATION_TYPES,
  CRM_CLAIM_VOCABULARY,
  CRM_OUTCOME_VOCABULARY,
  createCrmOntology,
};
