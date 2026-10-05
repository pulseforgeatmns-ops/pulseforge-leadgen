'use strict';

const crypto = require('crypto');

function uuid() {
  return crypto.randomUUID();
}

class MemoryStateStore {
  constructor(seed = {}) {
    this.clientId = seed.clientId || 1;
    this.users = seed.users || [];
    this.companies = seed.companies || [];
    this.prospects = seed.prospects || [];
    this.contacts = seed.contacts || [];
    this.expectations = seed.expectations || [];
    this.appliedClaims = new Map();
    this.ingestions = [];
    this.artifacts = [];
    this.claims = [];
    this.mutations = [];
    this.conflicts = [];
    this.evidenceLinks = [];
    this.verifyFailFields = new Set(seed.verifyFailFields || []);
  }

  snapshotContext() {
    return {
      clientId: this.clientId,
      users: this.users,
      companies: this.companies,
      prospects: this.prospects,
      contacts: this.contacts,
    };
  }

  async findAppliedClaim(fingerprint) {
    return this.appliedClaims.get(fingerprint) || null;
  }

  async recordAppliedClaim(entry) {
    this.appliedClaims.set(entry.claim_fingerprint, entry);
  }

  async persistIngestion(record) {
    this.ingestions.push(record);
    return record;
  }

  async persistArtifact(artifact) {
    this.artifacts.push(artifact);
    return artifact;
  }

  async persistClaim(claim) {
    this.claims.push(claim);
    return claim;
  }

  async persistMutation(mutation) {
    this.mutations.push(mutation);
    return mutation;
  }

  async persistConflict(conflict) {
    this.conflicts.push(conflict);
    return conflict;
  }

  async persistEvidenceLink(link) {
    this.evidenceLinks.push(link);
    return link;
  }

  async readProspect(id) {
    return this.prospects.find(p => p.id === id) || null;
  }

  async applyMutation(mutation) {
    if (mutation.commit_status === 'blocked' || mutation.commit_status === 'skipped_duplicate') {
      return { committed: false, mutation };
    }
    if (mutation.safety_class === 'C') {
      mutation.commit_status = 'blocked';
      return { committed: false, mutation };
    }

    if (mutation.entity_type === 'company' && mutation.field_name === 'create') {
      const company = {
        id: uuid(),
        client_id: this.clientId,
        name: mutation.intended_value.name,
      };
      this.companies.push(company);
      mutation.entity_id = company.id;
      mutation.commit_status = 'committed';
      return { committed: true, mutation, created: { company } };
    }

    if (mutation.entity_type === 'prospect' && mutation.field_name === 'create') {
      const company = this.companies.find(c => c.name === mutation.intended_value.company_name)
        || { id: uuid(), client_id: this.clientId, name: mutation.intended_value.company_name };
      if (!this.companies.some(c => c.id === company.id)) this.companies.push(company);
      const contact = mutation.intended_value.contact || {};
      const prospect = {
        id: uuid(),
        client_id: this.clientId,
        company_id: company.id,
        company_name: company.name,
        assigned_ao_id: mutation.intended_value.assigned_ao_id,
        source: mutation.intended_value.source || 'AO_REPORTED',
        first_name: contact.name ? contact.name.split(' ')[0] : null,
        last_name: contact.name ? contact.name.split(' ').slice(1).join(' ') : null,
        email: contact.email || null,
        phone: contact.phone || null,
        job_title: contact.title || null,
        status: 'warm',
        acquisition_metadata: { maxStateIngestion: { relationship_active: true } },
        unknown_fields: mutation.intended_value.partial_unknowns || [],
      };
      this.prospects.push(prospect);
      mutation.entity_id = prospect.id;
      mutation.commit_status = 'committed';
      return { committed: true, mutation, created: { prospect, company } };
    }

    const prospect = this.prospects.find(p => p.id === mutation.entity_id);
    if (!prospect) {
      mutation.commit_status = 'blocked';
      return { committed: false, mutation };
    }

    switch (mutation.field_name) {
      case 'assigned_ao_id':
        prospect.assigned_ao_id = mutation.intended_value;
        break;
      case 'ao_last_touch_at':
        prospect.ao_last_touch_at = mutation.intended_value;
        break;
      case 'follow_up_state':
        prospect.follow_up_state = mutation.intended_value;
        break;
      case 'relationship_active':
        prospect.relationship_active = mutation.intended_value;
        prospect.acquisition_metadata = {
          ...(prospect.acquisition_metadata || {}),
          maxStateIngestion: {
            ...(prospect.acquisition_metadata?.maxStateIngestion || {}),
            relationship_active: true,
          },
        };
        break;
      case 'suppress_cold_outreach':
        prospect.suppress_cold_outreach = mutation.intended_value;
        prospect.acquisition_metadata = {
          ...(prospect.acquisition_metadata || {}),
          maxStateIngestion: {
            ...(prospect.acquisition_metadata?.maxStateIngestion || {}),
            suppress_cold_outreach: true,
          },
        };
        break;
      case 'ao_next_action':
        prospect.ao_next_action = mutation.intended_value;
        break;
      case 'next_action_due_hint':
        prospect.next_action_due_hint = mutation.intended_value;
        break;
      case 'activity_append':
        prospect.activities = [...(prospect.activities || []), mutation.intended_value];
        break;
      case 'operator_correction':
        Object.assign(prospect, mutation.intended_value || {});
        break;
      default:
        break;
    }
    mutation.commit_status = 'committed';
    return { committed: true, mutation, prospect };
  }

  async verifyMutation(mutation) {
    if (mutation.field_name === 'create' || mutation.entity_type === 'ingestion' || mutation.entity_type === 'work_item' || mutation.entity_type === 'expectation') {
      mutation.verification_status = 'VERIFIED';
      return mutation;
    }
    const prospect = await this.readProspect(mutation.entity_id);
    if (!prospect) {
      mutation.verification_status = 'COMMIT_VERIFICATION_FAILED';
      return mutation;
    }
    if (this.verifyFailFields.has(mutation.field_name)) {
      mutation.observed_value = '__verification_simulated_mismatch__';
      mutation.verification_status = 'COMMIT_VERIFICATION_FAILED';
      return mutation;
    }
    const fieldMap = {
      assigned_ao_id: 'assigned_ao_id',
      ao_last_touch_at: 'ao_last_touch_at',
      follow_up_state: 'follow_up_state',
      relationship_active: 'relationship_active',
      suppress_cold_outreach: 'suppress_cold_outreach',
      ao_next_action: 'ao_next_action',
      next_action_due_hint: 'next_action_due_hint',
    };
    const field = fieldMap[mutation.field_name];
    if (field) {
      mutation.observed_value = prospect[field];
      const intended = mutation.intended_value;
      const observed = prospect[field];
      mutation.verification_status = JSON.stringify(observed) === JSON.stringify(intended) ? 'VERIFIED' : 'COMMIT_VERIFICATION_FAILED';
      return mutation;
    }
    mutation.verification_status = 'VERIFIED';
    return mutation;
  }

  async createExpectation(expectation) {
    const duplicate = this.expectations.find(e =>
      e.prospect_id === expectation.prospect_id
      && e.expectation_type === expectation.expectation_type
      && e.status === expectation.status
      && JSON.stringify(e.expected_window || {}) === JSON.stringify(expectation.expected_window || {})
      && e.source_ingestion_id === expectation.source_ingestion_id
    );
    if (duplicate) return duplicate;
    const openDuplicate = this.expectations.find(e =>
      e.prospect_id === expectation.prospect_id
      && e.expectation_type === expectation.expectation_type
      && ['OPEN', 'WAITING'].includes(e.status)
      && JSON.stringify(e.expected_window || {}) === JSON.stringify(expectation.expected_window || {})
    );
    if (openDuplicate) return openDuplicate;
    const row = { id: uuid(), ...expectation };
    this.expectations.push(row);
    return row;
  }

  async listOpenExpectations({ clientId = this.clientId } = {}) {
    return this.expectations.filter(e => e.client_id === clientId && ['OPEN', 'WAITING', 'OVERDUE'].includes(e.status));
  }

  async updateExpectation(id, patch = {}) {
    const row = this.expectations.find(e => e.id === id);
    if (!row) return null;
    Object.assign(row, patch);
    return row;
  }
}

module.exports = {
  MemoryStateStore,
};
