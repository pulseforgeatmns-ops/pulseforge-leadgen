'use strict';

const crypto = require('crypto');

function uuid() {
  return crypto.randomUUID();
}

class PostgresStateStore {
  constructor(db, { clientId }) {
    this.db = db;
    this.clientId = clientId;
  }

  async init() {
    const { ensureMaxStateIngestionSchema } = require('../../../../utils/maxStateIngestionSchema');
    await ensureMaxStateIngestionSchema(this.db);
  }

  async snapshotContext({ aoId = null } = {}) {
    const values = [this.clientId, aoId];
    const [users, companies, prospects, activities] = [
      await this.db.query(`SELECT id, name, email, role, client_id FROM users WHERE client_id = $1 AND ($2::integer IS NULL OR id = $2)`, values),
      await this.db.query(`SELECT c.* FROM companies c WHERE c.client_id = $1 AND ($2::integer IS NULL OR EXISTS (SELECT 1 FROM prospects p WHERE p.client_id=c.client_id AND p.company_id=c.id AND p.assigned_ao_id=$2)) ORDER BY c.id`, values),
      await this.db.query(`SELECT p.*, c.name AS company_name FROM prospects p LEFT JOIN companies c ON c.id=p.company_id AND c.client_id=p.client_id WHERE p.client_id=$1 AND ($2::integer IS NULL OR p.assigned_ao_id=$2) ORDER BY p.id`, values),
      await this.db.query(`SELECT a.* FROM ao_prospect_activity a JOIN prospects p ON p.id=a.prospect_id AND p.client_id=a.tenant_id WHERE a.tenant_id=$1 AND ($2::integer IS NULL OR p.assigned_ao_id=$2) ORDER BY a.id`, values),
    ];
    return { clientId: this.clientId, users: users.rows, companies: companies.rows,
      prospects: prospects.rows.map(p => { const company=companies.rows.find(c=>String(c.id)===String(p.company_id)) || {}; return {...p, website:p.website || company.website || null, address:p.ao_source_address || p.address || p.location || company.address || company.location || null}; }), contacts: prospects.rows.filter(p => p.first_name || p.last_name).map(p => ({
        id: p.id, prospect_id: p.id, name: [p.first_name,p.last_name].filter(Boolean).join(' '),
        email: p.email, phone: p.phone, title: p.job_title,
      })), activities: activities.rows };
  }

  async findAppliedClaim(fingerprint) {
    const { rows } = await this.db.query(
      `SELECT * FROM max_applied_claims WHERE claim_fingerprint = $1`,
      [fingerprint]
    );
    return rows[0] || null;
  }

  async recordAppliedClaim(entry) {
    await this.db.query(`
      INSERT INTO max_applied_claims (claim_fingerprint, ingestion_id, target_entity_type, target_entity_id)
      VALUES ($1,$2,$3,$4)
      ON CONFLICT (claim_fingerprint) DO NOTHING
    `, [entry.claim_fingerprint, entry.ingestion_id, entry.target_entity_type, entry.target_entity_id]);
  }

  async persistIngestion(record) {
    await this.db.query(`
      INSERT INTO max_operational_ingestions
        (id, client_id, source_type, source_actor, raw_source, received_at, receipt_summary, telemetry, pipeline_audit)
      VALUES ($1,$2,$3,$4,$5::jsonb,$6,$7,$8::jsonb,$9::jsonb)
    `, [
      record.id, record.client_id, record.source_type, record.source_actor,
      JSON.stringify(record.raw_source || {}), record.received_at,
      record.receipt_summary || null, JSON.stringify(record.telemetry || {}),
      JSON.stringify(record.pipeline_audit || {}),
    ]);
    return record;
  }

  async persistArtifact(artifact) {
    const id = artifact.id || uuid();
    await this.db.query(`
      INSERT INTO max_evidence_artifacts
        (id, ingestion_id, artifact_type, filename, metadata, raw_content)
      VALUES ($1,$2,$3,$4,$5::jsonb,$6::jsonb)
    `, [
      id, artifact.ingestion_id, artifact.artifact_type, artifact.filename || null,
      JSON.stringify(artifact.metadata || {}), JSON.stringify(artifact.raw_content || {}),
    ]);
    return { ...artifact, id };
  }

  async persistClaim(claim) {
    await this.db.query(`
      INSERT INTO max_ingestion_claims
        (ingestion_id, claim_type, claim_fingerprint, payload, resolution_status, resolution, safety_class)
      VALUES ($1,$2,$3,$4::jsonb,$5,$6::jsonb,$7)
      ON CONFLICT (ingestion_id, claim_fingerprint) DO NOTHING
    `, [
      claim.ingestion_id, claim.claim_type, claim.claim_fingerprint,
      JSON.stringify(claim.payload || {}), claim.resolution_status,
      JSON.stringify(claim.resolution || {}), claim.safety_class || null,
    ]);
    return claim;
  }

  async persistMutation(mutation) {
    await this.db.query(`
      INSERT INTO max_ingestion_mutations
        (ingestion_id, entity_type, entity_id, field_name, intended_value, safety_class, commit_status, observed_value, verification_status, metadata)
      VALUES ($1,$2,$3,$4,$5::jsonb,$6,$7,$8::jsonb,$9,$10::jsonb)
    `, [
      mutation.ingestion_id, mutation.entity_type, mutation.entity_id || null,
      mutation.field_name, JSON.stringify(mutation.intended_value ?? null),
      mutation.safety_class, mutation.commit_status || 'proposed',
      JSON.stringify(mutation.observed_value ?? null), mutation.verification_status || null,
      JSON.stringify({ claim_type: mutation.claim?.claim_type || null }),
    ]);
    return mutation;
  }

  async persistConflict(conflict) {
    await this.db.query(`
      INSERT INTO max_ingestion_conflicts
        (ingestion_id, conflict_type, existing_state, incoming_state)
      VALUES ($1,$2,$3::jsonb,$4::jsonb)
    `, [
      conflict.ingestion_id, conflict.conflict_type,
      JSON.stringify(conflict.existing_state || {}),
      JSON.stringify(conflict.incoming_state || {}),
    ]);
    return conflict;
  }

  async persistEvidenceLink(link) {
    await this.db.query(`
      INSERT INTO max_ingestion_evidence_links
        (client_id, entity_type, entity_id, field_name, ingestion_id, artifact_id, source_record, derivation, confidence)
      VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9)
    `, [
      link.client_id, link.entity_type, link.entity_id, link.field_name,
      link.ingestion_id, link.artifact_id || null,
      JSON.stringify(link.source_record || {}), link.derivation || null, link.confidence || null,
    ]);
    return link;
  }

  async readProspect(id) {
    const { rows } = await this.db.query(`
      SELECT p.*, c.name AS company_name
      FROM prospects p
      LEFT JOIN companies c ON c.id = p.company_id
      WHERE p.id = $1 AND p.client_id = $2
    `, [id, this.clientId]);
    return rows[0] || null;
  }

  async applyMutation(mutation) {
    if (mutation.safety_class === 'C') {
      mutation.commit_status = 'blocked';
      return { committed: false, mutation };
    }

    if (mutation.entity_type === 'company' && mutation.field_name === 'create') {
      const { rows } = await this.db.query(`
        INSERT INTO companies (name, client_id) VALUES ($1,$2) RETURNING id, name, client_id
      `, [mutation.intended_value.name, this.clientId]);
      mutation.entity_id = rows[0].id;
      mutation.commit_status = 'committed';
      return { committed: true, mutation, created: { company: rows[0] } };
    }

    if (mutation.entity_type === 'prospect' && mutation.field_name === 'create') {
      const payload = mutation.intended_value;
      let companyId = null;
      const existing = await this.db.query(
        `SELECT id FROM companies WHERE client_id = $1 AND lower(name) = lower($2) LIMIT 1`,
        [this.clientId, payload.company_name]
      );
      if (existing.rows[0]) {
        companyId = existing.rows[0].id;
      } else {
        const inserted = await this.db.query(
          `INSERT INTO companies (name, client_id) VALUES ($1,$2) RETURNING id`,
          [payload.company_name, this.clientId]
        );
        companyId = inserted.rows[0].id;
      }
      const contact = payload.contact || {};
      const names = String(contact.name || '').split(' ');
      const { rows } = await this.db.query(`
        INSERT INTO prospects
          (company_id, client_id, first_name, last_name, email, phone, job_title, assigned_ao_id, source, status)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'warm')
        RETURNING *
      `, [
        companyId, this.clientId,
        names[0] || null, names.slice(1).join(' ') || null,
        contact.email || null, contact.phone || null, contact.title || null,
        payload.assigned_ao_id || null, payload.source || 'AO_REPORTED',
      ]);
      mutation.entity_id = rows[0].id;
      mutation.commit_status = 'committed';
      return { committed: true, mutation, created: { prospect: rows[0] } };
    }

    const prospectId = mutation.entity_id;
    if (!prospectId) {
      mutation.commit_status = 'blocked';
      return { committed: false, mutation };
    }

    const fieldSql = {
      assigned_ao_id: 'assigned_ao_id = $2',
      ao_last_touch_at: 'ao_last_touch_at = $2::timestamptz',
      ao_next_action: 'ao_next_action = $2',
    };
    if (fieldSql[mutation.field_name]) {
      await this.db.query(
        `UPDATE prospects SET ${fieldSql[mutation.field_name]} WHERE id = $1 AND client_id = $3`,
        [prospectId, mutation.intended_value, this.clientId]
      );
    }
    if (['suppress_cold_outreach', 'relationship_active', 'follow_up_state', 'next_action_due_hint'].includes(mutation.field_name)) {
      await this.db.query(`
        UPDATE prospects
        SET acquisition_metadata = COALESCE(acquisition_metadata, '{}'::jsonb) || $2::jsonb
        WHERE id = $1 AND client_id = $3
      `, [
        prospectId,
        JSON.stringify({
          maxStateIngestion: {
            [mutation.field_name]: mutation.intended_value,
            relationship_active: mutation.field_name === 'relationship_active' ? true : undefined,
            suppress_cold_outreach: mutation.field_name === 'suppress_cold_outreach' ? true : undefined,
          },
        }),
        this.clientId,
      ]);
    }
    if (mutation.field_name === 'activity_append') {
      await this.db.query(`
        INSERT INTO ao_prospect_activity (prospect_id, tenant_id, activity_type, notes, metadata)
        VALUES ($1,$2,'note',$3,$4::jsonb)
      `, [prospectId, this.clientId, mutation.intended_value?.notes || 'AO reported activity', JSON.stringify(mutation.intended_value || {})]);
    }

    mutation.commit_status = 'committed';
    mutation.ingestion_id = mutation.ingestion_id || null;
    return { committed: true, mutation, prospect: await this.readProspect(prospectId) };
  }

  async verifyMutation(mutation) {
    if (!mutation.entity_id || mutation.field_name === 'create') {
      mutation.verification_status = 'VERIFIED';
      return mutation;
    }
    const prospect = await this.readProspect(mutation.entity_id);
    if (!prospect) {
      mutation.verification_status = 'COMMIT_VERIFICATION_FAILED';
      return mutation;
    }
    const map = {
      assigned_ao_id: 'assigned_ao_id',
      ao_next_action: 'ao_next_action',
      follow_up_state: 'follow_up_state',
    };
    const field = map[mutation.field_name];
    if (field) {
      mutation.observed_value = prospect[field];
      mutation.verification_status = String(prospect[field]) === String(mutation.intended_value)
        ? 'VERIFIED' : 'COMMIT_VERIFICATION_FAILED';
      return mutation;
    }
    mutation.verification_status = 'VERIFIED';
    return mutation;
  }

  async createExpectation(expectation) {
    const { rows } = await this.db.query(`
      INSERT INTO max_open_expectations
        (client_id, prospect_id, ao_id, expectation_type, description, expected_window, status, source_ingestion_id, source_claim_id, source_evidence)
      VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7,$8,$9,$10::jsonb)
      RETURNING *
    `, [
      expectation.client_id, expectation.prospect_id || null, expectation.ao_id || null,
      expectation.expectation_type, expectation.description || null,
      JSON.stringify(expectation.expected_window || {}), expectation.status || 'WAITING',
      expectation.source_ingestion_id || null, expectation.source_claim_id || null,
      JSON.stringify(expectation.source_evidence || {}),
    ]);
    return rows[0];
  }

  async listOpenExpectations({ clientId = this.clientId } = {}) {
    const { rows } = await this.db.query(`
      SELECT * FROM max_open_expectations
      WHERE client_id = $1 AND status IN ('OPEN','WAITING','OVERDUE')
      ORDER BY updated_at DESC
    `, [clientId]);
    return rows;
  }
}

module.exports = {
  PostgresStateStore,
};
