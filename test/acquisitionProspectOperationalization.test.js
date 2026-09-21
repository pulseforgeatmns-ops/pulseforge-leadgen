'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const {
  operationalizeAcquisitionProspect,
  getAcquisitionProspectProjection,
  projectionIdFor,
  resolveOperationalVertical,
} = require('../services/acquisitionProspectOperationalization');

function now() {
  return new Date('2026-09-10T12:00:00.000Z').toISOString();
}

function akProspect(overrides = {}) {
  return {
    id: 'ak_babrun_prospect_p001',
    tenant_id: '13',
    client_id: 13,
    object_type: 'prospect_intelligence',
    title: 'P001 - Lemon Cleaning',
    content: {
      company: 'Lemon Cleaning',
      contact: 'Max Walls',
      role: 'Founder',
      industry: 'Cleaning / home services',
      prospectCode: 'P001',
      recommendedNextAction: 'Send approved simplified first-touch outreach.',
      ...(overrides.content || {}),
    },
    evidence: [],
    provenance: { types: ['ASSISTANT_SYNTHESIS'], sourceDocument: 'babrun-acquisition-extraction.md' },
    relationships: [],
    epistemic_state: overrides.epistemic_state || 'UNKNOWN',
    validation_state: overrides.validation_state || 'UNVALIDATED',
    lifecycle_state: 'active',
    tags: ['babrun'],
    ...overrides,
  };
}

function akOutreach(overrides = {}) {
  return {
    id: 'ak_babrun_outreach_final_01',
    tenant_id: '13',
    client_id: 13,
    object_type: 'outreach_asset',
    title: 'Final first-ten outreach 1: Lemon Cleaning',
    relationships: [
      {
        type: 'targets_prospect',
        target: { id: 'ak_babrun_prospect_p001' },
      },
    ],
    lifecycle_state: 'STAKEHOLDER_VALIDATED',
    ...overrides,
  };
}

function createFakePool(seed = {}) {
  const state = {
    knowledge: seed.knowledge || [akProspect(), akOutreach()],
    companies: seed.companies || [],
    prospects: seed.prospects || [],
    projections: seed.projections || [],
    clients: seed.clients || [
      { id: 13, target_verticals: [], vertical_tiers: {} },
      { id: 10, target_verticals: [{ vertical: 'property_manager' }], vertical_tiers: { property_manager: 'A' } },
    ],
    statements: [],
    companySeq: 1,
    prospectSeq: 1,
  };

  function clone(value) {
    return JSON.parse(JSON.stringify(value));
  }

  const api = {
    state,
    async connect() {
      return {
        query: api.query,
        release() {},
      };
    },
    async query(sql, params = []) {
      const normalized = String(sql).replace(/\s+/g, ' ').trim();
      state.statements.push(normalized);
      if (/^BEGIN|^COMMIT|^ROLLBACK/i.test(normalized)) return { rows: [] };
      if (/CREATE EXTENSION|ALTER TABLE|CREATE TABLE|CREATE INDEX/i.test(normalized)) return { rows: [] };

      if (/FROM acquisition_knowledge_objects WHERE id = \$1 AND tenant_id = \$2/i.test(normalized)) {
        return {
          rows: state.knowledge
            .filter((row) => row.id === params[0] && row.tenant_id === params[1])
            .map(clone),
        };
      }
      if (/FROM acquisition_knowledge_objects WHERE tenant_id = \$1 AND object_type = 'outreach_asset'/i.test(normalized)) {
        return {
          rows: state.knowledge
            .filter((row) => row.tenant_id === params[0] && row.object_type === 'outreach_asset' && row.lifecycle_state !== 'archived')
            .map(clone),
        };
      }
      if (/SELECT target_verticals, vertical_tiers FROM clients WHERE id = \$1/i.test(normalized)) {
        return {
          rows: state.clients
            .filter((row) => row.id === params[0])
            .map(clone),
        };
      }
      if (/FROM acquisition_prospect_projections WHERE tenant_id = \$1 AND acquisition_knowledge_object_id = \$2/i.test(normalized)) {
        return {
          rows: state.projections
            .filter((row) => row.tenant_id === params[0] && row.acquisition_knowledge_object_id === params[1])
            .map(clone),
        };
      }
      if (/SELECT \* FROM acquisition_prospect_projections WHERE tenant_id = \$1/i.test(normalized)) {
        let rows = state.projections.filter((row) => row.tenant_id === params[0]);
        if (/acquisition_knowledge_object_id = \$2/i.test(normalized)) {
          rows = rows.filter((row) => row.acquisition_knowledge_object_id === params[1]);
        }
        return { rows: rows.map(clone) };
      }
      if (/WHERE p.client_id = \$1 AND p.acquisition_knowledge_object_id = \$2/i.test(normalized)) {
        return {
          rows: state.prospects
            .filter((row) => row.client_id === params[0] && row.acquisition_knowledge_object_id === params[1])
            .map((row) => ({ ...clone(row), company_name: state.companies.find((company) => company.id === row.company_id)?.name || null })),
        };
      }
      if (/lower\(coalesce\(c.name, ''\)\) = lower\(\$2\)/i.test(normalized)) {
        return {
          rows: state.prospects
            .filter((row) => {
              const company = state.companies.find((item) => item.id === row.company_id && item.client_id === row.client_id);
              return row.client_id === params[0] &&
                String(company?.name || '').toLowerCase() === String(params[1]).toLowerCase() &&
                String(row.first_name || '').toLowerCase() === String(params[2]).toLowerCase() &&
                String(row.last_name || '').toLowerCase() === String(params[3] || '').toLowerCase();
            })
            .map((row) => ({ ...clone(row), company_name: state.companies.find((company) => company.id === row.company_id)?.name || null })),
        };
      }
      if (/FROM companies WHERE client_id = \$1 AND lower\(coalesce\(name, ''\)\) = lower\(\$2\)/i.test(normalized)) {
        return {
          rows: state.companies
            .filter((row) => row.client_id === params[0] && String(row.name || '').toLowerCase() === String(params[1]).toLowerCase())
            .map(clone),
        };
      }
      if (/INSERT INTO companies/i.test(normalized)) {
        const row = {
          id: `company-${state.companySeq++}`,
          client_id: params[0],
          name: params[1],
          industry: params[2],
          acquisition_metadata: JSON.parse(params[3]),
        };
        state.companies.push(row);
        return { rows: [clone(row)] };
      }
      if (/INSERT INTO prospects/i.test(normalized)) {
        const row = {
          id: `prospect-${state.prospectSeq++}`,
          client_id: params[0],
          company_id: params[1],
          first_name: params[2],
          last_name: params[3],
          job_title: params[4],
          source: 'acquisition_knowledge',
          status: 'cold',
          vertical: params[5],
          acquisition_knowledge_object_id: params[6],
          acquisition_projection_id: params[7],
          acquisition_source: 'acquisition_knowledge',
          acquisition_metadata: JSON.parse(params[8]),
          email: null,
          email_verified: false,
        };
        state.prospects.push(row);
        return { rows: [clone(row)] };
      }
      if (/UPDATE prospects SET acquisition_knowledge_object_id/i.test(normalized)) {
        const row = state.prospects.find((item) => item.client_id === params[0] && item.id === params[1]);
        if (!row) return { rows: [] };
        row.acquisition_knowledge_object_id = row.acquisition_knowledge_object_id || params[2];
        row.acquisition_projection_id = params[3];
        row.acquisition_source = row.acquisition_source || 'acquisition_knowledge';
        row.acquisition_metadata = { ...(row.acquisition_metadata || {}), ...JSON.parse(params[4]) };
        return { rows: [clone(row)] };
      }
      if (/INSERT INTO acquisition_prospect_projections/i.test(normalized)) {
        const row = {
          id: params[0],
          tenant_id: params[1],
          client_id: params[2],
          acquisition_knowledge_object_id: params[3],
          prospect_id: params[4],
          company_id: params[5],
          company_identity: JSON.parse(params[6]),
          person_identity: JSON.parse(params[7]),
          provenance: JSON.parse(params[8]),
          epistemic_state: params[9],
          validation_state: params[10],
          linked_outreach_asset_ids: params[11],
          status: params[12],
          review_reason: params[13],
          created_at: now(),
          updated_at: now(),
        };
        const index = state.projections.findIndex((item) => (
          item.tenant_id === row.tenant_id &&
          item.acquisition_knowledge_object_id === row.acquisition_knowledge_object_id
        ));
        if (index >= 0) state.projections[index] = { ...state.projections[index], ...row, created_at: state.projections[index].created_at };
        else state.projections.push(row);
        return { rows: [clone(index >= 0 ? state.projections[index] : row)] };
      }
      throw new Error(`Unhandled fake SQL: ${normalized}`);
    },
  };
  return api;
}

test('operationalizes an AK prospect into a tenant-scoped prospect without fabricating email', async () => {
  const pool = createFakePool();
  const result = await operationalizeAcquisitionProspect({
    tenantId: 13,
    acquisitionKnowledgeObjectId: 'ak_babrun_prospect_p001',
    apply: true,
  }, { pool });

  assert.equal(result.dryRun, false);
  assert.equal(result.prospectId, 'prospect-1');
  assert.equal(result.companyId, 'company-1');
  assert.deepEqual(result.linkedOutreachAssetIds, ['ak_babrun_outreach_final_01']);
  assert.equal(result.fabricatedEmail, false);
  assert.equal(pool.state.prospects[0].email, null);
  assert.equal(pool.state.prospects[0].vertical, null);
  assert.equal(pool.state.prospects[0].acquisition_knowledge_object_id, 'ak_babrun_prospect_p001');
  assert.equal(pool.state.prospects[0].acquisition_metadata.sourceIndustry, 'Cleaning / home services');
  assert.equal(pool.state.prospects[0].acquisition_metadata.sourceAkObjectId, 'ak_babrun_prospect_p001');
  assert.equal(pool.state.prospects[0].acquisition_metadata.verticalResolution.reason, 'no_safe_canonical_mapping');
  assert.equal(pool.state.projections[0].epistemic_state, 'UNKNOWN');
  assert.equal(pool.state.projections[0].validation_state, 'UNVALIDATED');
});

test('resolves free-form industry with known canonical mapping', () => {
  assert.deepEqual(resolveOperationalVertical('Home services'), {
    sourceIndustry: 'Home services',
    vertical: 'home_services',
    matched: true,
    reason: 'already_canonical_or_exact_supported_slug',
  });
});

test('unknown free-form industry resolves to null', () => {
  assert.deepEqual(resolveOperationalVertical('Founder-led specialty trades with bespoke crews'), {
    sourceIndustry: 'Founder-led specialty trades with bespoke crews',
    vertical: null,
    matched: false,
    reason: 'no_safe_canonical_mapping',
  });
});

test('already-canonical vertical passes through only when supported', () => {
  assert.deepEqual(resolveOperationalVertical('commercial_cleaning'), {
    sourceIndustry: 'commercial_cleaning',
    vertical: 'commercial_cleaning',
    matched: true,
    reason: 'already_canonical_or_exact_supported_slug',
  });
});

test('free-form slash industry never becomes an invented prospect vertical', async () => {
  const pool = createFakePool();
  await operationalizeAcquisitionProspect({
    tenantId: 13,
    acquisitionKnowledgeObjectId: 'ak_babrun_prospect_p001',
    apply: true,
  }, { pool });

  assert.equal(pool.state.prospects[0].vertical, null);
  assert.notEqual(pool.state.prospects[0].vertical, 'cleaning_home_services');
  assert.equal(pool.state.prospects[0].acquisition_metadata.sourceIndustry, 'Cleaning / home services');
});

test('operationalization is idempotent and retry-safe', async () => {
  const pool = createFakePool();
  const input = {
    tenantId: 13,
    acquisitionKnowledgeObjectId: 'ak_babrun_prospect_p001',
    apply: true,
  };
  const first = await operationalizeAcquisitionProspect(input, { pool });
  const second = await operationalizeAcquisitionProspect(input, { pool });

  assert.equal(first.prospectId, second.prospectId);
  assert.equal(pool.state.companies.length, 1);
  assert.equal(pool.state.prospects.length, 1);
  assert.equal(pool.state.projections.length, 1);
});

test('dry run reports intended projection without writes', async () => {
  const pool = createFakePool();
  const result = await operationalizeAcquisitionProspect({
    tenantId: 13,
    acquisitionKnowledgeObjectId: 'ak_babrun_prospect_p001',
  }, { pool });

  assert.equal(result.dryRun, true);
  assert.equal(result.wouldCreateCompany, true);
  assert.equal(result.wouldCreateProspect, true);
  assert.equal(result.enrichmentEligible, true);
  assert.equal(pool.state.companies.length, 0);
  assert.equal(pool.state.prospects.length, 0);
  assert.equal(pool.state.projections.length, 0);
});

test('tenant isolation blocks tenant 10 from operationalizing tenant 13 AK objects', async () => {
  const pool = createFakePool();
  await assert.rejects(
    operationalizeAcquisitionProspect({
      tenantId: 10,
      acquisitionKnowledgeObjectId: 'ak_babrun_prospect_p001',
      apply: true,
    }, { pool }),
    /Acquisition knowledge object was not found for this tenant/
  );
  assert.equal(pool.state.prospects.length, 0);
  assert.equal(pool.state.projections.length, 0);
});

test('tenant-scoped projection retrieval cannot cross tenant boundaries', async () => {
  const pool = createFakePool();
  await operationalizeAcquisitionProspect({
    tenantId: 13,
    acquisitionKnowledgeObjectId: 'ak_babrun_prospect_p001',
    apply: true,
  }, { pool });

  const tenant13 = await getAcquisitionProspectProjection({
    tenantId: 13,
    acquisitionKnowledgeObjectId: 'ak_babrun_prospect_p001',
  }, { pool });
  const tenant10 = await getAcquisitionProspectProjection({
    tenantId: 10,
    acquisitionKnowledgeObjectId: 'ak_babrun_prospect_p001',
  }, { pool });

  assert.equal(tenant13.length, 1);
  assert.equal(tenant13[0].id, projectionIdFor('13', 'ak_babrun_prospect_p001'));
  assert.equal(tenant10.length, 0);
});

test('ambiguous deterministic identity matches require review instead of silent merge', async () => {
  const pool = createFakePool({
    companies: [
      { id: 'company-1', client_id: 13, name: 'Lemon Cleaning' },
      { id: 'company-2', client_id: 13, name: 'Lemon Cleaning' },
    ],
    prospects: [],
  });

  await assert.rejects(
    operationalizeAcquisitionProspect({
      tenantId: 13,
      acquisitionKnowledgeObjectId: 'ak_babrun_prospect_p001',
      apply: true,
    }, { pool }),
    /Multiple companies match/
  );
  assert.equal(pool.state.projections.length, 0);
});
