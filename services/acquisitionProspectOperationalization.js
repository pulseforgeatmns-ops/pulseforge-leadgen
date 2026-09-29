'use strict';

const crypto = require('node:crypto');
const { normalizeVertical } = require('../utils/verticalTiers');

const ESTABLISHED_OPERATIONAL_VERTICALS = new Set([
  'accounting',
  'architecture_engineering',
  'auto',
  'auto_repair',
  'b2b_accounting',
  'cleaning',
  'cleaning_company_overflow',
  'cleaning_residential',
  'commercial_cleaning',
  'commercial_electrical',
  'commercial_hvac',
  'commercial_insurance',
  'commercial_landscaping',
  'commercial_mechanical',
  'commercial_office',
  'commercial_roofing',
  'commercial_real_estate',
  'decks',
  'equipment_rental',
  'exterior_remodeling',
  'facility_services',
  'fire_protection',
  'fitness',
  'freight_brokerage',
  'hoa_management',
  'home_renovation',
  'home_services',
  'insurance_restoration',
  'interior_renovation',
  'investor_flipper',
  'janitorial',
  'landscaping',
  'landscaping_residential',
  'law_firm',
  'lead_gen_agency',
  'listing_agent',
  'low_voltage_security',
  'marketing_agency',
  'med_spa',
  'medical_office',
  'msp_it_services',
  'probate_attorney',
  'property',
  'property_management',
  'property_manager',
  'real_estate_developer',
  'realtor',
  'renovation_lender',
  'restaurant',
  'restoration',
  'restoration_remodeling_partner',
  'salon',
  'siding',
  'staffing_recruiting',
  'str_manager',
  'unknown',
  'wholesale_distribution',
  'windows',
]);

const OPERATIONAL_VERTICAL_ALIASES = Object.freeze({
  'accounting firm': 'accounting',
  'auto repair': 'auto_repair',
  'commercial cleaning': 'commercial_cleaning',
  'home service': 'home_services',
  'home services': 'home_services',
  'law firm': 'law_firm',
  'managed it services': 'msp_it_services',
  'med spa': 'med_spa',
  'property management': 'property_management',
  'short term rental manager': 'str_manager',
});

function defaultPool() {
  return require('../db');
}

function operationalizationError(code, message, details = {}) {
  const err = new Error(message || code);
  err.code = code;
  err.details = details;
  return err;
}

function normalizeTenantId(value) {
  const text = String(value ?? '').trim();
  if (!text) throw operationalizationError('tenant_required', 'tenantId is required.');
  return text;
}

function normalizeClientId(value) {
  const parsed = Number.parseInt(String(value ?? ''), 10);
  if (!Number.isFinite(parsed)) throw operationalizationError('client_required', 'clientId is required.');
  return parsed;
}

function asJson(value, fallback) {
  if (value == null) return fallback;
  if (typeof value === 'string') {
    try {
      return JSON.parse(value);
    } catch (_err) {
      return fallback;
    }
  }
  return value;
}

function clean(value) {
  return String(value ?? '').replace(/\s+/g, ' ').trim();
}

function canonicalVerticalSet(extra = []) {
  const values = new Set(ESTABLISHED_OPERATIONAL_VERTICALS);
  for (const value of extra || []) {
    const normalized = normalizeVertical(value);
    if (normalized) values.add(normalized);
  }
  return values;
}

function resolveOperationalVertical(value, opts = {}) {
  const sourceIndustry = clean(value);
  if (!sourceIndustry) {
    return {
      sourceIndustry: null,
      vertical: null,
      matched: false,
      reason: 'missing_source_industry',
    };
  }
  const canonicalValues = canonicalVerticalSet(opts.canonicalVerticals);
  const normalized = normalizeVertical(sourceIndustry);
  if (canonicalValues.has(normalized)) {
    return {
      sourceIndustry,
      vertical: normalized,
      matched: true,
      reason: 'already_canonical_or_exact_supported_slug',
    };
  }
  const aliasTarget = OPERATIONAL_VERTICAL_ALIASES[sourceIndustry.toLowerCase()];
  if (aliasTarget && canonicalValues.has(aliasTarget)) {
    return {
      sourceIndustry,
      vertical: aliasTarget,
      matched: true,
      reason: 'known_existing_alias',
    };
  }
  return {
    sourceIndustry,
    vertical: null,
    matched: false,
    reason: 'no_safe_canonical_mapping',
  };
}

function parsePersonName(name) {
  const text = clean(name);
  if (!text) return { firstName: null, lastName: null, fullName: null };
  const parts = text.split(' ').filter(Boolean);
  if (parts.length === 1) return { firstName: parts[0], lastName: null, fullName: text };
  return {
    firstName: parts.slice(0, -1).join(' '),
    lastName: parts[parts.length - 1],
    fullName: text,
  };
}

function projectionIdFor(tenantId, knowledgeObjectId) {
  const digest = crypto
    .createHash('sha256')
    .update(`${tenantId}:${knowledgeObjectId}`)
    .digest('hex')
    .slice(0, 24);
  return `akp_${digest}`;
}

function akRowFromDb(row = {}) {
  return {
    id: row.id,
    tenantId: row.tenant_id,
    clientId: row.client_id == null ? null : Number(row.client_id),
    objectType: row.object_type,
    title: row.title,
    content: asJson(row.content, {}),
    evidence: asJson(row.evidence, []),
    provenance: asJson(row.provenance, {}),
    relationships: asJson(row.relationships, []),
    epistemicState: row.epistemic_state || 'UNKNOWN',
    validationState: row.validation_state || 'UNVALIDATED',
    tags: row.tags || [],
  };
}

function projectionRowFromDb(row = {}) {
  return {
    id: row.id,
    tenantId: row.tenant_id,
    clientId: row.client_id,
    acquisitionKnowledgeObjectId: row.acquisition_knowledge_object_id,
    prospectId: row.prospect_id,
    companyId: row.company_id || null,
    companyIdentity: asJson(row.company_identity, {}),
    personIdentity: asJson(row.person_identity, {}),
    provenance: asJson(row.provenance, {}),
    epistemicState: row.epistemic_state,
    validationState: row.validation_state,
    linkedOutreachAssetIds: row.linked_outreach_asset_ids || [],
    status: row.status,
    reviewReason: row.review_reason || null,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

async function ensureOperationalizationSchema(pool = defaultPool()) {
  await pool.query(`
    CREATE EXTENSION IF NOT EXISTS pgcrypto;

    ALTER TABLE prospects
      ADD COLUMN IF NOT EXISTS acquisition_knowledge_object_id TEXT,
      ADD COLUMN IF NOT EXISTS acquisition_projection_id TEXT,
      ADD COLUMN IF NOT EXISTS acquisition_source TEXT,
      ADD COLUMN IF NOT EXISTS acquisition_metadata JSONB NOT NULL DEFAULT '{}'::jsonb;

    ALTER TABLE companies
      ADD COLUMN IF NOT EXISTS industry TEXT,
      ADD COLUMN IF NOT EXISTS location TEXT,
      ADD COLUMN IF NOT EXISTS website TEXT,
      ADD COLUMN IF NOT EXISTS acquisition_metadata JSONB NOT NULL DEFAULT '{}'::jsonb;

    CREATE TABLE IF NOT EXISTS acquisition_prospect_projections (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      client_id INTEGER NOT NULL,
      acquisition_knowledge_object_id TEXT NOT NULL,
      prospect_id UUID NOT NULL,
      company_id UUID,
      company_identity JSONB NOT NULL DEFAULT '{}'::jsonb,
      person_identity JSONB NOT NULL DEFAULT '{}'::jsonb,
      provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
      epistemic_state TEXT NOT NULL,
      validation_state TEXT NOT NULL,
      linked_outreach_asset_ids TEXT[] NOT NULL DEFAULT '{}',
      status TEXT NOT NULL DEFAULT 'enrichment_pending',
      review_reason TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE (tenant_id, acquisition_knowledge_object_id),
      UNIQUE (tenant_id, prospect_id)
    );

    CREATE INDEX IF NOT EXISTS acquisition_prospect_projections_tenant_status_idx
      ON acquisition_prospect_projections (tenant_id, status);

    CREATE INDEX IF NOT EXISTS acquisition_prospect_projections_prospect_idx
      ON acquisition_prospect_projections (client_id, prospect_id);

    CREATE UNIQUE INDEX IF NOT EXISTS prospects_ak_object_tenant_idx
      ON prospects (client_id, acquisition_knowledge_object_id)
      WHERE acquisition_knowledge_object_id IS NOT NULL;
  `);
}

async function loadAcquisitionKnowledgeObject(client, tenantId, id) {
  const result = await client.query(
    `SELECT * FROM acquisition_knowledge_objects WHERE id = $1 AND tenant_id = $2 LIMIT 1`,
    [id, tenantId]
  );
  return result.rows[0] ? akRowFromDb(result.rows[0]) : null;
}

async function loadTenantOperationalVerticals(client, clientId) {
  const result = await client.query(
    `SELECT target_verticals, vertical_tiers
     FROM clients
     WHERE id = $1
     LIMIT 1`,
    [clientId]
  );
  const row = result.rows[0] || {};
  const verticals = [];
  const tiers = asJson(row.vertical_tiers, {});
  if (tiers && typeof tiers === 'object' && !Array.isArray(tiers)) {
    verticals.push(...Object.keys(tiers));
  }
  const targets = asJson(row.target_verticals, []);
  if (Array.isArray(targets)) {
    for (const target of targets) {
      if (target?.vertical) verticals.push(target.vertical);
    }
  }
  return verticals;
}

async function findLinkedOutreachAssets(client, tenantId, akObjectId) {
  const result = await client.query(
    `SELECT id, relationships
     FROM acquisition_knowledge_objects
     WHERE tenant_id = $1
       AND object_type = 'outreach_asset'
       AND lifecycle_state <> 'archived'`,
    [tenantId]
  );
  return result.rows
    .filter((row) => {
      const relationships = asJson(row.relationships, []);
      return relationships.some((relationship) => (
        relationship?.type === 'targets_prospect' &&
        (relationship.target?.id || relationship.targetId) === akObjectId
      ));
    })
    .map((row) => row.id);
}

function extractIdentity(knowledge) {
  const content = knowledge.content || {};
  const companyName = clean(content.company || content.companyName || content.business || content.prospectCompany);
  const contactName = clean(content.contact || content.person || content.founder || content.prospectContact);
  const person = parsePersonName(contactName);
  const companyIdentity = {
    name: companyName || null,
    industry: clean(content.industry) || null,
    prospectCode: clean(content.prospectCode) || null,
    recommendedNextAction: clean(content.recommendedNextAction) || null,
  };
  const personIdentity = {
    fullName: person.fullName,
    firstName: person.firstName,
    lastName: person.lastName,
    role: clean(content.role) || null,
  };
  return { companyIdentity, personIdentity };
}

function assertEligibleProspect(knowledge) {
  if (!knowledge) throw operationalizationError('ak_object_not_found', 'Acquisition knowledge object was not found for this tenant.');
  if (knowledge.objectType !== 'prospect_intelligence') {
    throw operationalizationError('ak_object_not_prospect', 'Acquisition knowledge object is not prospect intelligence.', {
      objectType: knowledge.objectType,
    });
  }
  const { companyIdentity } = extractIdentity(knowledge);
  if (!companyIdentity.name) {
    throw operationalizationError('ak_prospect_missing_company', 'Acquisition prospect does not contain a company identity.');
  }
}

async function findExistingProjection(client, tenantId, akObjectId) {
  const result = await client.query(
    `SELECT * FROM acquisition_prospect_projections
     WHERE tenant_id = $1 AND acquisition_knowledge_object_id = $2
     LIMIT 1`,
    [tenantId, akObjectId]
  );
  return result.rows[0] ? projectionRowFromDb(result.rows[0]) : null;
}

async function findExplicitProspectLink(client, clientId, akObjectId) {
  const result = await client.query(
    `SELECT p.*, c.name AS company_name
     FROM prospects p
     LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
     WHERE p.client_id = $1 AND p.acquisition_knowledge_object_id = $2
     LIMIT 2`,
    [clientId, akObjectId]
  );
  if (result.rows.length > 1) {
    throw operationalizationError('ambiguous_existing_projection', 'Multiple operational prospects already reference this AK object.');
  }
  return result.rows[0] || null;
}

async function findDeterministicProspectMatch(client, clientId, companyIdentity, personIdentity) {
  if (!companyIdentity.name || !personIdentity.firstName) return null;
  const result = await client.query(
    `SELECT p.*, c.name AS company_name
     FROM prospects p
     LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
     WHERE p.client_id = $1
       AND lower(coalesce(c.name, '')) = lower($2)
       AND lower(coalesce(p.first_name, '')) = lower($3)
       AND lower(coalesce(p.last_name, '')) = lower(coalesce($4, ''))
     LIMIT 2`,
    [clientId, companyIdentity.name, personIdentity.firstName, personIdentity.lastName || '']
  );
  if (result.rows.length > 1) {
    throw operationalizationError('ambiguous_identity_match', 'Multiple operational prospects match this AK prospect identity.');
  }
  return result.rows[0] || null;
}

async function findDeterministicCompany(client, clientId, companyIdentity) {
  const result = await client.query(
    `SELECT * FROM companies
     WHERE client_id = $1 AND lower(coalesce(name, '')) = lower($2)
     LIMIT 2`,
    [clientId, companyIdentity.name]
  );
  if (result.rows.length > 1) {
    throw operationalizationError('ambiguous_company_match', 'Multiple companies match this AK prospect company identity.');
  }
  return result.rows[0] || null;
}

async function createCompanyProjection(client, clientId, companyIdentity, metadata) {
  const result = await client.query(
    `INSERT INTO companies (client_id, name, industry, acquisition_metadata)
     VALUES ($1, $2, $3, $4::jsonb)
     RETURNING *`,
    [
      clientId,
      companyIdentity.name,
      companyIdentity.industry,
      JSON.stringify(metadata),
    ]
  );
  return result.rows[0];
}

async function createProspectProjection(client, clientId, companyId, personIdentity, companyIdentity, metadata) {
  const result = await client.query(
    `INSERT INTO prospects (
       client_id, company_id, first_name, last_name, job_title, source, status, vertical,
       acquisition_knowledge_object_id, acquisition_projection_id, acquisition_source, acquisition_metadata
     ) VALUES (
       $1,$2,$3,$4,$5,'acquisition_knowledge','cold',$6,$7,$8,'acquisition_knowledge',$9::jsonb
     )
     RETURNING *`,
    [
      clientId,
      companyId,
      personIdentity.firstName,
      personIdentity.lastName,
      personIdentity.role,
      metadata.operationalVertical || null,
      metadata.acquisitionKnowledgeObjectId,
      metadata.projectionId,
      JSON.stringify(metadata),
    ]
  );
  return result.rows[0];
}

async function markProspectLinked(client, clientId, prospectId, akObjectId, projectionId, metadata) {
  const result = await client.query(
    `UPDATE prospects
     SET acquisition_knowledge_object_id = COALESCE(acquisition_knowledge_object_id, $3),
         acquisition_projection_id = $4,
         acquisition_source = COALESCE(acquisition_source, 'acquisition_knowledge'),
         acquisition_metadata = COALESCE(acquisition_metadata, '{}'::jsonb) || $5::jsonb
     WHERE client_id = $1 AND id = $2
     RETURNING *`,
    [clientId, prospectId, akObjectId, projectionId, JSON.stringify(metadata)]
  );
  return result.rows[0] || null;
}

async function upsertProjection(client, input) {
  const result = await client.query(
    `INSERT INTO acquisition_prospect_projections (
       id, tenant_id, client_id, acquisition_knowledge_object_id, prospect_id, company_id,
       company_identity, person_identity, provenance, epistemic_state, validation_state,
       linked_outreach_asset_ids, status, review_reason
     ) VALUES (
       $1,$2,$3,$4,$5,$6,$7::jsonb,$8::jsonb,$9::jsonb,$10,$11,$12,$13,$14
     )
     ON CONFLICT (tenant_id, acquisition_knowledge_object_id) DO UPDATE SET
       prospect_id = EXCLUDED.prospect_id,
       company_id = EXCLUDED.company_id,
       company_identity = EXCLUDED.company_identity,
       person_identity = EXCLUDED.person_identity,
       provenance = EXCLUDED.provenance,
       epistemic_state = EXCLUDED.epistemic_state,
       validation_state = EXCLUDED.validation_state,
       linked_outreach_asset_ids = EXCLUDED.linked_outreach_asset_ids,
       status = EXCLUDED.status,
       review_reason = EXCLUDED.review_reason,
       updated_at = NOW()
     RETURNING *`,
    [
      input.id,
      input.tenantId,
      input.clientId,
      input.acquisitionKnowledgeObjectId,
      input.prospectId,
      input.companyId,
      JSON.stringify(input.companyIdentity),
      JSON.stringify(input.personIdentity),
      JSON.stringify(input.provenance),
      input.epistemicState,
      input.validationState,
      input.linkedOutreachAssetIds,
      input.status,
      input.reviewReason || null,
    ]
  );
  return projectionRowFromDb(result.rows[0]);
}

async function operationalizeAcquisitionProspect(input = {}, opts = {}) {
  const tenantId = normalizeTenantId(input.tenantId);
  const clientId = normalizeClientId(input.clientId || input.tenantId);
  const akObjectId = clean(input.acquisitionKnowledgeObjectId || input.knowledgeObjectId || input.akObjectId);
  if (!akObjectId) throw operationalizationError('ak_object_required', 'acquisitionKnowledgeObjectId is required.');

  const pool = opts.pool || defaultPool();
  const externalClient = Boolean(opts.client);
  const client = opts.client || await pool.connect();
  const dryRun = input.apply !== true && opts.apply !== true;

  try {
    if (!externalClient) await client.query('BEGIN');
    await ensureOperationalizationSchema(client);

    const knowledge = await loadAcquisitionKnowledgeObject(client, tenantId, akObjectId);
    assertEligibleProspect(knowledge);
    const { companyIdentity, personIdentity } = extractIdentity(knowledge);
    const tenantOperationalVerticals = await loadTenantOperationalVerticals(client, clientId);
    const verticalResolution = resolveOperationalVertical(companyIdentity.industry, {
      canonicalVerticals: tenantOperationalVerticals,
    });
    const linkedOutreachAssetIds = await findLinkedOutreachAssets(client, tenantId, akObjectId);
    const projectionId = projectionIdFor(tenantId, akObjectId);
    const provenance = {
      source: 'acquisition_knowledge',
      acquisitionKnowledgeObjectId: akObjectId,
      acquisitionKnowledgeTitle: knowledge.title,
      knowledgeProvenance: knowledge.provenance,
      linkedOutreachAssetIds,
      operationalizedAt: new Date().toISOString(),
    };
    const metadata = {
      projectionId,
      tenantId,
      acquisitionKnowledgeObjectId: akObjectId,
      sourceAkObjectId: akObjectId,
      sourceIndustry: verticalResolution.sourceIndustry,
      operationalVertical: verticalResolution.vertical,
      verticalResolution,
      epistemicState: knowledge.epistemicState,
      validationState: knowledge.validationState,
      linkedOutreachAssetIds,
      provenance,
    };

    const existingProjection = await findExistingProjection(client, tenantId, akObjectId);
    if (existingProjection) {
      if (!externalClient) await client.query('COMMIT');
      return {
        dryRun,
        created: false,
        linked: true,
        projection: existingProjection,
        prospectId: existingProjection.prospectId,
        companyId: existingProjection.companyId,
        enrichmentEligible: true,
        fabricatedEmail: false,
      };
    }

    const explicit = await findExplicitProspectLink(client, clientId, akObjectId);
    let company = explicit ? { id: explicit.company_id } : null;
    let prospect = explicit;
    let matchStrategy = explicit ? 'explicit_prospect_link' : null;

    if (!prospect) {
      prospect = await findDeterministicProspectMatch(client, clientId, companyIdentity, personIdentity);
      if (prospect) {
        company = { id: prospect.company_id };
        matchStrategy = 'deterministic_company_person_match';
      }
    }
    if (!company?.id) {
      company = await findDeterministicCompany(client, clientId, companyIdentity);
      if (company) matchStrategy = matchStrategy || 'deterministic_company_match';
    }

    const wouldCreateCompany = !company?.id;
    const wouldCreateProspect = !prospect?.id;
    if (dryRun) {
      if (!externalClient) await client.query('ROLLBACK');
      return {
        dryRun: true,
        created: false,
        linked: false,
        wouldCreateCompany,
        wouldCreateProspect,
        matchStrategy: matchStrategy || 'new_projection',
        acquisitionKnowledgeObjectId: akObjectId,
        companyIdentity,
        sourceIndustry: verticalResolution.sourceIndustry,
        operationalVertical: verticalResolution.vertical,
        verticalResolution,
        personIdentity,
        linkedOutreachAssetIds,
        epistemicState: knowledge.epistemicState,
        validationState: knowledge.validationState,
        enrichmentEligible: true,
        fabricatedEmail: false,
      };
    }

    if (!company?.id) {
      company = await createCompanyProjection(client, clientId, companyIdentity, metadata);
      matchStrategy = 'created_company_projection';
    }
    if (!prospect?.id) {
      prospect = await createProspectProjection(client, clientId, company.id, personIdentity, companyIdentity, metadata);
      matchStrategy = matchStrategy === 'created_company_projection'
        ? 'created_company_and_prospect_projection'
        : 'created_prospect_projection';
    } else {
      prospect = await markProspectLinked(client, clientId, prospect.id, akObjectId, projectionId, metadata);
    }

    const projection = await upsertProjection(client, {
      id: projectionId,
      tenantId,
      clientId,
      acquisitionKnowledgeObjectId: akObjectId,
      prospectId: prospect.id,
      companyId: company.id || prospect.company_id || null,
      companyIdentity,
      personIdentity,
      provenance: { ...provenance, matchStrategy },
      epistemicState: knowledge.epistemicState,
      validationState: knowledge.validationState,
      linkedOutreachAssetIds,
      status: 'enrichment_pending',
    });

    await markProspectLinked(client, clientId, prospect.id, akObjectId, projection.id, metadata);
    if (!externalClient) await client.query('COMMIT');
    return {
      dryRun: false,
      created: wouldCreateProspect,
      linked: true,
      matchStrategy,
      projection,
      prospectId: projection.prospectId,
      companyId: projection.companyId,
      linkedOutreachAssetIds,
      enrichmentEligible: true,
      fabricatedEmail: false,
    };
  } catch (err) {
    if (!externalClient) {
      try { await client.query('ROLLBACK'); } catch (_rollbackErr) { /* ignore rollback failure */ }
    }
    throw err;
  } finally {
    if (!externalClient) client.release();
  }
}

async function getAcquisitionProspectProjection(input = {}, opts = {}) {
  const tenantId = normalizeTenantId(input.tenantId);
  const akObjectId = clean(input.acquisitionKnowledgeObjectId || input.knowledgeObjectId || input.akObjectId);
  const prospectId = clean(input.prospectId);
  const pool = opts.pool || defaultPool();
  await ensureOperationalizationSchema(pool);
  const params = [tenantId];
  let where = 'tenant_id = $1';
  if (akObjectId) {
    params.push(akObjectId);
    where += ` AND acquisition_knowledge_object_id = $${params.length}`;
  }
  if (prospectId) {
    params.push(prospectId);
    where += ` AND prospect_id = $${params.length}`;
  }
  const result = await pool.query(
    `SELECT * FROM acquisition_prospect_projections WHERE ${where} ORDER BY created_at DESC`,
    params
  );
  return result.rows.map(projectionRowFromDb);
}

module.exports = {
  ensureOperationalizationSchema,
  operationalizeAcquisitionProspect,
  getAcquisitionProspectProjection,
  extractIdentity,
  projectionIdFor,
  resolveOperationalVertical,
  operationalizationError,
};
