'use strict';

// Reuse attributable AK research and operational contacts through Scout's normal
// SEC boundary. A contactable research prospect is not a demonstrated buyer.
const { governedContactReason, founderFirst } = require('../utils/governedContactEligibility');
const { createGovernedOutboundTenantContext } = require('./governedOutboundTenant');
const { READINESS_STATES } = require('../packages/max/scoutAcquisition/Types');
const norm = value => String(value || '').trim().toLowerCase().replace(/[ -]+/g, '_');

function cohortOperatingEvidence(content = {}, provenance = {}, row = {}) {
  if (content.operatingEvidence?.ownerName) return content.operatingEvidence;
  if (!content.contact && !content.contactResolution?.bestEmail) return null;
  const sourceUrl = (provenance.sourceUrls || []).find(url => /^https:\/\//i.test(String(url || '')))
    || (content.website && /^https:\/\//i.test(content.website) ? content.website : null);
  if (!sourceUrl) return null;
  const location = String(content.location || '').trim();
  const [cityPart, regionPart] = location.split(',').map(part => part.trim());
  const observedAt = provenance.sourceVerification?.fetchedAt
    || row.acquisition_metadata?.provenance?.operationalizedAt
    || null;
  if (!observedAt) return null;
  const observation = content.icpEvaluation?.reasons?.find(r => r.kind === 'OBSERVED')?.text
    || `Scout persisted ${content.company || 'an operating business'} with attributable founder contact evidence.`;
  return {
    ownerName: content.contact,
    ownerRole: content.role || 'Founder',
    operatingBusiness: content.icpEvaluation?.fit !== false,
    country: 'United States',
    city: cityPart || null,
    region: regionPart || null,
    sourceUrl,
    observedAt,
    observation,
  };
}

function normalizedKnowledgeContent(row = {}) {
  const content = row.knowledge_content || {};
  const provenance = row.knowledge_provenance || content.provenance || {};
  const operatingEvidence = cohortOperatingEvidence(content, provenance, row);
  const icpFit = content.icpFit
    || (content.icpEvaluation?.fit ? 'Good' : null);
  if (!operatingEvidence && !icpFit) return content;
  return { ...content, icpFit, operatingEvidence: operatingEvidence || content.operatingEvidence };
}

function qualifyKnowledgeContact(row, mission, policy = {}) {
  const content = normalizedKnowledgeContent(row);
  const facts = content.operatingEvidence || {};
  const plan = mission.structuredMission || mission.payload?.structuredMission || {};
  const segment = norm(plan.market?.segment || mission.targetSegment);
  if (!['small_business_owner', 'small_business_owners', 'founder_led_smb', 'founder_led_small_business'].includes(segment)) return 'unsupported_knowledge_segment';
  const reason = governedContactReason(row, { tenantId: String(row.client_id), ...policy });
  if (reason) return reason;
  if (!facts.observedAt || !facts.sourceUrl || !/^https:\/\//.test(facts.sourceUrl)
    || !facts.ownerName || !/founder|owner/i.test(facts.ownerRole || '') || !facts.operatingBusiness) return 'operating_evidence_missing';
  if (norm(content.contact) !== norm(facts.ownerName) || norm(content.company) !== norm(row.company_name)) return 'knowledge_identity_mismatch';
  const geography = plan.geography || {};
  if (!geography.region || norm(facts.country) !== norm(geography.region)) return 'knowledge_geography_mismatch';
  if (geography.cities?.length && !geography.cities.some(city => norm(city) === norm(facts.city))) return 'knowledge_city_mismatch';
  // Historical analyst assessment is usable for cold-research selection only
  // with the stakeholder-approved prospect-bound asset, never as buyer intent.
  const recoverableCohort = norm(content.cohort || '').includes('babrun_cohort')
    && content.icpEvaluation?.fit === true
    && !governedContactReason(row, { tenantId: String(row.client_id), ...policy });
  if (!recoverableCohort && (!row.approved_asset || !/excellent|strong|good/i.test(content.icpFit || ''))) {
    return 'reviewed_research_fit_missing';
  }
  return null;
}

function approvedCopyIndexFromInventory(inventory = []) {
  const entries = [];
  for (const row of inventory) {
    if (row.qualificationReason || !row.approved_asset) continue;
    entries.push([String(row.company_id), row.approved_asset]);
    if (row.id) entries.push([String(row.id), row.approved_asset]);
    if (row.prospect_id) entries.push([String(row.prospect_id), row.approved_asset]);
  }
  return entries.length ? Object.fromEntries(entries) : undefined;
}

async function loadKnowledgeInventory(pool, mission, policy = {}) {
  const tenantId = String(mission.tenantId || mission.clientId || mission.tenant_id || '');
  if (!['10','13'].includes(tenantId) || !createGovernedOutboundTenantContext(tenantId).usesTenantMailboxTransport) return [];
  const { rows } = await pool.query(`SELECT p.*, p.id AS prospect_id, c.name AS company_name,
      c.website AS company_website, c.domain AS company_domain, k.id AS knowledge_id,
      k.content AS knowledge_content, k.evidence AS knowledge_evidence, k.provenance AS knowledge_provenance,
      asset.data AS approved_asset
    FROM prospects p JOIN companies c ON c.id=p.company_id AND c.client_id=p.client_id
    JOIN acquisition_knowledge_objects k ON k.id=p.acquisition_knowledge_object_id AND k.tenant_id=p.client_id::text
    LEFT JOIN LATERAL (SELECT jsonb_build_object('id',a.id,'version',a.version,'content',a.content) AS data
      FROM acquisition_knowledge_objects a WHERE a.tenant_id=k.tenant_id AND a.object_type='outreach_asset'
      AND a.validation_state='STAKEHOLDER_VALIDATED' AND a.lifecycle_state NOT IN ('RETIRED','ARCHIVED')
      AND EXISTS (SELECT 1 FROM jsonb_array_elements(a.relationships) rel
        WHERE rel->>'type'='targets_prospect' AND COALESCE(rel->'target'->>'id',rel->>'targetId')=k.id)
      ORDER BY a.updated_at DESC LIMIT 1) asset ON true
    WHERE p.client_id=$1 AND k.object_type='prospect_intelligence' AND k.lifecycle_state NOT IN ('RETIRED','ARCHIVED')
    ORDER BY p.id`, [Number(tenantId)]);
  return rows.sort(founderFirst).map(row => ({ ...row, qualificationReason: qualifyKnowledgeContact(row, mission, policy) }));
}

async function discoverKnowledgeInventory(mission, opts = {}) {
  if (!opts.pool) return null;
  const inventory = await loadKnowledgeInventory(opts.pool, mission);
  if (!inventory.length) return null;
  const store = new (require('./governedOutboundStore').GovernedOutboundStore)(opts.pool, String(mission.tenantId || mission.clientId));
  const fitCandidates = [];
  for (const row of inventory) {
    if (row.qualificationReason) continue;
    const identity = { candidateId: String(row.company_id), prospectId: String(row.id), companyId: String(row.company_id), email: row.email };
    if (await store.candidateOwnership(identity) || await store.suppression(identity, mission.id)) continue;
    const facts = row.knowledge_content.operatingEvidence;
    fitCandidates.push({ id: identity.companyId, companyId: identity.companyId, prospectId: identity.prospectId,
      name: row.company_name, website: row.company_website, location: `${facts.city}, ${facts.country}`,
      readinessState: READINESS_STATES.UNKNOWN, buyerReadiness: 'unknown',
      evidenceRefs: [{ id: `${row.knowledge_id}_operating`, source: facts.sourceUrl,
        label: facts.observation, observedAt: facts.observedAt, entityId: identity.companyId,
        provenance: { kind: 'first_party_website', knowledgeId: row.knowledge_id } },
      { id: `${row.knowledge_id}_research`, source: row.knowledge_id,
        label: `Historical analyst ICP assessment: ${row.knowledge_content.icpFit}; stakeholder-approved first-touch asset ${row.approved_asset.id}. Buyer intent unknown.`,
        provenance: { kind: 'historical_analyst_judgment', knowledgeId: row.knowledge_id } }],
      unknowns: ['Buyer intent, felt pain, willingness to discuss, employee count and budget are not established.'],
      signals: [],
    });
  }
  // Returning zero here intentionally keeps investigation open. Existing AK
  // inventory must not silently fall back to unrelated Places organizations.
  return { payload: { fitCandidates, qualifiedCount: fitCandidates.length, candidateUniverse: fitCandidates,
    candidateCount: inventory.length, discoveryStatus: fitCandidates.length ? 'complete' : 'incomplete',
    unknowns: ['Research fit does not establish buyer readiness.'], source: 'canonical_acquisition_knowledge' } };
}

// Governed daily preparation consumes the same clean, scoped CRM inventory that
// Max reports. It must not rediscover a different Places cohort and strand the
// contacts Scout has already qualified. Buyer readiness remains unknown.
async function discoverGovernedInventory(mission, opts = {}) {
  const tenantId = String(mission.tenantId || mission.clientId || '');
  if (!opts.pool || !opts.governedProgram || !['10', '13'].includes(tenantId)
    || mission.orchestrationMissionId !== opts.governedProgram.source_mission_id) return null;
  const program = opts.governedProgram;
  const { hash, missionScope } = require('../packages/acquisition-mission/DailyOutboundPolicy');
  if (hash(missionScope(mission)) !== program.scope_hash) return null;
  const store = new (require('./governedOutboundStore').GovernedOutboundStore)(opts.pool, tenantId);
  const inventory = await require('./maxOutboundControlLoop').loadCleanInventory(
    opts.pool, store, mission, Number(tenantId), program.policy
  );
  if (!inventory.clean.length) return null;
  const ids = inventory.clean.map(row => row.prospectId);
  const { rows } = await opts.pool.query(`SELECT p.id,p.company_id,p.vertical,p.service_area_match,
    p.enrichment_provenance,c.name,c.website,c.domain,c.location FROM prospects p
    JOIN companies c ON c.id=p.company_id AND c.client_id=p.client_id
    WHERE p.client_id=$2 AND p.id::text=ANY($1::text[])`, [ids, Number(tenantId)]);
  const fitCandidates = rows.map(row => ({ id: String(row.id), prospectId: String(row.id),
    companyId: String(row.company_id), name: row.name, website: row.website || row.domain,
    location: row.location, industry: row.vertical,
    readinessState: READINESS_STATES.UNKNOWN, buyerReadiness: 'unknown', signals: [],
    evidenceRefs: [{ id: `${row.id}_contact_acquisition`, entityId: String(row.id),
      source: row.enrichment_provenance.email.source_url || row.website || row.domain,
      label: `Verified contact acquired by ${row.enrichment_provenance.email.source}; company and recipient domain binding checked.`,
      observedAt: row.enrichment_provenance.email.resolved_at,
      provenance: row.enrichment_provenance.email },
    { id: `${row.id}_crm_scope`, entityId: String(row.id), source: `crm:${row.id}`,
      label: `Canonical company ${row.name}; recorded vertical ${row.vertical}; service area ${row.service_area_match || row.location}.`,
      provenance: { kind: 'canonical_crm_scope', prospectId: String(row.id), companyId: String(row.company_id) } }],
    unknowns: ['Buyer intent, incumbent vendor, budget and willingness to discuss are unknown.'],
  }));
  return { payload: { fitCandidates, qualifiedCount: fitCandidates.length, candidateUniverse: fitCandidates,
    candidateCount: fitCandidates.length, discoveryStatus: 'complete', source: 'governed_clean_inventory',
    unknowns: ['Clean contact eligibility does not establish buyer readiness.'] } };
}

module.exports = {
  qualifyKnowledgeContact,
  normalizedKnowledgeContent,
  approvedCopyIndexFromInventory,
  loadKnowledgeInventory,
  discoverKnowledgeInventory,
  discoverGovernedInventory,
};
