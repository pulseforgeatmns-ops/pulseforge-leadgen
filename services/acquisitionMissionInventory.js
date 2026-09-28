'use strict';

// Reuse attributable AK research and operational contacts through Scout's normal
// SEC boundary. A contactable research prospect is not a demonstrated buyer.
const { governedContactReason, founderFirst } = require('../utils/governedContactEligibility');
const { createGovernedOutboundTenantContext } = require('./governedOutboundTenant');
const norm = value => String(value || '').trim().toLowerCase().replace(/[ -]+/g, '_');

function qualifyKnowledgeContact(row, mission, policy = {}) {
  const content = row.knowledge_content || {};
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
  if (!row.approved_asset || !/excellent|strong|good/i.test(content.icpFit || '')) return 'reviewed_research_fit_missing';
  return null;
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
      readinessState: 'UNKNOWN', buyerReadiness: 'unknown',
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

module.exports = { qualifyKnowledgeContact, loadKnowledgeInventory, discoverKnowledgeInventory };
