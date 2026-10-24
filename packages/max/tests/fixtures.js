'use strict';

const {
  createKnowledgeRuntime,
  NODE_TYPES,
  EDGE_TYPES,
} = require('../../knowledge');

const TENANT = '10';
const AS_OF = '2026-07-26T12:00:00.000Z';

/**
 * Seed a rich graph for reasoning tests.
 * @param {import('../../knowledge/services/KnowledgeService').KnowledgeService} [knowledge]
 */
async function seedReasoningFixture(knowledge) {
  const runtime = knowledge
    ? { knowledge }
    : createKnowledgeRuntime({ withSync: false });
  const k = runtime.knowledge;

  const company = await k.createNode({
    tenantId: TENANT,
    type: NODE_TYPES.COMPANY,
    name: 'Lodgism',
    metadata: {
      industry: 'property_management',
      location: 'Manchester NH',
      technology: 'Guesty',
      website: 'https://lodgism.com',
      confidence: 0.75,
      growth: 'expansion',
    },
  });

  const relatedCo = await k.createNode({
    tenantId: TENANT,
    type: NODE_TYPES.COMPANY,
    name: 'CleanPartner',
    metadata: { industry: 'cleaning', confidence: 0.6 },
  });

  const owner = await k.createNode({
    tenantId: TENANT,
    type: NODE_TYPES.PERSON,
    name: 'Alex Owner',
    email: 'alex@lodgism.com',
    title: 'Owner',
    metadata: { confidence: 0.9 },
  });

  const ally = await k.createNode({
    tenantId: TENANT,
    type: NODE_TYPES.PERSON,
    name: 'Pat Ally',
    email: 'pat@cleanpartner.com',
    title: 'Founder',
    metadata: { confidence: 0.8 },
  });

  await k.createEdge({
    tenantId: TENANT,
    type: EDGE_TYPES.WORKS_FOR,
    fromId: owner.id,
    toId: company.id,
  });
  await k.createEdge({
    tenantId: TENANT,
    type: EDGE_TYPES.HAS_CONTACT,
    fromId: company.id,
    toId: owner.id,
  });
  await k.createEdge({
    tenantId: TENANT,
    type: EDGE_TYPES.WORKS_FOR,
    fromId: ally.id,
    toId: relatedCo.id,
  });
  await k.createEdge({
    tenantId: TENANT,
    type: EDGE_TYPES.KNOWS,
    fromId: owner.id,
    toId: ally.id,
  });

  const sent = await k.createNode({
    tenantId: TENANT,
    type: NODE_TYPES.INTERACTION,
    channel: 'email',
    actionType: 'sent',
    summary: 'Intro email',
    occurredAt: '2026-07-10T12:00:00.000Z',
  });
  const opened = await k.createNode({
    tenantId: TENANT,
    type: NODE_TYPES.INTERACTION,
    channel: 'email',
    actionType: 'open',
    summary: 'Opened intro',
    occurredAt: '2026-07-11T09:00:00.000Z',
  });
  const reply = await k.createNode({
    tenantId: TENANT,
    type: NODE_TYPES.INTERACTION,
    channel: 'email',
    actionType: 'reply',
    summary: 'Interested reply',
    occurredAt: '2026-07-20T15:00:00.000Z',
  });
  for (const interaction of [sent, opened, reply]) {
    await k.createEdge({
      tenantId: TENANT,
      type: EDGE_TYPES.PARTICIPATED_IN,
      fromId: owner.id,
      toId: interaction.id,
    });
  }

  const evidenceGrowth = await k.evidence.createEvidence({
    tenantId: TENANT,
    sourceType: 'website',
    sourceId: 'https://lodgism.com/careers',
    summary: 'Hiring for Operations Manager; expansion into new services',
    confidence: 0.88,
  });
  await k.evidence.attachEvidence(TENANT, evidenceGrowth.id, company.id);

  const evidenceRisk = await k.evidence.createEvidence({
    tenantId: TENANT,
    sourceType: 'crm_note',
    sourceId: 'note:decline-2026-06',
    summary: 'Recently declined; existing vendor contract mentioned',
    confidence: 0.7,
  });
  await k.evidence.attachEvidence(TENANT, evidenceRisk.id, company.id);

  const claimGrowth = await k.claims.createClaim({
    tenantId: TENANT,
    statement: 'Lodgism is hiring and expanding STR operations in Manchester',
    subjectId: company.id,
    evidenceIds: [evidenceGrowth.id],
  });

  const claimOverflow = await k.claims.createClaim({
    tenantId: TENANT,
    statement: 'Service demand overflow — understaffed and looking for vendor',
    subjectId: company.id,
    evidenceIds: [evidenceGrowth.id],
  });

  const claimRisk = await k.claims.createClaim({
    tenantId: TENANT,
    statement: 'Prospect recently declined due to existing vendor contract',
    subjectId: company.id,
    evidenceIds: [evidenceRisk.id],
  });

  const claimReferral = await k.claims.createClaim({
    tenantId: TENANT,
    statement: 'Warm referral / mutual contact via CleanPartner founder',
    subjectId: company.id,
    evidenceIds: [evidenceGrowth.id],
  });

  return {
    runtime,
    knowledge: k,
    tenantId: TENANT,
    asOf: AS_OF,
    company,
    relatedCo,
    owner,
    ally,
    interactions: { sent, opened, reply },
    evidence: { evidenceGrowth, evidenceRisk },
    claims: { claimGrowth, claimOverflow, claimRisk, claimReferral },
  };
}

/**
 * Sparse / weak graph for low-score high-confidence style cases.
 */
async function seedSparseFixture(knowledge) {
  const runtime = knowledge
    ? { knowledge }
    : createKnowledgeRuntime({ withSync: false });
  const k = runtime.knowledge;

  const company = await k.createNode({
    tenantId: TENANT,
    type: NODE_TYPES.COMPANY,
    name: 'QuietCo',
    metadata: { industry: 'unknown', confidence: 0.95 },
  });

  const evidence = await k.evidence.createEvidence({
    tenantId: TENANT,
    sourceType: 'crm_company',
    sourceId: 'company:quiet',
    summary: 'Company observed in CRM with no outreach history',
    confidence: 0.95,
  });
  await k.evidence.attachEvidence(TENANT, evidence.id, company.id);

  await k.claims.createClaim({
    tenantId: TENANT,
    statement: 'QuietCo exists in CRM',
    subjectId: company.id,
    evidenceIds: [evidence.id],
  });

  return {
    runtime,
    knowledge: k,
    tenantId: TENANT,
    asOf: AS_OF,
    company,
  };
}

module.exports = {
  TENANT,
  AS_OF,
  seedReasoningFixture,
  seedSparseFixture,
};
