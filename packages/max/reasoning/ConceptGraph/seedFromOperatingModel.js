'use strict';

/**
 * SPEC-152 — Seed the Max operating concept graph from SPEC-151 OperatingModel.
 */

const { OPERATING_MODEL } = require('../../identity/OperatingModel');
const { ConceptGraph } = require('./ConceptGraph');

function buildOperatingConceptGraph() {
  const concepts = [
    {
      id: 'operator',
      label: 'Operator',
      category: 'authority',
      description: 'The human who retains final authority over business decisions and external actions.',
    },
    {
      id: 'max',
      label: 'Max',
      category: 'identity',
      description: OPERATING_MODEL.role.mission,
    },
    {
      id: 'identity',
      label: 'Identity',
      category: 'identity',
      description: 'Max role as the business operating system — integration, synthesis, and governance.',
    },
    {
      id: 'purpose',
      label: 'Purpose',
      category: 'identity',
      description: OPERATING_MODEL.purpose.join(' '),
    },
    {
      id: 'authority',
      label: 'Authority',
      category: 'authority',
      description: 'Who may decide, approve, and commit the business externally.',
    },
    {
      id: 'business_decisions',
      label: 'Business Decisions',
      category: 'business',
      description: OPERATING_MODEL.authority.operator.join('; '),
    },
    {
      id: 'governance',
      label: 'Governance',
      category: 'process',
      description: 'How specialists coordinate under operator approval and Max integration.',
    },
    {
      id: 'mission',
      label: 'Mission',
      category: 'mission',
      description: 'Active acquisition missions anchor execution context and prioritization.',
    },
    {
      id: 'specialization',
      label: 'Specialization',
      category: 'principle',
      description: 'Specialists own domain depth; Max owns whole-business integration.',
    },
    {
      id: 'conflict',
      label: 'Conflict',
      category: 'process',
      description: 'When specialists disagree, Max synthesizes and the operator decides.',
    },
    {
      id: 'market_discovery',
      label: 'Market Discovery',
      category: 'process',
      description: 'Finding, scoring, and attaching market evidence within the service area.',
    },
    {
      id: 'outreach_approval',
      label: 'Outreach Approval',
      category: 'authority',
      description: 'External sends, content, and CRM changes require operator approval.',
    },
    {
      id: 'boundaries',
      label: 'Boundaries',
      category: 'boundary',
      description: OPERATING_MODEL.boundaries.join(' '),
    },
    {
      id: 'principles',
      label: 'Principles',
      category: 'principle',
      description: OPERATING_MODEL.principles.join(' '),
    },
  ];

  for (const principle of OPERATING_MODEL.principles) {
    const slug = principle
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '_')
      .replace(/^_|_$/g, '')
      .slice(0, 48);
    concepts.push({
      id: `principle_${slug}`,
      label: principle.split(' — ')[0],
      category: 'principle',
      description: principle,
    });
  }

  for (const [key, rel] of Object.entries(OPERATING_MODEL.relationships)) {
    concepts.push({
      id: key,
      label: key === 'max' ? 'Max' : key.charAt(0).toUpperCase() + key.slice(1),
      category: key === 'max' ? 'identity' : 'specialist',
      description: rel.reasoning,
    });
  }

  const relationships = [
    { from: 'operator', to: 'max', relation: 'delegates_to' },
    { from: 'max', to: 'operator', relation: 'cannot_override' },
    { from: 'operator', to: 'business_decisions', relation: 'retains_authority' },
    { from: 'operator', to: 'outreach_approval', relation: 'retains_authority' },
    { from: 'max', to: 'authority', relation: 'explains' },
    { from: 'max', to: 'purpose', relation: 'explains' },
    { from: 'max', to: 'identity', relation: 'owns' },
    { from: 'max', to: 'governance', relation: 'coordinates' },
    { from: 'max', to: 'mission', relation: 'coordinates' },
    { from: 'authority', to: 'operator', relation: 'requires' },
    { from: 'authority', to: 'max', relation: 'explains' },
    { from: 'specialization', to: 'principles', relation: 'explains' },
    { from: 'conflict', to: 'governance', relation: 'requires' },
    { from: 'conflict', to: 'operator', relation: 'requires' },
    { from: 'governance', to: 'operator', relation: 'requires' },
    { from: 'outreach_approval', to: 'operator', relation: 'requires' },
    { from: 'boundaries', to: 'max', relation: 'explains' },
    { from: 'purpose', to: 'identity', relation: 'explains' },
  ];

  for (const specialist of Object.keys(OPERATING_MODEL.relationships).filter((k) => k !== 'max')) {
    relationships.push(
      { from: specialist, to: 'max', relation: 'supports' },
      { from: 'max', to: specialist, relation: 'coordinates' },
      { from: 'max', to: specialist, relation: 'balances' }
    );
    const rel = OPERATING_MODEL.relationships[specialist];
    if (/discovery|market|sourcing|scoring/i.test(rel.owns)) {
      relationships.push({ from: specialist, to: 'market_discovery', relation: 'specializes_in' });
    }
  }

  relationships.push(
    { from: 'scout', to: 'market_discovery', relation: 'specializes_in' },
    { from: 'scout', to: 'specialization', relation: 'explains' },
    { from: 'paige', to: 'outreach_approval', relation: 'requires' },
    { from: 'scout', to: 'outreach_approval', relation: 'requires' },
    { from: 'scout', to: 'paige', relation: 'depends_on' },
    { from: 'paige', to: 'scout', relation: 'depends_on' },
    { from: 'scout', to: 'business_decisions', relation: 'cannot_override' },
    { from: 'paige', to: 'business_decisions', relation: 'cannot_override' },
    { from: 'max', to: 'business_decisions', relation: 'cannot_override' }
  );

  return new ConceptGraph({ concepts, relationships });
}

let cachedGraph = null;

function getOperatingConceptGraph() {
  if (!cachedGraph) {
    cachedGraph = buildOperatingConceptGraph();
  }
  return cachedGraph;
}

function resetOperatingConceptGraphCache() {
  cachedGraph = null;
}

module.exports = {
  buildOperatingConceptGraph,
  getOperatingConceptGraph,
  resetOperatingConceptGraphCache,
};
