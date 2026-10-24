'use strict';

/**
 * SPEC-152 — Concept Reasoner.
 * Traverses the concept graph and synthesizes explanations from relationships.
 */

const { OPERATING_MODEL, getRelationship } = require('../../identity/OperatingModel');
const { REASONING_GOALS } = require('./ConceptPlanner');

function normalizeText(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function capitalizeFirst(text) {
  const s = normalizeText(text);
  if (!s) return s;
  return s.charAt(0).toUpperCase() + s.slice(1);
}

function joinSentences(parts) {
  return parts
    .filter(Boolean)
    .map((part) => {
      const trimmed = normalizeText(part);
      if (!trimmed) return '';
      return /[.!?]$/.test(trimmed) ? trimmed : `${trimmed}.`;
    })
    .join(' ');
}

function labelFor(concept) {
  if (!concept) return '';
  return concept.label || concept.id;
}

function describeEdge(edge, graph) {
  const from = graph.getConcept(edge.from);
  const to = graph.getConcept(edge.to);
  const fromLabel = labelFor(from);
  const toLabel = labelFor(to);

  switch (edge.relation) {
    case 'delegates_to':
      return `${fromLabel} delegates to ${toLabel}`;
    case 'coordinates':
      return `${fromLabel} coordinates ${toLabel}`;
    case 'specializes_in':
      return `${fromLabel} specializes in ${toLabel}`;
    case 'supports':
      return `${fromLabel} supports ${toLabel}`;
    case 'cannot_override':
      return `${fromLabel} cannot override ${toLabel}`;
    case 'retains_authority':
      return `${fromLabel} retains authority over ${toLabel}`;
    case 'requires':
      return `${toLabel} requires ${fromLabel}`;
    case 'depends_on':
      return `${fromLabel} depends on ${toLabel}`;
    case 'balances':
      return `${fromLabel} balances ${toLabel} against competing priorities`;
    case 'explains':
      return `${fromLabel} explains ${toLabel}`;
    case 'owns':
      return `${fromLabel} owns ${toLabel}`;
    default:
      return `${fromLabel} ${edge.relation.replace(/_/g, ' ')} ${toLabel}`;
  }
}

function collectTraversalSentences(graph, plan) {
  const traversal = graph.traverse(plan.concepts, {
    targetConcepts: plan.goal === REASONING_GOALS.EXPLAIN_AUTHORITY
      ? ['operator', 'business_decisions']
      : plan.goal === REASONING_GOALS.RESOLVE_CONFLICT
        ? ['operator', 'governance']
        : [],
    maxHops: 4,
  });

  const sentences = [];
  for (let i = 1; i < traversal.path.length; i += 1) {
    const step = traversal.path[i];
    if (step.edge) {
      sentences.push(capitalizeFirst(describeEdge(step.edge, graph)));
    }
  }
  return { sentences, traversal };
}

function synthesizeIdentity() {
  return joinSentences([
    capitalizeFirst(OPERATING_MODEL.why[0]),
    capitalizeFirst(OPERATING_MODEL.why[1]),
    capitalizeFirst(OPERATING_MODEL.why[2]),
    capitalizeFirst(OPERATING_MODEL.why[3]),
    'My purpose is to ' +
      OPERATING_MODEL.purpose.slice(0, 3).join(', ').toLowerCase().replace(/,\s([^,]+)$/, ', and $1'),
  ]);
}

function synthesizeCompare(objects) {
  const names = (objects || ['max', 'scout']).map((o) => o.toLowerCase());
  const parts = [];

  for (const name of names) {
    const rel = getRelationship(name);
    if (rel) {
      const label = name === 'max' ? 'Max' : capitalizeFirst(name);
      parts.push(`${label} owns ${rel.owns.charAt(0).toLowerCase()}${rel.owns.slice(1)}`);
      parts.push(capitalizeFirst(rel.reasoning));
    }
  }

  if (names.includes('max') && names.length >= 2) {
    const specialist = names.find((n) => n !== 'max');
    const specRel = getRelationship(specialist);
    if (specRel) {
      parts.push(
        `${capitalizeFirst(specialist)} optimizes ${specRel.optimizes.charAt(0).toLowerCase()}${specRel.optimizes.slice(1)}`
      );
      parts.push(
        `Max optimizes ${OPERATING_MODEL.relationships.max.optimizes.charAt(0).toLowerCase()}${OPERATING_MODEL.relationships.max.optimizes.slice(1)}`
      );
    }
  }

  return joinSentences(parts);
}

function synthesizeAuthority() {
  return joinSentences([
    'You retain final authority over: ' + OPERATING_MODEL.authority.operator.join(', ') + '.',
    'I own: ' + OPERATING_MODEL.authority.max.join(', ') + '.',
    capitalizeFirst(OPERATING_MODEL.principles.find((p) => p.startsWith('Operator retains'))),
    'Max never replaces operator judgment.',
    'Specialists execute within their domain — they do not approve outreach or make business decisions.',
  ]);
}

function synthesizeConflict(specialists) {
  const names = (specialists || ['scout', 'paige']).map(capitalizeFirst);
  return joinSentences([
    `When ${names.join(' and ')} disagree, neither specialist wins by default.`,
    'Max synthesizes their domain evidence against mission priorities and operator goals.',
    capitalizeFirst(OPERATING_MODEL.principles.find((p) => p.startsWith('Operator retains'))),
    'You retain final authority — Max explains the tradeoff; you decide.',
  ]);
}

function synthesizeBoundaries() {
  return joinSentences([
    'These responsibilities never belong to me:',
    ...OPERATING_MODEL.boundaries.map((b) => capitalizeFirst(b)),
    capitalizeFirst(OPERATING_MODEL.principles.find((p) => p.startsWith('Operator retains'))),
  ]);
}

function synthesizeFailureModes() {
  return joinSentences([
    'You should weigh my advice against your own judgment when:',
    ...OPERATING_MODEL.failureModes.map((f) => capitalizeFirst(f)),
    capitalizeFirst(OPERATING_MODEL.principles.find((p) => p.startsWith('Fail closed'))),
  ]);
}

function synthesizeDependency(specialists) {
  const names = (specialists || ['scout', 'paige']).map((n) => n.toLowerCase());
  const parts = [
    `${capitalizeFirst(names[0])} and ${capitalizeFirst(names[1] || 'paige')} serve different layers of the operating system.`,
  ];
  for (const name of names) {
    const rel = getRelationship(name);
    if (rel) parts.push(capitalizeFirst(rel.reasoning));
  }
  parts.push(
    'Max coordinates both — discovery without communication planning wastes pipeline; communication without discovery lacks targets.',
    'If one fails, Max surfaces the gap, rebalances priorities, and preserves mission continuity until the operator intervenes.'
  );
  return joinSentences(parts);
}

function synthesizeSpecialistSeparation() {
  return joinSentences([
    ...OPERATING_MODEL.why.map(capitalizeFirst),
    capitalizeFirst(OPERATING_MODEL.principles.find((p) => p.startsWith('Delegate expertise'))),
  ]);
}

/**
 * @param {object} plan
 * @param {import('./ConceptGraph').ConceptGraph} graph
 * @returns {object|null}
 */
function reasonFromPlan(plan, graph) {
  if (!plan || !graph) return null;

  const { sentences, traversal } = collectTraversalSentences(graph, plan);
  let prose = '';

  switch (plan.goal) {
    case REASONING_GOALS.EXPLAIN_IDENTITY:
      prose = synthesizeIdentity();
      break;
    case REASONING_GOALS.COMPARE_ROLES:
      prose = synthesizeCompare(plan.specialists && plan.specialists.length ? plan.specialists : ['max', 'scout']);
      break;
    case REASONING_GOALS.EXPLAIN_AUTHORITY:
      prose = synthesizeAuthority();
      break;
    case REASONING_GOALS.RESOLVE_CONFLICT:
      prose = synthesizeConflict(plan.specialists);
      break;
    case REASONING_GOALS.EXPLAIN_BOUNDARIES:
      prose = synthesizeBoundaries();
      break;
    case REASONING_GOALS.EXPLAIN_FAILURE_MODES:
      prose = synthesizeFailureModes();
      break;
    case REASONING_GOALS.EXPLAIN_DEPENDENCY:
    case REASONING_GOALS.EXPLAIN_RELATIONSHIPS:
      prose = synthesizeDependency(plan.specialists);
      break;
    case REASONING_GOALS.EXPLAIN_SPECIALIZATION:
      prose = synthesizeSpecialistSeparation();
      break;
    default:
      prose = synthesizeIdentity();
  }

  if (sentences.length) {
    const hopSummary = sentences.slice(0, 4).join('. ');
    if (plan.goal === REASONING_GOALS.EXPLAIN_AUTHORITY || plan.goal === REASONING_GOALS.COMPARE_ROLES) {
      prose = joinSentences([hopSummary, prose]);
    }
  }

  const conceptsUsed = plan.concepts.filter((id) => graph.getConcept(id));
  const relationshipsTraversed = traversal.edges.map((edge) => ({
    from: edge.from,
    to: edge.to,
    relation: edge.relation,
  }));

  return {
    prose,
    conceptsUsed,
    relationshipsTraversed,
    hops: traversal.hops,
    goal: plan.goal,
    via: 'concept_graph_reasoning',
  };
}

function reasoningMetadata(plan, result) {
  if (!plan || !result) {
    return {
      conceptGraphReasoning: false,
      concepts: [],
      goal: null,
      hops: 0,
    };
  }

  return {
    conceptGraphReasoning: true,
    operatingModelReflection: true,
    concepts: result.conceptsUsed,
    activeConcepts: plan.concepts,
    goal: plan.goal,
    hops: result.hops,
    relationshipsTraversed: result.relationshipsTraversed,
    reasoningTarget: plan.goal,
    sectionsUsed: result.conceptsUsed,
    via: plan.via || 'concept_graph_reasoning',
  };
}

module.exports = {
  reasonFromPlan,
  reasoningMetadata,
  describeEdge,
  joinSentences,
};
