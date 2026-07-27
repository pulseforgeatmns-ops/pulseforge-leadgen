'use strict';

/**
 * EQL QueryPlanner — AST → deterministic execution plan (SPEC-020).
 *
 * Pure planning: no I/O, no domain branching.
 */

/**
 * @param {import('./types').EqlAst} ast
 * @returns {import('./types').EqlPlan}
 */
function planEql(ast) {
  if (!ast || !ast.kind) {
    throw new Error('QueryPlanner requires a parsed EQL AST');
  }

  /** @type {import('./types').EqlPlanStep[]} */
  const steps = [];

  switch (ast.kind) {
    case 'FIND':
      steps.push({
        op: 'scan',
        args: { target: ast.target, entity: ast.entity },
      });
      if (ast.where && ast.where.length > 0) {
        steps.push({ op: 'filter', args: { where: ast.where } });
      }
      if (ast.orderBy) {
        steps.push({ op: 'sort', args: { orderBy: ast.orderBy } });
      }
      if (ast.limit != null) {
        steps.push({ op: 'limit', args: { limit: ast.limit } });
      }
      break;

    case 'SHOW':
      if (ast.relation && ast.related) {
        steps.push({
          op: 'relate',
          args: {
            target: ast.target,
            relation: ast.relation,
            related: ast.related,
          },
        });
      } else {
        steps.push({
          op: 'scan',
          args: { target: ast.target, entity: null },
        });
      }
      if (ast.where && ast.where.length > 0) {
        steps.push({ op: 'filter', args: { where: ast.where } });
      }
      break;

    case 'REPLAY':
      steps.push({
        op: 'replay',
        args: {
          subject: ast.subject,
          from: ast.from,
          to: ast.to,
        },
      });
      break;

    case 'COMPARE':
      steps.push({
        op: 'compare',
        args: { left: ast.left, right: ast.right },
      });
      break;

    case 'EXPLAIN':
      steps.push({
        op: 'explain_entity',
        args: { entity: ast.entity },
      });
      break;

    default:
      throw new Error(`Unknown EQL statement kind: ${/** @type {any} */ (ast).kind}`);
  }

  if (ast.explain && ast.kind !== 'EXPLAIN') {
    steps.push({
      op: 'explain',
      args: {
        target: 'target' in ast ? ast.target : null,
        entity: 'entity' in ast ? ast.entity : 'related' in ast ? ast.related : null,
      },
    });
  }

  steps.push({ op: 'project', args: {} });

  return Object.freeze({
    kind: ast.kind,
    ast,
    steps: Object.freeze(steps.map((s) => Object.freeze({ ...s, args: Object.freeze({ ...(s.args || {}) }) }))),
    explain: Boolean(ast.explain),
  });
}

module.exports = {
  planEql,
  QueryPlanner: { plan: planEql },
};
