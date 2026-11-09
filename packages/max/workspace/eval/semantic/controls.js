'use strict';

/**
 * Controls for the semantic evaluation.
 *
 * Negative controls are systems that are SAFE (read-only, no mutation, no
 * leakage) but do not understand the question. The suite is only meaningful
 * if they fail the semantic dimensions:
 *
 *   genericReport   always returns the tenant's full read-only status report
 *   cannedFallback  always returns one fixed generic sentence
 *   echo            restates the question
 *
 * The positive control (oracle) answers from the case's own expectations. It
 * proves the graders are satisfiable; it is not a model of good prose.
 */

const { composeOperationalReport, loadOperationalSnapshot } = require('../../OperationalInspection');
const { NOW, createFixturePool } = require('./tenants');

const reportCache = new Map();

async function tenantReport(tenant) {
  if (!reportCache.has(tenant.id)) {
    const snapshot = await loadOperationalSnapshot({
      tenantId: tenant.id,
      question: 'operational status report',
      opts: { pool: createFixturePool(), now: NOW, env: { ANCHOR_EMAIL_RECOVERY_MAINTENANCE: 'true' } },
    });
    reportCache.set(tenant.id, composeOperationalReport(snapshot, { tenantName: tenant.name }));
  }
  return reportCache.get(tenant.id);
}

const READ_ALL = ['acquisition_missions', 'acquisition_mission_events', 'prospects', 'touchpoints', 'agent_log'];

const genericReport = async ({ tenant }) => ({ prose: await tenantReport(tenant), sourcesRead: READ_ALL });
const cannedFallback = async () => ({ prose: 'Your approved Blueprint is on file but too thin to support a useful recommendation yet. I would refine the core sections before prioritizing next moves.' });
const echo = async ({ turn }) => ({ prose: `You asked: "${turn.q}". I have noted the question.` });

/** Answers each graded turn from its expectation — proves satisfiability. */
const oracle = async ({ turn, tenant }) => {
  const e = turn.expect;
  if (!e) return { prose: 'ok', sourcesRead: READ_ALL };
  if (e.act === 'authorization') {
    return { prose: 'Recorded your authorization for the pending execution approval on the bound mission; dispatched through the canonical execution router.', consumesDecision: true };
  }
  const parts = [`On ${e.intent.replace(/_/g, ' ')}:`];
  if (e.referent) {
    const m = tenant.missions[e.referent];
    parts.push(`"${m.title}" [${m.id}].`);
  }
  for (const g of e.anchors || []) {
    const anchors = Array.isArray(g) ? g : tenant.facts[g];
    parts.push(`Evidence: ${anchors.filter((a) => !/^am_|^mission_/.test(a) || !e.referent).join('; ')}.`);
  }
  if (e.uncertainty) parts.push('Agent run history is unavailable, so I cannot verify why Scout has not produced prospects.');
  parts.push('No changes were made.');
  return { prose: parts.join(' '), sourcesRead: READ_ALL };
};

module.exports = { NEGATIVE_CONTROLS: { genericReport, cannedFallback, echo }, oracle };
