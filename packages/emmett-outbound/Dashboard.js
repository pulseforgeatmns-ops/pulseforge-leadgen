'use strict';

/**
 * SPEC-117 — operator dashboard snapshot.
 */

const { pacingWarning } = require('./Pacing');

function buildDashboard({ health, capacity, governor, queue, recommendations, approvedPlan, sentToday = 0 } = {}) {
  const recommended = Number(capacity?.recommended || 0);
  const scheduled = Number(queue?.selectedCount || 0);
  const remaining = Math.max(0, recommended - Number(sentToday || 0));
  const warning = pacingWarning(queue?.items || []);
  const warnings = [];
  if (health?.label && health.label !== 'healthy') {
    warnings.push({ title: `Inbox ${health.label}`, body: (health.reasons || []).join('. ') });
  }
  if ((health?.reasons || []).some((r) => /warming/i.test(r))) {
    warnings.push({ title: 'Domain warming', body: 'No action needed beyond the recommended cap.' });
  }
  if (governor?.halt) {
    warnings.push({ title: governor.outcome, body: governor.reason });
  }
  if (warning) warnings.push({ title: 'Campaign pacing', body: warning });

  return {
    kind: 'emmett_outbound_dashboard',
    spec: 'SPEC-117',
    inbox: {
      label: health?.label || 'unknown',
      score: health?.score ?? null,
      reasons: health?.reasons || [],
      factors: health?.factors || [],
    },
    capacity: {
      recommended,
      scheduled,
      remaining,
      sentToday: Number(sentToday || 0),
      ceiling: capacity?.ceiling,
      confidence: capacity?.confidence,
      statement: capacity?.statement,
      tomorrow: capacity?.tomorrow,
      factors: capacity?.factors || [],
    },
    governor: governor || null,
    queue: queue || null,
    approved: Boolean(approvedPlan && approvedPlan.status === 'approved'),
    approvedPlan: approvedPlan || null,
    warnings,
    suggestions: recommendations || [],
  };
}

module.exports = {
  buildDashboard,
};
