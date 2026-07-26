'use strict';

/**
 * Watch Alerts — consumes WatchRegistry evaluations on period transitions.
 * Detection only; briefing surfaces structured alerts (no push).
 */
function buildWatchAlertsSection(contexts) {
  /** @type {object[]} */
  const alerts = [];

  for (const ctx of contexts || []) {
    for (const hit of ctx.triggeredWatches || []) {
      alerts.push({
        watchId: hit.watchId,
        companyId: ctx.companyId,
        companyName: ctx.companyName,
        tenantId: hit.tenantId,
        targetType: hit.targetType,
        targetId: hit.targetId,
        condition: hit.condition,
        at: hit.at,
        snapshotId: hit.snapshotId,
        scoreDelta: hit.scoreDelta,
        confidenceDelta: hit.confidenceDelta,
        matchedChanges: hit.matchedChanges || [],
        message: describeAlert(hit, ctx),
      });
    }
  }

  alerts.sort((a, b) => {
    const w = String(a.watchId).localeCompare(String(b.watchId));
    if (w !== 0) return w;
    return String(a.companyId).localeCompare(String(b.companyId));
  });

  return {
    total: alerts.length,
    items: alerts,
  };
}

function describeAlert(hit, ctx) {
  const cond = hit.condition || {};
  const parts = [
    `watch=${hit.watchId}`,
    `company=${ctx.companyId}`,
    `op=${cond.op}`,
  ];
  if (cond.field) parts.push(`field=${cond.field}`);
  if (cond.value != null) parts.push(`value=${cond.value}`);
  if (hit.scoreDelta != null) parts.push(`scoreDelta=${hit.scoreDelta}`);
  if (hit.confidenceDelta != null) {
    parts.push(`confidenceDelta=${hit.confidenceDelta}`);
  }
  return parts.join(':');
}

module.exports = {
  buildWatchAlertsSection,
};
