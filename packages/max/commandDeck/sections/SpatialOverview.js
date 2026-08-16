'use strict';

/**
 * SPEC-097 Living Command Deck — spatial domain overview composer.
 * Presenter-only: summarizes existing intelligence into domain nodes.
 */

const {
  DOMAIN_IDS,
  DOMAIN_SLOTS,
  DISTANCE_BY_PRIORITY,
  collectDomainSignals,
  buildElevationReason,
  inferHlaDomain,
  buildDomainSummary,
  buildDomainDrawer,
  PRIORITY_STATES,
} = require('../spatial/DomainPriority');

const DOMAIN_ORDER = [
  DOMAIN_IDS.CONTENT,
  DOMAIN_IDS.ACQUISITION,
  DOMAIN_IDS.CLIENTS,
  DOMAIN_IDS.CAMPAIGNS,
];

const DOMAIN_LABELS = Object.freeze({
  acquisition: 'Acquisition',
  content: 'Content',
  clients: 'Clients',
  campaigns: 'Campaigns',
});

/**
 * @param {object} input
 * @param {object} input.model - composed CommandDeckModel (partial)
 * @param {object[]} [input.pendingRecommendations]
 * @param {object[]} [input.activeObjectives]
 * @param {object} [input.operatorBrief]
 * @param {Map<string, object>} [input.storedPriorities]
 * @param {Function} [input.reconcilePriority] - async (domainId, computed, reason, evidence) => reconciliation
 * @param {string|null} [input.lastVisitAt]
 * @returns {Promise<object>}
 */
async function composeSpatialOverview(input = {}) {
  const model = input.model || {};
  const generatedAt =
    (model.meta && model.meta.generatedAt) || new Date().toISOString();

  const missions = (model.operations && model.operations.missions) || [];
  const hlaDomain = inferHlaDomain(model);

  const signalsByDomain = collectDomainSignals({
    missions,
    priorityQueue: model.priorityQueue || [],
    watchAlerts: model.watchAlerts || [],
    pendingRecommendations: input.pendingRecommendations || [],
    activeObjectives: input.activeObjectives || [],
    operatorBrief: input.operatorBrief || model.operatorBrief || null,
    hlaDomain,
  });

  const reconcile = input.reconcilePriority || defaultReconcile;
  const domains = [];

  for (const domainId of DOMAIN_ORDER) {
    const signals = signalsByDomain[domainId];
    const computed = signals.priority;
    const elevation = buildElevationReason(domainId, signals);
    const reconciled = await reconcile({
      domainId,
      computed,
      reason: elevation && elevation.reason,
      evidenceRefs: (elevation && elevation.evidenceRefs) || [],
      stored: input.storedPriorities,
    });

    const priority = reconciled.priority || computed;
    const slot = DOMAIN_SLOTS[domainId];
    const distance = DISTANCE_BY_PRIORITY[priority] ?? DISTANCE_BY_PRIORITY.normal;
    const summary = buildDomainSummary(domainId, signals);
    const drawer = buildDomainDrawer(domainId, signals);
    const hasNewIntelligence =
      Boolean(reconciled.transition) ||
      (elevation && priority !== PRIORITY_STATES.MONITORED && priority !== PRIORITY_STATES.NORMAL);

    domains.push({
      id: domainId,
      label: DOMAIN_LABELS[domainId] || domainId,
      priority,
      previousPriority: reconciled.previousPriority || null,
      position: {
        slot: slot.label,
        x: slot.x,
        y: slot.y,
        distance,
      },
      intelligence: {
        active: hasNewIntelligence,
        reviewed: false,
      },
      summary,
      transition: reconciled.transition || null,
      drawer,
      actions: buildDomainActions(domainId, signals, elevation),
    });
  }

  const maxAnchor = buildMaxAnchor(model, domains);
  const unseenChanges = buildUnseenChanges(domains, input.lastVisitAt);
  const listFallback = domains.map((d) => ({
    id: d.id,
    label: d.label,
    priority: d.priority,
    summary: d.summary.compressed,
    intelligence: d.intelligence.active,
    transition: d.transition,
  }));

  return {
    id: 'spatial_overview',
    version: 1,
    maxAnchor,
    domains,
    unseenChanges,
    listFallback,
    generatedAt,
  };
}

async function defaultReconcile({ computed }) {
  return {
    priority: computed,
    previousPriority: null,
    transition: null,
  };
}

/**
 * @param {object} model
 * @param {object[]} domains
 */
function buildMaxAnchor(model, domains) {
  const elevated = domains.filter(
    (d) => d.priority === PRIORITY_STATES.ELEVATED || d.priority === PRIORITY_STATES.URGENT
  );
  const morning = model.morningBrief || {};
  let headline = morning.headline || null;
  let subline = morning.summary || null;

  if (elevated.length === 1) {
    const d = elevated[0];
    headline = d.summary.lines[0] || d.label;
    subline =
      d.transition && d.transition.reason
        ? d.transition.reason
        : d.summary.compressed;
  } else if (elevated.length > 1) {
    headline = `${elevated.length} domains need attention`;
    subline = elevated.map((d) => d.label).join(', ');
  } else if (!headline) {
    const activeCount = domains.filter(
      (d) => d.priority !== PRIORITY_STATES.MONITORED
    ).length;
    headline =
      activeCount > 0
        ? 'Nothing urgent. Objectives are in motion.'
        : 'All quiet. Max is monitoring.';
    subline = null;
  }

  return {
    headline,
    subline,
    askPlaceholder: elevated.length
      ? 'Ask Max about what changed…'
      : 'Ask Max…',
  };
}

/**
 * @param {object[]} domains
 * @param {string|null} lastVisitAt
 */
function buildUnseenChanges(domains, lastVisitAt) {
  if (!lastVisitAt) return [];
  const since = new Date(lastVisitAt).getTime();
  if (Number.isNaN(since)) return [];

  return domains
    .filter((d) => {
      if (!d.transition || !d.transition.changedAt) return false;
      const t = new Date(d.transition.changedAt).getTime();
      return t > since;
    })
    .map((d) => ({
      domainId: d.id,
      label: d.label,
      direction:
        d.transition.newState === PRIORITY_STATES.ELEVATED ||
        d.transition.newState === PRIORITY_STATES.URGENT
          ? 'up'
          : 'down',
      summary: d.transition.reason || d.summary.compressed,
    }));
}

/**
 * @param {string} domainId
 * @param {object} signals
 * @param {object|null} elevation
 */
function buildDomainActions(domainId, signals, elevation) {
  const actions = [
    {
      id: `explore_${domainId}`,
      type: 'explore_domain',
      label: 'Explore',
    },
    {
      id: `discuss_${domainId}`,
      type: 'discuss_with_max',
      label: 'Discuss with Max',
      payload: { domainId, context: domainId },
    },
  ];

  if (elevation) {
    actions.unshift({
      id: `explain_${domainId}`,
      type: 'explain_elevation',
      label: 'Elevated by Max',
      payload: {
        domainId,
        reason: elevation.reason,
        evidenceRefs: elevation.evidenceRefs,
      },
    });
  }

  if (domainId === DOMAIN_IDS.CONTENT) {
    const rec = (signals.pendingRecommendations || [])[0];
    if (rec) {
      actions.unshift({
        id: `review_rec_${rec.id}`,
        type: 'accept_recommendation',
        label: 'Review recommendation',
        payload: { recommendationId: rec.id },
      });
    }
  }

  return actions;
}

module.exports = {
  composeSpatialOverview,
  DOMAIN_ORDER,
  DOMAIN_LABELS,
};
