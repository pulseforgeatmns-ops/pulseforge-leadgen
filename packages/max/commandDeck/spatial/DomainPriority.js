'use strict';

/**
 * SPEC-097 Living Command Deck — discrete domain priority bands.
 * Deterministic summarization from existing intelligence signals only.
 * No LLM scoring, no continuous numeric ranking.
 */

const PRIORITY_STATES = Object.freeze({
  MONITORED: 'monitored',
  NORMAL: 'normal',
  ELEVATED: 'elevated',
  URGENT: 'urgent',
});

const DOMAIN_IDS = Object.freeze({
  ACQUISITION: 'acquisition',
  CONTENT: 'content',
  CLIENTS: 'clients',
  CAMPAIGNS: 'campaigns',
});

/** Stable spatial slots — position only changes via distance, not slot swap. */
const DOMAIN_SLOTS = Object.freeze({
  acquisition: { x: -1, y: 0, label: 'left' },
  content: { x: 0, y: -1, label: 'top' },
  clients: { x: 1, y: 0, label: 'right' },
  campaigns: { x: 0, y: 1, label: 'bottom' },
});

/** Proximity factor: lower = closer to Max anchor. */
const DISTANCE_BY_PRIORITY = Object.freeze({
  urgent: 0.32,
  elevated: 0.48,
  normal: 0.68,
  monitored: 0.88,
});

const PRIORITY_RANK = Object.freeze({
  monitored: 0,
  normal: 1,
  elevated: 2,
  urgent: 3,
});

/**
 * @param {string[]} states
 * @returns {string}
 */
function highestPriority(states) {
  let best = PRIORITY_STATES.MONITORED;
  let rank = -1;
  for (const s of states) {
    const r = PRIORITY_RANK[s] ?? 1;
    if (r > rank) {
      rank = r;
      best = s;
    }
  }
  return best;
}

/**
 * @param {object} input
 * @returns {object} signals per domain
 */
function collectDomainSignals(input = {}) {
  const missions = Array.isArray(input.missions) ? input.missions : [];
  const priorityQueue = Array.isArray(input.priorityQueue)
    ? input.priorityQueue
    : [];
  const watchAlerts = Array.isArray(input.watchAlerts) ? input.watchAlerts : [];
  const pendingRecommendations = Array.isArray(input.pendingRecommendations)
    ? input.pendingRecommendations
    : [];
  const activeObjectives = Array.isArray(input.activeObjectives)
    ? input.activeObjectives
    : [];
  const operatorBrief = input.operatorBrief || null;
  const hlaDomain = input.hlaDomain || null;
  const acquisitionIntelligence = input.acquisitionIntelligence || null;

  const missionSummary = summarizeMissions(missions);

  const acquisitionCandidates = [];
  if (priorityQueue.length) acquisitionCandidates.push(PRIORITY_STATES.ELEVATED);
  if (watchAlerts.length) acquisitionCandidates.push(PRIORITY_STATES.ELEVATED);
  if (operatorBrief) acquisitionCandidates.push(PRIORITY_STATES.ELEVATED);
  if (hlaDomain === DOMAIN_IDS.ACQUISITION) {
    acquisitionCandidates.push(PRIORITY_STATES.URGENT);
  }
  if (
    acquisitionIntelligence &&
    acquisitionIntelligence.priorityImpact &&
    PRIORITY_RANK[acquisitionIntelligence.priorityImpact.to] != null
  ) {
    acquisitionCandidates.push(acquisitionIntelligence.priorityImpact.to);
  }

  const contentPending = pendingRecommendations.filter(
    (r) => r && (r.status === 'pending' || r.status === 'refined')
  );
  const contentCandidates = [];
  if (contentPending.length) contentCandidates.push(PRIORITY_STATES.ELEVATED);
  if (contentPending.some((r) => r.requiresOperatorDecision)) {
    contentCandidates.push(PRIORITY_STATES.URGENT);
  }
  if (hlaDomain === DOMAIN_IDS.CONTENT) {
    contentCandidates.push(PRIORITY_STATES.URGENT);
  }

  const clientObjectives = activeObjectives.filter(
    (o) => o && (o.scope === 'client' || o.clientId != null)
  );
  const clientsCandidates = [];
  if (clientObjectives.length) clientsCandidates.push(PRIORITY_STATES.ELEVATED);
  if (hlaDomain === DOMAIN_IDS.CLIENTS) {
    clientsCandidates.push(PRIORITY_STATES.URGENT);
  }

  const campaignsCandidates = [];
  if (missionSummary.needsAttention > 0) {
    campaignsCandidates.push(PRIORITY_STATES.ELEVATED);
  }
  if (missionSummary.reviewRequired > 0) {
    campaignsCandidates.push(PRIORITY_STATES.URGENT);
  }
  if (hlaDomain === DOMAIN_IDS.CAMPAIGNS) {
    campaignsCandidates.push(PRIORITY_STATES.URGENT);
  }
  if (
    missionSummary.historicalContained > 0 &&
    missionSummary.active === 0 &&
    missionSummary.needsAttention === 0
  ) {
    campaignsCandidates.push(PRIORITY_STATES.MONITORED);
  }

  return {
    acquisition: {
      priority: acquisitionCandidates.length
        ? highestPriority(acquisitionCandidates)
        : PRIORITY_STATES.NORMAL,
      aoIntelligence: Boolean(operatorBrief),
      priorityCount: priorityQueue.length,
      watchCount: watchAlerts.length,
      operatorBrief,
      priorityItems: priorityQueue.slice(0, 8),
      watchAlerts: watchAlerts.slice(0, 6),
      acquisitionIntelligence,
    },
    content: {
      priority: contentCandidates.length
        ? highestPriority(contentCandidates)
        : PRIORITY_STATES.NORMAL,
      pendingRecommendations: contentPending,
      pendingCount: contentPending.length,
    },
    clients: {
      priority: clientsCandidates.length
        ? highestPriority(clientsCandidates)
        : PRIORITY_STATES.NORMAL,
      objectives: clientObjectives.slice(0, 8),
      needsAttention: clientObjectives.filter((o) => o.status === 'active').length,
    },
    campaigns: {
      priority: campaignsCandidates.length
        ? highestPriority(campaignsCandidates)
        : PRIORITY_STATES.NORMAL,
      summary: missionSummary,
      missions: missions.slice(0, 20),
    },
  };
}

/**
 * @param {object[]} missions
 */
function summarizeMissions(missions) {
  const active = missions.filter((m) =>
    ['requested', 'planning', 'executing'].includes(String(m.status || ''))
  ).length;
  const needsAttention = missions.filter((m) =>
    ['review_required', 'waiting'].includes(String(m.status || ''))
  ).length;
  const reviewRequired = missions.filter(
    (m) => String(m.status || '') === 'review_required'
  ).length;
  const historical = missions.filter((m) =>
    ['completed', 'reviewed', 'archived', 'failed'].includes(String(m.status || ''))
  ).length;
  const blocked = missions.filter((m) => String(m.status || '') === 'waiting').length;
  const historicalContained = historical + blocked;

  let containedLabel = null;
  if (historicalContained > 0 && needsAttention === 0) {
    containedLabel = `${historicalContained} contained · none urgent`;
  }

  return {
    active,
    needsAttention,
    reviewRequired,
    awaitingOperator: reviewRequired,
    historical,
    blocked,
    historicalContained,
    containedLabel,
    total: missions.length,
  };
}

/**
 * Build human-readable elevation reason from signals.
 * @param {string} domainId
 * @param {object} signals
 * @returns {{ reason: string, evidenceRefs: object[] }|null}
 */
function buildElevationReason(domainId, signals) {
  if (!signals) return null;

  if (domainId === DOMAIN_IDS.CONTENT) {
    const pending = signals.pendingRecommendations || [];
    const rec = pending[0];
    if (rec) {
      return {
        reason:
          rec.summary ||
          rec.title ||
          'A new content recommendation requires operator judgment.',
        evidenceRefs: [
          {
            kind: 'content_recommendation',
            id: rec.id,
            label: rec.title || 'Paige recommendation',
          },
        ],
      };
    }
  }

  if (domainId === DOMAIN_IDS.ACQUISITION) {
    if (signals.acquisitionIntelligence && signals.acquisitionIntelligence.summary) {
      return {
        reason: signals.acquisitionIntelligence.summary,
        evidenceRefs: [
          {
            kind: 'specialist_result',
            id: signals.acquisitionIntelligence.resultId || 'scout-ao',
            label: 'Scout acquisition intelligence',
          },
        ],
      };
    }
    if (signals.operatorBrief) {
      return {
        reason:
          (signals.operatorBrief.highestLeverage &&
            signals.operatorBrief.highestLeverage.title) ||
          'New acquisition intelligence is available.',
        evidenceRefs: [{ kind: 'operator_brief', id: 'ao', label: 'AO Intelligence' }],
      };
    }
    if (signals.watchCount > 0) {
      return {
        reason: `${signals.watchCount} market signal${signals.watchCount === 1 ? '' : 's'} need awareness.`,
        evidenceRefs: (signals.watchAlerts || []).slice(0, 3).map((w) => ({
          kind: 'watch_alert',
          id: w.id || w.companyId,
          label: w.headline || w.title || 'Watch alert',
        })),
      };
    }
  }

  if (domainId === DOMAIN_IDS.CLIENTS) {
    const obj = (signals.objectives || [])[0];
    if (obj) {
      return {
        reason: obj.title || obj.summary || 'A client objective needs attention.',
        evidenceRefs: [
          {
            kind: 'operator_objective',
            id: obj.id,
            label: obj.title || 'Client objective',
          },
        ],
      };
    }
  }

  if (domainId === DOMAIN_IDS.CAMPAIGNS) {
    const s = signals.summary || {};
    if (s.reviewRequired > 0) {
      return {
        reason: `${s.reviewRequired} campaign${s.reviewRequired === 1 ? '' : 's'} awaiting operator decision.`,
        evidenceRefs: (signals.missions || [])
          .filter((m) => m.status === 'review_required')
          .slice(0, 3)
          .map((m) => ({
            kind: 'mission',
            id: m.id,
            label: m.title || 'Mission',
          })),
      };
    }
  }

  return null;
}

/**
 * Infer which domain the HLA belongs to.
 * @param {object} model
 * @returns {string|null}
 */
function inferHlaDomain(model) {
  const hla = model && model.highestLeverageAction;
  if (!hla) return null;
  const rec = hla.recommendation || {};
  const type = String(rec.type || rec.recommendedAction || '').toLowerCase();
  if (/content|paige|linkedin|post|publish/.test(type)) return DOMAIN_IDS.CONTENT;
  if (/client|onboard|pilot|readiness/.test(type)) return DOMAIN_IDS.CLIENTS;
  if (/campaign|mission|mail|outreach/.test(type)) return DOMAIN_IDS.CAMPAIGNS;
  if (/prospect|acquisition|scout|pipeline|market/.test(type)) {
    return DOMAIN_IDS.ACQUISITION;
  }
  return null;
}

/**
 * @param {string} domainId
 * @param {object} signals
 * @returns {object}
 */
function buildDomainSummary(domainId, signals) {
  if (domainId === DOMAIN_IDS.ACQUISITION) {
    const intel = signals.acquisitionIntelligence;
    if (intel && intel.summary) {
      const lines = String(intel.summary)
        .split(/[.]+\s*/)
        .map((s) => s.trim())
        .filter(Boolean);
      return {
        lines: lines.length ? lines : [intel.summary],
        compressed: intel.summary,
      };
    }
    const lines = [];
    if (signals.aoIntelligence) {
      lines.push('AO Intelligence');
      if (signals.priorityCount) {
        lines.push(`${signals.priorityCount} opportunit${signals.priorityCount === 1 ? 'y' : 'ies'} developing`);
      }
      if (signals.watchCount) {
        lines.push(`${signals.watchCount} new market signal${signals.watchCount === 1 ? '' : 's'}`);
      }
    } else if (signals.priorityCount || signals.watchCount) {
      if (signals.priorityCount) {
        lines.push(`${signals.priorityCount} priorit${signals.priorityCount === 1 ? 'y' : 'ies'} in queue`);
      }
      if (signals.watchCount) {
        lines.push(`${signals.watchCount} watch alert${signals.watchCount === 1 ? '' : 's'}`);
      }
    } else {
      lines.push('Pipeline steady');
    }
    return {
      lines,
      compressed: lines.slice(0, 2).join(' · ') || 'Acquisition steady',
    };
  }

  if (domainId === DOMAIN_IDS.CONTENT) {
    const pending = signals.pendingCount || 0;
    const lines = [];
    if (pending > 0) {
      lines.push('Paige has a recommendation');
      const rec = (signals.pendingRecommendations || [])[0];
      if (rec && rec.channel) lines.push(`${rec.channel} experiment active`);
    } else {
      lines.push('Content channels monitored');
    }
    return {
      lines,
      compressed:
        pending > 0
          ? `${pending} recommendation${pending === 1 ? '' : 's'} ready`
          : 'Content steady',
    };
  }

  if (domainId === DOMAIN_IDS.CLIENTS) {
    const n = signals.needsAttention || 0;
    const lines = [];
    if (n > 0) {
      lines.push(`${n} need${n === 1 ? 's' : ''} attention`);
      const obj = (signals.objectives || [])[0];
      if (obj && obj.title) lines.push(obj.title);
    } else {
      lines.push('Clients monitored');
    }
    return {
      lines,
      compressed: n > 0 ? `${n} need attention` : 'Clients steady',
    };
  }

  if (domainId === DOMAIN_IDS.CAMPAIGNS) {
    const s = signals.summary || {};
    const lines = [];
    if (s.active) lines.push(`${s.active} active`);
    if (s.awaitingOperator) lines.push(`${s.awaitingOperator} awaiting you`);
    if (s.containedLabel) {
      lines.push(s.containedLabel);
    } else if (s.historicalContained) {
      lines.push(`${s.historicalContained} historical/contained`);
    }
    return {
      lines,
      compressed: [
        s.active ? `${s.active} active` : null,
        s.awaitingOperator ? `${s.awaitingOperator} awaiting you` : null,
        s.containedLabel ||
          (s.historicalContained
            ? `${s.historicalContained} historical/contained`
            : null),
      ]
        .filter(Boolean)
        .join(' · ') || 'No active campaigns',
    };
  }

  return { lines: [], compressed: '' };
}

/**
 * Build drawer items for progressive disclosure.
 * @param {string} domainId
 * @param {object} signals
 */
function buildDomainDrawer(domainId, signals) {
  if (domainId === DOMAIN_IDS.CAMPAIGNS) {
    const missions = signals.missions || [];
    const groups = {
      needsAttention: [],
      active: [],
      recent: [],
      archived: [],
    };
    for (const m of missions) {
      const status = String(m.status || '');
      if (status === 'review_required' || status === 'waiting') {
        groups.needsAttention.push(m);
      } else if (['requested', 'planning', 'executing'].includes(status)) {
        groups.active.push(m);
      } else if (['completed', 'reviewed'].includes(status)) {
        groups.recent.push(m);
      } else {
        groups.archived.push(m);
      }
    }
    return { groups };
  }

  if (domainId === DOMAIN_IDS.CONTENT) {
    return {
      recommendations: (signals.pendingRecommendations || []).map((r) => ({
        id: r.id,
        title: r.title || r.summary || 'Content recommendation',
        status: r.status,
        channel: r.channel || null,
        requiresOperatorDecision: Boolean(r.requiresOperatorDecision),
      })),
    };
  }

  if (domainId === DOMAIN_IDS.ACQUISITION) {
    return {
      aoIntelligence: signals.aoIntelligence || Boolean(signals.acquisitionIntelligence),
      priorityItems: (signals.priorityItems || []).map((p) => ({
        id: p.recommendationId || p.id,
        title: p.companyName || p.title || 'Priority',
        movement: p.movement || null,
      })),
      watchAlerts: (signals.watchAlerts || []).map((w) => ({
        id: w.id,
        title: w.headline || w.title || 'Alert',
      })),
    };
  }

  if (domainId === DOMAIN_IDS.CLIENTS) {
    return {
      objectives: (signals.objectives || []).map((o) => ({
        id: o.id,
        title: o.title,
        status: o.status,
        summary: o.summary || null,
      })),
    };
  }

  return {};
}

module.exports = {
  PRIORITY_STATES,
  DOMAIN_IDS,
  DOMAIN_SLOTS,
  DISTANCE_BY_PRIORITY,
  collectDomainSignals,
  summarizeMissions,
  buildElevationReason,
  inferHlaDomain,
  buildDomainSummary,
  buildDomainDrawer,
  highestPriority,
};
