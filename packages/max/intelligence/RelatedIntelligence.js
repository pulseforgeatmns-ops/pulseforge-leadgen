'use strict';

const { NODE_TYPES } = require('../../knowledge');
const {
  NAV_TYPES,
  buildNavRef,
  buildRecommendationId,
} = require('./IntelligenceTypes');

/**
 * Build Related Intelligence sections from graph + memory facts only.
 * Never invents nodes; skips refs whose ids are absent.
 */
class RelatedIntelligenceBuilder {
  /**
   * @param {object} deps
   * @param {import('../../knowledge/services/KnowledgeService').KnowledgeService} deps.knowledge
   * @param {import('../memory/MemoryEngine').MemoryEngine} [deps.memory]
   */
  constructor(deps) {
    if (!deps || !deps.knowledge) {
      throw new Error('RelatedIntelligenceBuilder requires knowledge');
    }
    this._knowledge = deps.knowledge;
    this._memory = deps.memory || null;
  }

  /**
   * Related pack for a company node.
   * @param {object} input
   * @param {string} input.tenantId
   * @param {string} input.companyId
   * @param {object} [input.company]
   * @param {object[]} [input.changes] - memory change rows
   * @param {number} [input.limit=8]
   */
  async forCompany(input) {
    const tenantId = String(input.tenantId);
    const companyId = String(input.companyId);
    const limit = input.limit != null ? Number(input.limit) : 8;

    const similarCompanies = [];
    const sharedSignals = [];
    const competingOpportunities = [];
    const recentChanges = [];

    let related = [];
    try {
      related = await this._knowledge.related({
        tenantId,
        nodeId: companyId,
        depth: 2,
      });
    } catch (_err) {
      related = [];
    }

    const seenCompanies = new Set([companyId]);
    for (const row of related) {
      const node = row && row.node;
      if (!node || !node.id) continue;
      if (node.type === NODE_TYPES.COMPANY && !seenCompanies.has(node.id)) {
        seenCompanies.add(node.id);
        const ref = buildNavRef({
          type: NAV_TYPES.COMPANY,
          id: node.id,
          label: node.name || node.id,
        });
        if (ref && similarCompanies.length < limit) similarCompanies.push(ref);
      }
      if (
        (node.type === NODE_TYPES.EVIDENCE || node.type === NODE_TYPES.CLAIM) &&
        sharedSignals.length < limit
      ) {
        const ref = buildNavRef({
          type:
            node.type === NODE_TYPES.CLAIM
              ? NAV_TYPES.CLAIM
              : NAV_TYPES.EVIDENCE,
          id: node.id,
          label: node.summary || node.statement || node.name || node.id,
        });
        if (ref) sharedSignals.push(ref);
      }
      if (
        node.type === NODE_TYPES.INTERACTION &&
        sharedSignals.length < limit
      ) {
        const ref = buildNavRef({
          type: NAV_TYPES.INTERACTION,
          id: node.id,
          label: node.summary || node.name || node.id,
        });
        if (ref) sharedSignals.push(ref);
      }
    }

    if (this._memory) {
      for (const otherId of seenCompanies) {
        if (otherId === companyId) continue;
        if (competingOpportunities.length >= limit) break;
        try {
          const snaps = await this._memory.repository.listByCompany(
            tenantId,
            otherId
          );
          const latest = snaps.length ? snaps[snaps.length - 1] : null;
          const rec = latest && latest.recommendation;
          if (rec && rec.id) {
            const ref = buildNavRef({
              type: NAV_TYPES.RECOMMENDATION,
              id: rec.id,
              label:
                (rec.subject && rec.subject.name) ||
                rec.recommendedAction ||
                rec.id,
            });
            if (ref) competingOpportunities.push(ref);
          } else {
            const ref = buildNavRef({
              type: NAV_TYPES.RECOMMENDATION,
              id: buildRecommendationId(tenantId, otherId),
              label: otherId,
            });
            if (ref) competingOpportunities.push(ref);
          }
        } catch (_err) {
          /* skip */
        }
      }
    }

    for (const change of input.changes || []) {
      if (recentChanges.length >= limit) break;
      const label =
        (change && (change.summary || change.type || change.field)) ||
        'change';
      recentChanges.push({
        type: change.type || 'change',
        field: change.field || null,
        summary: String(label),
        at: change.at || change.timestamp || null,
      });
    }

    return {
      similarCompanies,
      sharedSignals,
      competingOpportunities,
      recentChanges,
      supportingEvidence: [],
      contradictingEvidence: [],
      alternativeRecommendations: [],
      otherRecommendations: [],
      sourceInteractions: [],
    };
  }

  /**
   * Related pack for a recommendation (company-scoped).
   * @param {object} input
   */
  async forRecommendation(input) {
    const tenantId = String(input.tenantId);
    const companyId = String(input.companyId);
    const recommendation = input.recommendation || null;
    const limit = input.limit != null ? Number(input.limit) : 8;

    const supportingEvidence = signalRefs(
      recommendation && recommendation.supportingSignals,
      NAV_TYPES.EVIDENCE,
      limit
    );
    const contradictingEvidence = signalRefs(
      recommendation && recommendation.opposingSignals,
      NAV_TYPES.EVIDENCE,
      limit
    );

    const alternativeRecommendations = [];
    if (this._memory) {
      try {
        const companies = await this._knowledge.findCompanies({
          tenantId,
          limit: 40,
        });
        for (const co of companies) {
          if (!co || co.id === companyId) continue;
          if (alternativeRecommendations.length >= limit) break;
          const snaps = await this._memory.repository.listByCompany(
            tenantId,
            co.id
          );
          const latest = snaps.length ? snaps[snaps.length - 1] : null;
          const rec = latest && latest.recommendation;
          if (!rec || !rec.id) continue;
          const ref = buildNavRef({
            type: NAV_TYPES.RECOMMENDATION,
            id: rec.id,
            label: (rec.subject && rec.subject.name) || co.name || rec.id,
          });
          if (ref) alternativeRecommendations.push(ref);
        }
      } catch (_err) {
        /* fail closed */
      }
    }

    const companyPack = await this.forCompany({
      tenantId,
      companyId,
      changes: input.changes || [],
      limit,
    });

    return {
      ...companyPack,
      supportingEvidence,
      contradictingEvidence,
      alternativeRecommendations,
    };
  }

  /**
   * Related pack for an evidence node.
   * @param {object} input
   */
  async forEvidence(input) {
    const tenantId = String(input.tenantId);
    const evidenceId = String(input.evidenceId);
    const limit = input.limit != null ? Number(input.limit) : 8;

    const otherRecommendations = [];
    const sourceInteractions = [];

    let related = [];
    try {
      related = await this._knowledge.related({
        tenantId,
        nodeId: evidenceId,
        depth: 2,
      });
    } catch (_err) {
      related = [];
    }

    for (const row of related) {
      const node = row && row.node;
      if (!node || !node.id) continue;
      if (node.type === NODE_TYPES.INTERACTION && sourceInteractions.length < limit) {
        const ref = buildNavRef({
          type: NAV_TYPES.INTERACTION,
          id: node.id,
          label: node.summary || node.name || node.id,
        });
        if (ref) sourceInteractions.push(ref);
      }
      if (node.type === NODE_TYPES.COMPANY && this._memory) {
        try {
          const snaps = await this._memory.repository.listByCompany(
            tenantId,
            node.id
          );
          const latest = snaps.length ? snaps[snaps.length - 1] : null;
          const rec = latest && latest.recommendation;
          if (
            rec &&
            Array.isArray(rec.evidence) &&
            rec.evidence.includes(evidenceId) &&
            otherRecommendations.length < limit
          ) {
            const ref = buildNavRef({
              type: NAV_TYPES.RECOMMENDATION,
              id: rec.id,
              label: (rec.subject && rec.subject.name) || rec.id,
            });
            if (ref) otherRecommendations.push(ref);
          }
        } catch (_err) {
          /* skip */
        }
      }
    }

    return {
      similarCompanies: [],
      sharedSignals: [],
      competingOpportunities: [],
      recentChanges: [],
      supportingEvidence: [],
      contradictingEvidence: [],
      alternativeRecommendations: [],
      otherRecommendations,
      sourceInteractions,
    };
  }
}

function signalRefs(signals, defaultType, limit) {
  const out = [];
  for (const s of signals || []) {
    if (out.length >= limit) break;
    if (!s || s.id == null) continue;
    const ref = buildNavRef({
      type: s.kind === 'claim' ? NAV_TYPES.CLAIM : defaultType,
      id: s.id,
      label: s.summary || s.id,
    });
    if (ref) out.push(ref);
  }
  return out;
}

module.exports = {
  RelatedIntelligenceBuilder,
};
