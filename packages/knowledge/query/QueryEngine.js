'use strict';

const { NODE_TYPES } = require('../types/nodeTypes');
const { EDGE_TYPES } = require('../edges/edgeTypes');
const {
  requireTenantId,
  resolveLimit,
} = require('./QueryTypes');
const {
  matchesCompany,
  matchesPerson,
  matchesInteraction,
  matchesEvidence,
  matchesClaim,
  sortById,
  includesCI,
  meta,
  matchesCreatedAfter,
  matchesConfidenceMin,
} = require('./Filters');
const {
  neighbors: traverseNeighbors,
  related: traverseRelated,
  findPath,
} = require('./Traversal');
const { buildTimeline, timelinePosition } = require('./Timeline');
const {
  detectRepositoryType,
  MetricsCollector,
  MetricsSink,
} = require('./Metrics');

/**
 * Knowledge Query Engine — structured interrogation of the graph (SPEC-001C).
 * No LLM. No formatting. Domain objects only.
 */
class QueryEngine {
  /**
   * @param {object} deps
   * @param {import('../repositories/GraphRepository').GraphRepository} deps.repository
   * @param {(m: import('./QueryTypes').QueryMetrics) => void} [deps.onMetrics]
   */
  constructor(deps) {
    if (!deps || !deps.repository) {
      throw new Error('QueryEngine requires a repository');
    }
    this._repository = deps.repository;
    this._repositoryType = detectRepositoryType(deps.repository);
    this._metrics = new MetricsSink({ onEmit: deps.onMetrics });
  }

  /** @returns {import('./QueryTypes').QueryMetrics|null} */
  getLastMetrics() {
    return this._metrics.last;
  }

  /** @returns {import('./QueryTypes').QueryMetrics[]} */
  getMetricsHistory() {
    return this._metrics.history();
  }

  clearMetrics() {
    this._metrics.clear();
  }

  /**
   * @param {import('./QueryTypes').CompanyQuery} query
   * @returns {Promise<object[]>}
   */
  async findCompanies(query) {
    return this._runFiltered('findCompanies', query, NODE_TYPES.COMPANY, matchesCompany);
  }

  /**
   * @param {import('./QueryTypes').PersonQuery} query
   * @returns {Promise<object[]>}
   */
  async findPeople(query) {
    const tenantId = requireTenantId(query, 'PersonQuery');
    return this._timed('findPeople', async (metrics) => {
      let nodes = await this._repository.find(tenantId, { type: NODE_TYPES.PERSON });
      metrics.visitNodes(nodes.length);

      nodes = nodes.filter((n) => matchesPerson(n, query));

      if (query.companyId) {
        const filtered = [];
        for (const person of nodes) {
          const links = await this._repository.neighbors(tenantId, person.id, {
            direction: 'out',
            edgeType: EDGE_TYPES.WORKS_FOR,
          });
          metrics.traverseEdges(links.length);
          if (links.some((l) => l.node.id === query.companyId)) {
            filtered.push(person);
          }
        }
        nodes = filtered;
      }

      const limit = resolveLimit(query.limit);
      const results = sortById(nodes).slice(0, limit);
      metrics.setResults(results.length);
      return results;
    });
  }

  /**
   * @param {import('./QueryTypes').InteractionQuery} query
   * @returns {Promise<object[]>}
   */
  async findInteractions(query) {
    const tenantId = requireTenantId(query, 'InteractionQuery');
    return this._timed('findInteractions', async (metrics) => {
      let nodes = await this._repository.find(tenantId, { type: NODE_TYPES.INTERACTION });
      metrics.visitNodes(nodes.length);
      nodes = nodes.filter((n) => matchesInteraction(n, query));

      if (query.relatedNodeId) {
        const links = await this._repository.neighbors(tenantId, query.relatedNodeId, {
          direction: 'both',
        });
        metrics.traverseEdges(links.length);
        const relatedIds = new Set(
          links
            .filter((l) => l.node.type === NODE_TYPES.INTERACTION)
            .map((l) => l.node.id)
        );
        nodes = nodes.filter((n) => relatedIds.has(n.id));
      }

      const limit = resolveLimit(query.limit);
      const results = sortById(nodes).slice(0, limit);
      metrics.setResults(results.length);
      return results;
    });
  }

  /**
   * @param {import('./QueryTypes').EvidenceQuery} query
   * @returns {Promise<object[]>}
   */
  async findEvidence(query) {
    const tenantId = requireTenantId(query, 'EvidenceQuery');
    return this._timed('findEvidence', async (metrics) => {
      let nodes = await this._repository.find(tenantId, { type: NODE_TYPES.EVIDENCE });
      metrics.visitNodes(nodes.length);
      nodes = nodes.filter((n) => matchesEvidence(n, query));

      if (query.aboutNodeId) {
        const links = await this._repository.neighbors(tenantId, query.aboutNodeId, {
          direction: 'in',
          edgeType: EDGE_TYPES.ABOUT,
        });
        metrics.traverseEdges(links.length);
        const ids = new Set(
          links.filter((l) => l.node.type === NODE_TYPES.EVIDENCE).map((l) => l.node.id)
        );
        nodes = nodes.filter((n) => ids.has(n.id));
      }

      const limit = resolveLimit(query.limit);
      const results = sortById(nodes).slice(0, limit);
      metrics.setResults(results.length);
      return results;
    });
  }

  /**
   * @param {import('./QueryTypes').ClaimQuery} query
   * @returns {Promise<object[]>}
   */
  async findClaims(query) {
    const tenantId = requireTenantId(query, 'ClaimQuery');
    return this._timed('findClaims', async (metrics) => {
      let nodes = await this._repository.find(tenantId, { type: NODE_TYPES.CLAIM });
      metrics.visitNodes(nodes.length);
      nodes = nodes.filter((n) => matchesClaim(n, query));

      if (query.subjectId) {
        const links = await this._repository.neighbors(tenantId, query.subjectId, {
          direction: 'in',
          edgeType: EDGE_TYPES.ABOUT,
        });
        metrics.traverseEdges(links.length);
        const ids = new Set(
          links.filter((l) => l.node.type === NODE_TYPES.CLAIM).map((l) => l.node.id)
        );
        nodes = nodes.filter((n) => ids.has(n.id));
      }

      const limit = resolveLimit(query.limit);
      const results = sortById(nodes).slice(0, limit);
      metrics.setResults(results.length);
      return results;
    });
  }

  /**
   * @param {import('./QueryTypes').NeighborQuery} query
   * @returns {Promise<import('./QueryTypes').NeighborResult[]>}
   */
  async neighbors(query) {
    requireTenantId(query, 'NeighborQuery');
    if (!query.nodeId) throw new Error('NeighborQuery requires nodeId');
    return this._timed('neighbors', async (metrics) => {
      const results = await traverseNeighbors(this._repository, query, metrics);
      metrics.setResults(results.length);
      return results;
    });
  }

  /**
   * @param {import('./QueryTypes').RelatedQuery} query
   * @returns {Promise<import('./QueryTypes').RelatedNode[]>}
   */
  async related(query) {
    requireTenantId(query, 'RelatedQuery');
    if (!query.nodeId) throw new Error('RelatedQuery requires nodeId');
    return this._timed('related', async (metrics) =>
      traverseRelated(this._repository, query, metrics)
    );
  }

  /**
   * @param {import('./QueryTypes').TimelineQuery} query
   * @returns {Promise<import('./QueryTypes').TimelineEvent[]>}
   */
  async timeline(query) {
    requireTenantId(query, 'TimelineQuery');
    if (!query.nodeId) throw new Error('TimelineQuery requires nodeId');
    return this._timed('timeline', async (metrics) =>
      buildTimeline(this._repository, query, metrics)
    );
  }

  /**
   * @param {import('./QueryTypes').PathQuery} query
   * @returns {Promise<import('./QueryTypes').PathResult|null>}
   */
  async path(query) {
    requireTenantId(query, 'PathQuery');
    if (!query.fromId || !query.toId) {
      throw new Error('PathQuery requires fromId and toId');
    }
    return this._timed('path', async (metrics) => findPath(this._repository, query, metrics));
  }

  /**
   * Enhanced explainability:
   * Claim → Supporting Evidence → Original Source → Confidence → Timeline Position → Reason
   *
   * @param {import('./QueryTypes').ExplainQuery} query
   * @returns {Promise<object|null>}
   */
  async explain(query) {
    requireTenantId(query, 'ExplainQuery');
    if (!query.nodeId) throw new Error('ExplainQuery requires nodeId');

    return this._timed('explain', async (metrics) => {
      const tenantId = String(query.tenantId);
      const nodes = await this._repository.find(tenantId, { id: query.nodeId });
      const node = nodes[0] || null;
      if (!node) {
        metrics.setResults(0);
        return null;
      }
      metrics.visitNodes(1);

      if (node.type === NODE_TYPES.CLAIM) {
        const result = await this._explainClaim(tenantId, node, metrics);
        metrics.setResults(1);
        return result;
      }
      if (node.type === NODE_TYPES.EVIDENCE) {
        const result = await this._explainEvidence(tenantId, node, metrics);
        metrics.setResults(1);
        return result;
      }

      const claimLinks = await this._repository.neighbors(tenantId, query.nodeId, {
        direction: 'in',
        edgeType: EDGE_TYPES.ABOUT,
      });
      metrics.traverseEdges(claimLinks.length);

      const claims = claimLinks
        .map((l) => l.node)
        .filter((n) => n.type === NODE_TYPES.CLAIM && n.status === 'active');

      if (claims.length > 0) {
        const claimExplanations = [];
        for (const claim of claims) {
          claimExplanations.push(await this._explainClaim(tenantId, claim, metrics));
        }
        metrics.setResults(claimExplanations.length);
        return {
          subject: summarizeNode(node),
          claims: claimExplanations,
          confidence: maxConfidence(claimExplanations.map((c) => c.confidence)),
          reason: `Subject has ${claims.length} active claim(s)`,
        };
      }

      const evidence = claimLinks
        .map((l) => l.node)
        .filter((n) => n.type === NODE_TYPES.EVIDENCE);

      const evidenceExplained = [];
      for (const e of evidence) {
        evidenceExplained.push(await this._explainEvidence(tenantId, e, metrics));
      }
      metrics.setResults(evidenceExplained.length);
      return {
        subject: summarizeNode(node),
        claims: [],
        evidence: evidenceExplained,
        confidence: maxConfidence(evidence.map((e) => e.confidence)),
        reason:
          evidence.length > 0
            ? `Subject has ${evidence.length} attached evidence node(s) and no claims`
            : 'No claims or evidence attached',
      };
    });
  }

  /**
   * @param {string} tenantId
   * @param {object} claim
   * @param {MetricsCollector} metrics
   */
  async _explainClaim(tenantId, claim, metrics) {
    const supporting = await this._repository.neighbors(tenantId, claim.id, {
      direction: 'in',
      edgeType: EDGE_TYPES.SUPPORTS,
    });
    metrics.traverseEdges(supporting.length);

    const evidenceNodes = supporting
      .map((l) => l.node)
      .filter((n) => n.type === NODE_TYPES.EVIDENCE);

    const evidence = [];
    for (const e of evidenceNodes) {
      evidence.push(await this._explainEvidence(tenantId, e, metrics));
    }

    const timeline = await buildTimeline(
      this._repository,
      { tenantId, nodeId: claim.id },
      metrics
    );
    const position = timelinePosition(timeline, claim.id);

    return {
      claim: {
        id: claim.id,
        statement: claim.statement,
        status: claim.status,
      },
      evidence,
      supportingEvidence: evidence,
      originalSources: evidence.map((e) => e.originalSource),
      confidence: claim.confidence,
      timelinePosition: position,
      reason: claim.reason,
    };
  }

  /**
   * @param {string} tenantId
   * @param {object} evidence
   * @param {MetricsCollector} metrics
   */
  async _explainEvidence(tenantId, evidence, metrics) {
    const timeline = await buildTimeline(
      this._repository,
      { tenantId, nodeId: evidence.id },
      metrics
    );
    const position = timelinePosition(timeline, evidence.id);
    return {
      evidence: {
        id: evidence.id,
        summary: evidence.summary,
        confidence: evidence.confidence,
      },
      originalSource: {
        sourceType: evidence.sourceType,
        sourceId: evidence.sourceId,
      },
      confidence: evidence.confidence,
      timelinePosition: position,
      reason: evidence.summary || `Evidence from ${evidence.sourceType}`,
    };
  }

  /**
   * @param {string} name
   * @param {object} query
   * @param {string} type
   * @param {(node: object, query: object) => boolean} matcher
   */
  async _runFiltered(name, query, type, matcher) {
    const tenantId = requireTenantId(query, name);
    return this._timed(name, async (metrics) => {
      let nodes = await this._repository.find(tenantId, { type });
      metrics.visitNodes(nodes.length);
      nodes = nodes.filter((n) => matcher(n, query));

      // Technology via USES edges (in addition to metadata match already applied)
      if (name === 'findCompanies' && query.technology) {
        const withEdge = [];
        for (const company of nodes) {
          withEdge.push(company);
        }
        // Also include companies that match via USES but failed metadata-only — re-scan all
        const all = await this._repository.find(tenantId, { type: NODE_TYPES.COMPANY });
        const needle = String(query.technology).toLowerCase();
        for (const company of all) {
          if (nodes.some((n) => n.id === company.id)) continue;
          if (!matchesCompanyBaseWithoutTech(company, query)) continue;
          const uses = await this._repository.neighbors(tenantId, company.id, {
            direction: 'out',
            edgeType: EDGE_TYPES.USES,
          });
          metrics.traverseEdges(uses.length);
          if (uses.some((l) => String(l.node.name || '').toLowerCase().includes(needle))) {
            withEdge.push(company);
          }
        }
        nodes = withEdge;
      }

      const limit = resolveLimit(query.limit);
      const results = sortById(nodes).slice(0, limit);
      metrics.setResults(results.length);
      return results;
    });
  }

  /**
   * @template T
   * @param {string} queryName
   * @param {(metrics: MetricsCollector) => Promise<T>} fn
   * @returns {Promise<T>}
   */
  async _timed(queryName, fn) {
    const metrics = new MetricsCollector(queryName, this._repositoryType);
    try {
      return await fn(metrics);
    } finally {
      this._metrics.emit(metrics.finish());
    }
  }
}

function matchesCompanyBaseWithoutTech(node, query) {
  if (
    !includesCI(node.industry != null ? node.industry : meta(node, 'industry'), query.industry)
  ) {
    return false;
  }
  if (
    !includesCI(node.location != null ? node.location : meta(node, 'location'), query.location)
  ) {
    return false;
  }
  if (!matchesCreatedAfter(node, query.createdAfter)) return false;
  if (query.confidenceMin != null) {
    const conf = node.confidence != null ? node.confidence : meta(node, 'confidence');
    if (!matchesConfidenceMin({ confidence: conf }, query.confidenceMin)) return false;
  }
  return true;
}

function summarizeNode(node) {
  return {
    id: node.id,
    type: node.type,
    name: node.name || null,
    statement: node.statement || null,
  };
}

function maxConfidence(values) {
  if (!values || values.length === 0) return 0;
  return Math.max(...values.map((v) => Number(v) || 0));
}

module.exports = {
  QueryEngine,
};
