'use strict';

/**
 * GraphSyncEngine — translates production sync events into KnowledgeService
 * operations via the knowledge event bus. Never touches GraphRepository.
 *
 * Guarantees:
 * - tenant-aware (every event requires tenantId)
 * - idempotent (ledger + stable node ids + ensure* ingest)
 * - replayable (same envelopes re-apply safely)
 */
class GraphSyncEngine {
  /**
   * @param {object} deps
   * @param {import('../services/KnowledgeService').KnowledgeService} deps.knowledge
   * @param {import('../events/KnowledgeEventBus').KnowledgeEventBus} deps.bus
   * @param {{ has: Function, get: Function, mark: Function }} deps.ledger
   */
  constructor(deps) {
    if (!deps.knowledge || typeof deps.knowledge.ensureNode !== 'function') {
      throw new Error('GraphSyncEngine requires KnowledgeService');
    }
    if (!deps.bus || typeof deps.bus.publish !== 'function') {
      throw new Error('GraphSyncEngine requires KnowledgeEventBus');
    }
    if (!deps.ledger || typeof deps.ledger.has !== 'function') {
      throw new Error('GraphSyncEngine requires a sync ledger');
    }
    this.knowledge = deps.knowledge;
    this.bus = deps.bus;
    this.ledger = deps.ledger;
  }

  /**
   * Apply one sync envelope. Returns skipped if already in ledger.
   *
   * @param {object} syncEvent
   * @param {{ force?: boolean }} [options]
   */
  async apply(syncEvent, options = {}) {
    assertSyncEvent(syncEvent);
    const tenantId = String(syncEvent.tenantId);
    const key = String(syncEvent.id);

    if (!options.force && (await this.ledger.has(tenantId, key))) {
      const prior = await this.ledger.get(tenantId, key);
      return {
        status: 'skipped',
        reason: 'idempotent_replay',
        tenantId,
        key,
        prior,
      };
    }

    if (syncEvent.payload.ensureCompanyFirst) {
      const ref = syncEvent.payload.ensureCompanyFirst;
      const { companyNodeId } = require('./stableIds');
      const { NODE_TYPES } = require('../types/nodeTypes');
      const nodeId = companyNodeId(tenantId, ref.id);
      const existing = await this.knowledge.findNode(tenantId, nodeId);
      if (!existing) {
        // Create a stub company via KnowledgeService only (no repository access).
        await this.knowledge.ensureNode({
          tenantId,
          type: NODE_TYPES.COMPANY,
          id: nodeId,
          name: ref.name || `Company ${ref.id}`,
          metadata: {
            crmCompanyId: String(ref.id),
            ensuredFrom: 'prospect_company_ref',
          },
        });
      }
    }

    const knowledgeEvent = syncEvent.payload.knowledgeEvent;
    if (!knowledgeEvent || !knowledgeEvent.type) {
      throw new Error('sync event payload.knowledgeEvent is required');
    }

    const published = await this.bus.publish({
      ...knowledgeEvent,
      tenantId,
      id: knowledgeEvent.id || `${key}:knowledge`,
      occurredAt: syncEvent.occurredAt,
    });

    const result = {
      status: 'applied',
      tenantId,
      key,
      syncType: syncEvent.type,
      knowledgeType: knowledgeEvent.type,
      knowledgeResult: published.results[0] || null,
    };

    await this.ledger.mark(tenantId, key, {
      syncType: syncEvent.type,
      knowledgeType: knowledgeEvent.type,
      entityKind: syncEvent.payload.entityKind || null,
      entityId: syncEvent.payload.entityId || null,
    });

    return result;
  }

  /**
   * @param {object[]} syncEvents
   * @param {{ force?: boolean, continueOnError?: boolean }} [options]
   */
  async applyMany(syncEvents, options = {}) {
    const results = [];
    let applied = 0;
    let skipped = 0;
    let failed = 0;

    for (const event of syncEvents) {
      try {
        const result = await this.apply(event, options);
        results.push(result);
        if (result.status === 'applied') applied += 1;
        else skipped += 1;
      } catch (err) {
        failed += 1;
        const entry = {
          status: 'failed',
          tenantId: event?.tenantId || null,
          key: event?.id || null,
          error: err.message,
        };
        results.push(entry);
        if (!options.continueOnError) {
          return { applied, skipped, failed, results };
        }
      }
    }

    return { applied, skipped, failed, results };
  }

  /**
   * Rebuild graph for a tenant from a relational source reader.
   * Source only provides rows; all writes go through apply → KnowledgeService.
   *
   * @param {string} tenantId
   * @param {import('./RelationalSource').RelationalSource} source
   * @param {{ pageSize?: number, includeTouchpoints?: boolean, force?: boolean }} [options]
   */
  async rebuildFromRelational(tenantId, source, options = {}) {
    if (!source || typeof source.listCompanies !== 'function') {
      throw new Error('rebuildFromRelational requires a RelationalSource');
    }
    requireTenant(tenantId);
    const pageSize = options.pageSize || 100;
    const includeTouchpoints = options.includeTouchpoints !== false;
    const summary = {
      tenantId: String(tenantId),
      companies: { read: 0, applied: 0, skipped: 0 },
      prospects: { read: 0, applied: 0, skipped: 0 },
      touchpoints: { read: 0, applied: 0, skipped: 0 },
      failed: 0,
    };

    const { mapCompanyRow, mapProspectRow, mapTouchpointRow } = require('./mappers');

    let afterId = null;
    for (;;) {
      const page = await source.listCompanies(tenantId, { afterId, limit: pageSize });
      if (!page.length) break;
      summary.companies.read += page.length;
      const events = page.map((row) => mapCompanyRow(row));
      const batch = await this.applyMany(events, { force: options.force });
      summary.companies.applied += batch.applied;
      summary.companies.skipped += batch.skipped;
      summary.failed += batch.failed;
      afterId = page[page.length - 1].id;
      if (page.length < pageSize) break;
    }

    afterId = null;
    for (;;) {
      const page = await source.listProspects(tenantId, { afterId, limit: pageSize });
      if (!page.length) break;
      summary.prospects.read += page.length;
      const events = page.map((row) => mapProspectRow(row));
      const batch = await this.applyMany(events, { force: options.force });
      summary.prospects.applied += batch.applied;
      summary.prospects.skipped += batch.skipped;
      summary.failed += batch.failed;
      afterId = page[page.length - 1].id;
      if (page.length < pageSize) break;
    }

    if (includeTouchpoints && typeof source.listTouchpoints === 'function') {
      afterId = null;
      for (;;) {
        const page = await source.listTouchpoints(tenantId, { afterId, limit: pageSize });
        if (!page.length) break;
        summary.touchpoints.read += page.length;
        const events = page.map((row) => mapTouchpointRow(row));
        const batch = await this.applyMany(events, { force: options.force });
        summary.touchpoints.applied += batch.applied;
        summary.touchpoints.skipped += batch.skipped;
        summary.failed += batch.failed;
        afterId = page[page.length - 1].id;
        if (page.length < pageSize) break;
      }
    }

    return summary;
  }
}

function assertSyncEvent(event) {
  if (!event || !event.type) throw new Error('sync event.type is required');
  if (event.tenantId == null || event.tenantId === '') {
    throw new Error('sync event.tenantId is required');
  }
  if (!event.id) throw new Error('sync event.id (idempotency key) is required');
  if (!event.payload) throw new Error('sync event.payload is required');
}

function requireTenant(tenantId) {
  if (tenantId == null || tenantId === '') {
    throw new Error('tenantId is required');
  }
}

module.exports = {
  GraphSyncEngine,
};
