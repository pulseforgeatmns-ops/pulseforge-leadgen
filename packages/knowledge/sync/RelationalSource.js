'use strict';

/**
 * @typedef {object} RelationalSource
 * Contract for rebuilding the graph from existing CRM tables.
 * Implementations may read Postgres, fixtures, or exports — sync never writes here.
 *
 * @property {(tenantId: string, opts: { afterId?: string|number|null, limit: number }) => Promise<object[]>} listCompanies
 * @property {(tenantId: string, opts: { afterId?: string|number|null, limit: number }) => Promise<object[]>} listProspects
 * @property {(tenantId: string, opts: { afterId?: string|number|null, limit: number }) => Promise<object[]>} [listTouchpoints]
 */

/**
 * In-memory relational fixture source for tests and local rebuild demos.
 */
class MemoryRelationalSource {
  /**
   * @param {object} data
   * @param {object[]} [data.companies]
   * @param {object[]} [data.prospects]
   * @param {object[]} [data.touchpoints]
   */
  constructor(data = {}) {
    this.companies = [...(data.companies || [])];
    this.prospects = [...(data.prospects || [])];
    this.touchpoints = [...(data.touchpoints || [])];
  }

  async listCompanies(tenantId, { afterId = null, limit = 100 } = {}) {
    return pageTenantRows(this.companies, tenantId, afterId, limit);
  }

  async listProspects(tenantId, { afterId = null, limit = 100 } = {}) {
    return pageTenantRows(this.prospects, tenantId, afterId, limit);
  }

  async listTouchpoints(tenantId, { afterId = null, limit = 100 } = {}) {
    return pageTenantRows(this.touchpoints, tenantId, afterId, limit);
  }
}

function pageTenantRows(rows, tenantId, afterId, limit) {
  const tenant = String(tenantId);
  const filtered = rows
    .filter((r) => String(r.client_id) === tenant)
    .sort((a, b) => String(a.id).localeCompare(String(b.id), undefined, { numeric: true }));

  let start = 0;
  if (afterId != null) {
    const idx = filtered.findIndex((r) => String(r.id) === String(afterId));
    start = idx === -1 ? filtered.length : idx + 1;
  }
  return filtered.slice(start, start + limit);
}

module.exports = {
  MemoryRelationalSource,
};
