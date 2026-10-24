'use strict';

/**
 * Optional Postgres reader for rebuildFromRelational.
 * READ-ONLY — never writes to CRM or GraphRepository.
 */
class PostgresRelationalSource {
  /**
   * @param {{ query: Function }} pool - pg Pool or client with query()
   */
  constructor(pool) {
    if (!pool || typeof pool.query !== 'function') {
      throw new Error('PostgresRelationalSource requires a pg pool');
    }
    this.pool = pool;
  }

  async listCompanies(tenantId, { afterId = null, limit = 100 } = {}) {
    if (afterId == null) {
      const { rows } = await this.pool.query(
        `SELECT id, client_id, name, industry, location, website, icp_score, created_at
         FROM companies
         WHERE client_id = $1
         ORDER BY id ASC
         LIMIT $2`,
        [String(tenantId), limit]
      );
      return rows;
    }
    const { rows } = await this.pool.query(
      `SELECT id, client_id, name, industry, location, website, icp_score, created_at
       FROM companies
       WHERE client_id = $1 AND id > $2
       ORDER BY id ASC
       LIMIT $3`,
      [String(tenantId), afterId, limit]
    );
    return rows;
  }

  async listProspects(tenantId, { afterId = null, limit = 100 } = {}) {
    if (afterId == null) {
      const { rows } = await this.pool.query(
        `SELECT id, client_id, company_id, first_name, last_name, email, phone,
                job_title, icp_score, vertical, source, created_at
         FROM prospects
         WHERE client_id = $1
         ORDER BY id ASC
         LIMIT $2`,
        [String(tenantId), limit]
      );
      return rows;
    }
    const { rows } = await this.pool.query(
      `SELECT id, client_id, company_id, first_name, last_name, email, phone,
              job_title, icp_score, vertical, source, created_at
       FROM prospects
       WHERE client_id = $1 AND id > $2
       ORDER BY id ASC
       LIMIT $3`,
      [String(tenantId), afterId, limit]
    );
    return rows;
  }

  async listTouchpoints(tenantId, { afterId = null, limit = 100 } = {}) {
    if (afterId == null) {
      const { rows } = await this.pool.query(
        `SELECT id, client_id, prospect_id, channel, action_type, content_summary,
                outcome, sentiment, agent_id, external_ref, created_at
         FROM touchpoints
         WHERE client_id = $1
         ORDER BY id ASC
         LIMIT $2`,
        [String(tenantId), limit]
      );
      return rows;
    }
    const { rows } = await this.pool.query(
      `SELECT id, client_id, prospect_id, channel, action_type, content_summary,
              outcome, sentiment, agent_id, external_ref, created_at
       FROM touchpoints
       WHERE client_id = $1 AND id > $2
       ORDER BY id ASC
       LIMIT $3`,
      [String(tenantId), afterId, limit]
    );
    return rows;
  }
}

module.exports = {
  PostgresRelationalSource,
};
