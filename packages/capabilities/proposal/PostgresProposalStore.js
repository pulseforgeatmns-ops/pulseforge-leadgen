'use strict';

/**
 * Postgres-backed ProposalStore (SPEC-027B).
 * Falls back gracefully when pool is unavailable — callers should prefer in-memory in tests.
 */

const { PROPOSAL_STATUS } = require('./types');

class PostgresProposalStore {
  /**
   * @param {object} deps
   * @param {import('pg').Pool} deps.pool
   */
  constructor(deps) {
    if (!deps || !deps.pool) throw new Error('PostgresProposalStore requires pool');
    this._pool = deps.pool;
  }

  async create(input) {
    const opportunityId = input.opportunityId != null ? String(input.opportunityId) : null;
    const missionId = input.missionId != null ? String(input.missionId) : null;
    const versionRes = await this._pool.query(
      `SELECT COALESCE(MAX(version), 0) + 1 AS next_version
       FROM proposal_versions
       WHERE ($1::text IS NOT NULL AND opportunity_id = $1)
          OR ($2::text IS NOT NULL AND mission_id = $2)`,
      [opportunityId, missionId]
    );
    const version = Number(versionRes.rows[0].next_version) || 1;
    const res = await this._pool.query(
      `INSERT INTO proposal_versions (
         opportunity_id, mission_id, client_id, tenant_id, version, status,
         discovery_summary, discovery_profile_id, pricing_package_id,
         document, html, accepted_changes, client_outcome, win_loss, feedback
       ) VALUES (
         $1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9,$10::jsonb,$11,$12::jsonb,$13,$14,$15
       )
       RETURNING *`,
      [
        opportunityId,
        missionId,
        input.clientId != null ? input.clientId : null,
        String(input.tenantId || ''),
        version,
        input.status || PROPOSAL_STATUS.REVIEW,
        JSON.stringify(input.discoverySummary || {}),
        input.discoveryProfileId || null,
        input.pricingPackageId || 'setup_monthly',
        JSON.stringify(input.document || {}),
        input.html || null,
        JSON.stringify(input.acceptedChanges || []),
        input.clientOutcome || null,
        input.winLoss || null,
        input.feedback || null,
      ]
    );
    return mapRow(res.rows[0]);
  }

  async get(id) {
    const res = await this._pool.query(`SELECT * FROM proposal_versions WHERE id = $1`, [
      String(id),
    ]);
    return res.rows[0] ? mapRow(res.rows[0]) : null;
  }

  async listForOpportunity(opportunityOrMissionId) {
    if (!opportunityOrMissionId) return [];
    const key = String(opportunityOrMissionId);
    const res = await this._pool.query(
      `SELECT * FROM proposal_versions
       WHERE opportunity_id = $1 OR mission_id = $1
       ORDER BY version ASC`,
      [key]
    );
    return res.rows.map(mapRow);
  }

  async update(id, patch) {
    const current = await this.get(id);
    if (!current) return null;
    const next = { ...current, ...patch, id: current.id, version: current.version };
    const res = await this._pool.query(
      `UPDATE proposal_versions SET
         status = $2,
         document = $3::jsonb,
         html = $4,
         pricing_package_id = $5,
         accepted_changes = $6::jsonb,
         client_outcome = $7,
         win_loss = $8,
         feedback = $9,
         updated_at = NOW()
       WHERE id = $1
       RETURNING *`,
      [
        String(id),
        next.status,
        JSON.stringify(next.document || {}),
        next.html || null,
        next.pricingPackageId,
        JSON.stringify(next.acceptedChanges || []),
        next.clientOutcome || null,
        next.winLoss || null,
        next.feedback || null,
      ]
    );
    return res.rows[0] ? mapRow(res.rows[0]) : null;
  }
}

function mapRow(row) {
  return {
    id: String(row.id),
    opportunityId: row.opportunity_id,
    missionId: row.mission_id,
    clientId: row.client_id,
    tenantId: row.tenant_id,
    version: Number(row.version),
    status: row.status,
    discoverySummary: row.discovery_summary,
    discoveryProfileId: row.discovery_profile_id,
    pricingPackageId: row.pricing_package_id,
    document: row.document,
    html: row.html,
    acceptedChanges: row.accepted_changes || [],
    clientOutcome: row.client_outcome,
    winLoss: row.win_loss,
    feedback: row.feedback,
    createdAt: row.created_at && row.created_at.toISOString
      ? row.created_at.toISOString()
      : row.created_at,
    updatedAt: row.updated_at && row.updated_at.toISOString
      ? row.updated_at.toISOString()
      : row.updated_at,
  };
}

function createPostgresProposalStore(deps) {
  return new PostgresProposalStore(deps);
}

module.exports = {
  PostgresProposalStore,
  createPostgresProposalStore,
  mapRow,
};
