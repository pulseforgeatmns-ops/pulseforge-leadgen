'use strict';

/**
 * SPEC-143 — Scout Acquisition Intelligence Memory store.
 * In-memory and Postgres backends for durable investigation knowledge.
 */

const { MEMORY_TYPES, MEMORY_STATUS, asText } = require('./types');
const { reconcileClaimMemory, detectMemoryContradictions } = require('./ContradictionMemory');
const { refreshMemoryConfidence } = require('./MemoryConfidence');
const { buildMemoryGraphFromKnowledge, mergeMemoryGraphs, serializeMemoryGraph } = require('./MemoryGraph');

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function indexMemories(rows) {
  const byType = {
    [MEMORY_TYPES.MARKET]: new Map(),
    [MEMORY_TYPES.COMPANY]: new Map(),
    [MEMORY_TYPES.PERSON]: new Map(),
    [MEMORY_TYPES.CLAIM]: new Map(),
    [MEMORY_TYPES.INVESTIGATION]: new Map(),
  };
  for (const row of rows) {
    const type = row.type;
    if (!byType[type]) continue;
    byType[type].set(row.entityKey, clone(row));
  }
  return byType;
}

function upsertMemory(map, memory, reconcileFn) {
  const key = memory.entityKey;
  const existing = map.get(key);
  if (reconcileFn && existing) {
    const { memory: next } = reconcileFn(existing, memory);
    map.set(key, clone(next));
    return next;
  }
  if (existing) {
    const refreshed = refreshMemoryConfidence(existing, memory);
    map.set(key, clone({ ...existing, ...memory, ...refreshed }));
    return map.get(key);
  }
  map.set(key, clone(memory));
  return memory;
}

function createMemoryIntelligenceStore(snapshot = null) {
  /** @type {Map<string, object[]>} */
  const tenantRows = new Map();
  /** @type {Map<string, object>} */
  const tenantGraphs = new Map();

  if (snapshot && Array.isArray(snapshot.tenants)) {
    for (const tenant of snapshot.tenants) {
      tenantRows.set(String(tenant.tenantId), clone(tenant.memories || []));
      if (tenant.graph) tenantGraphs.set(String(tenant.tenantId), tenant.graph);
    }
  }

  function getRows(tenantId) {
    return tenantRows.get(String(tenantId)) || [];
  }

  function setRows(tenantId, rows) {
    tenantRows.set(String(tenantId), rows.map(clone));
  }

  function getIndex(tenantId) {
    return indexMemories(getRows(tenantId));
  }

  return {
    kind: 'memory',

    async query(tenantId, filters = {}) {
      const idx = getIndex(tenantId);
      const type = filters.type;
      const entityKey = filters.entityKey;
      const marketKey = filters.marketKey;

      let rows = getRows(tenantId);
      if (type) rows = rows.filter((r) => r.type === type);
      if (entityKey) rows = rows.filter((r) => r.entityKey === entityKey);
      if (marketKey) {
        rows = rows.filter(
          (r) =>
            r.entityKey === marketKey ||
            r.marketKey === marketKey ||
            (r.type === MEMORY_TYPES.MARKET && r.entityKey === marketKey)
        );
      }
      if (filters.status) rows = rows.filter((r) => r.status === filters.status);
      return rows.map(clone);
    },

    async loadForMarket(tenantId, geography, segment) {
      const { marketEntityKey } = require('./types');
      const key = marketEntityKey(geography, segment);
      const idx = getIndex(tenantId);
      const market = idx[MEMORY_TYPES.MARKET].get(key) || null;
      const investigation = idx[MEMORY_TYPES.INVESTIGATION].get(key) || null;

      const companies = [...idx[MEMORY_TYPES.COMPANY].values()];
      const people = [...idx[MEMORY_TYPES.PERSON].values()];
      const claims = [...idx[MEMORY_TYPES.CLAIM].values()];

      return {
        tenantId: String(tenantId),
        marketKey: key,
        market,
        investigation,
        companies: companies.map(clone),
        people: people.map(clone),
        claims: claims.map(clone),
      };
    },

    async persistKnowledge(tenantId, knowledge, opts = {}) {
      const id = String(tenantId);
      const idx = getIndex(id);
      const conflicts = detectMemoryContradictions(
        [...idx[MEMORY_TYPES.CLAIM].values()],
        knowledge.claims || []
      );

      if (knowledge.market) {
        upsertMemory(idx[MEMORY_TYPES.MARKET], knowledge.market);
      }
      for (const company of knowledge.companies || []) {
        upsertMemory(idx[MEMORY_TYPES.COMPANY], company);
      }
      for (const person of knowledge.people || []) {
        upsertMemory(idx[MEMORY_TYPES.PERSON], person);
      }
      for (const claim of knowledge.claims || []) {
        upsertMemory(idx[MEMORY_TYPES.CLAIM], claim, reconcileClaimMemory);
      }
      if (knowledge.investigation) {
        upsertMemory(idx[MEMORY_TYPES.INVESTIGATION], knowledge.investigation);
      }

      const allRows = [
        ...idx[MEMORY_TYPES.MARKET].values(),
        ...idx[MEMORY_TYPES.COMPANY].values(),
        ...idx[MEMORY_TYPES.PERSON].values(),
        ...idx[MEMORY_TYPES.CLAIM].values(),
        ...idx[MEMORY_TYPES.INVESTIGATION].values(),
      ];
      setRows(id, allRows);

      const existingGraph = tenantGraphs.get(id);
      const merged = mergeMemoryGraphs(existingGraph || { tenantId: id }, knowledge);
      tenantGraphs.set(id, merged);

      return {
        persisted: true,
        counts: knowledge.counts || {},
        conflicts,
        graph: serializeMemoryGraph(merged),
      };
    },

    async getGraph(tenantId) {
      const graph = tenantGraphs.get(String(tenantId));
      if (graph) return serializeMemoryGraph(graph);
      const knowledge = await this.loadForMarket(tenantId, '', '');
      const built = buildMemoryGraphFromKnowledge(knowledge);
      tenantGraphs.set(String(tenantId), built);
      return serializeMemoryGraph(built);
    },

    serialize() {
      return {
        tenants: [...tenantRows.entries()].map(([tenantId, memories]) => ({
          tenantId,
          memories,
          graph: tenantGraphs.has(tenantId)
            ? serializeMemoryGraph(tenantGraphs.get(tenantId))
            : null,
        })),
      };
    },

    clear() {
      tenantRows.clear();
      tenantGraphs.clear();
    },
  };
}

function createPostgresIntelligenceStore(pool) {
  const db = pool || require('../../../db');
  let ensured = false;

  async function ensureTables() {
    if (ensured) return;
    await db.query(`
      CREATE TABLE IF NOT EXISTS scout_intelligence_memory (
        id TEXT PRIMARY KEY,
        tenant_id TEXT NOT NULL,
        memory_type TEXT NOT NULL,
        entity_key TEXT NOT NULL,
        label TEXT,
        payload JSONB NOT NULL DEFAULT '{}'::jsonb,
        confidence DOUBLE PRECISION NOT NULL DEFAULT 0,
        verified_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        source_count INTEGER NOT NULL DEFAULT 1,
        verification_sources JSONB NOT NULL DEFAULT '[]'::jsonb,
        status TEXT NOT NULL DEFAULT 'active',
        mission_id TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (tenant_id, memory_type, entity_key)
      )
    `);
    await db.query(`
      CREATE INDEX IF NOT EXISTS scout_intelligence_memory_tenant_type_idx
        ON scout_intelligence_memory (tenant_id, memory_type)
    `);
    await db.query(`
      CREATE INDEX IF NOT EXISTS scout_intelligence_memory_tenant_market_idx
        ON scout_intelligence_memory (tenant_id, entity_key)
    `);
    await db.query(`
      CREATE TABLE IF NOT EXISTS scout_intelligence_memory_edges (
        tenant_id TEXT NOT NULL,
        from_id TEXT NOT NULL,
        to_id TEXT NOT NULL,
        relation TEXT NOT NULL DEFAULT 'related',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (tenant_id, from_id, to_id, relation)
      )
    `);
    ensured = true;
  }

  function mapRow(row) {
    if (!row) return null;
    const payload = row.payload || {};
    return {
      ...payload,
      id: row.id,
      type: row.memory_type,
      tenantId: row.tenant_id,
      entityKey: row.entity_key,
      label: row.label,
      confidence: row.confidence != null ? Number(row.confidence) : 0,
      verifiedAt:
        row.verified_at instanceof Date ? row.verified_at.toISOString() : String(row.verified_at),
      sourceCount: Number(row.source_count || 1),
      verificationSources: row.verification_sources || [],
      status: row.status || MEMORY_STATUS.ACTIVE,
      missionId: row.mission_id,
      updatedAt:
        row.updated_at instanceof Date ? row.updated_at.toISOString() : String(row.updated_at),
    };
  }

  async function upsertRow(memory) {
    await ensureTables();
    const result = await db.query(
      `INSERT INTO scout_intelligence_memory (
        id, tenant_id, memory_type, entity_key, label, payload,
        confidence, verified_at, source_count, verification_sources,
        status, mission_id, updated_at
      ) VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7,$8,$9,$10::jsonb,$11,$12,NOW())
      ON CONFLICT (tenant_id, memory_type, entity_key) DO UPDATE SET
        label = EXCLUDED.label,
        payload = EXCLUDED.payload,
        confidence = EXCLUDED.confidence,
        verified_at = EXCLUDED.verified_at,
        source_count = EXCLUDED.source_count,
        verification_sources = EXCLUDED.verification_sources,
        status = EXCLUDED.status,
        mission_id = EXCLUDED.mission_id,
        updated_at = NOW()
      RETURNING *`,
      [
        memory.id,
        String(memory.tenantId),
        memory.type,
        memory.entityKey,
        memory.label || null,
        JSON.stringify(memory),
        memory.confidence != null ? Number(memory.confidence) : 0,
        memory.verifiedAt || new Date().toISOString(),
        memory.sourceCount != null ? Number(memory.sourceCount) : 1,
        JSON.stringify(memory.verificationSources || []),
        memory.status || MEMORY_STATUS.ACTIVE,
        memory.missionId || null,
      ]
    );
    return mapRow(result.rows[0]);
  }

  const memoryStore = createMemoryIntelligenceStore();

  return {
    kind: 'postgres',

    async query(tenantId, filters = {}) {
      await ensureTables();
      const params = [String(tenantId)];
      let sql = `SELECT * FROM scout_intelligence_memory WHERE tenant_id = $1`;
      if (filters.type) {
        params.push(filters.type);
        sql += ` AND memory_type = $${params.length}`;
      }
      if (filters.entityKey) {
        params.push(filters.entityKey);
        sql += ` AND entity_key = $${params.length}`;
      }
      sql += ` ORDER BY updated_at DESC LIMIT 1000`;
      const result = await db.query(sql, params);
      return (result.rows || []).map(mapRow);
    },

    async loadForMarket(tenantId, geography, segment) {
      const { marketEntityKey } = require('./types');
      const key = marketEntityKey(geography, segment);
      const rows = await this.query(tenantId);
      const market = rows.find((r) => r.type === MEMORY_TYPES.MARKET && r.entityKey === key) || null;
      const investigation =
        rows.find((r) => r.type === MEMORY_TYPES.INVESTIGATION && r.entityKey === key) || null;
      const companies = rows.filter((r) => r.type === MEMORY_TYPES.COMPANY);
      const people = rows.filter((r) => r.type === MEMORY_TYPES.PERSON);
      const claims = rows.filter((r) => r.type === MEMORY_TYPES.CLAIM);
      return {
        tenantId: String(tenantId),
        marketKey: key,
        market,
        investigation,
        companies,
        people,
        claims,
      };
    },

    async persistKnowledge(tenantId, knowledge, opts = {}) {
      const conflicts = [];
      const memories = [
        knowledge.market,
        ...(knowledge.companies || []),
        ...(knowledge.people || []),
        ...(knowledge.claims || []),
        knowledge.investigation,
      ].filter(Boolean);

      for (const memory of memories) {
        if (memory.type === MEMORY_TYPES.CLAIM) {
          const existing = await this.query(tenantId, {
            type: MEMORY_TYPES.CLAIM,
            entityKey: memory.entityKey,
          });
          if (existing[0]) {
            const { memory: reconciled, conflict } = reconcileClaimMemory(existing[0], memory, opts);
            if (conflict) conflicts.push(conflict);
            await upsertRow({ ...reconciled, tenantId: String(tenantId) });
            continue;
          }
        }
        await upsertRow({ ...memory, tenantId: String(tenantId) });
      }

      const graphResult = await memoryStore.persistKnowledge(tenantId, knowledge, opts);
      await ensureTables();
      for (const edge of graphResult.graph?.edges || []) {
        await db.query(
          `INSERT INTO scout_intelligence_memory_edges (tenant_id, from_id, to_id, relation)
           VALUES ($1,$2,$3,$4)
           ON CONFLICT DO NOTHING`,
          [String(tenantId), edge.from, edge.to, edge.relation]
        );
      }

      return {
        persisted: true,
        counts: knowledge.counts || {},
        conflicts,
        graph: graphResult.graph,
      };
    },

    async getGraph(tenantId) {
      const knowledge = await this.loadForMarket(tenantId, '', '');
      const graph = buildMemoryGraphFromKnowledge(knowledge);
      return serializeMemoryGraph(graph);
    },
  };
}

module.exports = {
  createMemoryIntelligenceStore,
  createPostgresIntelligenceStore,
};
