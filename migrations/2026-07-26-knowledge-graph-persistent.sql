-- SPEC-001 Persistent Knowledge Store (v0.7.3)
-- Graph-owned schema. Not a mirror of CRM tables.

CREATE TABLE IF NOT EXISTS knowledge_nodes (
  tenant_id TEXT NOT NULL,
  id TEXT NOT NULL,
  type TEXT NOT NULL
    CHECK (type IN ('company', 'person', 'interaction')),
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  body JSONB NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY (tenant_id, id)
);

CREATE INDEX IF NOT EXISTS knowledge_nodes_tenant_type_idx
  ON knowledge_nodes (tenant_id, type);

CREATE TABLE IF NOT EXISTS knowledge_evidence (
  tenant_id TEXT NOT NULL,
  id TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  body JSONB NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY (tenant_id, id)
);

CREATE INDEX IF NOT EXISTS knowledge_evidence_tenant_idx
  ON knowledge_evidence (tenant_id);

CREATE TABLE IF NOT EXISTS knowledge_claims (
  tenant_id TEXT NOT NULL,
  id TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  body JSONB NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY (tenant_id, id)
);

CREATE INDEX IF NOT EXISTS knowledge_claims_tenant_idx
  ON knowledge_claims (tenant_id);

CREATE TABLE IF NOT EXISTS knowledge_edges (
  tenant_id TEXT NOT NULL,
  id TEXT NOT NULL,
  type TEXT NOT NULL,
  from_id TEXT NOT NULL,
  to_id TEXT NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (tenant_id, id)
);

CREATE INDEX IF NOT EXISTS knowledge_edges_tenant_from_idx
  ON knowledge_edges (tenant_id, from_id);

CREATE INDEX IF NOT EXISTS knowledge_edges_tenant_to_idx
  ON knowledge_edges (tenant_id, to_id);

CREATE INDEX IF NOT EXISTS knowledge_edges_tenant_type_idx
  ON knowledge_edges (tenant_id, type);
