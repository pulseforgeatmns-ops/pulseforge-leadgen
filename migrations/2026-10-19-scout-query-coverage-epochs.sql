BEGIN;
-- One scope-wide immutable receipt pins the complete epoch allocation. A later
-- disjoint allowlist or extended window cannot expand this same epoch.
CREATE TABLE IF NOT EXISTS scout_query_coverage_epoch_receipts (
  tenant_id text NOT NULL CHECK (tenant_id IN ('10','13')),
  search_fingerprint text NOT NULL CHECK (search_fingerprint ~ '^[a-f0-9]{64}$'),
  epoch integer NOT NULL CHECK (epoch=1),
  manifest_hash text NOT NULL CHECK (manifest_hash ~ '^[a-f0-9]{64}$'),
  approval_ref text NOT NULL,
  root_query_keys text[] NOT NULL CHECK (cardinality(root_query_keys) BETWEEN 1 AND 54),
  not_before timestamptz NOT NULL,
  expires_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id,search_fingerprint),
  UNIQUE (tenant_id,search_fingerprint,manifest_hash),
  CHECK (expires_at > not_before)
);
-- Immutable per-root mapping. No legacy rows or attempt history change.
CREATE TABLE IF NOT EXISTS scout_query_coverage_epochs (
  tenant_id text NOT NULL CHECK (tenant_id IN ('10','13')),
  search_fingerprint text NOT NULL CHECK (search_fingerprint ~ '^[a-f0-9]{64}$'),
  original_root_key text NOT NULL CHECK (original_root_key ~ '^[a-f0-9]{64}$'),
  epoch integer NOT NULL CHECK (epoch=1),
  epoch_root_key text NOT NULL CHECK (epoch_root_key ~ '^[a-f0-9]{64}$'),
  manifest_hash text NOT NULL CHECK (manifest_hash ~ '^[a-f0-9]{64}$'),
  approval_ref text NOT NULL,
  not_before timestamptz NOT NULL,
  expires_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id,search_fingerprint,original_root_key),
  UNIQUE (tenant_id,search_fingerprint,epoch_root_key),
  CHECK (expires_at > not_before),
  FOREIGN KEY (tenant_id,search_fingerprint,manifest_hash)
    REFERENCES scout_query_coverage_epoch_receipts(tenant_id,search_fingerprint,manifest_hash)
);
COMMIT;
