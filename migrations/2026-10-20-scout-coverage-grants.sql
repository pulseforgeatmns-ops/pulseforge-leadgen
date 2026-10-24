BEGIN;
-- Version2 separates permanent finite allocation from explicit execution grants.
-- No conversion/backfill of version1 receipts or existing page history.
CREATE TABLE IF NOT EXISTS scout_query_coverage_allocations (
 tenant_id text NOT NULL CHECK (tenant_id IN ('10','13')),
 search_fingerprint text NOT NULL CHECK (search_fingerprint ~ '^[a-f0-9]{64}$'),
 allocation_hash text NOT NULL CHECK (allocation_hash ~ '^[a-f0-9]{64}$'),
 epoch integer NOT NULL CHECK (epoch=1),
 max_roots integer NOT NULL CHECK (max_roots=54),
 max_pages_per_root integer NOT NULL CHECK (max_pages_per_root=3),
 created_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(tenant_id,search_fingerprint),
 UNIQUE(tenant_id,search_fingerprint,allocation_hash)
);
CREATE TABLE IF NOT EXISTS scout_query_coverage_grants (
 tenant_id text NOT NULL,
 search_fingerprint text NOT NULL,
 allocation_hash text NOT NULL,
 grant_hash text NOT NULL CHECK (grant_hash ~ '^[a-f0-9]{64}$'),
 approval_ref text NOT NULL,
 root_query_keys text[] NOT NULL CHECK (cardinality(root_query_keys) BETWEEN 1 AND 54),
 not_before timestamptz NOT NULL,
 expires_at timestamptz NOT NULL CHECK(expires_at>not_before),
 max_new_pages integer NOT NULL CHECK(max_new_pages BETWEEN 1 AND cardinality(root_query_keys)*3),
 revoked_at timestamptz,
 created_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(tenant_id,search_fingerprint,grant_hash),
 UNIQUE(tenant_id,search_fingerprint,approval_ref),
 FOREIGN KEY(tenant_id,search_fingerprint,allocation_hash)
  REFERENCES scout_query_coverage_allocations(tenant_id,search_fingerprint,allocation_hash)
);
CREATE TABLE IF NOT EXISTS scout_query_coverage_grant_roots (
 tenant_id text NOT NULL,
 search_fingerprint text NOT NULL,
 allocation_hash text NOT NULL,
 original_root_key text NOT NULL CHECK(original_root_key ~ '^[a-f0-9]{64}$'),
 epoch_root_key text NOT NULL CHECK(epoch_root_key ~ '^[a-f0-9]{64}$'),
 created_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(tenant_id,search_fingerprint,original_root_key),
 UNIQUE(tenant_id,search_fingerprint,epoch_root_key),
 FOREIGN KEY(tenant_id,search_fingerprint,allocation_hash)
  REFERENCES scout_query_coverage_allocations(tenant_id,search_fingerprint,allocation_hash)
);
CREATE TABLE IF NOT EXISTS scout_query_coverage_grant_pages (
 tenant_id text NOT NULL,
 search_fingerprint text NOT NULL,
 query_key text NOT NULL,
 grant_hash text NOT NULL,
 reservation_id uuid NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(tenant_id,search_fingerprint,query_key),
 FOREIGN KEY(tenant_id,search_fingerprint,grant_hash)
  REFERENCES scout_query_coverage_grants(tenant_id,search_fingerprint,grant_hash),
 FOREIGN KEY(tenant_id,search_fingerprint,query_key)
  REFERENCES scout_query_pages(tenant_id,search_fingerprint,query_key)
);
-- Mixed-version deployments cannot create two independent allocations for one
-- scope. Both formats serialize on the same key; no old rows are rewritten.
CREATE OR REPLACE FUNCTION scout_exclude_mixed_coverage_allocations() RETURNS trigger
LANGUAGE plpgsql SET search_path=public AS $$
BEGIN
 PERFORM pg_advisory_xact_lock(hashtextextended('scout-coverage-epoch:' || NEW.tenant_id || ':' || NEW.search_fingerprint,0));
 IF TG_TABLE_NAME='scout_query_coverage_epoch_receipts' THEN
  IF EXISTS(SELECT 1 FROM scout_query_coverage_allocations WHERE tenant_id=NEW.tenant_id AND search_fingerprint=NEW.search_fingerprint) THEN
   RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='coverage_epoch_format_conflict';
  END IF;
 ELSE
  IF EXISTS(SELECT 1 FROM scout_query_coverage_epoch_receipts WHERE tenant_id=NEW.tenant_id AND search_fingerprint=NEW.search_fingerprint) THEN
   RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='coverage_epoch_format_conflict';
  END IF;
 END IF;
 RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS scout_exclude_v2_coverage_allocation ON scout_query_coverage_epoch_receipts;
CREATE TRIGGER scout_exclude_v2_coverage_allocation BEFORE INSERT OR UPDATE OF tenant_id,search_fingerprint
 ON scout_query_coverage_epoch_receipts FOR EACH ROW EXECUTE FUNCTION scout_exclude_mixed_coverage_allocations();
DROP TRIGGER IF EXISTS scout_exclude_v1_coverage_allocation ON scout_query_coverage_allocations;
CREATE TRIGGER scout_exclude_v1_coverage_allocation BEFORE INSERT OR UPDATE OF tenant_id,search_fingerprint
 ON scout_query_coverage_allocations FOR EACH ROW EXECUTE FUNCTION scout_exclude_mixed_coverage_allocations();
COMMIT;
