BEGIN;
-- Explicit operator migration only. No runtime schema creation or budget reset.
CREATE TABLE scout_bounded_acquisition_runs (
  approval_ref text PRIMARY KEY,
  manifest_hash text NOT NULL UNIQUE,
  tenant_id text NOT NULL CHECK (tenant_id='10'),
  search_fingerprint text NOT NULL,
  grant_hash text NOT NULL,
  query_key text NOT NULL,
  execution_token uuid NOT NULL,
  max_usd_micros integer NOT NULL CHECK (max_usd_micros BETWEEN 1 AND 620000),
  state text NOT NULL DEFAULT 'claimed' CHECK (state IN ('claimed','completed','stopped')),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  finished_at timestamptz,
  UNIQUE(tenant_id,search_fingerprint,grant_hash),
  UNIQUE(tenant_id,search_fingerprint,query_key),
  FOREIGN KEY(tenant_id,search_fingerprint,grant_hash)
    REFERENCES scout_query_coverage_grants(tenant_id,search_fingerprint,grant_hash)
);
CREATE TABLE scout_bounded_acquisition_attempts (
  id uuid PRIMARY KEY,
  approval_ref text NOT NULL REFERENCES scout_bounded_acquisition_runs(approval_ref),
  operation text NOT NULL CHECK (operation IN ('text_search','place_details')),
  reserved_usd_micros integer NOT NULL CHECK (
    (operation='text_search' AND reserved_usd_micros=40000) OR
    (operation='place_details' AND reserved_usd_micros=25000)),
  api_usage_id uuid UNIQUE REFERENCES api_usage_events(id),
  state text NOT NULL DEFAULT 'reserved' CHECK (state IN ('reserved','sent','settled','uncertain','blocked')),
  http_status integer,
  response_evidence jsonb,
  response_hash text,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  finished_at timestamptz
);
CREATE INDEX scout_bounded_acquisition_attempts_run ON scout_bounded_acquisition_attempts(approval_ref);
COMMIT;
