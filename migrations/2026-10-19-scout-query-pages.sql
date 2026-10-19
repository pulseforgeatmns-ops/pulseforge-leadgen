BEGIN;
-- Additive, default-off continuation. Apply only through an approved migration.
-- Tokens are opaque provider continuation data, never credentials or log fields.
CREATE TABLE IF NOT EXISTS scout_query_pages (
  tenant_id text NOT NULL,
  search_fingerprint text NOT NULL,
  query_key text NOT NULL,
  root_query_key text NOT NULL,
  page_index integer NOT NULL CHECK (page_index BETWEEN 0 AND 2),
  state text NOT NULL CHECK (state IN ('reserved','fetched','blocked')),
  reservation_id uuid NOT NULL,
  request_token text CHECK (request_token IS NULL OR length(request_token) BETWEEN 1 AND 8192),
  next_page_token text CHECK (next_page_token IS NULL OR length(next_page_token) BETWEEN 1 AND 8192),
  raw_results jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(raw_results)='array'),
  blocked_reason text,
  available_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  fetched_at timestamptz,
  PRIMARY KEY (tenant_id, search_fingerprint, query_key),
  UNIQUE (tenant_id, search_fingerprint, root_query_key, page_index),
  CHECK ((page_index=0) = (request_token IS NULL)),
  CHECK ((state='fetched') = (fetched_at IS NOT NULL))
);

COMMIT;
