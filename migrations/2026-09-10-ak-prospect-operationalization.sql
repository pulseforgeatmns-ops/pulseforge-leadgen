-- SPEC-249: Acquisition Knowledge prospect operationalization bridge.
-- This migration creates only the durable projection/link contract. It does
-- not send outreach, enroll sequences, infer email addresses, or promote
-- epistemic/validation state.

BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

ALTER TABLE prospects
  ADD COLUMN IF NOT EXISTS acquisition_knowledge_object_id TEXT,
  ADD COLUMN IF NOT EXISTS acquisition_projection_id TEXT,
  ADD COLUMN IF NOT EXISTS acquisition_source TEXT,
  ADD COLUMN IF NOT EXISTS acquisition_metadata JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE companies
  ADD COLUMN IF NOT EXISTS industry TEXT,
  ADD COLUMN IF NOT EXISTS location TEXT,
  ADD COLUMN IF NOT EXISTS website TEXT,
  ADD COLUMN IF NOT EXISTS acquisition_metadata JSONB NOT NULL DEFAULT '{}'::jsonb;

DO $compat$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint c
    WHERE c.conrelid = 'public.companies'::regclass
      AND c.contype IN ('p','u')
      AND (SELECT array_agg(a.attname::text ORDER BY a.attname)
           FROM unnest(c.conkey) AS k(attnum)
           JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum)
          = ARRAY['client_id','id']
  ) THEN
    ALTER TABLE companies ADD CONSTRAINT companies_client_id_id_key UNIQUE (client_id, id);
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint c
    WHERE c.conrelid = 'public.prospects'::regclass
      AND c.contype IN ('p','u')
      AND (SELECT array_agg(a.attname::text ORDER BY a.attname)
           FROM unnest(c.conkey) AS k(attnum)
           JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum)
          = ARRAY['client_id','id']
  ) THEN
    ALTER TABLE prospects ADD CONSTRAINT prospects_client_id_id_key UNIQUE (client_id, id);
  END IF;
END
$compat$;

CREATE TABLE IF NOT EXISTS acquisition_prospect_projections (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  client_id INTEGER NOT NULL,
  acquisition_knowledge_object_id TEXT NOT NULL,
  prospect_id UUID NOT NULL,
  company_id UUID,
  company_identity JSONB NOT NULL DEFAULT '{}'::jsonb,
  person_identity JSONB NOT NULL DEFAULT '{}'::jsonb,
  provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
  epistemic_state TEXT NOT NULL,
  validation_state TEXT NOT NULL,
  linked_outreach_asset_ids TEXT[] NOT NULL DEFAULT '{}',
  status TEXT NOT NULL DEFAULT 'enrichment_pending',
  review_reason TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, acquisition_knowledge_object_id),
  UNIQUE (tenant_id, prospect_id),
  FOREIGN KEY (acquisition_knowledge_object_id) REFERENCES acquisition_knowledge_objects(id) ON DELETE RESTRICT,
  FOREIGN KEY (client_id, prospect_id) REFERENCES prospects(client_id, id) ON DELETE RESTRICT,
  FOREIGN KEY (client_id, company_id) REFERENCES companies(client_id, id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS acquisition_prospect_projections_tenant_status_idx
  ON acquisition_prospect_projections (tenant_id, status);

CREATE INDEX IF NOT EXISTS acquisition_prospect_projections_prospect_idx
  ON acquisition_prospect_projections (client_id, prospect_id);

CREATE UNIQUE INDEX IF NOT EXISTS prospects_ak_object_tenant_idx
  ON prospects (client_id, acquisition_knowledge_object_id)
  WHERE acquisition_knowledge_object_id IS NOT NULL;

COMMIT;
