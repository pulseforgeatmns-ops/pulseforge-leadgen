const pool = require('../db');

let schemaPromise;

function ensureTieredEnrichmentSchema() {
  if (!schemaPromise) {
    schemaPromise = (async () => {
      await pool.query(`
        ALTER TABLE prospects
          ADD COLUMN IF NOT EXISTS practice_area TEXT,
          ADD COLUMN IF NOT EXISTS firm_size TEXT,
          ADD COLUMN IF NOT EXISTS enrichment_status TEXT,
          ADD COLUMN IF NOT EXISTS enrichment_resolved_tier INTEGER,
          ADD COLUMN IF NOT EXISTS enrichment_provenance JSONB DEFAULT '{}'::jsonb,
          ADD COLUMN IF NOT EXISTS enrichment_checked_at TIMESTAMPTZ;

        ALTER TABLE companies
          ADD COLUMN IF NOT EXISTS practice_area TEXT,
          ADD COLUMN IF NOT EXISTS firm_size TEXT,
          ADD COLUMN IF NOT EXISTS enrichment_provenance JSONB DEFAULT '{}'::jsonb;

        CREATE TABLE IF NOT EXISTS enrichment_manual_queue (
          id BIGSERIAL PRIMARY KEY,
          client_id INTEGER NOT NULL,
          prospect_id UUID NOT NULL,
          company_name TEXT,
          website TEXT,
          missing_fields TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
          candidate_names JSONB NOT NULL DEFAULT '[]'::jsonb,
          candidate_emails JSONB NOT NULL DEFAULT '[]'::jsonb,
          partial_data JSONB NOT NULL DEFAULT '{}'::jsonb,
          status TEXT NOT NULL DEFAULT 'open',
          last_attempted_at TIMESTAMPTZ,
          resolved_at TIMESTAMPTZ,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          UNIQUE (client_id, prospect_id)
        );

        ALTER TABLE enrichment_manual_queue
          ADD COLUMN IF NOT EXISTS company_name TEXT,
          ADD COLUMN IF NOT EXISTS website TEXT,
          ADD COLUMN IF NOT EXISTS missing_fields TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
          ADD COLUMN IF NOT EXISTS candidate_names JSONB NOT NULL DEFAULT '[]'::jsonb,
          ADD COLUMN IF NOT EXISTS candidate_emails JSONB NOT NULL DEFAULT '[]'::jsonb,
          ADD COLUMN IF NOT EXISTS partial_data JSONB NOT NULL DEFAULT '{}'::jsonb,
          ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'open',
          ADD COLUMN IF NOT EXISTS last_attempted_at TIMESTAMPTZ,
          ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMPTZ,
          ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

        CREATE INDEX IF NOT EXISTS enrichment_manual_queue_client_status_idx
          ON enrichment_manual_queue (client_id, status);
      `);
    })().catch(err => {
      schemaPromise = null;
      throw err;
    });
  }
  return schemaPromise;
}

module.exports = { ensureTieredEnrichmentSchema };
