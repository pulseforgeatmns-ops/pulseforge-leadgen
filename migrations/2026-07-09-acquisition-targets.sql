BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS acquisition_targets (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  business_name TEXT NOT NULL,
  google_place_id TEXT NOT NULL UNIQUE,
  address TEXT,
  city TEXT,
  state TEXT,
  zip TEXT,
  phone TEXT,
  phone_type TEXT NOT NULL DEFAULT 'unknown'
    CHECK (phone_type IN ('landline', 'mobile', 'voip', 'unknown')),
  website_url TEXT,
  website_status TEXT NOT NULL DEFAULT 'none'
    CHECK (website_status IN ('none', 'dead', 'stale', 'active')),
  google_rating NUMERIC,
  review_count INTEGER,
  most_recent_review_date DATE,
  reviews_last_12mo INTEGER NOT NULL DEFAULT 0,
  years_on_google NUMERIC,
  service_type TEXT NOT NULL DEFAULT 'unknown'
    CHECK (service_type IN ('commercial', 'residential', 'mixed', 'unknown')),
  aging_score INTEGER NOT NULL DEFAULT 0 CHECK (aging_score >= 0 AND aging_score <= 100),
  aging_signals JSONB NOT NULL DEFAULT '[]'::jsonb,
  pulseforge_prospect_id UUID,
  status TEXT NOT NULL DEFAULT 'new'
    CHECK (status IN ('new', 'researching', 'letter_sent', 'contacted', 'in_talks', 'dead', 'acquired')),
  notes TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_acquisition_targets_score
  ON acquisition_targets (aging_score DESC, review_count ASC);

CREATE INDEX IF NOT EXISTS idx_acquisition_targets_city
  ON acquisition_targets (city);

COMMIT;
