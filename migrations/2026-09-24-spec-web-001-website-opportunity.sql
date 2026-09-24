-- SPEC-WEB-001 — Website Opportunity Intelligence persistence

CREATE TABLE IF NOT EXISTS website_opportunity_assessments (
  id BIGSERIAL PRIMARY KEY,
  client_id INTEGER NOT NULL REFERENCES clients(id),
  mission_id TEXT,
  prospect_id INTEGER REFERENCES prospects(id),
  cohort_tag TEXT,
  business_name TEXT NOT NULL,
  domain TEXT NOT NULL,
  industry TEXT,
  location TEXT,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  opportunity_score INTEGER,
  confidence NUMERIC(5,4),
  recommended_action TEXT,
  score_components JSONB NOT NULL DEFAULT '{}'::jsonb,
  economics JSONB NOT NULL DEFAULT '{}'::jsonb,
  capability_version TEXT NOT NULL DEFAULT '1.0.0',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS website_opportunity_assessments_client_idx
  ON website_opportunity_assessments (client_id);

CREATE INDEX IF NOT EXISTS website_opportunity_assessments_domain_idx
  ON website_opportunity_assessments (client_id, domain);

CREATE INDEX IF NOT EXISTS website_opportunity_assessments_cohort_idx
  ON website_opportunity_assessments (cohort_tag)
  WHERE cohort_tag IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS website_opportunity_assessments_client_domain_cohort_uidx
  ON website_opportunity_assessments (client_id, domain, COALESCE(cohort_tag, ''));

CREATE TABLE IF NOT EXISTS website_opportunity_events (
  id BIGSERIAL PRIMARY KEY,
  event_type TEXT NOT NULL,
  client_id INTEGER,
  mission_id TEXT,
  prospect_id INTEGER,
  domain TEXT,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS website_opportunity_events_type_idx
  ON website_opportunity_events (event_type, created_at DESC);
