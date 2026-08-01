-- Rollback SPEC-061 Market Intelligence Ingestion

DROP TABLE IF EXISTS market_intel_sync_state;
DROP TABLE IF EXISTS market_emails;
DROP TABLE IF EXISTS market_companies;
