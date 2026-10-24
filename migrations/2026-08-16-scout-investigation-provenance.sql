-- SPEC-099A — persist Scout investigation provenance on Max-owned AO state.
ALTER TABLE acquisition_intelligence_state
  ADD COLUMN IF NOT EXISTS investigation JSONB,
  ADD COLUMN IF NOT EXISTS coverage_confidence DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS coverage_band TEXT,
  ADD COLUMN IF NOT EXISTS conclusion_trust TEXT,
  ADD COLUMN IF NOT EXISTS market_absence_justified BOOLEAN NOT NULL DEFAULT FALSE;
