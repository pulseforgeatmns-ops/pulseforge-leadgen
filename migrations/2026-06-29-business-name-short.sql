ALTER TABLE companies
  ADD COLUMN IF NOT EXISTS business_name_short TEXT,
  ADD COLUMN IF NOT EXISTS business_name_short_confidence TEXT,
  ADD COLUMN IF NOT EXISTS business_name_short_flags TEXT[] DEFAULT ARRAY[]::TEXT[];
