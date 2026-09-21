-- Allow unassessed interest on ao_leads (NULL = no observed interaction yet).
-- Observed low/medium/high are set only after real Field Mode interaction.

ALTER TABLE ao_leads ALTER COLUMN interest_level DROP NOT NULL;
ALTER TABLE ao_leads ALTER COLUMN interest_level DROP DEFAULT;

ALTER TABLE ao_leads DROP CONSTRAINT IF EXISTS ao_leads_interest_level_check;

ALTER TABLE ao_leads ADD CONSTRAINT ao_leads_interest_level_check
  CHECK (interest_level IS NULL OR interest_level IN ('low', 'medium', 'high'));
