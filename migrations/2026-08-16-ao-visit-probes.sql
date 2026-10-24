-- AO visit flow: persist Max probing answers on leads and escalations
ALTER TABLE ao_leads ADD COLUMN IF NOT EXISTS probe_answers JSONB;
ALTER TABLE ao_escalations ADD COLUMN IF NOT EXISTS probe_answers JSONB;
