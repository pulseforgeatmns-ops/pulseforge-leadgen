BEGIN;

ALTER TABLE clients
  ADD COLUMN IF NOT EXISTS target_verticals JSONB NOT NULL DEFAULT '[]'::jsonb,
  ADD COLUMN IF NOT EXISTS vertical_tiers JSONB NOT NULL DEFAULT '{}'::jsonb;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'clients_target_verticals_array_check'
      AND conrelid = 'clients'::regclass
  ) THEN
    ALTER TABLE clients ADD CONSTRAINT clients_target_verticals_array_check
      CHECK (jsonb_typeof(target_verticals) = 'array');
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'clients_vertical_tiers_object_check'
      AND conrelid = 'clients'::regclass
  ) THEN
    ALTER TABLE clients ADD CONSTRAINT clients_vertical_tiers_object_check
      CHECK (jsonb_typeof(vertical_tiers) = 'object');
  END IF;
END $$;

UPDATE clients
SET
  target_verticals = $json$
  [
    {"vertical":"commercial_electrical","tier":"A","autonomous_sourcing":true,"seed_terms":["commercial electrical contractor {city}","commercial electrician {city}"]},
    {"vertical":"commercial_hvac","tier":"A","autonomous_sourcing":true,"seed_terms":["commercial HVAC contractor {city}","commercial heating cooling contractor {city}"]},
    {"vertical":"commercial_mechanical","tier":"A","autonomous_sourcing":true,"seed_terms":["commercial mechanical contractor {city}","mechanical contractor {city}"]},
    {"vertical":"commercial_roofing","tier":"A","autonomous_sourcing":true,"seed_terms":["commercial roofing contractor {city}","industrial roofing company {city}"]},
    {"vertical":"restoration","tier":"A","autonomous_sourcing":true,"seed_terms":["commercial restoration company {city}","water fire restoration contractor {city}"]},
    {"vertical":"commercial_cleaning","tier":"A","autonomous_sourcing":true,"seed_terms":["commercial cleaning company {city}","office cleaning services {city}"]},
    {"vertical":"janitorial","tier":"A","autonomous_sourcing":true,"seed_terms":["janitorial services {city}","janitorial company {city}"]},
    {"vertical":"facility_services","tier":"A","autonomous_sourcing":true,"seed_terms":["facility services company {city}","facility maintenance services {city}"]},
    {"vertical":"commercial_landscaping","tier":"A","autonomous_sourcing":true,"seed_terms":["commercial landscaping company {city}","commercial landscape contractor {city}"]},
    {"vertical":"property_management","tier":"A","autonomous_sourcing":true,"seed_terms":["property management company {city}","commercial property management {city}"]},
    {"vertical":"hoa_management","tier":"A","autonomous_sourcing":true,"seed_terms":["HOA management company {city}","community association management {city}"]},
    {"vertical":"low_voltage_security","tier":"A","autonomous_sourcing":true,"seed_terms":["low voltage security contractor {city}","commercial access control installer {city}"]},
    {"vertical":"fire_protection","tier":"A","autonomous_sourcing":true,"seed_terms":["fire protection contractor {city}","fire sprinkler company {city}"]},
    {"vertical":"staffing_recruiting","tier":"A","autonomous_sourcing":true,"seed_terms":["staffing agency {city}","recruiting firm {city}"]},
    {"vertical":"freight_brokerage","tier":"A","autonomous_sourcing":true,"seed_terms":["freight brokerage {city}","freight broker {city}"]},
    {"vertical":"commercial_insurance","tier":"A","autonomous_sourcing":true,"seed_terms":["commercial insurance agency {city}","business insurance broker {city}"]},
    {"vertical":"msp_it_services","tier":"A","autonomous_sourcing":true,"seed_terms":["managed IT services {city}","managed service provider {city}"]},
    {"vertical":"b2b_accounting","tier":"B","autonomous_sourcing":false,"seed_terms":[]},
    {"vertical":"architecture_engineering","tier":"B","autonomous_sourcing":false,"seed_terms":[]},
    {"vertical":"equipment_rental","tier":"B","autonomous_sourcing":false,"seed_terms":[]},
    {"vertical":"wholesale_distribution","tier":"B","autonomous_sourcing":false,"seed_terms":[]}
  ]
  $json$::jsonb,
  vertical_tiers = $json$
  {
    "commercial_electrical":"A","commercial_hvac":"A","commercial_mechanical":"A","commercial_roofing":"A","restoration":"A","commercial_cleaning":"A","janitorial":"A","facility_services":"A","commercial_landscaping":"A","property_management":"A","hoa_management":"A","low_voltage_security":"A","fire_protection":"A","staffing_recruiting":"A","freight_brokerage":"A","commercial_insurance":"A","msp_it_services":"A",
    "b2b_accounting":"B","architecture_engineering":"B","equipment_rental":"B","wholesale_distribution":"B",
    "restaurant":"C","salon":"C","fitness":"C","auto":"C","auto_repair":"C","med_spa":"C","cleaning_residential":"C","landscaping_residential":"C","home_services":"C","cleaning":"C","landscaping":"C",
    "marketing_agency":"W","lead_gen_agency":"W"
  }
  $json$::jsonb
WHERE id = 1;

UPDATE clients
SET
  target_verticals = $json$
  [
    {"vertical":"law_firm","tier":"A","autonomous_sourcing":true,"seed_terms":["law firm {city} {state}","law office {city} {state}"]},
    {"vertical":"accounting","tier":"A","autonomous_sourcing":true,"seed_terms":["accounting firm {city} {state}","CPA firm {city} {state}"]}
  ]
  $json$::jsonb,
  vertical_tiers = $json$
  {
    "law_firm":"A","accounting":"A",
    "restaurant":"C","salon":"C","fitness":"C","auto":"C","auto_repair":"C","med_spa":"C","cleaning_residential":"C","landscaping_residential":"C","home_services":"C","cleaning":"C","landscaping":"C",
    "marketing_agency":"W","lead_gen_agency":"W"
  }
  $json$::jsonb
WHERE id = 10;

-- Existing MSHI verticals stay explicitly Tier A so the new resolver does not
-- silently downgrade an unrelated active client to "unknown".
UPDATE clients
SET vertical_tiers = '{
  "property_management":"A","probate_attorney":"A","renovation_lender":"A",
  "insurance_restoration":"A","home_inspector":"A","listing_agent":"A"
}'::jsonb
WHERE id = 2;

DO $$
BEGIN
  IF to_regclass('public.scout_queue') IS NOT NULL THEN
    UPDATE scout_queue
    SET status = 'queued', parked_at = NULL, updated_at = NOW()
    WHERE client_id = 1 AND vertical = 'property_management';
  END IF;
END $$;

COMMIT;
