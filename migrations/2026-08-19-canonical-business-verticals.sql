-- PEC-116 — Canonical business vertical constraint for clients.vertical
-- Rollback: migrations/2026-08-19-canonical-business-verticals.rollback.sql
-- Values must stay in sync with utils/canonicalVerticals.js

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'clients_vertical_canonical_chk'
      AND conrelid = 'clients'::regclass
  ) THEN
    ALTER TABLE clients ADD CONSTRAINT clients_vertical_canonical_chk
      CHECK (vertical IS NULL OR vertical IN (
        'accounting',
        'architecture_engineering',
        'auto',
        'business_coaching',
        'cleaning',
        'commercial_cleaning',
        'commercial_hvac',
        'commercial_insurance',
        'commercial_landscaping',
        'commercial_roofing',
        'dental',
        'electrical',
        'equipment_rental',
        'facility_services',
        'fitness',
        'freight_brokerage',
        'home_renovation',
        'home_services',
        'hvac',
        'janitorial',
        'landscaping',
        'legal',
        'marketing_automation',
        'med_spa',
        'msp_it_services',
        'pest_control',
        'plumbing',
        'property_management',
        'restaurant',
        'restoration',
        'roofing',
        'salon',
        'staffing_recruiting',
        'wholesale_distribution'
      ));
  END IF;
END $$;
