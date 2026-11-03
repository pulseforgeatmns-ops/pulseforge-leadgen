-- Add Private Lending to the canonical business vertical constraints.
-- Apply explicitly before creating a private_lending tenant.
-- Rollback: migrations/2026-11-03-private-lending-vertical.rollback.sql

BEGIN;

ALTER TABLE clients
  DROP CONSTRAINT IF EXISTS clients_vertical_canonical_chk;

ALTER TABLE clients
  ADD CONSTRAINT clients_vertical_canonical_chk
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
    'private_lending',
    'property_management',
    'restaurant',
    'restoration',
    'roofing',
    'salon',
    'staffing_recruiting',
    'web_design',
    'wholesale_distribution'
  ));

ALTER TABLE prospects
  DROP CONSTRAINT IF EXISTS prospects_vertical_canonical_chk;

ALTER TABLE prospects
  ADD CONSTRAINT prospects_vertical_canonical_chk
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
    'private_lending',
    'property_management',
    'restaurant',
    'restoration',
    'roofing',
    'salon',
    'staffing_recruiting',
    'web_design',
    'wholesale_distribution'
  ));

COMMIT;
