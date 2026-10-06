-- Operator tenant switcher visibility (distinct from `active`, which gates runtime agents).
ALTER TABLE clients ADD COLUMN IF NOT EXISTS operator_switchable boolean DEFAULT true;

-- SPEC-WEB-001 test tenant — keep for cohort scripts, hide from production operator switching.
UPDATE clients
SET operator_switchable = false
WHERE slug = 'maynard-web';
