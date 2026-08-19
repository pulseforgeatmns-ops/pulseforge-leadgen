-- Rollback SPEC-115 password_change_required.

ALTER TABLE users DROP COLUMN IF EXISTS password_change_required;
