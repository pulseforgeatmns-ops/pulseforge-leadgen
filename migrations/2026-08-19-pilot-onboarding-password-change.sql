-- SPEC-115 Pilot 0 — forced password change on first login.
-- Additive only. Existing users remain unlocked (DEFAULT false).

ALTER TABLE users
  ADD COLUMN IF NOT EXISTS password_change_required BOOLEAN NOT NULL DEFAULT FALSE;
