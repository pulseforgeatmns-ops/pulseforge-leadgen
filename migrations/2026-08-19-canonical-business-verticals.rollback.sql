-- Rollback for migrations/2026-08-19-canonical-business-verticals.sql

ALTER TABLE clients DROP CONSTRAINT IF EXISTS clients_vertical_canonical_chk;
